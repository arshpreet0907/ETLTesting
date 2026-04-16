"""
utils/custom_execution_utils.py
--------------------------------
Pipeline step functions and helpers extracted from custom_execution.py.

All step functions accept a ``ctx`` dict (pipeline context) containing the
resolved configuration.  The context is built once in custom_execution.py
after all module-level config is finalised and passed into each call.

Context keys expected by various functions
------------------------------------------
    config               : dict   — auto-config from get_table_config()
    target_mode          : str    — "mysql" or "snowflake"
    verify_schema        : bool
    source_query         : str|None
    source_query_file    : str|None
    target_query         : str|None
    target_query_file    : str|None
    transform_file       : str|None
    primary_keys         : list[str]
    exclude_cols         : list[str]|None
    compare_cols         : list[str]|None
    source_ddl           : str|None
    target_ddl           : str|None
    source_csv           : str
    target_csv           : str
    report_csv           : str
    source_filter        : dict
    target_filter        : dict
    enable_partitioning  : bool
    needs_prequery_bounds: bool
    source_partition_col : str|None
    target_partition_col : str|None
    partition_lower_bound: int|None
    partition_upper_bound: int|None
    num_partitions       : int
"""

import json
import os
from typing import Literal, Optional, Set

from utils.auto_config import build_filter_for_query
from utils.connections.source_connection import get_source_connection
from utils.connections.target_connection import get_target_connection
from utils.compare import compare_and_report
from utils.csv_writer import save_dataframe_as_csv
from utils.get_data import get_data
from utils.logger import get_logger
from utils.perform_transform import perform_transform
from utils.query_filter import apply_filter_to_sql
from utils.verify_schema import verify_schema_from_ddl

logger = get_logger(__name__)


# ============================================================================
# STANDALONE HELPERS (no ctx needed)
# ============================================================================

def resolve_query(query: str, query_file: str, query_type: str) -> str:
    """Return SQL string from either inline query or file (no trailing semicolon)."""
    if query:
        sql = query.strip()
        return sql[:-1].strip() if sql.endswith(";") else sql
    if query_file:
        if not os.path.isfile(query_file):
            raise FileNotFoundError(
                f"{query_type.upper()} query file not found: {query_file}"
            )
        with open(query_file, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
        return content[:-1].strip() if content.endswith(";") else content
    raise ValueError(
        f"Either {query_type.upper()}_QUERY or {query_type.upper()}_QUERY_FILE "
        "must be provided"
    )


def load_csv_with_schema(spark, csv_path: str):
    """
    Read a CSV file, restoring the original DataFrame schema if a
    companion ``.schema.json`` file exists.

    Falls back to ``inferSchema=True`` when no schema file is found.

    Parameters
    ----------
    spark : SparkSession
    csv_path : str
        Path to the CSV file.

    Returns
    -------
    pyspark.sql.DataFrame
    """
    from pyspark.sql.types import StructType

    schema_path = os.path.splitext(csv_path)[0] + ".schema.json"

    if os.path.isfile(schema_path):
        logger.info("Schema file found: %s — applying saved types", schema_path)
        with open(schema_path, "r", encoding="utf-8") as fh:
            schema = StructType.fromJson(json.loads(fh.read()))

        df = (
            spark.read
            .option("header", True)
            .option("nullValue", "")
            .schema(schema)
            .csv(csv_path)
        )
        logger.info("Loaded %s with saved schema (%d columns)", csv_path, len(df.columns))
    else:
        logger.warning(
            "No schema file found at %s — falling back to inferSchema=True. "
            "Numeric/date types may not match the original DataFrame exactly.",
            schema_path,
        )
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("nullValue", "")
            .csv(csv_path)
        )
        logger.info("Loaded %s with inferred schema (%d columns)", csv_path, len(df.columns))

    return df


def detect_partition_bounds(
    spark, db_type: str, partition_col: str, config: dict, target_mode: str
) -> tuple:
    """
    Run a lightweight SELECT MIN/MAX pre-query to detect partition bounds.
    Used when ENABLE_PARTITIONING=True and PK_FILTER_MODE="full".

    Returns
    -------
    tuple[int, int]
        (lower_bound, upper_bound)
    """
    if db_type == "source":
        jdbc_opts = get_source_connection()
        table = config.get("source_table", "unknown")
        database = config.get("source_database", "")
    else:
        jdbc_opts = get_target_connection(mode=target_mode)
        table = config.get("target_table", "unknown")
        database = config.get("target_database", "")

    full_table = f"{database}.{table}" if database else table
    bounds_query = (
        f"SELECT MIN({partition_col}) AS min_val, "
        f"MAX({partition_col}) AS max_val FROM {full_table}"
    )

    bounds_df = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", bounds_query)
        .load()
    )
    row = bounds_df.first()
    lb = int(row["min_val"])
    ub = int(row["max_val"])
    return lb, ub


# ============================================================================
# PIPELINE STEPS (accept ctx dict)
# ============================================================================

def step_0_verify_source_schema(spark, ctx: dict) -> bool:
    """Step 0: Verify source schema (optional)."""
    if not ctx["verify_schema"] or not ctx["source_ddl"]:
        return True

    logger.info("=" * 60)
    logger.info("STEP 0: Verify Source Schema")
    logger.info("=" * 60)

    config = ctx["config"]
    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=get_source_connection(),
        ddl_file=ctx["source_ddl"],
        database=config.get("source_database"),
        table=config.get("source_table"),
        dialect="mysql",
    )

    if not passed:
        logger.error("Source schema verification FAILED")
        return False

    logger.info("Source schema verification PASSED")
    return True


def step_1_extract_source(spark, ctx: dict):
    """Step 1: Extract source data, applying SOURCE_FILTER WHERE clause."""
    source_filter = ctx["source_filter"]

    logger.info("=" * 60)
    logger.info("STEP 1: Extract Source Data  [filter: %s]", source_filter["description"])
    logger.info("=" * 60)

    base_sql = resolve_query(ctx["source_query"], ctx["source_query_file"], "source")
    final_sql = apply_filter_to_sql(base_sql, source_filter["where_clause"])

    if source_filter["where_clause"]:
        logger.info("Applied WHERE clause: %s", source_filter["where_clause"])

    # Build partition kwargs (empty dict = single-partition)
    partition_kwargs = {}
    if ctx["enable_partitioning"]:
        # Auto-detect bounds for full mode via pre-query
        if ctx["needs_prequery_bounds"]:
            logger.info("Running pre-query to detect partition bounds...")
            _lb, _ub = detect_partition_bounds(
                spark, "source", ctx["source_partition_col"],
                ctx["config"], ctx["target_mode"],
            )
            ctx["partition_lower_bound"] = _lb
            ctx["partition_upper_bound"] = _ub
            logger.info("Detected bounds: [%d, %d]", _lb, _ub)

        partition_kwargs = dict(
            partition_col=ctx["source_partition_col"],
            lower_bound=ctx["partition_lower_bound"],
            upper_bound=ctx["partition_upper_bound"],
            num_partitions=ctx["num_partitions"],
        )

    source_df = get_data(
        spark=spark, db_type="source", query=final_sql, **partition_kwargs
    )

    logger.info("Step 1 complete.")
    return source_df


def step_2_transform(source_df, ctx: dict, target_mode: Literal["mysql", "snowflake"] = "mysql"):
    """Step 2: Apply transformations to source data."""
    logger.info("=" * 60)
    logger.info("STEP 2: Apply Transformations")
    logger.info("=" * 60)

    transformed_df = perform_transform(
        df=source_df,
        transform_file=ctx["transform_file"],
        target_mode=target_mode,
    )

    # Cache transformed_df for reuse in step 3 (CSV write) and step 5 (compare)
    # Without this, step 3 and step 5 both recompute the entire transformation
    transformed_df.cache()
    row_count = transformed_df.count()  # Materialize cache
    logger.info("Transformed DataFrame cached: %d rows", row_count)

    logger.info("Step 2 complete.")
    return transformed_df


def step_3_save_source_csv(transformed_df, source_df, ctx: dict):
    """Step 3: Save transformed source data to CSV.
    Also unpersists source_df (raw JDBC cache) now that the transform
    and CSV write are complete — freeing driver memory before the target
    extraction begins.
    """
    logger.info("=" * 60)
    logger.info("STEP 3: Save Transformed CSV")
    logger.info("=" * 60)

    save_dataframe_as_csv(transformed_df, ctx["source_csv"])
    logger.info("Source data saved → %s", ctx["source_csv"])
    # Unpersist raw source cache — no longer needed after CSV write
    source_df.unpersist()
    logger.info("Source DataFrame unpersisted (raw JDBC cache released)")
    logger.info("Step 3 complete.")


def step_3_5_verify_target_schema(spark, ctx: dict) -> bool:
    """Step 3.5: Verify target schema (optional)."""
    if not ctx["verify_schema"] or not ctx["target_ddl"]:
        return True

    logger.info("=" * 60)
    logger.info("STEP 3.5: Verify Target Schema")
    logger.info("=" * 60)

    target_mode = ctx["target_mode"]
    config = ctx["config"]

    jdbc_opts = get_target_connection(mode=target_mode)
    schema = jdbc_opts.get("sfSchema", "PUBLIC") if target_mode == "snowflake" else None

    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=jdbc_opts,
        ddl_file=ctx["target_ddl"],
        database=config.get("target_database"),
        table=config.get("target_table"),
        dialect=target_mode,
        schema=schema,
    )

    if not passed:
        logger.error("Target schema verification FAILED")
        return False

    logger.info("Target schema verification PASSED")
    return True


def step_4_extract_target(spark, ctx: dict):
    """Step 4: Extract target data, applying TARGET_FILTER WHERE clause."""
    target_filter = ctx["target_filter"]
    target_mode = ctx["target_mode"]

    logger.info("=" * 60)
    logger.info("STEP 4: Extract Target Data  [filter: %s]", target_filter["description"])
    logger.info("=" * 60)

    base_sql = resolve_query(ctx["target_query"], ctx["target_query_file"], "target")
    final_sql = apply_filter_to_sql(base_sql, target_filter["where_clause"])

    if target_filter["where_clause"]:
        logger.info("Applied WHERE clause: %s", target_filter["where_clause"])

    # Partition only for MySQL targets — Snowflake handles parallelism server-side
    partition_kwargs = {}
    if ctx["enable_partitioning"] and target_mode == "mysql":
        partition_kwargs = dict(
            partition_col=ctx["target_partition_col"],
            lower_bound=ctx["partition_lower_bound"],
            upper_bound=ctx["partition_upper_bound"],
            num_partitions=ctx["num_partitions"],
        )

    target_df = get_data(
        spark=spark,
        db_type="target",
        query=final_sql,
        target_mode=target_mode,
        **partition_kwargs,
    )

    logger.info("Step 4 complete.")
    return target_df


def step_4_1_save_target_csv(target_df, ctx: dict):
    """Step 4.1: Save target data to CSV.
    NOTE: Does NOT unpersist target_df — it's still needed for step 5 (compare).
    Unpersist happens after step 5 completes in main().
    """
    logger.info("=" * 60)
    logger.info("STEP 4.1: Save Target CSV")
    logger.info("=" * 60)

    save_dataframe_as_csv(target_df, ctx["target_csv"])
    logger.info("Target data saved → %s", ctx["target_csv"])
    # NOTE: target_df cache is kept for step 5 (compare)
    # It will be unpersisted after step 5 completes
    logger.info("Step 4.1 complete.")


def step_5_compare(spark, transformed_df, target_df, ctx: dict) -> int:
    """Step 5: Compare source and target DataFrames and generate report."""
    logger.info("=" * 60)
    logger.info("STEP 5: Compare Data & Generate Report")
    logger.info("=" * 60)

    compare_cols = ctx["compare_cols"]
    if compare_cols is None:
        all_cols = set(transformed_df.columns) & set(target_df.columns)
        compare_cols = sorted(
            all_cols - set(ctx["primary_keys"]) - set(ctx["exclude_cols"] or [])
        )
        logger.info("Auto-detected compare columns: %s", compare_cols)

    exit_code = compare_and_report(
        spark=spark,
        source_df=transformed_df,
        target_df=target_df,
        primary_key_cols=ctx["primary_keys"],
        compare_cols=compare_cols,
        output_path=ctx["report_csv"],
    )

    logger.info("Step 5 complete.")
    return exit_code


# ============================================================================
# CSV LOADERS
# ============================================================================

def load_csvs(spark, ctx: dict):
    """
    Helper: Load existing CSVs from disk (for compare-only runs).

    Schema preservation
    -------------------
    When a CSV was saved by ``save_dataframe_as_csv``, a companion
    ``.schema.json`` file is written next to it containing the original
    PySpark StructType.  This function checks for those files and applies
    the schema on read so that numeric, date, and boolean columns keep
    their original types — critical for the comparator's normalisation
    logic (``_normalise_df`` checks ``isinstance(field.dataType, …)``).

    If no schema file is found, the function falls back to
    ``inferSchema=True`` (best-effort) and logs a warning.
    """
    logger.info("Loading source CSV: %s", ctx["source_csv"])
    transformed_df = load_csv_with_schema(spark, ctx["source_csv"])

    logger.info("Loading target CSV: %s", ctx["target_csv"])
    target_df = load_csv_with_schema(spark, ctx["target_csv"])

    return transformed_df, target_df


def load_source_csv(spark, ctx: dict):
    """
    Load only the source (transformed) CSV from disk.

    Used when ``USE_SAVED_SOURCE_CSV = True`` to skip steps 1-3
    (source extract → transform → CSV write) and jump straight to
    target extraction (step 4) and comparison (step 5).

    The loaded DataFrame is cached so downstream steps (compare, etc.)
    benefit from the same cache semantics as the live-extraction path.

    Returns
    -------
    pyspark.sql.DataFrame
        Transformed source data with original types restored.
    """
    source_csv = ctx["source_csv"]

    if not os.path.isfile(source_csv):
        raise FileNotFoundError(
            f"USE_SAVED_SOURCE_CSV is True but source CSV not found: {source_csv}\n"
            "Run the full pipeline (steps 1-3) at least once first."
        )

    logger.info("=" * 60)
    logger.info("LOADING SAVED SOURCE CSV  (steps 0-3 skipped)")
    logger.info("=" * 60)

    transformed_df = load_csv_with_schema(spark, source_csv)

    # Cache so step 5 (compare) reads from memory, matching the live path
    transformed_df.cache()
    row_count = transformed_df.count()  # materialise cache
    logger.info("Source CSV loaded and cached: %d rows, %d columns",
                row_count, len(transformed_df.columns))

    return transformed_df


def load_target_csv(spark, ctx: dict):
    """
    Load target data from a previously saved CSV on disk.

    Used when ``USE_SAVED_TARGET_CSV = True`` to skip steps 3.5-4
    (target schema verify → target extraction) and load from the CSV
    written by step 4.1 instead.

    The loaded DataFrame is cached so step 5 (compare) reads from memory,
    matching the cache semantics of the live-extraction path.

    Returns
    -------
    pyspark.sql.DataFrame
        Target data with original types restored via companion .schema.json.
    """
    target_csv = ctx["target_csv"]

    if not os.path.isfile(target_csv):
        raise FileNotFoundError(
            f"USE_SAVED_TARGET_CSV is True but target CSV not found: {target_csv}\n"
            "Run step 4.1 (save target CSV) at least once first."
        )

    logger.info("=" * 60)
    logger.info("LOADING SAVED TARGET CSV  (steps 3.5-4 skipped)")
    logger.info("=" * 60)

    target_df = load_csv_with_schema(spark, target_csv)

    # Cache so step 5 (compare) reads from memory, matching the live path
    target_df.cache()
    row_count = target_df.count()  # materialise cache
    logger.info("Target CSV loaded and cached: %d rows, %d columns",
                row_count, len(target_df.columns))

    return target_df


def build_load_filters(
    config: dict,
    pk_filter_mode: str,
    pk_range: dict,
    pk_set: Set,
    date_mode: str,
    date_from: Optional[str] = None,
    date_from_col: Optional[str] = None,
    date_to: Optional[str] = None,
    date_to_col: Optional[str] = None,
) -> tuple:
    """
    Build separate WHERE clause filters for source and target queries.
    Uses correct PK column for each (source_primary_keys vs target_primary_keys).

    Parameters
    ----------
    config : dict
        Auto-config from get_table_config().
    pk_filter_mode : str
        One of "full", "pk_range", "pk_set".
    pk_range : dict
        {"lower": int|None, "upper": int|None}
    pk_set : set
        Specific PK values for pk_set mode.
    date_mode : str
        One of "full", "range".
    date_from, date_from_col, date_to, date_to_col : str|None
        Date watermark filter settings.

    Returns
    -------
    tuple
        (source_filter_dict, target_filter_dict)
        Each dict has: {"where_clause": str, "pk_mode": str,
                        "date_mode": str, "description": str}
    """
    if not config:
        return (
            {"where_clause": "", "description": "no config"},
            {"where_clause": "", "description": "no config"},
        )

    source_filter = build_filter_for_query(
        query_type="source",
        config=config,
        pk_filter_mode=pk_filter_mode,
        pk_range=pk_range,
        pk_set=pk_set,
        date_mode=date_mode,
        date_from=date_from,
        date_from_col=date_from_col,
        date_to=date_to,
        date_to_col=date_to_col,
    )

    target_filter = build_filter_for_query(
        query_type="target",
        config=config,
        pk_filter_mode=pk_filter_mode,
        pk_range=pk_range,
        pk_set=pk_set,
        date_mode=date_mode,
        date_from=date_from,
        date_from_col=date_from_col,
        date_to=date_to,
        date_to_col=date_to_col,
    )

    return source_filter, target_filter


