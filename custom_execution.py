"""
custom_execution.py
--------------------
Custom ETL validation without Excel dependency (Scenario 2).

Configure the variables below and uncomment the steps you want to run.

FILTER QUICK REFERENCE
======================

PK_FILTER_MODE  options
-----------------------
  "full"      → load entire table (no PK filter)
  "pk_range"  → load rows where PK is within PK_RANGE bounds
                PK_RANGE = {"lower": 1, "upper": 500000}
                Set either bound to None for a one-sided limit.
  "pk_set"    → load only the specific PK values in PK_SET
                PK_SET = {1, 5, 99, 204}

DATE_WATERMARK_MODE  options
-----------------------------
  "full"   → no date filter applied
  "range"  → apply one or both of:
               DATE_FROM_COL >= DATE_FROM
               DATE_TO_COL   <= DATE_TO
             Set either (DATE_FROM / DATE_TO) to None for a one-sided filter.
             DATE_FROM_COL and DATE_TO_COL can be the same column or different columns.
             An error is raised if the specified column does not exist in the table.
"""

import os
import sys
import time
from datetime import timedelta
from typing import Literal, Optional, Set

from utils.connections.spark_session import get_spark_session
from utils.get_data import get_data
from utils.perform_transform import perform_transform
from utils.compare import compare_and_report
from utils.verify_schema import verify_schema_from_ddl
from utils.connections.source_connection import get_source_connection
from utils.connections.target_connection import get_target_connection
from utils.logger import get_logger
from utils.csv_writer import save_dataframe_as_csv
from utils.auto_config import get_table_config, list_available_tables, build_filter_for_query
from utils.query_filter import build_where_clause, apply_filter_to_sql, get_columns_from_ddl

logger = get_logger(__name__)


# ============================================================================
# SECTION 1 — TABLE & MODE CONFIGURATION
# ============================================================================

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ AUTO-CONFIGURATION MODE (Recommended)                                   │
# │ Just set the table name — query files, transform, PKs auto-detected.   │
# └─────────────────────────────────────────────────────────────────────────┘

TABLE_NAME = "cost_ledger"      # Set to None to use manual configuration below.
                                # Available: cost_ledger, employee_master,
                                # engine_assembly_log, logistics_shipments,
                                # paint_shop_log, parts_inventory,
                                # production_orders, quality_inspections,
                                # sales_orders, supplier_master,
                                # vehicle_master, warranty_claims

TARGET_MODE: Literal["mysql", "snowflake"] = "snowflake"

VERIFY_SCHEMA = True            # Set True to verify source & target schemas before run.
COMPARE_COLS  = None            # None = auto-detect all non-PK columns.
                                # Or specify: ["col1", "col2"]

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ MANUAL CONFIGURATION MODE (Advanced)                                    │
# │ Used only when TABLE_NAME = None.                                       │
# └─────────────────────────────────────────────────────────────────────────┘

# TABLE_NAME = None             # Uncomment to enable manual mode.

SOURCE_QUERY      = None        # Inline SQL string, or leave None to use file.
SOURCE_QUERY_FILE = None        # e.g. "generated/cost_ledger/03_extract_source.sql"
TARGET_QUERY      = None        # Inline SQL string, or leave None to use file.
TARGET_QUERY_FILE = None        # e.g. "generated/cost_ledger/05_extract_target_ms.sql"
TRANSFORM_FILE    = None        # e.g. "generated/cost_ledger/04_transform.py"
PRIMARY_KEYS      = None        # e.g. ["ledger_id"]  or  ["order_id", "line_id"]
EXCLUDE_COLS      = None        # e.g. ["load_ts", "updated_at"]
SOURCE_DDL        = None        # e.g. "generated/cost_ledger/01_create_source_table.sql"
TARGET_DDL        = None        # e.g. "generated/cost_ledger/02_create_target_ms.sql"
OUTPUT_DIR        = None        # e.g. "output/cost_ledger"


# ============================================================================
# SECTION 2 — PK FILTER
# ============================================================================
#
# Controls which rows are loaded from BOTH source and target.
# Mutually exclusive modes — choose exactly one.
#
# Mode      Variable(s) used
# --------  -----------------
# full      (none)
# pk_range  PK_RANGE
# pk_set    PK_SET

PK_FILTER_MODE: Literal["full", "pk_range", "pk_set"] = "full"

# Used when PK_FILTER_MODE = "pk_range"
# Set either bound to None for a one-sided limit:
#   {"lower": 1,    "upper": 500000}  → 1 <= pk <= 500000
#   {"lower": 1,    "upper": None}    → pk >= 1
#   {"lower": None, "upper": 500000}  → pk <= 500000
PK_RANGE: dict = {"lower": 110010000, "upper": 110020000}

# Used when PK_FILTER_MODE = "pk_set"
# Provide the exact PK values to load as a Python set:
#   PK_SET = {1, 5, 99, 204, 1001}
PK_SET: Set = {110010001,110010002,110010003,110010004,110010005,110010006,110010007,110010008,110010009,110010010}

# ============================================================================
# SECTION 3 — DATE WATERMARK FILTER
# ============================================================================
#
# Controls which rows are loaded based on date columns.
# Applied on top of any PK filter (both may be active simultaneously).
#
# Mode    Variables used
# ------  --------------
# full    (none)
# range   DATE_FROM + DATE_FROM_COL  (lower bound, optional)
#         DATE_TO   + DATE_TO_COL    (upper bound, optional)
#
# Rules:
#   - DATE_FROM_COL and DATE_TO_COL may be the same column or different columns.
#   - Set DATE_FROM = None to skip the lower bound.
#   - Set DATE_TO   = None to skip the upper bound.
#   - An error is raised if the column name is not found in the table DDL.

DATE_WATERMARK_MODE: Literal["full", "range"] = "range"

# Lower date bound: WHERE DATE_FROM_COL >= DATE_FROM
DATE_FROM:     Optional[str] = "2020-06-10 04:52:53"            # e.g. "2024-01-01" or "2024-01-01 00:00:00"
DATE_FROM_COL: Optional[str] = "created_at"   # Column to apply the lower bound on.

# Upper date bound: WHERE DATE_TO_COL <= DATE_TO
DATE_TO:       Optional[str] = "2024-09-07 16:26:19"            # e.g. "2024-06-30" or "2024-06-30 23:59:59"
DATE_TO_COL:   Optional[str] = "created_at"   # Column to apply the upper bound on.
                                               # Can be the same as DATE_FROM_COL.
# Pk and Date modes work in AND mode

# ============================================================================
# AUTO-CONFIGURATION LOADER  (Do not edit below this line)
# ============================================================================

_config: dict = {}

if TABLE_NAME:
    logger.info("Using AUTO-CONFIGURATION for table: %s", TABLE_NAME)
    try:
        _config = get_table_config(TABLE_NAME, target_mode=TARGET_MODE)

        SOURCE_QUERY_FILE = _config["source_query_file"]
        TARGET_QUERY_FILE = _config["target_query_file"]
        TRANSFORM_FILE    = _config["transform_file"]
        PRIMARY_KEYS      = _config["primary_keys"]
        EXCLUDE_COLS      = _config["exclude_cols"]
        SOURCE_DDL        = _config["source_ddl"]
        TARGET_DDL        = _config["target_ddl"]
        OUTPUT_DIR        = _config["output_dir"]

        logger.info("Auto-configuration loaded:")
        logger.info("  Source table : %s", _config.get("source_table"))
        logger.info("  Target table : %s", _config.get("target_table"))
        logger.info("  Primary Keys : %s", PRIMARY_KEYS)
        logger.info("  Transform    : %s", os.path.basename(TRANSFORM_FILE))

    except Exception as e:
        logger.error("Auto-configuration failed: %s", e)
        logger.info("Available tables: %s", ", ".join(list_available_tables(TARGET_MODE)))
        sys.exit(1)
else:
    logger.info("Using MANUAL CONFIGURATION")
    if not all([SOURCE_QUERY or SOURCE_QUERY_FILE,
                TARGET_QUERY or TARGET_QUERY_FILE,
                PRIMARY_KEYS]):
        logger.error(
            "Manual configuration incomplete. Required: "
            "SOURCE_QUERY/SOURCE_QUERY_FILE, TARGET_QUERY/TARGET_QUERY_FILE, PRIMARY_KEYS"
        )
        sys.exit(1)
    if not OUTPUT_DIR:
        OUTPUT_DIR = "output/custom"




# CSV file paths (derived — do not edit)
SOURCE_CSV = os.path.join(OUTPUT_DIR, "source_enriched.csv")
TARGET_CSV = os.path.join(OUTPUT_DIR, "target_actual.csv")
REPORT_CSV = os.path.join(OUTPUT_DIR, "diff_report.csv")


# ============================================================================
# FILTER BUILDER  (Do not edit — reads SECTION 2 & 3 variables above)
# ============================================================================

def _build_load_filters() -> tuple:
    """
    Build separate WHERE clause filters for source and target queries.
    Uses correct PK column for each (source_primary_keys vs target_primary_keys).

    Returns
    -------
    tuple
        (source_filter_dict, target_filter_dict)
        Each dict has: {"where_clause": str, "pk_mode": str, "date_mode": str, "description": str}
    """
    if not _config:
        return {"where_clause": "", "description": "no config"}, {"where_clause": "", "description": "no config"}
    
    try:
        source_filter = build_filter_for_query(
            query_type="source",
            config=_config,
            pk_filter_mode=PK_FILTER_MODE,
            pk_range=PK_RANGE,
            pk_set=PK_SET,
            date_mode=DATE_WATERMARK_MODE,
            date_from=DATE_FROM,
            date_from_col=DATE_FROM_COL,
            date_to=DATE_TO,
            date_to_col=DATE_TO_COL,
        )
        
        target_filter = build_filter_for_query(
            query_type="target",
            config=_config,
            pk_filter_mode=PK_FILTER_MODE,
            pk_range=PK_RANGE,
            pk_set=PK_SET,
            date_mode=DATE_WATERMARK_MODE,
            date_from=DATE_FROM,
            date_from_col=DATE_FROM_COL,
            date_to=DATE_TO,
            date_to_col=DATE_TO_COL,
        )
        
        return source_filter, target_filter
        
    except (ValueError, FileNotFoundError) as exc:
        logger.error("Filter configuration error: %s", exc)
        sys.exit(1)


# Build filters once at module load — fails fast on bad config
SOURCE_FILTER, TARGET_FILTER = _build_load_filters()

logger.info("Source filter: %s", SOURCE_FILTER["description"])
if SOURCE_FILTER["where_clause"]:
    logger.info("  WHERE %s", SOURCE_FILTER["where_clause"])

logger.info("Target filter: %s", TARGET_FILTER["description"])
if TARGET_FILTER["where_clause"]:
    logger.info("  WHERE %s", TARGET_FILTER["where_clause"])

EXCLUDE_COLS=["load_ts"]

# ============================================================================
# PIPELINE STEPS
# ============================================================================

def step_0_verify_source_schema(spark):
    """Step 0: Verify source schema (optional)."""
    if not VERIFY_SCHEMA or not SOURCE_DDL:
        return True

    logger.info("=" * 60)
    logger.info("STEP 0: Verify Source Schema")
    logger.info("=" * 60)

    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=get_source_connection(),
        ddl_file=SOURCE_DDL,
        database=_config.get("source_database"),
        table=_config.get("source_table"),
        dialect="mysql",
    )

    if not passed:
        logger.error("Source schema verification FAILED")
        return False

    logger.info("Source schema verification PASSED")
    return True


def step_1_extract_source(spark):
    """Step 1: Extract source data, applying SOURCE_FILTER WHERE clause."""
    logger.info("=" * 60)
    logger.info("STEP 1: Extract Source Data  [filter: %s]", SOURCE_FILTER["description"])
    logger.info("=" * 60)

    base_sql  = _resolve_query(SOURCE_QUERY, SOURCE_QUERY_FILE, "source")
    final_sql = apply_filter_to_sql(base_sql, SOURCE_FILTER["where_clause"])

    if SOURCE_FILTER["where_clause"]:
        logger.info("Applied WHERE clause: %s", SOURCE_FILTER["where_clause"])

    source_df = get_data(spark=spark, db_type="source", query=final_sql)

    logger.info("Step 1 complete.")
    return source_df


def step_2_transform(source_df, target_mode: Literal["mysql", "snowflake"] = "mysql"):
    """Step 2: Apply transformations to source data."""
    logger.info("=" * 60)
    logger.info("STEP 2: Apply Transformations")
    logger.info("=" * 60)

    transformed_df = perform_transform(
        df=source_df,
        transform_file=TRANSFORM_FILE,
        target_mode=target_mode,
    )

    logger.info("Step 2 complete.")
    return transformed_df


def step_3_save_source_csv(transformed_df):
    """Step 3: Save transformed source data to CSV."""
    logger.info("=" * 60)
    logger.info("STEP 3: Save Transformed CSV")
    logger.info("=" * 60)

    save_dataframe_as_csv(transformed_df, SOURCE_CSV)
    logger.info("Source data saved → %s", SOURCE_CSV)
    logger.info("Step 3 complete.")


def step_4_1_save_target_csv(target_df):
    """Step 4.1: Save target data to CSV."""
    logger.info("=" * 60)
    logger.info("STEP 4.1: Save Target CSV")
    logger.info("=" * 60)

    save_dataframe_as_csv(target_df, TARGET_CSV)
    logger.info("Target data saved → %s", TARGET_CSV)
    logger.info("Step 4.1 complete.")


def step_3_5_verify_target_schema(spark):
    """Step 3.5: Verify target schema (optional)."""
    if not VERIFY_SCHEMA or not TARGET_DDL:
        return True

    logger.info("=" * 60)
    logger.info("STEP 3.5: Verify Target Schema")
    logger.info("=" * 60)

    jdbc_opts = get_target_connection(mode=TARGET_MODE)
    schema = jdbc_opts.get("sfSchema", "PUBLIC") if TARGET_MODE == "snowflake" else None

    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=jdbc_opts,
        ddl_file=TARGET_DDL,
        database=_config.get("target_database"),
        table=_config.get("target_table"),
        dialect=TARGET_MODE,
        schema=schema,
    )

    if not passed:
        logger.error("Target schema verification FAILED")
        return False

    logger.info("Target schema verification PASSED")
    return True


def step_4_extract_target(spark):
    """Step 4: Extract target data, applying TARGET_FILTER WHERE clause."""
    logger.info("=" * 60)
    logger.info("STEP 4: Extract Target Data  [filter: %s]", TARGET_FILTER["description"])
    logger.info("=" * 60)

    base_sql  = _resolve_query(TARGET_QUERY, TARGET_QUERY_FILE, "target")
    final_sql = apply_filter_to_sql(base_sql, TARGET_FILTER["where_clause"])

    if TARGET_FILTER["where_clause"]:
        logger.info("Applied WHERE clause: %s", TARGET_FILTER["where_clause"])

    target_df = get_data(
        spark=spark,
        db_type="target",
        query=final_sql,
        target_mode=TARGET_MODE,
    )

    logger.info("Step 4 complete.")
    return target_df


def step_5_compare(spark, transformed_df, target_df):
    """Step 5: Compare source and target DataFrames and generate report."""
    logger.info("=" * 60)
    logger.info("STEP 5: Compare Data & Generate Report")
    logger.info("=" * 60)

    compare_cols = COMPARE_COLS
    if compare_cols is None:
        all_cols     = set(transformed_df.columns) & set(target_df.columns)
        compare_cols = sorted(all_cols - set(PRIMARY_KEYS) - set(EXCLUDE_COLS or []))
        logger.info("Auto-detected compare columns: %s", compare_cols)

    exit_code = compare_and_report(
        spark=spark,
        source_df=transformed_df,
        target_df=target_df,
        primary_key_cols=PRIMARY_KEYS,
        compare_cols=compare_cols,
        output_path=REPORT_CSV,
    )

    logger.info("Step 5 complete.")
    return exit_code


def load_csvs(spark):
    """Helper: Load existing CSVs from disk (for compare-only runs)."""
    logger.info("Loading source CSV: %s", SOURCE_CSV)
    transformed_df = (
        spark.read.option("header", True).option("inferSchema", False).csv(SOURCE_CSV)
    )
    logger.info("Loading target CSV: %s", TARGET_CSV)
    target_df = (
        spark.read.option("header", True).option("inferSchema", False).csv(TARGET_CSV)
    )
    return transformed_df, target_df


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    """
    Run custom ETL validation pipeline.

    Patterns
    --------
    A  Full pipeline   : steps 0, 1, 2, 3, 3.5, 4, 4.1, 5
    B  Compare only    : load_csvs() + step 5
    C  Source only     : steps 1, 2, 3
    D  Target only     : step 4
    E  Custom          : mix and match as needed
    """
    start_time = time.time()

    try:
        logger.info("=" * 60)
        logger.info("Custom ETL Validation Pipeline")
        logger.info("  Table  : %s", TABLE_NAME or "(manual)")
        logger.info("  Target : %s", TARGET_MODE)
        logger.info("  Source filter : %s", SOURCE_FILTER["description"])
        logger.info("  Target filter : %s", TARGET_FILTER["description"])
        logger.info("=" * 60)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        spark = get_spark_session(app_name="ETL_Custom")

        # Step 0: optional source schema check
        if not step_0_verify_source_schema(spark):
            logger.error("Exiting due to source schema verification failure")
            return 2

        # Step 1: extract source
        t0 = time.time()
        source_df = step_1_extract_source(spark)
        source_df.printSchema()
        row_count = source_df.count()
        logger.info("Source row count: %d  (%.2fs)", row_count, time.time() - t0)

        # Step 2: transform
        t0 = time.time()
        transformed_df = step_2_transform(source_df, TARGET_MODE)
        transformed_df.printSchema()
        row_count = transformed_df.count()
        logger.info("Transformed row count: %d  (%.2fs)", row_count, time.time() - t0)

        # Step 3: save source CSV
        step_3_save_source_csv(transformed_df)

        # Step 3.5: optional target schema check
        if not step_3_5_verify_target_schema(spark):
            logger.error("Exiting due to target schema verification failure")
            return 2

        # Step 4: extract target
        target_df = step_4_extract_target(spark)
        step_4_1_save_target_csv(target_df)

        # Step 5: compare
        exit_code = step_5_compare(spark, transformed_df, target_df)

        spark.stop()

        elapsed = timedelta(seconds=int(time.time() - start_time))
        minutes = elapsed.seconds // 60
        seconds = elapsed.seconds % 60
        logger.info("=" * 60)
        logger.info("Pipeline complete. Exit code: %d", exit_code)
        logger.info("Total time: %d min %d sec", minutes, seconds)
        logger.info("=" * 60)

        return exit_code

    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 1
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return 99


def _resolve_query(query: str, query_file: str, query_type: str) -> str:
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


if __name__ == "__main__":
    sys.exit(main())
