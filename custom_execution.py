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
from utils.auto_config import get_table_config, list_available_tables
from utils.logger import get_logger
from utils.custom_execution_utils import (
    step_0_verify_source_schema,
    step_1_extract_source,
    step_2_transform,
    step_3_save_source_csv,
    step_3_5_verify_target_schema,
    step_4_extract_target,
    step_4_1_save_target_csv,
    step_5_compare,
    load_csvs,
    load_source_csv,
    load_target_csv,
    build_load_filters,
)

logger = get_logger(__name__)


# ============================================================================
# SECTION 1 — TABLE & MODE CONFIGURATION
# ============================================================================

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ AUTO-CONFIGURATION MODE (Recommended)                                   │
# │ Just set the table name — query files, transform, PKs auto-detected.   │
# └─────────────────────────────────────────────────────────────────────────┘

TABLE_NAME = "employee_master"  # Set to None to use manual configuration below.
                                # Available: cost_ledger, employee_master,
                                # engine_assembly_log, logistics_shipments,
                                # paint_shop_log, parts_inventory,
                                # production_orders, quality_inspections,
                                # sales_orders, supplier_master,
                                # vehicle_master, warranty_claims

TARGET_MODE: Literal["mysql", "snowflake"] = "mysql"

VERIFY_SCHEMA = True            # Set True to verify source & target schemas before run.

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ CSV RE-USE MODE                                                         │
# │ Set True to skip steps 0–3 (source extract + transform + CSV write)     │
# │ and load the previously saved transformed CSV instead.                  │
# │ The schema is restored from the .schema.json file saved alongside the   │
# │ CSV, so numeric/date types are preserved for accurate comparison.       │
# │ Pipeline continues from step 3.5 (target schema check) onward.         │
# └─────────────────────────────────────────────────────────────────────────┘
USE_SAVED_SOURCE_CSV: bool = False

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ TARGET CSV RE-USE MODE                                                  │
# │ Set True to also skip steps 3.5–4 (target schema check + extraction)    │
# │ and load the previously saved target CSV instead.                       │
# │ Requires step 4.1 (save target CSV) to have been run at least once.     │
# │ When both USE_SAVED_SOURCE_CSV and USE_SAVED_TARGET_CSV are True,       │
# │ the pipeline skips all extraction and goes directly to step 5 (compare).│
# └─────────────────────────────────────────────────────────────────────────┘
USE_SAVED_TARGET_CSV: bool = False

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

DATE_WATERMARK_MODE: Literal["full", "range"] = "full"

# Lower date bound: WHERE DATE_FROM_COL >= DATE_FROM
DATE_FROM:     Optional[str] = "2020-06-10 04:52:53"            # e.g. "2024-01-01" or "2024-01-01 00:00:00"
DATE_FROM_COL: Optional[str] = "created_at"   # Column to apply the lower bound on.

# Upper date bound: WHERE DATE_TO_COL <= DATE_TO
DATE_TO:       Optional[str] = "2024-09-07 16:26:19"            # e.g. "2024-06-30" or "2024-06-30 23:59:59"
DATE_TO_COL:   Optional[str] = "created_at"   # Column to apply the upper bound on.
                                               # Can be the same as DATE_FROM_COL.
# Pk and Date modes work in AND mode

# ============================================================================
# SECTION 4 — JDBC PARTITIONING (MySQL only)
# ============================================================================
#
# Parallelises JDBC reads by splitting the query into N partitions,
# each fetched by a separate JDBC connection in parallel.
#
# NOTE: Only applies to MySQL (source + mysql target).
#       Snowflake targets always use single-partition JDBC — Snowflake
#       already parallelises execution server-side.
#
# When ENABLE_PARTITIONING = True:
#   - pk_range mode : bounds auto-derived from PK_RANGE if not set manually.
#   - pk_set mode   : partitioning is disabled (too few rows to benefit).
#   - full mode     : a pre-query SELECT MIN/MAX runs to detect bounds.
#
# Set ENABLE_PARTITIONING = False to use single-partition JDBC (original behaviour).

ENABLE_PARTITIONING: bool = False

PARTITION_COL: Optional[str] = None          # e.g. "ledger_id" — auto-set from PRIMARY_KEYS[0] if None
PARTITION_LOWER_BOUND: Optional[int] = None  # auto-derived from PK_RANGE when pk_range mode
PARTITION_UPPER_BOUND: Optional[int] = None  # auto-derived from PK_RANGE when pk_range mode
NUM_PARTITIONS: int = 4                      # parallel JDBC connections (4–8 for local[*])


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


# cols to exclude since this is different at each load
EXCLUDE_COLS=["load_ts","batch_id"]
# CSV file paths (derived — do not edit)
SOURCE_CSV = os.path.join(OUTPUT_DIR, "source_enriched.csv")
TARGET_CSV = os.path.join(OUTPUT_DIR, "target_actual.csv")
REPORT_CSV = os.path.join(OUTPUT_DIR, "diff_report.csv")


# ============================================================================
# FILTER BUILDER  (reads SECTION 2 & 3 variables above)
# ============================================================================

# Build filters once at module load — fails fast on bad config
try:
    SOURCE_FILTER, TARGET_FILTER = build_load_filters(
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
except (ValueError, FileNotFoundError) as exc:
    logger.error("Filter configuration error: %s", exc)
    sys.exit(1)


logger.info("Source filter: %s", SOURCE_FILTER["description"])
if SOURCE_FILTER["where_clause"]:
    logger.info("  WHERE %s", SOURCE_FILTER["where_clause"])

logger.info("Target filter: %s", TARGET_FILTER["description"])
if TARGET_FILTER["where_clause"]:
    logger.info("  WHERE %s", TARGET_FILTER["where_clause"])

# ============================================================================
# PARTITIONING AUTO-DERIVE  (Do not edit — reads SECTION 4 variables above)
# ============================================================================

_NEEDS_PREQUERY_BOUNDS = False  # set True when full mode needs MIN/MAX pre-query

if ENABLE_PARTITIONING:
    # Auto-set partition column from first primary key
    # Use source PK for source queries, target PK for target queries
    SOURCE_PARTITION_COL: Optional[str] = None
    TARGET_PARTITION_COL: Optional[str] = None

    if PARTITION_COL is None:
        if _config.get("source_primary_keys"):
            SOURCE_PARTITION_COL = _config["source_primary_keys"][0]
        if _config.get("target_primary_keys"):
            TARGET_PARTITION_COL = _config["target_primary_keys"][0]
        if not SOURCE_PARTITION_COL and PRIMARY_KEYS:
            SOURCE_PARTITION_COL = PRIMARY_KEYS[0]
        if not TARGET_PARTITION_COL and PRIMARY_KEYS:
            TARGET_PARTITION_COL = PRIMARY_KEYS[0]
        # Set PARTITION_COL to source PK for backward compat logging
        PARTITION_COL = SOURCE_PARTITION_COL
        logger.info("Partition columns auto-set — source: %s, target: %s",
                     SOURCE_PARTITION_COL, TARGET_PARTITION_COL)
    else:
        SOURCE_PARTITION_COL = PARTITION_COL
        TARGET_PARTITION_COL = PARTITION_COL
        logger.info("Partition column (manual): %s", PARTITION_COL)

    if PK_FILTER_MODE == "pk_set":
        # Too few rows to benefit from partitioning
        logger.warning(
            "Partitioning disabled: pk_set mode has too few rows to benefit. "
            "Using single-partition JDBC."
        )
        ENABLE_PARTITIONING = False

    elif PK_FILTER_MODE == "pk_range":
        # Auto-derive bounds from PK_RANGE if not manually set
        if PARTITION_LOWER_BOUND is None and PK_RANGE.get("lower") is not None:
            PARTITION_LOWER_BOUND = PK_RANGE["lower"]
        if PARTITION_UPPER_BOUND is None and PK_RANGE.get("upper") is not None:
            PARTITION_UPPER_BOUND = PK_RANGE["upper"]
        if PARTITION_LOWER_BOUND is None or PARTITION_UPPER_BOUND is None:
            logger.warning(
                "Partitioning disabled: pk_range mode but PK_RANGE bounds are incomplete. "
                "Set PARTITION_LOWER_BOUND/PARTITION_UPPER_BOUND manually."
            )
            ENABLE_PARTITIONING = False
        else:
            logger.info(
                "Partition bounds auto-derived from PK_RANGE: [%d, %d], %d partitions",
                PARTITION_LOWER_BOUND, PARTITION_UPPER_BOUND, NUM_PARTITIONS,
            )

    elif PK_FILTER_MODE == "full":
        # Need a pre-query to detect MIN/MAX bounds at runtime
        if PARTITION_LOWER_BOUND is None or PARTITION_UPPER_BOUND is None:
            _NEEDS_PREQUERY_BOUNDS = True
            logger.info(
                "Partitioning enabled (full mode): bounds will be auto-detected "
                "via SELECT MIN/MAX pre-query at runtime."
            )
        else:
            logger.info(
                "Partition bounds (manual): [%d, %d], %d partitions",
                PARTITION_LOWER_BOUND, PARTITION_UPPER_BOUND, NUM_PARTITIONS,
            )

    if ENABLE_PARTITIONING and not PARTITION_COL:
        logger.warning(
            "Partitioning disabled: no PARTITION_COL and no PRIMARY_KEYS available."
        )
        ENABLE_PARTITIONING = False

if ENABLE_PARTITIONING:
    logger.info("JDBC Partitioning: ENABLED (col=%s, partitions=%d)", PARTITION_COL, NUM_PARTITIONS)
else:
    logger.info("JDBC Partitioning: DISABLED (single-partition reads)")


# ============================================================================
# PIPELINE CONTEXT — bundles all resolved config for step functions
# ============================================================================

pipeline_ctx = dict(
    config=_config,
    target_mode=TARGET_MODE,
    verify_schema=VERIFY_SCHEMA,
    source_query=SOURCE_QUERY,
    source_query_file=SOURCE_QUERY_FILE,
    target_query=TARGET_QUERY,
    target_query_file=TARGET_QUERY_FILE,
    transform_file=TRANSFORM_FILE,
    primary_keys=PRIMARY_KEYS,
    exclude_cols=EXCLUDE_COLS,
    compare_cols=COMPARE_COLS,
    source_ddl=SOURCE_DDL,
    target_ddl=TARGET_DDL,
    source_csv=SOURCE_CSV,
    target_csv=TARGET_CSV,
    report_csv=REPORT_CSV,
    source_filter=SOURCE_FILTER,
    target_filter=TARGET_FILTER,
    enable_partitioning=ENABLE_PARTITIONING,
    needs_prequery_bounds=_NEEDS_PREQUERY_BOUNDS,
    source_partition_col=SOURCE_PARTITION_COL if ENABLE_PARTITIONING else None,
    target_partition_col=TARGET_PARTITION_COL if ENABLE_PARTITIONING else None,
    partition_lower_bound=PARTITION_LOWER_BOUND,
    partition_upper_bound=PARTITION_UPPER_BOUND,
    num_partitions=NUM_PARTITIONS,
)


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    """
    Run custom ETL validation pipeline.

    Patterns
    --------
    A  Full pipeline    : steps 0, 1, 2, 3, 3.5, 4, 4.1, 5
    B  Compare only     : load_csvs() + step 5
    C  Source only      : steps 1, 2, 3
    D  Target only      : step 4
    E  Custom           : mix and match as needed
    F  CSV re-use       : load_source_csv() + steps 3.5, 4, 5
                          (USE_SAVED_SOURCE_CSV = True)
    G  Full CSV re-use  : load_source_csv() + load_target_csv() + step 5
                          (USE_SAVED_SOURCE_CSV = True, USE_SAVED_TARGET_CSV = True)
    """
    start_time = time.time()

    try:
        logger.info("=" * 60)
        logger.info("Custom ETL Validation Pipeline")
        logger.info("  Table  : %s", TABLE_NAME or "(manual)")
        logger.info("  Target : %s", TARGET_MODE)
        logger.info("  Source : %s", "saved CSV" if USE_SAVED_SOURCE_CSV else "live extraction")
        logger.info("  Target : %s", "saved CSV" if USE_SAVED_TARGET_CSV else "live extraction")
        logger.info("  Source filter : %s", SOURCE_FILTER["description"])
        logger.info("  Target filter : %s", TARGET_FILTER["description"])
        logger.info("=" * 60)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        spark = get_spark_session(app_name="ETL_Custom")
        spark.conf.set("spark.sql.debug.maxToStringFields", 500)  # Default is 25

        if USE_SAVED_SOURCE_CSV:
            # ── Pattern F/G: load source from previously saved CSV ───────
            # Skips steps 0 (source schema verify), 1 (extract), 2 (transform), 3 (save CSV)
            transformed_df = load_source_csv(spark, pipeline_ctx)
        else:
            # ── Pattern A: full live extraction ─────────────────────────
            # Step 0: optional source schema check
            if not step_0_verify_source_schema(spark, pipeline_ctx):
                logger.error("Exiting due to source schema verification failure")
                return 2

            # Step 1: extract source
            source_df = step_1_extract_source(spark, pipeline_ctx)

            # Step 2: transform
            t0 = time.time()
            transformed_df = step_2_transform(source_df, pipeline_ctx, TARGET_MODE)
            transform_time = time.time() - t0
            logger.info("Transform time: %.2fs", transform_time)

            # Step 3: save source CSV
            step_3_save_source_csv(transformed_df, source_df, pipeline_ctx)

        if USE_SAVED_TARGET_CSV:
            # ── Pattern G: load target from previously saved CSV ─────────
            # Skips steps 3.5 (target schema verify) and 4 (target extraction)
            target_df = load_target_csv(spark, pipeline_ctx)
        else:
            # Step 3.5: optional target schema check
            if not step_3_5_verify_target_schema(spark, pipeline_ctx):
                logger.error("Exiting due to target schema verification failure")
                return 2

            # Step 4: extract target
            target_df = step_4_extract_target(spark, pipeline_ctx)
            step_4_1_save_target_csv(target_df, pipeline_ctx)

        # Step 5: compare
        t0 = time.time()
        exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
        compare_time = time.time() - t0
        logger.info("Compare and Report time: %.2fs", compare_time)

        # Cleanup: unpersist cached DataFrames after comparison completes
        # (compare_dataframes may have already released these as an optimisation)
        logger.info("Ensuring cached DataFrames are released...")
        if transformed_df.is_cached:
            transformed_df.unpersist()
        if target_df.is_cached:
            target_df.unpersist()
        logger.info("Cache cleanup complete.")

        # spark.stop()

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


if __name__ == "__main__":
    sys.exit(main())
