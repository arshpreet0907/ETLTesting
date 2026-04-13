"""
run_pipeline.py
----------------
Excel-based ETL validation pipeline (Scenario 1).

Configure the variables below and run:
    python run_pipeline.py
"""

import os
import sys
import time
from datetime import timedelta

from utils.connections.spark_session import get_spark_session
from utils.get_data import get_data
from utils.perform_transform import perform_transform
from utils.compare import compare_and_report
from utils.verify_schema import verify_schema_from_ddl
from utils.connections.source_connection import get_source_connection
from utils.connections.target_connection import get_target_connection
from utils.logger import get_logger
from utils.rulebook_loader import load_rulebook
from utils.csv_writer import save_dataframe_as_csv

logger = get_logger(__name__)


# ============================================================================
# CONFIGURATION — Edit these variables
# ============================================================================

# Table name (used to auto-detect all file paths)
# Example: "FACTORDERPGA", "orders", "cost_ledger"
TABLE_NAME = "FACTORDERPGA"

# Target database mode: "mysql" or "snowflake"
# Note: Both use same table/schema names, only connection and SQL dialect differ
TARGET_MODE = "mysql"  # Options: "mysql" or "snowflake"

# Schema verification (optional)
VERIFY_SCHEMA = True  # Set to True to enable schema verification

# Compare only mode (skip extraction, compare existing CSVs)
COMPARE_ONLY = False  # Set to True to skip extraction stages

# Output directory (None = auto-detect as output/{table_name})
OUTPUT_DIR = None

# ============================================================================
# AUTO-CONFIGURATION (Do not edit below this line)
# ============================================================================

# Auto-detect file paths based on TABLE_NAME
RULEBOOK_PATH = os.path.join("excel_files", f"{TABLE_NAME}_schema.json")
BASE_PATH = os.path.join("excel_files", "etl_output", TARGET_MODE, TABLE_NAME)

SOURCE_DDL = os.path.join(BASE_PATH, "01_create_source_table.sql")
SOURCE_QUERY_FILE = os.path.join(BASE_PATH, "03_extract_source.sql")
TRANSFORM_FILE = os.path.join(BASE_PATH, "04_transform.py")

# Select target query file based on TARGET_MODE
if TARGET_MODE.lower() == "snowflake":
    TARGET_DDL = os.path.join(BASE_PATH, "02_create_target_sf.sql")
    TARGET_QUERY_FILE = os.path.join(BASE_PATH, "05_extract_target_sf.sql")
else:
    TARGET_DDL = os.path.join(BASE_PATH, "02_create_target_ms.sql")
    TARGET_QUERY_FILE = os.path.join(BASE_PATH, "05_extract_target_ms.sql")

# ============================================================================


def main() -> int:
    """Run Excel-based ETL validation pipeline."""
    start_time = time.time()
    
    try:
        logger.info("=" * 60)
        logger.info("Excel-Based ETL Validation Pipeline")
        logger.info("=" * 60)
        logger.info("Table Name: %s", TABLE_NAME)
        logger.info("Target Mode: %s", TARGET_MODE)
        logger.info("Rulebook: %s", RULEBOOK_PATH)
        
        # Validate that files exist
        if not os.path.isfile(RULEBOOK_PATH):
            logger.error("Rulebook not found: %s", RULEBOOK_PATH)
            logger.error("Expected location: excel_files/{TABLE_NAME}_schema.json")
            return 1
        
        if not COMPARE_ONLY:
            for label, path in [
                ("Source DDL", SOURCE_DDL),
                ("Target DDL", TARGET_DDL),
                ("Source Query", SOURCE_QUERY_FILE),
                ("Target Query", TARGET_QUERY_FILE),
                ("Transform", TRANSFORM_FILE),
            ]:
                if not os.path.isfile(path):
                    logger.error("%s file not found: %s", label, path)
                    logger.error("Expected location: excel_files/etl_output/%s/%s/", TARGET_MODE, TABLE_NAME)
                    return 1
        
        # Load rulebook
        logger.info("Loading rulebook...")
        rulebook = load_rulebook(RULEBOOK_PATH)
        table_name = rulebook["meta"]["table_name"]
        logger.info("Processing table: %s", table_name)

        # Resolve output directory
        output_dir = OUTPUT_DIR or os.path.join("output", table_name)
        os.makedirs(output_dir, exist_ok=True)

        source_csv = os.path.join(output_dir, "source_enriched.csv")
        target_csv = os.path.join(output_dir, "target_actual.csv")
        report_csv = os.path.join(output_dir, "diff_report.csv")

        # Build Spark session
        spark = get_spark_session(app_name=f"ETL_Pipeline_{table_name}")

        if not COMPARE_ONLY:
            # Optional: Verify source schema
            if VERIFY_SCHEMA and SOURCE_DDL:
                logger.info("--- Verify Source Schema ---")
                source_db, source_table = _parse_table_name(rulebook["source"]["table"])
                jdbc_opts = get_source_connection()
                
                passed = verify_schema_from_ddl(
                    spark=spark,
                    jdbc_opts=jdbc_opts,
                    ddl_file=SOURCE_DDL,
                    database=source_db,
                    table=source_table,
                    dialect="mysql"
                )
                
                if not passed:
                    logger.error("Source schema verification FAILED")
                    return 2
                logger.info("Source schema verification PASSED")

            # Step 1: Extract from source
            logger.info("--- Step 1: Extract Source Data ---")
            source_query = _resolve_query(SOURCE_QUERY_FILE)
            source_df = get_data(
                spark=spark,
                db_type="source",
                query=source_query
            )

            # Step 2: Perform transformations
            logger.info("--- Step 2: Apply Transformations ---")
            transformed_df = perform_transform(
                df=source_df,
                transform_file=TRANSFORM_FILE,
                rulebook=rulebook
            )

            # Step 3: Save transformed CSV
            logger.info("--- Step 3: Save Transformed CSV ---")
            save_dataframe_as_csv(transformed_df, source_csv)
            logger.info("Source data saved → %s", source_csv)

            # Optional: Verify target schema
            if VERIFY_SCHEMA and TARGET_DDL:
                logger.info("--- Verify Target Schema ---")
                target_db, target_table = _parse_table_name(rulebook["target"]["table"])
                jdbc_opts = get_target_connection(mode=TARGET_MODE)
                
                passed = verify_schema_from_ddl(
                    spark=spark,
                    jdbc_opts=jdbc_opts,
                    ddl_file=TARGET_DDL,
                    database=target_db,
                    table=target_table,
                    dialect=TARGET_MODE
                )
                
                if not passed:
                    logger.error("Target schema verification FAILED")
                    return 2
                logger.info("Target schema verification PASSED")

            # Step 4: Extract from target
            logger.info("--- Step 4: Extract Target Data ---")
            target_query = _resolve_query(TARGET_QUERY_FILE)
            target_df = get_data(
                spark=spark,
                db_type="target",
                query=target_query,
                target_mode=TARGET_MODE
            )
            
            save_dataframe_as_csv(target_df, target_csv)
            logger.info("Target data saved → %s", target_csv)

        # Step 5: Compare and report
        logger.info("--- Step 5: Compare & Generate Report ---")
        
        # Load CSVs if compare-only
        if COMPARE_ONLY:
            transformed_df = spark.read.option("header", True).option("inferSchema", False).csv(source_csv)
            target_df = spark.read.option("header", True).option("inferSchema", False).csv(target_csv)

        pk_cols = rulebook["primary_key"]
        compare_cols = [
            c["target_col"]
            for c in rulebook["columns"]
            if c["target_col"] not in pk_cols
        ]

        exit_code = compare_and_report(
            spark=spark,
            source_df=transformed_df,
            target_df=target_df,
            primary_key_cols=pk_cols,
            compare_cols=compare_cols,
            output_path=report_csv
        )

        spark.stop()
        
        # Calculate and display execution time
        end_time = time.time()
        elapsed = timedelta(seconds=int(end_time - start_time))
        minutes = elapsed.seconds // 60
        seconds = elapsed.seconds % 60
        
        logger.info("=" * 60)
        logger.info("Pipeline complete. Exit code: %d", exit_code)
        logger.info("Total execution time: %d minutes %d seconds", minutes, seconds)
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


def _resolve_query(query_file: str) -> str:
    """Return SQL string from file."""
    if not os.path.isfile(query_file):
        raise FileNotFoundError(f"Query file not found: {query_file}")
    with open(query_file, "r", encoding="utf-8") as fh:
        content = fh.read().strip()
        # Remove trailing semicolon if present
        if content.endswith(";"):
            content = content[:-1].strip()
        return content


def _parse_table_name(full_table: str) -> tuple:
    """Split 'database.table' into (database, table)."""
    parts = full_table.split(".")
    if len(parts) != 2:
        raise ValueError(f"Expected 'database.table' format, got: {full_table}")
    return parts[0], parts[1]


if __name__ == "__main__":
    sys.exit(main())
