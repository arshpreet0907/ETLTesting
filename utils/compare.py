"""
compare.py
-----------
Modular comparison module that compares two DataFrames and generates a report.

Can be used as:
  1. Standalone script with CLI
  2. Imported function: compare_and_report()

Usage as module:
    from compare import compare_and_report
    
    exit_code = compare_and_report(
        spark=spark,
        source_df=source_df,
        target_df=target_df,
        primary_key_cols=["order_id"],
        compare_cols=["status", "price"],
        output_path="output/diff_report.csv"
    )
"""

import argparse
import os
import sys
from pyspark.sql import SparkSession, DataFrame

# ---------------------------------------------------------------------------
# Bootstrap: make project root importable regardless of working directory
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.logger import get_logger
from utils.rulebook_loader import load_rulebook
from utils.connections.spark_session import get_spark_session
from utils.comparator import compare_dataframes
from utils.reporter import generate_report

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Modular function for programmatic use                                       #
# --------------------------------------------------------------------------- #

def compare_and_report(
    spark: SparkSession,
    source_df: DataFrame,
    target_df: DataFrame,
    primary_key_cols: list,
    compare_cols: list,
    output_path: str,
) -> int:
    """
    Compare two DataFrames and generate a diff report.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    source_df : DataFrame
        Expected data (transformed source)
    target_df : DataFrame
        Actual data (target)
    primary_key_cols : list of str
        Primary key column names
    compare_cols : list of str
        Non-PK columns to compare
    output_path : str
        Path for diff_report.csv

    Returns
    -------
    int
        Exit code: 0 (pass), 3 (differences found)
    """
    logger.info("Starting comparison on PK=%s", primary_key_cols)

    diff_df, src_count, tgt_count, matched_count, total_diffs = compare_dataframes(
        source_df=source_df,
        target_df=target_df,
        primary_key_cols=primary_key_cols,
        compare_cols=compare_cols,
    )

    exit_code = generate_report(
        diff_df=diff_df,
        source_row_count=src_count,
        target_row_count=tgt_count,
        matched_row_count=matched_count,
        total_diff_count=total_diffs,
        output_path=output_path,
        exit_on_differences=False,
    )

    return exit_code


# --------------------------------------------------------------------------- #
# CLI function (backward compatibility)                                        #
# --------------------------------------------------------------------------- #

def run(
    rulebook_path: str,
    source_csv: str,
    target_csv: str,
    output_dir: str = None,
) -> None:
    """
    Full comparison pipeline:
      1. Load rulebook to get PK columns and compare-column list
      2. Build SparkSession
      3. Load both CSVs as string DataFrames
      4. Delegate comparison to utils.comparator.compare_dataframes
      5. Delegate reporting to utils.reporter.generate_report
    """
    # ------------------------------------------------------------------ #
    # 1. Validate inputs                                                  #
    # ------------------------------------------------------------------ #
    for label, path in [("source CSV", source_csv), ("target CSV", target_csv)]:
        if not os.path.isfile(os.path.abspath(path)):
            logger.error("%s not found: %s", label, path)
            sys.exit(1)

    # ------------------------------------------------------------------ #
    # 2. Load rulebook                                                    #
    # ------------------------------------------------------------------ #
    logger.info("Loading rulebook: %s", rulebook_path)
    rulebook = load_rulebook(rulebook_path)
    table_name: str = rulebook["meta"]["table_name"]

    pk_cols = rulebook["primary_key"]
    compare_cols = [
        c["target_col"]
        for c in rulebook["columns"]
        if c["target_col"] not in pk_cols
    ]

    logger.info(
        "Primary key: %s | Columns to compare: %d", pk_cols, len(compare_cols)
    )

    # ------------------------------------------------------------------ #
    # 3. Resolve output path                                              #
    # ------------------------------------------------------------------ #
    if not output_dir:
        output_dir = os.path.join(_PROJECT_ROOT, "../output", table_name)
    report_path = os.path.join(output_dir, "diff_report.csv")

    # ------------------------------------------------------------------ #
    # 4. Build Spark                                                      #
    # ------------------------------------------------------------------ #
    spark = get_spark_session(app_name=f"ETL_Compare_{table_name}")

    # ------------------------------------------------------------------ #
    # 5. Load CSVs — always inferSchema=False; treat everything as string #
    # ------------------------------------------------------------------ #
    logger.info("Loading source CSV: %s", source_csv)
    source_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(source_csv)
    )

    logger.info("Loading target CSV: %s", target_csv)
    target_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(target_csv)
    )

    # ------------------------------------------------------------------ #
    # 6. Delegate comparison                                              #
    # ------------------------------------------------------------------ #
    logger.info("Starting row-by-row comparison...")
    diff_df, src_count, tgt_count, matched_count, total_diffs = compare_dataframes(
        source_df=source_df,
        target_df=target_df,
        primary_key_cols=pk_cols,
        compare_cols=compare_cols,
    )

    # ------------------------------------------------------------------ #
    # 7. Delegate reporting — exits with code 0 or 3                     #
    # ------------------------------------------------------------------ #
    generate_report(
        diff_df=diff_df,
        source_row_count=src_count,
        target_row_count=tgt_count,
        matched_row_count=matched_count,
        total_diff_count=total_diffs,
        output_path=report_path,
        exit_on_differences=True,  # sys.exit(3) on FAIL, sys.exit(0) on PASS
    )

    spark.stop()


# --------------------------------------------------------------------------- #
# CLI entry point                                                              #
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare source_enriched.csv (expected) against target_actual.csv (actual) "
            "and produce a diff report."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--rulebook",
        required=True,
        help="Path to JSON rulebook, e.g. rulebook/orders_rulebook.json",
    )
    parser.add_argument(
        "--source-csv",
        dest="source_csv",
        required=True,
        help="Path to source_enriched.csv from get_source_data.py",
    )
    parser.add_argument(
        "--target-csv",
        dest="target_csv",
        required=True,
        help="Path to target_actual.csv from get_target_data.py",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Directory for diff_report.csv. Defaults to output/<table_name>/.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run(
            rulebook_path=args.rulebook,
            source_csv=args.source_csv,
            target_csv=args.target_csv,
            output_dir=args.output_dir,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(99)
