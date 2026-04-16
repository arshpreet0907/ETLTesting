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
import time

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

    start=time.time()
    diff_df, src_count, tgt_count, matched_count, total_diffs = compare_dataframes(
        source_df=source_df,
        target_df=target_df,
        primary_key_cols=primary_key_cols,
        compare_cols=compare_cols,
    )
    end=time.time()-start
    logger.info("Comparison completed in %.2fs", end)

    start=time.time()
    exit_code = generate_report(
        diff_df=diff_df,
        source_row_count=src_count,
        target_row_count=tgt_count,
        matched_row_count=matched_count,
        total_diff_count=total_diffs,
        output_path=output_path,
        exit_on_differences=False,
    )
    end=time.time()-start
    logger.info("Report generation completed in %.2fs", end)

    return exit_code

