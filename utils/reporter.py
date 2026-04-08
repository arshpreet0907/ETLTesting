"""
utils/reporter.py
------------------
Purpose : Accept a diff DataFrame and produce a human-readable comparison report.
          Separated from comparison logic so both can evolve independently and
          the reporter can be reused with any DataFrame that matches the schema.

Expected input schema
---------------------
The diff_df passed to `generate_report` must have these columns:
    primary_key_value : str   — primary key value(s) of the mismatched row
                                (composite PKs are joined with "|")
    column_name       : str   — name of the column that differs
    expected_value    : str   — value from source_enriched (transformed source)
    actual_value      : str   — value from target_actual

Usage
-----
    from utils.reporter import generate_report
    generate_report(
        diff_df=diff_dataframe,
        source_row_count=10_000,
        target_row_count=10_000,
        matched_row_count=9_985,
        output_path="output/orders/diff_report.csv",
    )
"""

import logging
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.csv_writer import save_dataframe_as_csv

logger = logging.getLogger(__name__)

# Exit code returned (via sys.exit) when differences are found
EXIT_CODE_DIFFERENCES = 3
EXIT_CODE_OK = 0

# Schema columns the diff_df must contain
REQUIRED_DIFF_COLUMNS = {
    "primary_key_value",
    "column_name",
    "expected_value",
    "actual_value",
}


def generate_report(
    diff_df: DataFrame,
    source_row_count: int,
    target_row_count: int,
    matched_row_count: int,
    output_path: str,
    exit_on_differences: bool = False,
) -> int:
    """
    Persist the diff DataFrame to CSV and print a summary to stdout.

    Parameters
    ----------
    diff_df : pyspark.sql.DataFrame
        Exploded differences; one row per (primary_key_value, column_name) pair.
        Must contain columns: primary_key_value, column_name, expected_value, actual_value.
        Pass an *empty* DataFrame (same schema) when there are no differences —
        a "PASS" report will be generated.
    source_row_count : int
        Total number of rows read from source_enriched.csv.
    target_row_count : int
        Total number of rows read from target_actual.csv.
    matched_row_count : int
        Number of rows that matched exactly (no column-level differences).
    output_path : str
        Full path for the output diff_report.csv file.
    exit_on_differences : bool
        If True, call sys.exit(EXIT_CODE_DIFFERENCES) when differences exist,
        or sys.exit(EXIT_CODE_OK) when there are none.
        Set to False (default) to simply return the exit code to the caller.

    Returns
    -------
    int
        EXIT_CODE_OK (0) if no differences, EXIT_CODE_DIFFERENCES (3) otherwise.

    Raises
    ------
    ValueError
        If diff_df is missing any required column.
    """
    _validate_diff_schema(diff_df)

    # ------------------------------------------------------------------ #
    # Persist the diff report CSV (even when empty — proves the run ran)  #
    # ------------------------------------------------------------------ #
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    save_dataframe_as_csv(diff_df, output_path)
    logger.info("Diff report written to: %s", output_path)

    # ------------------------------------------------------------------ #
    # Compute summary statistics from the diff DataFrame                  #
    # ------------------------------------------------------------------ #
    total_diff_rows: int = diff_df.count()
    mismatched_row_count = source_row_count - matched_row_count

    # Count differences per column
    col_counts: dict = {}
    if total_diff_rows > 0:
        col_count_rows = (
            diff_df.groupBy("column_name")
            .agg(F.count("*").alias("diff_count"))
            .orderBy(F.desc("diff_count"))
            .collect()
        )
        col_counts = {row["column_name"]: row["diff_count"] for row in col_count_rows}

    # ------------------------------------------------------------------ #
    # Print human-readable summary                                        #
    # ------------------------------------------------------------------ #
    _print_summary(
        source_row_count=source_row_count,
        target_row_count=target_row_count,
        matched_row_count=matched_row_count,
        mismatched_row_count=mismatched_row_count,
        col_counts=col_counts,
        output_path=output_path,
    )

    # ------------------------------------------------------------------ #
    # Determine exit code                                                 #
    # ------------------------------------------------------------------ #
    exit_code = EXIT_CODE_OK if total_diff_rows == 0 else EXIT_CODE_DIFFERENCES

    if exit_on_differences:
        sys.exit(exit_code)

    return exit_code


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _validate_diff_schema(diff_df: DataFrame) -> None:
    """Raise ValueError if the DataFrame is missing required columns."""
    actual_cols = set(diff_df.columns)
    missing = REQUIRED_DIFF_COLUMNS - actual_cols
    if missing:
        raise ValueError(
            f"diff_df is missing required column(s): {sorted(missing)}. "
            f"Actual columns: {sorted(actual_cols)}"
        )


def _print_summary(
    source_row_count: int,
    target_row_count: int,
    matched_row_count: int,
    mismatched_row_count: int,
    col_counts: dict,
    output_path: str,
) -> None:
    """Print the standardised comparison summary to stdout."""
    separator = "=" * 50

    print(separator)
    print("=== Comparison Summary ===")
    print(separator)
    print(f"  Total source rows  : {source_row_count:,}")
    print(f"  Total target rows  : {target_row_count:,}")
    print(f"  Matching rows      : {matched_row_count:,}")
    print(f"  Mismatched rows    : {mismatched_row_count:,}")

    if col_counts:
        col_summary = ", ".join(
            f"{col} ({cnt})" for col, cnt in col_counts.items()
        )
        print(f"  Columns with diffs : {col_summary}")
    else:
        print("  Columns with diffs : None")

    print(f"  Report saved to    : {output_path}")

    if mismatched_row_count == 0:
        print()
        print("  ✓ PASS — No differences found between source and target.")
    else:
        print()
        print("  ✗ FAIL — Differences detected. Review diff_report.csv.")

    print(separator)
