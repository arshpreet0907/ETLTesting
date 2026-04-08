"""
utils/compare.py
--------------------
Purpose : Accept two PySpark DataFrames (expected/source and actual/target),
          join them on primary key columns, and produce an exploded diff DataFrame
          where each row represents a single cell-level difference.

The output DataFrame is schema-compatible with utils/reporter.py — pass it
directly to generate_report() without any transformation.

Output schema
-------------
    primary_key_value : str   — PK value(s); composite PKs joined with "|"
    column_name       : str   — column name that differs
    expected_value    : str   — value from `source_df` (transformed source)
    actual_value      : str   — value from `target_df`

Null handling (per spec)
------------------------
    null  == null        → MATCH   (both sides absent — not a difference)
    null  == "null"      → MISMATCH (transform wrote literal string "null")
    ""    == null        → MISMATCH (empty string is not null)

Normalisation before compare
-----------------------------
    - Strip leading/trailing whitespace from all string values
    - Lowercase boolean literals ("True", "TRUE", "False" → "true"/"false")

Usage
-----
    from utils.comparator import compare_dataframes

    diff_df, source_count, target_count, matched_count = compare_dataframes(
        source_df=source_dataframe,
        target_df=target_dataframe,
        primary_key_cols=["ORDER_ID"],
        compare_cols=["ORDER_STATUS", "IS_ACTIVE_FLAG", "UNIT_PRICE_USD"],
    )
"""

import logging
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

# Prefixes used to disambiguate source / target column names after the join
_SRC_PREFIX = "src_"
_TGT_PREFIX = "tgt_"


def compare_dataframes(
    source_df: DataFrame,
    target_df: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int]:
    """
    Compare two DataFrames column-by-column after joining on primary key(s).

    Parameters
    ----------
    source_df : pyspark.sql.DataFrame
        The *expected* data — typically source_enriched loaded from CSV.
        All columns are treated as strings (inferSchema must be False at read time).
    target_df : pyspark.sql.DataFrame
        The *actual* data — typically target_actual loaded from CSV.
    primary_key_cols : list of str
        Column name(s) used as the join key.  Must exist in both DataFrames.
        Column names are matched case-insensitively.
    compare_cols : list of str
        Non-PK columns to compare.  Must exist in both DataFrames.

    Returns
    -------
    diff_df : pyspark.sql.DataFrame
        Exploded diff; schema: primary_key_value, column_name,
        expected_value, actual_value.
        Empty DataFrame (same schema) when there are no differences.
    source_row_count : int
    target_row_count : int
    matched_row_count : int
    """
    # ------------------------------------------------------------------ #
    # Counts before any transformations                                   #
    # ------------------------------------------------------------------ #
    source_row_count: int = source_df.count()
    target_row_count: int = target_df.count()
    logger.info(
        "Comparing %d source rows against %d target rows on PK=%s",
        source_row_count,
        target_row_count,
        primary_key_cols,
    )

    # ------------------------------------------------------------------ #
    # Normalise both DataFrames                                           #
    # ------------------------------------------------------------------ #
    all_cols = primary_key_cols + compare_cols
    source_norm = _normalise_df(source_df, all_cols)
    target_norm = _normalise_df(target_df, all_cols)

    # ------------------------------------------------------------------ #
    # Rename to src_ / tgt_ prefixes to avoid column name collisions     #
    # ------------------------------------------------------------------ #
    for col in all_cols:
        source_norm = source_norm.withColumnRenamed(col, f"{_SRC_PREFIX}{col}")
        target_norm = target_norm.withColumnRenamed(col, f"{_TGT_PREFIX}{col}")

    # ------------------------------------------------------------------ #
    # Join on primary key columns (null-safe, cast to string on both)     #
    # ------------------------------------------------------------------ #
    join_condition = _build_join_condition(primary_key_cols)
    joined = source_norm.join(target_norm, on=join_condition, how="full")

    # ------------------------------------------------------------------ #
    # Add mismatch flag columns for each non-PK column                   #
    # ------------------------------------------------------------------ #
    mismatch_cols = []
    for col in compare_cols:
        src_col = f"{_SRC_PREFIX}{col}"
        tgt_col = f"{_TGT_PREFIX}{col}"
        flag_col = f"mismatch_{col}"
        # eqNullSafe returns True when both are null → that is a MATCH
        # We want True when they are NOT equal (including null vs non-null)
        joined = joined.withColumn(
            flag_col,
            ~F.col(src_col).eqNullSafe(F.col(tgt_col)),
        )
        mismatch_cols.append(flag_col)

    # ------------------------------------------------------------------ #
    # Filter to rows that have at least one mismatch                     #
    # ------------------------------------------------------------------ #
    any_mismatch = F.lit(False)
    for flag in mismatch_cols:
        any_mismatch = any_mismatch | F.col(flag)

    mismatched_rows = joined.filter(any_mismatch)

    matched_row_count: int = source_row_count - mismatched_rows.count()
    logger.info(
        "Matched rows: %d | Mismatched rows: %d",
        matched_row_count,
        source_row_count - matched_row_count,
    )

    # ------------------------------------------------------------------ #
    # Explode to one row per (PK, column_name) difference                #
    # ------------------------------------------------------------------ #
    diff_df = _explode_differences(
        mismatched_rows=mismatched_rows,
        primary_key_cols=primary_key_cols,
        compare_cols=compare_cols,
        mismatch_cols=mismatch_cols,
    )

    return diff_df, source_row_count, target_row_count, matched_row_count


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _normalise_df(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Apply pre-comparison normalisation to selected columns:
      - Cast to string
      - Strip surrounding whitespace
      - Lowercase common boolean literals
    """
    for col in cols:
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNull(), F.lit(None).cast(StringType())
            ).otherwise(
                # Lowercase booleans, then trim whitespace
                F.trim(
                    F.regexp_replace(
                        F.col(col).cast(StringType()),
                        r"(?i)^(true|false)$",
                        F.lower(F.col(col).cast(StringType())),
                    )
                )
            ),
        )
    return df


def _build_join_condition(primary_key_cols: List[str]):
    """Build a null-safe join condition across all PK columns."""
    condition = None
    for col in primary_key_cols:
        src = F.col(f"{_SRC_PREFIX}{col}").cast(StringType())
        tgt = F.col(f"{_TGT_PREFIX}{col}").cast(StringType())
        clause = src.eqNullSafe(tgt)
        condition = clause if condition is None else condition & clause
    return condition


def _explode_differences(
    mismatched_rows: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
    mismatch_cols: List[str],
) -> DataFrame:
    """
    Convert wide-format mismatch rows into one record per differing cell.

    Returns a DataFrame with schema:
        primary_key_value | column_name | expected_value | actual_value
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType as ST

    spark = SparkSession.getActiveSession()

    schema = StructType([
        StructField("primary_key_value", ST(), True),
        StructField("column_name", ST(), True),
        StructField("expected_value", ST(), True),
        StructField("actual_value", ST(), True),
    ])

    if mismatched_rows.rdd.isEmpty():
        return spark.createDataFrame([], schema)

    # Build a list of struct expressions: one per compare_col
    # Each struct is (pk_value, col_name, expected, actual, is_mismatch)
    struct_exprs = []
    pk_concat = F.concat_ws("|", *[F.col(f"{_SRC_PREFIX}{k}") for k in primary_key_cols])

    for col, flag in zip(compare_cols, mismatch_cols):
        struct_exprs.append(
            F.struct(
                pk_concat.alias("primary_key_value"),
                F.lit(col).alias("column_name"),
                F.col(f"{_SRC_PREFIX}{col}").alias("expected_value"),
                F.col(f"{_TGT_PREFIX}{col}").alias("actual_value"),
                F.col(flag).alias("is_mismatch"),
            )
        )

    # Combine all structs into one array column, then explode
    all_diffs = (
        mismatched_rows
        .withColumn("_diffs", F.array(*struct_exprs))
        .withColumn("_diff", F.explode(F.col("_diffs")))
        .filter(F.col("_diff.is_mismatch"))
        .select(
            F.col("_diff.primary_key_value"),
            F.col("_diff.column_name"),
            F.col("_diff.expected_value"),
            F.col("_diff.actual_value"),
        )
    )

    return all_diffs
