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
    diff_type         : str   — MISSING_IN_TARGET | EXTRA_IN_TARGET | VALUE_MISMATCH

Difference types
----------------
    MISSING_IN_TARGET — Record exists in source but not in target
                        (expected_value has data, actual_value is '<MISSING>')
    EXTRA_IN_TARGET   — Record exists in target but not in source
                        (expected_value is '<EXTRA>', actual_value has data)
    VALUE_MISMATCH    — Record exists in both but values differ
                        (both expected_value and actual_value have data)

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
    Handles three scenarios:
    1. Records in source but NOT in target (missing in target)
    2. Records in target but NOT in source (extra in target)
    3. Records in both with value differences

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
        expected_value, actual_value, diff_type.
        Empty DataFrame (same schema) when there are no differences.
    source_row_count : int
    target_row_count : int
    matched_row_count : int
    total_diff_count : int
        Total number of differences (missing + extra + value mismatches)
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
    # Normalize column names to lowercase for case-insensitive comparison #
    # ------------------------------------------------------------------ #
    logger.info("Normalizing column names to lowercase")
    for col in source_df.columns:
        source_df = source_df.withColumnRenamed(col, col.lower())
    for col in target_df.columns:
        target_df = target_df.withColumnRenamed(col, col.lower())
    
    # Update column lists to lowercase
    primary_key_cols = [pk.lower() for pk in primary_key_cols]
    compare_cols = [col.lower() for col in compare_cols]

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
    # Cache to avoid recomputation for multiple filters                   #
    # ------------------------------------------------------------------ #
    join_condition = _build_join_condition(primary_key_cols)
    joined = source_norm.join(target_norm, on=join_condition, how="full").cache()

    # ------------------------------------------------------------------ #
    # Identify record presence: source-only, target-only, or both        #
    # ------------------------------------------------------------------ #
    # Check if first PK column is null to determine presence
    first_pk = primary_key_cols[0]
    src_pk_col = f"{_SRC_PREFIX}{first_pk}"
    tgt_pk_col = f"{_TGT_PREFIX}{first_pk}"
    
    joined = joined.withColumn(
        "_in_source", F.col(src_pk_col).isNotNull()
    ).withColumn(
        "_in_target", F.col(tgt_pk_col).isNotNull()
    )

    # Separate into three categories
    missing_in_target = joined.filter(F.col("_in_source") & ~F.col("_in_target"))
    extra_in_target = joined.filter(~F.col("_in_source") & F.col("_in_target"))
    in_both = joined.filter(F.col("_in_source") & F.col("_in_target"))

    missing_count = missing_in_target.count()
    extra_count = extra_in_target.count()
    in_both_count = in_both.count()

    logger.info(
        "Records: %d in source only | %d in target only | %d in both",
        missing_count, extra_count, in_both_count
    )

    # ------------------------------------------------------------------ #
    # For records in both: check for value mismatches                    #
    # ------------------------------------------------------------------ #
    mismatch_cols = []
    for col in compare_cols:
        src_col = f"{_SRC_PREFIX}{col}"
        tgt_col = f"{_TGT_PREFIX}{col}"
        flag_col = f"mismatch_{col}"
        in_both = in_both.withColumn(
            flag_col,
            ~F.col(src_col).eqNullSafe(F.col(tgt_col)),
        )
        mismatch_cols.append(flag_col)

    any_mismatch = F.lit(False)
    for flag in mismatch_cols:
        any_mismatch = any_mismatch | F.col(flag)

    value_mismatches = in_both.filter(any_mismatch)
    value_mismatch_count = value_mismatches.count()
    matched_row_count = in_both_count - value_mismatch_count

    logger.info(
        "Matched rows: %d | Value mismatches: %d",
        matched_row_count, value_mismatch_count
    )

    # ------------------------------------------------------------------ #
    # Short-circuit if no differences found                              #
    # ------------------------------------------------------------------ #
    if missing_count == 0 and extra_count == 0 and value_mismatch_count == 0:
        logger.info("No differences found - skipping diff DataFrame creation")
        # Unpersist cache before returning
        joined.unpersist()
        
        # Return empty diff DataFrame with correct schema
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType as ST
        spark = SparkSession.getActiveSession()
        schema = StructType([
            StructField("primary_key_value", ST(), True),
            StructField("column_name", ST(), True),
            StructField("expected_value", ST(), True),
            StructField("actual_value", ST(), True),
            StructField("diff_type", ST(), True),
        ])
        empty_diff = spark.createDataFrame([], schema)
        return empty_diff, source_row_count, target_row_count, matched_row_count, 0

    # ------------------------------------------------------------------ #
    # Explode all three types of differences                             #
    # ------------------------------------------------------------------ #
    logger.info("Creating diff report for %d missing, %d extra, %d value mismatches",
                missing_count, extra_count, value_mismatch_count)
    
    diff_missing = _explode_missing_records(
        missing_in_target, primary_key_cols, compare_cols, "MISSING_IN_TARGET"
    )
    diff_extra = _explode_missing_records(
        extra_in_target, primary_key_cols, compare_cols, "EXTRA_IN_TARGET"
    )
    diff_values = _explode_differences(
        mismatched_rows=value_mismatches,
        primary_key_cols=primary_key_cols,
        compare_cols=compare_cols,
        mismatch_cols=mismatch_cols,
    )
    
    logger.info("Combining all difference types into single report")

    # Combine all differences
    diff_df = diff_missing.union(diff_extra).union(diff_values)
    
    # Calculate total diff count (for missing/extra: 1 per row, for value mismatches: actual count)
    total_diff_count = missing_count + extra_count + value_mismatch_count
    
    logger.info("Total differences to report: %d", total_diff_count)
    
    # Unpersist the cached joined DataFrame
    joined.unpersist()

    return diff_df, source_row_count, target_row_count, matched_row_count, total_diff_count


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _normalise_df(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Apply pre-comparison normalisation to selected columns:
      - Cast to string and trim whitespace
      - Normalize numeric columns based on schema type (5.2 == 5.20)
      - Lowercase boolean literals (true/false)
      
    Numeric normalization is only applied to columns with numeric schema types
    (IntegerType, LongType, FloatType, DoubleType, DecimalType) to avoid
    issues with alphanumeric columns.
    """
    from pyspark.sql.types import (
        IntegerType, LongType, FloatType, DoubleType, 
        DecimalType, ShortType, ByteType
    )
    
    # Identify numeric columns by schema type
    numeric_types = (IntegerType, LongType, FloatType, DoubleType, 
                     DecimalType, ShortType, ByteType)
    numeric_cols = []
    
    for field in df.schema.fields:
        if field.name in cols and isinstance(field.dataType, numeric_types):
            numeric_cols.append(field.name)
    
    logger.info("Detected %d numeric columns for normalization: %s", 
                len(numeric_cols), numeric_cols[:5] if len(numeric_cols) > 5 else numeric_cols)
    
    # Normalize all columns
    for col in cols:
        if col in numeric_cols:
            # Numeric column: cast to double for normalization, then to string
            df = df.withColumn(
                col,
                F.when(
                    F.col(col).isNull(), F.lit(None).cast(StringType())
                ).otherwise(
                    F.col(col).cast("double").cast(StringType())
                ),
            )
        else:
            # Non-numeric column: trim and normalize booleans
            df = df.withColumn(
                col,
                F.when(
                    F.col(col).isNull(), F.lit(None).cast(StringType())
                ).otherwise(
                    F.regexp_replace(
                        F.trim(F.col(col).cast(StringType())),
                        r"(?i)^(true|false)$",
                        F.lower(F.col(col).cast(StringType())),
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


def _explode_missing_records(
    df: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
    diff_type: str,
) -> DataFrame:
    """
    Convert missing/extra records into diff format.
    Creates ONE row per missing/extra record (not one per column).
    
    For missing in target: expected_value = '<ALL_COLUMNS>', actual_value = '<MISSING>'
    For extra in target: expected_value = '<EXTRA>', actual_value = '<ALL_COLUMNS>'
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType as ST

    spark = SparkSession.getActiveSession()
    schema = StructType([
        StructField("primary_key_value", ST(), True),
        StructField("column_name", ST(), True),
        StructField("expected_value", ST(), True),
        StructField("actual_value", ST(), True),
        StructField("diff_type", ST(), True),
    ])

    if df.rdd.isEmpty():
        return spark.createDataFrame([], schema)

    # Determine which prefix to use based on diff_type
    prefix = _SRC_PREFIX if diff_type == "MISSING_IN_TARGET" else _TGT_PREFIX
    pk_concat = F.concat_ws("|", *[F.col(f"{prefix}{k}") for k in primary_key_cols])

    # Create single row per missing/extra record instead of one per column
    if diff_type == "MISSING_IN_TARGET":
        result = df.select(
            pk_concat.alias("primary_key_value"),
            F.lit("<ENTIRE_ROW>").alias("column_name"),
            F.lit("<PRESENT_IN_SOURCE>").alias("expected_value"),
            F.lit("<MISSING_IN_TARGET>").alias("actual_value"),
            F.lit(diff_type).alias("diff_type"),
        )
    else:  # EXTRA_IN_TARGET
        result = df.select(
            pk_concat.alias("primary_key_value"),
            F.lit("<ENTIRE_ROW>").alias("column_name"),
            F.lit("<EXTRA_IN_TARGET>").alias("expected_value"),
            F.lit("<PRESENT_IN_TARGET>").alias("actual_value"),
            F.lit(diff_type).alias("diff_type"),
        )

    return result


def _explode_differences(
    mismatched_rows: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
    mismatch_cols: List[str],
) -> DataFrame:
    """
    Convert wide-format mismatch rows into one record per differing cell.

    Returns a DataFrame with schema:
        primary_key_value | column_name | expected_value | actual_value | diff_type
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType as ST

    spark = SparkSession.getActiveSession()

    schema = StructType([
        StructField("primary_key_value", ST(), True),
        StructField("column_name", ST(), True),
        StructField("expected_value", ST(), True),
        StructField("actual_value", ST(), True),
        StructField("diff_type", ST(), True),
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
                F.lit("VALUE_MISMATCH").alias("diff_type"),
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
            F.col("_diff.diff_type"),
        )
    )

    return all_diffs
