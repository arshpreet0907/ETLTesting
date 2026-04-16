"""
get_data.py
-----------
Unified data extraction module for both source and target databases.
Returns PySpark DataFrames for further processing.

Two read paths
--------------
1. Single-partition (default):
   Uses spark.read ... .option("query", sql).load()
   One JDBC connection, one partition, sequential fetch.

2. Partitioned read (MySQL only):
   Uses spark.read ... .option("dbtable", subquery) with numPartitions,
   partitionColumn, lowerBound, upperBound.
   Spawns N parallel JDBC connections — one per partition — and reads
   the table in N concurrent slices.

   IMPORTANT: Do NOT use partitioning for Snowflake targets — Snowflake
   already parallelises query execution server-side. Partitioned JDBC
   would run the same query N times, wasting compute credits.

Usage
-----
    # Single partition (default):
    df, t = get_data(spark, "source", "SELECT * FROM orders")

    # Partitioned (MySQL only):
    df, t = get_data(
        spark, "source", "SELECT * FROM orders",
        partition_col="order_id",
        lower_bound=1,
        upper_bound=1_000_000,
        num_partitions=4,
    )
"""
import time
from typing import Literal, Optional

from pyspark.sql import DataFrame, SparkSession

from utils.connections.source_connection import get_source_connection
from utils.connections.target_connection import get_target_connection
from utils.logger import get_logger

logger = get_logger(__name__)


def get_data(
    spark: SparkSession,
    db_type: Literal["source", "target"],
    query: str,
    target_mode: str = "mysql",
    partition_col: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """
    Extract data from source or target database using SQL query.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    db_type : str
        "source" or "target"
    query : str
        SQL SELECT query to execute
    target_mode : str, optional
        "mysql" or "snowflake" (only used when db_type="target")
    partition_col : str, optional
        Numeric column to partition on (e.g. "order_id", "ledger_id").
        Must be a numeric type in MySQL for JDBC range partitioning to work.
    lower_bound : int, optional
        Minimum value of partition_col for stride calculation.
        Rows below this value are NOT excluded; they land in the first partition.
    upper_bound : int, optional
        Maximum value of partition_col for stride calculation.
        Rows above this value land in the last partition.
    num_partitions : int, optional
        Number of parallel JDBC connections / Spark partitions to create.
        Recommended: 4 for local[*] mode, cores × executors for distributed.

    Returns
    -------
    tuple[DataFrame, float]
        (DataFrame, extract_cache_time)
        extract_cache_time = JDBC read + cache materialisation time.
        Row count time is logged separately (reads from cache, near-instant).

    Raises
    ------
    ValueError
        If db_type is invalid, or if only some partition parameters are set.
    Exception
        If JDBC connection or query execution fails.
    """
    if db_type not in ("source", "target"):
        raise ValueError(f"db_type must be 'source' or 'target', got: {db_type}")

    # Validate that partition params are all-or-nothing
    partition_params = [partition_col, lower_bound, upper_bound, num_partitions]
    provided = [p for p in partition_params if p is not None]
    if 0 < len(provided) < 4:
        raise ValueError(
            "Partitioned read requires ALL FOUR parameters: "
            "partition_col, lower_bound, upper_bound, num_partitions. "
            f"Only {len(provided)} of 4 were provided."
        )
    use_partitioning = len(provided) == 4

    # Get JDBC connection options
    if db_type == "source":
        jdbc_opts = get_source_connection()
        logger.info("Extracting data from SOURCE database")
    else:
        jdbc_opts = get_target_connection(mode=target_mode)
        logger.info("Extracting data from TARGET database (mode=%s)", target_mode)

    logger.debug("Query (%d chars): %s", len(query), query[:200])

    try:
        # Build JDBC DataFrame (lazy - no execution yet)
        if use_partitioning:
            df = _read_partitioned(spark, jdbc_opts, query,
                                   partition_col, lower_bound,
                                   upper_bound, num_partitions)
        else:
            df = _read_single(spark, jdbc_opts, query)

        # Mark for caching (lazy - no execution yet)
        df.cache()
        logger.info("DataFrame marked for caching")

        # Phase 1: Materialise JDBC read + cache write
        # foreach touches every row/partition, forcing full execution,
        # but avoids the overhead of returning a result to the driver.
        start_time = time.time()
        df.foreach(lambda _: None)
        extract_cache_time = time.time() - start_time

        # Phase 2: Row count — reads from cache, near-instant
        count_start = time.time()
        row_count = df.count()
        count_time = time.time() - count_start

        logger.info(
            "JDBC + cache: %.2fs | Row count: %d in %.2fs (from cache)",
            extract_cache_time,
            row_count,
            count_time,
        )
        logger.info(
            "Extraction complete: %d rows, %d columns",
            row_count, len(df.columns),
        )
        logger.info("Columns: %s", df.columns)

        return df

    except Exception as exc:
        safe_url = jdbc_opts.get("url", "unknown")
        user = jdbc_opts.get("user", "unknown")
        logger.error(
            "JDBC extraction failed: %s\n  URL: %s\n  User: %s\n"
            "  Check connectivity, credentials, and JAR availability.",
            exc,
            safe_url,
            user,
        )
        raise


# --------------------------------------------------------------------------- #
# Private read helpers                                                         #
# --------------------------------------------------------------------------- #

def _read_single(spark: SparkSession, jdbc_opts: dict, query: str) -> DataFrame:
    """
    Standard single-partition JDBC read using the 'query' option.
    One connection, sequential fetch — default behaviour.
    """
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", query)
        .load()
    )


def _read_partitioned(
    spark: SparkSession,
    jdbc_opts: dict,
    query: str,
    partition_col: str,
    lower_bound: int,
    upper_bound: int,
    num_partitions: int,
) -> DataFrame:
    """
    Multi-threaded JDBC read using Spark partition parameters.

    Spark's partitioned JDBC requires the ``dbtable`` option, not ``query``.
    Wrapping the original SQL as a subquery satisfies this requirement while
    preserving any WHERE clauses, JOINs, and column selections.

    Spark divides [lower_bound, upper_bound] into num_partitions equal strides
    and issues one SQL query per stride, each on its own JDBC connection.
    """
    logger.info(
        "Partitioned JDBC read: col=%s  bounds=[%d, %d]  partitions=%d",
        partition_col, lower_bound, upper_bound, num_partitions,
    )

    # Wrap original query as a subquery — required for dbtable partitioning
    dbtable_expr = f"({query}) AS _jdbc_subquery"

    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", dbtable_expr)
        .option("partitionColumn", partition_col)
        .option("lowerBound", str(lower_bound))
        .option("upperBound", str(upper_bound))
        .option("numPartitions", str(num_partitions))
        .load()
    )

