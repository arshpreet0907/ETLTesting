"""
get_data.py
-----------
Unified data extraction module for both source and target databases.
Returns PySpark DataFrames for further processing.

Usage:
    from get_data import get_data
    
    df = get_data(
        spark=spark,
        db_type="source",
        query="SELECT * FROM orders"
    )
"""

from typing import Literal

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

    Returns
    -------
    DataFrame
        PySpark DataFrame with query results

    Raises
    ------
    ValueError
        If db_type is not "source" or "target"
    Exception
        If JDBC connection or query execution fails
    """
    if db_type not in ("source", "target"):
        raise ValueError(f"db_type must be 'source' or 'target', got: {db_type}")

    # Get JDBC connection options
    if db_type == "source":
        jdbc_opts = get_source_connection()
        logger.info("Extracting data from SOURCE database")
    else:
        jdbc_opts = get_target_connection(mode=target_mode)
        logger.info("Extracting data from TARGET database (mode=%s)", target_mode)

    logger.debug("Query (%d chars): %s", len(query), query[:200])

    try:
        df = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option("query", query)
            .load()
        )
        
        row_count = df.count()
        logger.info(
            "Extraction complete: %d rows, %d columns: %s",
            row_count,
            len(df.columns),
            df.columns
        )
        
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
