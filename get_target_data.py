"""
get_target_data.py
-------------------
Purpose : Extract data from the target database (MySQL or Snowflake) using a
          provided SQL query and save it to target_actual.csv.

          This is Stage 5 of the ETL validation pipeline.

Inputs  :
    --rulebook       Path to the JSON rulebook (e.g. rulebook/orders_rulebook.json)
    --query          Inline SQL SELECT string for the target table
    --query-file     Path to a .sql file containing the target SELECT query
    --target-mode    "mysql" or "snowflake" (default: reads from target_config.yaml 'type' key)
    --output-dir     Directory where target_actual.csv will be written
                     (default: output/<table_name>/)
    --verify-schema  Run DDL vs live-DB schema check before extract
    --no-verify-schema  Skip schema verification (default)

Outputs :
    output/<table_name>/target_actual.csv

Exit codes:
    0  — success
    1  — config / argument error
    2  — schema verification failed
    99 — unexpected runtime error

Usage   :
    python get_target_data.py \\
        --rulebook rulebook/orders_rulebook.json \\
        --query-file generated/orders/query_target.sql \\
        --target-mode mysql \\
        --output-dir output/orders

    python get_target_data.py \\
        --rulebook rulebook/orders_rulebook.json \\
        --query "SELECT ORDER_ID, ORDER_STATUS FROM dw.fact_orders" \\
        --target-mode snowflake
"""

import argparse
import logging
import os
import sys
from typing import Optional

# ---------------------------------------------------------------------------
# Bootstrap: make project root importable regardless of working directory
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.logger import get_logger
from utils.rulebook_loader import load_rulebook
from connections.spark_session import get_spark_session
from connections.target_connection import get_target_connection
from utils.csv_writer import save_dataframe_as_csv

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main pipeline function                                                       #
# --------------------------------------------------------------------------- #

def run(
    rulebook_path: str,
    query: Optional[str],
    query_file: Optional[str],
    target_mode: Optional[str],
    output_dir: Optional[str],
    verify_schema: bool,
) -> None:
    """
    Full target-extraction pipeline:
      1. Load rulebook
      2. Resolve target mode (mysql / snowflake)
      3. Build SparkSession
      4. Get target JDBC options
      5. (Optional) verify schema against live DB
      6. Read target data via JDBC
      7. Save target CSV
    """
    # ------------------------------------------------------------------ #
    # 1. Load rulebook                                                    #
    # ------------------------------------------------------------------ #
    logger.info("Loading rulebook: %s", rulebook_path)
    rulebook = load_rulebook(rulebook_path)
    table_name: str = rulebook["meta"]["table_name"]
    logger.info("Processing table: %s", table_name)

    # ------------------------------------------------------------------ #
    # 2. Resolve target mode                                              #
    # ------------------------------------------------------------------ #
    mode = _resolve_target_mode(target_mode)
    logger.info("Target mode: %s", mode)

    # ------------------------------------------------------------------ #
    # 3. Resolve output directory                                         #
    # ------------------------------------------------------------------ #
    if not output_dir:
        output_dir = os.path.join(_PROJECT_ROOT, "output", table_name)
    output_csv = os.path.join(output_dir, "target_actual.csv")

    # ------------------------------------------------------------------ #
    # 4. Resolve the SQL query                                            #
    # ------------------------------------------------------------------ #
    sql = _resolve_query(query, query_file)
    logger.info("Target query resolved (%d chars)", len(sql))
    logger.debug("Query: %s", sql)

    # ------------------------------------------------------------------ #
    # 5. Build Spark + target connection                                  #
    # ------------------------------------------------------------------ #
    spark = get_spark_session(app_name=f"ETL_Target_{table_name}")
    jdbc_opts = get_target_connection(mode=mode)

    # ------------------------------------------------------------------ #
    # 6. Optional schema verification                                     #
    # ------------------------------------------------------------------ #
    if verify_schema:
        logger.info("Running target schema verification...")
        from verify_schema import verify_target_schema, SchemaVerificationError
        passed = verify_target_schema(rulebook, spark, jdbc_opts, dialect=mode)
        if not passed:
            logger.error("Target schema verification FAILED. Aborting extraction.")
            sys.exit(2)
        logger.info("Target schema verification PASSED.")

    # ------------------------------------------------------------------ #
    # 7. Extract target data via JDBC                                     #
    # ------------------------------------------------------------------ #
    logger.info("Extracting target data from %s...", rulebook["target"]["table"])
    try:
        df = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option("query", sql)
            .load()
        )
    except Exception as exc:
        _log_jdbc_error(exc, jdbc_opts)
        sys.exit(99)

    logger.info(
        "Target extract complete. Row count: %d | Columns: %s",
        df.count(),
        df.columns,
    )

    # ------------------------------------------------------------------ #
    # 8. Save target CSV                                                  #
    # ------------------------------------------------------------------ #
    logger.info("Saving target data to: %s", output_csv)
    save_dataframe_as_csv(df, output_csv)
    logger.info("Target pipeline complete → %s", output_csv)

    spark.stop()


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _resolve_target_mode(mode: Optional[str]) -> str:
    """
    Determine target mode.
    Priority: CLI --target-mode > target_config.yaml 'type' key > default 'mysql'.
    """
    if mode:
        if mode not in ("mysql", "snowflake"):
            raise ValueError(
                f"--target-mode must be 'mysql' or 'snowflake', got: {mode!r}"
            )
        return mode

    # Fall back to config file
    try:
        from utils.config_loader import load_config
        cfg = load_config(os.path.join(_PROJECT_ROOT, "config", "target_config.yaml"))
        cfg_mode = cfg.get("type", "mysql")
        if cfg_mode in ("mysql", "snowflake"):
            logger.debug("Target mode from config: %s", cfg_mode)
            return cfg_mode
    except FileNotFoundError:
        pass

    return "mysql"


def _resolve_query(query: Optional[str], query_file: Optional[str]) -> str:
    """Return the SQL string from either --query or --query-file."""
    if query and query_file:
        raise ValueError("Provide either --query or --query-file, not both.")

    if query:
        return query.strip()

    if query_file:
        abs_path = os.path.abspath(query_file)
        if not os.path.isfile(abs_path):
            raise FileNotFoundError(f"Query file not found: {abs_path}")
        with open(abs_path, "r", encoding="utf-8") as fh:
            return fh.read().strip()

    raise ValueError("Either --query or --query-file must be provided.")


def _log_jdbc_error(exc: Exception, jdbc_opts: dict) -> None:
    """Log a JDBC error without exposing the password."""
    safe_url = jdbc_opts.get("url", "unknown")
    user = jdbc_opts.get("user", "unknown")
    logger.error(
        "JDBC read failed: %s\n  URL: %s\n  User: %s\n"
        "  Check host/port reachability, credentials, and that the JAR is in jars/.",
        exc,
        safe_url,
        user,
    )


# --------------------------------------------------------------------------- #
# CLI entry point                                                              #
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract target table data and save to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--rulebook",
        required=True,
        help="Path to JSON rulebook, e.g. rulebook/orders_rulebook.json",
    )

    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        "--query",
        help="Inline SQL SELECT statement for target extraction.",
    )
    query_group.add_argument(
        "--query-file",
        dest="query_file",
        help="Path to a .sql file containing the target SELECT query.",
    )

    parser.add_argument(
        "--target-mode",
        dest="target_mode",
        choices=["mysql", "snowflake"],
        default=None,
        help="Target DB type. Defaults to 'type' value in target_config.yaml.",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Directory for output CSV. Defaults to output/<table_name>/.",
    )
    parser.add_argument(
        "--verify-schema",
        dest="verify_schema",
        action="store_true",
        default=False,
        help="Verify DDL against live DB schema before extraction.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run(
            rulebook_path=args.rulebook,
            query=args.query,
            query_file=args.query_file,
            target_mode=args.target_mode,
            output_dir=args.output_dir,
            verify_schema=args.verify_schema,
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
