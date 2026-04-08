"""
get_source_data.py
-------------------
Purpose : Extract data from the source MySQL database using a provided SQL query,
          apply column transformations defined in the rulebook, and save the
          enriched result to source_enriched.csv.

          This is Stage 4 of the ETL validation pipeline.

Inputs  :
    --rulebook       Path to the JSON rulebook (e.g. rulebook/orders_rulebook.json)
    --query          Inline SQL SELECT string for the source table (mutually exclusive with --query-file)
    --query-file     Path to a .sql file containing the source SELECT query
    --transform-file Path to a generated transform.py (e.g. generated/orders/transform.py)
    --output-dir     Directory where source_enriched.csv will be written
                     (default: output/<table_name>/)
    --verify-schema  If provided, run DDL vs live-DB schema check before extract
                     (requires generated DDL files to exist)
    --no-verify-schema  Skip schema verification (default behaviour)

Outputs :
    output/<table_name>/source_enriched.csv

Exit codes:
    0 — success
    1 — config / argument error
    2 — schema verification failed
    99 — unexpected runtime error

Usage   :
    python get_source_data.py \\
        --rulebook rulebook/orders_rulebook.json \\
        --query-file generated/orders/query_source.sql \\
        --transform-file generated/orders/transform.py \\
        --output-dir output/orders

    # Or with an inline query:
    python get_source_data.py \\
        --rulebook rulebook/orders_rulebook.json \\
        --query "SELECT o.order_id, o.status FROM orders_db.orders o" \\
        --transform-file generated/orders/transform.py
"""

import argparse
import importlib.util
import logging
import os
import sys
import types
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
from connections.source_connection import get_source_connection
from utils.csv_writer import save_dataframe_as_csv

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main pipeline function                                                       #
# --------------------------------------------------------------------------- #

def run(
    rulebook_path: str,
    query: Optional[str],
    query_file: Optional[str],
    transform_file: str,
    output_dir: Optional[str],
    verify_schema: bool,
) -> None:
    """
    Full source-extraction pipeline:
      1. Load rulebook
      2. Build SparkSession
      3. Get source JDBC options
      4. (Optional) verify schema against live DB
      5. Read source data via JDBC
      6. Apply transforms
      7. Save enriched CSV

    Raises SystemExit on recoverable failures (schema mismatch, bad args).
    """
    # ------------------------------------------------------------------ #
    # 1. Load rulebook                                                    #
    # ------------------------------------------------------------------ #
    logger.info("Loading rulebook: %s", rulebook_path)
    rulebook = load_rulebook(rulebook_path)
    table_name: str = rulebook["meta"]["table_name"]
    logger.info("Processing table: %s", table_name)

    # ------------------------------------------------------------------ #
    # 2. Resolve output directory                                         #
    # ------------------------------------------------------------------ #
    if not output_dir:
        output_dir = os.path.join(_PROJECT_ROOT, "output", table_name)
    output_csv = os.path.join(output_dir, "source_enriched.csv")

    # ------------------------------------------------------------------ #
    # 3. Resolve the SQL query                                            #
    # ------------------------------------------------------------------ #
    sql = _resolve_query(query, query_file)
    logger.info("Source query resolved (%d chars)", len(sql))
    logger.debug("Query: %s", sql)

    # ------------------------------------------------------------------ #
    # 4. Build Spark + source connection                                  #
    # ------------------------------------------------------------------ #
    spark = get_spark_session(app_name=f"ETL_Source_{table_name}")
    jdbc_opts = get_source_connection()

    # ------------------------------------------------------------------ #
    # 5. Optional schema verification                                     #
    # ------------------------------------------------------------------ #
    if verify_schema:
        logger.info("Running source schema verification...")
        from verify_schema import verify_source_schema, SchemaVerificationError
        passed = verify_source_schema(rulebook, spark, jdbc_opts)
        if not passed:
            logger.error("Source schema verification FAILED. Aborting extraction.")
            sys.exit(2)
        logger.info("Source schema verification PASSED.")

    # ------------------------------------------------------------------ #
    # 6. Extract source data via JDBC                                     #
    # ------------------------------------------------------------------ #
    logger.info("Extracting source data from %s...", rulebook["source"]["table"])
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
        "Source extract complete. Columns: %s", df.columns
    )

    # ------------------------------------------------------------------ #
    # 7. Apply transformations                                            #
    # ------------------------------------------------------------------ #
    logger.info("Applying transformations from: %s", transform_file)
    apply_transforms = _load_transform_function(transform_file)
    df_transformed = apply_transforms(df, rulebook)
    logger.info(
        "Transformation complete. Output columns: %s", df_transformed.columns
    )

    # ------------------------------------------------------------------ #
    # 8. Save enriched CSV                                                #
    # ------------------------------------------------------------------ #
    logger.info("Saving enriched source data to: %s", output_csv)
    save_dataframe_as_csv(df_transformed, output_csv)
    logger.info("Source pipeline complete → %s", output_csv)

    spark.stop()


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _resolve_query(query: Optional[str], query_file: Optional[str]) -> str:
    """Return the SQL string from either --query or --query-file."""
    if query and query_file:
        raise ValueError("Provide either --query or --query-file, not both.")

    if query:
        return query.strip()

    if query_file:
        abs_path = os.path.abspath(query_file)
        if not os.path.isfile(abs_path):
            raise FileNotFoundError(
                f"Query file not found: {abs_path}"
            )
        with open(abs_path, "r", encoding="utf-8") as fh:
            return fh.read().strip()

    raise ValueError(
        "Either --query or --query-file must be provided."
    )


def _load_transform_function(transform_file: str):
    """
    Dynamically import apply_transforms from the given transform.py path.

    Returns the callable apply_transforms(df, rulebook) -> DataFrame.
    Raises SystemExit(1) if the file is missing or the function is not found.
    """
    abs_path = os.path.abspath(transform_file)
    if not os.path.isfile(abs_path):
        logger.error("Transform file not found: %s", abs_path)
        logger.error(
            "Run generate_rulebook.py first to generate the transform module."
        )
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("_dynamic_transform", abs_path)
    module: types.ModuleType = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore

    if not hasattr(module, "apply_transforms"):
        logger.error(
            "transform file %s has no 'apply_transforms' function.", abs_path
        )
        sys.exit(1)

    return module.apply_transforms


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
        description="Extract source table data, apply transforms, save to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--rulebook",
        required=True,
        help="Path to the JSON rulebook, e.g. rulebook/orders_rulebook.json",
    )

    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        "--query",
        help="Inline SQL SELECT statement for source extraction.",
    )
    query_group.add_argument(
        "--query-file",
        dest="query_file",
        help="Path to a .sql file containing the source SELECT query.",
    )

    parser.add_argument(
        "--transform-file",
        dest="transform_file",
        required=True,
        help="Path to the generated transform.py, e.g. generated/orders/transform.py",
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
            transform_file=args.transform_file,
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
