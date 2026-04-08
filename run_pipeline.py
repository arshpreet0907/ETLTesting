"""
run_pipeline.py
----------------
Purpose : Single entry point to run the complete ETL validation pipeline for
          one table.  Orchestrates all stages in order:

          Stage 4 — get_source_data : extract source → transform → source_enriched.csv
          Stage 5 — get_target_data : extract target → target_actual.csv
          Stage 6 — compare         : diff both CSVs → diff_report.csv + summary

          Stages 1–3 (excel_parser, generate_rulebook, verify_schema) are run
          separately before this script.  This script assumes:
            • The JSON rulebook already exists at --rulebook
            • The transform.py file already exists at --transform-file
            • Both SQL queries are provided (inline or via files)

Configuration
-------------
All connection details come from config/ YAML files.
All paths default to the project layout but can be overridden per-argument.

Usage examples
--------------

  # Full pipeline — inline queries, MySQL target
  python run_pipeline.py \\
    --rulebook  rulebook/orders_rulebook.json \\
    --source-query "SELECT o.order_id, o.status, o.unit_price FROM orders_db.orders o" \\
    --target-query "SELECT ORDER_ID, ORDER_STATUS, UNIT_PRICE_USD FROM dw.fact_orders" \\
    --transform-file generated/orders/transform.py \\
    --target-mode mysql

  # Full pipeline — queries from .sql files, Snowflake target
  python run_pipeline.py \\
    --rulebook  rulebook/orders_rulebook.json \\
    --source-query-file generated/orders/query_source.sql \\
    --target-query-file generated/orders/query_target.sql \\
    --transform-file generated/orders/transform.py \\
    --target-mode snowflake

  # Run only comparison (CSVs already exist)
  python run_pipeline.py \\
    --rulebook rulebook/orders_rulebook.json \\
    --compare-only \\
    --output-dir output/orders

Exit codes:
    0  — all stages passed, no data differences
    1  — config / argument / IO error
    2  — schema verification failed
    3  — data differences found (compare stage FAIL)
    99 — unexpected runtime error
"""

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# Bootstrap: make project root importable regardless of working directory
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Main orchestrator                                                            #
# --------------------------------------------------------------------------- #

def run(args: argparse.Namespace) -> None:
    """Execute the requested pipeline stages in order."""

    from utils.rulebook_loader import load_rulebook
    rulebook = load_rulebook(args.rulebook)
    table_name: str = rulebook["meta"]["table_name"]

    output_dir = args.output_dir or os.path.join(_PROJECT_ROOT, "output", table_name)
    source_csv = os.path.join(output_dir, "source_enriched.csv")
    target_csv = os.path.join(output_dir, "target_actual.csv")

    logger.info("=" * 60)
    logger.info("ETL Validation Pipeline — table: %s", table_name)
    logger.info("=" * 60)

    # ------------------------------------------------------------------ #
    # Stage 4: Extract + Transform source                                 #
    # ------------------------------------------------------------------ #
    if not args.compare_only:
        logger.info("--- Stage 4: Source extraction + transformation ---")
        import get_source_data
        get_source_data.run(
            rulebook_path=args.rulebook,
            query=args.source_query,
            query_file=args.source_query_file,
            transform_file=args.transform_file,
            output_dir=output_dir,
            verify_schema=args.verify_schema,
        )
        logger.info("Stage 4 complete.")

    # ------------------------------------------------------------------ #
    # Stage 5: Extract target                                             #
    # ------------------------------------------------------------------ #
    if not args.compare_only:
        logger.info("--- Stage 5: Target extraction ---")
        import get_target_data
        get_target_data.run(
            rulebook_path=args.rulebook,
            query=args.target_query,
            query_file=args.target_query_file,
            target_mode=args.target_mode,
            output_dir=output_dir,
            verify_schema=args.verify_schema,
        )
        logger.info("Stage 5 complete.")

    # ------------------------------------------------------------------ #
    # Stage 6: Compare                                                    #
    # ------------------------------------------------------------------ #
    logger.info("--- Stage 6: Comparison ---")
    import compare
    compare.run(
        rulebook_path=args.rulebook,
        source_csv=source_csv,
        target_csv=target_csv,
        output_dir=output_dir,
    )
    # compare.run calls sys.exit internally via reporter; we won't reach here on FAIL


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full ETL validation pipeline for one table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Core required arg
    parser.add_argument(
        "--rulebook",
        required=True,
        help="Path to JSON rulebook, e.g. rulebook/orders_rulebook.json",
    )

    # Source query — required unless --compare-only
    src_group = parser.add_mutually_exclusive_group()
    src_group.add_argument("--source-query", dest="source_query",
                           help="Inline SQL for source extraction.")
    src_group.add_argument("--source-query-file", dest="source_query_file",
                           help="Path to .sql file for source extraction.")

    # Target query — required unless --compare-only
    tgt_group = parser.add_mutually_exclusive_group()
    tgt_group.add_argument("--target-query", dest="target_query",
                           help="Inline SQL for target extraction.")
    tgt_group.add_argument("--target-query-file", dest="target_query_file",
                           help="Path to .sql file for target extraction.")

    parser.add_argument(
        "--transform-file",
        dest="transform_file",
        default=None,
        help="Path to generated transform.py. Required unless --compare-only.",
    )
    parser.add_argument(
        "--target-mode",
        dest="target_mode",
        choices=["mysql", "snowflake"],
        default=None,
        help="Target DB type. Reads from target_config.yaml if omitted.",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Output directory for all CSVs. Defaults to output/<table_name>/.",
    )
    parser.add_argument(
        "--verify-schema",
        dest="verify_schema",
        action="store_true",
        default=False,
        help="Run DDL schema verification before each extract stage.",
    )
    parser.add_argument(
        "--compare-only",
        dest="compare_only",
        action="store_true",
        default=False,
        help=(
            "Skip extraction stages and go straight to comparison. "
            "Assumes source_enriched.csv and target_actual.csv already exist."
        ),
    )

    args = parser.parse_args()

    # Validate: if not compare-only, query + transform-file are required
    if not args.compare_only:
        has_source = args.source_query or args.source_query_file
        has_target = args.target_query or args.target_query_file
        if not has_source:
            parser.error(
                "Provide --source-query or --source-query-file "
                "(or use --compare-only to skip extraction)."
            )
        if not has_target:
            parser.error(
                "Provide --target-query or --target-query-file "
                "(or use --compare-only to skip extraction)."
            )
        if not args.transform_file:
            parser.error(
                "Provide --transform-file "
                "(or use --compare-only to skip extraction)."
            )

    return args


if __name__ == "__main__":
    args = _parse_args()
    try:
        run(args)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        sys.exit(1)
    except SystemExit:
        raise  # Let compare.py's sys.exit propagate cleanly
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        sys.exit(99)
