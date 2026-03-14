"""
Entry point for the LEO Satellite Coverage Risk Analysis pipeline.

Two run modes
-------------
batch (default)
    Run the full pipeline end-to-end on the challenge CSV using the Claude agent.
    The agent loads, enriches, scores, validates, and reports on all ~4.67M locations.

    Example::

        python -m src.main --mode batch
        python -m src.main --mode batch --csv-path data/raw/locations_sample.csv

interactive
    Ask the agent a natural-language question about a specific location or about the
    results of a previous batch run.

    Example::

        python -m src.main --mode interactive \
            --query "What is the connectivity risk at 37.7749, -122.4194?"
        python -m src.main --mode interactive \
            --query "Which 5 states have the highest concentration of HIGH-risk locations?"

Pipeline-only (no agent)
    The pipeline can also run without the Claude agent — useful for testing individual
    modules or when the API key has insufficient credits.

    Example::

        python -m src.main --mode pipeline-only --csv-path data/raw/locations_sample.csv

Design notes
------------
- All configuration (paths, thresholds, model name) comes from src/config.py.
- Logging to console and to data/output/pipeline.log.
- Non-zero exit code on failure so this can be used in CI or shell scripts.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src import config
from src.utils.logging_config import setup_logging


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="leo-risk-pipeline",
        description="LEO Satellite Coverage Risk Analysis — agent-driven pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full batch run (default CSV, agent orchestration):
  python -m src.main

  # Batch run on a specific CSV:
  python -m src.main --mode batch --csv-path data/raw/locations_sample.csv

  # Interactive query:
  python -m src.main --mode interactive --query "Risk at 47.6, -122.3?"

  # Pipeline-only (no agent, useful if API credits unavailable):
  python -m src.main --mode pipeline-only
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["batch", "interactive", "pipeline-only"],
        default="batch",
        help=(
            "Run mode. "
            "'batch' = full pipeline via Claude agent (default). "
            "'interactive' = single query via agent. "
            "'pipeline-only' = run pipeline without agent (no API calls)."
        ),
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help=(
            "Path to the locations CSV file. "
            f"Defaults to: {config.LOCATIONS_CSV}"
        ),
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Natural language query for --mode interactive.",
    )
    parser.add_argument(
        "--force-resample",
        action="store_true",
        default=False,
        help=(
            "Force re-running raster sampling even if cached enriched data exists. "
            "WARNING: this will re-run the 20-60 minute sampling step."
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log verbosity level. Default: INFO.",
    )

    return parser.parse_args()


def run_batch_agent(csv_path: str | None, force_resample: bool = False) -> int:
    """Run the full pipeline via the Claude agent. Returns exit code."""
    from src.agent import PipelineAgent

    if not config.ANTHROPIC_API_KEY:
        logger = logging.getLogger(__name__)
        logger.error(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to your .env file or set the environment variable. "
            "To run without the agent, use --mode pipeline-only."
        )
        return 1

    agent = PipelineAgent()
    result = agent.run_pipeline(csv_path or str(config.LOCATIONS_CSV))

    print("\n" + "=" * 72)
    print("AGENT PIPELINE COMPLETE")
    print("=" * 72)
    print(result)
    print("=" * 72)
    print(f"\nMonitoring report: {config.MONITORING_REPORT_PATH}")
    print(f"Findings report:   {config.FINDINGS_REPORT_PATH}")
    print(f"Scored CSV:        {config.SCORED_LOCATIONS_PATH}\n")
    return 0


def run_interactive_agent(query: str | None) -> int:
    """Run an interactive query via the Claude agent. Returns exit code."""
    from src.agent import PipelineAgent

    if not config.ANTHROPIC_API_KEY:
        logger = logging.getLogger(__name__)
        logger.error("ANTHROPIC_API_KEY is not set. Cannot run interactive mode.")
        return 1

    if not query:
        query = input("Enter your query: ").strip()
    if not query:
        print("No query provided. Exiting.")
        return 1

    agent = PipelineAgent()
    result = agent.run_interactive(query)

    print("\n" + "=" * 72)
    print("AGENT RESPONSE")
    print("=" * 72)
    print(result)
    print("=" * 72 + "\n")
    return 0


def run_pipeline_only(csv_path: str | None) -> int:
    """Run the pipeline directly without the Claude agent. Returns exit code.

    Useful for:
    - Testing when API credits are unavailable
    - Debugging individual pipeline stages
    - Comparing agent-orchestrated vs direct execution
    """
    import pandas as pd
    from src.ingest import load_locations, validate_locations, generate_quality_report
    from src.risk_scoring import compute_composite_risk
    from src.validation import validate_results, generate_anomaly_report
    from src.reporting import generate_summary_stats, write_findings_report
    from src.utils.pipeline_utils import is_scored_cache_valid, save_scored_cache

    logger = logging.getLogger(__name__)
    csv_path = csv_path or str(config.LOCATIONS_CSV)

    print(f"\nPipeline-only mode: {csv_path}\n")

    # Step 1: Ingest
    print("Step 1/5: Loading and validating locations...")
    df = load_locations(csv_path)
    clean_df, quality_report = validate_locations(df)
    quality_text = generate_quality_report(quality_report)
    print(quality_text)

    config.VALIDATED_LOCATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(config.VALIDATED_LOCATIONS_PATH, index=False)

    # Step 2: Enrich (with idempotency)
    print("\nStep 2/5: Sampling environmental data...")
    if (
        config.ENRICHED_LOCATIONS_PATH.exists()
        and is_scored_cache_valid(config.VALIDATED_LOCATIONS_PATH, config.ENRICHED_LOCATIONS_PATH)
    ):
        print("  → Using cached enriched data (output is newer than input).")
        enriched_df = pd.read_csv(config.ENRICHED_LOCATIONS_PATH, dtype={"geoid_cb": str, "county_fips": str})
    else:
        from src.environment import download_raster, enrich_locations

        canopy_path = download_raster(config.CANOPY_RASTER_URL, config.CANOPY_RASTER_PATH)
        landcover_path = download_raster(config.LANDCOVER_RASTER_URL, config.LANDCOVER_RASTER_PATH)

        if not config.DEM_RASTER_PATH.exists():
            logger.error(
                "DEM raster not found at %s. "
                "Download from USGS National Map and save as data/raw/dem_conus.tif.",
                config.DEM_RASTER_PATH,
            )
            return 1

        enriched_df = enrich_locations(
            clean_df,
            canopy_path=canopy_path,
            dem_path=config.DEM_RASTER_PATH,
            landcover_path=landcover_path,
            slope_path=config.SLOPE_RASTER_PATH,
            batch_size=config.BATCH_SIZE,
        )
        save_scored_cache(enriched_df, config.ENRICHED_LOCATIONS_PATH)

    # Step 3: Score
    print("\nStep 3/5: Scoring risk...")
    scored_df = compute_composite_risk(enriched_df)
    save_scored_cache(scored_df, config.SCORED_LOCATIONS_PATH)

    tier_counts = scored_df["risk_tier"].value_counts()
    print(f"  Tier distribution:\n{tier_counts.to_string()}\n")

    # Step 4: Validate
    print("Step 4/5: Validating results...")
    is_valid, val_report = validate_results(scored_df)
    anomaly_text = generate_anomaly_report(scored_df, val_report)
    print(f"  Validation: is_valid={is_valid}")
    if not is_valid:
        print(f"  CRITICAL checks failed: {val_report.get('critical_checks', [])}")

    # Step 5: Report
    print("\nStep 5/5: Generating report...")
    stats = generate_summary_stats(scored_df)
    report_path = write_findings_report(stats)

    print(f"\nPipeline complete.")
    print(f"  Findings report: {report_path}")
    print(f"  Scored CSV:      {config.SCORED_LOCATIONS_PATH}\n")
    return 0


def main() -> None:
    args = _parse_args()

    setup_logging(
        level=args.log_level,
        log_file=config.OUTPUT_DIR / "pipeline.log",
    )

    logger = logging.getLogger(__name__)
    logger.info("LEO Satellite Coverage Risk Pipeline — mode=%s", args.mode)

    if args.mode == "batch":
        exit_code = run_batch_agent(args.csv_path, args.force_resample)
    elif args.mode == "interactive":
        exit_code = run_interactive_agent(args.query)
    elif args.mode == "pipeline-only":
        exit_code = run_pipeline_only(args.csv_path)
    else:
        logger.error("Unknown mode: %s", args.mode)
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
