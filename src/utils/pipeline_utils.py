"""
Pipeline run management utilities — caching and idempotency.

The most expensive operation in this pipeline is raster sampling (Phase 3):
reading canopy/DEM/land-cover values for ~4.67M coordinates from national
GeoTIFF files.  On a standard laptop this takes 20–60 minutes.

We make the pipeline idempotent using a simple file-timestamp cache:
  - If the scored output already exists AND is newer than the input CSV,
    skip the full ingest → enrich → score pipeline and load from cache.
  - If the input CSV has changed (new data), or if the output is missing,
    run the full pipeline and overwrite the cache.

This is the "process once, reuse" pattern common in geospatial pipelines
(ETL checkpointing) and avoids the need for a message queue or database at POC scale.

The agent uses `is_scored_cache_valid()` at the start of `run_pipeline()` to
decide whether to call `ingest_locations → sample_environment → score_risk`
or to jump straight to `validate_results → generate_report`.

Design decision — why mtime not a hash?
  File hashing 4.67M rows (~500MB) takes ~3 seconds on a modern SSD and adds
  complexity.  Modification-time comparison is instantaneous and good enough for
  this use case (single-developer, no concurrent writes).  A production system
  would use a content hash or version manifest.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def is_scored_cache_valid(input_path: Path, output_path: Path) -> bool:
    """Return True if *output_path* exists and is newer than *input_path*.

    The agent calls this at the start of the pipeline to decide whether to
    run the full ingest→enrich→score pipeline or load results from cache.

    Parameters
    ----------
    input_path:
        Path to the raw input CSV (e.g. ``data/raw/DATA_CHALLENGE_50.csv``).
    output_path:
        Path to the scored output file (e.g. ``data/processed/locations_scored.csv``).

    Returns
    -------
    bool
        True  → cache is valid — skip pipeline, load from output_path.
        False → cache is missing or stale — run the full pipeline.
    """
    if not output_path.exists():
        logger.info("Cache miss: scored output does not exist at %s", output_path)
        return False

    if not input_path.exists():
        logger.warning(
            "Input file does not exist at %s; treating cache as invalid.", input_path
        )
        return False

    input_mtime = input_path.stat().st_mtime
    output_mtime = output_path.stat().st_mtime

    if output_mtime >= input_mtime:
        logger.info(
            "Cache hit: scored output at %s is newer than input at %s. "
            "Skipping raster sampling.",
            output_path.name,
            input_path.name,
        )
        return True

    logger.info(
        "Cache stale: input %s is newer than output %s. Re-running pipeline.",
        input_path.name,
        output_path.name,
    )
    return False


def load_scored_cache(cache_path: Path) -> pd.DataFrame:
    """Load a previously scored locations DataFrame from *cache_path*.

    Called by the agent when :func:`is_scored_cache_valid` returns True.
    Reads the CSV with correct dtypes so the scored DataFrame is immediately
    usable by ``validate_results`` and ``generate_report`` without re-scoring.

    Parameters
    ----------
    cache_path:
        Path to the scored CSV file (``data/processed/locations_scored.csv``).

    Returns
    -------
    pd.DataFrame
        Scored DataFrame with columns: location_id, latitude, longitude,
        geoid_cb, state, county, canopy_pct, slope_deg, land_cover_code,
        canopy_score, slope_score, landcover_score, composite_score, risk_tier.

    Raises
    ------
    FileNotFoundError
        If *cache_path* does not exist (caller should check
        :func:`is_scored_cache_valid` first).
    """
    if not cache_path.exists():
        raise FileNotFoundError(
            f"Scored cache not found at {cache_path}. "
            "Run the full pipeline first to generate it."
        )

    logger.info("Loading scored cache from %s ...", cache_path)
    df = pd.read_csv(
        cache_path,
        low_memory=False,
        dtype={
            "geoid_cb": str,       # preserve leading zeros
            "county": str,         # 5-digit FIPS — preserve leading zeros (e.g. "06075")
            "location_id": str,
            "risk_tier": str,
        },
    )
    logger.info("Loaded %d scored locations from cache.", len(df))
    return df


def save_scored_cache(df: pd.DataFrame, cache_path: Path) -> None:
    """Persist a scored locations DataFrame to *cache_path*.

    Called after ``score_risk`` completes. Overwrites any existing cache.
    Creates parent directories if they don't exist.

    Parameters
    ----------
    df:
        Scored DataFrame produced by ``compute_composite_risk``.
    cache_path:
        Destination path (``data/processed/locations_scored.csv``).
    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False)
    logger.info(
        "Scored cache saved: %d rows → %s", len(df), cache_path
    )
