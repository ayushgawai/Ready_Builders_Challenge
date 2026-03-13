"""
Data ingestion and validation for the LEO Satellite Coverage Risk pipeline.

Responsibilities:
  - Load the raw locations CSV into a pandas DataFrame
  - Validate schema, coordinate ranges, duplicates, and state codes
  - Produce a structured quality report that is both logged and saved to disk

Validation pass order (sequential to avoid double-counting):
  Pass 0: Non-numeric lat/lon  → values like "abc", "N/A", "#VALUE!" are coerced;
          rows where coercion fails (non-null→NaN) are dropped and counted.
  Pass 1: Null/blank critical columns → null location_id/lat/lon dropped;
          whitespace-only location_id ("   ") also treated as blank and dropped.
  Pass 2: Out-of-range coordinates (CONUS bounding box) → spatially invalid rows.
  Pass 3: Duplicate location_id → keep first occurrence, drop the rest.
  Pass 4: Invalid state code → must be one of 50 states + DC.

Design decisions:
  - Type coercion (Pass 0) runs before null checks so the quality report can
    distinguish "was missing in CSV" vs "was a non-numeric string in CSV".
  - Blank location_id ("   ") is not the same as null in Python, but is
    functionally identical — the row cannot be identified in any downstream join.
  - All thresholds and constants are imported from config.py — nothing hardcoded here.
  - The quality_report dict is the canonical record of what happened to the data;
    the text report is a human-readable rendering of that dict.
"""

import logging
from pathlib import Path

import pandas as pd

from src.config import (
    CONUS_LAT_MAX,
    CONUS_LAT_MIN,
    CONUS_LON_MAX,
    CONUS_LON_MIN,
    CRITICAL_COLUMNS,
    DATA_QUALITY_REPORT_PATH,
    EXPECTED_COLUMNS,
)
from src.utils.geo_utils import fill_missing_geo_fields

# geoid_cb is EXPECTED but not CRITICAL.
# Actual CSV schema (DATA_CHALLENGE_50.csv): location_id, latitude, longitude, geoid_cb.
# No state/county in the source CSV — they are derived in load_locations() via
# Census FIPS parsing (primary) or reverse geocoding (fallback).
_OPTIONAL_COLUMNS = [c for c in EXPECTED_COLUMNS if c not in CRITICAL_COLUMNS]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid US state + territory abbreviations
# Includes DC; does NOT include territories (PR, VI, etc.) because CONUS
# coordinate check would already drop those records.
# ---------------------------------------------------------------------------
VALID_STATE_CODES: frozenset[str] = frozenset(
    {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC",
    }
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_locations(file_path: str | Path) -> pd.DataFrame:
    """Load the locations CSV and perform schema validation.

    Reads the file into a DataFrame and confirms that all expected columns
    are present. Does NOT modify data — validation and cleaning happen in
    :func:`validate_locations`.

    Parameters
    ----------
    file_path:
        Path to the locations CSV.  Must contain columns:
        location_id, latitude, longitude, state, county.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame exactly as loaded from the CSV.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist at *file_path*.
    ValueError
        If one or more expected columns are missing from the CSV.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Locations CSV not found: {file_path}\n"
            "Place your data file at data/raw/locations.csv "
            "or pass the correct path explicitly."
        )

    logger.info("Loading locations from %s", file_path)
    # Read geoid_cb as string to preserve leading zeros (e.g. CA state FIPS "06")
    # and avoid float64 scientific-notation conversion when nulls are present.
    df = pd.read_csv(file_path, low_memory=False, dtype={"geoid_cb": str})
    logger.info("Loaded %d rows from %s", len(df), file_path.name)

    _check_schema(df, file_path)

    # Auto-fill missing state/county from coordinates (offline, fast).
    # No-op if both columns are complete. Graceful degradation if
    # reverse_geocoder is not installed.
    df = fill_missing_geo_fields(df)

    return df


def validate_locations(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Validate and clean the raw locations DataFrame.

    Applies four sequential validation passes.  Each pass records the rows
    dropped and the reason; dropped rows are never re-evaluated in later
    passes (no double-counting).

    Validation order (see module docstring for rationale):
      1. Null critical columns  (location_id, latitude, longitude)
      2. Out-of-range coordinates  (CONUS bounding box from config)
      3. Duplicate location_id  (keep first, drop subsequent)
      4. Invalid state code  (not in the 50-state + DC set)

    Parameters
    ----------
    df:
        Raw DataFrame as returned by :func:`load_locations`.

    Returns
    -------
    clean_df : pd.DataFrame
        DataFrame containing only records that passed all checks.
    quality_report : dict
        Structured quality metrics (see :func:`generate_quality_report`
        for the full schema).
    """
    total = len(df)
    dropped: dict[str, int] = {}
    working = df.copy()

    # --- Pass 0: Non-numeric type check for latitude and longitude ---
    # If the CSV contains string values like "abc", "N/A", "#VALUE!", pandas
    # reads the whole column as object or StringDtype (pandas 3+). We coerce to
    # float64 and track rows where coercion failed (non-null → became NaN).
    # These are counted separately from genuine nulls (Pass 1) so the quality
    # report can tell you "3 rows had the string 'N/A' in latitude" vs
    # "2 rows simply had no latitude value at all".
    # Uses .assign() to avoid pandas Copy-on-Write (CoW) issues with StringDtype.
    for col in ["latitude", "longitude"]:
        if col not in working.columns:
            continue
        original_non_null = working[col].notna()
        coerced = pd.to_numeric(working[col], errors="coerce")
        non_numeric_mask = original_non_null & coerced.isna()
        count = int(non_numeric_mask.sum())
        if count:
            key = f"non_numeric_{col}"
            dropped[key] = count
            working = working.loc[~non_numeric_mask].copy()
            logger.debug("Dropped %d rows: %s", count, key)
        # Always convert column to float64 for safe downstream comparisons
        # regardless of whether any rows were dropped.
        working = working.assign(**{col: pd.to_numeric(working[col], errors="coerce")})

    # --- Pass 1: Null / blank critical columns ---
    # Null check: location_id, latitude, longitude are non-negotiable.
    # Blank location_id: "   " is not null but is functionally identical —
    # the row cannot be identified or joined in any downstream operation.
    for col in CRITICAL_COLUMNS:
        mask_null = working[col].isna()
        count = int(mask_null.sum())
        if count:
            key = f"null_{col}"
            dropped[key] = count
            working = working[~mask_null].copy()
            logger.debug("Dropped %d rows: %s", count, key)

    blank_id_mask = working["location_id"].astype(str).str.strip() == ""
    blank_count = int(blank_id_mask.sum())
    if blank_count:
        dropped["blank_location_id"] = blank_count
        working = working[~blank_id_mask].copy()
        logger.debug("Dropped %d rows: blank_location_id", blank_count)

    # --- Pass 2: Out-of-range coordinates (CONUS bounds) ---
    mask_lat = (working["latitude"] < CONUS_LAT_MIN) | (working["latitude"] > CONUS_LAT_MAX)
    mask_lon = (working["longitude"] < CONUS_LON_MIN) | (working["longitude"] > CONUS_LON_MAX)
    mask_range = mask_lat | mask_lon

    out_of_range_count = int(mask_range.sum())
    if out_of_range_count:
        dropped["out_of_range_coordinates"] = out_of_range_count
        dropped["out_of_range_lat"] = int(mask_lat.sum())
        dropped["out_of_range_lon"] = int(mask_lon.sum())
        working = working[~mask_range].copy()
        logger.debug("Dropped %d rows: out_of_range_coordinates", out_of_range_count)

    # --- Pass 3: Duplicate location_id (keep first) ---
    dup_mask = working.duplicated(subset=["location_id"], keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count:
        dropped["duplicate_location_id"] = dup_count
        working = working[~dup_mask].copy()
        logger.debug("Dropped %d rows: duplicate_location_id", dup_count)

    # --- Pass 4: Invalid state code ---
    # Only run if 'state' column exists (it is expected but non-critical)
    if "state" in working.columns:
        invalid_state_mask = ~working["state"].str.strip().str.upper().isin(VALID_STATE_CODES)
        invalid_count = int(invalid_state_mask.sum())
        if invalid_count:
            dropped["invalid_state_code"] = invalid_count
            working = working[~invalid_state_mask].copy()
            logger.debug("Dropped %d rows: invalid_state_code", invalid_count)

    valid = len(working)
    total_dropped = total - valid

    quality_report: dict = {
        "total_records": total,
        "valid_records": valid,
        "dropped_records": total_dropped,
        "drop_reasons": dropped,
        "retention_rate_pct": round(valid / total * 100, 2) if total else 0.0,
    }

    logger.info(
        "Validation complete: %d valid / %d total (%.1f%% retained, %d dropped)",
        valid,
        total,
        quality_report["retention_rate_pct"],
        total_dropped,
    )

    return working.reset_index(drop=True), quality_report


# Human-readable labels for each drop-reason key that appears in the quality report.
_DROP_REASON_LABELS: dict[str, str] = {
    "non_numeric_latitude":     "Non-numeric value in latitude (e.g. 'abc', 'N/A')",
    "non_numeric_longitude":    "Non-numeric value in longitude (e.g. 'abc', 'N/A')",
    "null_location_id":         "Null / missing location_id",
    "blank_location_id":        "Blank / whitespace-only location_id",
    "null_latitude":            "Null / missing latitude",
    "null_longitude":           "Null / missing longitude",
    "out_of_range_coordinates": "Coordinates outside CONUS bounding box",
    "duplicate_location_id":    "Duplicate location_id (kept first occurrence)",
    "invalid_state_code":       "Invalid state code (not in 50-state + DC set)",
}


def generate_quality_report(quality_report: dict) -> str:
    """Format the quality report as a human-readable string and save it to disk.

    Writes the report to ``data/output/data_quality_report.txt`` so it is
    available for review after the pipeline run.

    Parameters
    ----------
    quality_report:
        Dict produced by :func:`validate_locations`.

    Returns
    -------
    str
        The formatted report text (also written to disk).
    """
    lines = [
        "=" * 60,
        "  DATA QUALITY REPORT — Locations CSV",
        "=" * 60,
        "",
        f"  Total records loaded  : {quality_report['total_records']:>10,}",
        f"  Valid records         : {quality_report['valid_records']:>10,}",
        f"  Dropped records       : {quality_report['dropped_records']:>10,}",
        f"  Retention rate        : {quality_report['retention_rate_pct']:>9.2f}%",
        "",
        "  Drop Reasons:",
    ]

    drop_reasons = quality_report.get("drop_reasons", {})
    if drop_reasons:
        for reason, count in drop_reasons.items():
            # Skip sub-breakdown keys already captured by the parent key
            if reason in ("out_of_range_lat", "out_of_range_lon"):
                continue
            label = _DROP_REASON_LABELS.get(reason, reason)
            lines.append(f"    - {label:<45}: {count:>8,}")
    else:
        lines.append("    (none — all records passed validation)")

    lines += ["", "=" * 60]
    report_text = "\n".join(lines)

    # Save to disk
    try:
        DATA_QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        DATA_QUALITY_REPORT_PATH.write_text(report_text, encoding="utf-8")
        logger.info("Quality report saved to %s", DATA_QUALITY_REPORT_PATH)
    except OSError as exc:
        logger.warning("Could not write quality report to disk: %s", exc)

    return report_text


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_schema(df: pd.DataFrame, file_path: Path) -> None:
    """Validate column presence: raise on missing critical columns, warn on optional.

    Critical columns (location_id, latitude, longitude) are non-negotiable —
    without them the pipeline cannot map or identify any location.

    Optional columns (state, county) are expected per the build plan but are
    not guaranteed by the challenge spec. Missing optional columns reduce
    reporting capability (no state-level aggregation) but do not halt ingestion.
    """
    # Critical check — hard failure
    missing_critical = [col for col in CRITICAL_COLUMNS if col not in df.columns]
    if missing_critical:
        raise ValueError(
            f"Missing required columns in {file_path.name}: {missing_critical}\n"
            f"Required: {CRITICAL_COLUMNS}\n"
            f"Found:    {list(df.columns)}"
        )

    # Optional check — warn only
    missing_optional = [col for col in _OPTIONAL_COLUMNS if col not in df.columns]
    if missing_optional:
        logger.warning(
            "Optional columns missing from %s: %s. "
            "State/county will be derived from coordinates via reverse geocoding.",
            file_path.name,
            missing_optional,
        )
