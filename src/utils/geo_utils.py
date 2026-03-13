"""
Geographic helper utilities reused across the pipeline.

These functions abstract common geospatial operations so they are not
duplicated across environment.py, risk_scoring.py, and validation.py.
"""

import logging
import numpy as np
import pandas as pd
from rasterio.crs import CRS
from rasterio.transform import Affine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Census FIPS state code (2-digit string) → 2-letter abbreviation.
# Source: US Census Bureau FIPS state codes (FIPS 5-2).
# Note: FIPS codes are NOT sequential (e.g., 03, 07, 14... are unused).
# ---------------------------------------------------------------------------
_FIPS_STATE_TO_ABBR: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

# ---------------------------------------------------------------------------
# US state full name → 2-letter abbreviation
# Used when reverse_geocoder returns admin1 as a full state name.
# ---------------------------------------------------------------------------
_STATE_NAME_TO_ABBR: dict[str, str] = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC",
}


def is_geographic_crs(crs: CRS) -> bool:
    """Return True if *crs* uses geographic (degree) coordinates.

    Geographic CRS examples: EPSG:4326 (WGS84), EPSG:4269 (NAD83).
    Projected CRS examples:  EPSG:5070 (Albers Equal Area — NLCD native),
                             UTM zones (metres).

    This distinction matters for:
      - Slope computation: geographic CRS requires degree→metre conversion
      - Coordinate reprojection: only reproject if raster is not already
        in the same geographic frame as the input lat/lon

    Parameters
    ----------
    crs:
        A :class:`rasterio.crs.CRS` instance.

    Returns
    -------
    bool
    """
    return crs.is_geographic


def cell_size_to_meters(
    transform: Affine,
    crs: CRS,
    center_lat: float,
) -> tuple[float, float]:
    """Return approximate pixel cell size in metres (res_x_m, res_y_m).

    For geographic CRS rasters (degrees), pixel sizes are converted using
    the standard approximation at *center_lat*:

        metres_per_degree_y ≈ 111,319.5 m  (constant — Earth's meridian arc)
        metres_per_degree_x ≈ 111,319.5 * cos(center_lat)  (shrinks with lat)

    Accuracy across CONUS (24–49 °N): better than 0.3%.
    Not accurate at the poles — acceptable for this project.

    For projected CRS rasters (metres), pixel sizes are read directly from
    the affine transform.

    Parameters
    ----------
    transform:
        Rasterio affine transform of the source raster.
    crs:
        CRS of the source raster.
    center_lat:
        Centre latitude of the raster in decimal degrees. Used only for
        geographic CRS; ignored for projected CRS.

    Returns
    -------
    tuple[float, float]
        ``(res_x_m, res_y_m)`` — pixel width and height in metres.
    """
    if is_geographic_crs(crs):
        res_x_deg = abs(transform.a)
        res_y_deg = abs(transform.e)
        meters_per_deg_y = 111_319.5
        meters_per_deg_x = 111_319.5 * np.cos(np.radians(center_lat))
        return res_x_deg * meters_per_deg_x, res_y_deg * meters_per_deg_y
    else:
        return abs(transform.a), abs(transform.e)


def derive_state_county_from_geoid(df: pd.DataFrame) -> pd.DataFrame:
    """Parse Census block GEOIDs to derive state abbreviation and county FIPS.

    The ``geoid_cb`` column contains 15-digit Census block GEOIDs in the format:
    ``SSCCCTTTTTTBBBB``
      SS    — 2-digit state FIPS (e.g. "37" = North Carolina)
      CCC   — 3-digit county FIPS (e.g. "179" = Union County)
      TTTTTT — 6-digit census tract
      BBBB  — 4-digit census block

    This function adds two derived columns:
      ``state``       — 2-letter state abbreviation (e.g. "NC")
      ``county_fips`` — 5-digit state+county FIPS (e.g. "37179")

    These are the standard identifiers for government data aggregation
    (BEAD reporting, FCC mapping) and are more authoritative than coordinates-
    based reverse geocoding because they encode the Census Bureau's own
    geographic assignment.

    Only rows where ``geoid_cb`` is non-null, numeric, and ≥12 characters are
    processed. Rows with null or malformed GEOIDs are skipped (their
    state/county_fips will remain null and will be caught by the reverse
    geocoding fallback in :func:`fill_missing_geo_fields`).

    Parameters
    ----------
    df:
        DataFrame containing a ``geoid_cb`` column.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with ``state`` and ``county_fips`` columns added
        or updated.
    """
    df = df.copy()

    if "geoid_cb" not in df.columns:
        return df

    # Ensure state and county_fips columns exist
    if "state" not in df.columns:
        df["state"] = pd.NA
    if "county_fips" not in df.columns:
        df["county_fips"] = pd.NA

    geoid_str = df["geoid_cb"].astype(str).str.strip()
    # Valid GEOID: all digits, at least 12 chars (block group minimum)
    valid_geoid_mask = (
        geoid_str.str.match(r"^\d{12,}$") & df["geoid_cb"].notna()
    )

    state_fips = geoid_str.str[:2]
    county_fips_5 = geoid_str.str[:5]

    state_abbr = state_fips.map(_FIPS_STATE_TO_ABBR)

    needs_state = df["state"].isna()
    needs_county = df["county_fips"].isna()

    state_filled = int((valid_geoid_mask & needs_state & state_abbr.notna()).sum())
    county_filled = int((valid_geoid_mask & needs_county).sum())

    df.loc[valid_geoid_mask & needs_state, "state"] = state_abbr[valid_geoid_mask & needs_state]
    df.loc[valid_geoid_mask & needs_county, "county_fips"] = county_fips_5[valid_geoid_mask & needs_county]

    if state_filled or county_filled:
        logger.info(
            "Derived %d state values and %d county_fips values from geoid_cb.",
            state_filled,
            county_filled,
        )

    skipped = int((df["geoid_cb"].notna() & ~valid_geoid_mask).sum())
    if skipped:
        logger.warning(
            "%d rows have non-null but malformed geoid_cb (non-numeric or too short). "
            "State/county will fall back to reverse geocoding for those rows.",
            skipped,
        )

    return df


def fill_missing_geo_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure every row has ``state`` and ``county_fips`` populated.

    Two-stage approach:

    **Stage 1 — Census GEOID parsing (primary, authoritative):**
    Calls :func:`derive_state_county_from_geoid` to extract state abbreviation
    and 5-digit county FIPS from the ``geoid_cb`` column. This is the preferred
    method because the GEOID encodes the Census Bureau's own geographic
    assignment — more reliable than nearest-neighbour coordinate lookup.

    **Stage 2 — Reverse geocoding (fallback):**
    For any rows still missing state after Stage 1 (null or malformed
    ``geoid_cb``), uses the ``reverse_geocoder`` library (offline GeoNames
    KD-tree, no API key required) to derive state from coordinates.

    Logs how many rows each stage filled — visible in the quality report.
    Degrades gracefully if ``reverse_geocoder`` is not installed.

    Parameters
    ----------
    df:
        DataFrame with at least ``latitude``, ``longitude``, and optionally
        ``geoid_cb`` columns.

    Returns
    -------
    pd.DataFrame
        DataFrame with ``state`` and ``county_fips`` columns present and
        populated where possible.
    """
    df = df.copy()

    # Stage 1: GEOID parsing (fast, authoritative, no dependencies)
    if "geoid_cb" in df.columns:
        df = derive_state_county_from_geoid(df)

    # Stage 2: Reverse geocoding fallback for any still-null state values
    still_missing_state = (
        "state" not in df.columns
        or df["state"].isna().any()
    )
    if not still_missing_state:
        return df

    try:
        import reverse_geocoder as rg  # type: ignore[import]
    except ImportError:
        logger.warning(
            "reverse_geocoder is not installed. "
            "Rows with null geoid_cb will have null state values. "
            "Run: pip install reverse_geocoder"
        )
        return df

    if "state" not in df.columns:
        df["state"] = pd.NA

    state_null_mask = df["state"].isna()
    if not state_null_mask.any():
        return df

    # Only reverse-geocode rows that have valid (non-null) coordinates.
    # Rows with null lat/lon will be dropped later by validate_locations;
    # passing NaN to reverse_geocoder causes worker-process crashes.
    valid_coord_mask = df["latitude"].notna() & df["longitude"].notna()
    geocode_mask = state_null_mask & valid_coord_mask

    if not geocode_mask.any():
        return df

    fill_indices = df.index[geocode_mask].tolist()
    coords = list(
        zip(df.loc[geocode_mask, "latitude"], df.loc[geocode_mask, "longitude"])
    )

    logger.info(
        "Reverse geocoding %d rows with null state after GEOID parsing (offline lookup)...",
        len(coords),
    )
    results = rg.search(coords, verbose=False)

    state_filled = 0
    for idx, result in zip(fill_indices, results):
        if result.get("cc") != "US":
            # Non-US coords should already have been dropped by CONUS bounds
            # check in validate_locations, but guard here defensively.
            continue
        full_name = result.get("admin1", "")
        abbr = _STATE_NAME_TO_ABBR.get(full_name)
        if abbr:
            df.at[idx, "state"] = abbr
            state_filled += 1

    logger.info(
        "Reverse geocoding filled %d state values (fallback for null geoid_cb rows).",
        state_filled,
    )
    return df
