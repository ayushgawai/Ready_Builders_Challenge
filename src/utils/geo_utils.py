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


def fill_missing_geo_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Auto-fill missing state and county values using offline reverse geocoding.

    Uses the ``reverse_geocoder`` library (offline GeoNames nearest-neighbour
    dataset — no API key or internet required at runtime) to look up the US
    state and county for any row where those values are absent or null.

    Why this is useful:
      - The challenge specifies all five columns, but null values in state/county
        are common in multi-period BEAD filing compilations.
        State and county may be missing from some provider submissions.
      - Reverse geocoding from coordinates is fast (~1M rows in under a minute
        using reverse_geocoder's internal KD-tree).
      - State-level aggregation in the final report requires state codes.
        This function makes the report more complete without imposing a hard
        requirement on the input schema.

    Behaviour:
      - If ``state`` column is missing entirely: add it and fill all rows.
      - If ``state`` column exists but has null values: fill nulls only.
      - Same logic for ``county``.
      - Logs how many rows were auto-filled for auditing in the quality report.
      - If ``reverse_geocoder`` is not installed, logs a warning and returns
        the DataFrame unchanged (graceful degradation — the rest of the
        pipeline does not depend on state/county).

    Parameters
    ----------
    df:
        DataFrame with at least ``latitude`` and ``longitude`` columns.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with ``state`` and ``county`` columns present,
        filled where possible.
    """
    try:
        import reverse_geocoder as rg  # type: ignore[import]
    except ImportError:
        logger.warning(
            "reverse_geocoder is not installed. "
            "Missing state/county values will not be auto-filled. "
            "Run: pip install reverse_geocoder"
        )
        return df

    df = df.copy()

    needs_state = "state" not in df.columns or df["state"].isna().any()
    needs_county = "county" not in df.columns or df["county"].isna().any()

    if not (needs_state or needs_county):
        return df

    # Identify rows that need any geo field filled
    state_null_mask = (
        df["state"].isna() if "state" in df.columns else pd.Series(True, index=df.index)
    )
    county_null_mask = (
        df["county"].isna() if "county" in df.columns else pd.Series(True, index=df.index)
    )
    rows_to_fill_mask = state_null_mask | county_null_mask

    if not rows_to_fill_mask.any():
        return df

    fill_indices = df.index[rows_to_fill_mask].tolist()
    coords = list(
        zip(df.loc[rows_to_fill_mask, "latitude"], df.loc[rows_to_fill_mask, "longitude"])
    )

    logger.info(
        "Reverse geocoding %d rows with missing state/county (offline lookup)...",
        len(coords),
    )
    results = rg.search(coords, verbose=False)

    # Ensure columns exist
    if "state" not in df.columns:
        df["state"] = pd.NA
    if "county" not in df.columns:
        df["county"] = pd.NA

    state_filled = 0
    county_filled = 0

    for idx, result in zip(fill_indices, results):
        if result.get("cc") != "US":
            # Only fill US records — non-US coords should have been dropped
            # by the CONUS bounds check before this step.
            continue

        if state_null_mask.loc[idx]:
            full_name = result.get("admin1", "")
            abbr = _STATE_NAME_TO_ABBR.get(full_name)
            if abbr:
                df.at[idx, "state"] = abbr
                state_filled += 1

        if county_null_mask.loc[idx]:
            county_name = result.get("admin2", "")
            if county_name:
                df.at[idx, "county"] = county_name
                county_filled += 1

    logger.info(
        "Auto-filled %d state values and %d county values from coordinates.",
        state_filled,
        county_filled,
    )
    return df
