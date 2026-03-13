"""
Geographic helper utilities reused across the pipeline.

These functions abstract common geospatial operations so they are not
duplicated across environment.py, risk_scoring.py, and validation.py.

State and county enrichment strategy
-------------------------------------
``state`` and ``county`` columns are populated in two stages:

Stage 1 — Census GEOID (primary, authoritative, fast):
  For rows with a valid ``geoid_cb`` Census block GEOID:
  - ``state``       : via :pypi:`us` library — ``us.states.lookup(fips).abbr``
  - ``county``      : via :pypi:`pygris` — Census TIGER/Line NAMELSAD field
                      (e.g. "Santa Clara County", "Orleans Parish")
  - ``county_fips`` : first 5 chars of geoid_cb (e.g. "06085")

Stage 2 — Reverse geocoding (fallback, only for null-state / null-county rows):
  For rows where GEOID parsing failed (null or malformed geoid_cb):
  - Uses :pypi:`reverse_geocoder` offline GeoNames KD-tree (no API key, no
    network after first library install).
  - ``county`` set from ``admin2`` (e.g. "Santa Clara County") — kept as-is.
  - ``state``  set from ``admin1`` (full name) via ``us.states.lookup()``.

Design rationale:
  - Running reverse_geocoder on every row of 4.67M is wasteful when 99%+
    have a valid GEOID. Geocoder is only called for the small fallback set.
  - No hardcoded state or county name dictionaries.  State names and FIPS
    codes are looked up from the ``us`` library (51 entries, permanent).
    County names (3,235 entries, can change on redistricting) are loaded
    from the authoritative Census TIGER/Line file via pygris, cached to
    ``~/.pygris/`` after first download (~2 MB).
"""

import functools
import logging

import numpy as np
import pandas as pd
from rasterio.crs import CRS
from rasterio.transform import Affine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Census county FIPS → NAMELSAD lookup (built once via pygris, then cached)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _county_namelsad_lookup() -> dict[str, str]:
    """Return a dict mapping 5-digit county FIPS → NAMELSAD county name.

    NAMELSAD is the Census Bureau's Legal Statistical Area Description:
      '06085' → 'Santa Clara County'
      '22071' → 'Orleans Parish'
      '02170' → 'Matanuska-Susitna Borough'

    The cartographic boundary file (~2 MB) is downloaded once from the
    Census Bureau via :pypi:`pygris` and cached to ``~/.pygris/``.
    All subsequent calls return the in-process cache (lru_cache) — zero I/O.

    Raises
    ------
    ImportError
        If pygris is not installed.  Callers should handle this gracefully.
    """
    try:
        import pygris  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "pygris is required for county name lookup. "
            "Run: pip install pygris"
        ) from exc

    logger.info(
        "Loading Census TIGER county boundaries via pygris "
        "(cartographic boundary, 2022, ~2 MB — cached after first download)..."
    )
    gdf = pygris.counties(cb=True, year=2022, cache=True)
    lookup: dict[str, str] = dict(zip(gdf["GEOID"], gdf["NAMELSAD"]))
    logger.info("County lookup ready: %d counties loaded.", len(lookup))
    return lookup


# ---------------------------------------------------------------------------
# CRS / raster geometry helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Geo enrichment
# ---------------------------------------------------------------------------

def derive_state_county_from_geoid(df: pd.DataFrame) -> pd.DataFrame:
    """Parse Census block GEOIDs to derive state, county, and county_fips.

    The ``geoid_cb`` column contains 15-digit Census block GEOIDs:
    ``SSCCCTTTTTTBBBB``
      SS    — 2-digit state FIPS  (e.g. "06" = California)
      CCC   — 3-digit county FIPS (e.g. "085" = Santa Clara)
      TTTTTT — 6-digit census tract
      BBBB  — 4-digit census block

    Adds / updates three columns:
      ``state``       — 2-letter abbreviation (e.g. "CA")
                        Source: :pypi:`us` library — no hardcoded dict.
      ``county``      — NAMELSAD county name (e.g. "Santa Clara County")
                        Source: Census TIGER/Line via :pypi:`pygris` — no
                        hardcoded dict.  Downloaded once, cached to ``~/.pygris/``.
      ``county_fips`` — 5-digit state+county FIPS (e.g. "06085")
                        Useful for FCC/BEAD broadband data joins.

    Only rows with a non-null, all-digit geoid_cb ≥ 12 chars are processed.

    Parameters
    ----------
    df:
        DataFrame containing a ``geoid_cb`` column.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with ``state``, ``county``, and ``county_fips``
        columns added or updated.
    """
    import us  # type: ignore[import]

    df = df.copy()

    if "geoid_cb" not in df.columns:
        return df

    for col in ("state", "county", "county_fips"):
        if col not in df.columns:
            df[col] = pd.NA

    geoid_str = df["geoid_cb"].astype(str).str.strip()
    valid_geoid_mask = (
        geoid_str.str.match(r"^\d{12,}$") & df["geoid_cb"].notna()
    )

    state_fips_series = geoid_str.str[:2]
    county_fips_series = geoid_str.str[:5]

    # --- State: us.states.lookup() handles all 50 states + DC ---
    def _fips_to_abbr(fips: str) -> str | None:
        obj = us.states.lookup(fips)
        return obj.abbr if obj else None

    # na_action="ignore" prevents pandas from passing pd.NA / NaN values
    # into _fips_to_abbr (which would crash us.states.lookup on non-string input).
    state_abbr_series = state_fips_series.map(_fips_to_abbr, na_action="ignore")

    # --- County name: Census TIGER NAMELSAD via pygris ---
    try:
        county_lookup = _county_namelsad_lookup()
        county_name_series = county_fips_series.map(county_lookup, na_action="ignore")
    except ImportError:
        logger.warning(
            "pygris not installed — county names will be null for GEOID rows. "
            "Run: pip install pygris"
        )
        county_name_series = pd.Series(pd.NA, index=df.index)

    needs_state  = df["state"].isna()
    needs_county = df["county"].isna()
    needs_fips   = df["county_fips"].isna()

    state_filled  = int((valid_geoid_mask & needs_state  & state_abbr_series.notna()).sum())
    county_filled = int((valid_geoid_mask & needs_county & county_name_series.notna()).sum())
    fips_filled   = int((valid_geoid_mask & needs_fips).sum())

    df.loc[valid_geoid_mask & needs_state,  "state"]       = state_abbr_series[valid_geoid_mask & needs_state]
    df.loc[valid_geoid_mask & needs_county, "county"]      = county_name_series[valid_geoid_mask & needs_county]
    df.loc[valid_geoid_mask & needs_fips,   "county_fips"] = county_fips_series[valid_geoid_mask & needs_fips]

    if state_filled or county_filled or fips_filled:
        logger.info(
            "GEOID parsing: %d state, %d county name, %d county_fips values derived.",
            state_filled, county_filled, fips_filled,
        )

    skipped = int((df["geoid_cb"].notna() & ~valid_geoid_mask).sum())
    if skipped:
        logger.warning(
            "%d rows have non-null but malformed geoid_cb — "
            "state/county will fall back to reverse geocoding.",
            skipped,
        )

    return df


def fill_missing_geo_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Populate ``state``, ``county``, and ``county_fips`` for every row.

    **Stage 1 — Census GEOID (primary):**
    Calls :func:`derive_state_county_from_geoid`.  For rows with a valid
    ``geoid_cb``, derives ``state`` (via :pypi:`us`), ``county`` (NAMELSAD
    from :pypi:`pygris` Census TIGER data), and ``county_fips``.

    **Stage 2 — Reverse geocoding (fallback only):**
    Only runs for rows that are *still* missing ``state`` or ``county`` after
    Stage 1 — typically rows with null or malformed ``geoid_cb``.
    Uses :pypi:`reverse_geocoder` (offline GeoNames KD-tree, no API key):
      - ``county`` ← ``admin2`` field as-is (e.g. "Santa Clara County")
      - ``state``  ← ``admin1`` full name → abbreviation via ``us.states.lookup()``

    Why not reverse_geocode every row?
      Running a KD-tree query on 4.67M rows for rows that already have an
      authoritative GEOID wastes 30–60 seconds for zero benefit.  The GEOID
      encodes the Census Bureau's own geographic assignment and is strictly
      more accurate than nearest-neighbour coordinate lookup.

    Parameters
    ----------
    df:
        DataFrame with ``latitude``, ``longitude``, and optionally ``geoid_cb``.

    Returns
    -------
    pd.DataFrame
        DataFrame with ``state``, ``county``, and ``county_fips`` present
        and populated where possible.
    """
    import us  # type: ignore[import]

    df = df.copy()

    # Stage 1: GEOID parsing (state + county name + county_fips)
    if "geoid_cb" in df.columns:
        df = derive_state_county_from_geoid(df)

    for col in ("state", "county", "county_fips"):
        if col not in df.columns:
            df[col] = pd.NA

    # Stage 2: reverse geocoding ONLY for rows still missing state or county
    still_missing = df["state"].isna() | df["county"].isna()
    if not still_missing.any():
        return df

    try:
        import reverse_geocoder as rg  # type: ignore[import]
    except ImportError:
        logger.warning(
            "reverse_geocoder is not installed — "
            "rows with null geoid_cb will have null state/county. "
            "Run: pip install reverse_geocoder"
        )
        return df

    # Guard: only geocode rows with valid, numeric coordinates.
    # Passing NaN or non-numeric strings to the KD-tree causes worker crashes.
    lat_numeric = pd.to_numeric(df["latitude"], errors="coerce")
    lon_numeric = pd.to_numeric(df["longitude"], errors="coerce")
    valid_coord_mask = lat_numeric.notna() & lon_numeric.notna()

    fallback_mask = still_missing & valid_coord_mask
    if not fallback_mask.any():
        return df

    fallback_indices = df.index[fallback_mask].tolist()
    coords = list(zip(lat_numeric[fallback_mask], lon_numeric[fallback_mask]))

    logger.info(
        "Reverse geocoding %d rows with missing state/county "
        "(fallback for null/malformed geoid_cb)...",
        len(coords),
    )
    results = rg.search(coords, verbose=False)

    county_filled = 0
    state_filled = 0

    for idx, result in zip(fallback_indices, results):
        if result.get("cc") != "US":
            # Non-US coordinates — CONUS bounds check drops these later.
            continue

        if pd.isna(df.at[idx, "county"]):
            raw_county = result.get("admin2", "")
            if raw_county:
                # Keep the GeoNames admin2 value as-is — no manual suffix stripping.
                df.at[idx, "county"] = raw_county
                county_filled += 1

        if pd.isna(df.at[idx, "state"]):
            full_name = result.get("admin1", "")
            state_obj = us.states.lookup(full_name)
            if state_obj:
                df.at[idx, "state"] = state_obj.abbr
                state_filled += 1

    logger.info(
        "Reverse geocoding fallback: %d county, %d state values filled.",
        county_filled, state_filled,
    )
    return df
