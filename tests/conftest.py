"""
Shared pytest fixtures for the LEO Satellite Coverage Risk pipeline tests.

Design rationale (industry standard for geospatial Python projects):
  - DataFrame fixtures: built in-memory — no CSV files committed to git.
  - GeoTIFF fixtures: built programmatically with rasterio + numpy, stored in
    pytest-managed tmp_path_factory directories (session-scoped for performance).
    No binary files committed to git.
  - This follows the approach used by geopandas/pyogrio and EarthPy: generate
    test data as code, not as committed data artefacts.

Fixture scoping:
  - session scope: DataFrames and rasters that are read-only and expensive to
    create. A single instance is shared across all tests in the run.
  - function scope (default): used only for fixtures that write state to disk
    (e.g. a CSV that a test may modify). Not used here — our tmp files are
    read-only after creation.
"""

import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds

# ---------------------------------------------------------------------------
# Raster fixture geometry
# ---------------------------------------------------------------------------
# All test rasters cover: 44–45 °N, -107 to -106 °W  (Montana, CONUS)
# 10 × 10 pixels  →  0.1° per pixel
# Center of pixel (row, col):
#   row r  →  lat  = 44.95 - r * 0.1
#   col c  →  lon  = -106.95 + c * 0.1
# So (row=0, col=0) has center ≈ (44.95, -106.95)
#    (row=9, col=9) has center ≈ (44.05, -106.05)

_RASTER_WEST = -107.0
_RASTER_EAST = -106.0
_RASTER_SOUTH = 44.0
_RASTER_NORTH = 45.0
_RASTER_WIDTH = 10
_RASTER_HEIGHT = 10

# Two sampling test coordinates with analytically known pixel positions
# These are used across multiple tests; centralise them here.
SAMPLE_COORD_CENTER = (44.95, -106.95)   # → pixel (row=0, col=0)
SAMPLE_COORD_FAR = (44.05, -106.05)      # → pixel (row=9, col=9)
SAMPLE_COORD_OOB = (30.0, -80.0)         # outside the raster extent → NaN


# ---------------------------------------------------------------------------
# DataFrame fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def valid_locations_df() -> pd.DataFrame:
    """Ten valid CONUS location rows — all pass every validation check.

    geoid_cb values use real state+county FIPS with synthetic tract/block:
      format: SS CCC TTTTTT BBBB  (state 2 + county 3 + tract 6 + block 4 = 15 digits)
    e.g. "530330101001001" = WA (53) + King County (033) + synthetic tract/block
    """
    return pd.DataFrame(
        [
            {"location_id": "LOC_0001", "latitude": 47.6062, "longitude": -122.3321, "geoid_cb": "530330101001001"},  # WA King
            {"location_id": "LOC_0002", "latitude": 45.5231, "longitude": -122.6765, "geoid_cb": "410510101001001"},  # OR Multnomah
            {"location_id": "LOC_0003", "latitude": 37.7749, "longitude": -122.4194, "geoid_cb": "060750101001001"},  # CA San Francisco
            {"location_id": "LOC_0004", "latitude": 44.0521, "longitude": -123.0868, "geoid_cb": "410390101001001"},  # OR Lane
            {"location_id": "LOC_0005", "latitude": 48.7519, "longitude": -122.4787, "geoid_cb": "530730101001001"},  # WA Whatcom
            {"location_id": "LOC_0006", "latitude": 46.8797, "longitude": -110.3626, "geoid_cb": "300490101001001"},  # MT Meagher
            {"location_id": "LOC_0007", "latitude": 44.5588, "longitude":  -72.5778, "geoid_cb": "500230101001001"},  # VT Washington
            {"location_id": "LOC_0008", "latitude": 35.2271, "longitude":  -80.8431, "geoid_cb": "371190101001001"},  # NC Mecklenburg
            {"location_id": "LOC_0009", "latitude": 39.7392, "longitude": -104.9903, "geoid_cb": "080310101001001"},  # CO Denver
            {"location_id": "LOC_0010", "latitude": 36.1627, "longitude":  -86.7816, "geoid_cb": "470370101001001"},  # TN Davidson
        ]
    )


@pytest.fixture(scope="session")
def locations_with_issues_df() -> pd.DataFrame:
    """Twelve rows covering every validation failure type including new type checks.

    Schema matches the actual CSV: location_id, latitude, longitude, geoid_cb.
    state and county are DERIVED from geoid_cb in load_locations().

    Tracing through sequential validation in validate_locations:
      Pass 0 — non-numeric type check:
        LOC_BAD_LAT_TYPE (lat="not_a_number") : 1 dropped  → non_numeric_latitude
      Pass 1 — null / blank critical columns:
        null_location_id                       : 1 dropped  → null_location_id
        LOC_NULL_LAT                           : 1 dropped  → null_latitude
        LOC_NULL_LON                           : 1 dropped  → null_longitude
        LOC_BLANK_ID (location_id="   ")       : 1 dropped  → blank_location_id
      Pass 2 — out-of-range coordinates:
        LOC_OOR_LAT (lat=85.0)                 : 1 dropped
        LOC_OOR_LON (lon=-170.0)               : 1 dropped
      Pass 3 — duplicate location_id:
        second LOC_GOOD_01                     : 1 dropped
      Pass 4 — invalid state (safeguard on derived state):
        LOC_NULL_GEOID has null geoid_cb → state derived via reverse_geocoder
        (not dropped — reverse_geocoder fills state from valid CONUS coords)
      ─────────────────────────────────────────────────────────────────
      Total dropped : 8
      Total valid   : 4  (LOC_GOOD_01, LOC_GOOD_02, LOC_NULL_GEOID, LOC_GOOD_03)
    """
    return pd.DataFrame(
        [
            # Four clean rows with valid GEOIDs
            {"location_id": "LOC_GOOD_01",    "latitude": 47.6062,         "longitude": -122.3321, "geoid_cb": "530330101001001"},  # WA King
            {"location_id": "LOC_GOOD_02",    "latitude": 45.5231,         "longitude": -122.6765, "geoid_cb": "410510101001001"},  # OR Multnomah
            # Pass 0: non-numeric latitude — the string cannot be coerced to float
            {"location_id": "LOC_BAD_LAT",    "latitude": "not_a_number",  "longitude": -110.0,    "geoid_cb": "300490101001001"},
            # Pass 1: null critical columns (each counted separately)
            {"location_id": "LOC_NULL_LAT",   "latitude": None,            "longitude": -122.4194, "geoid_cb": "060750101001001"},
            {"location_id": "LOC_NULL_LON",   "latitude": 37.7749,         "longitude": None,      "geoid_cb": "410390101001001"},
            {"location_id": None,             "latitude": 48.7519,         "longitude": -122.4787, "geoid_cb": "530730101001001"},
            # Pass 1: blank/whitespace-only location_id — functionally same as null
            {"location_id": "   ",            "latitude": 40.0,            "longitude": -105.0,    "geoid_cb": "080310101001001"},
            # Pass 2: out-of-range coordinates
            {"location_id": "LOC_OOR_LAT",    "latitude": 85.0,            "longitude": -110.3626, "geoid_cb": "300490101001001"},
            {"location_id": "LOC_OOR_LON",    "latitude": 44.5588,         "longitude": -170.0,    "geoid_cb": "500230101001001"},
            # Pass 3: duplicate location_id — second occurrence should be dropped
            {"location_id": "LOC_GOOD_01",    "latitude": 35.2271,         "longitude":  -80.8431, "geoid_cb": "371190101001001"},
            # Valid: null geoid_cb → state filled via reverse geocoding (not dropped)
            {"location_id": "LOC_NULL_GEOID", "latitude": 33.4484,         "longitude": -112.0740, "geoid_cb": None},  # Phoenix AZ
            # Valid: one more clean row
            {"location_id": "LOC_GOOD_03",    "latitude": 29.7604,         "longitude":  -95.3698, "geoid_cb": "482010101001001"},  # TX Harris
        ]
    )


# ---------------------------------------------------------------------------
# CSV file fixtures  (write DataFrame to tmp_path → used by load_locations tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def valid_locations_csv(tmp_path_factory, valid_locations_df) -> "Path":
    """Temporary CSV file for load_locations() file-I/O tests."""
    path = tmp_path_factory.mktemp("csv") / "locations_valid.csv"
    valid_locations_df.to_csv(path, index=False)
    return path


@pytest.fixture(scope="session")
def issues_locations_csv(tmp_path_factory, locations_with_issues_df) -> "Path":
    """Temporary CSV file for end-to-end validation tests."""
    path = tmp_path_factory.mktemp("csv_issues") / "locations_issues.csv"
    locations_with_issues_df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Scored DataFrame fixture  (for Phase 5 validation tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def clean_scored_df() -> pd.DataFrame:
    """Ten fully-scored rows with a healthy, balanced tier distribution.

    All values are within valid ranges.  No NaN.  Three tiers represented.
    Used as the baseline "everything is fine" fixture for validation tests.

    Tier breakdown:
      HIGH      : 3 rows  (30%)
      MODERATE  : 4 rows  (40%)
      LOW       : 3 rows  (30%)
    """
    rows = [
        # HIGH risk rows (canopy HIGH, steep slope, forest land cover)
        {"location_id": "SC_H01", "latitude": 47.0, "longitude": -122.0,
         "canopy_pct": 75.0,  "slope_deg": 25.0, "land_cover_code": 41,
         "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
         "composite_score": 1.0, "risk_tier": "HIGH"},
        {"location_id": "SC_H02", "latitude": 47.1, "longitude": -122.1,
         "canopy_pct": 65.0,  "slope_deg": 22.0, "land_cover_code": 42,
         "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
         "composite_score": 1.0, "risk_tier": "HIGH"},
        {"location_id": "SC_H03", "latitude": 47.2, "longitude": -122.2,
         "canopy_pct": 80.0,  "slope_deg": 21.0, "land_cover_code": 43,
         "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
         "composite_score": 1.0, "risk_tier": "HIGH"},
        # MODERATE risk rows
        {"location_id": "SC_M01", "latitude": 45.0, "longitude": -120.0,
         "canopy_pct": 35.0,  "slope_deg": 15.0, "land_cover_code": 21,
         "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.5,
         "composite_score": 0.5, "risk_tier": "MODERATE"},
        {"location_id": "SC_M02", "latitude": 45.1, "longitude": -120.1,
         "canopy_pct": 40.0,  "slope_deg": 12.0, "land_cover_code": 22,
         "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.5,
         "composite_score": 0.5, "risk_tier": "MODERATE"},
        {"location_id": "SC_M03", "latitude": 45.2, "longitude": -120.2,
         "canopy_pct": 30.0,  "slope_deg": 18.0, "land_cover_code": 23,
         "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.5,
         "composite_score": 0.5, "risk_tier": "MODERATE"},
        {"location_id": "SC_M04", "latitude": 45.3, "longitude": -120.3,
         "canopy_pct": 25.0,  "slope_deg": 11.0, "land_cover_code": 24,
         "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.5,
         "composite_score": 0.5, "risk_tier": "MODERATE"},
        # LOW risk rows
        {"location_id": "SC_L01", "latitude": 40.0, "longitude": -100.0,
         "canopy_pct": 5.0,   "slope_deg": 2.0,  "land_cover_code": 82,
         "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.0,
         "composite_score": 0.0, "risk_tier": "LOW"},
        {"location_id": "SC_L02", "latitude": 40.1, "longitude": -100.1,
         "canopy_pct": 0.0,   "slope_deg": 0.0,  "land_cover_code": 71,
         "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.0,
         "composite_score": 0.0, "risk_tier": "LOW"},
        {"location_id": "SC_L03", "latitude": 40.2, "longitude": -100.2,
         "canopy_pct": 10.0,  "slope_deg": 5.0,  "land_cover_code": 31,
         "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.0,
         "composite_score": 0.0, "risk_tier": "LOW"},
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# GeoTIFF raster fixtures
# ---------------------------------------------------------------------------

def _write_raster(path, data: np.ndarray, crs: CRS, transform, nodata=None) -> None:
    """Write a numpy 2D array to a GeoTIFF file."""
    with rasterio.open(
        path,
        mode="w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


@pytest.fixture(scope="session")
def small_canopy_raster(tmp_path_factory) -> "Path":
    """10×10 canopy-cover GeoTIFF in EPSG:4326 with known values.

    Pixel value layout (0–100, representing canopy % cover):
      top-left  quadrant  (rows 0-4, cols 0-4) : 75  → HIGH risk  (>50)
      top-right quadrant  (rows 0-4, cols 5-9) : 35  → MODERATE  (20-50)
      bottom-left         (rows 5-9, cols 0-4) : 10  → LOW risk   (<20)
      bottom-right        (rows 5-9, cols 5-9) :  0  → LOW risk
      pixel (row=9, col=9)                      : 255 → nodata sentinel

    Sample test coordinates (centres of known pixels):
      (44.95, -106.95) → row=0, col=0 → value = 75
      (44.05, -106.05) → row=9, col=9 → nodata → NaN
    """
    tmp = tmp_path_factory.mktemp("rasters_4326")
    path = tmp / "canopy.tif"

    data = np.zeros((_RASTER_HEIGHT, _RASTER_WIDTH), dtype=np.uint8)
    data[0:5, 0:5] = 75   # HIGH
    data[0:5, 5:10] = 35  # MODERATE
    data[5:10, 0:5] = 10  # LOW
    data[5:10, 5:10] = 0  # LOW
    data[9, 9] = 255       # nodata sentinel

    transform = from_bounds(
        _RASTER_WEST, _RASTER_SOUTH, _RASTER_EAST, _RASTER_NORTH,
        _RASTER_WIDTH, _RASTER_HEIGHT,
    )
    _write_raster(path, data, CRS.from_epsg(4326), transform, nodata=255)
    return path


@pytest.fixture(scope="session")
def small_dem_raster(tmp_path_factory) -> "Path":
    """10×10 elevation GeoTIFF in EPSG:4326 with a uniform eastward slope.

    Elevation = col * 200  (metres)
    → Columns 0..9 have elevations 0, 200, 400, … 1800 m
    → Slope is entirely in the east-west (x) direction.

    At centre latitude ~44.5°N:
      0.1° longitude ≈ 7,980 m
      dz/dx = 200 / 7980 ≈ 0.02506 m/m
      expected slope ≈ arctan(0.02506) * 180/π ≈ 1.44°

    Interior pixels (not on edges) should all have slope ≈ 1.44°
    because numpy.gradient uses central differences internally.
    """
    tmp = tmp_path_factory.mktemp("dem_4326")
    path = tmp / "dem.tif"

    col_indices = np.arange(_RASTER_WIDTH, dtype=np.float32)
    data = np.tile(col_indices * 200, (_RASTER_HEIGHT, 1)).astype(np.float32)

    transform = from_bounds(
        _RASTER_WEST, _RASTER_SOUTH, _RASTER_EAST, _RASTER_NORTH,
        _RASTER_WIDTH, _RASTER_HEIGHT,
    )
    _write_raster(path, data, CRS.from_epsg(4326), transform, nodata=-9999)
    return path


@pytest.fixture(scope="session")
def small_dem_flat_raster(tmp_path_factory) -> "Path":
    """10×10 flat DEM (all zeros) — slope should be exactly 0 everywhere."""
    tmp = tmp_path_factory.mktemp("dem_flat")
    path = tmp / "dem_flat.tif"

    data = np.zeros((_RASTER_HEIGHT, _RASTER_WIDTH), dtype=np.float32)
    transform = from_bounds(
        _RASTER_WEST, _RASTER_SOUTH, _RASTER_EAST, _RASTER_NORTH,
        _RASTER_WIDTH, _RASTER_HEIGHT,
    )
    _write_raster(path, data, CRS.from_epsg(4326), transform, nodata=-9999)
    return path


@pytest.fixture(scope="session")
def small_landcover_raster(tmp_path_factory) -> "Path":
    """10×10 land-cover GeoTIFF in EPSG:4326 with known NLCD codes.

    Top half  (rows 0-4) : 41  → Deciduous Forest  → HIGH risk
    Bottom half (rows 5-9): 81  → Pasture/Hay       → LOW risk
    """
    tmp = tmp_path_factory.mktemp("lc_4326")
    path = tmp / "landcover.tif"

    data = np.zeros((_RASTER_HEIGHT, _RASTER_WIDTH), dtype=np.uint8)
    data[0:5, :] = 41   # Forest
    data[5:10, :] = 81  # Pasture

    transform = from_bounds(
        _RASTER_WEST, _RASTER_SOUTH, _RASTER_EAST, _RASTER_NORTH,
        _RASTER_WIDTH, _RASTER_HEIGHT,
    )
    _write_raster(path, data, CRS.from_epsg(4326), transform, nodata=0)
    return path


@pytest.fixture(scope="session")
def small_canopy_raster_5070(tmp_path_factory) -> "Path":
    """10×10 canopy raster in EPSG:5070 (Albers Equal Area — NLCD native CRS).

    Used to verify that sample_raster_at_points correctly reprojects lat/lon
    input coordinates into the raster's CRS before sampling.

    The raster covers approximately the same Montana area as the 4326 fixtures,
    but stored in Albers coordinates. Pixel values: uniform 60 (HIGH risk).
    The test transforms a known lat/lon to EPSG:5070 to confirm the sample
    returns 60 rather than NaN.
    """
    from pyproj import Transformer

    tmp = tmp_path_factory.mktemp("rasters_5070")
    path = tmp / "canopy_5070.tif"

    # Project the 4326 bounding box to EPSG:5070
    t = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    west_5070, south_5070 = t.transform(_RASTER_WEST, _RASTER_SOUTH)
    east_5070, north_5070 = t.transform(_RASTER_EAST, _RASTER_NORTH)

    data = np.full((_RASTER_HEIGHT, _RASTER_WIDTH), 60, dtype=np.uint8)

    transform = from_bounds(
        west_5070, south_5070, east_5070, north_5070,
        _RASTER_WIDTH, _RASTER_HEIGHT,
    )
    _write_raster(path, data, CRS.from_epsg(5070), transform, nodata=255)
    return path
