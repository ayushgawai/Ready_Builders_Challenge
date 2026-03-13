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
    """Ten valid CONUS location rows — all pass every validation check."""
    return pd.DataFrame(
        [
            {"location_id": "LOC_0001", "latitude": 47.6062, "longitude": -122.3321, "state": "WA", "county": "King"},
            {"location_id": "LOC_0002", "latitude": 45.5231, "longitude": -122.6765, "state": "OR", "county": "Multnomah"},
            {"location_id": "LOC_0003", "latitude": 37.7749, "longitude": -122.4194, "state": "CA", "county": "San Francisco"},
            {"location_id": "LOC_0004", "latitude": 44.0521, "longitude": -123.0868, "state": "OR", "county": "Lane"},
            {"location_id": "LOC_0005", "latitude": 48.7519, "longitude": -122.4787, "state": "WA", "county": "Whatcom"},
            {"location_id": "LOC_0006", "latitude": 46.8797, "longitude": -110.3626, "state": "MT", "county": "Meagher"},
            {"location_id": "LOC_0007", "latitude": 44.5588, "longitude":  -72.5778, "state": "VT", "county": "Washington"},
            {"location_id": "LOC_0008", "latitude": 35.2271, "longitude":  -80.8431, "state": "NC", "county": "Mecklenburg"},
            {"location_id": "LOC_0009", "latitude": 39.7392, "longitude": -104.9903, "state": "CO", "county": "Denver"},
            {"location_id": "LOC_0010", "latitude": 36.1627, "longitude":  -86.7816, "state": "TN", "county": "Davidson"},
        ]
    )


@pytest.fixture(scope="session")
def locations_with_issues_df() -> pd.DataFrame:
    """Ten rows covering every validation failure type.

    Tracing through sequential validation in validate_locations:
      Pass 1 — null critical columns:
        null_location_id : 1 dropped  (LOC_NULL_ID row)
        null_latitude    : 1 dropped  (LOC_NULL_LAT row)
        null_longitude   : 1 dropped  (LOC_NULL_LON row)
      Pass 2 — out-of-range coordinates:
        LOC_OOR_LAT (lat=85.0)   : 1 dropped
        LOC_OOR_LON (lon=-170.0) : 1 dropped
      Pass 3 — duplicate location_id:
        second LOC_GOOD_01       : 1 dropped
      Pass 4 — invalid state:
        LOC_BAD_STATE (XX)       : 1 dropped
      ─────────────────────────────────────
      Total dropped : 7
      Total valid   : 3  (LOC_GOOD_01, LOC_GOOD_02, LOC_GOOD_03)
    """
    return pd.DataFrame(
        [
            # Two clean rows
            {"location_id": "LOC_GOOD_01",  "latitude": 47.6062, "longitude": -122.3321, "state": "WA", "county": "King"},
            {"location_id": "LOC_GOOD_02",  "latitude": 45.5231, "longitude": -122.6765, "state": "OR", "county": "Multnomah"},
            # Null critical columns (each is a separate row → counted separately)
            {"location_id": "LOC_NULL_LAT", "latitude": None,    "longitude": -122.4194, "state": "CA", "county": "San Francisco"},
            {"location_id": "LOC_NULL_LON", "latitude": 37.7749, "longitude": None,      "state": "OR", "county": "Lane"},
            {"location_id": None,           "latitude": 48.7519, "longitude": -122.4787, "state": "WA", "county": "Whatcom"},
            # Out-of-range coordinates
            {"location_id": "LOC_OOR_LAT",  "latitude": 85.0,   "longitude": -110.3626, "state": "MT", "county": "Meagher"},
            {"location_id": "LOC_OOR_LON",  "latitude": 44.5588, "longitude": -170.0,   "state": "VT", "county": "Washington"},
            # Duplicate location_id — second occurrence should be dropped
            {"location_id": "LOC_GOOD_01",  "latitude": 35.2271, "longitude":  -80.8431, "state": "NC", "county": "Mecklenburg"},
            # Invalid state code
            {"location_id": "LOC_BAD_STATE","latitude": 33.4484, "longitude": -112.0740, "state": "XX", "county": "Maricopa"},
            # One more clean row to confirm non-zero valid count
            {"location_id": "LOC_GOOD_03",  "latitude": 29.7604, "longitude":  -95.3698, "state": "TX", "county": "Harris"},
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
