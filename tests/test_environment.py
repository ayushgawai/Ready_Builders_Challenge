"""
Tests for src/environment.py — environmental data acquisition and raster sampling.

Test strategy:
  - download_raster: HTTP layer mocked with unittest.mock. The rasterio logic
    itself is NOT mocked — only the network call is. This is the correct
    industry approach: mock external I/O, test your own logic.
  - sample_raster_at_points: tested against real GeoTIFF fixtures created
    programmatically in tests/conftest.py. Actual rasterio.open() calls run
    against real files with known values — no mocking of geospatial logic.
  - compute_slope_raster: tested with a synthetic flat DEM (expect slope ≈ 0)
    and a uniform-slope DEM (expect analytically computed slope ± tolerance).
  - enrich_locations: integration test verifying all four columns are added
    with correct types and NaN handling.

GeoTIFF fixtures (from conftest.py):
  - small_canopy_raster    : 10×10, EPSG:4326, known canopy values
  - small_dem_raster       : 10×10, EPSG:4326, uniform eastward slope
  - small_dem_flat_raster  : 10×10, EPSG:4326, all-zero elevation
  - small_landcover_raster : 10×10, EPSG:4326, known NLCD codes
  - small_canopy_raster_5070 : 10×10, EPSG:5070, uniform value (CRS test)
"""

import io
import math
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import rasterio

from src.environment import (
    _extract_tif_from_zip,
    _is_geographic_4326,
    _is_zip,
    _verify_raster,
    compute_slope_raster,
    download_raster,
    enrich_locations,
    sample_raster_at_points,
)
from tests.conftest import (
    SAMPLE_COORD_CENTER,
    SAMPLE_COORD_FAR,
    SAMPLE_COORD_OOB,
    _RASTER_HEIGHT,
    _RASTER_WIDTH,
)


# ---------------------------------------------------------------------------
# _verify_raster
# ---------------------------------------------------------------------------

class TestVerifyRaster:

    def test_valid_raster_returns_true(self, small_canopy_raster):
        assert _verify_raster(small_canopy_raster) is True

    def test_nonexistent_file_returns_false(self, tmp_path):
        assert _verify_raster(tmp_path / "missing.tif") is False

    def test_non_raster_file_returns_false(self, tmp_path):
        bad = tmp_path / "bad.tif"
        bad.write_text("this is not a tif")
        assert _verify_raster(bad) is False


# ---------------------------------------------------------------------------
# _is_zip and _extract_tif_from_zip
# ---------------------------------------------------------------------------

class TestZipHelpers:

    def test_is_zip_by_url_extension(self):
        buf = io.BytesIO(b"not a zip")
        assert _is_zip("https://example.com/file.zip", "", buf) is True

    def test_is_zip_by_content_type(self):
        buf = io.BytesIO(b"not a zip")
        assert _is_zip("https://example.com/file", "application/zip", buf) is True

    def test_is_zip_by_magic_bytes(self):
        zip_magic = b"PK\x03\x04" + b"\x00" * 20
        buf = io.BytesIO(zip_magic)
        assert _is_zip("https://example.com/file", "application/octet-stream", buf) is True

    def test_is_not_zip(self):
        buf = io.BytesIO(b"\xff\xd8\xff" + b"\x00" * 20)  # JPEG magic
        assert _is_zip("https://example.com/file.tif", "image/tiff", buf) is False

    def test_extract_tif_from_zip(self, tmp_path, small_canopy_raster):
        """Wrap a real GeoTIFF in a zip and verify extraction works."""
        import zipfile as zf_lib

        zip_buffer = io.BytesIO()
        with zf_lib.ZipFile(zip_buffer, "w") as zf:
            zf.write(small_canopy_raster, arcname="canopy.tif")
        zip_buffer.seek(0)

        dest = tmp_path / "extracted.tif"
        _extract_tif_from_zip(zip_buffer, dest)
        assert dest.exists()
        assert _verify_raster(dest)

    def test_extract_tif_raises_if_no_tif_in_zip(self, tmp_path):
        zip_buffer = io.BytesIO()
        import zipfile as zf_lib
        with zf_lib.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("readme.txt", "no tif here")
        zip_buffer.seek(0)

        with pytest.raises(ValueError, match="No .tif file found"):
            _extract_tif_from_zip(zip_buffer, tmp_path / "out.tif")


# ---------------------------------------------------------------------------
# download_raster
# ---------------------------------------------------------------------------

class TestDownloadRaster:

    def test_skips_download_if_valid_raster_exists(self, tmp_path, small_canopy_raster):
        """If the dest_path is already a valid raster, no HTTP call is made."""
        dest = tmp_path / "existing.tif"
        # Copy the valid raster to the dest location
        import shutil
        shutil.copy(small_canopy_raster, dest)

        with patch("src.environment.requests.get") as mock_get:
            result = download_raster("https://example.com/file.tif", dest)
            mock_get.assert_not_called()

        assert result == dest

    def test_downloads_plain_tif_successfully(self, tmp_path, small_canopy_raster):
        """Simulate a successful plain .tif download (no zip)."""
        tif_bytes = small_canopy_raster.read_bytes()
        dest = tmp_path / "downloaded.tif"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"Content-Type": "image/tiff", "Content-Length": str(len(tif_bytes))}
        mock_response.iter_content.return_value = [tif_bytes]

        with patch("src.environment.requests.get", return_value=mock_response):
            result = download_raster("https://example.com/file.tif", dest)

        assert result == dest
        assert dest.exists()
        assert _verify_raster(dest)

    def test_downloads_and_extracts_zip(self, tmp_path, small_canopy_raster):
        """Simulate downloading a zip that contains a .tif."""
        import zipfile as zf_lib

        zip_buffer = io.BytesIO()
        with zf_lib.ZipFile(zip_buffer, "w") as zf:
            zf.write(small_canopy_raster, arcname="canopy.tif")
        zip_bytes = zip_buffer.getvalue()

        dest = tmp_path / "from_zip.tif"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(zip_bytes)),
        }
        mock_response.iter_content.return_value = [zip_bytes]

        with patch("src.environment.requests.get", return_value=mock_response):
            result = download_raster("https://example.com/file.zip", dest)

        assert result == dest
        assert _verify_raster(dest)

    def test_raises_after_max_retries(self, tmp_path):
        """Network failure should raise RuntimeError after all retries."""
        import requests as req_lib

        dest = tmp_path / "fail.tif"

        with patch("src.environment.requests.get", side_effect=req_lib.exceptions.ConnectionError("network down")):
            with patch("src.environment.time.sleep"):  # speed up test
                with pytest.raises(RuntimeError, match="failed after"):
                    download_raster("https://example.com/fail.tif", dest)

    def test_raises_if_downloaded_file_is_not_valid_raster(self, tmp_path):
        """A 200 OK response with garbage content should raise ValueError."""
        dest = tmp_path / "garbage.tif"
        garbage = b"this is not a raster file"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"Content-Type": "image/tiff", "Content-Length": str(len(garbage))}
        mock_response.iter_content.return_value = [garbage]

        with patch("src.environment.requests.get", return_value=mock_response):
            with pytest.raises(ValueError, match="not a valid GeoTIFF"):
                download_raster("https://example.com/garbage.tif", dest)


# ---------------------------------------------------------------------------
# sample_raster_at_points
# ---------------------------------------------------------------------------

class TestSampleRasterAtPoints:

    def test_samples_known_value_at_center_pixel(self, small_canopy_raster):
        """pixel (row=0, col=0) should return 75 (HIGH canopy quadrant)."""
        lat, lon = SAMPLE_COORD_CENTER
        result = sample_raster_at_points(small_canopy_raster, np.array([lat]), np.array([lon]))
        assert result.shape == (1,)
        assert result[0] == pytest.approx(75.0)

    def test_samples_low_canopy_quadrant(self, small_canopy_raster):
        """Bottom-left quadrant center → value = 10 (LOW canopy)."""
        # Row ~7, Col ~2: lat ≈ 44.25, lon ≈ -106.75
        lat, lon = 44.25, -106.75
        result = sample_raster_at_points(small_canopy_raster, np.array([lat]), np.array([lon]))
        assert result[0] == pytest.approx(10.0)

    def test_returns_nan_for_nodata_pixel(self, small_canopy_raster):
        """Pixel (row=9, col=9) is set to nodata=255 → should return NaN."""
        lat, lon = SAMPLE_COORD_FAR
        result = sample_raster_at_points(small_canopy_raster, np.array([lat]), np.array([lon]))
        assert np.isnan(result[0])

    def test_returns_nan_for_out_of_bounds(self, small_canopy_raster):
        """Coordinates outside raster extent → NaN, no exception."""
        lat, lon = SAMPLE_COORD_OOB
        result = sample_raster_at_points(small_canopy_raster, np.array([lat]), np.array([lon]))
        assert np.isnan(result[0])

    def test_mixed_in_and_out_of_bounds(self, small_canopy_raster):
        """In-bounds and out-of-bounds points in one call — only OOB get NaN."""
        lats = np.array([SAMPLE_COORD_CENTER[0], SAMPLE_COORD_OOB[0]])
        lons = np.array([SAMPLE_COORD_CENTER[1], SAMPLE_COORD_OOB[1]])
        result = sample_raster_at_points(small_canopy_raster, lats, lons)
        assert not np.isnan(result[0])       # in-bounds → has a value
        assert np.isnan(result[1])            # out-of-bounds → NaN

    def test_returns_array_of_same_length_as_input(self, small_canopy_raster):
        lats = np.array([44.5, 44.6, 44.7, 44.8])
        lons = np.array([-106.5, -106.6, -106.7, -106.8])
        result = sample_raster_at_points(small_canopy_raster, lats, lons)
        assert result.shape == (4,)

    def test_returns_all_nan_for_entirely_oob_input(self, small_canopy_raster):
        lats = np.array([10.0, 11.0])   # Far south of raster
        lons = np.array([-80.0, -81.0])
        result = sample_raster_at_points(small_canopy_raster, lats, lons)
        assert np.all(np.isnan(result))

    def test_crs_reprojection_epsg5070(self, small_canopy_raster_5070):
        """Sampling a raster in EPSG:5070 with lat/lon input coordinates.

        The fixture has uniform value 60 across all pixels. Any in-bounds
        lat/lon → reprojected to EPSG:5070 → should return 60.
        """
        lat, lon = 44.5, -106.5  # Centre of the raster area
        result = sample_raster_at_points(
            small_canopy_raster_5070, np.array([lat]), np.array([lon])
        )
        assert not np.isnan(result[0]), (
            "Expected a valid sample but got NaN — CRS reprojection may be broken"
        )
        assert result[0] == pytest.approx(60.0)

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            sample_raster_at_points(
                tmp_path / "missing.tif",
                np.array([44.5]),
                np.array([-106.5]),
            )

    def test_batch_processing_gives_same_result_as_single_batch(self, small_canopy_raster):
        """Splitting into multiple batches should give the same result as one batch."""
        lats = np.array([44.95, 44.75, 44.55, 44.35, 44.15])
        lons = np.array([-106.95, -106.75, -106.55, -106.35, -106.15])

        result_single = sample_raster_at_points(small_canopy_raster, lats, lons, batch_size=100)
        result_batched = sample_raster_at_points(small_canopy_raster, lats, lons, batch_size=2)

        np.testing.assert_array_equal(
            np.nan_to_num(result_single, nan=-1),
            np.nan_to_num(result_batched, nan=-1),
        )

    def test_dem_elevation_values_are_numeric(self, small_dem_raster):
        """DEM sampling should return numeric elevation values, not all NaN."""
        lat, lon = SAMPLE_COORD_CENTER
        result = sample_raster_at_points(small_dem_raster, np.array([lat]), np.array([lon]))
        assert not np.isnan(result[0]), "Expected numeric elevation, got NaN"
        assert result[0] >= 0  # col=0 → elevation = 0 * 200 = 0

    def test_landcover_returns_known_code(self, small_landcover_raster):
        """Top half = code 41 (Forest). Sample there and verify."""
        lat, lon = SAMPLE_COORD_CENTER  # row=0 → top half → code 41
        result = sample_raster_at_points(small_landcover_raster, np.array([lat]), np.array([lon]))
        assert result[0] == pytest.approx(41.0)


# ---------------------------------------------------------------------------
# compute_slope_raster
# ---------------------------------------------------------------------------

class TestComputeSlopeRaster:

    def test_flat_dem_produces_zero_slope(self, small_dem_flat_raster, tmp_path):
        """A completely flat DEM must produce a slope of exactly 0 everywhere."""
        slope_path = tmp_path / "slope_flat.tif"
        result_path = compute_slope_raster(small_dem_flat_raster, slope_path)

        assert result_path.exists()
        with rasterio.open(result_path) as src:
            slope = src.read(1).astype(float)
            nodata = src.nodata
            # Exclude nodata pixels (edges may be nodata if DEM has nodata)
            valid_slope = slope[slope != nodata] if nodata is not None else slope
            assert np.allclose(valid_slope, 0.0, atol=1e-4), (
                f"Expected all-zero slope for flat DEM, got max={valid_slope.max():.6f}"
            )

    def test_uniform_slope_dem_produces_expected_slope(self, small_dem_raster, tmp_path):
        """DEM with elevation = col * 200 should produce a predictable slope.

        At centre latitude 44.5°N:
          0.1° longitude ≈ 7,980 m  (111,319.5 * cos(44.5°) * 0.1)
          dz/dx = 200 / 7980 ≈ 0.02506 m/m
          slope = arctan(0.02506) * 180/π ≈ 1.435°

        numpy.gradient uses central differences for interior pixels, so
        interior pixel slopes should be close to the analytical value.
        Edge pixels use forward/backward differences (same result for
        uniform slope) — all pixels should match.
        """
        slope_path = tmp_path / "slope_uniform.tif"
        compute_slope_raster(small_dem_raster, slope_path)

        center_lat = 44.5
        deg_x = 0.1
        meters_per_deg_x = 111_319.5 * math.cos(math.radians(center_lat))
        res_x_m = deg_x * meters_per_deg_x
        expected_slope = math.degrees(math.atan(200.0 / res_x_m))

        with rasterio.open(slope_path) as src:
            slope = src.read(1).astype(float)
            nodata = src.nodata

        valid_slope = slope[slope != nodata] if nodata is not None else slope.ravel()

        assert np.allclose(valid_slope, expected_slope, atol=0.05), (
            f"Expected slope ≈ {expected_slope:.3f}°, "
            f"got range [{valid_slope.min():.3f}, {valid_slope.max():.3f}]"
        )

    def test_slope_raster_has_same_crs_as_dem(self, small_dem_raster, tmp_path):
        slope_path = tmp_path / "slope_crs.tif"
        compute_slope_raster(small_dem_raster, slope_path)

        with rasterio.open(small_dem_raster) as dem_src, rasterio.open(slope_path) as slope_src:
            assert dem_src.crs == slope_src.crs

    def test_slope_raster_has_same_dimensions_as_dem(self, small_dem_raster, tmp_path):
        slope_path = tmp_path / "slope_dims.tif"
        compute_slope_raster(small_dem_raster, slope_path)

        with rasterio.open(small_dem_raster) as dem_src, rasterio.open(slope_path) as slope_src:
            assert dem_src.width == slope_src.width
            assert dem_src.height == slope_src.height

    def test_skips_computation_if_slope_raster_exists(self, small_dem_raster, tmp_path):
        """If slope_path already exists, compute_slope_raster should not overwrite it."""
        slope_path = tmp_path / "slope_existing.tif"
        # Create a sentinel file
        slope_path.write_text("sentinel")
        result = compute_slope_raster(small_dem_raster, slope_path)
        # Should return the path without overwriting
        assert slope_path.read_text() == "sentinel"

    def test_raises_if_dem_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="DEM not found"):
            compute_slope_raster(tmp_path / "missing.tif", tmp_path / "slope.tif")


# ---------------------------------------------------------------------------
# enrich_locations
# ---------------------------------------------------------------------------

class TestEnrichLocations:

    def _make_df(self, lats, lons):
        """Helper: build a minimal locations DataFrame."""
        return pd.DataFrame(
            {
                "location_id": [f"LOC_{i:03d}" for i in range(len(lats))],
                "latitude": lats,
                "longitude": lons,
                "state": "MT",
                "county": "Test",
            }
        )

    def test_adds_all_four_columns(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        df = self._make_df([44.5], [-106.5])
        slope_path = tmp_path / "slope.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        for col in ["canopy_pct", "elevation_m", "slope_deg", "land_cover_code"]:
            assert col in enriched.columns, f"Missing column: {col}"

    def test_does_not_modify_original_dataframe(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        df = self._make_df([44.5], [-106.5])
        original_cols = list(df.columns)
        slope_path = tmp_path / "slope_orig.tif"
        enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        assert list(df.columns) == original_cols

    def test_in_bounds_points_get_non_nan_values(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        lat, lon = SAMPLE_COORD_CENTER
        df = self._make_df([lat], [lon])
        slope_path = tmp_path / "slope_ib.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        row = enriched.iloc[0]
        assert not np.isnan(row["canopy_pct"])
        assert not np.isnan(row["elevation_m"])
        assert not np.isnan(row["land_cover_code"])

    def test_out_of_bounds_points_get_nan(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        lat, lon = SAMPLE_COORD_OOB
        df = self._make_df([lat], [lon])
        slope_path = tmp_path / "slope_oob.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        row = enriched.iloc[0]
        assert np.isnan(row["canopy_pct"])

    def test_output_row_count_matches_input(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        lats = np.array([44.5, 44.6, 44.7])
        lons = np.array([-106.5, -106.6, -106.7])
        df = self._make_df(lats, lons)
        slope_path = tmp_path / "slope_count.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        assert len(enriched) == len(df)

    def test_canopy_pct_values_in_valid_range(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        """Canopy values should be in 0-100 range (or NaN)."""
        lats = np.array([44.95, 44.85, 44.55, 44.15])
        lons = np.array([-106.95, -106.15, -106.95, -106.15])
        df = self._make_df(lats, lons)
        slope_path = tmp_path / "slope_range.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        valid = enriched["canopy_pct"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_slope_values_are_non_negative(
        self, small_canopy_raster, small_dem_raster, small_landcover_raster, tmp_path
    ):
        """Slope in degrees must be ≥ 0 (it's a magnitude)."""
        lat, lon = SAMPLE_COORD_CENTER
        df = self._make_df([lat], [lon])
        slope_path = tmp_path / "slope_nn.tif"
        enriched = enrich_locations(
            df, small_canopy_raster, small_dem_raster, small_landcover_raster,
            slope_path=slope_path,
        )
        valid_slope = enriched["slope_deg"].dropna()
        assert (valid_slope >= 0).all()
