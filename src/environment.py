"""
Environmental data acquisition and raster point sampling.

Responsibilities:
  - Download national raster datasets (NLCD canopy, USGS DEM, NLCD land cover)
  - Sample raster pixel values at arbitrary lat/lon coordinates
  - Pre-compute a slope raster from the DEM and cache it to disk
  - Enrich a locations DataFrame with canopy_pct, elevation_m, slope_deg,
    land_cover_code columns

Design decisions:
  - rasterio.DatasetReader.sample() for point sampling: performs windowed reads
    internally — memory-efficient at 1M+ points (no full raster load into RAM).
  - pyproj.Transformer for CRS reprojection: NLCD rasters are in EPSG:5070
    (Albers Equal Area); input coordinates are EPSG:4326. Reprojection happens
    transparently inside sample_raster_at_points.
  - Pre-computed slope raster: computing slope on-the-fly per point would
    require 1M windowed reads from the DEM. Pre-computing once and sampling the
    result raster is orders of magnitude faster.
  - Geographic DEM slope approximation: when the DEM is in a geographic CRS
    (lat/lon degrees), pixel sizes are converted to metres using the standard
    approximation (111,319.5 m/° in Y; 111,319.5 * cos(lat) m/° in X).
    Accuracy across CONUS: ±0.3%. Adequate for HIGH/MODERATE/LOW tier
    assignment. See DECISION_LOG.md for the full tradeoff discussion.
  - Retry logic on download: MRLC/USGS servers return transient errors.
    3 retries with exponential backoff (2s → 4s → 8s) handle this without
    hanging the pipeline indefinitely.
"""

import io
import logging
import time
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from tqdm import tqdm

import requests

from src.config import (
    BATCH_SIZE,
    CANOPY_RASTER_PATH,
    CANOPY_RASTER_URL,
    DEM_RASTER_PATH,
    LANDCOVER_RASTER_PATH,
    LANDCOVER_RASTER_URL,
    SLOPE_RASTER_PATH,
)

logger = logging.getLogger(__name__)

# Retry configuration for raster downloads
_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE = 2  # seconds; doubles each retry


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_raster(url: str, dest_path: Path) -> Path:
    """Download a raster file and save it to *dest_path*.

    If *dest_path* already exists and is a valid GeoTIFF, the download is
    skipped entirely (idempotent).

    Handles zip archives transparently: if the response is a zip file, the
    first .tif inside is extracted to *dest_path*.

    Parameters
    ----------
    url:
        HTTP(S) URL of the raster to download.
    dest_path:
        Destination path for the .tif file. Parent directories are created
        automatically.

    Returns
    -------
    Path
        The path to the downloaded (or already-existing) GeoTIFF.

    Raises
    ------
    RuntimeError
        If the download fails after all retries.
    ValueError
        If the downloaded file is not a valid GeoTIFF.
    """
    dest_path = Path(dest_path)

    if dest_path.exists() and _verify_raster(dest_path):
        logger.info("Raster already exists and is valid — skipping download: %s", dest_path)
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading raster from %s → %s", url, dest_path)

    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            total_bytes = int(response.headers.get("Content-Length", 0)) or None

            # Stream into memory buffer to decide whether to unzip
            buffer = io.BytesIO()
            with tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest_path.name,
                leave=False,
            ) as progress:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        buffer.write(chunk)
                        progress.update(len(chunk))

            buffer.seek(0)

            if _is_zip(url, content_type, buffer):
                buffer.seek(0)
                _extract_tif_from_zip(buffer, dest_path)
            else:
                dest_path.write_bytes(buffer.getvalue())

            if not _verify_raster(dest_path):
                dest_path.unlink(missing_ok=True)
                raise ValueError(
                    f"Downloaded file is not a valid GeoTIFF: {dest_path}\n"
                    f"Source URL: {url}"
                )

            logger.info("Download complete: %s", dest_path)
            return dest_path

        except requests.exceptions.RequestException as exc:
            if attempt == _DOWNLOAD_MAX_RETRIES:
                raise RuntimeError(
                    f"Raster download failed after {_DOWNLOAD_MAX_RETRIES} attempts.\n"
                    f"URL: {url}\n"
                    f"Last error: {exc}\n"
                    "Download the file manually and place it at: "
                    f"{dest_path}"
                ) from exc
            wait = _DOWNLOAD_BACKOFF_BASE ** attempt
            logger.warning(
                "Download attempt %d/%d failed (%s). Retrying in %ds…",
                attempt,
                _DOWNLOAD_MAX_RETRIES,
                exc,
                wait,
            )
            time.sleep(wait)

    # Unreachable — loop always returns or raises
    raise RuntimeError("Unexpected exit from download retry loop")


def sample_raster_at_points(
    raster_path: Path,
    lats: np.ndarray,
    lons: np.ndarray,
    batch_size: int = BATCH_SIZE,
) -> np.ndarray:
    """Sample raster pixel values at the given geographic coordinates.

    Reprojects coordinates to the raster's native CRS if necessary, then
    uses rasterio's sample() method (windowed reads) for memory efficiency.

    Parameters
    ----------
    raster_path:
        Path to a GeoTIFF raster file.
    lats:
        1-D array of latitudes (decimal degrees, EPSG:4326).
    lons:
        1-D array of longitudes (decimal degrees, EPSG:4326).
    batch_size:
        Number of points processed per rasterio.sample() call. Controls the
        tradeoff between memory usage and per-call overhead.

    Returns
    -------
    np.ndarray
        1-D float array, same length as *lats*. Points outside the raster
        extent or matching the nodata value are returned as ``np.nan``.

    Raises
    ------
    FileNotFoundError
        If *raster_path* does not exist.
    """
    raster_path = Path(raster_path)
    if not raster_path.exists():
        raise FileNotFoundError(
            f"Raster not found: {raster_path}\n"
            "Run download_raster() first or place the file manually."
        )

    lats = np.asarray(lats, dtype=np.float64)
    lons = np.asarray(lons, dtype=np.float64)
    n_points = len(lats)
    result = np.full(n_points, np.nan, dtype=np.float64)

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        nodata = src.nodata
        bounds = src.bounds

        # --- CRS reprojection -------------------------------------------------
        # Input coords are always EPSG:4326 (lat/lon). Many rasters (e.g. NLCD)
        # are in EPSG:5070 (Albers Equal Area). Reproject before sampling.
        if raster_crs and not _is_geographic_4326(raster_crs):
            transformer = Transformer.from_crs(
                "EPSG:4326", raster_crs, always_xy=True
            )
            # always_xy=True: input as (lon, lat), output as (x, y)
            xs, ys = transformer.transform(lons, lats)
        else:
            # Raster is already in geographic lat/lon; swap to (x=lon, y=lat)
            xs = lons.copy()
            ys = lats.copy()

        # --- Spatial filter ---------------------------------------------------
        # rasterio.sample() raises if asked to sample outside the raster extent.
        # Pre-filter to in-bounds points; out-of-bounds remain NaN.
        in_bounds_mask = (
            (xs >= bounds.left)
            & (xs <= bounds.right)
            & (ys >= bounds.bottom)
            & (ys <= bounds.top)
        )
        valid_indices = np.where(in_bounds_mask)[0]

        if valid_indices.size == 0:
            logger.warning(
                "No input points fall within raster extent %s for %s",
                bounds,
                raster_path.name,
            )
            return result

        out_of_bounds_count = n_points - valid_indices.size
        if out_of_bounds_count:
            logger.debug(
                "%d / %d points are outside raster extent → NaN",
                out_of_bounds_count,
                n_points,
            )

        # --- Batched sampling -------------------------------------------------
        n_batches = int(np.ceil(valid_indices.size / batch_size))
        for batch_num, start in enumerate(
            tqdm(
                range(0, valid_indices.size, batch_size),
                total=n_batches,
                desc=f"Sampling {raster_path.name}",
                unit="batch",
                leave=False,
            )
        ):
            batch_idx = valid_indices[start : start + batch_size]
            coords = list(zip(xs[batch_idx], ys[batch_idx]))

            try:
                sampled = np.array(
                    [val[0] for val in src.sample(coords)], dtype=np.float64
                )
            except Exception as exc:
                logger.warning(
                    "Batch %d/%d sampling failed for %s: %s — filling with NaN",
                    batch_num + 1,
                    n_batches,
                    raster_path.name,
                    exc,
                )
                continue

            # Replace nodata sentinel with NaN
            if nodata is not None:
                sampled[sampled == nodata] = np.nan

            result[batch_idx] = sampled

    return result


def compute_slope_raster(dem_path: Path, slope_path: Path) -> Path:
    """Pre-compute a terrain slope raster from a DEM and save it to disk.

    Reads the full DEM into memory and computes slope using numpy.gradient.
    For geographic CRS rasters (degrees), pixel sizes are converted to metres
    using the standard approximation at the centre latitude of the raster.

    The slope raster is saved at *slope_path* with the same CRS and transform
    as the source DEM.

    Parameters
    ----------
    dem_path:
        Path to a GeoTIFF Digital Elevation Model.
    slope_path:
        Destination path for the output slope raster (degrees).

    Returns
    -------
    Path
        The path to the written slope raster.

    Raises
    ------
    FileNotFoundError
        If *dem_path* does not exist.

    Notes
    -----
    Memory: Reads the full DEM into RAM as a float32 array. For a CONUS-wide
    30m DEM (~1-2 GB), this requires 2-4 GB of available RAM. For 10m DEMs,
    use a tiled approach (e.g. rasterio windowed reads with overlap). This
    implementation is optimised for the 30m resolution NLCD/3DEP products.
    """
    dem_path = Path(dem_path)
    slope_path = Path(slope_path)

    if not dem_path.exists():
        raise FileNotFoundError(
            f"DEM not found: {dem_path}\n"
            "Run download_raster() or place the DEM file manually."
        )

    if slope_path.exists():
        logger.info("Slope raster already exists — skipping computation: %s", slope_path)
        return slope_path

    logger.info("Computing slope raster from DEM: %s", dem_path)
    slope_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(dem_path) as src:
        elevation = src.read(1).astype(np.float32)
        transform = src.transform
        crs = src.crs
        nodata_in = src.nodata
        bounds = src.bounds

        # Replace nodata with NaN before gradient computation so nodata pixels
        # don't contaminate neighbouring slope values.
        if nodata_in is not None:
            elevation[elevation == nodata_in] = np.nan

        # --- Cell size in metres ---
        if crs and crs.is_geographic:
            # CRS is in degrees (e.g. EPSG:4326). Convert to metres.
            center_lat = (bounds.top + bounds.bottom) / 2.0
            res_x_deg = abs(transform.a)
            res_y_deg = abs(transform.e)
            meters_per_deg_y = 111_319.5
            meters_per_deg_x = 111_319.5 * np.cos(np.radians(center_lat))
            res_x_m = res_x_deg * meters_per_deg_x
            res_y_m = res_y_deg * meters_per_deg_y
        else:
            # CRS is projected (metres). Use pixel size directly.
            res_x_m = abs(transform.a)
            res_y_m = abs(transform.e)

        # numpy.gradient(f, dy, dx):
        #   First return  → gradient along axis 0 (rows = Y = north-south)
        #   Second return → gradient along axis 1 (cols = X = east-west)
        dz_dy, dz_dx = np.gradient(elevation, res_y_m, res_x_m)
        slope_rad = np.arctan(np.sqrt(dz_dx ** 2 + dz_dy ** 2))
        slope_deg = np.degrees(slope_rad).astype(np.float32)

    _write_float_raster(slope_path, slope_deg, crs, transform, nodata=-9999.0)
    logger.info("Slope raster saved: %s", slope_path)
    return slope_path


def enrich_locations(
    df: pd.DataFrame,
    canopy_path: Path,
    dem_path: Path,
    landcover_path: Path,
    slope_path: Optional[Path] = None,
    batch_size: int = BATCH_SIZE,
) -> pd.DataFrame:
    """Add environmental columns to the locations DataFrame.

    Samples three rasters (canopy cover, DEM elevation, land cover) at each
    location's coordinates. Also pre-computes and samples a slope raster from
    the DEM.

    New columns added:
      - ``canopy_pct``       : tree canopy cover percentage (0–100, float)
      - ``elevation_m``      : terrain elevation in metres (float)
      - ``slope_deg``        : terrain slope in degrees (float)
      - ``land_cover_code``  : NLCD land cover integer code (float, NaN if unknown)

    Parameters
    ----------
    df:
        Validated locations DataFrame with ``latitude`` and ``longitude`` columns.
    canopy_path:
        Path to the NLCD tree canopy cover GeoTIFF.
    dem_path:
        Path to the USGS 3DEP DEM GeoTIFF.
    landcover_path:
        Path to the NLCD land cover classification GeoTIFF.
    slope_path:
        Destination for the pre-computed slope raster. Defaults to
        ``config.SLOPE_RASTER_PATH``.
    batch_size:
        Passed through to ``sample_raster_at_points``.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with the four new columns appended.
    """
    if slope_path is None:
        slope_path = SLOPE_RASTER_PATH

    lats = df["latitude"].to_numpy(dtype=np.float64)
    lons = df["longitude"].to_numpy(dtype=np.float64)
    n = len(df)

    logger.info("Enriching %d locations with environmental data…", n)

    # --- Canopy cover ---
    logger.info("Sampling canopy cover…")
    canopy = sample_raster_at_points(canopy_path, lats, lons, batch_size)

    # --- Elevation ---
    logger.info("Sampling elevation…")
    elevation = sample_raster_at_points(dem_path, lats, lons, batch_size)

    # --- Slope ---
    if not Path(slope_path).exists():
        logger.info("Slope raster not found — computing from DEM…")
        compute_slope_raster(dem_path, slope_path)

    logger.info("Sampling slope…")
    slope = sample_raster_at_points(slope_path, lats, lons, batch_size)

    # --- Land cover ---
    logger.info("Sampling land cover…")
    land_cover = sample_raster_at_points(landcover_path, lats, lons, batch_size)

    # --- Assemble result ---
    enriched = df.copy()
    enriched["canopy_pct"] = canopy
    enriched["elevation_m"] = elevation
    enriched["slope_deg"] = slope
    enriched["land_cover_code"] = land_cover

    # Log NaN rates — important for data quality awareness
    for col, arr in [
        ("canopy_pct", canopy),
        ("elevation_m", elevation),
        ("slope_deg", slope),
        ("land_cover_code", land_cover),
    ]:
        nan_count = int(np.isnan(arr).sum())
        if nan_count:
            logger.warning(
                "%s: %d / %d values are NaN (%.1f%%) — points outside raster extent",
                col,
                nan_count,
                n,
                nan_count / n * 100,
            )

    logger.info("Enrichment complete.")
    return enriched


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _verify_raster(path: Path) -> bool:
    """Return True if *path* is a readable GeoTIFF with at least one band."""
    try:
        with rasterio.open(path) as src:
            return src.count >= 1
    except Exception:
        return False


def _is_geographic_4326(crs: CRS) -> bool:
    """Return True if *crs* is a geographic (lat/lon) CRS close to EPSG:4326.

    Handles WGS84 variants (EPSG:4326, EPSG:4269, etc.) that all use
    degree-based coordinates.
    """
    return crs.is_geographic


def _is_zip(url: str, content_type: str, buffer: io.BytesIO) -> bool:
    """Return True if the content appears to be a ZIP archive."""
    if url.lower().endswith(".zip"):
        return True
    if "zip" in content_type.lower():
        return True
    # Check ZIP magic bytes (PK\x03\x04)
    header = buffer.read(4)
    buffer.seek(0)
    return header[:2] == b"PK"


def _extract_tif_from_zip(zip_buffer: io.BytesIO, dest_path: Path) -> None:
    """Extract the first .tif file from *zip_buffer* to *dest_path*."""
    with zipfile.ZipFile(zip_buffer) as zf:
        tif_names = [n for n in zf.namelist() if n.lower().endswith(".tif")]
        if not tif_names:
            raise ValueError(
                f"No .tif file found inside zip archive. Contents: {zf.namelist()}"
            )
        if len(tif_names) > 1:
            logger.warning(
                "Multiple .tif files in archive — using the first: %s", tif_names[0]
            )
        with zf.open(tif_names[0]) as src_file, open(dest_path, "wb") as dst_file:
            dst_file.write(src_file.read())
    logger.debug("Extracted %s → %s", tif_names[0], dest_path)


def _write_float_raster(
    path: Path,
    data: np.ndarray,
    crs: CRS,
    transform,
    nodata: float = -9999.0,
) -> None:
    """Write a float32 2D numpy array as a single-band GeoTIFF."""
    with rasterio.open(
        path,
        mode="w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=rasterio.float32,
        crs=crs,
        transform=transform,
        nodata=nodata,
        compress="lzw",     # LZW compression — lossless, standard for float rasters
    ) as dst:
        dst.write(data.astype(np.float32), 1)
