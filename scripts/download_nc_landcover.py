"""
Download Annual NLCD 2021 Land Cover for North Carolina via the MRLC WCS endpoint.

Uses the USGS EROS dmsdata WCS service (no email/account required).
Tiles the NC extent into 2000×2000 pixel chunks, downloads each tile,
then merges them into a single GeoTIFF at data/raw/landcover_conus.tif.

NLCD class codes returned (same as pipeline expects):
  11=Water, 21-24=Developed, 31=Barren, 41-43=Forest,
  52=Shrub, 71=Grassland, 81-82=Agriculture, 90/95=Wetlands

Run from the project root:
    python scripts/download_nc_landcover.py

No authentication required. The MRLC WCS service is public.
Estimated download time: 3–6 minutes.
Estimated output size: ~100–150 MB (land cover is integer, compresses well).
"""

from __future__ import annotations

import sys
import time
import tempfile
import urllib.request
import urllib.parse
from pathlib import Path

import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.crs import CRS

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import LANDCOVER_RASTER_PATH
from src.utils.logging_config import setup_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Full NC bounding box — data spans lat 33.84–36.59, lon -84.32 to -75.46.
# LAT_MAX must reach 36.65 to cover northern NC border counties with Virginia.
NC_LAT_MIN = 33.75
NC_LAT_MAX = 36.65
NC_LON_MIN = -84.50
NC_LON_MAX = -75.20

# MRLC Annual NLCD WCS endpoint (USGS EROS public service)
WCS_BASE = (
    "https://dmsdata.cr.usgs.gov/geoserver/"
    "mrlc_Land-Cover-Native_conus_year_data/wcs"
)
WCS_COVERAGE = (
    "mrlc_Land-Cover-Native_conus_year_data:Land-Cover-Native_conus_year_data"
)
NLCD_YEAR = 2021

# 8000×8000 tested and confirmed working (11s/tile). NC fits in ~5 tiles total.
# 2000px was slow (~75s/tile) — WCS throttles smaller repeated requests.
PIXELS_PER_DEGREE = 3600   # 30m = 1 arc-second
API_MAX_PIXELS = 8000
TILE_TIMEOUT_SECS = 120
TILE_MAX_RETRIES = 3


def _compute_tiles() -> list[dict]:
    """Split NC extent into 2-D grid, each tile ≤ API_MAX_PIXELS."""
    step = API_MAX_PIXELS / PIXELS_PER_DEGREE
    tiles = []
    lat = NC_LAT_MIN
    while lat < NC_LAT_MAX:
        lat_end = min(lat + step, NC_LAT_MAX)
        lon = NC_LON_MIN
        while lon < NC_LON_MAX:
            lon_end = min(lon + step, NC_LON_MAX)
            tiles.append({
                "lat_min": lat, "lat_max": lat_end,
                "lon_min": lon, "lon_max": lon_end,
            })
            lon = lon_end
        lat = lat_end
    return tiles


def _pixel_dims(tile: dict) -> tuple[int, int]:
    """Return (width, height) capped at API_MAX_PIXELS."""
    w = round((tile["lon_max"] - tile["lon_min"]) * PIXELS_PER_DEGREE)
    h = round((tile["lat_max"] - tile["lat_min"]) * PIXELS_PER_DEGREE)
    return min(w, API_MAX_PIXELS), min(h, API_MAX_PIXELS)


def _download_tile(tile: dict, out_path: Path) -> None:
    """Download one NLCD tile via WCS GetCoverage."""
    width, height = _pixel_dims(tile)
    bbox = (
        f"{tile['lon_min']},{tile['lat_min']},"
        f"{tile['lon_max']},{tile['lat_max']}"
    )

    params = {
        "SERVICE": "WCS",
        "VERSION": "1.0.0",
        "REQUEST": "GetCoverage",
        "COVERAGE": WCS_COVERAGE,
        "BBOX": bbox,
        "CRS": "EPSG:4326",
        "RESPONSE_CRS": "EPSG:4326",
        "WIDTH": str(width),
        "HEIGHT": str(height),
        "FORMAT": "GeoTIFF",
        "TIME": str(NLCD_YEAR),
    }

    url = WCS_BASE + "?" + urllib.parse.urlencode(params)
    print(f"  Tile: lon {tile['lon_min']:.2f}→{tile['lon_max']:.2f}  "
          f"({width}×{height} px)")

    last_exc: Exception | None = None
    for attempt in range(1, TILE_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "LEO-Risk-Pipeline/1.0"}
            )
            with urllib.request.urlopen(req, timeout=TILE_TIMEOUT_SECS) as resp:
                data = resp.read()
            # Sanity check: reject HTML error pages
            if data[:4] != b"II*\x00" and data[:4] != b"MM\x00*":
                raise ValueError(
                    f"Response is not a TIFF (got {data[:50]!r})"
                )
            out_path.write_bytes(data)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < TILE_MAX_RETRIES:
                wait = 10 * attempt
                print(f"    Attempt {attempt} failed ({exc}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    All {TILE_MAX_RETRIES} attempts failed.")

    raise RuntimeError(
        f"Could not download NLCD tile after {TILE_MAX_RETRIES} attempts. "
        f"Last error: {last_exc}"
    )


def download_nc_landcover(output_path: Path = LANDCOVER_RASTER_PATH) -> Path:
    """
    Download and merge all NC NLCD 2021 land cover tiles.

    Returns:
        Path to the merged output GeoTIFF.
    """
    import logging
    setup_logging()
    logger = logging.getLogger(__name__)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        logger.info("Land cover already exists at %s — skipping.", output_path)
        print(f"Land cover already exists: {output_path}")
        return output_path

    tiles = _compute_tiles()
    print(f"\nDownloading Annual NLCD {NLCD_YEAR} Land Cover for NC")
    print(f"Source: MRLC WCS (no email/account required)")
    print(f"Coverage: lat {NC_LAT_MIN}–{NC_LAT_MAX}°N, lon {NC_LON_MIN}–{NC_LON_MAX}°W")
    print(f"Tiles: {len(tiles)}  |  max {API_MAX_PIXELS}×{API_MAX_PIXELS} px each")
    print(f"Output: {output_path}\n")

    tile_paths = []
    with tempfile.TemporaryDirectory(prefix="nc_lc_") as tmpdir:
        for i, tile in enumerate(tiles, start=1):
            tile_path = Path(tmpdir) / f"lc_tile_{i}.tif"
            _download_tile(tile, tile_path)
            tile_size = tile_path.stat().st_size / 1e6
            print(f"    Saved: {tile_size:.1f} MB")
            tile_paths.append(tile_path)

        print(f"\nMerging {len(tile_paths)} tiles → {output_path} ...")

        open_files = [rasterio.open(p) for p in tile_paths]
        try:
            merged, merged_transform = merge(open_files)
        finally:
            for f in open_files:
                f.close()

        profile = open_files[0].profile.copy()
        profile.update(
            width=merged.shape[2],
            height=merged.shape[1],
            transform=merged_transform,
            crs=CRS.from_epsg(4326),
            dtype="uint8",       # NLCD codes 11–95 fit in uint8
            nodata=0,
            compress="lzw",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )
        # Cast to uint8 (WCS may return int16 or float32)
        merged_uint8 = merged.astype(np.uint8)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(merged_uint8)

    size_mb = output_path.stat().st_size / 1e6
    print(f"\nLand cover download complete.")
    print(f"Output: {output_path}  ({size_mb:.0f} MB)")
    logger.info("NC land cover downloaded: %s (%.0f MB)", output_path, size_mb)
    return output_path


if __name__ == "__main__":
    download_nc_landcover()
