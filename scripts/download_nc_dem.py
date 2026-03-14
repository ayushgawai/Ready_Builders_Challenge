"""
Download a 30m (1 arc-second) DEM for North Carolina from the USGS 3DEP REST API.

Uses the ImageServer exportImage endpoint to download the NC extent in 5 column
tiles (each ≤ 8000 pixels wide — the API hard limit), then merges them into a
single GeoTIFF at data/raw/dem_conus.tif.

Run from the project root:
    python scripts/download_nc_dem.py

No authentication required. The USGS 3DEP dynamic service is public.
Estimated download time: 2–5 minutes on a fast connection.
Estimated output size: ~250 MB.
"""

from __future__ import annotations

import sys
import time
import tempfile
import urllib.request
import urllib.parse
from pathlib import Path

import rasterio
from rasterio.merge import merge
from rasterio.crs import CRS

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import DEM_RASTER_PATH
from src.utils.logging_config import setup_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Full NC bounding box — data spans lat 33.84–36.59, lon -84.32 to -75.46.
# Using 0.1° buffer on each side. LAT_MAX must reach 36.65 to include all northern NC
# (Surry, Stokes, Caswell, Person, Granville, Vance, Warren, Northampton counties).
NC_LAT_MIN = 33.75
NC_LAT_MAX = 36.65
NC_LON_MIN = -84.50
NC_LON_MAX = -75.20

# USGS 3DEP ImageServer endpoint
THREEDEP_URL = (
    "https://elevation.nationalmap.gov/arcgis/rest/services/"
    "3DEPElevation/ImageServer/exportImage"
)

# 1 arc-second (30m) = 1/3600 degree per pixel
# API hard limit is 8000×8000 pixels, but large tiles cause 504 Gateway Timeout.
# 3000 pixels = 0.833° per tile — small enough for the server to render quickly.
PIXELS_PER_DEGREE = 3600   # 1 arc-second resolution
API_MAX_PIXELS = 2000       # confirmed max: 3000 triggers HTTP 500 "Error exporting image"; 2000 tested OK
TILE_TIMEOUT_SECS = 120     # 2-minute timeout per tile download
TILE_MAX_RETRIES = 3        # retry up to 3 times on failure


def _compute_tiles() -> list[dict]:
    """Split the NC extent into a 2-D grid of tiles that each fit within API_MAX_PIXELS.

    Both the longitude (width) and latitude (height) dimensions are capped.
    A 3000×3000 tile is ~9M pixels — well within the server's rendering budget.
    """
    step = API_MAX_PIXELS / PIXELS_PER_DEGREE   # degrees per tile side
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
    """Return (width, height) in pixels for a tile at 1 arc-second resolution."""
    w = round((tile["lon_max"] - tile["lon_min"]) * PIXELS_PER_DEGREE)
    h = round((tile["lat_max"] - tile["lat_min"]) * PIXELS_PER_DEGREE)
    return min(w, API_MAX_PIXELS), min(h, API_MAX_PIXELS)


def _download_tile(tile: dict, out_path: Path) -> None:
    """Download one DEM tile from the USGS 3DEP exportImage API."""
    width, height = _pixel_dims(tile)
    bbox = f"{tile['lon_min']},{tile['lat_min']},{tile['lon_max']},{tile['lat_max']}"

    # Only the four parameters the ImageServer endpoint reliably accepts.
    # pixelType / noData / noDataInterpretation / interpolation all trigger HTTP 500
    # when combined with imageSR reprojection — confirmed by testing.
    params = {
        "bbox": bbox,
        "bboxSR": "4326",
        "size": f"{width},{height}",
        "imageSR": "4326",
        "format": "tiff",
        "f": "image",
    }

    url = THREEDEP_URL + "?" + urllib.parse.urlencode(params)
    print(f"  Downloading tile: lon {tile['lon_min']:.2f}→{tile['lon_max']:.2f}  "
          f"({width}×{height} px)")

    # Use explicit timeout + retry — urlretrieve has no timeout and causes 504 hangs
    last_exc: Exception | None = None
    for attempt in range(1, TILE_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "LEO-Risk-Pipeline/1.0"}
            )
            with urllib.request.urlopen(req, timeout=TILE_TIMEOUT_SECS) as response:
                out_path.write_bytes(response.read())
            return   # success
        except Exception as exc:
            last_exc = exc
            if attempt < TILE_MAX_RETRIES:
                wait = 10 * attempt
                print(f"    Attempt {attempt} failed ({exc}). Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    All {TILE_MAX_RETRIES} attempts failed.")

    raise RuntimeError(
        f"Could not download DEM tile after {TILE_MAX_RETRIES} attempts. "
        f"Last error: {last_exc}"
    )


def download_nc_dem(output_path: Path = DEM_RASTER_PATH) -> Path:
    """
    Download and merge all NC DEM tiles into a single GeoTIFF.

    Returns:
        Path to the merged output file.
    """
    import logging
    setup_logging()
    logger = logging.getLogger(__name__)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        logger.info("DEM already exists at %s — skipping download.", output_path)
        print(f"DEM already exists: {output_path}")
        return output_path

    tiles = _compute_tiles()
    print(f"\nDownloading NC DEM in {len(tiles)} tiles at 30m (1 arc-second) resolution...")
    print(f"Coverage: lat {NC_LAT_MIN}–{NC_LAT_MAX}°N, lon {NC_LON_MIN}–{NC_LON_MAX}°W")
    print(f"Tile size: max {API_MAX_PIXELS}×{API_MAX_PIXELS} px  |  timeout: {TILE_TIMEOUT_SECS}s  |  retries: {TILE_MAX_RETRIES}")
    print()

    tile_paths = []
    with tempfile.TemporaryDirectory(prefix="nc_dem_") as tmpdir:
        # Download each tile
        for i, tile in enumerate(tiles, start=1):
            tile_path = Path(tmpdir) / f"dem_tile_{i}.tif"
            _download_tile(tile, tile_path)
            tile_size = tile_path.stat().st_size / 1e6
            print(f"    Saved: {tile_size:.1f} MB")
            tile_paths.append(tile_path)

        print(f"\nMerging {len(tile_paths)} tiles → {output_path} ...")

        # Open all tiles and merge
        open_files = [rasterio.open(p) for p in tile_paths]
        try:
            merged, merged_transform = merge(open_files)
        finally:
            for f in open_files:
                f.close()

        # Write merged output
        profile = open_files[0].profile.copy()
        profile.update(
            width=merged.shape[2],
            height=merged.shape[1],
            transform=merged_transform,
            crs=CRS.from_epsg(4326),
            nodata=-9999.0,
            dtype="float32",
            compress="lzw",
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(merged)

    size_mb = output_path.stat().st_size / 1e6
    print(f"\nDEM download complete.")
    print(f"Output: {output_path} ({size_mb:.0f} MB)")
    logger.info("NC DEM downloaded: %s (%.0f MB)", output_path, size_mb)
    return output_path


if __name__ == "__main__":
    download_nc_dem()
