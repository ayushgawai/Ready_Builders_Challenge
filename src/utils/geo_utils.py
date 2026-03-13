"""
Geographic helper utilities reused across the pipeline.

These functions abstract common geospatial operations so they are not
duplicated across environment.py, risk_scoring.py, and validation.py.
"""

import numpy as np
from rasterio.crs import CRS
from rasterio.transform import Affine


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
