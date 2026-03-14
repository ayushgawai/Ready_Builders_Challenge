"""
Geocoding and spatial search utilities for on-demand location analysis.

Provides:
  resolve_location     — accepts address string OR "lat,lon" OR lat/lon floats
                         and returns (lat, lon).  Address input is geocoded via
                         Nominatim (OpenStreetMap, free, no API key required).
  find_nearest_scored  — finds the closest pre-scored location in the scored CSV
                         within a configurable distance tolerance.
  find_alternatives    — finds lower-risk scored locations within a buffer radius.
  haversine_m          — point-to-point distance in metres (scalar).
  haversine_vec        — vectorised haversine for pandas Series (returns np.ndarray).

Design notes
------------
- Nominatim is rate-limited to 1 req/sec for the free tier (per OSM policy).
  For single-location interactive queries this is not a problem.
  A custom user_agent is required by Nominatim's terms of service.
- Haversine is accurate to <0.5% across CONUS latitudes (24-49°N), which is
  sufficient for the 50-500m buffers we use in this pipeline.
- The scored CSV (4.67M rows) is not loaded here; callers pass a pre-loaded
  DataFrame.  The PipelineAgent caches the DataFrame after first load to avoid
  re-reading the file on every query.
"""

from __future__ import annotations

import logging
import math
import re

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Nominatim user-agent — required by OpenStreetMap terms of service.
_NOMINATIM_USER_AGENT = "leo-satellite-risk-pipeline/1.0 (ready-builders-challenge)"

# Regex that matches bare "lat, lon" strings before we try geocoding.
_COORD_RE = re.compile(
    r"^\s*(-?\d{1,3}(?:\.\d+)?)\s*[,\s]\s*(-?\d{1,3}(?:\.\d+)?)\s*$"
)


# ---------------------------------------------------------------------------
# Distance helpers
# ---------------------------------------------------------------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in metres between two WGS-84 points."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def haversine_vec(
    lat: float, lon: float, df_lat: pd.Series, df_lon: pd.Series
) -> np.ndarray:
    """Vectorised haversine: distance in metres from (lat, lon) to every row."""
    R = 6_371_000.0
    phi1 = math.radians(lat)
    phi2 = np.radians(df_lat.values)
    dphi = np.radians(df_lat.values - lat)
    dlam = np.radians(df_lon.values - lon)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


# ---------------------------------------------------------------------------
# Input resolution
# ---------------------------------------------------------------------------

def resolve_location(
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> tuple[float, float] | None:
    """Resolve a location from address string or explicit coordinates.

    Accepts any of:
      - address="1600 Amphitheatre Pkwy, Mountain View, CA"
      - address="37.4220, -122.0841"   (parsed as coordinates, no geocoding)
      - latitude=37.4220, longitude=-122.0841
      - address="Seattle, WA"

    Parameters
    ----------
    address:
        Free-text address or "lat, lon" string.
    latitude, longitude:
        Explicit numeric coordinates (override address if provided together).

    Returns
    -------
    tuple[float, float] or None
        (latitude, longitude) in WGS-84 decimal degrees, or None on failure.
    """
    # Explicit numeric coordinates take priority
    if latitude is not None and longitude is not None:
        return (float(latitude), float(longitude))

    if not address:
        return None

    # Try to parse "lat, lon" or "lat lon" before calling the geocoder
    m = _COORD_RE.match(address)
    if m:
        return (float(m.group(1)), float(m.group(2)))

    # Address geocoding via Nominatim (OpenStreetMap, free, no API key)
    return _geocode_nominatim(address)


def _geocode_nominatim(address: str) -> tuple[float, float] | None:
    """Geocode an address string using Nominatim (OSM).

    Rate-limited to 1 request per second per Nominatim's terms.
    Returns None on failure (timeout, service error, or no result found).
    """
    try:
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
    except ImportError:
        logger.error(
            "geopy is not installed — address geocoding unavailable. "
            "Run: pip install geopy"
        )
        return None

    geolocator = Nominatim(user_agent=_NOMINATIM_USER_AGENT)
    try:
        logger.info("Geocoding address via Nominatim: '%s'", address)
        result = geolocator.geocode(address, country_codes="us", timeout=10)
        if result:
            logger.info(
                "Geocoded '%s' → (%.6f, %.6f) — '%s'",
                address, result.latitude, result.longitude, result.address,
            )
            return (result.latitude, result.longitude)
        else:
            logger.warning("Nominatim returned no result for: '%s'", address)
            return None
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as exc:
        logger.warning("Geocoding failed for '%s': %s", address, exc)
        return None


# ---------------------------------------------------------------------------
# Scored CSV spatial search
# ---------------------------------------------------------------------------

def find_nearest_scored(
    lat: float,
    lon: float,
    scored_df: pd.DataFrame,
    max_distance_m: float = 150.0,
) -> dict | None:
    """Find the closest scored location within max_distance_m metres.

    Uses a bounding-box pre-filter (O(1) pandas boolean index) before
    computing exact haversine distances on the small candidate set.

    Parameters
    ----------
    lat, lon:
        Query point in WGS-84 decimal degrees.
    scored_df:
        Pre-loaded scored DataFrame (caller is responsible for loading).
    max_distance_m:
        Maximum match distance in metres.  Default 150m — roughly half a
        30m raster pixel, so any pre-scored location within the same pixel
        will be found.

    Returns
    -------
    dict or None
        Row dict with an added ``distance_m`` key, or None if no match.
    """
    # Bounding box: 0.002° ≈ 222m at CONUS latitudes — generous margin for max_distance_m ≤ 500m
    margin = max(max_distance_m / 111_320 * 1.5, 0.002)
    candidates = scored_df[
        (scored_df["latitude"].between(lat - margin, lat + margin))
        & (scored_df["longitude"].between(lon - margin * 1.3, lon + margin * 1.3))
    ]

    if candidates.empty:
        return None

    distances = haversine_vec(lat, lon, candidates["latitude"], candidates["longitude"])
    min_idx = int(np.argmin(distances))
    min_dist = float(distances[min_idx])

    if min_dist > max_distance_m:
        return None

    row = candidates.iloc[min_idx].to_dict()
    row["distance_m"] = round(min_dist, 1)
    return row


def find_alternatives(
    lat: float,
    lon: float,
    scored_df: pd.DataFrame,
    radius_m: float = 500.0,
    current_tier: str = "HIGH",
    max_results: int = 10,
) -> list[dict]:
    """Find lower-risk scored locations within radius_m metres.

    Parameters
    ----------
    lat, lon:
        Centre of the search circle.
    scored_df:
        Pre-loaded scored DataFrame.
    radius_m:
        Search radius in metres.  Default 500m.
    current_tier:
        The risk tier of the query location.  Only locations with a LOWER
        tier (e.g. MODERATE or LOW for a HIGH-risk query) are returned.
    max_results:
        Maximum number of alternatives to return.  Default 10.

    Returns
    -------
    list[dict]
        Sorted by distance ascending.  Each entry includes location_id,
        distance_m, risk_tier, canopy_pct, composite_score, state, county.
    """
    _TIER_ORDER = {"LOW": 0, "MODERATE": 1, "HIGH": 2, "UNKNOWN": 3}
    current_order = _TIER_ORDER.get(current_tier, 3)

    margin = radius_m / 111_320 * 1.5
    candidates = scored_df[
        (scored_df["latitude"].between(lat - margin, lat + margin))
        & (scored_df["longitude"].between(lon - margin * 1.3, lon + margin * 1.3))
        & (scored_df["risk_tier"].map(lambda t: _TIER_ORDER.get(t, 3)) < current_order)
    ].copy()

    if candidates.empty:
        return []

    distances = haversine_vec(lat, lon, candidates["latitude"], candidates["longitude"])
    candidates = candidates.copy()
    candidates["_dist"] = distances
    candidates = candidates[candidates["_dist"] <= radius_m].sort_values("_dist")

    results = []
    for _, row in candidates.head(max_results).iterrows():
        results.append({
            "location_id": row.get("location_id"),
            "distance_m": round(float(row["_dist"])),
            "risk_tier": row.get("risk_tier"),
            "canopy_pct": round(float(row["canopy_pct"]), 1) if pd.notna(row.get("canopy_pct")) else None,
            "slope_deg": round(float(row["slope_deg"]), 1) if pd.notna(row.get("slope_deg")) else None,
            "composite_score": round(float(row["composite_score"]), 3) if pd.notna(row.get("composite_score")) else None,
            "state": row.get("state"),
            "county": row.get("county"),
        })
    return results
