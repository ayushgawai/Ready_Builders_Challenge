"""
Configuration for LEO Satellite Coverage Risk Analysis pipeline.

All file paths, scoring thresholds, dataset URLs, and constants are
defined here. Nothing is hardcoded in the analysis modules — every
tunable value lives in this file.

Design decision: pathlib.Path throughout for OS-agnostic path handling.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Project Layout
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"
DOCS_DIR = PROJECT_ROOT / "docs"
SRC_DIR = PROJECT_ROOT / "src"

# Ensure runtime directories exist at import time.
# data/ is .gitignored so it must be created locally.
for _d in [RAW_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    _d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

ANTHROPIC_API_KEY: str | None = os.getenv("ANTHROPIC_API_KEY")


# ---------------------------------------------------------------------------
# Input Data
# ---------------------------------------------------------------------------

LOCATIONS_CSV = RAW_DIR / "DATA_CHALLENGE_50.csv"
SAMPLE_LOCATIONS_CSV = RAW_DIR / "locations_sample.csv"  # dev/test fixture

# Columns the pipeline expects to find in the locations CSV.
# Actual schema confirmed from DATA_CHALLENGE_50.csv (4.67M rows):
#   location_id, latitude, longitude, geoid_cb
# geoid_cb is a 15-digit Census block GEOID (e.g. 371790203162002).
# state (2-letter abbr) and county (5-digit FIPS) are DERIVED from it.
# No state/county columns are present in the source CSV.
EXPECTED_COLUMNS = ["location_id", "latitude", "longitude", "geoid_cb"]

# Columns we treat as critical — rows with nulls here are dropped entirely.
CRITICAL_COLUMNS = ["location_id", "latitude", "longitude"]

# Census GEOID column name — used to derive state and county FIPS.
GEOID_CB_COLUMN = "geoid_cb"


# ---------------------------------------------------------------------------
# CONUS Coordinate Bounds  (for input validation)
# ---------------------------------------------------------------------------
# Continental United States bounding box.
# Locations outside these bounds are flagged as out-of-range and dropped.

CONUS_LAT_MIN = 24.396308
CONUS_LAT_MAX = 49.384358
CONUS_LON_MIN = -124.848974
CONUS_LON_MAX = -66.885444


# ---------------------------------------------------------------------------
# Raster Data Sources
# ---------------------------------------------------------------------------
# NLCD = National Land Cover Database (USGS / Multi-Resolution Land
# Characteristics Consortium). Free, national, 30m resolution.

# NLCD Tree Canopy Cover 2021 — CONUS
CANOPY_RASTER_URL = (
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/metadata/"
    "nlcd_tcc_conus_2021_v2021-4.zip"
)
CANOPY_RASTER_PATH = RAW_DIR / "nlcd_tcc_conus_2021.tif"

# USGS 3DEP Digital Elevation Model — acquired per-tile via USGS National Map
# Full CONUS DEM is assembled from tiles; path below is the merged output.
DEM_RASTER_PATH = RAW_DIR / "dem_conus.tif"

# NLCD Land Cover Classification 2021 — CONUS
LANDCOVER_RASTER_URL = (
    "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/metadata/"
    "nlcd_land_cover_l48_2021_20230630.zip"
)
LANDCOVER_RASTER_PATH = RAW_DIR / "nlcd_landcover_conus_2021.tif"

# Derived products (computed from raw rasters, stored locally)
SLOPE_RASTER_PATH = PROCESSED_DIR / "slope_degrees.tif"


# ---------------------------------------------------------------------------
# NLCD Land Cover Code Mappings
# Reference: https://www.mrlc.gov/data/legends/national-land-cover-database-class-legend-and-description
# ---------------------------------------------------------------------------

FOREST_CODES: list[int] = [41, 42, 43]
# 41 = Deciduous Forest, 42 = Evergreen Forest, 43 = Mixed Forest

DEVELOPED_CODES: list[int] = [21, 22, 23, 24]
# 21 = Developed Open Space, 22 = Developed Low, 23 = Developed Medium,
# 24 = Developed High Intensity

OPEN_CODES: list[int] = [31, 52, 71, 81, 82]
# 31 = Barren Land, 52 = Shrub/Scrub, 71 = Grassland/Herbaceous,
# 81 = Pasture/Hay, 82 = Cultivated Crops

WATER_CODES: list[int] = [11, 12]
# 11 = Open Water, 12 = Perennial Ice/Snow


# ---------------------------------------------------------------------------
# Risk Scoring Thresholds
# ---------------------------------------------------------------------------
# Derived from Starlink Install Guide requirements:
# Dish needs 100-110° unobstructed FOV; even partial obstructions cause
# service interruptions (branches, poles, terrain edges).
#
# Methodology: weighted composite of canopy%, terrain slope, land cover type.
# See docs/analysis_rationale.md for full justification.

# Tree Canopy Cover (percent of 30m pixel covered by tree canopy)
CANOPY_HIGH_THRESHOLD: int = 50   # >50% → high risk score
CANOPY_MOD_THRESHOLD: int = 20    # 20–50% → moderate risk score

# Terrain Slope (degrees, derived from 3DEP DEM)
SLOPE_HIGH_THRESHOLD: int = 20    # >20° → high risk score
SLOPE_MOD_THRESHOLD: int = 10     # 10–20° → moderate risk score

# Composite Risk Weights  (must sum to 1.0)
WEIGHT_CANOPY: float = 0.50
WEIGHT_SLOPE: float = 0.30
WEIGHT_LANDCOVER: float = 0.20

# Risk Tier Cutoffs (applied to composite score 0.0 – 1.0)
RISK_HIGH_THRESHOLD: float = 0.6
RISK_MOD_THRESHOLD: float = 0.3


# ---------------------------------------------------------------------------
# Risk Tier Labels
# ---------------------------------------------------------------------------

RISK_TIER_HIGH = "HIGH"
RISK_TIER_MODERATE = "MODERATE"
RISK_TIER_LOW = "LOW"
RISK_TIER_UNKNOWN = "UNKNOWN"   # Used when scoring inputs are NaN


# ---------------------------------------------------------------------------
# Agent Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL = "claude-opus-4-5"
# claude-opus-4-5 is used here for its strong multi-step reasoning.
# Swap to claude-haiku-4-5 for faster/cheaper runs during dev.
# Model names are centralised here so a one-line change updates the whole pipeline.

MAX_AGENT_TURNS = 20
# Safety ceiling: stop the agent loop after this many turns regardless.
# At 20 turns the full 5-tool pipeline has ample headroom with retries.


# ---------------------------------------------------------------------------
# Processing Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 50_000
# Number of locations processed per batch during raster sampling.
# DATA_CHALLENGE_50.csv has ~4.67M locations (not ~1M as originally estimated).
# At 4.67M rows, BATCH_SIZE=50K → ~93 batches. Tune down on low-memory machines.


# ---------------------------------------------------------------------------
# Validation / Anomaly Detection Thresholds
# ---------------------------------------------------------------------------

CANOPY_IMPOSSIBLE_MAX: int = 100    # canopy_pct > 100 → impossible value
SLOPE_IMPOSSIBLE_MIN: float = 0.0   # slope_deg < 0 → impossible value
SLOPE_IMPOSSIBLE_MAX: float = 90.0  # slope_deg > 90 → impossible value

# If one risk tier accounts for more than this fraction of results, flag
# as suspicious (may indicate raster sampling failure or scoring bug).
DOMINANT_TIER_THRESHOLD: float = 0.90

# Cross-validation: if land_cover = Forest but canopy_pct < this, flag as anomaly.
FOREST_LOW_CANOPY_THRESHOLD: int = 5


# ---------------------------------------------------------------------------
# Output / Reporting
# ---------------------------------------------------------------------------

DATA_QUALITY_REPORT_PATH = OUTPUT_DIR / "data_quality_report.txt"
ANOMALY_REPORT_PATH = OUTPUT_DIR / "anomaly_report.txt"
FINDINGS_REPORT_PATH = OUTPUT_DIR / "findings_report.md"
RISK_DISTRIBUTION_CHART_PATH = OUTPUT_DIR / "risk_distribution.png"
STATIC_MAP_PATH = OUTPUT_DIR / "risk_map_static.png"
INTERACTIVE_MAP_PATH = OUTPUT_DIR / "risk_map_interactive.html"
SCORED_LOCATIONS_PATH = PROCESSED_DIR / "locations_scored.csv"
MONITORING_REPORT_PATH = OUTPUT_DIR / "agent_monitoring_report.json"
