"""
Anthropic tool_use schemas for the LEO Satellite Coverage Risk pipeline.

Each entry in PIPELINE_TOOLS follows the Anthropic API tool schema format:
  https://docs.anthropic.com/en/docs/tool-use

Design principles:
  - Descriptions are written for the *model*, not the developer. They tell
    Claude exactly what the tool does, what it returns, and when to call it.
  - Input schemas use JSON Schema with explicit types, descriptions, and
    required field markers — Claude uses these to construct valid tool calls.
  - Parameters are minimal: only what the agent actually needs to pass.
    File paths between tools are standardised (written to config paths) so
    Claude does not need to track them explicitly.
  - One tool per pipeline stage. Order is enforced by the system prompt in
    agent.py, not by the tool definitions themselves.

Tool groups
-----------
  PIPELINE_TOOLS (batch):
    ingest_locations → sample_environment → score_risk → validate_results
    → generate_report

  ON_DEMAND_TOOLS (interactive / single-location):
    analyze_location  — comprehensive single-point risk assessment
                        (factors + alternatives + seasonal context)
    assess_area       — state or county level risk briefing

  ALL_TOOLS = PIPELINE_TOOLS + ON_DEMAND_TOOLS (registered together in the
  agent so Claude can call any tool in any conversation mode).
"""

from typing import Any

# ---------------------------------------------------------------------------
# Tool 1: ingest_locations
# ---------------------------------------------------------------------------

_INGEST_LOCATIONS: dict[str, Any] = {
    "name": "ingest_locations",
    "description": (
        "Load and validate the locations CSV file. "
        "Performs five sequential data quality passes: "
        "(1) type coercion — non-numeric latitude/longitude values are detected and dropped; "
        "(2) null/blank critical columns — rows with null or whitespace-only location_id, "
        "latitude, or longitude are dropped; "
        "(3) CONUS coordinate bounds — rows outside the continental US bounding box are dropped; "
        "(4) duplicate location_ids — second and later occurrences are dropped, first is kept; "
        "(5) invalid state codes — rows with unrecognised 2-letter state codes are dropped. "
        "After validation, derives state (2-letter abbreviation) and county (NAMELSAD, e.g. "
        "'Santa Clara County') from the Census block GEOID column using the 'us' and 'pygris' "
        "libraries. Saves the validated dataset to disk. "
        "Returns a quality report with exact drop counts per reason and the retention rate. "
        "Call this tool FIRST, before any other tool."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "file_path": {
                "type": "string",
                "description": (
                    "Absolute or relative path to the input locations CSV file. "
                    "Expected columns: location_id, latitude, longitude, geoid_cb. "
                    "Defaults to the challenge dataset path if omitted."
                ),
            }
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 2: sample_environment
# ---------------------------------------------------------------------------

_SAMPLE_ENVIRONMENT: dict[str, Any] = {
    "name": "sample_environment",
    "description": (
        "Download the three national raster datasets (if not already present) and "
        "sample their values at each validated location's coordinates. "
        "Rasters used: "
        "(1) NLCD Tree Canopy Cover 2021 (30m GeoTIFF) — provides canopy_pct per location; "
        "(2) USGS 3DEP Digital Elevation Model (10-30m) — provides elevation_m; "
        "(3) NLCD Land Cover Classification 2021 (30m GeoTIFF) — provides land_cover_code. "
        "Slope (slope_deg) is derived from the DEM via numpy central-difference gradient, "
        "accounting for geographic pixel size at each latitude. "
        "This tool is IDEMPOTENT: if an enriched output file already exists and is newer "
        "than the validated input, raster sampling is skipped and the cached result is "
        "returned immediately. "
        "WARNING: Full processing of 4.67M locations takes 20-60 minutes depending on hardware. "
        "Saves the enriched dataset to disk. "
        "Call this tool AFTER ingest_locations succeeds."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "force_resample": {
                "type": "boolean",
                "description": (
                    "If true, ignore any existing cached enriched file and re-run "
                    "raster sampling from scratch. Default: false. Only set to true "
                    "if you have reason to believe the cached data is stale or corrupt."
                ),
            }
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 3: score_risk
# ---------------------------------------------------------------------------

_SCORE_RISK: dict[str, Any] = {
    "name": "score_risk",
    "description": (
        "Apply the LEO satellite connectivity risk scoring methodology to the "
        "enriched locations. "
        "Computes three individual risk scores (each 0.0–1.0): "
        "canopy_score (weight 50%) based on tree canopy cover percentage, "
        "slope_score (weight 30%) based on terrain slope in degrees, "
        "landcover_score (weight 20%) based on NLCD land cover class. "
        "Combines them into composite_score = (canopy*0.50) + (slope*0.30) + (landcover*0.20). "
        "Assigns risk_tier: HIGH (>=0.6), MODERATE (>=0.3), LOW (<0.3), or UNKNOWN (NaN inputs). "
        "Thresholds are defined in config.py and documented in docs/analysis_rationale.md. "
        "Saves the scored dataset to disk. "
        "Returns tier distribution with counts and percentages. "
        "Call this tool AFTER sample_environment succeeds."
    ),
    "input_schema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 4: validate_results
# ---------------------------------------------------------------------------

_VALIDATE_RESULTS: dict[str, Any] = {
    "name": "validate_results",
    "description": (
        "Run automated anomaly detection on the scored results. "
        "Performs four checks: "
        "(1) UNKNOWN rate — if >50% of scored rows are UNKNOWN tier (meaning raster sampling "
        "returned NaN for most locations), this is a CRITICAL failure indicating a raster "
        "download or CRS problem; "
        "(2) Tier dominance — if >=90% of scored rows share the same tier, this is CRITICAL "
        "and likely indicates a scoring bug or misconfigured threshold; "
        "(3) Impossible values — canopy_pct > 100 or slope_deg outside [0, 90] are CRITICAL, "
        "indicating raster data corruption; "
        "(4) Forest/canopy cross-validation — NLCD forest land cover (codes 41-43) with "
        "canopy_pct < 5% is a WARNING (known NLCD logging-lag artefact, not a pipeline error). "
        "Returns is_valid (True if no CRITICAL checks fail), severity, and an anomaly report. "
        "If is_valid is False, diagnose the issue before proceeding. "
        "If only warnings are present, proceed to generate_report. "
        "Call this tool AFTER score_risk."
    ),
    "input_schema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 5: generate_report
# ---------------------------------------------------------------------------

_GENERATE_REPORT: dict[str, Any] = {
    "name": "generate_report",
    "description": (
        "Generate the final analysis report and output artifacts. "
        "Produces: "
        "(1) Summary statistics — total locations, risk tier distribution (count + %), "
        "average canopy and slope per tier, breakdown by state (all states), "
        "top 20 highest-risk counties by HIGH-tier count; "
        "(2) A findings report (Markdown) saved to data/output/findings_report.md — "
        "includes key findings, top risk drivers, state summaries, and data quality notes; "
        "(3) Risk distribution chart (bar chart saved as PNG); "
        "(4) Static risk map (colour-coded scatter plot of CONUS locations by risk tier). "
        "Returns the summary statistics and paths to all output files. "
        "Call this tool LAST, after validate_results."
    ),
    "input_schema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 6: analyze_location (on-demand, single-point)
# ---------------------------------------------------------------------------

_ANALYZE_LOCATION: dict[str, Any] = {
    "name": "analyze_location",
    "description": (
        "Perform a comprehensive LEO satellite connectivity risk assessment for a SINGLE "
        "location specified by address or coordinates. "
        "This is the primary tool for field technicians and on-demand queries. "
        "Accepts: "
        "(a) a free-text US address (street address, city/state, or ZIP) — geocoded via "
        "    OpenStreetMap Nominatim (free, no API key); "
        "(b) a coordinate string such as '47.6062, -122.3321'; "
        "(c) explicit latitude and longitude as separate numbers. "
        "Returns a detailed risk breakdown with four components: "
        "(A) Factor analysis — composite_score, risk_tier, and the contribution of each "
        "    factor (canopy_pct, slope_deg, land_cover) to the final score, the primary "
        "    risk driver, and a plain-English recommendation for the installer; "
        "(B) Nearby alternatives — up to 10 lower-risk scored locations within the search "
        "    radius, sorted by distance, so the technician can consider nearby sites; "
        "(C) Seasonal context — if the location is in Deciduous Forest (NLCD code 41), "
        "    notes that NLCD captures peak-summer canopy and that winter installations "
        "    (November–March) may face lower effective obstruction; "
        "(D) Lookup source — whether the result came from the pre-scored CSV or was "
        "    computed on-the-fly from raster data. "
        "IMPORTANT: If the scored dataset has not been generated yet, on-the-fly scoring "
        "will be attempted if raster data is available; otherwise an actionable error is "
        "returned. "
        "Use this tool for any user question that names a specific location."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "address": {
                "type": "string",
                "description": (
                    "Free-text US address or place name to geocode, e.g. "
                    "'1600 Amphitheatre Pkwy, Mountain View, CA' or 'Seattle, WA'. "
                    "Also accepts a coordinate string such as '47.6062, -122.3321'. "
                    "Provide either this OR latitude+longitude, not both."
                ),
            },
            "latitude": {
                "type": "number",
                "description": "WGS-84 latitude in decimal degrees. Provide with longitude.",
            },
            "longitude": {
                "type": "number",
                "description": "WGS-84 longitude in decimal degrees. Provide with latitude.",
            },
            "radius_meters": {
                "type": "number",
                "description": (
                    "Radius in metres to search for nearby lower-risk alternatives. "
                    "Default 500. Increase for rural areas; decrease for dense urban areas."
                ),
            },
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 7: assess_area (on-demand, state/county level)
# ---------------------------------------------------------------------------

_ASSESS_AREA: dict[str, Any] = {
    "name": "assess_area",
    "description": (
        "Generate a risk briefing for a US state or county. "
        "Useful for program managers and ISPs reviewing broadband coverage at a "
        "geographic area level rather than a single location. "
        "Filters the scored dataset to the requested area and computes: "
        "(1) tier distribution — count and percentage of HIGH, MODERATE, LOW, UNKNOWN; "
        "(2) environmental averages — mean canopy_pct and slope_deg per tier; "
        "(3) top high-risk counties within the state (or top zip codes within the county); "
        "(4) primary risk driver — which factor (canopy vs slope vs land cover) accounts "
        "    for most HIGH-tier locations in this area; "
        "(5) plain-English briefing paragraph suitable for a programme status report. "
        "IMPORTANT: The scored dataset (data/processed/locations_scored.csv) must exist. "
        "Run the batch pipeline (ingest → sample → score → validate → report) first if "
        "it does not. "
        "Use this tool when the user names a state or county rather than a single address."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "state": {
                "type": "string",
                "description": (
                    "2-letter US state abbreviation, e.g. 'WA', 'CA', 'TX'. "
                    "Required if county is not provided."
                ),
            },
            "county": {
                "type": "string",
                "description": (
                    "County NAMELSAD as stored in the scored dataset, e.g. "
                    "'Whatcom County' or 'Santa Clara County'. "
                    "If provided together with state, filters to that specific county. "
                    "If omitted, returns a state-level briefing."
                ),
            },
            "top_n": {
                "type": "integer",
                "description": (
                    "Number of sub-areas (counties or locations) to list in the briefing. "
                    "Default 10."
                ),
            },
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Tool 8: query_top_counties (on-demand, read-only aggregation from scored CSV)
# ---------------------------------------------------------------------------

_QUERY_TOP_COUNTIES: dict[str, Any] = {
    "name": "query_top_counties",
    "description": (
        "Return the top N US counties by count of locations in a given risk tier. "
        "Reads only the existing scored dataset (no sampling or downloads). "
        "Use this for questions like 'which county has the highest number of low risk "
        "locations?' or 'top counties by HIGH risk count'. "
        "Fast and cache-friendly; always use this instead of running the batch pipeline."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "state": {
                "type": "string",
                "description": (
                    "2-letter US state abbreviation, e.g. 'NC', 'WA'. Required."
                ),
            },
            "tier": {
                "type": "string",
                "enum": ["HIGH", "MODERATE", "LOW"],
                "description": (
                    "Risk tier to rank counties by. Use LOW for 'most low-risk locations', "
                    "HIGH for 'most high-risk locations'. Default LOW."
                ),
            },
            "top_n": {
                "type": "integer",
                "description": "Number of top counties to return. Default 15.",
            },
        },
        "required": ["state"],
    },
}

# ---------------------------------------------------------------------------
# Tool 9: assess_polygon (on-demand, custom bbox or polygon)
# ---------------------------------------------------------------------------

_ASSESS_POLYGON: dict[str, Any] = {
    "name": "assess_polygon",
    "description": (
        "Risk briefing for a custom geographic area defined by a bounding box or polygon. "
        "Use when the user asks for 'risk in this area', 'risk in this polygon', or gives "
        "min/max lat/lon (e.g. a rectangle). Reads only the scored CSV; no sampling. "
        "Either pass bbox (min_lat, max_lat, min_lon, max_lon) or coordinates as a list of "
        "[lat, lon] points forming a polygon (first and last point will be closed)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "min_lat": {"type": "number", "description": "Minimum latitude (south bound). Use with max_lat, min_lon, max_lon for bbox."},
            "max_lat": {"type": "number", "description": "Maximum latitude (north bound)."},
            "min_lon": {"type": "number", "description": "Minimum longitude (west bound)."},
            "max_lon": {"type": "number", "description": "Maximum longitude (east bound)."},
            "coordinates": {
                "type": "array",
                "items": {"type": "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 2},
                "description": "Optional. List of [lat, lon] points forming polygon. If provided, overrides bbox; points must be in order (e.g. clockwise).",
            },
            "label": {"type": "string", "description": "Optional label for the area (e.g. 'Downtown Raleigh')."},
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Exported lists — passed directly to the Anthropic API messages.create() call
# ---------------------------------------------------------------------------

PIPELINE_TOOLS: list[dict[str, Any]] = [
    _INGEST_LOCATIONS,
    _SAMPLE_ENVIRONMENT,
    _SCORE_RISK,
    _VALIDATE_RESULTS,
    _GENERATE_REPORT,
]

ON_DEMAND_TOOLS: list[dict[str, Any]] = [
    _ANALYZE_LOCATION,
    _ASSESS_AREA,
    _ASSESS_POLYGON,
    _QUERY_TOP_COUNTIES,
]

ALL_TOOLS: list[dict[str, Any]] = PIPELINE_TOOLS + ON_DEMAND_TOOLS

# Map tool names to their schemas — used by agent.py for lookup
TOOL_REGISTRY: dict[str, dict[str, Any]] = {t["name"]: t for t in ALL_TOOLS}
