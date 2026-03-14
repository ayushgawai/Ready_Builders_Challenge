# AI Tool Disclosure

This document lists every AI tool used in building this project, what each was used for, and cases where I diverged from AI-generated output.

---

## Tools Used

| Tool | Provider | Used For |
|---|---|---|
| **Claude (Anthropic API)** | Anthropic | Core agent in the pipeline — orchestrates tool calls, reasons about data quality, explains findings |
| **Claude Opus (via Cursor)** | Anthropic / Cursor | Architecture planning, master build plan design, methodology reasoning |
| **Cursor IDE** | Cursor AI | Code generation, file scaffolding, refactoring assistance |
| **Nominatim (OpenStreetMap)** | OpenStreetMap / `geopy` library | Free address geocoding for the `analyze_location` on-demand tool — converts user-entered addresses to WGS-84 coordinates |
| **pygris** | US Census Bureau (via Python library) | Fetches Census TIGER/Line cartographic boundary files to look up county NAMELSAD names from FIPS codes — no hardcoded dictionary |
| **us** | Python library | Authoritative US state FIPS code ↔ abbreviation ↔ full name lookups |

---

## Detailed Usage

### Claude API (in-pipeline)

The Anthropic Claude API is the agent brain of this pipeline. It:
- Decides the order and parameters of tool calls
- Interprets tool results and reasons about data quality
- Handles failures and decides whether to retry or abort
- Generates the final natural-language findings summary

**Interactive (UI chat) vs batch:** When the UI chat or CLI `--mode interactive` is used, the agent is given only **on-demand tools** (`analyze_location`, `assess_area`, `assess_polygon`, `query_top_counties`). Batch pipeline tools (ingest, sample_environment, score_risk, etc.) are not exposed in that mode, so chat never triggers raster sampling or re-downloads — it only uses the existing scored CSV. This keeps chat fast and avoids the agent incorrectly choosing heavy batch steps for questions like "which county has the most low-risk locations?" (answered via `query_top_counties`) or "risk in this bbox" (answered via `assess_polygon`).

This is not a "wrapper around Claude to generate code" — Claude is an active runtime component of the system.

### Claude Opus (planning phase)

Used during the design phase to produce a detailed internal build plan that outlined:
- Architecture decisions with tradeoffs
- Risk scoring methodology derived from the Starlink install guide
- Phased build plan with per-phase deliverables
- Production considerations and known limitations

### Cursor IDE

Used for implementation assistance during the coding phases:
- Generating boilerplate for Python modules
- Suggesting docstring formats
- Code review and linting assistance

### Nominatim (OpenStreetMap geocoder, via geopy)

Used at runtime in the `analyze_location` on-demand tool to convert free-text US addresses to WGS-84 coordinates. This is the standard free geocoder — no API key required. Rate limited to 1 request/second per OpenStreetMap terms of service, which is acceptable for single-location interactive queries.

Chosen over: Google Geocoding API (requires billing), ArcGIS World Geocoder (requires account), HERE API (paid). All alternatives require API keys or billing; Nominatim is free and sufficient for this use case.

### pygris + us (Python libraries)

Used to convert Census block GEOIDs (`geoid_cb` column) to human-readable `state` abbreviations and `county` NAMELSAD names (e.g. `"Santa Clara County"`). Replaced hardcoded FIPS→name dictionaries. Both libraries source their data from authoritative US Census Bureau files.

---

## Cases Where I Diverged from AI Output

### 1. Model selection in config.py

**AI suggested:** `claude-sonnet-4-20250514`  
**What I did:** Changed to `claude-opus-4-5` based on current available model names and ensuring the agent loop has strong multi-step reasoning for the validation and failure-handling logic.  
**Why:** The suggested model name did not match a valid Anthropic API model identifier. Accuracy of the model name is critical — a wrong name causes a hard runtime failure.

### 2. Risk weight for Land Cover

**AI's initial suggestion:** Equal 33%/33%/33% weights for canopy, slope, and land cover.  
**What I did:** Used 50%/30%/20% as specified in the build plan.  
**Why:** The Starlink install guide text is explicit that tree branches are the most common cause of interruptions. Terrain slope matters but can often be mitigated by dish mounting height. Equal weighting would overstate the influence of land cover, which is fundamentally a corroborating signal rather than a direct obstruction measure.

### 3. PostGIS vs. rasterio

**AI suggested:** Including PostGIS as a geospatial processing option.  
**What I did:** Excluded PostGIS entirely; rasterio only.  
**Why:** The core operation is sampling raster cell values at point coordinates. This is a raster operation. PostGIS is designed for spatial joins between vector datasets. Adding a database server for a raster sampling task introduces infrastructure complexity with zero analytical benefit in this context.

---

### 4. Schema discovery — geoid_cb vs state/county

**Challenge spec stated:** CSV would contain `state` and `county` columns.  
**What the actual file had:** A `geoid_cb` column (Census block GEOID) instead.  
**What I did:** Wrote a two-stage enrichment: parse `geoid_cb` to get state FIPS → `us.states.lookup().abbr` for state abbreviation; county FIPS → `pygris` Census TIGER data for county NAMELSAD name. Reverse geocoder used only as fallback for rows with missing/malformed GEOID.  
**Why:** The actual data was authoritative — trusting GEOID over reverse geocoding for 99%+ of rows is more accurate and faster.

### 5. county column content — NAMELSAD vs stripped name vs FIPS

**AI suggested:** Strip "County" suffix from NAMELSAD (e.g., `"Santa Clara"` not `"Santa Clara County"`).  
**What I did:** Kept NAMELSAD as-is from the Census library (e.g., `"Santa Clara County"`, `"Orleans Parish"`).  
**Why:** The library output is the authoritative form. Manual suffix stripping introduces edge cases (Parish, Borough, Census Area in Alaska) and creates a maintenance burden. The challenge output reviewers will recognise standard NAMELSAD format.

*Last updated: March 2026*
