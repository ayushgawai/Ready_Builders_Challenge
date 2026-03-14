# Run & Setup Guide

**Challenge:** [Ready.net Builders Challenge #50](https://github.com/ready/builders-challenge/issues/50) — LEO Satellite Coverage Risk Analysis.

---

## What This Is

An agent-driven pipeline that scores ~4.67M North Carolina broadband locations (HIGH / MODERATE / LOW risk) for LEO satellite connectivity using three national datasets (tree canopy, terrain slope, land cover). One Claude agent orchestrates **nine tools**: five for batch (ingest → sample → score → validate → report) and four for on-demand (single location, state/county, polygon/bbox, top counties). Interactive UI: map, search, chat, admin dashboard.

---

## How to Run

1. **Prereqs:** Python 3.11+, `data/raw/DATA_CHALLENGE_50.csv`, three rasters (see Data setup below). `cp .env.example .env` and set `ANTHROPIC_API_KEY`.
2. **Batch:** `python -m src.main` → writes `data/processed/locations_scored.csv`, `data/output/findings_report.md`, charts, `agent_monitoring_report.json`.
3. **Interactive CLI:** `python -m src.main --mode interactive --query "Risk at 35.78, -78.64?"`
4. **UI:** `python -m app.run` → http://127.0.0.1:5001 (map, Analyze, Chat, Admin at /admin).
5. **Tests:** `pytest tests/ -v`

---

## Data Setup (Rasters)

Three rasters are required. Scripts are idempotent (skip if file exists).

| File | Source | Method |
|------|--------|--------|
| `nlcd_tcc_conus_2021_v2021-4.tif` | NLCD Tree Canopy 2021 | `curl -L "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/nlcd_tcc_CONUS_2021_v2021-4.zip" -o data/raw/nlcd_tcc_conus_2021.zip` then `unzip` in `data/raw/` |
| `nlcd_landcover_conus_2021.tif` | NLCD Land Cover 2021 | `python scripts/download_nc_landcover.py` |
| `dem_conus.tif` | USGS 3DEP DEM | `python scripts/download_nc_dem.py` |

Paths are in `src/config.py`. Full download details and troubleshooting are in the README "Add your data" and "Download rasters" sections.

---

## UI (Phase 7)

- **Start:** `python -m app.run` → http://127.0.0.1:5001  
- **Requires:** `data/processed/locations_scored.csv` (run batch first), `ANTHROPIC_API_KEY` in `.env`
- **Features:** Map (choropleth + risk points, Satellite basemap), location search & GPS, click point/county for assessment, Full Report, Admin (stats, API usage & cost, Plotly charts), Chat (on-demand tools only: `analyze_location`, `assess_area`, `assess_polygon`, `query_top_counties`).
- **Troubleshooting:** Missing `locations_scored.csv` → run `python -m src.main`. Missing API key → add to `.env`. Chat slow → ensure app uses latest code (on-demand-only tools).

---

## Monitoring & Pricing

- **Source:** [Anthropic Claude API pricing](https://docs.anthropic.com/en/docs/about-claude/pricing). Input $5/1M tokens, output $25/1M tokens (config in `src/config.py`).
- **Output:** `data/output/agent_monitoring_report.json` — `input_tokens`, `output_tokens`, `estimated_cost_usd`, `mode`, `tools_restricted`. Admin dashboard shows latest run.

---

## Tool Verification

All 9 tools (5 batch + 4 on-demand) are defined in `src/tools.py` with JSON schemas. Batch order: `ingest_locations` → `sample_environment` → `score_risk` → `validate_results` → `generate_report`. On-demand: `analyze_location`, `assess_area`, `assess_polygon`, `query_top_counties`. Requirement scenarios (datasource download/analyze, location TCC, buffer alternatives) are covered and tested. See [architecture.md](architecture.md) for the full tool list and flow.

---

## Where to Find What

| Need | Document |
|------|----------|
| Setup, run, decision log, findings | [README.md](../README.md) |
| Architecture, tools, Mermaid diagram | [architecture.md](architecture.md) |
| Methodology, thresholds, limitations | [analysis_rationale.md](analysis_rationale.md) |
| AI tools and divergence cases | [AI_TOOLS.md](../AI_TOOLS.md) |

---

## Submission Checklist

- [x] README with decision log
- [x] AI_TOOLS.md
- [x] docs/ (architecture, analysis rationale, this guide)
- [x] src/ (pipeline + agent)
- [x] Batch and interactive modes; UI with map, chat, admin
- [x] Monitoring report with token usage and estimated cost
