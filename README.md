# LEO Satellite Coverage Risk Analysis

An agent-driven data pipeline that identifies which of ~4.67M broadband locations committed to by LEO satellite providers (Starlink) are at elevated risk of connectivity issues due to environmental obstructions: trees, terrain, and land cover.

Documentation: [Setup & run](docs/guide.md) · [Architecture](docs/architecture.md)

---

## The Problem

US states awarded LEO satellite providers grant funding to deliver broadband to underserved communities. But Starlink requires a **100–110° unobstructed field of view** of the sky. Tree canopy, steep terrain, and surrounding structures can degrade signal quality, leaving residents in the "served" footprint with an underperforming connection.

This pipeline analyzes each committed location against three publicly available national datasets to assign a **risk tier** (HIGH / MODERATE / LOW) based on the environmental conditions at that point.

| Dataset | Source | What it shows | Why we use it | Why we trust it |
|---------|--------|---------------|---------------|-----------------|
| **NLCD Tree Canopy Cover 2021** | USGS / MRLC ([mrlc.gov](https://www.mrlc.gov/data/nlcd-2021-usfs-tree-canopy-cover-conus)) | % of each 30m pixel covered by tree canopy (0–100) | Starlink’s install guide cites tree branches as the main cause of obstruction. Canopy % directly measures this at scale. | US federal standard, nationally consistent, same dataset used in broadband and environmental planning. |
| **USGS 3DEP DEM** | USGS 3D Elevation Program ([nationalmap.gov](https://apps.nationalmap.gov/downloader/)) | Elevation (m) at ~30m resolution; we derive slope (degrees) | Steep terrain narrows the visible sky arc. Slope quantifies horizon obstruction risk. | Authoritative US elevation source; standard for terrain analysis. |
| **NLCD Land Cover 2021** | USGS / MRLC ([mrlc.gov](https://www.mrlc.gov/data/nlcd-2021-land-cover-conus)) | Land cover class per 30m pixel (e.g. forest, developed, open) | Cross-validates canopy and adds context (e.g. forest vs developed). Used in composite risk weight (20%). | Same vintage and grid as canopy layer; well-documented codes. |

The same agent also works interactively: a field technician can enter an address or coordinates and get a plain-English risk breakdown, nearby lower-risk alternatives, and seasonal installation guidance, without any GIS knowledge.

---

## Architecture

**How it works (overview)** — for a non-technical audience:

![How it works](docs/architecture_overview.png)

**System architecture (technical)** — tools, files, and data flow:

![System architecture](docs/architecture_diagram.png)

The system uses **Claude (Anthropic API)** as the agent orchestrator with **nine tools** across two modes:

**Batch pipeline** (processes all 4.67M locations end-to-end):
```
ingest_locations → sample_environment → score_risk → validate_results → generate_report
```

**On-demand tools** (single-location or area queries; also used by the UI chat):
```
analyze_location     technician enters address or coordinates: full risk breakdown
assess_area          state or county: risk briefing
assess_polygon       custom area (bbox or polygon): risk briefing
query_top_counties   top N counties by tier count (read-only from scored CSV)
```

The agent routes between modes automatically: specific-location queries call `analyze_location`, area queries call `assess_area`, custom areas (bbox or polygon) call `assess_polygon`, and questions like "which county has the most low-risk locations?" use `query_top_counties`. Full-pipeline requests (CLI only) run all five batch tools in order. **The UI chat is restricted to on-demand tools only**; it never runs the batch pipeline or re-samples rasters, so responses stay fast and use only cached scored data.

The diagram’s **Output** (scored CSV, findings report, charts, monitoring JSON) is what the **UI** consumes: the map and search use the scored CSV, the Full Report page renders the Markdown, and the Admin dashboard reads the monitoring JSON. See [docs/architecture.md](docs/architecture.md) for the full Mermaid diagram and component descriptions.

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone https://github.com/ayushgawai/Ready_Builders_Challenge
cd Ready_Builders_Challenge

python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 4. Add your data

Place the locations CSV at `data/raw/DATA_CHALLENGE_50.csv`.  
The file must contain columns: `location_id`, `latitude`, `longitude`, `geoid_cb`.

### 5. Download rasters (one-time setup)

The pipeline needs **three raster datasets** to score each location. A raster is a grid of values over the map (like a heat map): at every 30 m cell we have a number for tree canopy %, elevation, and land cover. For each of the 4.67M locations we **look up** the value at that point in these grids; that gives us the inputs to the risk formula (canopy weight 50%, slope 30%, land cover 20%). Without the rasters we cannot compute environmental risk. They are stored locally as GeoTIFFs and read during the batch run; the interactive UI and chat use the **already scored** CSV, so they do not require the rasters after the first full run.

**In plain language:** Think of the rasters as three “layers” of the map: one says “how much tree cover is here,” one “how steep is the ground,” and one “what kind of land (forest, town, field).” We download these layers once from US government sources. For every address or coordinate we care about, we look up the three values at that spot and combine them into a single risk score (HIGH / MODERATE / LOW). No database or real-time API is used for this; we use the pre-downloaded layers so we can process millions of points quickly.

Three raster files are required. Scripts are idempotent; they skip if the file already exists.

```bash
# 1. NLCD Tree Canopy Cover 2021 (~3.7 GB ZIP), direct MRLC download
curl -L "https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/nlcd_tcc_CONUS_2021_v2021-4.zip" \
     -o data/raw/nlcd_tcc_conus_2021.zip
cd data/raw/ && unzip nlcd_tcc_conus_2021.zip && cd ../..

# 2. Annual NLCD Land Cover 2021, automated via MRLC WCS (no account needed)
python scripts/download_nc_landcover.py   # ~10 min, skips if already done

# 3. USGS 3DEP DEM, automated tiling from USGS 3DEP REST API
python scripts/download_nc_dem.py         # ~5 min, skips if already done
```

See [docs/guide.md](docs/guide.md) for raster download steps and troubleshooting.

---

## Data storage

There is **no database**. All data is file-based:

| Layer | Location | Contents |
|-------|----------|----------|
| **Raw** | `data/raw/` | Input CSV (`DATA_CHALLENGE_50.csv`), three GeoTIFF rasters (canopy, DEM, land cover) |
| **Processed** | `data/processed/` | Validated locations CSV, slope raster (derived), scored locations CSV (~4.67M rows with risk tier and component scores) |
| **Output** | `data/output/` | Findings report (Markdown), risk chart and map (PNG), monitoring JSON (tokens, cost, mode) |

The batch pipeline reads from `raw/` and `processed/` and writes to `processed/` and `output/`. The UI and interactive CLI read the scored CSV and monitoring JSON only; they do not write new data. **All of `data/` is in `.gitignore`**: these outputs are not committed to git. They stay local and are only used by the UI (and your own analysis); the UI loads them from disk at runtime (e.g. map from scored CSV, Full Report from the Markdown file, Admin from the monitoring JSON).

---

## How to Run

### Batch mode (full pipeline, all 4.67M locations)

```bash
python -m src.main
```

### Interactive mode (single location or area query)

```bash
python -m src.main --mode interactive --query "What is the risk at 35.22, -80.84?"
python -m src.main --mode interactive --query "123 Main St, Charlotte NC"
python -m src.main --mode interactive --query "Give me a risk briefing for Buncombe County, NC"
```

### Run tests

```bash
pytest tests/ -v
# 250 tests; unit, integration, and on-demand tool tests
```

**End-to-end verification:** Run the full pipeline (`python -m src.main`), then interactive queries (`--mode interactive --query "..."`), then the UI (`python -m app.run` → http://127.0.0.1:5001). See [docs/guide.md](docs/guide.md) for UI verification steps.

### Phase 7: Interactive Web UI

Requires `data/processed/locations_scored.csv` (run the full pipeline first) and `ANTHROPIC_API_KEY` in `.env`.

```bash
python -m app.run
# Opens http://127.0.0.1:5001  (port 5001 to avoid conflict with macOS AirPlay on 5000)
```

**Features:**
- **Map:** NC county choropleth and clustered risk points. **Satellite** basemap option (layer control). **Hover** a point for lat/lon and tier; **click** a point for single-location assessment in the left pane (with Download report). **Show risk overlay** off hides only the county color fill; outlines, point clusters, and **Statewide** stats stay visible.
- **Resizable sidebar:** Drag the divider to resize. **Country** from dataset/fallback (`/api/countries`). **County:** "All counties" plus full NC list (`/api/counties`, pygris fallback if needed); select one to zoom. **Risk toggles:** show or hide High / Moderate / Low point layers.
- **Left pane:** Shows the risk result for **Location search** (address or coordinates only) and when you **click a point**. You can type e.g. `35.78, -78.64` in the search box and get the full risk breakdown, installer recommendation, and nearby alternatives in the left pane. Clicking a map point also fills the left pane with that location’s assessment.
- **Location search:** Address or coordinates in the search box (no need for full sentences). Full risk breakdown appears in the left pane.
- **Use GPS:** Browser geolocation to analyze current location. Locations outside North Carolina show a clear "data available for NC only" message.
- **Click county:** County-level risk briefing (LLM). **Click point:** that location only (on-demand).
- **Full Report:** `findings_report.md` rendered as HTML.
- **Admin** (`/admin`): Data team dashboard with summary stats, **API usage and cost** (tokens, estimated USD, pricing source), and **interactive** Plotly charts (bar, pie, scatter map). Rendered from scored data and `agent_monitoring_report.json`.
- **Chat:** Natural-language questions (e.g. "What is the risk at 35.78, -78.64?", "Give me a briefing for Wake County"). Response appears in the chat thread and in the left panel when it includes a location; the map zooms to coordinates when present. Use the **search box** for address or coordinates only; use **Chat** for full sentences and area questions.
- **Out-of-coverage:** Locations outside NC (e.g. San Jose) still get an agent analysis; the UI shows a "Limited data, NC only" banner.

The UI uses the same **Claude model** as the pipeline (`CLAUDE_MODEL`) for on-demand analyze/county/chat. See [docs/guide.md](docs/guide.md).

---

## Project Structure

```
.
├── src/
│   ├── config.py          # All paths, thresholds, and constants
│   ├── ingest.py          # Data ingestion and 5-pass validation
│   ├── environment.py     # Raster downloading and point sampling
│   ├── risk_scoring.py    # Risk methodology and scoring
│   ├── validation.py      # Result validation and anomaly detection
│   ├── reporting.py       # Summary stats and findings report
│   ├── tools.py           # All 9 agent tool schemas (Anthropic tool_use format)
│   ├── agent.py           # Claude agent orchestrator (batch + on-demand)
│   ├── main.py            # Entry point (batch / interactive / pipeline-only modes)
│   └── utils/
│       ├── geo_utils.py         # CRS helpers, GEOID→state/county (pygris + us)
│       ├── geocoding_utils.py   # Address geocoding (Nominatim), haversine, spatial search
│       ├── pipeline_utils.py    # Idempotency cache helpers
│       └── logging_config.py    # Logging setup
├── tests/
│   ├── conftest.py              # Shared pytest fixtures (programmatic DataFrames + GeoTIFFs)
│   ├── test_environment.py
│   ├── test_ingest.py
│   ├── test_risk_scoring.py
│   ├── test_validation.py
│   ├── test_integration.py      # Real-data tests: 1000 rows sampled from DATA_CHALLENGE_50.csv
│   └── test_on_demand_tools.py  # on-demand tools: analyze_location, assess_area, assess_polygon, query_top_counties
├── docs/
│   ├── architecture.md       # System design + Mermaid diagrams
│   ├── analysis_rationale.md # Methodology justification
│   └── guide.md              # Run, data setup, UI, monitoring, tools
├── app/                         # Phase 7: Interactive Web UI
│   ├── run.py                   # Entry: python -m app.run
│   ├── app.py                   # Flask routes (stats, map-data, analyze, county, report)
│   ├── static/css/style.css
│   ├── static/js/map.js        # Leaflet map, search, GPS, panels
│   └── templates/              # index.html, report.html
├── data/                        # .gitignored; created locally
│   ├── raw/                     # Input CSV + downloaded rasters
│   ├── processed/               # Validated, enriched, and scored CSVs
│   └── output/                  # Report, charts, maps, monitoring JSON
├── .env.example
├── requirements.txt
└── AI_TOOLS.md
```

---

## Risk Scoring Methodology

Based on Starlink's install guide requirements:

| Factor | High Risk | Moderate Risk | Low Risk | Weight |
|---|---|---|---|---|
| Tree Canopy Cover | >50% | 20–50% | <20% | 50% |
| Terrain Slope | >20° | 10–20° | <10° | 30% |
| Land Cover Type | Forest (41,42,43) | Developed (21–24) | Open/Ag/Barren | 20% |

**Composite score** = `canopy×0.50 + slope×0.30 + landcover×0.20`

**Risk tiers:** HIGH (≥0.6) · MODERATE (0.3–0.6) · LOW (<0.3)

See [`docs/analysis_rationale.md`](docs/analysis_rationale.md) for full methodology justification.

---

## Decision Log

| Decision | Alternatives Considered | Reasoning | What I'd Revisit |
|---|---|---|---|
| Anthropic tool_use for agent | LangGraph, LangChain | Tool_use is native to Claude API, fewer abstractions, clear for the current implementation | LangGraph for complex multi-agent scenarios with state checkpointing |
| Download rasters locally | Per-point API calls | 4.67M locations × API call = infeasible. One 1–4 GB raster download enables batch processing in minutes | Cloud-Optimized GeoTIFFs (COGs) on S3 for serverless production |
| rasterio for point sampling | PostGIS, GDAL directly | We're doing raster sampling at points, not spatial joins. rasterio is the right tool. PostGIS adds a DB server with zero benefit here | No change needed |
| Python venv + requirements.txt | Poetry, conda | Simple and reproducible with clear dependencies | Poetry for production with locked versions |
| Pandas + numpy (no Spark) | PySpark, Dask | 4.67M rows fits in RAM with careful batching. Distributed compute adds infrastructure cost without benefit at this scale | Dask at 100M+ locations |
| Canopy weight 50%, Slope 30%, Landcover 20% | Equal weights, model-driven weights | Weights reflect Starlink guide hierarchy: dish doc emphasizes canopy as primary obstruction. Slope matters but can be mitigated by mounting height. Land cover is corroborating signal | Could calibrate with ground-truth data from actual Starlink installs |
| Sequential drop ordering in validate_locations | Drop all at once, flag-only | Sequential drops prevent double-counting: a row with null lat AND bad state is counted once (null lat wins). Clean ordering also makes the quality report accurate and auditable | No change needed for this use case |
| Drop invalid state codes | Warn but keep | State code is used for state-level reporting; a row with an unrecognisable state produces wrong aggregations. Drop is cleaner than propagating bad labels into the report | Could loosen to warn-only if the dataset has many non-standard state formats |
| pygris + us for county/state names | Hardcoded FIPS dictionary | Hardcoding 3000+ county names doesn't scale and requires manual maintenance. pygris fetches from authoritative Census TIGER files; us library handles all 50 states + DC | No change needed |
| Nominatim for address geocoding | Google Maps, ArcGIS, HERE | All commercial alternatives require API keys or billing. Nominatim (OpenStreetMap) is free, no key, and sufficient for single-location interactive queries | Switch to paid geocoder if submitting thousands of addresses per session |
| Seasonal risk as advisory note only | Adjust stored composite score by season | Changing the score based on query date would create two different scores for the same location, breaking report consistency. Advisory note in analyze_location response is the clean solution | Could add a separate `winter_risk_tier` column to the scored CSV in a future phase |
| UI chat restricted to on-demand tools only | Expose all tools and rely on prompt | When chat had access to batch tools, the agent sometimes called `sample_environment` for questions like "which county has most low-risk locations?", re-sampling rasters and taking minutes. Restricting `run_interactive` to `ON_DEMAND_TOOLS` (analyze_location, assess_area, assess_polygon, query_top_counties) keeps chat fast and cache-only | None; batch pipeline remains CLI-only by design |
| Agent API calls in agent.py only | Move to utils (e.g. agent_utils.py) | All Claude messages.create and tool handling live in agent.py; we call the API multiple times per run (batch tool chain, interactive turns). Repeated patterns could be refactored into shared helpers for clarity and reuse | Add a small utils layer for agent call patterns so we do not duplicate logic when we call the API multiple times (batch + interactive); no rework planned now due to time |

---

## Key Findings

> Results from the completed pipeline run on **4,674,905 North Carolina locations**:

| Risk Tier | Count | % |
|-----------|------:|---:|
| **HIGH** | 907,525 | 19.4% |
| MODERATE | 1,463,751 | 31.3% |
| LOW | 2,303,629 | 49.3% |
| UNKNOWN | 0 | 0.0% |

- **50.7%** of NC locations carry MODERATE or HIGH LEO connectivity risk
- **Top HIGH-risk counties:** Wake (102K), Mecklenburg (82K), Durham (39K), Buncombe (38K)
- Western NC (Appalachian mountains) shows combined canopy + terrain slope risk
- **0% UNKNOWN:** all 4.67M locations fully resolved against real rasters

Full narrative in `data/output/findings_report.md` (generated when you run the pipeline).

---

## Known Limitations

- Tree canopy % ≠ tree height (LiDAR required for height)
- NLCD is 30m resolution; sub-parcel variation not captured
- No building obstruction modeling (no national height dataset)
- **Seasonal variation:** NLCD = peak summer canopy. The `analyze_location` tool partially addresses this for Deciduous Forest locations (NLCD code 41): it flags that winter installations (Nov–Mar) may see 30–60% lower canopy obstruction due to leaf drop and that the risk tier may be effectively lower in winter.
- Microsite mounting options require on-site assessment

See [docs/analysis_rationale.md](docs/analysis_rationale.md) and [docs/guide.md](docs/guide.md) for methodology and limitations.

---

## Production Considerations

- **Orchestration:** Replace Python loop with Apache Airflow DAG for scheduling and monitoring
- **Raster storage:** Use Cloud-Optimized GeoTIFFs (COGs) on S3 with GDAL virtual file system reads
- **Scale:** Dask or Spark for distributed raster sampling at 100M+ locations. See `docs/architecture.md` → "Scale Architecture" for the full Snowflake / dbt / Airflow / AWS production plan.
- **GPS input:** The `analyze_location` tool accepts lat/lon coordinates. The Phase 7 UI supports browser geolocation (Use GPS) for field technicians at an install site; out-of-NC locations still get analysis with a "Limited data, NC only" banner.
- **Drift detection:** Quarterly reruns with automated alerts if risk tier distribution shifts >5%
- **Monitoring:** Per-agent token usage, latency, and tool-call accuracy via LangSmith or custom metrics

---

## Output Format

The batch pipeline produces these artefacts:

| Artefact | Location | Description |
|---|---|---|
| Scored CSV | `data/processed/locations_scored.csv` | All locations with canopy/slope/landcover scores, composite score, and risk tier |
| Findings report | `data/output/findings_report.md` | Key findings narrative, tier distribution, top risk states/counties |
| Risk chart | `data/output/risk_distribution.png` | Bar chart of tier distribution (Phase 7) |
| Risk map | `data/output/risk_map_static.png` | Scatter plot of CONUS locations colour-coded by tier (Phase 7) |
| Monitoring report | `data/output/agent_monitoring_report.json` | Token usage, estimated cost (USD), mode, tools_restricted, per-tool timings and success/failure |

The analysis narrative is written by the Claude agent at the end of the pipeline run and printed to stdout. **Why both CSV and Markdown?** The CSV holds the structured data (every location, scores, tiers) for the UI map, charts, and programmatic use. The Markdown report is the human-readable summary (key findings, percentages, top counties); the UI renders it as the “Full Report” page. Both are needed: CSV for data, MD for narrative.

---

## AI Tool Disclosure

See [`AI_TOOLS.md`](AI_TOOLS.md).
