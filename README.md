# LEO Satellite Coverage Risk Analysis

An agent-driven data pipeline that identifies which of ~1M broadband locations committed to by LEO satellite providers (Starlink) are at elevated risk of connectivity issues due to environmental obstructions — trees, terrain, and land cover.

Built for the [Ready.net Builders Challenge](https://github.com/ready/builders-challenge/issues/50).

---

## The Problem

US states awarded LEO satellite providers grant funding to deliver broadband to underserved communities. But Starlink requires a **100–110° unobstructed field of view** of the sky. Tree canopy, steep terrain, and surrounding structures can degrade signal quality — leaving residents in the "served" footprint with an underperforming connection.

This pipeline analyzes each committed location against three publicly available national datasets to assign a **risk tier** (HIGH / MODERATE / LOW) based on the environmental conditions at that point.

---

## Architecture

The system uses **Claude (Anthropic API)** as the agent orchestrator, with five well-defined tools that handle the full pipeline:

```
ingest_locations → sample_environment → score_risk → validate_results → generate_report
```

The agent reasons about the workflow, handles failures, and explains its findings in plain language.

See [`docs/architecture.md`](docs/architecture.md) for the full Mermaid architecture diagram and component descriptions.

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone <repo-url>
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

Place the locations CSV at `data/raw/locations.csv`.  
For development/testing, a sample CSV (`data/raw/locations_sample.csv`) is included.

Environmental rasters (NLCD canopy, USGS DEM, NLCD land cover) are downloaded automatically on first run.  
See [`docs/data_sourcing.md`](docs/data_sourcing.md) for manual download instructions.

---

## How to Run

### Batch mode (full pipeline on CSV)

```bash
python src/main.py --mode batch --input data/raw/locations.csv
```

### Interactive mode (analyze a single location)

```bash
python src/main.py --mode interactive
# You will be prompted: Enter latitude and longitude
```

### Run tests

```bash
pytest tests/ -v
```

---

## Project Structure

```
.
├── src/
│   ├── config.py          # All paths, thresholds, and constants
│   ├── ingest.py          # Data ingestion and validation
│   ├── environment.py     # Raster downloading and point sampling
│   ├── risk_scoring.py    # Risk methodology and scoring
│   ├── validation.py      # Result validation and anomaly detection
│   ├── reporting.py       # Summary stats and visualizations
│   ├── tools.py           # Agent tool schemas (Anthropic tool_use format)
│   ├── agent.py           # Claude agent orchestrator
│   └── main.py            # Entry point
├── tests/
│   ├── test_ingest.py
│   ├── test_risk_scoring.py
│   └── test_validation.py
├── docs/
│   ├── architecture.md            # System design + Mermaid diagrams
│   ├── step0_install_guide_analysis.md  # Install guide Q&A + dataset selection
│   ├── analysis_rationale.md      # Methodology justification
│   └── data_sourcing.md           # Dataset details and quality notes
├── data/                  # .gitignored — created locally
│   ├── raw/               # Input CSV + downloaded rasters
│   ├── processed/         # Enriched and scored outputs
│   └── output/            # Reports, charts, maps
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
| Anthropic tool_use for agent | LangGraph, LangChain | Tool_use is native to Claude API, fewer abstractions, cleaner for a POC | LangGraph for complex multi-agent scenarios with state checkpointing |
| Download rasters locally | Per-point API calls | 1M locations × API call = infeasible. One 1-4GB download enables batch processing in minutes | Cloud-Optimized GeoTIFFs (COGs) on S3 for serverless production |
| rasterio for point sampling | PostGIS, GDAL directly | We're doing raster sampling at points, not spatial joins. rasterio is the right tool. PostGIS adds a DB server with zero benefit here | No change needed |
| Python venv + requirements.txt | Poetry, conda | Simple and reproducible for a POC with clear dependencies | Poetry for production with locked versions |
| Pandas + numpy (no Spark) | PySpark, Dask | 1M rows fits comfortably in RAM. Distributed compute adds infrastructure cost with no POC benefit | Dask at 100x scale (100M locations) |
| Canopy weight 50%, Slope 30%, Landcover 20% | Equal weights, model-driven weights | Weights reflect Starlink guide hierarchy: dish doc emphasizes canopy as primary obstruction. Slope matters but can be mitigated by mounting height. Land cover is corroborating signal | Could calibrate with ground-truth data from actual Starlink installs |
| Sequential drop ordering in validate_locations | Drop all at once, flag-only | Sequential drops prevent double-counting — a row with null lat AND bad state is counted once (null lat wins). Clean ordering also makes the quality report accurate and auditable | No change needed for this use case |
| Drop invalid state codes | Warn but keep | State code is used for state-level reporting; a row with an unrecognisable state produces wrong aggregations. Drop is cleaner than propagating bad labels into the report | Could loosen to warn-only if the dataset has many non-standard state formats |

---

## Key Findings

> *(Populated after Phase 7 — Reporting)*

---

## Known Limitations

- Tree canopy % ≠ tree height (LiDAR required for height)
- NLCD is 30m resolution — sub-parcel variation not captured
- No building obstruction modeling (no national height dataset)
- Seasonal variation not captured (NLCD = peak summer canopy)
- Microsite mounting options require on-site assessment

See [`docs/step0_install_guide_analysis.md`](docs/step0_install_guide_analysis.md) for the full limitations analysis.

---

## Production Considerations

- **Orchestration:** Replace Python loop with Apache Airflow DAG for scheduling and monitoring
- **Raster storage:** Use Cloud-Optimized GeoTIFFs (COGs) on S3 with GDAL virtual file system reads
- **Scale:** Dask or Spark for distributed raster sampling at 100M+ locations
- **Drift detection:** Quarterly reruns with automated alerts if risk tier distribution shifts >5%
- **Monitoring:** Per-agent token usage, latency, and tool-call accuracy via LangSmith or custom metrics

---

## Output Format

The pipeline produces three artefacts:

| Artefact | Location | Description |
|---|---|---|
| Scored CSV | `data/output/locations_scored.csv` | All locations with canopy/slope/landcover scores, composite score, and risk tier (HIGH / MODERATE / LOW) |
| Summary statistics | `data/output/summary_stats.json` | Risk tier distribution, state-level breakdowns, data quality metrics |
| Visual report | `data/output/` | Choropleth map (folium), bar charts of tier distribution by state (matplotlib) |

The analysis narrative (plain-English findings) is written by the Claude agent at the end of the pipeline run and printed to stdout + appended to the scored CSV header.

---

## AI Tool Disclosure

See [`AI_TOOLS.md`](AI_TOOLS.md).
