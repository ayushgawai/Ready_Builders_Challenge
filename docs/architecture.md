# System Architecture

**Project:** LEO Satellite Coverage Risk Analysis  
**Version:** 1.0  
**Last updated:** March 2026

---

## Overview

The pipeline is an **agent-orchestrated data system** that takes ~1M broadband locations (lat/lon) and produces a risk-scored output indicating which locations are likely to experience LEO satellite connectivity issues due to environmental obstructions.

The Claude API (via Anthropic's tool_use protocol) acts as the **reasoning layer**: it decides which tools to invoke, in what order, and how to handle failures or anomalies. The underlying Python functions do the actual computation — the agent provides the decision logic.

---

## High-Level System Diagram

```mermaid
flowchart TB
    subgraph Input["📥 Input Layer"]
        CSV["Locations CSV\n~1M records\n(location_id, lat, lon, state, county)"]
        GUIDE["Starlink Install Guide\nRequirements Reference\n(translated to thresholds in config.py)"]
    end

    subgraph Agent["🤖 Agent Orchestrator (Claude API)"]
        ORCH["Orchestrator Agent\n─────────────────────\n• Reasons about workflow state\n• Calls tools sequentially\n• Handles tool failures\n• Explains results in natural language\n• Decides whether to retry or abort"]
    end

    subgraph Tools["🔧 Tool Layer  (defined in src/tools.py)"]
        T1["ingest_locations\n─────────────────\nLoad CSV → validate schema\nclean data → log quality metrics"]
        T2["sample_environment\n─────────────────\nSample canopy % at lat/lon\nSample elevation → compute slope\nExtract land cover class"]
        T3["score_risk\n─────────────────\nApply risk methodology\nAssign HIGH / MOD / LOW\nCompute composite score"]
        T4["validate_results\n─────────────────\nCheck for anomalies\nFlag impossible values\nDistribution sanity check"]
        T5["generate_report\n─────────────────\nSummary statistics\nVisualizations\nKey findings narrative"]
    end

    subgraph Data["💾 Data Layer  (local rasters, .gitignored)"]
        NLCD_TCC["NLCD Tree Canopy Cover 2021\n30m GeoTIFF — CONUS\nSource: USGS / MRLC"]
        DEM["USGS 3DEP DEM\n10m GeoTIFF — CONUS\nSource: USGS National Map"]
        NLCD_LC["NLCD Land Cover 2021\n30m GeoTIFF — CONUS\nSource: USGS / MRLC"]
    end

    subgraph Output["📊 Output Layer"]
        SCORED["locations_scored.csv\n(with risk tier + component scores)"]
        REPORT["findings_report.md\n(key stats + narrative)"]
        VIZ["risk_distribution.png\nrisk_map_static.png\nrisk_map_interactive.html"]
    end

    CSV --> ORCH
    GUIDE -.->|"thresholds\ncodified in\nconfig.py"| ORCH
    ORCH --> T1
    T1 --> T2
    NLCD_TCC --> T2
    DEM --> T2
    NLCD_LC --> T2
    T2 --> T3
    T3 --> T4
    T4 -->|"✅ Pass"| T5
    T4 -->|"⚠️ Anomaly detected"| ORCH
    T5 --> SCORED
    T5 --> REPORT
    T5 --> VIZ

    style Input fill:#e3f2fd,stroke:#1565C0
    style Agent fill:#f0f4ff,stroke:#2E75B6
    style Tools fill:#fff8e1,stroke:#BF8F00
    style Data fill:#e8f5e9,stroke:#548235
    style Output fill:#fce4ec,stroke:#C62828
```

---

## Agent Interaction Sequence

```mermaid
sequenceDiagram
    participant U as User / Runner
    participant A as Agent (Claude)
    participant T1 as ingest_locations
    participant T2 as sample_environment
    participant T3 as score_risk
    participant T4 as validate_results
    participant T5 as generate_report

    U->>A: Run pipeline for locations.csv
    A->>A: Reason: Need to load and validate data first
    A->>T1: Call { file_path: "data/raw/locations.csv" }
    T1-->>A: { valid: 950K, dropped: 50K, quality_log }
    A->>A: Reason: 5% dropped — review quality log, proceed if acceptable
    A->>T2: Call { locations: <validated data ref> }
    T2-->>A: { enriched locations: canopy_pct, elevation_m, slope_deg, land_cover_code }
    A->>A: Reason: Environmental data attached. Check NaN rate before scoring.
    A->>T3: Call { enriched_data: <ref> }
    T3-->>A: { scored locations: composite_score, risk_tier }
    A->>A: Reason: Scoring done. Must validate before trusting results.
    A->>T4: Call { scored_data: <ref> }
    T4-->>A: { is_valid: true, anomalies: 120, validation_summary }
    A->>A: Reason: Valid. 120 anomalies flagged but within acceptable range.
    A->>T5: Call { validated_data: <ref> }
    T5-->>A: { summary_stats, chart_paths, findings_text }
    A-->>U: Pipeline complete. 23% of locations are HIGH risk. Key findings: ...
```

---

## Component Descriptions

### Agent Orchestrator (`src/agent.py`)

- **What it does:** Holds the conversation loop with Claude. Sends tool results back to Claude after each invocation and lets Claude decide what to call next.
- **Scope:** Reasoning, ordering, failure handling. Does NOT do computation.
- **Tool access:** All 5 tools.
- **State management:** The current scored DataFrame is held in memory and referenced by name between tool calls. A `PipelineState` dataclass tracks what has been completed.
- **Failure handling:** If a tool throws an exception, the agent receives the error message and can retry with adjusted parameters or abort gracefully with an explanation.

### Tool: `ingest_locations` (`src/ingest.py`)

- Loads the raw CSV with pandas.
- Validates schema (expected columns present), coordinate ranges, duplicate IDs.
- Returns a quality report (counts of dropped records and drop reasons).
- Saves cleaned data to `data/processed/`.

### Tool: `sample_environment` (`src/environment.py`)

- Downloads national rasters if not already present (canopy, DEM, land cover).
- Samples raster pixel values at each location's (lat, lon) using rasterio.
- Computes terrain slope from the DEM.
- Processes in batches of `config.BATCH_SIZE` (default 50K) for memory efficiency.
- Returns enriched DataFrame with `canopy_pct`, `elevation_m`, `slope_deg`, `land_cover_code`.

### Tool: `score_risk` (`src/risk_scoring.py`)

- Applies per-factor risk scorers (vectorized with numpy for 1M-row performance).
- Computes weighted composite score: `canopy×0.50 + slope×0.30 + landcover×0.20`.
- Assigns risk tier: HIGH (≥0.6), MODERATE (0.3–0.6), LOW (<0.3).
- All thresholds read from `config.py` — no hardcoded magic numbers.

### Tool: `validate_results` (`src/validation.py`)

- Checks for impossible values (canopy>100, slope<0).
- Checks score distribution — flags if >90% of locations land in one tier.
- Cross-validates: forest land cover + canopy<5% → anomaly.
- Returns `(is_valid: bool, validation_report: dict)`.

### Tool: `generate_report` (`src/reporting.py`)

- Computes summary statistics (tier distribution, top-risk states/counties).
- Generates static charts (bar, pie, scatter map).
- Writes findings narrative to `findings_report.md`.

---

## Data Flow and State Management

```
Raw CSV → [T1: ingest] → cleaned_locations.csv (data/processed/)
       → [T2: enrich] → enriched_locations.csv (data/processed/)
       → [T3: score]  → locations_scored.csv   (data/processed/)
       → [T4: validate] → validation_report.json (data/output/)
       → [T5: report]  → findings_report.md, charts (data/output/)
```

Each tool call persists its output to disk. If the pipeline is interrupted and restarted, completed steps can be skipped (file existence check in each tool). The agent is told which intermediate files exist at startup.

---

## Failure Handling

| Failure Scenario | Behavior |
|---|---|
| Missing raster file | `sample_environment` raises `FileNotFoundError` with download instructions; agent logs and informs user |
| Network failure during raster download | `download_raster` retries 3× with exponential backoff; raises after final failure |
| Locations all outside CONUS bounds | Validation raises `DataQualityError`; agent aborts and reports which file was provided |
| Validation anomaly rate too high | `validate_results` returns `is_valid=False`; agent reports anomalies and asks user whether to proceed |
| Claude API error | `PipelineAgent` catches and logs; non-agent fallback mode available via `main.py --mode batch --no-agent` |

---

## Production Considerations

This pipeline is a proof-of-concept optimized for a 4-day build sprint. For production deployment:

| Concern | POC Approach | Production Approach |
|---|---|---|
| Orchestration | Python `while` loop | Apache Airflow DAG |
| Raster storage | Local GeoTIFF files | Cloud-Optimized GeoTIFFs (COGs) on S3 |
| Scale | Serial batches (50K) | Distributed (Dask / Spark) |
| Raster access | Full CONUS download | GDAL Virtual File System reads from S3 COG |
| Monitoring | JSON log file | Prometheus + Grafana, LLM observability (LangSmith) |
| Drift detection | Manual | Quarterly scheduled reruns; alert if tier distribution shifts >5% |
| Data updates | NLCD 2021 (static) | Subscribe to MRLC update notifications; re-run on new releases |
