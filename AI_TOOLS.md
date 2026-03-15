# AI Tool Disclosure

This document lists AI tools used in this project and **cases where I diverged from AI-generated output**. The code is the artifact; the reasoning behind overrides is the signal. I focus on product, UX, and architecture decisions — not trivial tweaks (e.g. model name strings). Over the build there were 15+ such divergence points; below are the ones that best show how we aligned the system with the real problem and the end user.

---

## Tools Used

| Tool | Provider | Used For |
|---|---|---|
| **Claude (Anthropic API)** | Anthropic | In-pipeline agent: tool orchestration, data-quality reasoning, findings narrative |
| **Claude Opus (via Cursor)** | Anthropic / Cursor | Architecture and build planning, methodology from Starlink install guide |
| **Cursor IDE** | Cursor AI | Code generation, scaffolding, refactors |
| **Nominatim (geopy)** | OpenStreetMap | Free US address → coordinates for `analyze_location` |
| **pygris** | PyPI (Python) | County NAMELSAD from FIPS via Census TIGER boundaries |
| **us** | PyPI (Python) | State FIPS ↔ abbreviation / name; e.g. `us.states.lookup(fips).abbr` → "CA" |

---

## Detailed Usage

**Claude API (in-pipeline):** The agent decides tool order and parameters, interprets results, handles failures, and produces the final summary. In UI chat and `--mode interactive`, only **on-demand tools** are exposed (`analyze_location`, `assess_area`, `assess_polygon`, `query_top_counties`). Batch tools (ingest, sample_environment, score_risk, etc.) are not in the tool list, so chat never triggers raster sampling or re-downloads — it stays fast and uses the existing scored CSV. Claude is a runtime component of the system, not a code generator that was then discarded.

**Nominatim:** Used for address → (lat, lon) in `analyze_location`. Free, no key, 1 req/s; sufficient for single-location queries. We argued over alternatives (Google, ArcGIS, HERE) and landed on Nominatim for cost and simplicity.

**pygris + us:** GEOID (`geoid_cb`) → state abbreviation and county **name** (NAMELSAD). **us** is the name of the Python package ([PyPI: us](https://pypi.org/project/us/)), not "United States" — it provides state metadata (FIPS code, abbreviation, full name) for all 50 states + DC. We use `us.states.lookup(fips).abbr` to get the 2-letter state code from the first two digits of `geoid_cb`. **pygris** is another Python package that fetches Census TIGER/Line boundaries and returns county names (NAMELSAD) from county FIPS. I insisted on storing and showing the actual county name everywhere (reports, UI, tool outputs), not just FIPS/ID — see divergences below.

---

## Cases Where I Diverged from AI Output

*Representative of 15+ such decisions. Format: what AI did → what I said/did → why that was the right call for the project and the user.*

### 1. County: show name, not just ID

**AI:** Kept county as FIPS/ID or a minimal code in reports and UI.  
**Me:** Use the actual county name (NAMELSAD from pygris) everywhere — reports, admin, analyze_location output, map labels.  
**Why:** Programme managers and field techs need "Wake County" and "Buncombe County", not codes. The artifact is for humans making decisions; IDs are for systems. I changed it so every user-facing surface shows the real name.

### 2. Map overlay: add “no overlay” option

**AI:** Added a risk choropleth overlay on the map (county colour by risk %).  
**Me:** Add a toggle to turn the overlay **off** so we can see the basemap and points clearly when the fill is distracting.  
**Why:** With overlay always on, the map was hard to read in some regions. A simple toggle lets users focus on either risk summary or geography. I pushed for this so the map serves both “where is high risk?” and “what does this place actually look like?”

### 3. Open in Google Maps / Google Earth

**AI:** Location detail showed coordinates and risk only.  
**Me:** Add links to open the assessed location in Google Maps and Google Earth (new tab).  
**Why:** Installers need to see the real site — roof, trees, terrain — not just our score. One click to satellite view in Maps or Earth closes the loop between our risk output and field verification. I added this so the tool supports the real workflow.

### 4. AI chat in the UI

**AI:** Proposed a simpler UI: map + search only, or minimal controls.  
**Me:** Add a chat box so users can ask natural-language questions (“which county has the most low-risk locations?”, “risk in this polygon”) and get answers from the same agent.  
**Why:** The agent already supports those questions via on-demand tools; hiding that behind a map-only UI underuses it. Chat makes the system a single place for both “risk at this point” and “risk in this area.” I pushed for chat as a first-class feature.

### 5. Chat must not trigger batch pipeline

**AI:** Early design exposed all tools in chat; the agent sometimes chose `sample_environment` for questions like “which county has most low risk?”  
**Me:** Restrict chat (and `--mode interactive`) to **on-demand tools only**. Batch tools are not in the tool list in that mode.  
**Why:** Re-sampling 4.67M points from chat would take 20–60 minutes and block the UI. The right answer is “query the scored CSV.” I made the boundary architectural: the API receives a different tool list in interactive mode, so the model cannot call batch steps. That’s a product and architecture call, not a prompt tweak.

### 6. Risk weights: 50/30/20, not equal

**AI:** Suggested equal 33%/33%/33% for canopy, slope, land cover.  
**Me:** 50% canopy, 30% slope, 20% land cover, per Starlink install guide and build plan.  
**Why:** The guide states tree branches are the primary obstruction. Land cover is corroborating context, not a direct obstruction measure. Equal weights would overstate land cover and mislead programme decisions. I kept the methodology aligned with the source.

### 7. Data sources: argued to a decision

**AI:** Suggested some alternative data sources or APIs (e.g. different geocoder, different raster extent or vendor).  
**Me:** We argued tradeoffs — cost, coverage, latency, maintenance — and landed on: Nominatim for US geocoding (free, no key); NLCD + 3DEP for rasters; GEOID + pygris for state/county.  
**Why:** Picking sources is a product and ops decision. I didn’t accept the first suggestion; we iterated until the choices matched real constraints (budget, single-location vs batch, US-only) and documented rationale in the README and decision log.

### 8. Schema reality: geoid_cb, not state/county columns

**Spec said:** CSV would have `state` and `county` columns.  
**Actual data:** Had `geoid_cb` (Census block GEOID) only.  
**Me:** Two-stage enrichment: derive state and county from GEOID (us + pygris) first; use reverse geocoder only for rows with missing/malformed GEOID.  
**Why:** The delivered data is the source of truth. Building on GEOID is more accurate and faster than reverse-geocoding 4.67M rows. I treated the real schema as a constraint and designed the pipeline around it.

### 9. County names: keep NAMELSAD, don’t strip “County”

**AI:** Strip “County” suffix (e.g. “Santa Clara” instead of “Santa Clara County”).  
**Me:** Keep Census NAMELSAD as-is (“Santa Clara County”, “Orleans Parish”, “Matanuska-Susitna Borough”).  
**Why:** NAMELSAD is the standard form. Stripping suffixes is fragile (Parish, Borough, Census Area) and adds maintenance for no user benefit. I kept the authoritative form everywhere.

### 10. PostGIS vs rasterio

**AI:** Suggested PostGIS as an option for geospatial processing.  
**Me:** Rasterio only; no PostGIS.  
**Why:** The core job is sampling raster cells at points. That’s a raster operation. PostGIS is for vector joins and adds a database server with no gain for this task. I rejected scope creep and kept the stack matched to the problem.

### 11. File-based state between tools, no database

**AI:** Could have passed DataFrames in memory or introduced a DB for intermediate state.  
**Me:** Each tool reads/writes known paths (validated → enriched → scored CSV). No DB for pipeline state.  
**Why:** Reproducibility and debuggability: re-run any step, inspect CSVs, no hidden state. Fits a batch + read-heavy UI and keeps the system easy to reason about and to scale later (e.g. warehouse + orchestration) without overbuilding now.

### 12. Idempotency for raster sampling

**AI:** Could re-run sampling every time.  
**Me:** If enriched output exists and is newer than validated input, skip sampling and load from cache.  
**Why:** Sampling 4.67M points is 20–60 minutes. Unchanged input should not re-trigger it. I made the pipeline idempotent so reruns are cheap when data hasn’t changed.

### 13. Out-of-coverage still get an answer

**AI:** Could error or hide results when the user asks about a location outside NC (e.g. San Jose).  
**Me:** Still run the agent and return a risk answer; flag “out of coverage” or “NC-only data” in the UI so it’s clear but not broken.  
**Why:** The system should degrade gracefully and stay useful. A banner is better than a dead screen when we expand to more states later.

### 14. Seasonal note as advisory, not stored score change

**AI:** Could adjust stored composite score by season (e.g. lower in winter).  
**Me:** Keep one stored score (peak-summer canopy). For deciduous/mixed forest (NLCD 41/43), add an **advisory note** in analyze_location that winter installations may see lower effective obstruction.  
**Why:** Two scores for the same location would break consistency of reports and comparisons. Advisory text informs the human without changing the canonical data. I chose clarity and consistency over a second seasonal score.

### 15. _personal and IRD not in repo

**AI:** Listed _personal in README tree or put IRD in the repo.  
**Me:** _personal is gitignored; don’t list it in the public README. IRD lives in _personal and is shared via Google, not in git.  
**Why:** Reviewers see only what’s in the repo. Private prep and IRD stay out of version control and out of the public file tree.

---

*Other divergence points over the build included: doc structure and what goes in README vs docs vs _personal; exact wording of error messages and user-facing copy; test coverage priorities; and when to add a utils layer for agent call patterns (deferred). I don’t list minor technical tweaks (e.g. model identifier strings) — the above are the ones that show how I steered the project toward the right problem and the right user experience.*

*Last updated: March 2026*
