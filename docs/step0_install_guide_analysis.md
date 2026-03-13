# Step 0: Starlink Install Guide Analysis

**Challenge Requirement:** Before writing code, read the Starlink install guide and answer the four questions below. Use those answers to drive data sourcing and methodology.

**Source:** [StarlinkInstallGuide_Business_English.pdf](https://starlink.com/public-files/StarlinkInstallGuide_Business_English.pdf)

---

## Q1. What physical conditions cause service interruptions?

Service interruptions occur whenever **any object obstructs the dish's field of view (FOV)** — the unobstructed cone of sky the dish needs to communicate with LEO satellites.

Specific obstructions cited in the install guide:
- **Tree branches** — the most common cause; even a single branch crossing the FOV causes intermittent outages
- **Roof edges and overhangs** — obstruct the lower elevation angles of the FOV cone
- **Poles and wires** — narrow vertical obstructions that create recurring dropout events as the dish tracks satellites through them
- **Walls and structures** — block large sections of the FOV at lower elevations
- **Terrain features** — hills and ridges reduce visible sky horizon, effectively cutting off access to satellites near the horizon

The dish requires a **minimum ~25° elevation angle above the horizon** to be clear in all directions. Obstructions below this elevation line are less impactful but still contribute to degradation.

Even **partial, transient obstructions** matter: because LEO satellites at ~550 km altitude are constantly moving across the sky, a dish must access the entire arc — not just one fixed direction. A tree that blocks 20% of the cone will cause brief outages every few minutes throughout the day.

---

## Q2. What does the dish need from its environment to maintain connectivity?

The Starlink dish requires:

1. **100–110° unobstructed field of view** (roughly a cone centered slightly north of vertical in the US). This is the single most critical requirement.

2. **Clear access to the full sky arc** — not just straight up. Satellites pass at varying elevation angles. Low-elevation access is especially important because that's where the sky arc is widest and satellites spend the most transit time.

3. **Elevated mounting when ground is obstructed** — the guide recommends roof or pole mounting to clear vegetation and terrain obstacles. Ground mounting in a clearing is acceptable only when the surrounding area is genuinely open.

4. **Stable, vibration-free mounting** — the dish auto-levels and auto-tilts to its optimal orientation. Movement or vibration degrades the mechanical pointing accuracy.

5. **Slightly northward tilt preference** — in the Northern Hemisphere (US), the satellite constellation concentrates toward the north. The dish auto-adjusts, but a northward-facing installation point reduces correction angle and improves signal stability.

---

## Q3. What publicly available geospatial datasets would let you model these risks at scale?

Three national datasets directly map the physical obstructions identified above:

| Dataset | Source | Resolution | Obstruction Factor It Models |
|---|---|---|---|
| **NLCD Tree Canopy Cover 2021** | USGS / MRLC | 30 m | Tree branches and vegetation obstructing FOV cone |
| **USGS 3DEP Digital Elevation Model** | USGS | 10–30 m | Terrain elevation; used to derive slope and identify valleys/ridges |
| **NLCD Land Cover Classification 2021** | USGS / MRLC | 30 m | Land use context; validates canopy data and identifies structural development density |

**Why these three:**
- They are freely available, nationally consistent, and regularly updated by USGS
- 30m resolution is appropriate for 1M+ locations across CONUS — fine enough to distinguish forested from open land, without excessive storage overhead
- They are standard in the geospatial industry and well-documented, making the methodology auditable and reproducible

**How they map to risk:**
- High canopy cover → high obstruction risk (tree branches in FOV)
- Steep terrain slope → horizon obstruction risk (ridge or valley walls cutting sky access)
- Forest land cover code → corroborating signal; confirms canopy data and adds confidence

---

## Q4. What can't you model remotely — and why?

Remote sensing with 30m rasters has real limits. The following factors **cannot be adequately captured** with publicly available national datasets:

| Factor | Why It Can't Be Modeled Remotely |
|---|---|
| **Individual building heights and positions** | No national dataset exists at sub-parcel resolution. Building footprint data (e.g., Microsoft USBuildingFootprints) has footprints but not heights or obstruction angles. |
| **Exact tree heights** | Canopy cover % from NLCD tells you *how much* of a pixel is covered, not *how tall* the trees are. A 90% canopy could be 5-foot shrubs or 120-foot pines. LiDAR point clouds (available in some states) would resolve this. |
| **Seasonal variation** | NLCD captures peak-season (summer) canopy. Deciduous forests can lose 80-90% of leaf cover in winter, dramatically reducing FOV obstruction. Seasonal risk is real but unmapped. |
| **Microsite mounting options** | Whether a specific address has a viable rooftop, clear pole location, or is stuck at ground level with no clear sky requires on-site assessment. Two neighboring addresses can have wildly different installation outcomes. |
| **HOA and structural restrictions** | Deed restrictions, HOA rules, roof condition, and renter/owner status all affect whether a clear-sky installation is actually achievable. Entirely outside the scope of remote analysis. |
| **Understory density** | A forest pixel might have 80% canopy cover but open understory (no obstruction below 10 feet). Or dense scrub at 3 feet. Canopy % alone doesn't capture this. |

**Practical implication for our analysis:** Our risk scores identify *candidate locations for follow-up review*, not definitive service failures. A HIGH risk score means "conditions at this location are likely to create connectivity challenges that warrant on-site assessment." It does not mean "Starlink will not work here."

This is the correct framing for communicating findings to a state broadband officer: **our model narrows the field from 1M locations to a prioritized shortlist for ground-truthing.**

---

## Dataset Selection Rationale

After completing the install guide analysis, the following datasets were selected for the pipeline:

1. **NLCD Tree Canopy Cover 2021 (CONUS)** — primary obstruction signal, weighted 50% in composite score
2. **USGS 3DEP 1/3 arc-second DEM** — used to compute terrain slope, weighted 30%
3. **NLCD Land Cover 2021 (CONUS)** — contextual cross-validation, weighted 20%

Datasets considered and rejected:
- **Microsoft Building Footprints** — provides footprint polygons but no height data; can't model obstruction angle
- **OpenStreetMap buildings** — incomplete coverage in rural areas where most underserved locations are
- **SRTM DEM (NASA)** — 90m resolution; used for global analysis but 3DEP is higher resolution for CONUS
- **Per-point elevation APIs (USGS, Google)** — rate-limited and high latency; infeasible at 1M locations without significant cost and time

Full dataset sourcing details are documented in [`docs/data_sourcing.md`](data_sourcing.md).
