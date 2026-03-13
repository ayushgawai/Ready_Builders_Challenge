# Data Sourcing & Quality

**Project:** LEO Satellite Coverage Risk Analysis  
**Last updated:** March 2026

This document maps each dataset to a specific obstruction factor from the Starlink install guide, explains why it was chosen over alternatives, documents known quality issues, and explicitly lists what cannot be modeled with public data.

---

## Selected Datasets

### 1. NLCD Tree Canopy Cover 2021 (CONUS)

| Attribute | Value |
|---|---|
| **Source** | USGS / Multi-Resolution Land Characteristics Consortium (MRLC) |
| **URL** | https://www.mrlc.gov/data/nlcd-2021-usfs-tree-canopy-cover-conus |
| **Resolution** | 30 metres |
| **CRS** | EPSG:5070 (Albers Equal Area Conic — CONUS) |
| **Format** | GeoTIFF, uint8 |
| **Value range** | 0–100 (% of pixel covered by tree canopy) |
| **Download size** | ~1.5 GB (zip) |
| **Config key** | `CANOPY_RASTER_URL`, `CANOPY_RASTER_PATH` |

**Obstruction factor it models:**  
Tree branches are the most common cause of signal interruption cited in the Starlink install guide. "Objects that obstruct the connection... such as a tree branch" causes intermittent outages as satellites pass through the blocked arc. Canopy cover % directly quantifies this obstruction at each location.

**Why this dataset:**  
- Freely available, nationally consistent, produced by USGS (federal standard)
- Updated on multi-year cycles — 2021 is the most recent available
- 30m resolution is appropriate for 1M point analysis across CONUS
- Widely used in broadband planning and environmental analysis

**Limitations:**  
- Canopy % ≠ tree height. 80% canopy could be 6-foot shrubs or 120-foot pines. LiDAR point clouds resolve height but are not available nationally.
- 30m pixel covers an area larger than a single residence. The score represents the average for the surrounding area, not the specific yard.
- NLCD 2021 reflects peak summer (maximum leaf cover). Deciduous forests in winter may have significantly lower effective obstruction.

---

### 2. USGS 3DEP Digital Elevation Model — 1/3 Arc-Second (~10m)

| Attribute | Value |
|---|---|
| **Source** | USGS National Map / 3D Elevation Program (3DEP) |
| **URL** | https://www.usgs.gov/3d-elevation-program |
| **Download** | USGS National Map Downloader: https://apps.nationalmap.gov/downloader/ |
| **Resolution** | 1/3 arc-second ≈ 10 metres at mid-latitudes |
| **CRS** | EPSG:4269 (NAD83 geographic) or EPSG:26N (UTM, varies by tile) |
| **Format** | GeoTIFF, float32 (elevation in metres) |
| **Download size** | ~200-500 MB per state tile |
| **Config key** | `DEM_RASTER_PATH`, `SLOPE_RASTER_PATH` |

**Obstruction factor it models:**  
Steep terrain limits the visible sky arc. A Starlink dish installed in a narrow valley or on a steep hillside has a reduced elevation angle above the horizon, which cuts off access to satellites near the horizon — effectively shrinking the usable FOV cone. The terrain slope (derived from the DEM) quantifies this risk.

**Why this dataset:**  
- Freely available, authoritative — produced by USGS
- 10m resolution provides sub-parcel detail for rural locations
- Standard for terrain analysis in the US

**How slope is derived:**  
1. DEM downloaded and stored locally
2. Slope raster pre-computed using `numpy.gradient` (rise/run → arctan → degrees)
3. Cell sizes converted from degrees to metres using latitude-based approximation
4. Slope raster saved at `data/processed/slope_degrees.tif` for repeated sampling

**Limitations:**  
- DEM captures macroterrain (hills, valleys). Does not capture micro-obstructions like building rooflines at the dish mounting level.
- 10m resolution may still miss narrow ravines or cut slopes.

**Download note:**  
The full CONUS 3DEP dataset is assembled from state or regional tiles. Use the [USGS National Map Downloader](https://apps.nationalmap.gov/downloader/) to select 1/3 arc-second Elevation Products (3DEP) for your region of interest.

---

### 3. NLCD Land Cover Classification 2021 (CONUS)

| Attribute | Value |
|---|---|
| **Source** | USGS / Multi-Resolution Land Characteristics Consortium (MRLC) |
| **URL** | https://www.mrlc.gov/data/nlcd-2021-land-cover-conus |
| **Resolution** | 30 metres |
| **CRS** | EPSG:5070 (Albers Equal Area Conic — CONUS) |
| **Format** | GeoTIFF, uint8 |
| **Value range** | Integer codes (11, 21–24, 31, 41–43, 52, 71, 81–82, 90, 95) |
| **Download size** | ~900 MB (zip) |
| **Config key** | `LANDCOVER_RASTER_URL`, `LANDCOVER_RASTER_PATH` |

**Obstruction factor it models:**  
Land cover type provides a corroborating contextual signal. If a location is in a Forest class (codes 41, 42, 43), this independently confirms that canopy obstruction risk is high. If canopy % is high but land cover shows Developed land, the canopy data may be stale or misclassified — land cover acts as a cross-validation layer.

**Why this dataset:**  
- Same source and vintage (2021) as the canopy layer — consistent classification base
- Standard NLCD codes are well-documented and widely understood
- Adds context that pure canopy % cannot provide (e.g. distinguishes open grassland from low-tree shrubland)

**NLCD codes used in risk scoring:**

| Code(s) | Class | Risk Score |
|---|---|---|
| 41, 42, 43 | Deciduous, Evergreen, Mixed Forest | HIGH (1.0) |
| 21, 22, 23, 24 | Developed (Open Space to High Intensity) | MODERATE (0.5) |
| 31 | Barren Land | LOW (0.0) |
| 52 | Shrub/Scrub | LOW (0.0) |
| 71 | Grassland/Herbaceous | LOW (0.0) |
| 81, 82 | Pasture, Cultivated Crops | LOW (0.0) |
| 11, 12 | Open Water, Ice/Snow | LOW (0.0) |
| Other | Unknown/unlisted code | LOW (0.0) |

**Limitations:**  
- 30m resolution — same spatial limitations as canopy layer
- Land cover codes are updated every 5 years; 2021 vintage may not reflect recent deforestation or development

---

## Datasets Considered and Rejected

| Dataset | Why Rejected |
|---|---|
| **Microsoft US Building Footprints** | Provides polygon footprints but no building height data. Cannot model obstruction angle without height. |
| **OpenStreetMap Buildings** | Sparse and inconsistent in rural areas — exactly where most underserved locations are. |
| **SRTM DEM (NASA, 90m)** | Lower resolution than 3DEP for CONUS. 3DEP is the authoritative US source. |
| **Per-point elevation API (USGS/Google)** | Rate-limited. At 1M locations, API-based elevation collection is impractical in terms of latency and cost. A one-time raster download is the right architecture. |
| **LiDAR point clouds (USGS 3DEP LiDAR)** | Available for many areas but not nationally consistent. Extremely large files. Would require significant processing infrastructure. Strong candidate for a Phase 2 production system. |

---

## Known Quality Issues in the Locations CSV

> **Note:** The actual ~1M location CSV has not been received at time of writing. The issues documented below are based on common patterns in FCC/BEAD provider submission data, which this dataset is compiled from. They will be updated once the real data is received.

### Expected issues based on FCC/BEAD filing patterns:

| Issue | Expected Volume | Handling |
|---|---|---|
| **Duplicate location IDs** | Low-moderate (filings from multiple periods) | Keep first occurrence; log count |
| **Coordinates outside CONUS bounds** | Low (Hawaii, Puerto Rico, territories) | Drop with reason logged |
| **Null coordinates** | Very low (data entry errors) | Drop with reason logged |
| **Missing state/county** | Unknown | Warn and continue; state-level reporting skipped for affected rows |
| **Coordinate precision variations** | Common (4 vs 6 decimal places) | No issue — stored as float64 |
| **Slight coordinate offsets** | Possible (address geocoding drift) | Within 30m raster tolerance — no action needed |

### Schema assumption:

The challenge description guarantees: `location_id`, `latitude`, `longitude`.  
Our pipeline additionally expects: `state`, `county`.

These are present in FCC BEAD submission data, but if absent, the pipeline will:
- Warn (not fail) on load
- Proceed with all analysis using coordinates only
- Skip state/county level aggregation in the report

**Open question for Ready.net team:** Can you confirm the exact schema and column names for the locations CSV?

---

## What Cannot Be Modeled with Public Data

| Factor | Why Unmodelable Remotely | Impact |
|---|---|---|
| **Building heights** | No national height dataset at parcel resolution | Cannot model rooftop obstruction angles |
| **Exact tree heights** | Canopy % ≠ height; requires LiDAR | 80% canopy from 6ft shrubs vs 80ft pines have different obstruction profiles |
| **Seasonal canopy variation** | NLCD = peak summer. Deciduous trees lose 80-90% leaf cover in winter | Winter obstruction may be significantly lower for deciduous forest locations |
| **Microsite mounting options** | Requires site visit | Rooftop availability, pole mounting options, HOA restrictions |
| **Obstruction below dish height** | DEM gives ground elevation; dish mounted at roof height | True sky clearance depends on mounting height, not ground slope |
| **Real-time vegetation changes** | Rasters updated every 5 years | Recent clearing or new growth not captured |

---

## Download Instructions (Manual)

If automatic download fails (MRLC server issues are common), download manually:

**NLCD Tree Canopy Cover 2021:**  
1. Go to https://www.mrlc.gov/viewer/
2. Select "Tree Canopy Cover" → "2021" → "CONUS"
3. Download and unzip; place `.tif` at `data/raw/nlcd_tcc_conus_2021.tif`

**NLCD Land Cover 2021:**  
1. Same portal → "Land Cover" → "2021" → "L48"
2. Place `.tif` at `data/raw/nlcd_landcover_conus_2021.tif`

**USGS 3DEP DEM:**  
1. Go to https://apps.nationalmap.gov/downloader/
2. Select "Elevation Products (3DEP)" → "1/3 arc-second" → draw your area of interest
3. Download tile(s) and merge; place merged `.tif` at `data/raw/dem_conus.tif`
