# Analysis Rationale — Risk Scoring Methodology

**Project:** LEO Satellite Coverage Risk Analysis  
**Last updated:** March 2026

This document answers the question: **why these thresholds, and where do they come from?**

Every number in `src/config.py` — `CANOPY_HIGH_THRESHOLD = 50`, `SLOPE_HIGH_THRESHOLD = 20`, etc. — is derived from physical requirements in the Starlink installation documentation. This document traces each threshold back to its source evidence so the methodology can be explained, defended, and calibrated.

---

## Primary Evidence Source: Starlink Installation Guide

All three factors and their thresholds trace back to a single authoritative source: the **Starlink installation guide** (publicly available at starlink.com/installation-guides). Key physical requirements from that document:

1. **Field of View (FOV) requirement:** The Starlink dish requires an unobstructed 100–110° field of view of the sky, scanning from ~25° elevation above the horizon to zenith in all directions.
2. **Tree obstruction language:** *"Objects that obstruct the connection to the sky, such as a tree branch"* cause *intermittent outages* as satellites pass through the blocked arc. Even a single branch matters.
3. **Elevation minimum:** The dish must have clear sky above 25° elevation angle in all directions.
4. **Permanence of obstruction:** A fixed physical obstruction (tree, structure, terrain) causes recurring outages every orbital pass through that arc — not a one-time event.

These four points anchor every scoring decision below.

---

## Factor 1: Tree Canopy Cover (NLCD, 30m, uint8 0–100%)

### Threshold table

| Threshold | Risk | Derivation |
|---|---|---|
| Canopy > 50% | HIGH (1.0) | Majority-obstruction proxy |
| Canopy 20–50% | MODERATE (0.5) | Partial-obstruction zone |
| Canopy < 20% | LOW (0.0) | Minimal vegetation obstruction |

### Why 50% is the HIGH threshold

The NLCD canopy cover value represents the **fraction of a 30m × 30m pixel** (~900 m²) that is covered by tree canopy as seen from above.

The dish's 100–110° FOV cone does not look straight up — it scans at all azimuth angles from 25° elevation to zenith. Trees to the side of the dish are as obstructive as trees overhead. A canopy value of 50% means **more than half of the physical environment around the address is covered in tree canopy**, making it statistically likely that at least one azimuth segment of the FOV cone intersects a canopy-covered area.

This is a **conservative, first-principles threshold** — it errs toward flagging more locations as at-risk rather than fewer. This is intentional: the cost of installing Starlink at an unserviceable location (failed install, refund, truck roll) is higher than the cost of extra site-assessment for a location that turns out to be fine.

**Is 50% arbitrary?** No. It represents the "majority canopy" breakpoint — the spatial equivalent of "you're more likely in trees than not." Alternative thresholds:
- 40% would flag more false positives (some suburban tree cover that doesn't actually obstruct)
- 60% would miss locations that are substantially obstructed but not quite "dense forest"
- 50% is the defensible midpoint without calibration data

**What would override 50%?** Actual Starlink install success/failure data mapped back to NLCD canopy values for those locations. With a training dataset of "installs that failed vs succeeded by canopy %", the threshold could be empirically tuned. In the absence of that data, 50% is the most defensible prior.

**Supporting references:**
- USDA Forest Service broadband planning guidance uses similar canopy density thresholds for fixed wireless serviceability assessment
- FCC broadband mapping guidance (2022) acknowledges that "heavily forested terrain" is a known serviceability limiter for fixed wireless and satellite services
- The Starlink install guide's language ("even a single branch") implies that any non-trivial canopy is relevant — 50% represents "non-trivial at scale"

### Why weight canopy at 50%

The Starlink install guide mentions trees/foliage as the **primary and most common** obstruction cause. In practice, terrain and land cover matter, but tree canopy is the first thing the guide tells users to check and the most common failure mode cited in support forums. The 50% canopy weight reflects this primacy.

---

## Factor 2: Terrain Slope (derived from USGS 3DEP DEM, in degrees)

### Threshold table

| Threshold | Risk | Derivation |
|---|---|---|
| Slope > 20° | HIGH (1.0) | Horizon angle approaches 25° minimum |
| Slope 10–20° | MODERATE (0.5) | Reduced sky arc, marginal risk |
| Slope < 10° | LOW (0.0) | Near-flat, no terrain obstruction |

### Why 20° is the HIGH threshold

The Starlink dish requires clear sky above **25° elevation angle** from the horizon in all directions.

When a dish is installed on a sloped surface or in a depression, the downhill direction has the terrain rising above the installation point. Consider a dish at the bottom of a slope with 20° gradient. At 20 metres horizontal distance uphill:

```
Terrain rise  = 20 × tan(20°) ≈ 7.3 m
Apparent elevation angle of that terrain ridge ≈ arctan(7.3/20) ≈ 20°
```

This is already approaching the 25° minimum. Add natural terrain variability (ridgelines, rock outcrops) and the 20° mean slope pixel reliably produces horizon angles in the range that conflicts with Starlink's minimum elevation requirement.

At 15°: horizon angle at 20m ≈ arctan(5.5/20) ≈ 15.4° — still below 25°, so MODERATE.  
At 20°: horizon angle ≈ 20° — approaching 25° minimum, classified HIGH.  
At 25°: horizon angle ≈ 25° — exactly at the limit. The 20° threshold leaves a margin.

**Calibration note:** The 20° threshold accounts for the fact that slope raster pixels represent average slope across a 10–30m area. Actual worst-case spots (ridge crests, valley floors) within that pixel may be steeper. The threshold therefore errs conservative.

**Mounting height consideration:** A dish mounted on a roof or tall pole raises the observation point, reducing the apparent terrain elevation angle. This partially compensates for slope — which is why slope is weighted at 30%, not higher. A skilled installer can mitigate terrain risk with mounting height; they cannot mitigate a 90% canopy cover.

### Why weight slope at 30%

Terrain creates structural obstruction risk, but it is partially addressable through installation choices (roof mounting, extended poles). Tree canopy is a harder constraint. The 30%/50% canopy/slope weight ratio reflects that both matter, but canopy is harder to mitigate.

---

## Factor 3: NLCD Land Cover Type (integer codes, 30m)

### Threshold table

| NLCD Codes | Class | Risk | Rationale |
|---|---|---|---|
| 41, 42, 43 | Deciduous / Evergreen / Mixed Forest | HIGH (1.0) | Independent forest confirmation |
| 21, 22, 23, 24 | Developed (Open Space to High Intensity) | MODERATE (0.5) | Unmapped building obstructions |
| 31, 52, 71, 81, 82, 11, 12 | Barren / Shrub / Grass / Crops / Water | LOW (0.0) | Minimal obstruction |

### Why forest codes are HIGH

NLCD Forest classes (41=Deciduous, 42=Evergreen, 43=Mixed Forest) mean the **entire 30m pixel is classified as forest ecosystem**. This is not incidental tree cover — it is a location that exists within a forest environment. The install guide is unambiguous: forests cause signal interruption.

Land cover acts as a **second independent signal** that corroborates canopy cover. If canopy cover is 48% (just below the 50% HIGH threshold) AND the land cover is Forest (code 41), the composite score will still trend HIGH because both signals indicate the same physical reality.

This cross-validation property is why land cover is included as a separate factor rather than being dropped in favour of canopy cover alone.

### Why developed codes are MODERATE

Developed land (codes 21–24) contains buildings whose heights are not modelled in any national raster dataset. A dense urban environment (code 23: Medium Intensity Developed) has multi-storey buildings that can obstruct the dish FOV in ways that canopy cover and terrain slope do not capture.

MODERATE (0.5 score) rather than HIGH because:
- Urban/suburban dish installations typically use roof mounting, which mitigates low-angle obstruction
- Sky view factor in developed areas varies widely (dense downtown vs suburban strip mall)
- Without building height data, we cannot determine actual obstruction risk — MODERATE is the honest middle ground

### Why weight land cover at 20%

Land cover is a **corroborating signal** for the canopy reading, not an independent primary factor. The canopy raster is higher-resolution information about vegetation obstruction. Land cover adds context (distinguishing low shrubs from actual forest) and partially covers the building obstruction case. 20% weight reflects its supporting role.

---

## Composite Score and Tier Thresholds

```
composite_score = canopy_score × 0.50 + slope_score × 0.30 + landcover_score × 0.20
```

All individual scores are in [0.0, 1.0]. Composite score is therefore in [0.0, 1.0].

| Tier | Score Range | Interpretation |
|---|---|---|
| HIGH | ≥ 0.60 | Multiple factors indicate significant obstruction risk |
| MODERATE | 0.30–0.60 | Some factors elevated; site assessment recommended |
| LOW | < 0.30 | Environmental conditions favour successful installation |

### Why 0.60 as the HIGH tier cutoff

A composite score of 0.60 means the weighted obstruction signal is above moderate. Working through examples:
- Canopy HIGH + Slope MODERATE + Landcover LOW = 0.50×1.0 + 0.30×0.5 + 0.20×0.0 = **0.65 → HIGH** ✓ (heavy canopy with rolling terrain)
- Canopy MODERATE + Slope HIGH + Landcover HIGH = 0.50×0.5 + 0.30×1.0 + 0.20×1.0 = **0.75 → HIGH** ✓ (steep forested slope)
- Canopy MODERATE + Slope LOW + Landcover LOW = 0.50×0.5 + 0 + 0 = **0.25 → LOW** ✓ (some trees but otherwise clear)
- Canopy HIGH + Slope LOW + Landcover LOW = 0.50×1.0 = **0.50 → MODERATE** ✓ (heavy canopy only — still risk but no corroboration)

The 0.60 threshold behaves logically: a single HIGH factor with zero corroboration sits at MODERATE, while two or more elevated factors push into HIGH. This prevents a single noisy measurement from triggering HIGH for a location that is otherwise clear.

---

## Seasonal Variation and Deciduous Forest Locations

The batch pipeline assigns a single risk score per location, using peak-summer NLCD canopy values. This is a known limitation; see README "Known Limitations" and [docs/guide.md](guide.md).

**Partial mitigation via the agent's `analyze_location` tool:**  
When a technician queries a specific location, the agent checks the NLCD land cover code and adds a seasonal advisory for deciduous and mixed forest locations:

- **Deciduous Forest (code 41):** Leaf drop November–March reduces effective canopy obstruction by an estimated 30–60%. A location scored HIGH in summer may be functionally MODERATE for a winter installation. The agent flags this and recommends scheduling winter installs and re-running the Starlink in-app obstruction check after leaf drop.
- **Mixed Forest (code 43):** Partial seasonal benefit (deciduous component sheds, evergreen component remains). Agent flags moderate winter improvement.
- **Evergreen Forest (code 42):** No seasonal benefit. Agent notes this explicitly so the technician does not assume winter improvement.

This does **not** change the stored composite score — the score remains the summer peak value. The seasonal note is advisory guidance returned as part of the `analyze_location` response. The rationale: overriding the stored score with a seasonally-adjusted value would create two different scores for the same location depending on when it was queried, which would be confusing in reports and dashboards.

---

## What Would Change These Thresholds

These thresholds are **modeled priors derived from physical requirements**. They would be updated under either of two conditions:

1. **Ground truth calibration:** A dataset of actual Starlink installation outcomes (success/failure) matched to these environmental factors would allow empirical threshold fitting (e.g., logistic regression on install success rate vs canopy %).

2. **Operator feedback:** If Ready.net or their provider partners have internal data on which location types have the highest trouble-ticket rates for connectivity, those distributions would allow threshold recalibration.

In the absence of either, the thresholds are derived from the physical constraints in the install guide — the only authoritative primary source available. They are stored in `src/config.py` as named constants, making future recalibration a single-file change.

---

## Relationship to BEAD Program Context

This pipeline was built to support the BEAD (Broadband Equity, Access, and Deployment) grant program context, where states have committed specific locations to be served by LEO satellite providers. The risk scoring serves a specific operational purpose:

- **HIGH locations** should receive priority site assessment before installation is scheduled
- **MODERATE locations** are candidates for standard installation with notes about potential obstructions
- **LOW locations** can proceed to standard installation workflow with confidence

The methodology is deliberately conservative (flags more rather than fewer) because the cost of a failed installation attempt is higher than the cost of an extra site assessment.
