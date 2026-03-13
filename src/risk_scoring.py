"""
Risk scoring methodology for LEO satellite coverage obstruction analysis.

Translates environmental measurements (canopy %, slope °, land cover code)
into a composite risk score and a categorical risk tier for each location.

Methodology source:
  Derived from Starlink Install Guide requirements: the dish needs a 100-110°
  unobstructed field of view. Each factor is scored independently on 0.0–1.0,
  then combined into a weighted composite (see WEIGHT_* in config.py).

Design principles:
  - All thresholds and weights are imported from config.py — nothing hardcoded.
  - All scorers are fully vectorized using numpy.where for performance at 1M+ rows.
  - NaN inputs produce NaN scores (no silent substitution). The validation layer
    (validation.py) handles NaN-scored locations as UNKNOWN tier.
  - Boundary values follow the plan's "strict greater-than" rule for HIGH tier:
      e.g. canopy > 50% → HIGH, canopy == 50% → MODERATE.
"""

import logging

import numpy as np
import pandas as pd

from src.config import (
    CANOPY_HIGH_THRESHOLD,
    CANOPY_MOD_THRESHOLD,
    DEVELOPED_CODES,
    FOREST_CODES,
    RISK_HIGH_THRESHOLD,
    RISK_MOD_THRESHOLD,
    RISK_TIER_HIGH,
    RISK_TIER_LOW,
    RISK_TIER_MODERATE,
    RISK_TIER_UNKNOWN,
    SLOPE_HIGH_THRESHOLD,
    SLOPE_MOD_THRESHOLD,
    WEIGHT_CANOPY,
    WEIGHT_LANDCOVER,
    WEIGHT_SLOPE,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual factor scorers
# ---------------------------------------------------------------------------

def score_canopy_risk(canopy_pct: np.ndarray | pd.Series) -> np.ndarray:
    """Score tree canopy cover as an obstruction risk factor.

    Tree branches are the most common cause of FOV obstruction cited in the
    Starlink install guide. Canopy % directly maps to signal obstruction risk.

    Parameters
    ----------
    canopy_pct:
        Array of tree canopy cover percentages (0–100). NaN is propagated.

    Returns
    -------
    np.ndarray
        Float array of risk scores:
        - 1.0  : canopy > CANOPY_HIGH_THRESHOLD  (>50% → HIGH)
        - 0.5  : canopy >= CANOPY_MOD_THRESHOLD  (20–50% → MODERATE)
        - 0.0  : canopy < CANOPY_MOD_THRESHOLD   (<20% → LOW)
        - NaN  : input was NaN
    """
    arr = np.asarray(canopy_pct, dtype=np.float64)
    return np.where(
        np.isnan(arr),
        np.nan,
        np.where(
            arr > CANOPY_HIGH_THRESHOLD,
            1.0,
            np.where(arr >= CANOPY_MOD_THRESHOLD, 0.5, 0.0),
        ),
    )


def score_slope_risk(slope_deg: np.ndarray | pd.Series) -> np.ndarray:
    """Score terrain slope as a sky-horizon obstruction risk factor.

    Steep terrain reduces the visible sky arc by raising the effective
    horizon, which blocks satellites at low elevation angles.

    Parameters
    ----------
    slope_deg:
        Array of terrain slope values in degrees (0–90). NaN is propagated.

    Returns
    -------
    np.ndarray
        Float array of risk scores:
        - 1.0  : slope > SLOPE_HIGH_THRESHOLD  (>20° → HIGH)
        - 0.5  : slope >= SLOPE_MOD_THRESHOLD  (10–20° → MODERATE)
        - 0.0  : slope < SLOPE_MOD_THRESHOLD   (<10° → LOW)
        - NaN  : input was NaN
    """
    arr = np.asarray(slope_deg, dtype=np.float64)
    return np.where(
        np.isnan(arr),
        np.nan,
        np.where(
            arr > SLOPE_HIGH_THRESHOLD,
            1.0,
            np.where(arr >= SLOPE_MOD_THRESHOLD, 0.5, 0.0),
        ),
    )


def score_landcover_risk(land_cover_code: np.ndarray | pd.Series) -> np.ndarray:
    """Score NLCD land cover class as a contextual obstruction risk factor.

    Land cover is a corroborating signal: it cross-validates the canopy data
    and provides a structural context score. A forest classification
    independently confirms high obstruction risk.

    Parameters
    ----------
    land_cover_code:
        Array of NLCD integer land cover codes. NaN is propagated.

    Returns
    -------
    np.ndarray
        Float array of risk scores:
        - 1.0  : FOREST codes (41, 42, 43) → HIGH
        - 0.5  : DEVELOPED codes (21, 22, 23, 24) → MODERATE
        - 0.0  : all other known codes (open, agricultural, barren, water)
        - NaN  : input was NaN
    """
    arr = np.asarray(land_cover_code, dtype=np.float64)
    forest = np.isin(arr, FOREST_CODES)
    developed = np.isin(arr, DEVELOPED_CODES)
    return np.where(
        np.isnan(arr),
        np.nan,
        np.where(forest, 1.0, np.where(developed, 0.5, 0.0)),
    )


# ---------------------------------------------------------------------------
# Composite scoring
# ---------------------------------------------------------------------------

def compute_composite_risk(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all three risk scorers and compute the weighted composite score.

    Composite formula (weights from config.py, must sum to 1.0):
        composite = canopy_risk * 0.50
                  + slope_risk  * 0.30
                  + landcover_risk * 0.20

    Risk tier assignment (thresholds from config.py):
        HIGH     : composite >= RISK_HIGH_THRESHOLD  (≥ 0.6)
        MODERATE : composite >= RISK_MOD_THRESHOLD   (≥ 0.3, < 0.6)
        LOW      : composite < RISK_MOD_THRESHOLD    (< 0.3)
        UNKNOWN  : composite is NaN (any input factor was NaN)

    NaN propagation:
        If any individual factor score is NaN, numpy arithmetic propagates
        NaN to the composite. These locations are flagged as UNKNOWN tier
        and surfaced by the validation layer for review.

    Parameters
    ----------
    df:
        Enriched locations DataFrame. Must contain columns:
        canopy_pct, slope_deg, land_cover_code (added by enrich_locations).

    Returns
    -------
    pd.DataFrame
        Copy of *df* with five new columns added:
        canopy_risk, slope_risk, landcover_risk, composite_score, risk_tier.

    Raises
    ------
    KeyError
        If any required input column is missing.
    """
    required = {"canopy_pct", "slope_deg", "land_cover_code"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(
            f"Missing required columns for risk scoring: {missing}. "
            "Run enrich_locations() before compute_composite_risk()."
        )

    result = df.copy()

    canopy_scores = score_canopy_risk(df["canopy_pct"].to_numpy())
    slope_scores = score_slope_risk(df["slope_deg"].to_numpy())
    landcover_scores = score_landcover_risk(df["land_cover_code"].to_numpy())

    composite = (
        canopy_scores * WEIGHT_CANOPY
        + slope_scores * WEIGHT_SLOPE
        + landcover_scores * WEIGHT_LANDCOVER
    )

    result["canopy_risk"] = canopy_scores
    result["slope_risk"] = slope_scores
    result["landcover_risk"] = landcover_scores
    result["composite_score"] = composite

    result["risk_tier"] = np.where(
        np.isnan(composite),
        RISK_TIER_UNKNOWN,
        np.where(
            composite >= RISK_HIGH_THRESHOLD,
            RISK_TIER_HIGH,
            np.where(composite >= RISK_MOD_THRESHOLD, RISK_TIER_MODERATE, RISK_TIER_LOW),
        ),
    )

    n_total = len(result)
    tier_counts = result["risk_tier"].value_counts().to_dict()
    logger.info(
        "Risk scoring complete: %d locations | HIGH=%d (%.1f%%) | MODERATE=%d (%.1f%%) | "
        "LOW=%d (%.1f%%) | UNKNOWN=%d (%.1f%%)",
        n_total,
        tier_counts.get(RISK_TIER_HIGH, 0),
        tier_counts.get(RISK_TIER_HIGH, 0) / n_total * 100,
        tier_counts.get(RISK_TIER_MODERATE, 0),
        tier_counts.get(RISK_TIER_MODERATE, 0) / n_total * 100,
        tier_counts.get(RISK_TIER_LOW, 0),
        tier_counts.get(RISK_TIER_LOW, 0) / n_total * 100,
        tier_counts.get(RISK_TIER_UNKNOWN, 0),
        tier_counts.get(RISK_TIER_UNKNOWN, 0) / n_total * 100,
    )

    return result
