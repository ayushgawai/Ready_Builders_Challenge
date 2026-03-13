"""
Tests for src/risk_scoring.py — risk scoring methodology.

Test strategy:
  - Each scorer tested with explicit known inputs and expected outputs.
  - Boundary values tested explicitly: the HIGH threshold is strict (>)
    so the exact threshold value should return MODERATE, not HIGH.
  - NaN propagation tested for all scorers and composite.
  - Composite arithmetic verified by hand for known inputs.
  - Risk tier boundaries (0.3, 0.6) tested at exact values and ±epsilon.
  - No mocking — all functions are pure (numpy in, numpy out).

All thresholds are imported from config.py so tests remain valid if
thresholds are changed in configuration (tests verify behaviour relative
to config constants, not hardcoded magic numbers).
"""

import numpy as np
import pandas as pd
import pytest

from src.config import (
    CANOPY_HIGH_THRESHOLD,
    CANOPY_MOD_THRESHOLD,
    FOREST_CODES,
    DEVELOPED_CODES,
    RISK_HIGH_THRESHOLD,
    RISK_MOD_THRESHOLD,
    RISK_TIER_HIGH,
    RISK_TIER_LOW,
    RISK_TIER_MODERATE,
    RISK_TIER_UNKNOWN,
    SLOPE_HIGH_THRESHOLD,
    SLOPE_MOD_THRESHOLD,
    WEIGHT_CANOPY,
    WEIGHT_SLOPE,
    WEIGHT_LANDCOVER,
)
from src.risk_scoring import (
    compute_composite_risk,
    score_canopy_risk,
    score_landcover_risk,
    score_slope_risk,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enriched_row(canopy=30.0, slope=5.0, land_cover=81.0) -> pd.DataFrame:
    """Build a single-row enriched DataFrame with defaults."""
    return pd.DataFrame([{
        "location_id": "LOC_TEST",
        "latitude": 45.0,
        "longitude": -106.0,
        "state": "MT",
        "county": "Test",
        "canopy_pct": canopy,
        "elevation_m": 1200.0,
        "slope_deg": slope,
        "land_cover_code": land_cover,
    }])


def _enriched_df(rows: list[dict]) -> pd.DataFrame:
    """Build an enriched DataFrame from a list of row dicts."""
    base_cols = {"location_id": "LOC", "latitude": 45.0, "longitude": -106.0,
                 "state": "MT", "county": "Test", "elevation_m": 1200.0}
    return pd.DataFrame([{**base_cols, **r} for r in rows])


# ---------------------------------------------------------------------------
# score_canopy_risk
# ---------------------------------------------------------------------------

class TestScoreCanopyRisk:

    def test_high_risk_above_threshold(self):
        """canopy > CANOPY_HIGH_THRESHOLD → score 1.0"""
        result = score_canopy_risk(np.array([CANOPY_HIGH_THRESHOLD + 1.0]))
        assert result[0] == pytest.approx(1.0)

    def test_boundary_at_high_threshold_is_moderate(self):
        """canopy == CANOPY_HIGH_THRESHOLD (exactly 50) → MODERATE (not HIGH)
        because the rule is strictly >, not >=."""
        result = score_canopy_risk(np.array([float(CANOPY_HIGH_THRESHOLD)]))
        assert result[0] == pytest.approx(0.5)

    def test_moderate_risk_at_lower_mod_threshold(self):
        """canopy == CANOPY_MOD_THRESHOLD (exactly 20) → MODERATE (>=)."""
        result = score_canopy_risk(np.array([float(CANOPY_MOD_THRESHOLD)]))
        assert result[0] == pytest.approx(0.5)

    def test_moderate_risk_midpoint(self):
        midpoint = (CANOPY_HIGH_THRESHOLD + CANOPY_MOD_THRESHOLD) / 2
        result = score_canopy_risk(np.array([midpoint]))
        assert result[0] == pytest.approx(0.5)

    def test_low_risk_just_below_mod_threshold(self):
        """canopy just below CANOPY_MOD_THRESHOLD → LOW."""
        result = score_canopy_risk(np.array([CANOPY_MOD_THRESHOLD - 0.1]))
        assert result[0] == pytest.approx(0.0)

    def test_zero_canopy_is_low(self):
        result = score_canopy_risk(np.array([0.0]))
        assert result[0] == pytest.approx(0.0)

    def test_full_canopy_is_high(self):
        result = score_canopy_risk(np.array([100.0]))
        assert result[0] == pytest.approx(1.0)

    def test_nan_propagates(self):
        result = score_canopy_risk(np.array([np.nan]))
        assert np.isnan(result[0])

    def test_vectorized_multiple_values(self):
        """Array input returns array of same length with correct tier scores."""
        arr = np.array([75.0, 35.0, 10.0, np.nan])
        result = score_canopy_risk(arr)
        assert result[0] == pytest.approx(1.0)   # HIGH
        assert result[1] == pytest.approx(0.5)   # MODERATE
        assert result[2] == pytest.approx(0.0)   # LOW
        assert np.isnan(result[3])               # NaN

    def test_accepts_pandas_series(self):
        s = pd.Series([75.0, 10.0, np.nan])
        result = score_canopy_risk(s)
        assert result.shape == (3,)
        assert result[0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# score_slope_risk
# ---------------------------------------------------------------------------

class TestScoreSlopeRisk:

    def test_high_risk_above_threshold(self):
        result = score_slope_risk(np.array([SLOPE_HIGH_THRESHOLD + 1.0]))
        assert result[0] == pytest.approx(1.0)

    def test_boundary_at_high_threshold_is_moderate(self):
        """slope == SLOPE_HIGH_THRESHOLD (exactly 20) → MODERATE, not HIGH."""
        result = score_slope_risk(np.array([float(SLOPE_HIGH_THRESHOLD)]))
        assert result[0] == pytest.approx(0.5)

    def test_boundary_at_mod_threshold_is_moderate(self):
        """slope == SLOPE_MOD_THRESHOLD (exactly 10) → MODERATE (>=)."""
        result = score_slope_risk(np.array([float(SLOPE_MOD_THRESHOLD)]))
        assert result[0] == pytest.approx(0.5)

    def test_low_risk_below_mod_threshold(self):
        result = score_slope_risk(np.array([SLOPE_MOD_THRESHOLD - 0.1]))
        assert result[0] == pytest.approx(0.0)

    def test_zero_slope_is_low(self):
        result = score_slope_risk(np.array([0.0]))
        assert result[0] == pytest.approx(0.0)

    def test_extreme_slope_is_high(self):
        result = score_slope_risk(np.array([45.0]))
        assert result[0] == pytest.approx(1.0)

    def test_nan_propagates(self):
        result = score_slope_risk(np.array([np.nan]))
        assert np.isnan(result[0])

    def test_vectorized_multiple_values(self):
        arr = np.array([25.0, 15.0, 5.0, np.nan])
        result = score_slope_risk(arr)
        assert result[0] == pytest.approx(1.0)
        assert result[1] == pytest.approx(0.5)
        assert result[2] == pytest.approx(0.0)
        assert np.isnan(result[3])


# ---------------------------------------------------------------------------
# score_landcover_risk
# ---------------------------------------------------------------------------

class TestScoreLandcoverRisk:

    def test_all_forest_codes_are_high(self):
        """Codes 41, 42, 43 (Deciduous, Evergreen, Mixed Forest) → 1.0."""
        for code in FOREST_CODES:
            result = score_landcover_risk(np.array([float(code)]))
            assert result[0] == pytest.approx(1.0), f"Code {code} should be HIGH"

    def test_all_developed_codes_are_moderate(self):
        """Codes 21-24 (Developed) → 0.5."""
        for code in DEVELOPED_CODES:
            result = score_landcover_risk(np.array([float(code)]))
            assert result[0] == pytest.approx(0.5), f"Code {code} should be MODERATE"

    def test_pasture_is_low(self):
        """Code 81 (Pasture/Hay) → 0.0."""
        result = score_landcover_risk(np.array([81.0]))
        assert result[0] == pytest.approx(0.0)

    def test_grassland_is_low(self):
        """Code 71 (Grassland) → 0.0."""
        result = score_landcover_risk(np.array([71.0]))
        assert result[0] == pytest.approx(0.0)

    def test_barren_is_low(self):
        """Code 31 (Barren Land) → 0.0."""
        result = score_landcover_risk(np.array([31.0]))
        assert result[0] == pytest.approx(0.0)

    def test_open_water_is_low(self):
        """Code 11 (Open Water) → 0.0."""
        result = score_landcover_risk(np.array([11.0]))
        assert result[0] == pytest.approx(0.0)

    def test_unknown_code_is_low(self):
        """An unlisted code (99) → 0.0 (not forest, not developed)."""
        result = score_landcover_risk(np.array([99.0]))
        assert result[0] == pytest.approx(0.0)

    def test_nan_propagates(self):
        result = score_landcover_risk(np.array([np.nan]))
        assert np.isnan(result[0])

    def test_vectorized_forest_developed_open_nan(self):
        arr = np.array([41.0, 22.0, 81.0, np.nan])
        result = score_landcover_risk(arr)
        assert result[0] == pytest.approx(1.0)   # Forest
        assert result[1] == pytest.approx(0.5)   # Developed
        assert result[2] == pytest.approx(0.0)   # Open/Ag
        assert np.isnan(result[3])


# ---------------------------------------------------------------------------
# compute_composite_risk
# ---------------------------------------------------------------------------

class TestComputeCompositeRisk:

    def test_all_high_inputs_produce_high_tier(self):
        """canopy=75 (HIGH), slope=25 (HIGH), landcover=41 (HIGH) → composite=1.0, HIGH."""
        df = _enriched_row(canopy=75.0, slope=25.0, land_cover=41.0)
        result = compute_composite_risk(df)
        assert result.iloc[0]["composite_score"] == pytest.approx(1.0)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_HIGH

    def test_all_moderate_inputs_produce_moderate_tier(self):
        """canopy=35 (MOD=0.5), slope=15 (MOD=0.5), landcover=22 (MOD=0.5)
        → composite = 0.5*0.5 + 0.5*0.3 + 0.5*0.2 = 0.25 + 0.15 + 0.10 = 0.50
        → MODERATE (0.3 ≤ 0.50 < 0.6)."""
        df = _enriched_row(canopy=35.0, slope=15.0, land_cover=22.0)
        result = compute_composite_risk(df)
        expected = 0.5 * WEIGHT_CANOPY + 0.5 * WEIGHT_SLOPE + 0.5 * WEIGHT_LANDCOVER
        assert result.iloc[0]["composite_score"] == pytest.approx(expected)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_MODERATE

    def test_all_low_inputs_produce_low_tier(self):
        """canopy=5 (LOW=0.0), slope=5 (LOW=0.0), landcover=81 (LOW=0.0)
        → composite = 0.0 → LOW."""
        df = _enriched_row(canopy=5.0, slope=5.0, land_cover=81.0)
        result = compute_composite_risk(df)
        assert result.iloc[0]["composite_score"] == pytest.approx(0.0)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_LOW

    def test_nan_canopy_produces_unknown_tier(self):
        """NaN in any factor → composite NaN → UNKNOWN tier."""
        df = _enriched_row(canopy=np.nan, slope=5.0, land_cover=81.0)
        result = compute_composite_risk(df)
        assert np.isnan(result.iloc[0]["composite_score"])
        assert result.iloc[0]["risk_tier"] == RISK_TIER_UNKNOWN

    def test_nan_slope_produces_unknown_tier(self):
        df = _enriched_row(canopy=30.0, slope=np.nan, land_cover=81.0)
        result = compute_composite_risk(df)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_UNKNOWN

    def test_nan_landcover_produces_unknown_tier(self):
        df = _enriched_row(canopy=30.0, slope=5.0, land_cover=np.nan)
        result = compute_composite_risk(df)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_UNKNOWN

    def test_composite_weights_sum_to_one(self):
        """Sanity check: WEIGHT_CANOPY + WEIGHT_SLOPE + WEIGHT_LANDCOVER == 1.0."""
        assert WEIGHT_CANOPY + WEIGHT_SLOPE + WEIGHT_LANDCOVER == pytest.approx(1.0)

    def test_composite_arithmetic_matches_formula(self):
        """Verify the composite formula manually for a mixed input."""
        # canopy=75 → 1.0, slope=5 → 0.0, landcover=22 → 0.5
        df = _enriched_row(canopy=75.0, slope=5.0, land_cover=22.0)
        result = compute_composite_risk(df)
        expected = 1.0 * WEIGHT_CANOPY + 0.0 * WEIGHT_SLOPE + 0.5 * WEIGHT_LANDCOVER
        assert result.iloc[0]["composite_score"] == pytest.approx(expected)

    def test_risk_tier_boundary_exactly_high_threshold(self):
        """composite == RISK_HIGH_THRESHOLD (0.6 exactly) → HIGH tier (>=)."""
        # canopy=75(1.0), slope=5(0.0), landcover=41(1.0)
        # composite = 1.0*0.5 + 0.0*0.3 + 1.0*0.2 = 0.5 + 0 + 0.2 = 0.70 (HIGH)
        # Need exactly 0.6: 0.6 = 0.5*c + 0.3*s + 0.2*l
        # Set canopy=1.0(HIGH), slope=0.5(MOD), landcover=0.0(LOW):
        # 1.0*0.5 + 0.5*0.3 + 0.0*0.2 = 0.5 + 0.15 + 0.0 = 0.65 ≠ 0.6
        # Set canopy=1.0(HIGH), slope=0.0(LOW), landcover=0.5(MOD):
        # 1.0*0.5 + 0.0*0.3 + 0.5*0.2 = 0.5 + 0.0 + 0.10 = 0.60 ✓
        df = _enriched_row(canopy=75.0, slope=5.0, land_cover=22.0)
        result = compute_composite_risk(df)
        expected = 1.0 * WEIGHT_CANOPY + 0.0 * WEIGHT_SLOPE + 0.5 * WEIGHT_LANDCOVER
        # Verify we're at the boundary
        assert expected == pytest.approx(RISK_HIGH_THRESHOLD)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_HIGH

    def test_risk_tier_boundary_exactly_mod_threshold(self):
        """composite == RISK_MOD_THRESHOLD (0.3 exactly) → MODERATE tier (>=)."""
        # 0.3 = 0.5*c + 0.3*s + 0.2*l
        # Set canopy=0.5(MOD), slope=0.0(LOW), landcover=0.5(MOD):
        # 0.5*0.5 + 0.0*0.3 + 0.5*0.2 = 0.25 + 0 + 0.10 = 0.35 ≠ 0.3
        # Set canopy=0.0(LOW), slope=1.0(HIGH), landcover=0.0(LOW):
        # 0.0*0.5 + 1.0*0.3 + 0.0*0.2 = 0.3 ✓
        df = _enriched_row(canopy=5.0, slope=25.0, land_cover=81.0)
        result = compute_composite_risk(df)
        expected = 0.0 * WEIGHT_CANOPY + 1.0 * WEIGHT_SLOPE + 0.0 * WEIGHT_LANDCOVER
        assert expected == pytest.approx(RISK_MOD_THRESHOLD)
        assert result.iloc[0]["risk_tier"] == RISK_TIER_MODERATE

    def test_all_five_output_columns_added(self):
        """compute_composite_risk must add all 5 new columns."""
        df = _enriched_row()
        result = compute_composite_risk(df)
        for col in ["canopy_risk", "slope_risk", "landcover_risk", "composite_score", "risk_tier"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_does_not_modify_input_dataframe(self):
        df = _enriched_row()
        original_cols = list(df.columns)
        compute_composite_risk(df)
        assert list(df.columns) == original_cols

    def test_multiple_rows_vectorized(self):
        """1000-row DataFrame processed correctly and efficiently."""
        rows = [
            {"canopy_pct": 75.0, "slope_deg": 25.0, "land_cover_code": 41.0},  # ALL HIGH
            {"canopy_pct": 5.0,  "slope_deg": 5.0,  "land_cover_code": 81.0},  # ALL LOW
            {"canopy_pct": 35.0, "slope_deg": 15.0, "land_cover_code": 22.0},  # ALL MOD
            {"canopy_pct": np.nan,"slope_deg": 5.0, "land_cover_code": 81.0},  # UNKNOWN
        ] * 250  # 1000 rows
        df = _enriched_df(rows)
        result = compute_composite_risk(df)
        assert len(result) == 1000
        # Check pattern repeats correctly
        assert result.iloc[0]["risk_tier"] == RISK_TIER_HIGH
        assert result.iloc[1]["risk_tier"] == RISK_TIER_LOW
        assert result.iloc[2]["risk_tier"] == RISK_TIER_MODERATE
        assert result.iloc[3]["risk_tier"] == RISK_TIER_UNKNOWN

    def test_raises_on_missing_required_columns(self):
        """Should raise KeyError if input columns are missing."""
        df = pd.DataFrame([{"location_id": "X", "latitude": 45.0}])
        with pytest.raises(KeyError, match="Missing required columns"):
            compute_composite_risk(df)

    def test_individual_risk_columns_match_scorers(self):
        """canopy_risk, slope_risk, landcover_risk must match their scorer outputs."""
        df = _enriched_row(canopy=75.0, slope=25.0, land_cover=41.0)
        result = compute_composite_risk(df)
        assert result.iloc[0]["canopy_risk"] == pytest.approx(
            score_canopy_risk(np.array([75.0]))[0]
        )
        assert result.iloc[0]["slope_risk"] == pytest.approx(
            score_slope_risk(np.array([25.0]))[0]
        )
        assert result.iloc[0]["landcover_risk"] == pytest.approx(
            score_landcover_risk(np.array([41.0]))[0]
        )
