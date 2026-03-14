"""
Tests for src/validation.py — result validation and anomaly detection.

Test strategy:
  - validate_results: tested with programmatically constructed DataFrames
    targeting each specific check in isolation and in combination.
  - generate_anomaly_report: tested for output format, content, and disk write.
  - All DataFrames are built in-memory — no CSV or binary files committed.

Helper _make_scored_df():
  Builds a minimal valid scored DataFrame with n rows at a given tier.
  Overrides allow surgical injection of specific anomaly types.
"""

import pandas as pd
import pytest

from src.validation import (
    _REQUIRED_COLUMNS,
    generate_anomaly_report,
    validate_results,
)
from src.config import (
    DOMINANT_TIER_THRESHOLD,
    FOREST_LOW_CANOPY_THRESHOLD,
    RISK_TIER_HIGH,
    RISK_TIER_LOW,
    RISK_TIER_MODERATE,
    RISK_TIER_UNKNOWN,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scored_df(
    n: int = 5,
    tier: str = "MODERATE",
    canopy_pct: float = 35.0,
    slope_deg: float = 15.0,
    land_cover_code: int = 21,
    composite_score: float = 0.5,
) -> pd.DataFrame:
    """Build a minimal all-valid scored DataFrame with *n* identical rows."""
    row = {
        "location_id": None,  # filled below
        "canopy_pct": canopy_pct,
        "slope_deg": slope_deg,
        "land_cover_code": land_cover_code,
        "canopy_risk": 0.5,
        "slope_risk": 0.5,
        "landcover_risk": 0.5,
        "composite_score": composite_score,
        "risk_tier": tier,
    }
    rows = [{**row, "location_id": f"LOC_{i:04d}"} for i in range(n)]
    return pd.DataFrame(rows)


def _make_mixed_scored_df(n: int = 9) -> pd.DataFrame:
    """Build a mixed-tier scored DataFrame with roughly equal tier distribution.

    Produces rows cycling through HIGH / MODERATE / LOW so no single tier
    dominates (avoids triggering the dominance check when testing other checks).
    n should be divisible by 3 for a perfectly balanced split; otherwise the
    last tier gets the remainder.
    """
    tiers = [RISK_TIER_HIGH, RISK_TIER_MODERATE, RISK_TIER_LOW]
    scores = {RISK_TIER_HIGH: 1.0, RISK_TIER_MODERATE: 0.5, RISK_TIER_LOW: 0.0}
    rows = []
    for i in range(n):
        tier = tiers[i % 3]
        rows.append({
            "location_id": f"MX_{i:04d}",
            "canopy_pct": 35.0 if tier == RISK_TIER_MODERATE else (75.0 if tier == RISK_TIER_HIGH else 5.0),
            "slope_deg": 15.0 if tier == RISK_TIER_MODERATE else (25.0 if tier == RISK_TIER_HIGH else 2.0),
            "land_cover_code": 21,  # Developed — not forest (avoids cross-validation trigger)
            "canopy_risk": scores[tier],
            "slope_risk": scores[tier],
            "landcover_risk": 0.5,
            "composite_score": scores[tier],
            "risk_tier": tier,
        })
    return pd.DataFrame(rows)


def _inject_nan_rows(df: pd.DataFrame, count: int) -> pd.DataFrame:
    """Replace the last *count* rows with NaN composite_score / UNKNOWN tier."""
    df = df.copy()
    idx = df.index[-count:]
    df.loc[idx, "composite_score"] = float("nan")
    df.loc[idx, "canopy_pct"] = float("nan")
    df.loc[idx, "slope_deg"] = float("nan")
    df.loc[idx, "risk_tier"] = RISK_TIER_UNKNOWN
    return df


# ---------------------------------------------------------------------------
# validate_results — missing columns guard
# ---------------------------------------------------------------------------

class TestValidateResultsInputGuard:

    def test_raises_on_missing_column(self):
        df = pd.DataFrame({"canopy_pct": [30.0], "slope_deg": [10.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            validate_results(df)

    def test_raises_lists_missing_columns(self):
        df = pd.DataFrame({"canopy_pct": [30.0]})
        with pytest.raises(ValueError, match="slope_deg"):
            validate_results(df)

    def test_accepts_all_required_columns_present(self, clean_scored_df):
        is_valid, _ = validate_results(clean_scored_df)
        assert isinstance(is_valid, bool)


# ---------------------------------------------------------------------------
# validate_results — UNKNOWN tier rate (Check 1)
# ---------------------------------------------------------------------------

class TestValidateResultsUnknownRate:

    def test_no_unknown_tiers_is_valid(self, clean_scored_df):
        is_valid, report = validate_results(clean_scored_df)
        assert is_valid is True
        assert report["unknown_tier_count"] == 0
        assert report["unknown_tier_pct"] == 0.0

    def test_moderate_unknown_rate_is_warning_not_critical(self):
        """30% UNKNOWN → warning but still valid.

        Uses a mixed-tier base so the remaining 70% don't all share the same
        tier (which would trigger the dominance check as a separate critical).
        """
        df = _make_mixed_scored_df(n=12)   # 4 HIGH, 4 MOD, 4 LOW — balanced
        df = _inject_nan_rows(df, count=3)  # replace last 3: ~25% UNKNOWN
        is_valid, report = validate_results(df)
        assert is_valid is True
        assert report["unknown_tier_count"] == 3
        assert len(report["warnings"]) >= 1

    def test_exactly_50_percent_unknown_is_still_valid(self):
        """Threshold is >50%, so exactly 50% should be a warning, not critical."""
        df = _make_mixed_scored_df(n=12)    # balanced base (4 each tier)
        df = _inject_nan_rows(df, count=6)  # 6/12 = 50.0% UNKNOWN
        is_valid, report = validate_results(df)
        assert is_valid is True
        assert report["unknown_tier_pct"] == 50.0

    def test_above_50_percent_unknown_is_critical(self):
        """51% UNKNOWN → CRITICAL → is_valid = False."""
        df = _make_scored_df(n=100, tier=RISK_TIER_LOW)
        df = _inject_nan_rows(df, count=51)  # 51% UNKNOWN
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert any("UNKNOWN" in issue for issue in report["critical_issues"])

    def test_all_unknown_is_critical(self):
        df = _make_scored_df(n=10, tier=RISK_TIER_MODERATE)
        df = _inject_nan_rows(df, count=10)  # 100% UNKNOWN
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["unknown_tier_count"] == 10
        assert report["unknown_tier_pct"] == 100.0


# ---------------------------------------------------------------------------
# validate_results — tier distribution dominance (Check 2)
# ---------------------------------------------------------------------------

class TestValidateResultsDominance:

    def test_balanced_distribution_is_valid(self, clean_scored_df):
        """30% HIGH / 40% MOD / 30% LOW — no single tier dominates."""
        is_valid, report = validate_results(clean_scored_df)
        assert is_valid is True
        assert report["dominant_tier"] is None

    def test_dominant_tier_flagged_as_critical(self):
        """95% same tier triggers the dominance check."""
        n = 100
        df_high = _make_scored_df(n=95, tier=RISK_TIER_HIGH, composite_score=1.0)
        df_low = _make_scored_df(n=5, tier=RISK_TIER_LOW, composite_score=0.0)
        df = pd.concat([df_high, df_low], ignore_index=True)

        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["dominant_tier"] == RISK_TIER_HIGH
        assert any("HIGH" in issue for issue in report["critical_issues"])

    def test_exactly_at_threshold_is_critical(self):
        """DOMINANT_TIER_THRESHOLD = 0.90 means exactly 90% is critical."""
        n = 100
        dominant_count = int(DOMINANT_TIER_THRESHOLD * 100)  # 90
        df_dom = _make_scored_df(n=dominant_count, tier=RISK_TIER_HIGH, composite_score=1.0)
        df_rest = _make_scored_df(n=100 - dominant_count, tier=RISK_TIER_LOW, composite_score=0.0)
        df = pd.concat([df_dom, df_rest], ignore_index=True)

        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["dominant_tier"] == RISK_TIER_HIGH

    def test_just_below_threshold_is_valid(self):
        """89% same tier — below the 90% threshold — should be valid."""
        n = 100
        df_dom = _make_scored_df(n=89, tier=RISK_TIER_HIGH, composite_score=1.0)
        df_rest = _make_scored_df(n=11, tier=RISK_TIER_LOW, composite_score=0.0)
        df = pd.concat([df_dom, df_rest], ignore_index=True)

        is_valid, report = validate_results(df)
        assert is_valid is True
        assert report["dominant_tier"] is None

    def test_unknown_rows_excluded_from_dominance_check(self):
        """UNKNOWN tiers don't count in the dominance check — only scored tiers."""
        df = _make_scored_df(n=10, tier=RISK_TIER_HIGH, composite_score=1.0)
        df = _inject_nan_rows(df, count=9)  # 9 UNKNOWN + 1 HIGH
        # 1/1 scored = 100% HIGH → dominance critical
        # But 9/10 UNKNOWN = 90% → also critical (separate check)
        is_valid, report = validate_results(df)
        assert is_valid is False


# ---------------------------------------------------------------------------
# validate_results — impossible values (Check 3)
# ---------------------------------------------------------------------------

class TestValidateResultsImpossibleValues:

    def test_valid_values_produce_no_impossible_flags(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        assert report["total_impossible"] == 0
        assert all(v == 0 for v in report["impossible_values"].values())

    def test_canopy_above_100_is_critical(self):
        df = _make_scored_df(n=5)
        df.loc[0, "canopy_pct"] = 101.0
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["impossible_values"]["canopy_above_100"] == 1
        assert report["total_impossible"] == 1

    def test_canopy_below_0_is_critical(self):
        df = _make_scored_df(n=5)
        df.loc[0, "canopy_pct"] = -1.0
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["impossible_values"]["canopy_below_0"] == 1

    def test_slope_below_0_is_critical(self):
        df = _make_scored_df(n=5)
        df.loc[0, "slope_deg"] = -0.1
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["impossible_values"]["slope_below_0"] == 1

    def test_slope_above_90_is_critical(self):
        df = _make_scored_df(n=5)
        df.loc[0, "slope_deg"] = 91.0
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["impossible_values"]["slope_above_90"] == 1

    def test_multiple_impossible_values_all_counted(self):
        df = _make_scored_df(n=5)
        df.loc[0, "canopy_pct"] = 110.0   # above 100
        df.loc[1, "slope_deg"] = -5.0     # below 0
        df.loc[2, "slope_deg"] = 95.0     # above 90
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["total_impossible"] == 3

    def test_nan_values_are_not_flagged_as_impossible(self):
        """NaN canopy/slope are legitimate (raster nodata) and must not be counted."""
        df = _make_scored_df(n=5)
        df.loc[0, "canopy_pct"] = float("nan")
        df.loc[1, "slope_deg"] = float("nan")
        _, report = validate_results(df)
        assert report["total_impossible"] == 0

    def test_exact_boundary_values_are_valid(self):
        """canopy=100, slope=0, slope=90 are on the boundary — not impossible."""
        df = _make_scored_df(n=3)
        df.loc[0, "canopy_pct"] = 100.0
        df.loc[1, "slope_deg"] = 0.0
        df.loc[2, "slope_deg"] = 90.0
        _, report = validate_results(df)
        assert report["total_impossible"] == 0


# ---------------------------------------------------------------------------
# validate_results — cross-validation: forest vs canopy (Check 4)
# ---------------------------------------------------------------------------

class TestValidateResultsCrossValidation:

    def test_no_cross_validation_issues_on_clean_data(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        assert report["cross_validation"]["forest_low_canopy_count"] == 0
        assert report["cross_validation"]["forest_low_canopy_sample_ids"] == []

    def test_forest_code_with_normal_canopy_is_not_flagged(self):
        df = _make_scored_df(n=5, land_cover_code=41, canopy_pct=60.0)
        _, report = validate_results(df)
        assert report["cross_validation"]["forest_low_canopy_count"] == 0

    def test_forest_code_with_low_canopy_is_flagged_as_warning(self):
        """Forest code + low canopy is a WARNING only — not critical.

        Uses a mixed-tier base with forest rows injected so the overall tier
        distribution stays balanced (avoids triggering dominance check).
        """
        base = _make_mixed_scored_df(n=9)   # 3 HIGH, 3 MOD, 3 LOW
        # Overwrite the 3 LOW rows with forest/low-canopy MODERATE rows
        for i in [2, 5, 8]:  # every 3rd row (LOW tier from cycling)
            base.loc[i, "land_cover_code"] = 41
            base.loc[i, "canopy_pct"] = 3.0
        is_valid, report = validate_results(base)
        assert is_valid is True
        assert report["cross_validation"]["forest_low_canopy_count"] == 3
        assert len(report["warnings"]) >= 1

    def test_all_three_forest_codes_trigger_cross_validation(self):
        """Codes 41, 42, 43 should all trigger cross-validation when canopy low.

        Uses a mixed-tier base (one HIGH, one MODERATE, one LOW) with forest
        codes injected so no single tier dominates.
        """
        rows = [
            {"location_id": "F41", "canopy_pct": 2.0, "slope_deg": 5.0,
             "land_cover_code": 41, "canopy_risk": 0.0, "slope_risk": 0.0,
             "landcover_risk": 1.0, "composite_score": 1.0, "risk_tier": RISK_TIER_HIGH},
            {"location_id": "F42", "canopy_pct": 1.0, "slope_deg": 5.0,
             "land_cover_code": 42, "canopy_risk": 0.0, "slope_risk": 0.0,
             "landcover_risk": 1.0, "composite_score": 0.5, "risk_tier": RISK_TIER_MODERATE},
            {"location_id": "F43", "canopy_pct": 0.0, "slope_deg": 5.0,
             "land_cover_code": 43, "canopy_risk": 0.0, "slope_risk": 0.0,
             "landcover_risk": 1.0, "composite_score": 0.0, "risk_tier": RISK_TIER_LOW},
        ]
        df = pd.DataFrame(rows)
        is_valid, report = validate_results(df)
        assert is_valid is True
        assert report["cross_validation"]["forest_low_canopy_count"] == 3

    def test_exactly_at_threshold_is_not_flagged(self):
        """canopy_pct == FOREST_LOW_CANOPY_THRESHOLD is NOT below threshold."""
        df = _make_scored_df(n=3, land_cover_code=41,
                             canopy_pct=float(FOREST_LOW_CANOPY_THRESHOLD))
        _, report = validate_results(df)
        assert report["cross_validation"]["forest_low_canopy_count"] == 0

    def test_non_forest_code_with_low_canopy_is_not_flagged(self):
        """Low canopy + non-forest land cover (e.g. 82 = crops) is valid."""
        df = _make_scored_df(n=3, land_cover_code=82, canopy_pct=1.0)
        _, report = validate_results(df)
        assert report["cross_validation"]["forest_low_canopy_count"] == 0

    def test_sample_ids_are_included_in_report(self):
        df = _make_scored_df(n=5, land_cover_code=42, canopy_pct=2.0)
        _, report = validate_results(df)
        assert len(report["cross_validation"]["forest_low_canopy_sample_ids"]) == 5

    def test_forest_with_nan_canopy_not_flagged(self):
        """Null canopy has no value to compare — should not trigger cross-validation."""
        df = _make_scored_df(n=5, land_cover_code=41, canopy_pct=float("nan"))
        _, report = validate_results(df)
        assert report["cross_validation"]["forest_low_canopy_count"] == 0


# ---------------------------------------------------------------------------
# validate_results — is_valid logic and report structure
# ---------------------------------------------------------------------------

class TestValidateResultsIsValid:

    def test_clean_data_is_valid(self, clean_scored_df):
        is_valid, report = validate_results(clean_scored_df)
        assert is_valid is True
        assert report["is_valid"] is True
        assert len(report["critical_issues"]) == 0

    def test_return_value_matches_report_field(self, clean_scored_df):
        """The bool return value must always match report['is_valid']."""
        is_valid, report = validate_results(clean_scored_df)
        assert is_valid == report["is_valid"]

    def test_critical_issue_makes_is_valid_false(self):
        df = _make_scored_df(n=10)
        df = _inject_nan_rows(df, count=10)  # 100% UNKNOWN → critical
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert report["is_valid"] is False

    def test_multiple_critical_issues_all_reported(self):
        df = _make_scored_df(n=10)
        df = _inject_nan_rows(df, count=10)   # UNKNOWN critical
        df.loc[0, "canopy_pct"] = 999.0       # impossible value critical
        is_valid, report = validate_results(df)
        assert is_valid is False
        assert len(report["critical_issues"]) >= 2

    def test_report_has_all_required_keys(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        for key in [
            "total_scored", "tier_distribution", "tier_distribution_pct",
            "unknown_tier_count", "unknown_tier_pct", "dominant_tier",
            "impossible_values", "total_impossible", "cross_validation",
            "critical_issues", "warnings", "is_valid",
        ]:
            assert key in report, f"Key '{key}' missing from validation_report"

    def test_tier_distribution_sums_to_total(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        assert sum(report["tier_distribution"].values()) == report["total_scored"]

    def test_warnings_only_does_not_fail_validation(self):
        """Forest/canopy cross-validation produces a warning — not a critical failure."""
        # Mixed tiers so dominance check doesn't also trigger
        df = _make_mixed_scored_df(n=9)
        for i in [2, 5, 8]:  # inject forest/low-canopy into LOW-tier rows
            df.loc[i, "land_cover_code"] = 41
            df.loc[i, "canopy_pct"] = 1.0
        is_valid, report = validate_results(df)
        assert is_valid is True
        assert len(report["warnings"]) >= 1
        assert len(report["critical_issues"]) == 0


# ---------------------------------------------------------------------------
# generate_anomaly_report
# ---------------------------------------------------------------------------

class TestGenerateAnomalyReport:

    def test_returns_string(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        text = generate_anomaly_report(clean_scored_df, report)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_report_contains_key_metrics(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        text = generate_anomaly_report(clean_scored_df, report)
        assert "Total locations scored" in text
        assert "Validation status" in text
        assert "Risk Tier Distribution" in text

    def test_passed_status_shown_for_clean_data(self, clean_scored_df):
        _, report = validate_results(clean_scored_df)
        text = generate_anomaly_report(clean_scored_df, report)
        assert "PASSED" in text

    def test_failed_status_shown_for_bad_data(self):
        df = _make_scored_df(n=10)
        df = _inject_nan_rows(df, count=10)
        _, report = validate_results(df)
        text = generate_anomaly_report(df, report)
        assert "FAILED" in text

    def test_critical_issues_appear_in_report(self):
        df = _make_scored_df(n=10)
        df = _inject_nan_rows(df, count=10)
        _, report = validate_results(df)
        text = generate_anomaly_report(df, report)
        assert "CRITICAL ISSUES" in text

    def test_warnings_appear_in_report(self):
        df = _make_scored_df(n=5, land_cover_code=42, canopy_pct=1.0)
        _, report = validate_results(df)
        text = generate_anomaly_report(df, report)
        assert "WARNINGS" in text

    def test_report_saved_to_disk(self, tmp_path, monkeypatch):
        """Report must be written to the configured anomaly report path."""
        import src.validation as validation_module
        fake_path = tmp_path / "anomaly_report.txt"
        monkeypatch.setattr(validation_module, "ANOMALY_REPORT_PATH", fake_path)

        df = _make_scored_df(n=5)
        _, report = validate_results(df)
        generate_anomaly_report(df, report)

        assert fake_path.exists()
        content = fake_path.read_text()
        assert "ANOMALY REPORT" in content

    def test_forest_anomaly_sample_ids_in_report(self):
        df = _make_scored_df(n=3, land_cover_code=41, canopy_pct=2.0)
        _, report = validate_results(df)
        text = generate_anomaly_report(df, report)
        assert "FOREST/CANOPY" in text
        assert "LOC_0000" in text  # first location_id from the fixture
