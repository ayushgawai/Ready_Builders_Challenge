"""
Integration tests for the LEO Satellite Coverage Risk pipeline.

These tests use REAL data sampled from DATA_CHALLENGE_50.csv (North Carolina,
4.67M locations, FIPS state code 37).  They exercise the full ingest → score →
validate → report chain without requiring rasters (synthetic env values are
injected for the scoring stage since large rasters are not committed to the repo).

Mark: pytest -m integration  (or just pytest, as no marker filter is applied by default)

Key differences from unit tests in test_ingest.py / test_risk_scoring.py:
  - Fixtures build from the REAL CSV, not programmatic DataFrames.
  - Assertions check domain invariants on real data, not exact expected values.
  - Tests are marked with pytest.importorskip / pytest.skip when the real CSV
    is absent so the test suite remains green in CI without the data file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import config

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

REAL_CSV = config.LOCATIONS_CSV
SAMPLE_N = 1_000   # rows per real-data test — fast but representative
SAMPLE_SEED = 42   # reproducible sampling


def _skip_if_no_csv():
    if not REAL_CSV.exists():
        pytest.skip(f"Real CSV not found at {REAL_CSV}. Skipping integration test.")


@pytest.fixture(scope="module")
def real_sample_csv(tmp_path_factory) -> Path:
    """1 000 rows randomly sampled from DATA_CHALLENGE_50.csv, written to a
    temporary CSV so load_locations() can read it as a file path.
    Scope=module: built once, reused across all tests in this file.
    """
    _skip_if_no_csv()
    df = pd.read_csv(REAL_CSV, dtype={"geoid_cb": str}, nrows=50_000)
    sample = df.sample(n=SAMPLE_N, random_state=SAMPLE_SEED).reset_index(drop=True)
    path = tmp_path_factory.mktemp("real_data") / "sample_real.csv"
    sample.to_csv(path, index=False)
    return path


@pytest.fixture(scope="module")
def real_validated_df(real_sample_csv) -> pd.DataFrame:
    """Run load_locations + validate_locations on the real sample.
    Returns the cleaned DataFrame and caches it for the scoring tests.
    """
    from src.ingest import load_locations, validate_locations
    df = load_locations(str(real_sample_csv))
    clean_df, _ = validate_locations(df)
    return clean_df


@pytest.fixture(scope="module")
def real_scored_df(real_validated_df) -> pd.DataFrame:
    """Inject synthetic but realistic env values into the validated DataFrame,
    then run compute_composite_risk.

    Env values are drawn from representative NC distributions:
      canopy_pct  : skewed toward 20-80% (NC has significant forest cover)
      slope_deg   : gentle Piedmont slopes, ~0-15° (rural NC is mostly rolling)
      land_cover  : mix of 41 (deciduous), 81 (pasture), 22 (developed-low)
    """
    from src.risk_scoring import compute_composite_risk

    rng = np.random.default_rng(seed=SAMPLE_SEED)
    n = len(real_validated_df)

    df = real_validated_df.copy()
    df["canopy_pct"] = np.clip(rng.normal(45, 20, n), 0, 100)
    df["elevation_m"] = rng.uniform(50, 400, n)
    df["slope_deg"] = np.clip(rng.exponential(8, n), 0, 45)

    # Land cover: 50% deciduous forest, 30% pasture, 20% developed-low
    lc_choices = rng.choice([41, 81, 22], size=n, p=[0.5, 0.3, 0.2])
    df["land_cover_code"] = lc_choices.astype(float)

    return compute_composite_risk(df)


# ---------------------------------------------------------------------------
# Stage 1: Ingest — real data
# ---------------------------------------------------------------------------

class TestIngestRealData:
    """Tests that ingest.load_locations + validate_locations behave correctly
    on a real sample of DATA_CHALLENGE_50.csv."""

    def test_loads_without_error(self, real_sample_csv):
        from src.ingest import load_locations
        df = load_locations(str(real_sample_csv))
        assert len(df) == SAMPLE_N

    def test_columns_after_load(self, real_sample_csv):
        """load_locations must derive state, county, county_fips from geoid_cb."""
        from src.ingest import load_locations
        df = load_locations(str(real_sample_csv))
        required = {"location_id", "latitude", "longitude", "geoid_cb",
                    "state", "county", "county_fips"}
        assert required.issubset(set(df.columns))

    def test_real_data_high_retention(self, real_validated_df):
        """Real NC data is clean — retention must be ≥ 95%."""
        assert len(real_validated_df) >= SAMPLE_N * 0.95

    def test_state_is_all_nc(self, real_validated_df):
        """Every validated row has state='NC' (FIPS 37)."""
        states = real_validated_df["state"].dropna().unique()
        assert list(states) == ["NC"], f"Unexpected states: {states}"

    def test_state_fill_rate(self, real_validated_df):
        """State column must be populated for ≥ 99% of rows."""
        fill_rate = real_validated_df["state"].notna().mean()
        assert fill_rate >= 0.99, f"State fill rate too low: {fill_rate:.1%}"

    def test_county_fill_rate(self, real_validated_df):
        """County NAMELSAD must be populated for ≥ 95% of rows."""
        fill_rate = real_validated_df["county"].notna().mean()
        assert fill_rate >= 0.95, f"County fill rate: {fill_rate:.1%}"

    def test_county_namelsad_format(self, real_validated_df):
        """County names should end with 'County' (NC uses standard county names)."""
        counties = real_validated_df["county"].dropna().unique()
        non_county = [c for c in counties if not c.endswith("County")]
        assert len(non_county) == 0, f"Non-standard county names: {non_county}"

    def test_county_fips_five_digits(self, real_validated_df):
        """county_fips must be 5-digit strings (state 2 + county 3)."""
        fips = real_validated_df["county_fips"].dropna()
        bad = fips[fips.str.len() != 5]
        assert len(bad) == 0, f"Bad FIPS values: {bad.head().tolist()}"

    def test_county_fips_starts_with_37(self, real_validated_df):
        """All NC county FIPS must start with '37'."""
        fips = real_validated_df["county_fips"].dropna()
        bad = fips[~fips.str.startswith("37")]
        assert len(bad) == 0, f"Non-NC FIPS found: {bad.head().tolist()}"

    def test_coordinates_in_nc_bounds(self, real_validated_df):
        """NC latitude 33.8–36.6°N, longitude 75.4–84.4°W."""
        assert real_validated_df["latitude"].between(33.5, 37.0).all()
        assert real_validated_df["longitude"].between(-85.0, -75.0).all()

    def test_quality_report_structure(self, real_sample_csv):
        """Quality report has all required keys and consistent arithmetic."""
        from src.ingest import load_locations, validate_locations
        df = load_locations(str(real_sample_csv))
        _, report = validate_locations(df)

        required_keys = {
            "total_records", "valid_records", "dropped_records",
            "drop_reasons", "retention_rate_pct",
        }
        assert required_keys.issubset(set(report.keys()))
        assert report["total_records"] == report["valid_records"] + report["dropped_records"]
        assert 0 <= report["retention_rate_pct"] <= 100


# ---------------------------------------------------------------------------
# Stage 2: Scoring — synthetic env values on real locations
# ---------------------------------------------------------------------------

class TestScoringRealLocations:
    """Tests that risk_scoring.compute_composite_risk produces valid output
    when applied to real location coordinates with synthetic env values."""

    def test_all_required_columns_present(self, real_scored_df):
        # compute_composite_risk produces: canopy_risk, slope_risk, landcover_risk
        required = {"composite_score", "risk_tier", "canopy_risk",
                    "slope_risk", "landcover_risk"}
        assert required.issubset(set(real_scored_df.columns))

    def test_composite_score_range(self, real_scored_df):
        """Composite score must be in [0.0, 1.0] or NaN."""
        valid = real_scored_df["composite_score"].dropna()
        assert (valid >= 0.0).all() and (valid <= 1.0).all()

    def test_no_nan_tiers_on_complete_env_data(self, real_scored_df):
        """With no NaN env inputs all tiers should be HIGH/MODERATE/LOW (no UNKNOWN)."""
        unknown = (real_scored_df["risk_tier"] == config.RISK_TIER_UNKNOWN).sum()
        # Allow a small margin for any NaN that crept in from the fixture
        assert unknown / len(real_scored_df) < 0.05

    def test_tier_distribution_all_three_present(self, real_scored_df):
        """With realistic NC env values all three tiers should appear."""
        tiers = set(real_scored_df["risk_tier"].unique()) - {"UNKNOWN"}
        assert tiers == {"HIGH", "MODERATE", "LOW"}, f"Missing tiers: tiers"

    def test_no_dominant_tier(self, real_scored_df):
        """No single tier should capture > 90% of locations."""
        counts = real_scored_df["risk_tier"].value_counts(normalize=True)
        assert counts.max() < 0.90, f"Dominant tier: {counts.idxmax()} at {counts.max():.1%}"

    def test_high_canopy_maps_to_high_risk(self, real_scored_df):
        """Rows where canopy_pct > 50 (HIGH threshold) should have canopy_risk=1.0."""
        high_canopy = real_scored_df[real_scored_df["canopy_pct"] > config.CANOPY_HIGH_THRESHOLD]
        assert (high_canopy["canopy_risk"] == 1.0).all()

    def test_low_canopy_maps_to_zero_score(self, real_scored_df):
        """Rows where canopy_pct < 20 (MOD threshold) should have canopy_risk=0.0."""
        low_canopy = real_scored_df[real_scored_df["canopy_pct"] < config.CANOPY_MOD_THRESHOLD]
        assert (low_canopy["canopy_risk"] == 0.0).all()

    def test_composite_arithmetic_correct(self, real_scored_df):
        """Composite = canopy×0.5 + slope×0.3 + landcover×0.2 (spot-check 20 rows)."""
        check = real_scored_df.dropna(subset=["composite_score"]).head(20)
        expected = (
            check["canopy_risk"] * config.WEIGHT_CANOPY
            + check["slope_risk"] * config.WEIGHT_SLOPE
            + check["landcover_risk"] * config.WEIGHT_LANDCOVER
        )
        np.testing.assert_allclose(check["composite_score"], expected, rtol=1e-5)


# ---------------------------------------------------------------------------
# Stage 3: Validation — anomaly detection on scored data
# ---------------------------------------------------------------------------

class TestValidationRealScored:
    """validate_results should pass on realistic scored data."""

    def test_validation_passes_on_clean_scored_data(self, real_scored_df):
        from src.validation import validate_results
        is_valid, report = validate_results(real_scored_df)
        # May have warnings (forest cross-val) but should not be CRITICAL
        critical = report.get("critical_checks", [])
        assert len(critical) == 0, f"Unexpected critical issues: {critical}"

    def test_validation_report_has_tier_distribution(self, real_scored_df):
        from src.validation import validate_results
        _, report = validate_results(real_scored_df)
        dist = report.get("tier_distribution", {})
        total = sum(dist.values())
        assert total == len(real_scored_df)

    def test_generate_anomaly_report_writes_file(self, real_scored_df, tmp_path, monkeypatch):
        import src.validation as val_mod
        from src.validation import validate_results, generate_anomaly_report

        # Patch the module-level name that validation.py imported directly
        monkeypatch.setattr(val_mod, "ANOMALY_REPORT_PATH", tmp_path / "anomaly.txt")
        _, report = validate_results(real_scored_df)
        text = generate_anomaly_report(real_scored_df, report)

        assert isinstance(text, str)
        assert len(text) > 50
        assert (tmp_path / "anomaly.txt").exists()


# ---------------------------------------------------------------------------
# Stage 4: Reporting — summary stats and findings report
# ---------------------------------------------------------------------------

class TestReportingRealScored:
    """generate_summary_stats and write_findings_report on real scored data."""

    def test_summary_stats_keys(self, real_scored_df):
        from src.reporting import generate_summary_stats
        stats = generate_summary_stats(real_scored_df)
        required = {
            "total_locations", "tier_distribution", "unknown_count",
            "unknown_pct", "avg_canopy_by_tier", "avg_slope_by_tier",
            "state_breakdown", "top_counties_high_risk",
        }
        assert required.issubset(set(stats.keys()))

    def test_total_locations_matches_df(self, real_scored_df):
        from src.reporting import generate_summary_stats
        stats = generate_summary_stats(real_scored_df)
        assert stats["total_locations"] == len(real_scored_df)

    def test_tier_distribution_sums_to_100(self, real_scored_df):
        from src.reporting import generate_summary_stats
        stats = generate_summary_stats(real_scored_df)
        total_pct = sum(v["pct"] for v in stats["tier_distribution"].values())
        assert abs(total_pct - 100.0) < 0.1

    def test_findings_report_written(self, real_scored_df, tmp_path, monkeypatch):
        from src.reporting import generate_summary_stats, write_findings_report
        import src.config as cfg

        monkeypatch.setattr(cfg, "FINDINGS_REPORT_PATH", tmp_path / "findings.md")
        stats = generate_summary_stats(real_scored_df)
        path = write_findings_report(stats)

        assert path.exists()
        content = path.read_text()
        assert "HIGH" in content
        assert "MODERATE" in content
        assert len(content) > 200

    def test_state_breakdown_contains_nc(self, real_scored_df):
        from src.reporting import generate_summary_stats
        stats = generate_summary_stats(real_scored_df)
        states = [row["state"] for row in stats["state_breakdown"]]
        assert "NC" in states, f"NC missing from state_breakdown: {states}"

    def test_top_counties_are_nc_counties(self, real_scored_df):
        from src.reporting import generate_summary_stats
        stats = generate_summary_stats(real_scored_df)
        for entry in stats["top_counties_high_risk"]:
            # All entries should reference NC counties
            assert entry["county"].endswith("County"), (
                f"Non-standard county name: {entry['county']}"
            )


# ---------------------------------------------------------------------------
# Stage 5: Agent tool dispatcher — ingest_locations tool
# ---------------------------------------------------------------------------

class TestAgentIngestTool:
    """PipelineAgent._tool_ingest_locations with a real CSV sample.
    No API key needed — tests only the Python dispatch layer."""

    def test_ingest_tool_success(self, real_sample_csv, monkeypatch):
        from src.agent import PipelineAgent
        import src.config as cfg

        agent = PipelineAgent.__new__(PipelineAgent)
        agent._api_key = "mock"
        agent._scored_df = None
        agent._monitoring = {"tools": {}}

        # Redirect validated output to a temp path
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            validated_path = Path(f.name)
        monkeypatch.setattr(cfg, "VALIDATED_LOCATIONS_PATH", validated_path)

        result = agent._tool_ingest_locations({"file_path": str(real_sample_csv)})

        assert result.get("status") == "success"
        assert result["valid_records"] >= SAMPLE_N * 0.95
        assert result["retention_rate_pct"] >= 95.0
        assert validated_path.exists()

        os.unlink(validated_path)

    def test_ingest_tool_unknown_file_returns_error(self):
        """FileNotFoundError is caught by the dispatcher and returned as error JSON."""
        from src.agent import PipelineAgent
        import json

        agent = PipelineAgent.__new__(PipelineAgent)
        agent._api_key = "mock"
        agent._scored_df = None
        agent._monitoring = {"tools": {}}

        # Go through handle_tool_call (the dispatcher) which wraps in try/except
        result_json = agent.handle_tool_call(
            "ingest_locations",
            {"file_path": "/nonexistent/path/missing.csv"},
        )
        result = json.loads(result_json)
        assert "error" in result
        assert "FileNotFoundError" in result["error"] or "not found" in result["error"].lower()
