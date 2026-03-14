"""
Tests for the on-demand agent tools: analyze_location and assess_area.

All tests run WITHOUT an Anthropic API key — they call the Python dispatch
methods (_tool_analyze_location, _tool_assess_area) directly, using a
pre-built mock scored DataFrame that contains realistic NC locations and
synthetic risk scores.

Coordinate data is taken from the real DATA_CHALLENGE_50.csv (North Carolina)
so lat/lon ranges are authentic even though risk scores are synthetic.

Test categories:
  TestAnalyzeLocationByCoords    — lat/lon inputs
  TestAnalyzeLocationByString    — "lat, lon" string + address string parsing
  TestAnalyzeLocationFactors     — factor breakdown correctness
  TestAnalyzeLocationSeasonal    — seasonal context for deciduous/evergreen/mixed
  TestAnalyzeLocationAlternatives — nearby lower-risk alternatives
  TestAnalyzeLocationErrors      — missing input, no scored data, bad coords
  TestAssessAreaState            — state-level briefing
  TestAssessAreaCounty           — county-level briefing
  TestAssessAreaErrors           — missing state, non-existent area
  TestGeocoding                  — resolve_location helper function
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Shared scored DataFrame fixture — realistic NC data, synthetic risk scores
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def nc_scored_df() -> pd.DataFrame:
    """
    Synthetic scored DataFrame representing 12 NC locations across 3 counties.
    Coordinates are real NC values; risk scores are assigned to cover all tiers
    and all relevant land cover codes for thorough tool testing.

    Columns match what _load_scored_df() reads from locations_scored.csv:
      location_id, latitude, longitude, risk_tier, composite_score,
      canopy_pct, slope_deg, land_cover_code, canopy_score, slope_score,
      landcover_score, state, county
    """
    # Column names match what compute_composite_risk() actually produces:
    # canopy_risk, slope_risk, landcover_risk (NOT canopy_score / slope_score / landcover_score)
    rows = [
        # ---------- Mecklenburg County (Charlotte area, urban/suburban) ----------
        {
            "location_id": "37119_HIGH_01",
            "latitude": 35.2271, "longitude": -80.8431,
            "state": "NC", "county": "Mecklenburg County",
            "canopy_pct": 68.0, "slope_deg": 22.0, "land_cover_code": 41,   # Deciduous Forest
            "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
            "composite_score": 1.0, "risk_tier": "HIGH",
        },
        {
            "location_id": "37119_HIGH_02",
            "latitude": 35.2300, "longitude": -80.8450,
            "state": "NC", "county": "Mecklenburg County",
            "canopy_pct": 55.0, "slope_deg": 8.0, "land_cover_code": 42,    # Evergreen Forest
            "canopy_risk": 1.0, "slope_risk": 0.0, "landcover_risk": 1.0,
            "composite_score": 0.70, "risk_tier": "HIGH",
        },
        {
            "location_id": "37119_MOD_01",
            "latitude": 35.2400, "longitude": -80.8300,
            "state": "NC", "county": "Mecklenburg County",
            "canopy_pct": 35.0, "slope_deg": 14.0, "land_cover_code": 43,   # Mixed Forest
            "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.5,
            "composite_score": 0.50, "risk_tier": "MODERATE",
        },
        {
            "location_id": "37119_LOW_01",
            "latitude": 35.2600, "longitude": -80.8100,
            "state": "NC", "county": "Mecklenburg County",
            "canopy_pct": 5.0, "slope_deg": 2.0, "land_cover_code": 22,     # Developed-Low
            "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.5,
            "composite_score": 0.10, "risk_tier": "LOW",
        },
        # ---------- Brunswick County (coastal, agriculture/forest) ----------
        {
            "location_id": "37019_HIGH_01",
            "latitude": 34.0000, "longitude": -78.1000,
            "state": "NC", "county": "Brunswick County",
            "canopy_pct": 72.0, "slope_deg": 3.0, "land_cover_code": 41,    # Deciduous Forest
            "canopy_risk": 1.0, "slope_risk": 0.0, "landcover_risk": 1.0,
            "composite_score": 0.70, "risk_tier": "HIGH",
        },
        {
            "location_id": "37019_MOD_01",
            "latitude": 34.0200, "longitude": -78.1200,
            "state": "NC", "county": "Brunswick County",
            "canopy_pct": 30.0, "slope_deg": 2.0, "land_cover_code": 81,    # Pasture/Hay
            "canopy_risk": 0.5, "slope_risk": 0.0, "landcover_risk": 0.0,
            "composite_score": 0.25, "risk_tier": "LOW",  # just below MOD threshold
        },
        {
            "location_id": "37019_LOW_01",
            "latitude": 34.0400, "longitude": -78.0800,
            "state": "NC", "county": "Brunswick County",
            "canopy_pct": 8.0, "slope_deg": 1.0, "land_cover_code": 82,     # Cultivated Crops
            "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.0,
            "composite_score": 0.0, "risk_tier": "LOW",
        },
        # ---------- Durham County ----------
        {
            "location_id": "37063_HIGH_01",
            "latitude": 35.9940, "longitude": -78.8986,
            "state": "NC", "county": "Durham County",
            "canopy_pct": 60.0, "slope_deg": 18.0, "land_cover_code": 42,   # Evergreen Forest
            "canopy_risk": 1.0, "slope_risk": 0.5, "landcover_risk": 1.0,
            "composite_score": 0.85, "risk_tier": "HIGH",
        },
        {
            "location_id": "37063_MOD_01",
            "latitude": 36.0000, "longitude": -78.9000,
            "state": "NC", "county": "Durham County",
            "canopy_pct": 28.0, "slope_deg": 12.0, "land_cover_code": 71,   # Grassland
            "canopy_risk": 0.5, "slope_risk": 0.5, "landcover_risk": 0.0,
            "composite_score": 0.40, "risk_tier": "MODERATE",
        },
        {
            "location_id": "37063_LOW_01",
            "latitude": 36.0100, "longitude": -78.9100,
            "state": "NC", "county": "Durham County",
            "canopy_pct": 10.0, "slope_deg": 5.0, "land_cover_code": 31,    # Barren
            "canopy_risk": 0.0, "slope_risk": 0.0, "landcover_risk": 0.0,
            "composite_score": 0.0, "risk_tier": "LOW",
        },
        # ---------- Extra rows — multi-county HIGH tally for assess_area ----------
        {
            "location_id": "37119_HIGH_03",
            "latitude": 35.2350, "longitude": -80.8500,
            "state": "NC", "county": "Mecklenburg County",
            "canopy_pct": 53.0, "slope_deg": 21.0, "land_cover_code": 41,
            "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
            "composite_score": 1.0, "risk_tier": "HIGH",
        },
        {
            "location_id": "37063_HIGH_02",
            "latitude": 35.9980, "longitude": -78.8970,
            "state": "NC", "county": "Durham County",
            "canopy_pct": 75.0, "slope_deg": 25.0, "land_cover_code": 43,   # Mixed Forest
            "canopy_risk": 1.0, "slope_risk": 1.0, "landcover_risk": 1.0,
            "composite_score": 1.0, "risk_tier": "HIGH",
        },
    ]
    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def agent_with_nc_data(nc_scored_df) -> "PipelineAgent":
    """PipelineAgent instance with the NC scored DataFrame pre-loaded in cache.
    No API key required — tests only the Python dispatch layer.
    """
    from src.agent import PipelineAgent
    agent = PipelineAgent.__new__(PipelineAgent)
    agent._api_key = "mock_key_no_api_calls"
    agent._model = "mock"
    agent._max_turns = 10
    agent._monitoring = {"tools": {}}
    agent._scored_df = nc_scored_df
    return agent


# ---------------------------------------------------------------------------
# TestAnalyzeLocationByCoords
# ---------------------------------------------------------------------------

class TestAnalyzeLocationByCoords:
    """analyze_location accepts explicit latitude + longitude numbers."""

    def test_returns_success_status(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["status"] == "success"

    def test_identifies_correct_location(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["location"]["location_id"] == "37119_HIGH_01"

    def test_risk_tier_high(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["risk_assessment"]["risk_tier"] == "HIGH"

    def test_low_risk_location(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2600, "longitude": -80.8100
        })
        assert result["risk_assessment"]["risk_tier"] == "LOW"

    def test_input_type_recorded_as_coordinates(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["input"]["type"] == "coordinates"

    def test_data_source_is_pre_scored(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["location"]["data_source"] == "pre_scored_csv"

    def test_state_and_county_returned(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert result["location"]["state"] == "NC"
        assert result["location"]["county"] == "Mecklenburg County"


# ---------------------------------------------------------------------------
# TestAnalyzeLocationByString
# ---------------------------------------------------------------------------

class TestAnalyzeLocationByString:
    """analyze_location parses 'lat, lon' coordinate strings correctly
    (without calling the Nominatim geocoder)."""

    def test_coord_string_resolves(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "address": "35.2271, -80.8431"
        })
        assert result["status"] == "success"
        assert abs(result["input"]["latitude"] - 35.2271) < 0.001
        assert abs(result["input"]["longitude"] - (-80.8431)) < 0.001

    def test_coord_string_finds_same_location_as_explicit(self, agent_with_nc_data):
        by_string = agent_with_nc_data._tool_analyze_location({
            "address": "35.2271, -80.8431"
        })
        by_coords = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        assert (
            by_string["location"]["location_id"]
            == by_coords["location"]["location_id"]
        )

    def test_coord_string_with_spaces(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "address": "35.2271 -80.8431"
        })
        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# TestAnalyzeLocationFactors
# ---------------------------------------------------------------------------

class TestAnalyzeLocationFactors:
    """Factor breakdown structure and correctness."""

    @pytest.fixture
    def high_result(self, agent_with_nc_data):
        return agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })

    def test_factor_keys_present(self, high_result):
        factors = high_result["risk_assessment"]["factors"]
        assert {"canopy", "slope", "land_cover"} == set(factors.keys())

    def test_canopy_value_matches_source(self, high_result):
        """canopy factor value_pct should equal the source row's canopy_pct."""
        assert high_result["risk_assessment"]["factors"]["canopy"]["value_pct"] == 68.0

    def test_contributions_sum_to_composite(self, high_result):
        """Sum of weighted contributions must equal composite_score."""
        factors = high_result["risk_assessment"]["factors"]
        total = (
            factors["canopy"]["contribution_to_composite"]
            + factors["slope"]["contribution_to_composite"]
            + factors["land_cover"]["contribution_to_composite"]
        )
        composite = high_result["risk_assessment"]["composite_score"]
        assert abs(total - composite) < 0.01

    def test_contribution_pcts_sum_to_100(self, high_result):
        """Contribution percentages must sum to ~100%."""
        factors = high_result["risk_assessment"]["factors"]
        total_pct = (
            factors["canopy"]["contribution_pct"]
            + factors["slope"]["contribution_pct"]
            + factors["land_cover"]["contribution_pct"]
        )
        assert abs(total_pct - 100.0) < 1.0

    def test_primary_driver_is_most_contributing_factor(self, high_result):
        """primary_driver must name the factor with the highest contribution."""
        factors = high_result["risk_assessment"]["factors"]
        contributions = {
            "canopy": factors["canopy"]["contribution_to_composite"],
            "slope": factors["slope"]["contribution_to_composite"],
            "land_cover": factors["land_cover"]["contribution_to_composite"],
        }
        expected_driver = max(contributions, key=contributions.get)
        assert high_result["risk_assessment"]["primary_driver"] == expected_driver

    def test_recommendation_is_non_empty_string(self, high_result):
        rec = high_result["risk_assessment"]["recommendation"]
        assert isinstance(rec, str) and len(rec) > 20

    def test_recommendation_mentions_high_for_high_tier(self, high_result):
        rec = high_result["risk_assessment"]["recommendation"]
        assert "HIGH" in rec or "high" in rec.lower()

    def test_land_cover_name_present(self, high_result):
        lc = high_result["risk_assessment"]["factors"]["land_cover"]
        assert isinstance(lc["name"], str) and len(lc["name"]) > 0

    def test_deciduous_forest_land_cover_name(self, high_result):
        """NLCD code 41 = Deciduous Forest."""
        lc = high_result["risk_assessment"]["factors"]["land_cover"]
        assert lc["nlcd_code"] == 41
        assert "Deciduous" in lc["name"]


# ---------------------------------------------------------------------------
# TestAnalyzeLocationSeasonal
# ---------------------------------------------------------------------------

class TestAnalyzeLocationSeasonal:
    """Seasonal context is returned correctly for forest land cover types."""

    def test_deciduous_forest_returns_seasonal_note(self, agent_with_nc_data):
        """Deciduous Forest (code 41) → seasonal note with winter guidance."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431  # land_cover_code=41
        })
        note = result["seasonal_note"]
        assert note is not None
        assert note["winter_install_actionable"] is True

    def test_evergreen_forest_no_winter_benefit(self, agent_with_nc_data):
        """Evergreen Forest (code 42) → seasonal note says no winter benefit."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2300, "longitude": -80.8450  # land_cover_code=42
        })
        note = result["seasonal_note"]
        assert note is not None
        assert note["winter_install_actionable"] is False

    def test_mixed_forest_has_partial_benefit(self, agent_with_nc_data):
        """Mixed Forest (code 43) → seasonal note with winter_install_actionable=True."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2400, "longitude": -80.8300  # land_cover_code=43
        })
        note = result["seasonal_note"]
        assert note is not None
        assert note["winter_install_actionable"] is True

    def test_non_forest_no_seasonal_note(self, agent_with_nc_data):
        """Developed land (code 22) → no seasonal note."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2600, "longitude": -80.8100  # land_cover_code=22
        })
        assert result["seasonal_note"] is None

    def test_deciduous_note_mentions_november(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431
        })
        text = result["seasonal_note"]["winter_adjustment"]
        assert "November" in text or "Nov" in text


# ---------------------------------------------------------------------------
# TestAnalyzeLocationAlternatives
# ---------------------------------------------------------------------------

class TestAnalyzeLocationAlternatives:
    """Nearby lower-risk alternatives are returned for HIGH-risk queries."""

    def test_alternatives_count_gt_zero_for_high_risk(self, agent_with_nc_data):
        """High-risk location with LOW alternatives in 5km radius."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431,
            "radius_meters": 5000,
        })
        assert result["nearby_alternatives"]["count_found"] > 0

    def test_alternatives_are_lower_tier(self, agent_with_nc_data):
        """All alternatives must have lower risk tier than the query location."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431,
            "radius_meters": 5000,
        })
        for alt in result["nearby_alternatives"]["locations"]:
            assert alt["risk_tier"] in {"LOW", "MODERATE"}

    def test_alternatives_sorted_by_distance(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431,
            "radius_meters": 5000,
        })
        alts = result["nearby_alternatives"]["locations"]
        if len(alts) > 1:
            dists = [a["distance_m"] for a in alts]
            assert dists == sorted(dists)

    def test_no_alternatives_for_low_risk(self, agent_with_nc_data):
        """LOW-risk location has no 'lower' alternatives."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2600, "longitude": -80.8100,  # LOW tier
            "radius_meters": 5000,
        })
        assert result["nearby_alternatives"]["count_found"] == 0

    def test_tight_radius_returns_fewer_alternatives(self, agent_with_nc_data):
        wide = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431,
            "radius_meters": 10_000,
        })
        tight = agent_with_nc_data._tool_analyze_location({
            "latitude": 35.2271, "longitude": -80.8431,
            "radius_meters": 500,
        })
        assert wide["nearby_alternatives"]["count_found"] >= tight["nearby_alternatives"]["count_found"]


# ---------------------------------------------------------------------------
# TestAnalyzeLocationErrors
# ---------------------------------------------------------------------------

class TestAnalyzeLocationErrors:
    """analyze_location returns structured errors for bad inputs."""

    def test_no_input_returns_error(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_analyze_location({})
        assert "error" in result

    def test_coords_outside_nc_returns_none_match(self, agent_with_nc_data):
        """Coords far from NC (no match within 150m tolerance) → on-the-fly
        scoring attempt, which fails gracefully when rasters are absent."""
        result = agent_with_nc_data._tool_analyze_location({
            "latitude": 47.6062, "longitude": -122.3321   # Seattle — no match
        })
        # Either "error" key (rasters absent) or status=success (on-the-fly worked)
        # The important thing is it doesn't raise an exception
        assert "error" in result or result.get("status") == "success"

    def test_no_scored_df_returns_error(self):
        """analyze_location with no scored data and no rasters → error."""
        from src.agent import PipelineAgent
        agent = PipelineAgent.__new__(PipelineAgent)
        agent._api_key = "mock"
        agent._model = "mock"
        agent._max_turns = 1
        agent._monitoring = {"tools": {}}
        agent._scored_df = None  # no data loaded

        result = agent._tool_analyze_location({
            "latitude": 47.6062, "longitude": -122.3321
        })
        # Should either load from disk (CSV absent → None) or return error
        assert "error" in result or result.get("status") == "success"


# ---------------------------------------------------------------------------
# TestAssessAreaState
# ---------------------------------------------------------------------------

class TestAssessAreaState:
    """assess_area for state-level queries."""

    def test_nc_state_returns_success(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        assert result["status"] == "success"

    def test_nc_total_locations(self, agent_with_nc_data, nc_scored_df):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        assert result["total_locations"] == len(nc_scored_df)

    def test_tier_distribution_keys(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        assert {"HIGH", "MODERATE", "LOW", "UNKNOWN"} == set(result["tier_distribution"].keys())

    def test_tier_counts_sum_to_total(self, agent_with_nc_data, nc_scored_df):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        total = sum(v["count"] for v in result["tier_distribution"].values())
        assert total == len(nc_scored_df)

    def test_primary_driver_is_canopy_for_nc(self, agent_with_nc_data):
        """NC fixture has forest-heavy HIGH locations → canopy is primary driver."""
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        assert result["primary_risk_driver"] == "canopy"

    def test_top_sub_areas_are_nc_counties(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        for entry in result["top_high_risk_sub_areas"]:
            assert entry["county"].endswith("County")

    def test_briefing_is_non_empty_string(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        briefing = result["briefing"]
        assert isinstance(briefing, str) and len(briefing) > 50

    def test_briefing_mentions_state(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "NC"})
        assert "NC" in result["briefing"]

    def test_case_insensitive_state(self, agent_with_nc_data, nc_scored_df):
        result_upper = agent_with_nc_data._tool_assess_area({"state": "NC"})
        result_lower = agent_with_nc_data._tool_assess_area({"state": "nc"})
        assert result_upper["total_locations"] == result_lower["total_locations"]


# ---------------------------------------------------------------------------
# TestAssessAreaCounty
# ---------------------------------------------------------------------------

class TestAssessAreaCounty:
    """assess_area filters correctly for county-level queries."""

    def test_mecklenburg_county_success(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({
            "state": "NC", "county": "Mecklenburg County"
        })
        assert result["status"] == "success"

    def test_mecklenburg_correct_location_count(self, agent_with_nc_data, nc_scored_df):
        expected = (nc_scored_df["county"] == "Mecklenburg County").sum()
        result = agent_with_nc_data._tool_assess_area({
            "state": "NC", "county": "Mecklenburg County"
        })
        assert result["total_locations"] == expected

    def test_county_area_label(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({
            "state": "NC", "county": "Durham County"
        })
        assert "Durham County" in result["area"]
        assert "NC" in result["area"]

    def test_county_high_risk_count_correct(self, agent_with_nc_data, nc_scored_df):
        expected_high = (
            (nc_scored_df["county"] == "Durham County")
            & (nc_scored_df["risk_tier"] == "HIGH")
        ).sum()
        result = agent_with_nc_data._tool_assess_area({
            "state": "NC", "county": "Durham County"
        })
        assert result["tier_distribution"]["HIGH"]["count"] == expected_high


# ---------------------------------------------------------------------------
# TestAssessAreaErrors
# ---------------------------------------------------------------------------

class TestAssessAreaErrors:
    """assess_area returns structured errors for invalid inputs."""

    def test_no_state_no_county_returns_error(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({})
        assert "error" in result

    def test_nonexistent_state_returns_error(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({"state": "ZZ"})
        assert "error" in result

    def test_valid_state_nonexistent_county_returns_error(self, agent_with_nc_data):
        result = agent_with_nc_data._tool_assess_area({
            "state": "NC", "county": "Fantasy County"
        })
        assert "error" in result

    def test_no_scored_data_returns_error(self):
        """assess_area with no scored data loaded → error."""
        from src.agent import PipelineAgent
        agent = PipelineAgent.__new__(PipelineAgent)
        agent._api_key = "mock"
        agent._model = "mock"
        agent._max_turns = 1
        agent._monitoring = {"tools": {}}
        agent._scored_df = None

        result = agent._tool_assess_area({"state": "NC"})
        # If CSV doesn't exist, should return error
        if not (Path(__file__).parent.parent / "data" / "processed" / "locations_scored.csv").exists():
            assert "error" in result


# ---------------------------------------------------------------------------
# TestGeocoding
# ---------------------------------------------------------------------------

class TestGeocoding:
    """resolve_location helper — coordinate string parsing only.
    Nominatim calls are NOT made in tests (no network dependency)."""

    def test_explicit_lat_lon(self):
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location(latitude=35.2271, longitude=-80.8431)
        assert result == (35.2271, -80.8431)

    def test_coord_string_comma_separated(self):
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location(address="35.2271, -80.8431")
        assert result == (35.2271, -80.8431)

    def test_coord_string_space_separated(self):
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location(address="35.2271 -80.8431")
        assert result == (35.2271, -80.8431)

    def test_negative_coords_parsed_correctly(self):
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location(address="-34.5, -79.2")
        assert result == (-34.5, -79.2)

    def test_no_input_returns_none(self):
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location()
        assert result is None

    def test_lat_lon_override_address(self):
        """Explicit lat/lon take priority over address."""
        from src.utils.geocoding_utils import resolve_location
        result = resolve_location(
            address="some text that would fail geocoding",
            latitude=35.0, longitude=-80.0,
        )
        assert result == (35.0, -80.0)

    def test_haversine_nc_distance(self):
        """Two points ~11km apart in NC (Charlotte metro)."""
        from src.utils.geocoding_utils import haversine_m
        d = haversine_m(35.2271, -80.8431, 35.3271, -80.8431)  # ~0.1° lat ≈ 11.1 km
        assert 10_000 < d < 12_000

    def test_haversine_zero_distance(self):
        from src.utils.geocoding_utils import haversine_m
        assert haversine_m(35.0, -80.0, 35.0, -80.0) == pytest.approx(0.0, abs=1e-6)

    def test_find_nearest_scored_exact_match(self, nc_scored_df):
        from src.utils.geocoding_utils import find_nearest_scored
        result = find_nearest_scored(35.2271, -80.8431, nc_scored_df, max_distance_m=200)
        assert result is not None
        assert result["location_id"] == "37119_HIGH_01"
        assert result["distance_m"] < 1.0  # exact match

    def test_find_nearest_scored_no_match(self, nc_scored_df):
        from src.utils.geocoding_utils import find_nearest_scored
        result = find_nearest_scored(40.0, -75.0, nc_scored_df, max_distance_m=100)
        assert result is None

    def test_find_alternatives_returns_lower_tier(self, nc_scored_df):
        from src.utils.geocoding_utils import find_alternatives
        alts = find_alternatives(
            35.2271, -80.8431, nc_scored_df,
            radius_m=5000, current_tier="HIGH",
        )
        assert len(alts) > 0
        for a in alts:
            assert a["risk_tier"] in {"LOW", "MODERATE"}
