"""
Tests for src/ingest.py — data ingestion and validation.

Test strategy:
  - load_locations: tested against temporary CSV files created from in-memory
    DataFrames (via conftest.py fixtures), exercising the full file-I/O path.
  - validate_locations: tested with in-memory DataFrames for surgical control
    over exactly which rows are bad and why.
  - generate_quality_report: tested for output format and disk write.

No CSV data files are committed to the repository. All test data is generated
programmatically in tests/conftest.py following industry standard practice.
"""

from pathlib import Path

import pandas as pd
import pytest

from src.ingest import (
    VALID_STATE_CODES,
    generate_quality_report,
    load_locations,
    validate_locations,
)


def _make_df(**overrides) -> pd.DataFrame:
    """Build a minimal valid single-row DataFrame, with optional column overrides.

    Represents post-load data (after geoid_cb parsing has derived state).
    validate_locations() only uses location_id, latitude, longitude, and state —
    the remaining columns are carried through unchanged.
    """
    base = {
        "location_id": "LOC_TEST_01",
        "latitude": 45.0,
        "longitude": -100.0,
        "geoid_cb": "300490101001001",  # MT Meagher — valid FIPS
        "state": "MT",
        "county_fips": "30049",
    }
    base.update(overrides)
    return pd.DataFrame([base])


def _make_valid_df(n: int = 5) -> pd.DataFrame:
    """Build a DataFrame with *n* valid CONUS records.

    Represents post-load data with state derived from geoid_cb.
    """
    rows = [
        {
            "location_id": f"LOC_{i:04d}",
            "latitude": 35.0 + i * 2,
            "longitude": -100.0 - i,
            "geoid_cb": f"4800{i:01d}0101001001",  # TX, synthetic county
            "state": "TX",
            "county_fips": f"4800{i:01d}",
        }
        for i in range(n)
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# load_locations
# ---------------------------------------------------------------------------

class TestLoadLocations:

    def test_loads_valid_csv(self, valid_locations_csv):
        df = load_locations(valid_locations_csv)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10

    def test_returns_expected_columns(self, valid_locations_csv):
        """After loading, state and county_fips must be derived from geoid_cb."""
        df = load_locations(valid_locations_csv)
        for col in ["location_id", "latitude", "longitude", "geoid_cb", "state", "county_fips"]:
            assert col in df.columns, f"Expected column '{col}' not found"

    def test_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            load_locations(tmp_path / "nonexistent.csv")

    def test_raises_on_missing_critical_columns(self, tmp_path):
        """CSV missing location_id, latitude, longitude must raise ValueError."""
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("lat,lon\n45.0,-100.0\n")
        with pytest.raises(ValueError, match="Missing required columns"):
            load_locations(bad_csv)

    def test_loads_successfully_without_optional_columns(self, tmp_path):
        """CSV with only critical columns loads without error.

        reverse_geocoder auto-fills state and county from coordinates, so the
        returned DataFrame will have those columns even if the CSV did not.
        The test verifies the critical columns are present and the row count is
        correct — not the absence of optional columns, since the pipeline now
        fills them proactively.
        """
        minimal_csv = tmp_path / "minimal.csv"
        minimal_csv.write_text("location_id,latitude,longitude\nLOC_01,45.0,-100.0\n")
        df = load_locations(minimal_csv)
        assert len(df) == 1
        assert "location_id" in df.columns
        assert "latitude" in df.columns
        assert "longitude" in df.columns

    def test_loads_issues_csv(self, issues_locations_csv):
        """CSV with problematic rows must load without error — cleaning is separate."""
        df = load_locations(issues_locations_csv)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0


# ---------------------------------------------------------------------------
# validate_locations — null handling
# ---------------------------------------------------------------------------

class TestValidateLocationsNulls:

    def test_drops_null_latitude(self):
        df = pd.DataFrame([
            {"location_id": "GOOD", "latitude": 40.0, "longitude": -90.0, "state": "IL", "county": "Cook"},
            {"location_id": "NULL_LAT", "latitude": None, "longitude": -90.0, "state": "IL", "county": "Cook"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert clean.iloc[0]["location_id"] == "GOOD"
        assert report["drop_reasons"]["null_latitude"] == 1

    def test_drops_null_longitude(self):
        df = pd.DataFrame([
            {"location_id": "GOOD", "latitude": 40.0, "longitude": -90.0, "state": "IL", "county": "Cook"},
            {"location_id": "NULL_LON", "latitude": 40.0, "longitude": None, "state": "IL", "county": "Cook"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["null_longitude"] == 1

    def test_drops_null_location_id(self):
        df = pd.DataFrame([
            {"location_id": "GOOD", "latitude": 40.0, "longitude": -90.0, "state": "IL", "county": "Cook"},
            {"location_id": None, "latitude": 40.0, "longitude": -90.0, "state": "IL", "county": "Cook"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["null_location_id"] == 1

    def test_drops_multiple_null_types_counted_separately(self):
        """Null lat and null lon are on different rows — both should be counted."""
        df = pd.DataFrame([
            {"location_id": "GOOD",     "latitude": 40.0, "longitude": -90.0, "state": "IL", "county": "Cook"},
            {"location_id": "NULL_LAT", "latitude": None, "longitude": -90.0, "state": "IL", "county": "Cook"},
            {"location_id": "NULL_LON", "latitude": 40.0, "longitude": None,  "state": "IL", "county": "Cook"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"].get("null_latitude", 0) == 1
        assert report["drop_reasons"].get("null_longitude", 0) == 1

    def test_all_valid_rows_returns_zero_dropped(self):
        df = _make_valid_df(5)
        clean, report = validate_locations(df)
        assert report["dropped_records"] == 0
        assert report["valid_records"] == 5
        assert len(clean) == 5


# ---------------------------------------------------------------------------
# validate_locations — out-of-range coordinates
# ---------------------------------------------------------------------------

class TestValidateLocationsOutOfRange:

    def test_drops_latitude_too_high(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",    "latitude": 45.0,  "longitude": -100.0, "state": "MT", "county": "C"},
            {"location_id": "LAT_HIGH","latitude": 85.0,  "longitude": -100.0, "state": "MT", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["out_of_range_coordinates"] == 1

    def test_drops_latitude_too_low(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",   "latitude": 40.0, "longitude": -100.0, "state": "CO", "county": "C"},
            {"location_id": "LAT_LOW","latitude": 10.0, "longitude": -100.0, "state": "CO", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["out_of_range_coordinates"] == 1

    def test_drops_longitude_too_low(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",   "latitude": 40.0, "longitude": -100.0,  "state": "CO", "county": "C"},
            {"location_id": "LON_LOW","latitude": 40.0, "longitude": -170.0,  "state": "CO", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["out_of_range_coordinates"] == 1

    def test_drops_longitude_too_high(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",    "latitude": 40.0, "longitude": -100.0, "state": "CO", "county": "C"},
            {"location_id": "LON_HIGH","latitude": 40.0, "longitude": -20.0,  "state": "CO", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["out_of_range_coordinates"] == 1

    def test_boundary_values_are_valid(self):
        """Exact boundary coordinates should NOT be dropped."""
        from src.config import CONUS_LAT_MIN, CONUS_LAT_MAX, CONUS_LON_MIN, CONUS_LON_MAX
        df = pd.DataFrame([
            {"location_id": "MIN_CORNER", "latitude": CONUS_LAT_MIN, "longitude": CONUS_LON_MIN, "state": "FL", "county": "C"},
            {"location_id": "MAX_CORNER", "latitude": CONUS_LAT_MAX, "longitude": CONUS_LON_MAX, "state": "ME", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 2
        assert report["dropped_records"] == 0


# ---------------------------------------------------------------------------
# validate_locations — duplicate location_id
# ---------------------------------------------------------------------------

class TestValidateLocationsDuplicates:

    def test_keeps_first_drops_duplicate(self):
        df = pd.DataFrame([
            {"location_id": "DUP_01", "latitude": 40.0, "longitude": -100.0, "state": "CO", "county_fips": "08013"},
            {"location_id": "DUP_01", "latitude": 41.0, "longitude": -101.0, "state": "CO", "county_fips": "08014"},
            {"location_id": "UNIQUE", "latitude": 42.0, "longitude": -102.0, "state": "CO", "county_fips": "08015"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 2
        assert report["drop_reasons"]["duplicate_location_id"] == 1
        # The first occurrence (county_fips "08013") should be retained
        dup_row = clean[clean["location_id"] == "DUP_01"].iloc[0]
        assert dup_row["county_fips"] == "08013"

    def test_multiple_duplicates(self):
        df = pd.DataFrame([
            {"location_id": "DUP", "latitude": 40.0, "longitude": -100.0, "state": "CO", "county_fips": "08013"},
            {"location_id": "DUP", "latitude": 40.1, "longitude": -100.1, "state": "CO", "county_fips": "08014"},
            {"location_id": "DUP", "latitude": 40.2, "longitude": -100.2, "state": "CO", "county_fips": "08015"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["duplicate_location_id"] == 2


# ---------------------------------------------------------------------------
# validate_locations — invalid state codes
# ---------------------------------------------------------------------------

class TestValidateLocationsStateCodes:

    def test_drops_invalid_state(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",    "latitude": 40.0, "longitude": -100.0, "state": "CO", "county": "C"},
            {"location_id": "BAD_ST",  "latitude": 40.0, "longitude": -100.0, "state": "XX", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["drop_reasons"]["invalid_state_code"] == 1

    def test_state_code_case_insensitive(self):
        """Lowercase state codes should be normalised and accepted."""
        df = pd.DataFrame([
            {"location_id": "LOWER", "latitude": 40.0, "longitude": -100.0, "state": "co", "county": "C"},
        ])
        clean, report = validate_locations(df)
        assert len(clean) == 1
        assert report["dropped_records"] == 0

    def test_all_50_states_plus_dc_are_valid(self):
        """Every state in VALID_STATE_CODES must pass validation."""
        rows = [
            {
                "location_id": f"LOC_{st}",
                "latitude": 40.0,
                "longitude": -100.0,
                "state": st,
                "county": "TestCounty",
            }
            for st in VALID_STATE_CODES
        ]
        df = pd.DataFrame(rows)
        clean, report = validate_locations(df)
        assert report["dropped_records"] == 0
        assert len(clean) == len(VALID_STATE_CODES)


# ---------------------------------------------------------------------------
# validate_locations — quality report metrics
# ---------------------------------------------------------------------------

class TestQualityReportMetrics:

    def test_report_counts_are_consistent(self):
        df = _make_valid_df(10)
        clean, report = validate_locations(df)
        assert report["total_records"] == 10
        assert report["valid_records"] == len(clean)
        assert report["dropped_records"] == 10 - len(clean)
        assert report["total_records"] == report["valid_records"] + report["dropped_records"]

    def test_retention_rate_100_percent_for_clean_data(self):
        df = _make_valid_df(8)
        _, report = validate_locations(df)
        assert report["retention_rate_pct"] == 100.0

    def test_retention_rate_50_percent(self):
        df = pd.DataFrame([
            {"location_id": "GOOD",  "latitude": 40.0, "longitude": -100.0, "state": "CO", "county": "C"},
            {"location_id": "NULL",  "latitude": None,  "longitude": -100.0, "state": "CO", "county": "C"},
        ])
        _, report = validate_locations(df)
        assert report["retention_rate_pct"] == 50.0


# ---------------------------------------------------------------------------
# generate_quality_report
# ---------------------------------------------------------------------------

class TestGenerateQualityReport:

    def test_returns_string(self):
        _, report = validate_locations(_make_valid_df(5))
        text = generate_quality_report(report)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_report_contains_key_metrics(self):
        _, report = validate_locations(_make_valid_df(5))
        text = generate_quality_report(report)
        assert "Total records" in text
        assert "Valid records" in text
        assert "Dropped records" in text
        assert "Retention rate" in text

    def test_report_saved_to_disk(self, tmp_path, monkeypatch):
        """Report must be written to OUTPUT_DIR/data_quality_report.txt."""
        import src.ingest as ingest_module
        fake_path = tmp_path / "data_quality_report.txt"
        monkeypatch.setattr(ingest_module, "DATA_QUALITY_REPORT_PATH", fake_path)

        _, report = validate_locations(_make_valid_df(3))
        generate_quality_report(report)

        assert fake_path.exists()
        content = fake_path.read_text()
        assert "Total records" in content

    def test_issues_fixture_full_pipeline(self, issues_locations_csv):
        """End-to-end: load issues CSV → validate → report.

        The issues fixture (see conftest.py) has 10 rows with 6 expected drops:
          1 null lat, 1 null lon, 1 null location_id,
          1 out-of-range lat, 1 out-of-range lon,
          1 duplicate location_id.
        The LOC_NULL_GEOID row (null geoid_cb) gets state filled by reverse
        geocoding and is NOT dropped — it counts as valid.
        Exactly 4 rows should survive.
        """
        df = load_locations(issues_locations_csv)
        clean, report = validate_locations(df)
        text = generate_quality_report(report)

        assert report["total_records"] == len(df)
        assert report["dropped_records"] == 6
        assert report["valid_records"] == 4
        assert isinstance(text, str)
