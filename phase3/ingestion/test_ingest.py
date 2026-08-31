import os
import shutil
import json
import tempfile
import pandas as pd
import pytest

from ingest import (
    detect_files, validate_schema, validate_minimum_quality,
    route_file, run_ingestion, already_processed
)


@pytest.fixture
def temp_dirs():
    """Create isolated temp folders for each test, so tests never touch real data/."""
    base = tempfile.mkdtemp()
    landing = os.path.join(base, "landing")
    raw = os.path.join(base, "raw")
    rejected = os.path.join(base, "rejected")
    for d in [landing, raw, rejected]:
        os.makedirs(d)
    log_path = os.path.join(base, "ingestion_log.jsonl")
    yield landing, raw, rejected, log_path
    shutil.rmtree(base)


def write_csv(path, rows_df):
    rows_df.to_csv(path, index=False)


def test_valid_file_routes_to_raw(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    valid_df = pd.DataFrame({
        "datetime": ["2013-11-01 00:00:00"],
        "CellID": [1], "countrycode": [0],
        "smsin": [0.35], "smsout": [0.0], "callin": [0.0],
        "callout": [0.02], "internet": [0.0],
    })
    filename = "sms-call-internet-mi-2013-11-01.csv"
    write_csv(os.path.join(landing, filename), valid_df)

    results = run_ingestion(landing, raw, rejected, log_path)

    assert results[0]["status"] == "ACCEPTED"
    assert os.path.exists(os.path.join(raw, filename))
    assert not os.path.exists(os.path.join(rejected, filename))


def test_missing_column_routes_to_rejected(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    invalid_df = pd.DataFrame({
        # countrycode deliberately missing
        "datetime": ["2013-11-08 00:00:00"],
        "CellID": [1],
        "smsin": [0.35], "smsout": [0.0], "callin": [0.0],
        "callout": [0.02], "internet": [0.0],
    })
    filename = "sms-call-internet-mi-2013-11-08.csv"
    write_csv(os.path.join(landing, filename), invalid_df)

    results = run_ingestion(landing, raw, rejected, log_path)

    assert results[0]["status"] == "REJECTED"
    assert "MISSING_COLUMNS" in results[0]["reason"]
    assert os.path.exists(os.path.join(rejected, filename))
    assert not os.path.exists(os.path.join(raw, filename))


def test_negative_activity_value_rejected(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    invalid_df = pd.DataFrame({
        "datetime": ["2013-11-09 00:00:00"],
        "CellID": [1], "countrycode": [0],
        "smsin": [-5.0], "smsout": [0.0], "callin": [0.0],
        "callout": [0.02], "internet": [0.0],
    })
    filename = "sms-call-internet-mi-2013-11-09.csv"
    write_csv(os.path.join(landing, filename), invalid_df)

    results = run_ingestion(landing, raw, rejected, log_path)

    assert results[0]["status"] == "REJECTED"
    assert "NEGATIVE_VALUES" in results[0]["reason"]


def test_malformed_timestamp_rejected(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    invalid_df = pd.DataFrame({
        "datetime": ["NOT_A_TIMESTAMP"],
        "CellID": [1], "countrycode": [0],
        "smsin": [0.35], "smsout": [0.0], "callin": [0.0],
        "callout": [0.02], "internet": [0.0],
    })
    filename = "sms-call-internet-mi-2013-11-10.csv"
    write_csv(os.path.join(landing, filename), invalid_df)

    results = run_ingestion(landing, raw, rejected, log_path)

    assert results[0]["status"] == "REJECTED"
    assert "MALFORMED_TIMESTAMP" in results[0]["reason"]


def test_rerun_does_not_duplicate_processed_file(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    valid_df = pd.DataFrame({
        "datetime": ["2013-11-01 00:00:00"],
        "CellID": [1], "countrycode": [0],
        "smsin": [0.35], "smsout": [0.0], "callin": [0.0],
        "callout": [0.02], "internet": [0.0],
    })
    filename = "sms-call-internet-mi-2013-11-01.csv"
    write_csv(os.path.join(landing, filename), valid_df)

    # First run — should accept and move the file
    run_ingestion(landing, raw, rejected, log_path)
    assert os.path.exists(os.path.join(raw, filename))

    # Simulate the file re-appearing in landing (e.g. re-delivered)
    write_csv(os.path.join(landing, filename), valid_df)
    results = run_ingestion(landing, raw, rejected, log_path)

    assert results[0]["status"] == "SKIPPED"


def test_reference_geojson_never_processed(temp_dirs):
    landing, raw, rejected, log_path = temp_dirs
    # Place a GeoJSON-like file in landing by mistake — should be ignored entirely
    with open(os.path.join(landing, "milano-grid.geojson"), "w") as f:
        f.write('{"type": "FeatureCollection", "features": []}')

    results = run_ingestion(landing, raw, rejected, log_path)

    assert len(results) == 0  # nothing processed — GeoJSON doesn't match the pattern