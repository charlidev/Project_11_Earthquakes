import pandas as pd
import pytest

from seismic_intelligence.ingestion.geojson_parser import parse_geojson
from seismic_intelligence.quality.profiling import profile_earthquake_data
from seismic_intelligence.transformations.earthquake_transformer import (
    transform_earthquake_events,
)


@pytest.fixture
def sample_geojson():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "us-test-001",
                "properties": {
                    "mag": 4.2,
                    "magType": "mb",
                    "place": "10 km NE of Example",
                    "time": 1735689600000,
                    "updated": 1735689601000,
                    "type": "earthquake",
                    "status": "reviewed",
                    "tsunami": 0,
                    "sig": 280,
                    "alert": None,
                    "url": "https://example.com/event",
                },
                "geometry": {"type": "Point", "coordinates": [-115.5, 32.1, 10.0]},
            }
        ],
    }


def test_parser_extracts_event_correctly(sample_geojson):

    events = parse_geojson(sample_geojson)

    assert len(events) == 1
    assert events[0]["event_id"] == "us-test-001"
    assert events[0]["magnitude"] == 4.2
    assert events[0]["longitude"] == -115.5
    assert events[0]["latitude"] == 32.1


def test_parser_rejects_invalid_payload():

    with pytest.raises(ValueError):
        parse_geojson({"type": "Feature"})


def test_transformer_converts_timestamps(sample_geojson):

    events = parse_geojson(sample_geojson)

    df = transform_earthquake_events(events)

    assert len(df) == 1
    assert pd.api.types.is_float_dtype(df["magnitude"])
    assert str(df["event_time_utc"].dtype) == "datetime64[ns, UTC]"
    assert df["event_time_utc"].notna().all()


def test_transformer_handles_empty_input():

    df = transform_earthquake_events([])

    assert df.empty
    assert len(df.columns) == 15


def test_profiling_detects_duplicate_events_and_invalid_coordinates(sample_geojson):

    # Duplicate the event and introduce invalid coordinates.
    duplicate = sample_geojson["features"][0].copy()

    duplicate["geometry"] = {"type": "Point", "coordinates": [200, 95, 10]}

    payload = {"type": "FeatureCollection", "features": [sample_geojson["features"][0], duplicate]}

    events = parse_geojson(payload)
    df = transform_earthquake_events(events)

    report = profile_earthquake_data(df)

    assert report["summary"]["total_records"] == 2
    assert report["summary"]["duplicate_event_ids"] == 1
    assert report["summary"]["invalid_coordinate_records"] == 1
