"""
USGS GeoJSON Parser.

Extracts earthquake attributes and geographical coordinates
from the USGS GeoJSON FeatureCollection response.
"""

from typing import Any


def parse_geojson(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Flatten a USGS GeoJSON FeatureCollection into earthquake records.

    Timestamps remain in Unix milliseconds.
    Data type conversions are handled by the transformation layer.
    """

    if not isinstance(payload, dict):
        raise TypeError("Payload must be a dictionary.")

    if payload.get("type") != "FeatureCollection":
        raise ValueError("Expected a GeoJSON FeatureCollection.")

    features = payload.get("features")

    if not isinstance(features, list):
        raise ValueError("GeoJSON features must be a list.")

    events = []

    for index, feature in enumerate(features):
        if not isinstance(feature, dict):
            raise ValueError(f"Invalid feature at position {index}.")

        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}

        coordinates = geometry.get("coordinates") or []

        if not isinstance(coordinates, list):
            coordinates = []

        longitude = coordinates[0] if len(coordinates) > 0 else None
        latitude = coordinates[1] if len(coordinates) > 1 else None
        depth_km = coordinates[2] if len(coordinates) > 2 else None

        event = {
            "event_id": feature.get("id"),
            "magnitude": properties.get("mag"),
            "magnitude_type": properties.get("magType"),
            "place": properties.get("place"),
            "event_time_ms": properties.get("time"),
            "updated_time_ms": properties.get("updated"),
            "longitude": longitude,
            "latitude": latitude,
            "depth_km": depth_km,
            "event_type": properties.get("type"),
            "status": properties.get("status"),
            "tsunami": properties.get("tsunami"),
            "alert": properties.get("alert"),
            "significance": properties.get("sig"),
            "event_url": properties.get("url"),
        }

        events.append(event)

    return events
