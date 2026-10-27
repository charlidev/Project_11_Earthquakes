"""
Earthquake Data Transformer.

Converts parsed USGS earthquake records into
a standardized Pandas DataFrame.
"""

import pandas as pd

OUTPUT_COLUMNS = [
    "event_id",
    "magnitude",
    "magnitude_type",
    "place",
    "event_time_utc",
    "updated_time_utc",
    "longitude",
    "latitude",
    "depth_km",
    "event_type",
    "status",
    "tsunami",
    "alert",
    "significance",
    "event_url",
]


def transform_earthquake_events(
    events: list[dict],
) -> pd.DataFrame:
    """
    Normalize parsed earthquake events into a Pandas DataFrame.

    Timestamp values are converted from Unix milliseconds
    to timezone-aware UTC timestamps.

    Numeric and string fields are explicitly converted.
    No business filtering is applied at this stage.
    """

    if not isinstance(events, list):
        raise TypeError("Events must be provided as a list.")

    df = pd.DataFrame.from_records(events)

    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Convert Unix milliseconds to UTC timestamps.
    timestamp_mapping = {
        "event_time_ms": "event_time_utc",
        "updated_time_ms": "updated_time_utc",
    }

    for source, target in timestamp_mapping.items():
        values = df[source] if source in df.columns else pd.Series(pd.NA, index=df.index)

        df[target] = pd.to_datetime(
            values,
            unit="ms",
            utc=True,
            errors="coerce",
        )

    # Convert numeric fields.
    numeric_columns = [
        "magnitude",
        "longitude",
        "latitude",
        "depth_km",
        "significance",
        "tsunami",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    # Convert text fields.
    string_columns = [
        "event_id",
        "magnitude_type",
        "place",
        "event_type",
        "status",
        "alert",
        "event_url",
    ]

    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].astype("string")

    # Ensure a consistent output schema.
    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    return df[OUTPUT_COLUMNS].copy()
