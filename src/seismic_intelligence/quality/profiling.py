"""
Earthquake Data Quality Profiling.

Provides descriptive data quality metrics for earthquake datasets.
"""

import pandas as pd


def profile_earthquake_data(df: pd.DataFrame) -> dict:
    """
    Generate a data quality report for earthquake records.

    Returns:
        Dictionary containing summary metrics and profiling DataFrames.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a Pandas DataFrame.")

    if df.empty:
        return {
            "summary": {
                "total_records": 0,
                "total_columns": len(df.columns),
                "duplicate_event_ids": 0,
                "missing_event_ids": 0,
                "invalid_latitudes": 0,
                "invalid_longitudes": 0,
            },
            "null_profile": pd.DataFrame(),
            "duplicate_records": pd.DataFrame(),
            "invalid_coordinates": pd.DataFrame(),
        }

    required_columns = [
        "event_id",
        "magnitude",
        "latitude",
        "longitude",
        "depth_km",
        "event_time_utc",
    ]

    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # --------------------------------------------------------
    # NULL PROFILING
    # --------------------------------------------------------

    null_profile = pd.DataFrame(
        {
            "column_name": df.columns,
            "null_count": df.isnull().sum().values,
            "null_percentage": (df.isnull().mean().values * 100),
        }
    ).sort_values("null_percentage", ascending=False)

    # --------------------------------------------------------
    # DUPLICATE EVENT IDS
    # --------------------------------------------------------

    duplicate_records = df[
        df["event_id"].notna() & df.duplicated(subset=["event_id"], keep=False)
    ].copy()

    # --------------------------------------------------------
    # INVALID COORDINATES
    # --------------------------------------------------------

    invalid_latitude = df["latitude"].notna() & ~df["latitude"].between(-90, 90)

    invalid_longitude = df["longitude"].notna() & ~df["longitude"].between(-180, 180)

    invalid_coordinates = df[invalid_latitude | invalid_longitude].copy()

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    summary = {
        "total_records": len(df),
        "total_columns": len(df.columns),
        "unique_event_ids": df["event_id"].nunique(),
        "duplicate_event_ids": int(df["event_id"].duplicated().sum()),
        "missing_event_ids": int(df["event_id"].isna().sum()),
        "missing_magnitudes": int(df["magnitude"].isna().sum()),
        "missing_event_dates": int(df["event_time_utc"].isna().sum()),
        "missing_coordinates": int((df["latitude"].isna() | df["longitude"].isna()).sum()),
        "invalid_latitudes": int(invalid_latitude.sum()),
        "invalid_longitudes": int(invalid_longitude.sum()),
        "invalid_coordinate_records": len(invalid_coordinates),
    }

    return {
        "summary": summary,
        "null_profile": null_profile,
        "duplicate_records": duplicate_records,
        "invalid_coordinates": invalid_coordinates,
    }
