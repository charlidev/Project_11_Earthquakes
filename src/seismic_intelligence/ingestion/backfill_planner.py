"""
USGS Historical Backfill Planner.

Generates non-overlapping monthly extraction windows
and counts expected earthquake records.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any

MAX_EVENTS_PER_REQUEST = 20_000


def subtract_years(value: datetime, years: int) -> datetime:
    """Subtract calendar years while handling leap days."""

    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def generate_monthly_windows(
    start_time: datetime,
    end_time: datetime,
):
    """
    Generate consecutive monthly windows.

    Windows do not overlap. Each interval is inclusive,
    with millisecond precision at month boundaries.
    """

    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("Dates must be timezone-aware.")

    cursor = start_time.astimezone(timezone.utc)
    end_time = end_time.astimezone(timezone.utc)

    if cursor >= end_time:
        raise ValueError("start_time must be earlier than end_time.")

    while cursor < end_time:
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        window_end = min(next_month - timedelta(milliseconds=1), end_time)

        yield cursor, window_end

        if window_end >= end_time:
            break

        # Advance to the next month without overlap.
        cursor = next_month


def count_backfill_windows(
    client: Any,
    start_time: datetime,
    end_time: datetime,
    min_magnitude: float = 2.5,
    pause_seconds: float = 0.5,
) -> list[dict]:

    results = []

    for index, (window_start, window_end) in enumerate(
        generate_monthly_windows(start_time, end_time), start=1
    ):
        event_count = client.count_earthquakes(
            starttime=window_start,
            endtime=window_end,
            min_magnitude=min_magnitude,
        )

        result = {
            "window_number": index,
            "start_time_utc": window_start,
            "end_time_utc": window_end,
            "event_count": event_count,
            "requires_subdivision": (event_count > MAX_EVENTS_PER_REQUEST),
            "status": "COUNTED",
        }

        results.append(result)

        print(
            f"[{index}] "
            f"{window_start:%Y-%m-%d} to {window_end:%Y-%m-%d} | "
            f"Events: {event_count:,} | "
            f"Subdivision: {result['requires_subdivision']}"
        )

        time.sleep(pause_seconds)

    return results
