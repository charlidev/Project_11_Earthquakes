"""
USGS Historical Backfill.

Downloads GeoJSON pages and persists progress locally.
Supports checkpoint-based recovery and bounded execution.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_iso(value: datetime) -> str:
    """Return a timezone-aware UTC ISO timestamp."""

    if value.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware.")

    return value.astimezone(timezone.utc).isoformat()


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON safely using a temporary file."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, separators=(",", ":"))

    temporary_path.replace(path)


def _load_checkpoint(
    checkpoint_path: Path,
    start_time: datetime,
    end_time: datetime,
    min_magnitude: float,
    page_size: int,
) -> dict[str, Any]:

    if checkpoint_path.exists():
        with checkpoint_path.open("r", encoding="utf-8") as file:
            checkpoint = json.load(file)

        expected = {
            "start_time": _utc_iso(start_time),
            "end_time": _utc_iso(end_time),
            "min_magnitude": min_magnitude,
            "page_size": page_size,
        }

        for key, value in expected.items():
            if checkpoint.get(key) != value:
                raise ValueError(
                    f"Checkpoint configuration mismatch: {key}. "
                    "Use the original parameters or a new output directory."
                )

        return checkpoint

    return {
        **{
            "start_time": _utc_iso(start_time),
            "end_time": _utc_iso(end_time),
            "min_magnitude": min_magnitude,
            "page_size": page_size,
        },
        "next_offset": 1,
        "total_records": 0,
        "pages": 0,
        "completed": False,
        "status": "pending",
        "updated_at": _utc_iso(datetime.now(timezone.utc)),
    }


def download_window(
    client: Any,
    start_time: datetime,
    end_time: datetime,
    output_dir: str | Path,
    min_magnitude: float = 2.5,
    page_size: int = 5000,
    max_pages_per_run: int = 2,
    pause_seconds: float = 0.5,
) -> dict[str, Any]:
    """
    Download one historical time window.

    Each page is persisted independently.
    The checkpoint is updated after each successful page.

    max_pages_per_run limits work per execution.
    Running the function again resumes from next_offset.
    """

    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time.")

    if not 1 <= page_size <= 20000:
        raise ValueError("page_size must be between 1 and 20,000.")

    if max_pages_per_run < 1:
        raise ValueError("max_pages_per_run must be at least 1.")

    window_name = f"{start_time:%Y-%m-%d}_{end_time:%Y-%m-%d}"

    window_dir = Path(output_dir) / window_name
    window_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = window_dir / "checkpoint.json"

    checkpoint = _load_checkpoint(
        checkpoint_path=checkpoint_path,
        start_time=start_time,
        end_time=end_time,
        min_magnitude=min_magnitude,
        page_size=page_size,
    )

    if checkpoint["completed"]:
        print(f"Window already completed: {window_name}")
        return checkpoint

    pages_this_run = 0

    print(f"\nProcessing window: {window_name}")
    print(f"Starting offset: {checkpoint['next_offset']}")

    while pages_this_run < max_pages_per_run:
        offset = checkpoint["next_offset"]

        try:
            payload = client.get_earthquakes(
                starttime=start_time,
                endtime=end_time,
                min_magnitude=min_magnitude,
                limit=page_size,
                offset=offset,
                orderby="time-asc",
            )

            features = payload.get("features", [])

            if not isinstance(features, list):
                raise ValueError("Invalid GeoJSON features structure.")

            if not features:
                checkpoint["completed"] = True
                checkpoint["status"] = "completed"
                checkpoint["updated_at"] = _utc_iso(datetime.now(timezone.utc))

                _atomic_write_json(checkpoint_path, checkpoint)

                print("No additional events. Window completed.")
                break

            page_number = checkpoint["pages"] + 1

            page_path = window_dir / f"page_{page_number:04d}_offset_{offset:08d}.geojson"

            # Persist the original GeoJSON response.
            _atomic_write_json(page_path, payload)

            # Update checkpoint only after page persistence.
            checkpoint["pages"] += 1
            checkpoint["total_records"] += len(features)
            checkpoint["next_offset"] += len(features)

            # A short page indicates the final page.
            checkpoint["completed"] = len(features) < page_size

            checkpoint["status"] = "completed" if checkpoint["completed"] else "in_progress"

            checkpoint["updated_at"] = _utc_iso(datetime.now(timezone.utc))

            _atomic_write_json(checkpoint_path, checkpoint)

            pages_this_run += 1

            print(
                f"Page {page_number} | "
                f"Offset: {offset} | "
                f"Records: {len(features):,} | "
                f"Total: {checkpoint['total_records']:,}"
            )

            if checkpoint["completed"]:
                print("Window completed successfully.")
                break

            if pages_this_run < max_pages_per_run:
                time.sleep(pause_seconds)

        except Exception as error:
            checkpoint["status"] = "failed"
            checkpoint["last_error"] = str(error)
            checkpoint["updated_at"] = _utc_iso(datetime.now(timezone.utc))

            _atomic_write_json(checkpoint_path, checkpoint)

            raise

    if not checkpoint["completed"]:
        print(f"Execution limit reached. Next offset: {checkpoint['next_offset']}")

    return checkpoint
