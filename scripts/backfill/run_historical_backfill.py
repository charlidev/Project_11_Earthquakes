"""
Historical Backfill Orchestrator.

Coordinates monthly USGS extraction windows.
Supports dry runs, bounded testing and checkpoint recovery.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from seismic_intelligence.ingestion.backfill_planner import (
    generate_monthly_windows,
    subtract_years,
)
from seismic_intelligence.ingestion.historical_backfill import (
    download_window,
)
from seismic_intelligence.ingestion.usgs_client import (
    USGSEarthquakeClient,
)

OUTPUT_DIR = Path("data/raw/usgs/historical")
MANIFEST_PATH = OUTPUT_DIR / "backfill_manifest.json"

MIN_MAGNITUDE = 2.5
PAGE_SIZE = 5000
MAX_PAGES_PER_BATCH = 2
PAUSE_BETWEEN_WINDOWS = 1.0


def load_or_create_manifest() -> dict:
    """Load an existing extraction configuration or create one."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("r", encoding="utf-8") as file:
            manifest = json.load(file)

        print("Existing backfill manifest loaded.")

        return manifest

    end_time = datetime.now(timezone.utc)
    start_time = subtract_years(end_time, 2)

    manifest = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "min_magnitude": MIN_MAGNITUDE,
        "page_size": PAGE_SIZE,
        "output_dir": str(OUTPUT_DIR),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    temporary_path = MANIFEST_PATH.with_suffix(".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=4)

    temporary_path.replace(MANIFEST_PATH)

    print("New backfill manifest created.")

    return manifest


def run_backfill(
    max_months: int | None = None,
    dry_run: bool = False,
) -> None:

    manifest = load_or_create_manifest()

    start_time = datetime.fromisoformat(manifest["start_time"])
    end_time = datetime.fromisoformat(manifest["end_time"])

    min_magnitude = manifest["min_magnitude"]
    page_size = manifest["page_size"]
    output_dir = Path(manifest["output_dir"])

    windows = generate_monthly_windows(
        start_time,
        end_time,
    )

    print("\n========== HISTORICAL BACKFILL ==========")
    print("Start:", start_time)
    print("End:", end_time)
    print("Minimum magnitude:", min_magnitude)
    print("Page size:", page_size)
    print("Output:", output_dir)

    total_records = 0
    completed_windows = 0
    processed_windows = 0

    for index, (window_start, window_end) in enumerate(
        windows,
        start=1,
    ):
        if max_months is not None and index > max_months:
            break

        processed_windows += 1

        print(f"\n========== WINDOW {index} ==========")

        print("Period:", window_start, "->", window_end)

        if dry_run:
            print("DRY RUN: No API request will be performed.")
            continue

        with USGSEarthquakeClient(
            timeout=30,
            max_retries=2,
            backoff_factor=1.0,
        ) as client:
            # Continue until this monthly window is completed.
            while True:
                result = download_window(
                    client=client,
                    start_time=window_start,
                    end_time=window_end,
                    output_dir=output_dir,
                    min_magnitude=min_magnitude,
                    page_size=page_size,
                    max_pages_per_run=MAX_PAGES_PER_BATCH,
                    pause_seconds=0.5,
                )

                if result["completed"]:
                    break

                print("Continuing from saved checkpoint...")
                time.sleep(1.0)

        completed_windows += 1
        total_records += result["total_records"]

        print(f"Window completed: {result['total_records']:,} records.")

        if not dry_run:
            time.sleep(PAUSE_BETWEEN_WINDOWS)

    print("\n========== BACKFILL SUMMARY ==========")
    print("Windows processed:", processed_windows)

    if dry_run:
        print("Dry run completed. No data downloaded.")
    else:
        print("Windows completed during run:", completed_windows)
        print("Records in completed windows:", f"{total_records:,}")
        print("Historical backfill execution finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="USGS historical earthquake backfill")

    parser.add_argument(
        "--max-months",
        type=int,
        default=None,
        help="Limit the number of monthly windows.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Display extraction windows without calling USGS.",
    )

    args = parser.parse_args()

    if args.max_months is not None and args.max_months < 1:
        parser.error("--max-months must be greater than zero.")

    run_backfill(
        max_months=args.max_months,
        dry_run=args.dry_run,
    )
