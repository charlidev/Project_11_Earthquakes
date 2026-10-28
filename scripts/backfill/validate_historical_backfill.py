import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("data/raw/usgs/historical")

manifest_path = ROOT / "backfill_manifest.json"

if not manifest_path.exists():
    raise FileNotFoundError("Backfill manifest not found.")

with manifest_path.open(encoding="utf-8") as file:
    manifest = json.load(file)

event_ids = set()

total_records = 0
total_pages = 0
completed_windows = 0
incomplete_windows = []
missing_ids = 0
duplicate_ids = 0
invalid_magnitudes = 0
out_of_range_magnitudes = 0

earliest_event = None
latest_event = None

window_directories = sorted(path for path in ROOT.iterdir() if path.is_dir())

for directory in window_directories:
    checkpoint_path = directory / "checkpoint.json"

    if not checkpoint_path.exists():
        incomplete_windows.append(directory.name)
        continue

    with checkpoint_path.open(encoding="utf-8") as file:
        checkpoint = json.load(file)

    if not checkpoint.get("completed"):
        incomplete_windows.append(directory.name)
        continue

    completed_windows += 1

    pages = sorted(directory.glob("page_*.geojson"))
    total_pages += len(pages)

    window_records = 0

    for page_path in pages:
        with page_path.open(encoding="utf-8") as file:
            payload = json.load(file)

        if payload.get("type") != "FeatureCollection":
            raise ValueError(f"Invalid GeoJSON: {page_path}")

        for feature in payload.get("features", []):
            properties = feature.get("properties") or {}

            event_id = feature.get("id")

            if not event_id:
                missing_ids += 1
            elif event_id in event_ids:
                duplicate_ids += 1
            else:
                event_ids.add(event_id)

            magnitude = properties.get("mag")

            if magnitude is None:
                invalid_magnitudes += 1
            elif magnitude < manifest["min_magnitude"]:
                out_of_range_magnitudes += 1

            event_time = properties.get("time")

            if event_time is not None:
                event_datetime = datetime.fromtimestamp(event_time / 1000, tz=timezone.utc)

                if earliest_event is None or event_datetime < earliest_event:
                    earliest_event = event_datetime

                if latest_event is None or event_datetime > latest_event:
                    latest_event = event_datetime

            window_records += 1

    total_records += window_records

    if window_records != checkpoint["total_records"]:
        print(f"WARNING: Record mismatch in {directory.name}")

print("\n========== BACKFILL AUDIT ==========")

print("Windows found:", len(window_directories))
print("Completed windows:", completed_windows)
print("Pages found:", total_pages)
print("Total records:", total_records)
print("Unique event IDs:", len(event_ids))
print("Duplicate IDs:", duplicate_ids)
print("Missing IDs:", missing_ids)
print("Missing magnitudes:", invalid_magnitudes)
print("Magnitudes below threshold:", out_of_range_magnitudes)
print("Incomplete windows:", incomplete_windows)

print("Earliest event:", earliest_event)
print("Latest event:", latest_event)

print("\nExpected count from previous COUNT: 57,523")
print("Difference:", 57523 - total_records)

assert duplicate_ids == 0, "Duplicate event IDs detected."
assert missing_ids == 0, "Missing event IDs detected."
assert not incomplete_windows, "Incomplete windows detected."

print("\nBackfill audit completed.")
