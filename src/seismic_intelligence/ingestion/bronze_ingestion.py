"""
Bronze Ingestion - Project 11 Earthquakes.

Reads original USGS GeoJSON files from ADLS Gen2
and prepares an auditable Bronze Delta dataset.

Execution environment: Azure Databricks.
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_SOURCE_PATH = (
    "abfss://seismic-dev@rgseismic.dfs.core.windows.net/"
    "landing/usgs/historical/2026-09-01_2026-09-30/"
)

DEFAULT_TARGET_TABLE = "seismic_dev.bronze.earthquake_events"


# ============================================================
# SOURCE READER
# ============================================================


def read_geojson_source(spark, source_path: str):
    """
    Read original USGS GeoJSON FeatureCollections from ADLS.

    Only .geojson files are included.
    JSON metadata and checkpoint files are excluded.
    """

    source_df = (
        spark.read.format("json")
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.geojson")
        .load(source_path)
    )

    return source_df


# ============================================================
# BRONZE TRANSFORMATION
# ============================================================


def build_bronze_dataframe(source_df, batch_id: str):
    """
    Explode GeoJSON FeatureCollections.

    Preserve original event information as a nested structure.
    Add ingestion metadata without applying business rules.
    """

    events_df = source_df.select(
        F.explode("features").alias("event"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
    )

    bronze_df = events_df.select(
        F.col("event.id").alias("event_id"),
        # Preserve the complete original GeoJSON Feature.
        F.col("event").alias("event_payload"),
        # Source traceability.
        F.col("source_file"),
        F.col("source_file_name"),
        F.col("source_file_modified_at"),
        # Ingestion metadata.
        F.lit("USGS").alias("source_system"),
        F.lit(batch_id).alias("ingestion_batch_id"),
        F.current_timestamp().alias("ingested_at_utc"),
        F.current_date().alias("ingestion_date"),
    )

    return bronze_df


# ============================================================
# DATA VALIDATION
# ============================================================


def validate_bronze_dataframe(df):
    """Validate the Bronze dataset before persistence."""

    if df.isEmpty():
        raise ValueError("No earthquake events were found.")

    total_records = df.count()

    missing_ids = df.filter(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "")).count()

    duplicate_ids = df.groupBy("event_id").count().filter(F.col("count") > 1).count()

    source_files = df.select("source_file").distinct().count()

    print("\n========== BRONZE VALIDATION ==========")

    print(f"Total records: {total_records:,}")
    print(f"Source files: {source_files:,}")
    print(f"Missing event IDs: {missing_ids:,}")
    print(f"Duplicate event IDs: {duplicate_ids:,}")

    if missing_ids > 0:
        raise ValueError("Bronze validation failed: missing event IDs.")

    if duplicate_ids > 0:
        raise ValueError("Bronze validation failed: duplicate event IDs.")

    return total_records


# ============================================================
# DELTA PERSISTENCE
# ============================================================


def write_bronze_table(df, target_table: str):
    """
    Persist the validated DataFrame as a managed Delta table.

    This initial implementation uses append mode.
    Idempotent incremental writes will be implemented later.
    """

    (df.write.format("delta").mode("append").saveAsTable(target_table))

    print(f"Bronze table written: {target_table}")


# ============================================================
# MAIN EXECUTION
# ============================================================


def main():

    parser = argparse.ArgumentParser(description="USGS Bronze ingestion")

    parser.add_argument(
        "--source-path",
        default=DEFAULT_SOURCE_PATH,
    )

    parser.add_argument(
        "--target-table",
        default=DEFAULT_TARGET_TABLE,
    )

    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist the validated data into Delta.",
    )

    args = parser.parse_args()

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    spark = SparkSession.builder.getOrCreate()

    print("\n========== BRONZE INGESTION ==========")

    print("Source:", args.source_path)
    print("Target:", args.target_table)
    print("Batch ID:", batch_id)
    print("Write enabled:", args.write)

    # Read source files.
    source_df = read_geojson_source(
        spark,
        args.source_path,
    )

    # Transform into Bronze structure.
    bronze_df = build_bronze_dataframe(
        source_df,
        batch_id,
    )

    # Validate.
    total_records = validate_bronze_dataframe(bronze_df)

    print("\n========== BRONZE SCHEMA ==========")
    bronze_df.printSchema()

    print("\n========== SAMPLE RECORDS ==========")

    bronze_df.select(
        "event_id",
        "source_file_name",
        "ingested_at_utc",
    ).show(10, truncate=False)

    # Persist only when explicitly requested.
    if args.write:
        write_bronze_table(
            bronze_df,
            args.target_table,
        )

    else:
        print("\nValidation-only execution.")
        print("No Delta table was modified.")

    print("\n========== EXECUTION SUMMARY ==========")
    print(f"Records processed: {total_records:,}")
    print("Execution completed successfully.")


if __name__ == "__main__":
    main()
