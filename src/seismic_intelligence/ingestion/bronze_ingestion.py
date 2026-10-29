"""
Bronze Ingestion - Project 11 Earthquakes.

Reads original USGS GeoJSON files from ADLS Gen2,
validates records and persists versioned events
into a managed Delta Lake table.

Execution environment: Azure Databricks Serverless.
"""

import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
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


def read_geojson_source(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    """
    Read original USGS GeoJSON FeatureCollections from ADLS.

    Only .geojson files are included.
    Checkpoints and manifest files are excluded.
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


def build_bronze_dataframe(
    source_df: DataFrame,
    batch_id: str,
) -> DataFrame:
    """
    Explode GeoJSON FeatureCollections.

    Preserve the original event as JSON text.

    Calculate SHA-256 to identify individual event versions.
    """

    events_df = source_df.select(
        F.explode("features").alias("event"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
    )

    event_json = F.to_json(
        F.col("event"),
        {"ignoreNullFields": "false"},
    )

    bronze_df = events_df.select(
        # Business identifier.
        F.col("event.id").cast("string").alias("event_id"),
        # Original GeoJSON Feature.
        event_json.alias("event_payload"),
        # Event version identifier.
        F.sha2(event_json, 256).alias("payload_hash"),
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


def validate_bronze_dataframe(df: DataFrame) -> int:
    """
    Validate incoming Bronze records before persistence.

    Repeated event IDs are allowed when their payload hashes
    differ, because they represent different event versions.
    """

    if df.limit(1).count() == 0:
        raise ValueError("No earthquake events were found.")

    total_records = df.count()

    missing_ids = df.filter(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "")).count()

    missing_hashes = df.filter(F.col("payload_hash").isNull()).count()

    duplicate_versions = (
        df.groupBy("event_id", "payload_hash").count().filter(F.col("count") > 1).count()
    )

    unique_event_ids = df.select("event_id").distinct().count()

    source_files = df.select("source_file").distinct().count()

    print("\n========== BRONZE VALIDATION ==========")

    print(f"Total records: {total_records:,}")
    print(f"Unique event IDs: {unique_event_ids:,}")
    print(f"Source files: {source_files:,}")
    print(f"Missing event IDs: {missing_ids:,}")
    print(f"Missing payload hashes: {missing_hashes:,}")
    print(f"Duplicate event versions: {duplicate_versions:,}")

    if missing_ids > 0:
        raise ValueError("Bronze validation failed: missing event IDs.")

    if missing_hashes > 0:
        raise ValueError("Bronze validation failed: missing payload hashes.")

    if duplicate_versions > 0:
        raise ValueError("Bronze validation failed: duplicate event versions.")

    return total_records


# ============================================================
# DELTA PERSISTENCE
# ============================================================


def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
) -> None:
    """
    Persist Bronze records as a managed Delta table.

    Initial execution:
        Create the Delta table.

    Subsequent executions:
        Insert only event versions not already present.

    Existing historical versions are never overwritten.
    """

    if not spark.catalog.tableExists(target_table):
        (df.write.format("delta").mode("errorifexists").saveAsTable(target_table))

        print(f"Initial Delta table created: {target_table}")

    else:
        delta_table = DeltaTable.forName(
            spark,
            target_table,
        )

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                """
                target.event_id = source.event_id
                AND target.payload_hash = source.payload_hash
                """,
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Idempotent MERGE completed: {target_table}")

    print(f"Bronze table persisted: {target_table}")


# ============================================================
# MAIN EXECUTION
# ============================================================


def main() -> None:

    parser = argparse.ArgumentParser(description="USGS Bronze ingestion.")

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
        help="Persist validated records into Delta.",
    )

    args = parser.parse_args()

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    spark = SparkSession.builder.getOrCreate()

    # Standardize timestamp handling.
    spark.conf.set(
        "spark.sql.session.timeZone",
        "UTC",
    )

    print("\n========== BRONZE INGESTION ==========")

    print("Source:", args.source_path)
    print("Target:", args.target_table)
    print("Batch ID:", batch_id)
    print("Write enabled:", args.write)

    # --------------------------------------------------------
    # READ SOURCE
    # --------------------------------------------------------

    source_df = read_geojson_source(
        spark,
        args.source_path,
    )

    # --------------------------------------------------------
    # BUILD BRONZE DATAFRAME
    # --------------------------------------------------------

    bronze_df = build_bronze_dataframe(
        source_df,
        batch_id,
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    total_records = validate_bronze_dataframe(bronze_df)

    print("\n========== BRONZE SCHEMA ==========")

    bronze_df.printSchema()

    print("\n========== SAMPLE RECORDS ==========")

    bronze_df.select(
        "event_id",
        "payload_hash",
        "source_file_name",
        "ingestion_batch_id",
        "ingested_at_utc",
    ).show(
        10,
        truncate=False,
    )

    # --------------------------------------------------------
    # PERSISTENCE
    # --------------------------------------------------------

    if args.write:
        write_bronze_table(
            spark,
            bronze_df,
            args.target_table,
        )

    else:
        print("\nValidation-only execution.")
        print("No Delta table was modified.")

    # --------------------------------------------------------
    # EXECUTION SUMMARY
    # --------------------------------------------------------

    print("\n========== EXECUTION SUMMARY ==========")

    print(f"Records processed: {total_records:,}")
    print("Execution completed successfully.")


if __name__ == "__main__":
    main()
