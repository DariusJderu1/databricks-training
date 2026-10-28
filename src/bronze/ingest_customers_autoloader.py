"""TODO STUB — Bronze: ingest `customers` (CSV) with Auto Loader + schema evolution.

Goal
----
Stream the customer CSV files from the landing Volume into an append-only bronze
Delta table using **Auto Loader** (`cloudFiles`) in directory-listing mode with
schema evolution, attaching ingestion metadata.

Acceptance criteria
-------------------
- `insurance.bronze.customers` exists, with `_source_file` and `_ingest_ts` columns.
- Re-running is **idempotent** (already-seen files are skipped via the checkpoint).
- Columns added by later delta files are picked up automatically (schema evolution).

Exam domain 2 (Data Ingestion and Loading).
Pattern references: `src/bronze/load_reference.py` · `docs/studybook/M2_bronze_ingestion.md`.
"""
from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401  (you'll use F for the metadata cols)

from src.common import config

SOURCE = "customers"


def ingest(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")

    src_path = config.volume_path(SOURCE)              # /Volumes/insurance/landing/raw/customers
    checkpoint = config.checkpoint_path(f"bronze_{SOURCE}")
    target = config.table("bronze", SOURCE)            # insurance.bronze.customers

    # TODO 1: build the Auto Loader readStream
    #   stream = (spark.readStream.format("cloudFiles")
    #       .option("cloudFiles.format", "csv")
    #       .option("header", "true")
    #       .option("cloudFiles.schemaLocation", checkpoint)
    #       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    #       .option("rescuedDataColumn", "_rescued_data")
    #       .load(src_path))
    # TODO 2: add ingestion metadata
    #   .selectExpr("*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingest_ts")
    # TODO 3: write the stream (batch-incremental is budget-friendly on Free Edition)
    #   (stream.writeStream
    #       .option("checkpointLocation", checkpoint)
    #       .trigger(availableNow=True)
    #       .toTable(target))
    raise NotImplementedError("Implement the customers Auto Loader ingestion — see studybook M2.")


if __name__ == "__main__":
    ingest()
