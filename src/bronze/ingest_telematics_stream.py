"""TODO STUB — Bronze: ingest `telematics` (JSONL) with Auto Loader streaming.

Goal
----
Stream the high-volume telematics JSONL events into bronze with Auto Loader. This is
the source that exercises **schema evolution**: delta batch 2 adds `device_fw`, which
must appear automatically — no manual schema edit.

Acceptance criteria
-------------------
- `insurance.bronze.telematics` exists with `_source_file` + `_ingest_ts`.
- After loading the batch-2 delta files, the table gains `device_fw` automatically.
- Re-running is idempotent (checkpoint tracks processed files).

Exam domain 2 (Data Ingestion and Loading) — Auto Loader streaming, schema evolution.
Pattern references: `src/bronze/load_reference.py` · `docs/studybook/M2_bronze_ingestion.md`.
"""
from __future__ import annotations

from src.common import config

SOURCE = "telematics"


def ingest(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")

    src_path = config.volume_path(SOURCE)
    checkpoint = config.checkpoint_path(f"bronze_{SOURCE}")
    target = config.table("bronze", SOURCE)

    # TODO 1: readStream cloudFiles with format "json", schemaLocation=checkpoint,
    #         cloudFiles.schemaEvolutionMode="addNewColumns", rescuedDataColumn set.
    # TODO 2: add _source_file (_metadata.file_path) and _ingest_ts.
    # TODO 3: writeStream with checkpointLocation, trigger(availableNow=True), toTable(target).
    # NOTE: on Free Edition you use directory-listing discovery (file-notification needs an
    #       access mode that serverless/FE does not provide).
    raise NotImplementedError("Implement the telematics Auto Loader stream — see studybook M2.")


if __name__ == "__main__":
    ingest()
