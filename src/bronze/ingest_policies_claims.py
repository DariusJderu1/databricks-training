"""TODO STUB — Bronze: ingest `policies` (nested JSON arrays) and `claims` (ndjson).

Goal
----
Land the two JSON sources into bronze:
- **policies** are written as multi-line JSON arrays with a nested `coverages[]` field →
  read with `multiLine=true`; keep the array intact (you explode it in silver, M3).
- **claims** are newline-delimited JSON → the default JSON read.

You may use Auto Loader (`cloudFiles`, format `json`) or the `read_files` TVF. For a
first pass, `read_files` is the simplest; switch to Auto Loader for incremental loads.

Acceptance criteria
-------------------
- `insurance.bronze.policies` keeps `coverages` as `array<struct>` (+ `vehicle`/`property`).
- `insurance.bronze.claims` loads all claim records.
- Both carry `_source_file` + `_ingest_ts`; re-runs are incremental/idempotent.

Exam domain 2 (Data Ingestion and Loading) — semi-structured/nested JSON, `read_files`,
`multiLine`. Pattern references: `src/bronze/load_reference.py` · `docs/studybook/M2_*`.
"""
from __future__ import annotations

from src.common import config


def ingest_policies(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")
    target = config.table("bronze", "policies")
    src_path = config.volume_path("policies")
    # TODO: read JSON with multiLine=true (read_files(..., format=>'json', multiLine=>true)
    #       OR Auto Loader cloudFiles json), add _source_file + _ingest_ts, write to `target`.
    #       Keep `coverages` nested — do NOT explode here (that's silver/conform_policies).
    raise NotImplementedError("Implement policies JSON ingestion (multiLine) — see studybook M2.")


def ingest_claims(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")
    target = config.table("bronze", "claims")
    src_path = config.volume_path("claims")
    # TODO: read newline-delimited JSON (Auto Loader cloudFiles json or read_files),
    #       add ingestion metadata, write to `target`.
    raise NotImplementedError("Implement claims JSON ingestion — see studybook M2.")


if __name__ == "__main__":
    ingest_policies()
    ingest_claims()
