"""TODO STUB — Silver: clean & dedupe `customers`.

Goal
----
Read `bronze.customers`, fix the seeded defects, and write a clean, de-duplicated
`silver.customers` (one current row per `customer_id`) plus a `silver.customers_quarantine`
for rows that fail validation.

Defects to handle (see docs/03_data_dictionary.md):
- exact + **fuzzy duplicates** (normalize text, then dedupe by `customer_id`)
- **null** email/phone (quarantine or impute per your rule)
- casing/whitespace in `city`; **invalid/typo `state`** → validate against `bronze.ref_us_states`
- malformed/future `date_of_birth` → parse with fallbacks

Stretch: build a **Type-2** `dim_customer` by reusing the pattern in
`src/silver/clean_agents_scd2.py` (deltas land customer updates with newer `updated_at`).

Acceptance
----------
- `silver.customers` has no duplicate `customer_id` and only canonical 2-letter states.
- Quarantined rows are counted into the DQ scorecard (see `validate_expectations.py`).

Exam domain 3 (Data Transformation and Modeling). Studybook: `docs/studybook/M3_silver_transform.md`.
"""
from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from src.common import config


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    bronze = spark.read.table(config.table("bronze", "customers"))
    states = spark.read.table(config.table("bronze", "ref_us_states"))  # broadcast this small dim

    # TODO 1: trim/standardize text (city, state); normalize state casing.
    # TODO 2: validate state via a broadcast join to ref_us_states; route invalids to quarantine.
    # TODO 3: parse date_of_birth (try multiple formats; null-out future/garbage dates).
    # TODO 4: dedupe — exact first, then fuzzy (same customer_id, normalized attrs).
    # TODO 5: split passed vs quarantined; write silver.customers and silver.customers_quarantine.
    raise NotImplementedError("Implement customers cleaning — see studybook M3.")


if __name__ == "__main__":
    build()
