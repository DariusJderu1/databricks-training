"""TODO STUB — Silver: conform `policies` (explode coverages, clean, drop orphans).

Goal
----
Turn the nested `bronze.policies` into two tidy silver tables:
- `silver.policies` — one row per policy, cleaned (status mapped, premium fixed, dates parsed).
- `silver.policy_coverages` — one row per coverage (the `coverages[]` array **exploded**).

Defects to handle:
- `status` typos → map to canonical (`ACTIVE/CANCELLED/LAPSED/PENDING`).
- `annual_premium` may be **negative** or a **string** (`"$1,234.50"`) → strip & cast to double.
- malformed/future `effective_date` → parse.
- **orphan `customer_id`** (no matching customer) → quarantine via anti-join.

Acceptance
----------
- `silver.policy_coverages` has `policy_id, coverage_code, limit, deductible, peril_code`.
- `annual_premium` is a clean positive double; statuses are canonical; no orphan policies.

Exam domain 3 — explode arrays, joins, cleaning. Studybook: `docs/studybook/M3_silver_transform.md`.
"""
from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from src.common import config


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    policies = spark.read.table(config.table("bronze", "policies"))
    # customers = spark.read.table(config.table("silver", "customers"))  # for the orphan anti-join

    # TODO 1: clean annual_premium (regexp_replace currency/commas -> cast double; drop/flag negatives).
    # TODO 2: map status typos to canonical values.
    # TODO 3: parse effective_date / expiration_date.
    # TODO 4: anti-join to silver.customers to find orphan customer_id -> quarantine.
    # TODO 5: write silver.policies (one row per policy, drop the coverages array).
    # TODO 6: explode coverages -> silver.policy_coverages
    #   policies.select("policy_id", F.explode("coverages").alias("c")).select("policy_id", "c.*")
    # raise NotImplementedError("Implement policies conform + coverages explode — see studybook M3.")

    # TODO 1: clean annual_premium (regexp_replace currency/commas -> cast double; drop/flag negatives).
    # numeric-as-string "$1,234.50" -> double, then negatives are invalid
    policies = (policies
        .withColumn("annual_premium",
                    F.regexp_replace(F.col("annual_premium").cast("string"), r"[$,]", "").cast("double"))
        .withColumn("premium_valid", F.col("annual_premium") >= 0)
        .withColumn("status", F.upper(F.trim("status")))
        # TODO 3: parse effective_date / expiration_date.
        .withColumn("effective_date", F.coalesce(
                                        F.try_to_date("effective_date", "yyyy-MM-dd"),
                                        F.try_to_date("effective_date", "MM/dd/yyyy"),
                                        F.try_to_date("effective_date", "dd-MMM-yyyy")))
        .withColumn("expiration_date", F.coalesce(
                                        F.try_to_date("expiration_date", "yyyy-MM-dd"),
                                        F.try_to_date("expiration_date", "MM/dd/yyyy"),
                                        F.try_to_date("expiration_date", "dd-MMM-yyyy"))))


if __name__ == "__main__":
    build()
