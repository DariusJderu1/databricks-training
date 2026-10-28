"""TODO STUB — Silver: clean `claims` (normalize fraud flag, map status, validate amounts).

Goal
----
Read `bronze.claims` and write a clean `silver.claims` + `silver.claims_quarantine`.

Defects to handle:
- **`fraud_flag`** is mixed (`true/false`, `Y/N`, `1/0`, `YES/NO`) → normalize to **boolean**.
- `claim_status` typos → map via `bronze.ref_claim_status`.
- malformed/future `claim_date` → parse.
- **`loss_amount` > policy `sum_insured`** → flag/quarantine (join `silver.policies`).
- **orphan `policy_id`** → quarantine via anti-join.
- null `peril_code`/`reported_amount` → per your DQ rule.

Acceptance
----------
- `silver.claims.fraud_flag` is a real boolean; fraud rate ≈ 4%.
- No claim references a missing policy; `loss_amount` never silently exceeds `sum_insured`.

Exam domain 3 — cleaning, joins, validation. Studybook: `docs/studybook/M3_silver_transform.md`.
"""
from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from src.common import config

TRUE_TOKENS = ["true", "y", "yes", "1"]


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    claims = spark.read.table(config.table("bronze", "claims"))
    # policies = spark.read.table(config.table("silver", "policies"))   # for orphan + sum_insured checks

    # TODO 1: normalize fraud_flag -> boolean
    #   F.lower(F.trim("fraud_flag")).isin(TRUE_TOKENS)
    # TODO 2: map claim_status typos via ref_claim_status (broadcast join).
    # TODO 3: parse claim_date / loss_date.
    # TODO 4: join silver.policies; flag/quarantine loss_amount > sum_insured and orphan policy_id.
    # TODO 5: write silver.claims + silver.claims_quarantine; record counts for the DQ scorecard.
    raise NotImplementedError("Implement claims cleaning — see studybook M3.")


if __name__ == "__main__":
    build()
