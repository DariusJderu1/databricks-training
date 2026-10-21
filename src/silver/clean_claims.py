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

    claims = spark.read.table(config.table("bronze", "claims")).dropDuplicates(["claim_id"])
    policies = spark.read.table(config.table("silver", "policies"))   # for orphan + sum_insured checks
    status = spark.read.table(config.table("bronze", "ref_claim_status")) 

    # TODO 1: normalize fraud_flag -> boolean
    #   F.lower(F.trim("fraud_flag")).isin(TRUE_TOKENS)
    # TODO 2: map claim_status typos via ref_claim_status (broadcast join).
    # TODO 3: parse claim_date / loss_date.
    # TODO 4: join silver.policies; flag/quarantine loss_amount > sum_insured and orphan policy_id.
    # TODO 5: write silver.claims + silver.claims_quarantine; record counts for the DQ scorecard.
    # raise NotImplementedError("Implement claims cleaning — see studybook M3.")

    # TODO 1: normalize fraud_flag -> boolean + some castings/trimmings for the future
    claims = (
        claims
        .withColumn("fraud_flag", F.when(
                                    F.lower(F.trim("fraud_flag")).isin(TRUE_TOKENS), 
                                    F.lit(True))
                                  .otherwise(F.lit(False)))
    )

    claims = (
        claims
        .withColumn("loss_amount",
            F.regexp_replace(F.col("loss_amount"), r"[$,]", "").cast("double"))
    )

    claims = (
        claims
        .withColumn("claim_date", F.coalesce(
            F.try_to_date("claim_date", "yyyy-MM-dd"),
            F.try_to_date("claim_date", "MM/dd/yyyy"),
            F.try_to_date("claim_date", "dd-MMM-yyyy")
        ))
        .withColumn("claim_date_future_flag",
                    F.col("claim_date") > F.current_date())
        .withColumn("claim_date", F.when(
            F.col("claim_date_future_flag"),
            F.lit(None).cast("date")
        ).otherwise(F.col("claim_date")))
        .drop("claim_date_future_flag")
    )

    claims = (
        claims
        .withColumn("loss_date", F.coalesce(
            F.try_to_date("loss_date", "yyyy-MM-dd"),
            F.try_to_date("loss_date", "MM/dd/yyyy"),
            F.try_to_date("loss_date", "dd-MMM-yyyy")
        ))
        .withColumn("loss_date_future_flag",
                    F.col("loss_date") > F.current_date())
        .withColumn("loss_date", F.when(
            F.col("loss_date_future_flag"),
            F.lit(None).cast("date")
        ).otherwise(F.col("loss_date")))
        .drop("loss_date_future_flag")
    )


    # TODO 2: map claim_status typos via ref_claim_status (broadcast join).
    status = status.select("status_code")

    claims = (
        claims
        .withColumn("claim_status", F.trim("claim_status"))
    )
    
    claims = (
        claims
        .join(F.broadcast(status), # uses the broadcast method for the small table (the result is a table with the columns from both sides)
            claims.claim_status == status.status_code, "left") # uses a left join 
        .withColumn("claim_status", F.col("status_code"))
    )
    
    # this is the condition based on what I will check the rows that are invalid
    invalid_status =  F.col("status_code").isNull()

    status_quarantine = (
        claims.where(invalid_status)
        .withColumn("_dq_reason", F.lit("invalid_claim_status"))
        .drop("status_code")
    )

    claims = claims.where(~invalid_status).drop("status_code")


    # TODO 3: parse claim_date / loss_date.

    invalid_claim_date = F.col("claim_date").isNull()
    invalid_loss_date = F.col("loss_date").isNull()

    invalid_dates = invalid_claim_date | invalid_loss_date

    date_quarantine = (
        claims
        .where(invalid_dates)
        .withColumn("_dq_reason", F.lit("invalid_claim_or_loss_date"))
    )

    claims = claims.where(~invalid_dates)


    # TODO 4: join silver.policies; flag/quarantine loss_amount > sum_insured and orphan policy_id.

    # the quarantine table for the rows that have no valid policy_id
    policy_id_quarantine = (
        claims.join(
            policies,
            claims.policy_id == policies.policy_id,
            "left_anti" 
        )
        .withColumn("_dq_reason", F.lit("orphan_policy_id"))
    )

    # clean the table and keeps the sum_insured, not 2 policy ids
    claims = claims.join(
        policies.select("policy_id", "sum_insured"),
        on="policy_id",
        how="inner"
    )

    # loss_amount > sum_insured

    claims = (
        claims
        .withColumn("sum_insured", F.regexp_replace(F.col("sum_insured").cast("string"), r"[$,]", "").cast("double"))
    )

    invalid_amount = (
        F.col("loss_amount").isNull()
        | F.col("sum_insured").isNull()
        | (F.col("loss_amount") > F.col("sum_insured"))
    )

    amount_quarantine = (
        claims.where(invalid_amount)
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("loss_amount").isNull()
                | F.col("sum_insured").isNull(),
                F.lit("missing_or_invalid_amount")
            )
            .otherwise(F.lit("loss_exceeds_sum_insured"))
        )
    )

    claims = claims.where(~invalid_amount).drop("sum_insured")


    # TODO 5: write silver.claims + silver.claims_quarantine; record counts for the DQ scorecard.

    quarantine = (
        status_quarantine
        .unionByName(date_quarantine, allowMissingColumns=True)
        .unionByName(policy_id_quarantine, allowMissingColumns=True)
        .unionByName(amount_quarantine, allowMissingColumns=True)
    )

    (claims.write.mode("overwrite")
        .saveAsTable(config.table("silver", "claims")))

    (quarantine.write.mode("overwrite")
        .saveAsTable(config.table("silver", "claims_quarantine")))


    # record counts for the DQ scorecard.
    # creates a df with the alias passed containing the number of rows from claims
    passed_counts = claims.agg(F.count("*").alias("passed"))

    # same as above for quarantine
    quarantine_counts = quarantine.agg(F.count("*").alias("quarantined"))

    scorecard = (
        passed_counts
        .crossJoin(quarantine_counts)
        .withColumn("table_name", F.lit(config.table("silver", "claims")))
        .withColumn("run_ts", F.current_timestamp())
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('ops')}"
    )

    (scorecard.write.mode("append")
        .saveAsTable(config.table("ops", "dq_scorecard")))

if __name__ == "__main__":
    build()
