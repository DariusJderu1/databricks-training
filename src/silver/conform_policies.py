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

from pyspark.sql import Window

from src.common import config


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    policies = spark.read.table(config.table("bronze", "policies"))
    customers = spark.read.table(config.table("silver", "customers"))  # for the orphan anti-join

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

    # trims the annual_premium and removes the dollar sign at the beginning
    premium_text = F.regexp_replace(
        F.trim(F.col("annual_premium").cast("string")),
        r"^\$", ""
    )

    # Matches a standard integer or a standard decimal number using the dot as the decimal separator
    plain_number = r"^[0-9]+(?:\.[0-9]+)?$"
    # Matches numbers formatted in a style that has comma separators for thousands, with an optional decimal point
    thousands_number = r"^[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?$"
    # Matches the numbers that uses a comma for the decimal separator, strictly limited to 1 or 2 decimal places
    decimal_comma = r"^[0-9]+,[0-9]{1,2}$"

    policies = (
        policies
        .withColumn(
            "annual_premium",
            F.when(
                premium_text.rlike(decimal_comma),
                F.regexp_replace(premium_text, ",", ".")
            )
            .when(
                premium_text.rlike(plain_number)
                | premium_text.rlike(thousands_number),
                F.regexp_replace(premium_text, ",", "")
            )
            .otherwise(F.lit(None))
            .cast("double")
        )
        .withColumn("premium_valid", F.col("annual_premium") > 0)
    )

    policies = (policies
        # TODO 3: parse effective_date / expiration_date.
        .withColumn("effective_date", F.coalesce(
                                        F.try_to_date("effective_date", "yyyy-MM-dd"),
                                        F.try_to_date("effective_date", "MM/dd/yyyy"),
                                        F.try_to_date("effective_date", "dd-MMM-yyyy")))
            .withColumn("effective_date_flag", F.col("effective_date") > F.current_date()) # creates a new boolean column, if the date is in the future
                .withColumn("effective_date", F.when( # if
                                                F.col("effective_date_flag"), # this is true
                                                F.lit(None).cast("date") # column becomes null
                                            ).otherwise(F.col("effective_date"))) # else keep the value
                .drop("effective_date_flag") 
        .withColumn("expiration_date", F.coalesce(
                                        F.try_to_date("expiration_date", "yyyy-MM-dd"),
                                        F.try_to_date("expiration_date", "MM/dd/yyyy"),
                                        F.try_to_date("expiration_date", "dd-MMM-yyyy")))
        # for the TODO 2 trimming & uppercase
        .withColumn("status", F.upper(F.trim("status"))))


    # Dealing with the annual_premium
    invalid_annual_premium = (
        (F.col("premium_valid") == False)
        | F.col("annual_premium").isNull()
    )

    annual_premium_quarantine = (
        policies.where(invalid_annual_premium)
        .withColumn("_dq_reason", F.lit("invalid_annual_premium"))
    )

    policies = policies.where(~invalid_annual_premium).drop("premium_valid")

    # TODO 3
    invalid_dates = (
        F.col("effective_date").isNull()
        | F.col("expiration_date").isNull()
        | (F.col("effective_date") > F.col("expiration_date"))
    )

    effective_date_quarantine = (
        policies.where(invalid_dates)
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("effective_date").isNull(),
                F.lit("invalid_or_future_effective_date")
            )
            .when(
                F.col("expiration_date").isNull(),
                F.lit("invalid_expiration_date")
            )
            .otherwise(F.lit("expiration_before_effective_date"))
        )
    )

    policies = policies.where(~invalid_dates)
    
    # TODO 2: map status typos to canonical values
    policies = (
        policies
        .withColumn(
            "status",
            F.when(F.col("status") == "LAPSE", "LAPSED")
                .when(F.col("status").isin("PENING", "PEND"), "PENDING")
                .when(F.col("status").isin("ACTV", "ACTIVEE"), "ACTIVE")
                .otherwise(F.col("status"))
        )
    )

    # the invalid status condition
    invalid_status = (
        F.col("status").isNull()
        | (
            (F.col("status") != "ACTIVE")
            & (F.col("status") != "CANCELLED")
            & (F.col("status") != "LAPSED")
            & (F.col("status") != "PENDING")
        )
    )

    # the quarantine status
    status_quarantine = (
        policies.where(invalid_status)
        .withColumn("_dq_reason", F.lit("invalid_status"))
    )

    # clean policies without that invalid status
    policies = policies.where(~invalid_status)

    # TODO 4: anti-join to silver.customers to find orphan customer_id -> quarantine.

    # the quarantine table for the rows that have no valid customer_id
    customer_id_quarantine = (
        policies.join(
            customers,
            policies.customer_id == customers.customer_id,
            "left_anti" # keep the rows from the left df (policies) for which there is no match on the right df
        )
        .withColumn("_dq_reason", F.lit("invalid_customer_id"))
    )

    # clean the table
    policies = policies.join(
        customers,
        policies.customer_id == customers.customer_id,
        "left_semi" # keep the rows from the left df policies for which there is a match on the right side - customers
    )

    # TODO 5: write silver.policies (one row per policy, drop the coverages array).

    # Dedupe - one row per policy
    policies = policies.dropDuplicates() # dedupes all the rows, based on each of the columns
    
    w = (Window.partitionBy("policy_id") 
            .orderBy(F.col("_ingest_ts").desc_nulls_last())) 

    # fuzzy
    policies = (
        policies
            .withColumn("_rn", F.row_number().over(w)) # adds the _rn column that represents the row number over the window
            .where("_rn = 1") # where the rows have the _rn column 1 (like it keeps only the rows that have that column 1.)
            .drop("_rn") # deletes the column _rn
    ) 

    coverages = policies

    policies = policies.drop("coverages")

    (policies.write.mode("overwrite")  
            .saveAsTable(config.table("silver", "policies")))

    # TODO 6: explode coverages -> silver.policy_coverages
    coverages = (
        coverages
        .select(
            "policy_id",
            "product_line",
            F.explode("coverages").alias("coverage")
        )
        .select(
            "policy_id",
            "product_line",
            "coverage.coverage_code",
            "coverage.limit",
            "coverage.deductible",
            "coverage.peril_code"
        )
    )

    (coverages.write.mode("overwrite")  
                .saveAsTable(config.table("silver", "policy_coverages")))


    # Union the quarantine tables and save it
    quarantine = (
        status_quarantine
        .unionByName(customer_id_quarantine, allowMissingColumns=True)
        .unionByName(effective_date_quarantine, allowMissingColumns=True)
        .unionByName(annual_premium_quarantine, allowMissingColumns=True)
    )

    (quarantine.write.mode("overwrite")
            .saveAsTable(config.table("silver", "policies_quarantine")))


    # Record counts for the DQ scorecard.
    passed_counts = policies.agg(F.count("*").alias("passed"))
    quarantine_counts = quarantine.agg(F.count("*").alias("quarantined"))

    scorecard = (
        passed_counts
        .crossJoin(quarantine_counts)
        .withColumn("table_name", F.lit(config.table("silver", "policies")))
        .withColumn("run_ts", F.current_timestamp())
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('ops')}"
    )

    (scorecard.write.mode("append")
        .saveAsTable(config.table("ops", "dq_scorecard")))

if __name__ == "__main__":
    build()
