from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from pyspark.sql import Window

from src.common import config


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    bronze = spark.read.table(config.table("bronze", "payments"))

    policies = (
        spark.read.table(config.table("silver", "policies"))
        .select("policy_id")
    )

    # policy_id

    # quarantine the payments with a missing or unmatched policy_id
    policy_id_quarantine = (
        bronze.join(policies, on="policy_id", how="left_anti")
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("policy_id").isNull(),
                F.lit("missing_policy_id")
            ).otherwise(F.lit("orphan_policy_id"))
        )
    )

    clean = bronze.join(policies, on="policy_id", how="left_semi")


    # Dates
    clean = (
        clean
        .withColumn("_paid_date_present", F.col("paid_date").isNotNull())
        .withColumn("due_date", F.expr("try_cast(due_date AS DATE)"))
        .withColumn("paid_date", F.expr("try_cast(paid_date AS DATE)"))
    )

    invalid_dates = (
        F.col("due_date").isNull() # the due_date is null after conversion or
        | ( # the paid_date is present but after the cast it got null 
            F.col("_paid_date_present")
            & F.col("paid_date").isNull()
        )
        | F.coalesce( # using coalesce in case paid_date doesn't exist and the > returns null
            F.col("paid_date") > F.current_date(), 
            F.lit(False)
        )
    )

    date_quarantine = (
        clean.where(invalid_dates)
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("due_date").isNull(),
                F.lit("missing_or_invalid_due_date")
            )
            .when(
                F.col("paid_date").isNull(),
                F.lit("invalid_paid_date")
            )
            .otherwise(F.lit("future_paid_date"))
        )
        .drop("_paid_date_present")
    )

    clean = clean.where(~invalid_dates).drop("_paid_date_present")


    # Amount_paid
    invalid_amount_paid = (
        F.col("amount_paid").isNotNull()
        & (F.col("amount_paid") < 0)
    )

    amount_paid_quarantine = (
        clean.where(invalid_amount_paid)
        .withColumn("_dq_reason", F.lit("negative_amount_paid"))
    )

    clean = clean.where(~invalid_amount_paid)


    # Amount_Due
    invalid_amount_due = (
        F.col("amount_due").isNull()
        | (F.col("amount_due") < 0)
    )

    amount_due_quarantine = (
        clean.where(invalid_amount_due)
        .withColumn(
            "_dq_reason",
            F.lit("missing_or_negative_amount_due")
        )
    )

    clean = clean.where(~invalid_amount_due)


    # Payment method
    clean = clean.withColumn(
        "payment_method",
        F.upper(F.trim("payment_method"))
    )

    invalid_payment_method = (
        F.col("payment_method").isNull()
        | ~F.col("payment_method").isin("ACH", "CARD", "CHECK", "CASH")
    )

    payment_method_quarantine = (
        clean.where(invalid_payment_method)
        .withColumn("_dq_reason", F.lit("invalid_payment_method"))
    )

    clean = clean.where(~invalid_payment_method)


    # Status
    clean = clean.withColumn("status", F.upper(F.trim("status")))

    paid_status = F.col("status").isin("PAID", "LATE")
    unpaid_status = F.col("status").isin("MISSED", "PENDING")

    invalid_status = (
        F.col("status").isNull()
        | ~F.col("status").isin("PAID", "LATE", "MISSED", "PENDING")
        | (
            paid_status
            & (
                F.col("paid_date").isNull()
                | F.col("amount_paid").isNull()
            )
        )
        | (
            unpaid_status
            & (
                F.col("paid_date").isNotNull()
                | F.col("amount_paid").isNotNull()
            )
        )
        | (
            (F.col("status") == "PAID")
            & (F.col("paid_date") > F.col("due_date"))
        )
        | (
            (F.col("status") == "LATE")
            & (F.col("paid_date") <= F.col("due_date"))
        )
    )

    status_quarantine = (
        clean.where(invalid_status)
        .withColumn("_dq_reason", F.lit("invalid_or_inconsistent_status"))
    )

    clean = clean.where(~invalid_status)


    # Adding the tables
    quarantine = (
        policy_id_quarantine
        .unionByName(date_quarantine, allowMissingColumns=True)
        .unionByName(amount_paid_quarantine, allowMissingColumns=True)
        .unionByName(amount_due_quarantine, allowMissingColumns=True)
        .unionByName(payment_method_quarantine, allowMissingColumns=True)
        .unionByName(status_quarantine, allowMissingColumns=True)
    )

    (clean.write.mode("overwrite")
        .saveAsTable(config.table("silver", "payments")))

    (quarantine.write.mode("overwrite")
        .saveAsTable(config.table("silver", "payments_quarantine")))


    # Record counts for the DQ scorecard.
    passed_counts = clean.agg(F.count("*").alias("passed"))
    quarantine_counts = quarantine.agg(F.count("*").alias("quarantined"))

    scorecard = (
        passed_counts
        .crossJoin(quarantine_counts)
        .withColumn("table_name", F.lit(config.table("silver", "payments")))
        .withColumn("run_ts", F.current_timestamp())
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('ops')}"
    )

    (scorecard.write.mode("append")
        .saveAsTable(config.table("ops", "dq_scorecard")))

if __name__ == "__main__":
    build()
