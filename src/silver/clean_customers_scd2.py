"""WORKED EXAMPLE — Silver: build a Type-2 slowly-changing dimension for agents.

Demonstrates the canonical Databricks **SCD2 MERGE** (the "mergeKey = NULL" trick):
a single MERGE both *expires* the previous current row and *inserts* a new version
whenever a tracked attribute changes. The delta loads land agent *updates* (same
``agent_id``, newer ``updated_at``) into bronze; this job folds them into a full
versioned history with ``effective_from`` / ``effective_to`` / ``is_current``.

Copy this pattern for the other SCD2 dimension you build in M3 (``dim_customer``).

Exam domain 3 (Data Transformation and Modeling): cleaning, dedup, MERGE / SCD2.

    from src.silver.clean_agents_scd2 import upsert
    upsert(spark)
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common import config

# Tracked attributes — a change in any of these opens a new SCD2 version.
TRACKED = ["first_name", "last_name", "email", "city", "state", "segment"]

def upsert(spark=None) -> None:
    spark = spark or config.get_spark()
    target = config.table("silver", "dim_customer")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    # reads the already cleaned customers
    src = (
        spark.read.table(config.table("silver", "customers"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )

    # calculates the hash usinig the TRACKED
    hash_input = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("<null>"))
            for c in TRACKED
        ]
    )

    src = src.withColumn("_hash", F.sha2(hash_input, 256))

    # all the columns, including hash
    insert_cols = src.columns

    # First run: seed every current row as version 1.
    if not spark.catalog.tableExists(target):
        (
            src.withColumn("effective_from", F.col("updated_at"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta")
            .saveAsTable(target)
        )
        print(f"seeded {src.count():,} agents -> {target}")
        return

    tgt = DeltaTable.forName(spark, target)

    # Existing keys whose tracked attributes changed (inner join => existing only).
    current = (
        spark.read.table(target)
        .where("is_current = true")
        .select("customer_id", "_hash")
    )

    changed_existing = (
        src.alias("s")
        .join(current.alias("c"), "customer_id", "inner")
        .where("s._hash <> c._hash")
        .select("s.*")
    )

    # Two staged copies per changed row:
    #   • mergeKey = NULL   -> never matches -> INSERTS the new current version
    #   • mergeKey = agent_id -> matches the current row -> EXPIRES it
    # Brand-new agents appear only in the keyed copy -> single INSERT.
    staged = (
        changed_existing
        .selectExpr("CAST(NULL AS STRING) AS mergeKey", "*")
        .unionByName(
            src.selectExpr("customer_id AS mergeKey", "*")
        )
    )

    (
        tgt.alias("t")
        .merge(
            staged.alias("s"),
            "t.customer_id = s.mergeKey AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t._hash <> s._hash",
            set={"is_current": F.lit(False), "effective_to": F.col("s.updated_at")},
        )
        .whenNotMatchedInsert(
            values={
                **{c: F.col(f"s.{c}") for c in insert_cols},
                "effective_from": F.col("s.updated_at"),
                "effective_to": F.lit(None).cast("timestamp"),
                "is_current": F.lit(True),
            }
        )
        .execute()
    )
    print(f"merged SCD2 updates into {target}")


if __name__ == "__main__":
    upsert()
