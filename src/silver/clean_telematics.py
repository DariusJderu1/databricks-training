from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from src.common import config

def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    telematics = spark.readStream.table(config.table("bronze", "telematics"))

    # Telematics — watermark + dedupe late/out-of-order events into silver.telematics.

    # value casting
    telematics = (
        telematics
            .withColumn("event_ts", F.expr("try_cast(event_ts AS TIMESTAMP)"))
            .withColumn("latitude", F.expr("try_cast(latitude AS DOUBLE)"))
            .withColumn("longitude", F.expr("try_cast(longitude AS DOUBLE)"))
            .withColumn("speed_kmh", F.expr("try_cast(speed_kmh AS DOUBLE)"))
    )

    # wrong columns conditions
    has_nulls = (
        F.col("event_id").isNull()
        | F.col("event_ts").isNull()
        | F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | F.col("speed_kmh").isNull()
    )

    invalid_latitude = (
        (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
    )

    invalid_longitude = (
        (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
    )

    invalid_speed = (
        (F.col("speed_kmh") < 0)
        | (F.col("speed_kmh") > 300)
    )

    # final chain of conditions
    invalid = has_nulls | invalid_latitude | invalid_longitude | invalid_speed

    # the clean data frame
    clean = telematics.where(~invalid)

    quarantine = (
        telematics
        .where(invalid)
        .withColumn(
            "_dq_reason",
            # the null columns
            F.when(
                F.col("event_id").isNull(),
                F.lit("missing_event_id")
            )
            .when(
                F.col("event_ts").isNull(),
                F.lit("missing_or_invalid_event_ts"),
            )
            .when(
                F.col("latitude").isNull(),
                F.lit("missing_latitude"),
            )
            .when(
                F.col("longitude").isNull(),
                F.lit("missing_longitude"),
            )
            .when(
                F.col("speed_kmh").isNull(),
                F.lit("missing_speed"),
            )
            # Out of bounds
            .when(
                ~F.col("latitude").between(-90, 90),
                F.lit("latitude_out_of_bounds"),
            )
            .when(
                ~F.col("longitude").between(-180, 180),
                F.lit("longitude_out_of_bounds"),
            )
            .when(
                ~F.col("speed_kmh").between(0, 300),
                F.lit("speed_out_of_bounds"),
            )
        )
    )

    (clean
        .withWatermark("event_ts", "2 hours")
        .dropDuplicates(["event_id", "event_ts"])
        .writeStream
        .option("checkpointLocation", config.checkpoint_path("silver_telematics"))
        .trigger(availableNow=True)
        .toTable(config.table("silver", "telematics")))

    (quarantine
        .writeStream
        .option("checkpointLocation", config.checkpoint_path("silver_telematics_quarantine"))
        .trigger(availableNow=True)
        .toTable(config.table("silver", "telematics_quarantine")))
    