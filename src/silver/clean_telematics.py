from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from pyspark.sql import Window

from src.common import config

def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    telematics = spark.read.table(config.table("bronze", "telematics"))

    # Telematics — watermark + dedupe late/out-of-order events into silver.telematics.

    (spark.readStream.table(config.table("bronze", "telematics"))
        # F.expr() - execute the sql expression from the string
        .withColumn("event_ts", F.expr("try_cast(event_ts AS TIMESTAMP)"))
        .withWatermark("event_ts", "2 hours") # use event_ts as a landmark and consider that data may arive up to 2 hours late
        .dropDuplicates(["event_id", "event_ts"]) # dedupe after the event_id & ts column                           
        .writeStream.option("checkpointLocation", config.checkpoint_path("silver_telematics"))
        .trigger(availableNow=True)
        .toTable(config.table("silver", "telematics")))