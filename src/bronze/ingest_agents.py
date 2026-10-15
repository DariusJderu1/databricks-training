from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401  (you'll use F for the metadata cols)

from src.common import config

SOURCE = "agents"


def ingest(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")

    src_path = config.volume_path(SOURCE)              # /Volumes/insurance/landing/raw/customers
    checkpoint = config.checkpoint_path(f"bronze_{SOURCE}")
    target = config.table("bronze", SOURCE)            # insurance.bronze.customers

    stream = (spark.readStream.format("cloudFiles") # TODO 1: build the Auto Loader readStream
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(src_path)
        .selectExpr("*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingest_ts"))  # TODO 2: add ingestion metadata
      
    # TODO 3: write the stream (batch-incremental is budget-friendly on Free Edition)
    (stream.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target))
    
    # raise NotImplementedError("Implement the customers Auto Loader ingestion — see studybook M2.")


if __name__ == "__main__":
    ingest()
