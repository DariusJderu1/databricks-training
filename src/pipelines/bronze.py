"""TODO STUB — Lakeflow Spark Declarative Pipeline (Python) for the insurance medallion.

Re-expresses bronze → silver → gold as ONE declarative pipeline with **expectations**.
Deployed by the bundle resource `resources/insurance_pipeline.pipeline.yml` (serverless).
(Lakeflow Spark Declarative Pipelines = formerly Delta Live Tables / DLT.)

Use streaming tables for incremental bronze/silver and materialized views for gold
aggregates. Exam domains 2 & 3. Studybook: `docs/studybook/M5_declarative_pipelines.md`.
"""
import dlt
from pyspark.sql import functions as F  # noqa: F401

LANDING = "/Volumes/insurance/landing/raw"


# ----------------------------- BRONZE (streaming ingest) ----------------------------- #

# Notes to myself
# the name specifies the name of the table databricks will create. - the spark.readStream returns a Streaming DataFrame. A streaming table is a delta table with managed streaming processing and checkpoint state. Designed for incremental streaming processing like for bronze ingestion. SQL equivalent is STREAMING TABLE (like at the second run it will continue after the checkpoint with the processing)
# Databricks automatically creates and manages the checkpoints for the schemaEvolution and already processed files internally - I don't have access to a checkpoint folder
# The trigger(availableNow=True) is replaced by the pipeline processing logic


# TODO: add bronze_telematics (json), bronze_policies (json, multiLine), bronze_claims,
#       bronze_payments — each as a streaming table from cloudFiles.

@dlt.table(name="bronze_customers", comment="Raw customers via Auto Loader")
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 (spark is provided by the pipeline)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # provided automatically - added for clarity
            .option("rescuedDataColumn", "_rescued_data") # provided automatically - added for clarity
            .load(f"{LANDING}/customers")
            .selectExpr("*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingest_ts")
    )


@dlt.table(name="bronze_telematics", comment="Raw per-trip IoT events")
def bronze_telematics():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # device_fw drift
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{LANDING}/telematics")
            .selectExpr("*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingest_ts")
    )


@dlt.table(name="bronze_claims", comment="Raw claims via Auto Loader")
def bronze_claims():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING}/claims")
        .selectExpr(
            "*",
            "_metadata.file_path AS _source_file",
            "current_timestamp() AS _ingest_ts",
        )
    )


@dlt.table(name="bronze_policies", comment="Raw nested policies via Auto Loader")
def bronze_policies():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING}/policies")
        .selectExpr(
            "*",
            "_metadata.file_path AS _source_file",
            "current_timestamp() AS _ingest_ts",
        )
    )


@dlt.table(name="bronze_payments", comment="Raw payments via Auto Loader")
def bronze_payments():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING}/payments")
        .selectExpr(
            "*",
            "_metadata.file_path AS _source_file",
            "current_timestamp() AS _ingest_ts",
        )
    )


@dlt.table(name="bronze_agents", comment="Raw agents via Auto Loader")
def bronze_agents():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING}/agents")
        .selectExpr(
            "*",
            "_metadata.file_path AS _source_file",
            "current_timestamp() AS _ingest_ts",
        )
    )
