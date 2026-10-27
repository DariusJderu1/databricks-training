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

# TODO: add bronze_telematics (json), bronze_policies (json, multiLine), bronze_claims,
#       bronze_payments — each as a streaming table from cloudFiles.

# the name specifies the name of the table databricks will create.
# Databricks automatically creates and manages the checkpoints for the schemaEvolution and already processed files internally - I don't have access to a checkpoint folder
# The trigger(availableNow=True) is replaced by the pipeline processing logic
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
