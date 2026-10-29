"""TODO STUB — Lakeflow Spark Declarative Pipeline (Python) for the insurance medallion.

Re-expresses bronze → silver → gold as ONE declarative pipeline with **expectations**.
Deployed by the bundle resource `resources/insurance_pipeline.pipeline.yml` (serverless).
(Lakeflow Spark Declarative Pipelines = formerly Delta Live Tables / DLT.)

Use streaming tables for incremental bronze/silver and materialized views for gold
aggregates. Exam domains 2 & 3. Studybook: `docs/studybook/M5_declarative_pipelines.md`.
"""
import dlt
from pyspark.sql import functions as F  # noqa: F401
from src.common import config
from pyspark.sql import Window

LANDING = "/Volumes/insurance/landing/raw"


# ----------------------------- SILVER (clean + expectations) ------------------------- #


# Databricks handles the table creation, writing data into the table,
# checkpoints for streaming, warn drop fail 
# Manually manage: cleaning data, data quality rules, dedup, quarantine

# TODO: silver_policies (+ explode coverages), silver_claims (normalize fraud_flag),
#       silver_agents (SCD2 — apply_changes), with expectations per the data dictionary.


# CUSTOMER

@dlt.view(name="prepare_silver_customer", comment="Cleans customer data and assigns validation reasons before splitting valid and quarantined records")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("has_email", "email IS NOT NULL")
def prepare_silver_customers():
    customers = dlt.read_stream("bronze_customers") # a streaming data frame - it only processes the new data using the checkpoints
    states = dlt.read(config.table("bronze", "ref_us_states"))

    # Cleaning
    clean = (
        customers
            .withColumn("city", F.initcap(F.trim("city"))) 
            .withColumn("email", F.lower(F.trim("email")))
            .withColumn("phone", F.lower(F.trim("phone")))
            .withColumn("state_raw", F.upper(F.trim("state")))
            .withColumn("updated_at", F.col("updated_at").try_cast("timestamp"))
    )

    # Updated at 
    invalid_updated_at = F.col("updated_at").isNull()

    # State
    states = states.select("state_code")
    
    clean = (clean.join(F.broadcast(states), # uses the broadcast method for the small table (the result is a table with the columns from both sides)
                        clean.state_raw == states.state_code, "left") # uses a left join - keeps the rows from clean
                    .withColumn("state", F.col("state_code")))
    
    invalid_state = F.col("state_code").isNull()


    # Dates
    clean = (
        clean
        .withColumn("date_of_birth", 
            F.coalesce( 
                    F.try_to_date("date_of_birth", "yyyy-MM-dd"),
                    F.try_to_date("date_of_birth", "MM/dd/yyyy"),
                    F.try_to_date("date_of_birth", "dd-MMM-yyyy"))
            )
            .withColumn("dob_future_flag", F.col("date_of_birth") > F.current_date()) # creates a new boolean column, if the date is in the future
            .withColumn("date_of_birth", F.when( # if
                F.col("dob_future_flag"), # this is true
                F.lit(None).cast("date") # column becomes null
            ).otherwise(F.col("date_of_birth"))) # else keep the value from date_of_birth
            .drop("dob_future_flag")
    ) 

    invalid_dob = F.col("date_of_birth").isNull()


    # Deduplication
    # clean = clean.dropDuplicates() # dedupes all the rows, based on each of the columns

    # w = (Window.partitionBy("customer_id") # treat all rows with the same customer ID as one group
    #         .orderBy(F.col("updated_at").desc_nulls_last())) # inside each group, put the newest row first based on that updated_at

    # clean = (clean
    #             .withColumn("_rn", F.row_number().over(w)) # adds the _rn column that represents the row number over the window
    #             .where("_rn = 1") # where the rows have the _rn column 1 (like it keeps only the rows that have that column 1.)
    #             .drop("_rn")) # deletes the column _rn


    # Contact
    clean = (
        clean
        .withColumn("_dq_reason",
            F.when(
                invalid_updated_at,
                F.lit("invalid_updated_at")
            )
            .when(
                invalid_dob,
                F.lit("invalid_date_of_birth")
            )
            .when(
                invalid_state,
                F.lit("invalid_state")
            )
            .when(
                F.col("email").isNull() & F.col("phone").isNull(), 
                F.lit("missing_phone_email")
            )
            .when(
                F.col("email").isNull(), 
                F.lit("missing_email")
            ) # if the column is null insert in the _dq_reason the string with explanation
            .when(
                F.col("phone").isNull(), 
                F.lit("missing_phone")
            )
            .otherwise(F.lit(None).cast("string")))
    )

    clean = clean.drop("state_code", "state_raw")

    return clean

# Quarantine for customers
@dlt.table(name="silver_customers_quarantine")
def silver_customers_quarantine():
    return (
        dlt.read_stream("prepare_silver_customer")
        .where(F.col("_dq_reason").isNotNull())
    )

# View for the clean customers - keeps the multiple possible versions of rows with the same customer_id
# and drops the rows with the dq column not null
@dlt.view(name="valid_silver_customers")
def valid_silver_customers():
    return (
        dlt.read_stream("prepare_silver_customer")
        .where(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
    )

# Creates the streaming table for the clean customers - no data in it
dlt.create_streaming_table(name="silver_customers")

# Add the changes in the streaming table
dlt.apply_changes(
    target="silver_customers", # apply the changes in the silver_customers table
    source="valid_silver_customers", # take the new data from the valid_... talbe
    keys=["customer_id"], # you identify a customer after that column. A new row with an existing Id is treated as an updated of the same customer
    sequence_by=F.col("updated_at"), # Use the value from the updated_at column to determine the order of the versions of the rows
    stored_as_scd_type=1 # this expresses the use of SCD Type 1: When a client changes, update the existing row, don't keep the older versions in the table
)

####################################################################################

# POLICIES

@dlt.view(name="prepare_silver_policies", comment="Cleans policies and assigns validation reasons")
def prepare_silver_policies():
    policies = dlt.read_stream("bronze_policies")
    customers = dlt.read("silver_customers")

    premium_text = F.regexp_replace(
        F.trim(F.col("annual_premium").cast("string")),
        r"^\$", ""
    )

    plain_number = r"^[0-9]+(?:\.[0-9]+)?$"
    thousands_number = r"^[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?$"
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

    policies = (
        policies
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
        .withColumn("status", F.upper(F.trim("status")))
    )

    invalid_annual_premium = (
        (F.col("premium_valid") == False)
        | F.col("annual_premium").isNull()
    )

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

    invalid_status = (
        F.col("status").isNull()
        | (
            (F.col("status") != "ACTIVE")
            & (F.col("status") != "CANCELLED")
            & (F.col("status") != "LAPSED")
            & (F.col("status") != "PENDING")
        )
    )


    # Customer ID 
    customer_keys = (
        customers
        .select("customer_id")
        .distinct()
        .withColumn("_customer_exists", F.lit(True))
    )

    policies = policies.join(
        customer_keys,
        on="customer_id",
        how="left"
    )

    invalid_customer_id = F.col("_customer_exists").isNull()


    # Updating the table with dq
    clean = (
        policies
        .withColumn("_dq_reason",
            F.when(
                invalid_annual_premium,
                F.lit("invalid_annual_premium")
            )
            .when(
                F.col("effective_date").isNull(),
                F.lit("invalid_or_future_effective_date")
            )
            .when(
                F.col("expiration_date").isNull(),
                F.lit("invalid_expiration_date")
            )
            .when(
                (F.col("effective_date") > F.col("expiration_date")),
                F.lit("expiration_before_effective_date")
            )
            .when(
                invalid_status,
                F.lit("invalid_status")
            )
            .when(
                invalid_customer_id,
                F.lit("invalid_customer_id")
            )
            .otherwise(F.lit(None).cast("string")))
    )

    clean = clean.drop("premium_valid", "_customer_exists")

    return clean
