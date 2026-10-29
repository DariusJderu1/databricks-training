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

# Notes to muself
# Databricks handles the table creation, writing data into the table,
# checkpoints for streaming, warn drop fail 
# Manually manage: cleaning data, data quality rules, dedup, quarantine

# dlt.read_stream(...) treats the source as an event stream: gives new rows that have arrived since the last checkpoint
# dlt.read(...) gives the current contents of the dataset as a table (like all the data)

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


@dlt.table(name="silver_policies_quarantine", comment="Adds the impure data in quarantine table")
def silver_policies_quarantine():
    return (
        dlt.read_stream("prepare_silver_policies")
        .where(F.col("_dq_reason").isNotNull())
    )


@dlt.view(name="valid_silver_policies", comment="View for polices without the impure data")
def valid_silver_policies():
    return (
        dlt.read_stream("prepare_silver_policies")
        .where(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
    )


@dlt.view(name="valid_policy_records", comment="View for policies without coverages")
def valid_policy_records():
    return (
        dlt.read_stream("valid_silver_policies")
        .drop("coverages")
    )


# Creates the the final table for policies
dlt.create_streaming_table(name="silver_policies")


# Adds the data in the table from the view and applies SCD1
dlt.apply_changes(
    target="silver_policies",
    source="valid_policy_records",
    keys=["policy_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1
)


@dlt.view(name="valid_policy_coverages")
def valid_policy_coverages():
    return (
        dlt.read_stream("valid_silver_policies")
        .select(
            "policy_id",
            "product_line",
            "_ingest_ts",
            F.explode("coverages").alias("coverage")
        )
        .select(
            "policy_id",
            "product_line",
            "_ingest_ts",
            F.col("coverage.coverage_code").alias("coverage_code"),
            F.col("coverage.limit").alias("limit"),
            F.col("coverage.deductible").alias("deductible"),
            F.col("coverage.peril_code").alias("peril_code")
        )
    )


dlt.create_streaming_table(name="silver_policy_coverages")


dlt.apply_changes(
    target="silver_policy_coverages",
    source="valid_policy_coverages",
    keys=["policy_id", "coverage_code"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1
)


####################################################################################

# CLAIMS

@dlt.view(name="prepare_silver_claims", comment="Cleans claims and assigns validation reasons before splitting valid and quarantined records")
def prepare_silver_claims():
    TRUE_TOKENS = ["true", "y", "yes", "1"]

    claims = (
        dlt.read_stream("bronze_claims")
        .dropDuplicates(["claim_id"])
    )

    policies = dlt.read("silver_policies")
    status = dlt.read(config.table("bronze", "ref_claim_status"))

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
            F.regexp_replace(F.col("loss_amount"), r"[$,]", "").try_cast("double"))
        .withColumn("paid_amount",
                    F.regexp_replace(F.col("paid_amount"), r"[$,]", "").try_cast("double"))
    )

    claims = (
        claims
        .withColumn("claim_date", F.coalesce(
            F.try_to_date("claim_date", "yyyy-MM-dd"),
            F.try_to_date("claim_date", "MM/dd/yyyy"),
            F.try_to_date("claim_date", "dd-MMM-yyyy"),
            F.try_to_date("claim_date", "dd-MM-yyyy")
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
            F.try_to_date("loss_date", "dd-MMM-yyyy"),
            F.try_to_date("loss_date", "dd-MM-yyyy")
        ))
        .withColumn("loss_date_future_flag",
                    F.col("loss_date") > F.current_date())
        .withColumn("loss_date", F.when(
            F.col("loss_date_future_flag"),
            F.lit(None).cast("date")
        ).otherwise(F.col("loss_date")))
        .drop("loss_date_future_flag")
    )

    status = status.select("status_code")

    claims = (
        claims
        .withColumn(
            "claim_status",
            F.regexp_replace(
                F.upper(F.trim(F.col("claim_status"))),
                r"\s+",
                "_"
            )
        )
    )

    claims = (
        claims
        .withColumn(
            "claim_status",
            F.when(F.col("claim_status") == "OPN", "OPEN")
                .when(F.col("claim_status") == "CLOSE", "CLOSED")
                .when(F.col("claim_status") == "REVIEWING", "IN_REVIEW")
                .otherwise(F.col("claim_status"))
        )
    )

    claims = (
        claims
        .join(
            F.broadcast(status), # uses the broadcast method for the small table (the result is a table with the columns from both sides)
            claims.claim_status == status.status_code, 
            "left" # uses a left join 
        ) 
    )

    invalid_status =  F.col("status_code").isNull()

    invalid_claim_date = F.col("claim_date").isNull()
    invalid_loss_date = F.col("loss_date").isNull()
    invalid_dates = invalid_claim_date | invalid_loss_date

    policy_keys = (
        policies
        .select("policy_id", "sum_insured")
        .distinct()
        .withColumn("_policy_exists", F.lit(True))
    )

    claims = claims.join(
        policy_keys,
        on="policy_id",
        how="left"
    )

    invalid_policy_id = F.col("_policy_exists").isNull()

    invalid_amount = (
        F.col("loss_amount").isNull()
        | F.col("sum_insured").isNull()
        | (F.col("loss_amount") > F.col("sum_insured"))
    )


    clean = (
        claims
        .withColumn("_dq_reason",
            F.when(
                invalid_status,
                F.lit("invalid_claim_status")
            )
            .when(
                invalid_dates,
                F.lit("invalid_claim_or_loss_date")
            )
            .when(
                invalid_policy_id,
                F.lit("invalid_policy_id")
            )
            .when(
                invalid_amount,
                F.when(
                    F.col("loss_amount").isNull()
                    | F.col("sum_insured").isNull(),
                    F.lit("missing_or_invalid_amount")
                )
                .otherwise(F.lit("loss_exceeds_sum_insured"))
            )
            .otherwise(F.lit(None).cast("string")))
    )

    clean = clean.drop("sum_insured", "_policy_exists", "status_code")

    return clean
    

@dlt.table(name="silver_claims_quarantine")
def silver_claims_quarantine():
    return (
        dlt.read_stream("prepare_silver_claims")
        .where(F.col("_dq_reason").isNotNull())
    )


@dlt.view(name="valid_silver_claims")
def valid_silver_claims():
    return (
        dlt.read_stream("prepare_silver_claims")
        .where(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
    )


dlt.create_streaming_table(name="silver_claims")


# Add the changes in the streaming table
dlt.apply_changes(
    target="silver_claims",
    source="valid_silver_claims", 
    keys=["claim_id"], 
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1  
)

####################################################################################

# PAYMENTS

@dlt.view(name="prepare_silver_payments", comment="Cleans payments and assigns validation reasons")
def prepare_silver_payments():
    payments = dlt.read_stream("bronze_payments")

    policies = (
        dlt.read("silver_policies")
        .select("policy_id")
        .withColumn("_policy_exists", F.lit(True))
    )

    # Policy ID
    payments = payments.join(
        policies,
        on="policy_id",
        how="left"
    )

    invalid_policy_id = F.col("_policy_exists").isNull()


    # Dates
    payments = (
        payments
        .withColumn(
            "_paid_date_present",
            F.col("paid_date").isNotNull()
        )
        .withColumn(
            "due_date",
            F.expr("try_cast(due_date AS DATE)")
        )
        .withColumn(
            "paid_date",
            F.expr("try_cast(paid_date AS DATE)")
        )
    )

    invalid_due_date = F.col("due_date").isNull()

    invalid_paid_date = (
        F.col("_paid_date_present")
        & F.col("paid_date").isNull()
    )

    future_paid_date = F.coalesce(
        F.col("paid_date") > F.current_date(),
        F.lit(False)
    )

    invalid_dates = (
        invalid_due_date
        | invalid_paid_date
        | future_paid_date
    )


    # Amount paid
    invalid_amount_paid = (
        F.col("amount_paid").isNotNull()
        & (F.col("amount_paid") < 0)
    )


    # Amount due
    invalid_amount_due = (
        F.col("amount_due").isNull()
        | (F.col("amount_due") < 0)
    )


    # Payment method
    payments = payments.withColumn(
        "payment_method",
        F.upper(F.trim("payment_method"))
    )

    invalid_payment_method = (
        F.col("payment_method").isNull()
        | ~F.col("payment_method").isin("ACH", "CARD", "CHECK", "CASH")
    )


    # Status
    payments = payments.withColumn(
        "status",
        F.upper(F.trim("status"))
    )

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


    # Assign the first validation failure in the same order as clean_payments.py
    clean = (
        payments
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("policy_id").isNull(),
                F.lit("missing_policy_id")
            )
            .when(
                invalid_policy_id,
                F.lit("orphan_policy_id")
            )
            .when(
                invalid_dates,
                F.when(
                    invalid_due_date,
                    F.lit("missing_or_invalid_due_date")
                )
                .when(
                    invalid_paid_date,
                    F.lit("invalid_paid_date")
                )
                .otherwise(F.lit("future_paid_date"))
            )
            .when(
                invalid_amount_paid,
                F.lit("negative_amount_paid")
            )
            .when(
                invalid_amount_due,
                F.lit("missing_or_negative_amount_due")
            )
            .when(
                invalid_payment_method,
                F.lit("invalid_payment_method")
            )
            .when(
                invalid_status,
                F.lit("invalid_or_inconsistent_status")
            )
            .otherwise(F.lit(None).cast("string"))
        )
        .drop("_policy_exists", "_paid_date_present")
    )

    return clean


# Quarantine for payments
@dlt.table(name="silver_payments_quarantine")
def silver_payments_quarantine():
    return (
        dlt.read_stream("prepare_silver_payments")
        .where(F.col("_dq_reason").isNotNull())
    )


@dlt.table(name="silver_payments")
def silver_payments():
    return (
        dlt.read_stream("prepare_silver_payments")
        .where(F.col("_dq_reason").isNull())
        .drop("_dq_reason")
    )


####################################################################################

# TELEMATICS

@dlt.view(name="prepare_silver_telematics", comment="Cleans telematics and assigns validation reasons")
def prepare_silver_telematics():
    telematics = dlt.read_stream("bronze_telematics")


    # Value casting
    telematics = (
        telematics
        .withColumn(
            "event_ts",
            F.expr("try_cast(event_ts AS TIMESTAMP)")
        )
        .withColumn(
            "latitude",
            F.expr("try_cast(latitude AS DOUBLE)")
        )
        .withColumn(
            "longitude",
            F.expr("try_cast(longitude AS DOUBLE)")
        )
        .withColumn(
            "speed_kmh",
            F.expr("try_cast(speed_kmh AS DOUBLE)")
        )
    )


    # Null columns
    missing_event_id = F.col("event_id").isNull()
    missing_or_invalid_event_ts = F.col("event_ts").isNull()
    missing_latitude = F.col("latitude").isNull()
    missing_longitude = F.col("longitude").isNull()
    missing_speed = F.col("speed_kmh").isNull()


    # Out of bounds
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


    # Assign the first validation failure in the same order as clean_telematics.py
    clean = (
        telematics
        .withColumn(
            "_dq_reason",
            F.when(
                missing_event_id,
                F.lit("missing_event_id")
            )
            .when(
                missing_or_invalid_event_ts,
                F.lit("missing_or_invalid_event_ts")
            )
            .when(
                missing_latitude,
                F.lit("missing_latitude")
            )
            .when(
                missing_longitude,
                F.lit("missing_longitude")
            )
            .when(
                missing_speed,
                F.lit("missing_speed")
            )
            .when(
                invalid_latitude,
                F.lit("latitude_out_of_bounds")
            )
            .when(
                invalid_longitude,
                F.lit("longitude_out_of_bounds")
            )
            .when(
                invalid_speed,
                F.lit("speed_out_of_bounds")
            )
            .otherwise(F.lit(None).cast("string"))
        )
    )

    return clean


# Quarantine for telematics
@dlt.table(name="silver_telematics_quarantine")
def silver_telematics_quarantine():
    return (
        dlt.read_stream("prepare_silver_telematics")
        .where(F.col("_dq_reason").isNotNull())
    )
