"""TODO STUB — Silver: clean & dedupe `customers`.

Goal
----
Read `bronze.customers`, fix the seeded defects, and write a clean, de-duplicated
`silver.customers` (one current row per `customer_id`) plus a `silver.customers_quarantine`
for rows that fail validation.

Defects to handle (see docs/03_data_dictionary.md):
- exact + **fuzzy duplicates** (normalize text, then dedupe by `customer_id`)
- **null** email/phone (quarantine or impute per your rule)
- casing/whitespace in `city`; **invalid/typo `state`** → validate against `bronze.ref_us_states`
- malformed/future `date_of_birth` → parse with fallbacks

Stretch: build a **Type-2** `dim_customer` by reusing the pattern in
`src/silver/clean_agents_scd2.py` (deltas land customer updates with newer `updated_at`).

Acceptance
----------
- `silver.customers` has no duplicate `customer_id` and only canonical 2-letter states.
- Quarantined rows are counted into the DQ scorecard (see `validate_expectations.py`).

Exam domain 3 (Data Transformation and Modeling). Studybook: `docs/studybook/M3_silver_transform.md`.
"""
from __future__ import annotations

from pyspark.sql import functions as F  # noqa: F401

from pyspark.sql import Window

from src.common import config


def build(spark=None):
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('silver')}")

    bronze = spark.read.table(config.table("bronze", "customers"))
    states = spark.read.table(config.table("bronze", "ref_us_states"))  # broadcast this small dim

    # TODO 1: trim/standardize text (city, state); normalize state casing.
    # TODO 2: validate state via a broadcast join to ref_us_states; route invalids to quarantine.
    # TODO 3: parse date_of_birth (try multiple formats; null-out future/garbage dates).
    # TODO 4: dedupe — exact first, then fuzzy (same customer_id, normalized attrs).
    # TODO 5: split passed vs quarantined; write silver.customers and silver.customers_quarantine.
    # raise NotImplementedError("Implement customers cleaning — see studybook M3.")

    # TODO 1: trim/standardize text (city, state); normalize state casing.
    # where() keep the clients that have a customer_id column non NULL
    # withColumn() - creates or edits the column with the expression
    clean = (bronze.where(F.col("customer_id").isNotNull()) 
                    .withColumn("city", F.initcap(F.trim("city"))) 
                    .withColumn("email", F.lower(F.trim("email")))
                    .withColumn("phone", F.lower(F.trim("phone")))
                    .withColumn("state_raw", F.upper(F.trim("state"))))
    

    # TODO 2: validate state via a broadcast join to ref_us_states; route invalids to quarantine.

    # just the column we are interested in
    states = states.select("state_code")

    clean = (clean.join(F.broadcast(states), # uses the broadcast method for the small table (the result is a table with the columns from both sides)
                    clean.state_raw == states.state_code, "left") # uses a left join - keeps the rows from clean
              .withColumn("state", F.col("state_code")))

    # this is the condition based on what I will check the rows that are invalid
    invalid =  F.col("state_code").isNull()

    # the table for quarantine 
    state_quarantine = (clean.where(invalid)
                  .withColumn("_dq_reason", F.lit("invalid_state"))
                  .drop("state_code", "state"))

    # keep only rows with a valid state
    clean = (clean.where(~invalid)
             .drop("state_code", "state_raw"))

                        
    # TODO 3: parse date_of_birth (try multiple formats; null-out future/garbage dates).
    clean = (clean.withColumn("date_of_birth", 
                F.coalesce( # give the first non null result, if all are garbage - return null
                    F.to_date("date_of_birth", "yyyy-MM-dd"),
                    F.to_date("date_of_birth", "MM/dd/yyyy"),
                    F.to_date("date_of_birth", "dd-MMM-yyyy"))
                )
                .withColumn("dob_future_flag", F.col("date_of_birth") > F.current_date()) # creates a new boolean column, if the date is in the future
                .withColumn("date_of_birth", F.when( # if
                                F.col("dob_future_flag"), # this is true
                                F.lit(None).cast("date") # column becomes null
                            ).otherwise(F.col("date_of_birth"))) # else keep the value from date_of_birth
                .drop("dob_future_flag")) 

    invalid_dob = F.col("date_of_birth").isNull()

    dob_quarantine = (clean.where(invalid_dob)
                            .withColumn("_dq_reason", F.lit("invalid_date_of_birth")))

    clean = clean.where(~invalid_dob)


    # TODO 4: dedupe — exact first, then fuzzy (same customer_id, normalized attrs).
    clean = clean.dropDuplicates() # dedupes all the rows, based on each of the columns

    w = (Window.partitionBy("customer_id") # treat all rows with the same customer ID as one group
            .orderBy(F.col("updated_at").desc_nulls_last())) # inside each group, put the newest row first based on that updated_at

    clean = (clean
                .withColumn("_rn", F.row_number().over(w)) # adds the _rn column that represents the row number over the window
                .where("_rn = 1") # where the rows have the _rn column 1 (like it keeps only the rows that have that column 1.)
                .drop("_rn")) # deletes the column _rn


    # TODO **null** email/phone (quarantine or impute per your rule)
    # create the object rule for filtering
    invalid = (
        F.col("email").isNull()
        | F.col("phone").isNull()
    )

    # create a dataframe for the quarantine table
    contact_quarantine = (clean.where(invalid) # all the invalid rows from the clean
                  .withColumn( 
                      "_dq_reason", # edit the column 
                       F.when(F.col("email").isNull() & F.col("phone").isNull(), F.lit("missing_phone_email"))
                        .when(F.col("email").isNull(), F.lit("missing_email")) # if the column is null insert in the _dq_reason the string with explanation
                        .when(F.col("phone").isNull(), F.lit("missing_phone"))
                ))

    # df with all the clean rows
    clean = clean.where(~invalid)

    # stack the rows of the DataFrames underneath each other, matching columns by their names
    # allowMissingColumns True means that if a one table has a column that the other doesn't, don't fail use NULL to fill the gap
    quarantine = (state_quarantine.unionByName(contact_quarantine, allowMissingColumns=True)
                                    .unionByName(dob_quarantine, allowMissingColumns=True))

    # we want to overwrite the silver.customers with the clean passed data
    (clean.write.mode("overwrite")  
        .saveAsTable(config.table("silver", "customers")))

    # we want to overwrite / create the quarantine table with its data
    (quarantine.write.mode("overwrite")
        .saveAsTable(config.table("silver", "customers_quarantine")))


if __name__ == "__main__":
    build()
