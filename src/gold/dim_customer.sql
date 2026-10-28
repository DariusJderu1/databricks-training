-- TODO STUB — Gold: dim_customer (BI customer dimension).
-- Goal: one clean current row per customer for BI, from silver.customers.
-- Acceptance: unique customer_id; derived attrs (age band, tenure_years, region via zip1).
-- Exam domain 3 (Gold objects). Requirement #7 (Customer 360). Studybook: M4.
-- Stretch: make it SCD2 by reusing src/silver/clean_agents_scd2.py.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.dim_customer AS
SELECT
  c.customer_id,
  -- TODO: name, email/phone (consider masking — see M9), city, state, region,
  --       segment, customer_since, tenure_years, age_band
  c.state
FROM insurance.silver.customers c;
-- TODO: join ref_postal_region for region; compute tenure_years and age_band.
