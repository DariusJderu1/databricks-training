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
  c.first_name,
  c.last_name,
  c.email,
  c.phone,
  c.city,
  c.state,
  rpr.region,
  c.segment, 
  c.customer_since,
  FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), TO_DATE(c.customer_since, "yyyy-MM-dd")) / 12) AS tenure_years,
  CASE
    WHEN c.date_of_birth IS NULL THEN 'Unknown'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 18 THEN '<18'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 25 THEN '18-24'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 35 THEN '25-34'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 45 THEN '35-44'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 55 THEN '45-54'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 65 THEN '55-64'
    ELSE '65+'
  END AS age_band
FROM insurance.silver.customers c
-- TODO: join ref_postal_region for region; compute tenure_years and age_band.
LEFT JOIN insurance.bronze.ref_postal_region rpr ON SUBSTRING(c.postal_code, 1, 1) = CAST(rpr.zip1 AS STRING);
