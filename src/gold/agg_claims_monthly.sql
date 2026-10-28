-- TODO STUB — Gold: agg_claims_monthly (requirement #2).
-- Goal: monthly claims frequency & severity; severity by peril.
--   frequency = claim_count / active_policies ; severity = avg(loss_amount).
-- Exam domain 3 (aggregates: count, avg). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_claims_monthly AS
-- TODO: group fact_claims by month (+ peril for the severity-by-peril view); compute
--       count(*) AS claim_count, avg(loss_amount) AS severity, approx_count_distinct(policy_id).
SELECT CAST(NULL AS INT) AS month_key, CAST(NULL AS STRING) AS peril_code,
       CAST(NULL AS BIGINT) AS claim_count, CAST(NULL AS DOUBLE) AS severity
WHERE 1 = 0;
