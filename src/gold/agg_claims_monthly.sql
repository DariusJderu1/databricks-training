-- TODO STUB — Gold: agg_claims_monthly (requirement #2).
-- Goal: monthly claims frequency & severity; severity by peril.
--   frequency = claim_count / active_policies ; severity = avg(loss_amount).
-- Exam domain 3 (aggregates: count, avg). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

-- CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_claims_monthly AS
-- TODO: group fact_claims by month (+ peril for the severity-by-peril view); compute
--       count(*) AS claim_count, avg(loss_amount) AS severity, approx_count_distinct(policy_id).
-- SELECT CAST(NULL AS INT) AS month_key, CAST(NULL AS STRING) AS peril_code,
--        CAST(NULL AS BIGINT) AS claim_count, CAST(NULL AS DOUBLE) AS severity
-- WHERE 1 = 0;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_claims_monthly AS
WITH months AS (
    SELECT DISTINCT year_month, first_of_month, last_of_month
    FROM insurance.gold.dim_date
),
active_policies AS (
    SELECT m.year_month, COUNT(DISTINCT dp.policy_id) AS active_policy_count
    FROM months m
    LEFT JOIN insurance.gold.dim_policy dp ON dp.effective_date <= m.last_of_month AND dp.expiration_date >= m.first_of_month
    GROUP BY m.year_month
)
SELECT dd.year_month AS month_key, rpc.peril_name,
       COUNT(*) AS claim_count,
       AVG(fc.loss_amount) AS severity,  -- avg loss per claim
       approx_count_distinct(fc.policy_id) AS policies_with_claims,
       AVG(CASE WHEN fc.fraud_flag THEN 1 ELSE 0 END) AS fraud_rate, -- ~0.04 after silver
       claim_count / NULLIF(ap.active_policy_count, 0) AS frequency
FROM insurance.gold.fact_claims fc
JOIN insurance.gold.dim_date dd ON fc.date_key = dd.date_key
LEFT JOIN insurance.bronze.ref_peril_codes rpc ON fc.peril_code = rpc.peril_code
JOIN active_policies ap ON dd.year_month = ap.year_month
GROUP BY dd.year_month, rpc.peril_name, active_policy_count;
