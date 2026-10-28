-- TODO STUB — Gold: agg_customer_value (requirement #7).
-- Goal: per-customer 360 metrics: tenure, #active policies, lifetime premium, claim count,
--       and a churn flag (e.g. all policies CANCELLED/LAPSED).
-- Exam domain 3 (aggregates). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_customer_value AS
-- TODO: from dim_customer + dim_policy (+ fact_claims): count active policies, sum premium,
--       count claims, derive churn_flag; one row per customer.
SELECT CAST(NULL AS STRING) AS customer_id, CAST(NULL AS BIGINT) AS active_policies,
       CAST(NULL AS DOUBLE) AS lifetime_premium, CAST(NULL AS BOOLEAN) AS churn_flag
WHERE 1 = 0;
