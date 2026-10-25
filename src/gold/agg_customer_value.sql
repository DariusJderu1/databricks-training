-- TODO STUB — Gold: agg_customer_value (requirement #7).
-- Goal: per-customer 360 metrics: tenure, #active policies, lifetime premium, claim count,
--       and a churn flag (e.g. all policies CANCELLED/LAPSED).
-- Exam domain 3 (aggregates). Studybook: M4.

-- Customer 360 — one current row per customer (fuzzy/exact dupes removed), with tenure, #active policies, 
-- lifetime premium, and a churn flag. Accept: customer count after dedup < raw row count; 
-- no duplicate customer_id in dim_customer.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_customer_value AS
-- TODO: from dim_customer + dim_policy (+ fact_claims): count active policies, sum premium,
--       count claims, derive churn_flag; one row per customer.

-- SELECT CAST(NULL AS STRING) AS customer_id, CAST(NULL AS BIGINT) AS active_policies,
--        CAST(NULL AS DOUBLE) AS lifetime_premium, CAST(NULL AS BOOLEAN) AS churn_flag
-- WHERE 1 = 0;

SELECT dc.customer_id,
       COUNT(dp.status) AS active_policies

FROM insurance.gold.dim_customer dc

LEFT JOIN insurance.gold.dim_policy dp
    ON dc.customer_id = dp.customer_id 
    AND dp.status = 'ACTIVE'

GROUP BY dc.customer_id;
