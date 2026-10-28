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

WITH policy_rollup AS (
    SELECT dp.customer_id,
           SUM(CASE WHEN dp.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_policies,
           COUNT(*) = SUM(
               CASE WHEN dp.status IN ('CANCELLED', 'LAPSED') THEN 1 ELSE 0 END
           ) AS churn_flag

    FROM insurance.gold.dim_policy dp

    GROUP BY dp.customer_id
),

premium_rollup AS (
    SELECT dp.customer_id,
           SUM(fp.earned_premium) AS lifetime_premium

    FROM insurance.gold.dim_policy dp

    JOIN insurance.gold.fact_premium fp
        ON dp.policy_id = fp.policy_id

    GROUP BY dp.customer_id
),

claim_rollup AS (
    SELECT fc.customer_id,
           COUNT(fc.claim_id) AS claim_count
    
    FROM insurance.gold.fact_claims fc

    GROUP BY fc.customer_id
)

SELECT dc.customer_id,
       dc.tenure_years,
       COALESCE(pr.active_policies, 0) AS active_policies,
       COALESCE(prem.lifetime_premium, 0) AS lifetime_premium,
       COALESCE(cr.claim_count, 0) AS claim_count,
       COALESCE(pr.churn_flag, FALSE) AS churn_flag

FROM insurance.gold.dim_customer dc

LEFT JOIN policy_rollup pr ON dc.customer_id = pr.customer_id

LEFT JOIN premium_rollup prem ON dc.customer_id = prem.customer_id

LEFT JOIN claim_rollup cr ON dc.customer_id = cr.customer_id;
