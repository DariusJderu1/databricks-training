-- TODO STUB — Gold: agg_loss_ratio (requirement #1).
-- Goal: monthly loss ratio = incurred losses / earned premium, by product_line & state.
-- Acceptance: loss_ratio per (month, product_line, state); sane 0..cap for >95% of cells.
-- Exam domain 3 (aggregate Gold object — a materialized view is a good fit). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_loss_ratio AS
-- TODO: aggregate incurred losses (sum loss_amount from fact_claims) by month×product_line×state,
--       aggregate earned premium (sum earned_premium from fact_premium) by the same grain,
--       join them, and compute loss_ratio = losses / NULLIF(earned, 0).

-- SELECT CAST(NULL AS INT) AS month_key, CAST(NULL AS STRING) AS product_line,
--        CAST(NULL AS STRING) AS state, CAST(NULL AS DOUBLE) AS loss_ratio
WITH losses AS (
    SELECT CAST(date_format(dd.date, 'yyyyMM') AS INT) AS month_key, fc.product_line AS product_line, dc.state AS state, 
    SUM(fc.loss_amount) AS incurred_losses
    FROM insurance.gold.fact_claims fc
    JOIN insurance.gold.dim_date dd on fc.date_key = dd.date_key
    JOIN insurance.gold.dim_customer dc on dc.customer_id = fc.customer_id
    GROUP BY CAST(date_format(dd.date, 'yyyyMM') AS INT), fc.product_line, dc.state
), 
earned AS (
    SELECT fp.month_key, fp.product_line, dc.state, SUM(fp.earned_premium) AS earned_premium
    FROM insurance.gold.fact_premium fp
    JOIN insurance.gold.dim_policy dp ON fp.policy_id = dp.policy_id
    JOIN insurance.gold.dim_customer dc ON dp.customer_id = dc.customer_id
    GROUP BY fp.month_key, fp.product_line, dc.state
)
SELECT e.month_key, e.product_line, e.state, 
       COALESCE(l.incurred_losses, 0) AS incurred_losses, 
       e.earned_premium,
       COALESCE(l.incurred_losses, 0) / NULLIF(e.earned_premium, 0) AS loss_ratio
FROM earned e
LEFT JOIN losses l on l.month_key = e.month_key AND l.product_line = e.product_line AND l.state = e.state