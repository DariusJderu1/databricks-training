-- TODO STUB — Gold: agg_loss_ratio (requirement #1).
-- Goal: monthly loss ratio = incurred losses / earned premium, by product_line & state.
-- Acceptance: loss_ratio per (month, product_line, state); sane 0..cap for >95% of cells.
-- Exam domain 3 (aggregate Gold object — a materialized view is a good fit). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_loss_ratio AS
-- TODO: aggregate incurred losses (sum loss_amount from fact_claims) by month×product_line×state,
--       aggregate earned premium (sum earned_premium from fact_premium) by the same grain,
--       join them, and compute loss_ratio = losses / NULLIF(earned, 0).
SELECT CAST(NULL AS INT) AS month_key, CAST(NULL AS STRING) AS product_line,
       CAST(NULL AS STRING) AS state, CAST(NULL AS DOUBLE) AS loss_ratio
WHERE 1 = 0;  -- TODO: replace with the real aggregation
