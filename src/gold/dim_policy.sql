-- TODO STUB — Gold: dim_policy (BI policy dimension).
-- Goal: one row per policy from silver.policies, with a coverage rollup from
--       silver.policy_coverages (e.g. coverage_count, total_limit).
-- Acceptance: unique policy_id; product_line/status canonical; clean sum_insured/premium.
-- Exam domain 3 (Gold objects). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.dim_policy AS
SELECT
  p.policy_id, p.customer_id, p.agent_id, p.product_line, p.status,
  p.effective_date, p.expiration_date, p.annual_premium, p.sum_insured, p.payment_frequency
  -- TODO: LEFT JOIN a coverage rollup from silver.policy_coverages
  --       (count(*) AS coverage_count, sum(limit) AS total_limit) grouped by policy_id
FROM insurance.silver.policies p;
