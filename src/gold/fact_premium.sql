-- TODO STUB — Gold: fact_premium (grain = policy × month).
-- Goal: monthly EARNED premium per policy so loss ratio (req #1) and written-vs-collected
--       (req #4) are computable. Earned premium (simplified) = annual_premium / 12 for each
--       month the policy is in force between effective_date and expiration_date.
-- Acceptance: one row per (policy_id, month) within the policy's active window.
-- Exam domain 3 (Gold objects). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.fact_premium AS
-- TODO: explode each policy into its active months. Hint:
--   sequence(date_trunc('MONTH', effective_date), date_trunc('MONTH', expiration_date), INTERVAL 1 MONTH)
--   then explode, and emit annual_premium/12 AS earned_premium per month.
WITH calendar AS (
  SELECT explode(sequence(date_trunc('MONTH', p.effective_date), date_trunc('MONTH', p.expiration_date), INTERVAL 1 MONTH)) AS generated_month,
  p.policy_id, p.product_line, p.annual_premium / 12 AS earned_premium 
  FROM insurance.silver.policies p
)
SELECT
  c.policy_id, c.product_line,
  CAST(DATE_FORMAT(c.generated_month, "yyyyMM") AS INT) AS month_key,          -- TODO: yyyyMM from the exploded month
  c.earned_premium  -- TODO: only for active months
FROM calendar c;
