-- TODO STUB — Gold: fact_claims (grain = one claim).
-- Goal: claim facts joined to conformed dimensions for BI (requirements #2, #3).
-- Acceptance: every claim joins dim_policy + dim_date; fraud_flag is boolean; peril resolves.
-- Exam domain 3 (Gold objects + star join). Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.fact_claims AS
SELECT
  cl.claim_id,
  cl.policy_id,
  cl.customer_id,
  -- TODO: date_key from dim_date for claim_date (CAST(date_format(...,'yyyyMMdd') AS INT))
  dd.date_key AS date_key, -- FK to the surrogate key from dim_date
  cl.peril_code,
  cl.claim_status,
  cl.loss_amount,
  cl.paid_amount,
  cl.fraud_flag,
  dp.product_line as product_line, -- Note: IMO this is not good. We have a dim_policy and can retrieve data from it via policy_id
  rpc.peril_name as peril_name -- same note as above
  -- TODO: join dim_policy for product_line/state; 
  --       join dim_date on claim_date;
  --       join ref_peril_codes for peril_name.
FROM insurance.silver.claims cl
JOIN insurance.gold.dim_date dd on cl.claim_date = dd.date
JOIN insurance.gold.dim_policy dp on cl.policy_id = dp.policy_id
LEFT JOIN insurance.bronze.ref_peril_codes rpc on cl.peril_code = rpc.peril_code;
