-- TODO STUB — Gold: fact_payments (grain = one installment).
-- Goal: payment facts for billing-leakage analysis (req #4): amount_due vs amount_paid,
--       overdue exposure (status LATE/MISSED).
-- Acceptance: joins dim_policy + dim_date; collected <= written; overdue computed.
-- Exam domain 3. Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.fact_payments AS
SELECT
  p.payment_id, p.policy_id, p.installment_no, p.due_date, p.paid_date, p.amount_due, p.amount_paid, p.status,
  dd.date_key,
  CASE WHEN p.status IN ('LATE', 'MISSED') THEN true ELSE false END AS is_overdue
  -- TODO: date_key from dim_date on due_date; is_overdue = status IN ('LATE','MISSED').
FROM insurance.silver.payments p   -- TODO: ensure a cleaned silver.payments exists (from bronze.payments)
JOIN insurance.gold.dim_date dd ON p.due_date = dd.date
JOIN insurance.gold.dim_policy dp ON p.policy_id = dp.policy_id;
