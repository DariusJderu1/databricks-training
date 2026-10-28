-- TODO STUB — Gold: fact_payments (grain = one installment).
-- Goal: payment facts for billing-leakage analysis (req #4): amount_due vs amount_paid,
--       overdue exposure (status LATE/MISSED).
-- Acceptance: joins dim_policy + dim_date; collected <= written; overdue computed.
-- Exam domain 3. Studybook: M4.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.fact_payments AS
SELECT
  pay.payment_id, pay.policy_id, pay.installment_no,
  pay.due_date, pay.paid_date, pay.amount_due, pay.amount_paid, pay.status
  -- TODO: date_key from dim_date on due_date; is_overdue = status IN ('LATE','MISSED').
FROM insurance.silver.payments pay;   -- TODO: ensure a cleaned silver.payments exists (from bronze.payments)
