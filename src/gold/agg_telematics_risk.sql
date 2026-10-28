-- TODO STUB — Gold: agg_telematics_risk (requirement #6).
-- Goal: per-policy harsh-driving score from silver telematics (valid speeds only, late
--       events de-duplicated), compared to claim likelihood.
--   score = weighted harsh_brake/accel/corner events per 100 miles.
-- Exam domain 3 (aggregates). Studybook: M4 (+ watermarking covered in M3).

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_telematics_risk AS
-- TODO: aggregate silver.telematics by policy_id: sum harsh_* events, sum mileage,
--       score = 100 * (w1*brake + w2*accel + w3*corner) / NULLIF(mileage, 0);
--       LEFT JOIN a per-policy claim flag from fact_claims to compare score vs claim rate.
SELECT CAST(NULL AS STRING) AS policy_id, CAST(NULL AS DOUBLE) AS harsh_score,
       CAST(NULL AS BOOLEAN) AS had_claim
WHERE 1 = 0;
