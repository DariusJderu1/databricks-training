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

-- SELECT CAST(NULL AS STRING) AS policy_id, CAST(NULL AS DOUBLE) AS harsh_score,
--        CAST(NULL AS BOOLEAN) AS had_claim
-- WHERE 1 = 0;

WITH indicators AS (
    SELECT SUM(CASE WHEN t.harsh_accel IS TRUE THEN 1 ELSE 0 END) AS i1, 
           SUM(CASE WHEN t.harsh_brake IS TRUE THEN 1 ELSE 0 END) AS i2, 
           SUM(CASE WHEN t.harsh_corner IS TRUE THEN 1 ELSE 0 END) AS i3,
           t.policy_id

    FROM insurance.silver.telematics t 

    GROUP BY policy_id
),

policies_with_claims AS (
    SELECT fc.policy_id
    FROM insurance.gold.fact_claims fc
    GROUP BY fc.policy_id
),

policy_scores AS (
    SELECT t.policy_id,
       CASE WHEN pwc.policy_id IS NULL THEN FALSE ELSE TRUE END AS had_claim,
       i.i1 AS sum_harsh_accel, i.i2 AS sum_harsh_brake, i.i3 AS sum_harsh_corner,
       SUM(t.mileage) AS sum_mileage,
       -- I assume the weights are 1 for all 3
       100 * ((1 * i.i1) + (1 * i.i2) + (1 * i.i3)) / NULLIF(SUM(t.mileage), 0) AS harsh_score

    FROM insurance.silver.telematics t

    JOIN indicators i 
        ON t.policy_id = i.policy_id

    LEFT JOIN policies_with_claims pwc 
        ON t.policy_id = pwc.policy_id 

    GROUP BY t.policy_id, i.i1, i.i2, i.i3, pwc.policy_id
),

score_quintile AS (
    SELECT ps.policy_id, 
       ps.had_claim, 
       ps.sum_harsh_accel, ps.sum_harsh_brake, ps.sum_harsh_corner,
       ps.harsh_score,
       NTILE(5) OVER (ORDER BY ps.harsh_score, ps.policy_id) AS harsh_score_quintile_number

    FROM policy_scores ps

    WHERE ps.harsh_score IS NOT NULL
)

SELECT sq.harsh_score_quintile_number,
       COUNT(sq.policy_id) AS number_of_policies,
       SUM(CASE WHEN sq.had_claim IS TRUE THEN 1 ELSE 0 END) AS number_of_policies_with_claims,
       SUM(CASE WHEN sq.had_claim IS TRUE THEN 1 ELSE 0 END) / COUNT(sq.policy_id) * 100 AS claim_rate

FROM score_quintile sq

GROUP BY sq.harsh_score_quintile_number;
