CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.telematics_policy_scores AS

SELECT
    policy_id,
    100.0 * SUM(
        CASE WHEN LOWER(TRIM(harsh_accel))  = 'true' THEN 1 ELSE 0 END
      + CASE WHEN LOWER(TRIM(harsh_brake))  = 'true' THEN 1 ELSE 0 END
      + CASE WHEN LOWER(TRIM(harsh_corner)) = 'true' THEN 1 ELSE 0 END
    ) / NULLIF(SUM(mileage), 0) AS harsh_score
FROM insurance.silver.telematics
GROUP BY policy_id;
