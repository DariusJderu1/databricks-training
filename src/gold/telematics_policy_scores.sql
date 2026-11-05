CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.telematics_policy_scores AS

SELECT
    policy_id,
    100.0 * SUM(
        CAST(harsh_accel AS INT) + CAST(harsh_brake AS INT) + CAST(harsh_corner AS INT)
    ) / NULLIF(SUM(mileage), 0) AS harsh_score
FROM insurance.silver.telematics
GROUP BY policy_id;
