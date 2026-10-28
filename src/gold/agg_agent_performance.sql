-- TODO STUB — Gold: agg_agent_performance (requirement #5).
-- Goal: per-agent policies sold, retention, and loss ratio. Use the CURRENT version of the
--       SCD2 dim_agent (WHERE is_current = true) for agent attributes.
-- Exam domain 3 (aggregates + SCD2 join). Studybook: M4 (dim from src/silver/clean_agents_scd2.py).

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_agent_performance AS
-- TODO: join dim_policy -> dim_agent (is_current = true); aggregate policies_sold,
--       retention (renewed/total), and loss_ratio (losses from fact_claims / earned premium).

-- SELECT CAST(NULL AS STRING) AS agent_id, CAST(NULL AS BIGINT) AS policies_sold,
--        CAST(NULL AS DOUBLE) AS retention, CAST(NULL AS DOUBLE) AS loss_ratio
-- WHERE 1 = 0;

-- I assume that the renewed policies are the ones that have the same customer,
-- same product line, different policy id, same agent id and the new policy began after 0 - 30 days
-- after the old policy expired
WITH agents_with_renewed_policies AS (
    SELECT DISTINCT
        old.agent_id,
        old.policy_id AS previous_policy_id
    FROM insurance.gold.dim_policy old
    JOIN insurance.gold.dim_policy new
        ON old.customer_id = new.customer_id
        AND old.product_line = new.product_line
        AND old.policy_id != new.policy_id
        AND old.agent_id = new.agent_id
        AND DATEDIFF(new.effective_date, old.expiration_date) BETWEEN 0 AND 30
),

fact_claims_loss_amount_per_policy AS (
    SELECT policy_id,
           SUM(loss_amount) AS sum_loss_amount
    FROM insurance.gold.fact_claims
    GROUP BY policy_id
),

fact_premium_earned_premium_per_policy AS (
    SELECT policy_id,
           SUM(earned_premium) AS sum_earned_premium
    FROM insurance.gold.fact_premium
    GROUP BY policy_id
)

SELECT da.agent_id, 
       COUNT(dp.policy_id) AS policies_sold,
       COUNT(awrp.agent_id) / COUNT(da.agent_id) AS retention,
       COALESCE(SUM(fclapp.sum_loss_amount), 0)
            / NULLIF(SUM(fpeppp.sum_earned_premium), 0) AS loss_ratio

FROM insurance.silver.dim_agent da

JOIN insurance.gold.dim_policy dp 
    ON da.agent_id = dp.agent_id

LEFT JOIN agents_with_renewed_policies awrp 
    ON dp.policy_id = awrp.previous_policy_id

LEFT JOIN fact_claims_loss_amount_per_policy fclapp
    ON dp.policy_id = fclapp.policy_id

LEFT JOIN fact_premium_earned_premium_per_policy fpeppp 
    ON dp.policy_id = fpeppp.policy_id

WHERE da.is_current IS TRUE

GROUP BY da.agent_id;
