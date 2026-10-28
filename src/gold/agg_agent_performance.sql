-- TODO STUB — Gold: agg_agent_performance (requirement #5).
-- Goal: per-agent policies sold, retention, and loss ratio. Use the CURRENT version of the
--       SCD2 dim_agent (WHERE is_current = true) for agent attributes.
-- Exam domain 3 (aggregates + SCD2 join). Studybook: M4 (dim from src/silver/clean_agents_scd2.py).

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_agent_performance AS
-- TODO: join dim_policy -> dim_agent (is_current = true); aggregate policies_sold,
--       retention (renewed/total), and loss_ratio (losses from fact_claims / earned premium).
SELECT CAST(NULL AS STRING) AS agent_id, CAST(NULL AS BIGINT) AS policies_sold,
       CAST(NULL AS DOUBLE) AS retention, CAST(NULL AS DOUBLE) AS loss_ratio
WHERE 1 = 0;
