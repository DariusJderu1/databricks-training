-- TODO STUB — Gold/Ops: dq_scorecard (requirement #8, the meta dashboard).
-- Goal: surface rows passed vs quarantined per silver table/rule, plus source freshness.
--       Reads the ops.dq_scorecard rows your silver jobs append (validate_expectations.py).
-- Acceptance: passed + quarantined = ingested per table; latest run per table is visible.
-- Exam domains 3 (DQ checks) & 7 (governance scorecard). Studybook: M3/M4.

CREATE SCHEMA IF NOT EXISTS insurance.ops;

-- The silver jobs append raw rows to insurance.ops.dq_scorecard. This view rolls them up
-- to the latest run per table for the dashboard.
CREATE OR REPLACE VIEW insurance.gold.v_dq_scorecard AS
-- TODO: SELECT table_name, passed, quarantined, quarantined/(passed+quarantined) AS quarantine_rate,
--        run_ts, ranked to the latest run per table (window: row_number() over (partition by table_name
--        order by run_ts desc) = 1).
SELECT CAST(NULL AS STRING) AS table_name, CAST(NULL AS BIGINT) AS passed,
       CAST(NULL AS BIGINT) AS quarantined, CAST(NULL AS TIMESTAMP) AS run_ts
WHERE 1 = 0;
