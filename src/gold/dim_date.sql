-- WORKED EXAMPLE — Gold: a conformed date dimension.
--
-- Every fact in the gold layer (claims, premium, payments) joins to this dimension,
-- so it is the natural first gold object to build. It is a generated dimension — no
-- source data — produced with sequence() + explode(), then enriched with the calendar
-- attributes a BI dashboard needs (year / quarter / month / week / weekend flag).
--
-- Exam domain 3 (Data Transformation and Modeling): building Gold objects (tables /
-- views) in Unity Catalog for BI and analytics teams.
--
-- Note on names: this uses the canonical `insurance.gold` schema. If you work in a
-- per-user dev sandbox (bundle dev target), either `USE CATALOG insurance; USE SCHEMA
-- <you>_gold;` first and drop the qualifier, or pass the schema as a SQL parameter.

CREATE SCHEMA IF NOT EXISTS insurance.gold;

CREATE OR REPLACE TABLE insurance.gold.dim_date AS
WITH calendar AS (
  SELECT explode(sequence(DATE'2018-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date
)
SELECT
  CAST(date_format(date, 'yyyyMMdd') AS INT)        AS date_key,      -- surrogate key (e.g. 20260504)
  date,
  year(date)                                        AS year,
  quarter(date)                                     AS quarter,
  concat(year(date), '-Q', quarter(date))           AS year_quarter,
  month(date)                                        AS month,
  date_format(date, 'MMMM')                          AS month_name,
  date_format(date, 'yyyy-MM')                       AS year_month,
  day(date)                                          AS day_of_month,
  dayofweek(date)                                    AS day_of_week,   -- 1 = Sunday .. 7 = Saturday
  date_format(date, 'EEEE')                          AS day_name,
  weekofyear(date)                                   AS week_of_year,
  (dayofweek(date) IN (1, 7))                        AS is_weekend,
  date_trunc('MONTH', date)                          AS first_of_month,
  last_day(date)                                     AS last_of_month
FROM calendar;

-- quick sanity check
-- SELECT count(*) AS days, min(date) AS from_date, max(date) AS to_date FROM insurance.gold.dim_date;
