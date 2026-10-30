-- Notes to myself:

-- The pipeline automatically uses the default schema and catalog

-- For pipelines the CREATE OR REFRESH is used rather than CREATE OR REPLACE

-- For datasets created and managed by a pipeline, it must be used 
-- a streaming table (incremental streaming processing) or a 
-- materialized view for batch queries like joins, dimensions, aggregations

-- Materialized views store previous query results and can track changes,
-- allowing efficient incremental refreshes instead of recomputing all
-- Similar to streaming tables, they maintain internal state, but use different mechanisms

-------------------------------------------------------------------------------

-- dim_date

CREATE OR REFRESH MATERIALIZED VIEW dim_date AS
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

-------------------------------------------------------------------------------

-- dim_policy

CREATE OR REFRESH MATERIALIZED VIEW dim_policy AS
WITH coverage_rollup AS (
  SELECT
    policy_id,
    COUNT(*) AS coverage_count,
    SUM(`limit`) AS total_limit
  FROM silver_policy_coverages
  GROUP BY policy_id
)
SELECT
  p.policy_id,
  p.customer_id,
  p.agent_id,
  p.product_line,
  p.status,
  p.effective_date,
  p.expiration_date,
  p.annual_premium,
  p.sum_insured,
  p.payment_frequency,
  COALESCE(c.coverage_count, 0) AS coverage_count,
  COALESCE(c.total_limit, 0) AS total_limit
FROM silver_policies AS p
LEFT JOIN coverage_rollup AS c
  ON p.policy_id = c.policy_id;

-------------------------------------------------------------------------------

-- dim_customer

CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.phone,
  c.city,
  c.state,
  rpr.region,
  c.segment, 
  c.customer_since,
  FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), TO_DATE(c.customer_since, 'yyyy-MM-dd')) / 12) AS tenure_years,
  CASE
    WHEN c.date_of_birth IS NULL THEN 'Unknown'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 18 THEN '<18'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 25 THEN '18-24'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 35 THEN '25-34'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 45 THEN '35-44'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 55 THEN '45-54'
    WHEN timestampdiff(YEAR, c.date_of_birth, current_date()) < 65 THEN '55-64'
    ELSE '65+'
  END AS age_band
FROM silver_customers c
LEFT JOIN insurance.bronze.ref_postal_region rpr ON SUBSTRING(c.postal_code, 1, 1) = CAST(rpr.zip1 AS STRING);

-------------------------------------------------------------------------------

-- fact_premium

CREATE OR REFRESH MATERIALIZED VIEW fact_premium AS
WITH calendar AS (
  SELECT explode(sequence(date_trunc('MONTH', p.effective_date), date_trunc('MONTH', p.expiration_date), INTERVAL 1 MONTH)) AS generated_month,
  p.policy_id, p.product_line, p.annual_premium / 12 AS earned_premium 
  FROM silver_policies p
)
SELECT
  c.policy_id, c.product_line,
  CAST(DATE_FORMAT(c.generated_month, "yyyyMM") AS INT) AS month_key,       
  c.earned_premium  
FROM calendar c;

-------------------------------------------------------------------------------

-- fact_payments

CREATE OR REFRESH MATERIALIZED VIEW fact_payments AS
SELECT
  p.payment_id, p.policy_id, p.installment_no, p.due_date, p.paid_date, p.amount_due, p.amount_paid, p.status,
  dd.date_key,
  CASE WHEN p.status IN ('LATE', 'MISSED') THEN true ELSE false END AS is_overdue
FROM silver_payments p   
JOIN dim_date dd ON p.due_date = dd.date
JOIN dim_policy dp ON p.policy_id = dp.policy_id;

-------------------------------------------------------------------------------

-- fact_claims

CREATE OR REFRESH MATERIALIZED VIEW fact_claims AS
SELECT
  cl.claim_id,
  cl.policy_id,
  cl.customer_id,
  dd.date_key AS date_key, 
  cl.peril_code,
  cl.claim_status,
  cl.loss_amount,
  cl.paid_amount,
  cl.fraud_flag,
  dp.product_line as product_line,
  rpc.peril_name as peril_name 
FROM silver_claims cl
JOIN dim_date dd on cl.claim_date = dd.date
JOIN dim_policy dp on cl.policy_id = dp.policy_id
LEFT JOIN insurance.bronze.ref_peril_codes rpc on cl.peril_code = rpc.peril_code;

-------------------------------------------------------------------------------

-- agg_agent_performance

CREATE OR REFRESH MATERIALIZED VIEW agg_agent_performance AS
WITH agents_with_renewed_policies AS (
    SELECT DISTINCT
        old.agent_id,
        old.policy_id AS previous_policy_id
    FROM dim_policy old
    JOIN dim_policy new
        ON old.customer_id = new.customer_id
        AND old.product_line = new.product_line
        AND old.policy_id != new.policy_id
        AND old.agent_id = new.agent_id
        AND DATEDIFF(new.effective_date, old.expiration_date) BETWEEN 0 AND 30
),

fact_claims_loss_amount_per_policy AS (
    SELECT
        policy_id,
        SUM(loss_amount) AS sum_loss_amount
    FROM fact_claims
    GROUP BY policy_id
),

fact_premium_earned_premium_per_policy AS (
    SELECT
        policy_id,
        SUM(earned_premium) AS sum_earned_premium
    FROM fact_premium
    GROUP BY policy_id
)

SELECT
    da.agent_id,
    COUNT(dp.policy_id) AS policies_sold,
    COUNT(awrp.agent_id) / COUNT(da.agent_id) AS retention,
    COALESCE(SUM(fclapp.sum_loss_amount), 0)
        / NULLIF(SUM(fpeppp.sum_earned_premium), 0) AS loss_ratio

FROM silver_agents da

JOIN dim_policy dp
    ON da.agent_id = dp.agent_id

LEFT JOIN agents_with_renewed_policies awrp
    ON dp.policy_id = awrp.previous_policy_id

LEFT JOIN fact_claims_loss_amount_per_policy fclapp
    ON dp.policy_id = fclapp.policy_id

LEFT JOIN fact_premium_earned_premium_per_policy fpeppp
    ON dp.policy_id = fpeppp.policy_id

WHERE da.__END_AT IS NULL

GROUP BY da.agent_id;

-------------------------------------------------------------------------------

-- agg_claims_monthly

CREATE OR REFRESH MATERIALIZED VIEW agg_claims_monthly AS
WITH months AS (
    SELECT DISTINCT year_month, first_of_month, last_of_month
    FROM dim_date
),
active_policies AS (
    SELECT m.year_month, COUNT(DISTINCT dp.policy_id) AS active_policy_count
    FROM months m
    LEFT JOIN dim_policy dp ON dp.effective_date <= m.last_of_month AND dp.expiration_date >= m.first_of_month
    GROUP BY m.year_month
)
SELECT dd.year_month AS month_key, rpc.peril_name,
       COUNT(*) AS claim_count,
       AVG(fc.loss_amount) AS severity,  -- avg loss per claim
       approx_count_distinct(fc.policy_id) AS policies_with_claims,
       AVG(CASE WHEN fc.fraud_flag THEN 1 ELSE 0 END) AS fraud_rate, -- ~0.04 after silver
       claim_count / NULLIF(ap.active_policy_count, 0) AS frequency
FROM fact_claims fc
JOIN dim_date dd ON fc.date_key = dd.date_key
JOIN insurance.bronze.ref_peril_codes rpc ON fc.peril_code = rpc.peril_code
JOIN active_policies ap ON dd.year_month = ap.year_month
GROUP BY dd.year_month, rpc.peril_name, active_policy_count;

-------------------------------------------------------------------------------

-- agg_customer_value

CREATE OR REFRESH MATERIALIZED VIEW agg_customer_value AS

WITH policy_rollup AS (
    SELECT dp.customer_id,
           SUM(CASE WHEN dp.status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_policies,
           COUNT(*) = SUM(
               CASE WHEN dp.status IN ('CANCELLED', 'LAPSED') THEN 1 ELSE 0 END
           ) AS churn_flag

    FROM dim_policy dp

    GROUP BY dp.customer_id
),

premium_rollup AS (
    SELECT dp.customer_id,
           SUM(fp.earned_premium) AS lifetime_premium

    FROM dim_policy dp

    JOIN fact_premium fp
        ON dp.policy_id = fp.policy_id

    GROUP BY dp.customer_id
),

claim_rollup AS (
    SELECT fc.customer_id,
           COUNT(fc.claim_id) AS claim_count
    
    FROM fact_claims fc

    GROUP BY fc.customer_id
)

SELECT dc.customer_id,
       dc.tenure_years,
       COALESCE(pr.active_policies, 0) AS active_policies,
       COALESCE(prem.lifetime_premium, 0) AS lifetime_premium,
       COALESCE(cr.claim_count, 0) AS claim_count,
       COALESCE(pr.churn_flag, FALSE) AS churn_flag

FROM dim_customer dc

LEFT JOIN policy_rollup pr ON dc.customer_id = pr.customer_id

LEFT JOIN premium_rollup prem ON dc.customer_id = prem.customer_id

LEFT JOIN claim_rollup cr ON dc.customer_id = cr.customer_id;

-------------------------------------------------------------------------------

-- agg_loss_ratio

CREATE OR REFRESH MATERIALIZED VIEW agg_loss_ratio AS
WITH losses AS (
    SELECT CAST(date_format(dd.date, 'yyyyMM') AS INT) AS month_key, fc.product_line AS product_line, dc.state AS state, 
    SUM(fc.loss_amount) AS incurred_losses
    FROM fact_claims fc
    JOIN dim_date dd on fc.date_key = dd.date_key
    JOIN dim_customer dc on dc.customer_id = fc.customer_id
    GROUP BY CAST(date_format(dd.date, 'yyyyMM') AS INT), fc.product_line, dc.state
), 
earned AS (
    SELECT fp.month_key, fp.product_line, dc.state, SUM(fp.earned_premium) AS earned_premium
    FROM fact_premium fp
    JOIN dim_policy dp ON fp.policy_id = dp.policy_id
    JOIN dim_customer dc ON dp.customer_id = dc.customer_id
    GROUP BY fp.month_key, fp.product_line, dc.state
)
SELECT e.month_key, e.product_line, e.state, 
       COALESCE(l.incurred_losses, 0) AS incurred_losses, 
       e.earned_premium,
       COALESCE(l.incurred_losses, 0) / NULLIF(e.earned_premium, 0) AS loss_ratio
FROM earned e
LEFT JOIN losses l on l.month_key = e.month_key AND l.product_line = e.product_line AND l.state = e.state;

-------------------------------------------------------------------------------

-- agg_telematics

CREATE OR REFRESH MATERIALIZED VIEW agg_telematics_risk AS

WITH indicators AS (
    SELECT SUM(CASE WHEN t.harsh_accel IS TRUE THEN 1 ELSE 0 END) AS i1, 
           SUM(CASE WHEN t.harsh_brake IS TRUE THEN 1 ELSE 0 END) AS i2, 
           SUM(CASE WHEN t.harsh_corner IS TRUE THEN 1 ELSE 0 END) AS i3,
           t.policy_id

    FROM silver_telematics t 

    GROUP BY policy_id
),

policies_with_claims AS (
    SELECT fc.policy_id
    FROM fact_claims fc
    GROUP BY fc.policy_id
),

policy_scores AS (
    SELECT t.policy_id,
       CASE WHEN pwc.policy_id IS NULL THEN FALSE ELSE TRUE END AS had_claim,
       i.i1 AS sum_harsh_accel, i.i2 AS sum_harsh_brake, i.i3 AS sum_harsh_corner,
       SUM(t.mileage) AS sum_mileage,
       -- I assume the weights are 1 for all 3
       100 * ((1 * i.i1) + (1 * i.i2) + (1 * i.i3)) / NULLIF(SUM(t.mileage), 0) AS harsh_score

    FROM silver_telematics t

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

-------------------------------------------------------------------------------

-- dq_scorecard


