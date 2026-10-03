-- Verification, not part of the pipeline.
-- Recomputes what gold SHOULD contain directly from the generator's definitions
-- (sql/generator.sql), without touching bronze, Spark, or silver, then compares
-- it cell by cell with the gold table the pipeline actually built.
-- Parameter: @last_ingest (DATE) - the last ingest partition loaded.

WITH orders AS (
  SELECT di, di * 1000000 + i AS n
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(@last_ingest, DATE '2026-08-01', DAY))) AS di,
       UNNEST(GENERATE_ARRAY(0, 999999)) AS i
),
attrs AS (
  SELECT
    n, di,
    IF(platform.u01(n, 'late') < 0.03,
       DATE_SUB(DATE_ADD(DATE '2026-08-01', INTERVAL di DAY),
                INTERVAL 1 + CAST(FLOOR(platform.u01(n, 'lag') * 5) AS INT64) DAY),
       DATE_ADD(DATE '2026-08-01', INTERVAL di DAY)) AS order_date,
    ['GE', 'DE', 'US', 'GB', 'TR'][OFFSET(CAST(FLOOR(platform.u01(n, 'ctry') * 5) AS INT64))] AS country,
    ROUND(5 + platform.u01(n, 'amt') * 495, 2) AS amount,
    platform.u01(n, 'bad')  AS bad,
    platform.u01(n, 'corr') AS corr
  FROM orders
),
truth AS (
  SELECT
    order_date, country,
    COUNT(*) AS orders,
    -- the correction for a day-di order arrives on day di+1; count it only if that day was ingested
    SUM(CAST(IF(corr < 0.02 AND di < DATE_DIFF(@last_ingest, DATE '2026-08-01', DAY),
                ROUND(amount * (0.8 + 0.4 * platform.u01(n, 'cadj')), 2),
                amount) AS NUMERIC)) AS revenue
  FROM attrs
  WHERE bad >= 0.010
  GROUP BY order_date, country
)
SELECT
  COUNT(*)                                                  AS cells_compared,
  COUNTIF(g.orders IS NULL OR t.orders IS NULL)             AS cells_missing_on_one_side,
  COUNTIF(g.orders != t.orders)                             AS order_count_mismatches,
  COUNTIF(ABS(g.revenue - t.revenue) > 0.01)                AS revenue_mismatches_over_1_cent,
  MAX(ABS(g.revenue - t.revenue))                           AS max_revenue_diff,
  SUM(t.orders)                                             AS truth_orders,
  SUM(g.orders)                                             AS gold_orders,
  SUM(t.revenue)                                            AS truth_revenue,
  SUM(g.revenue)                                            AS gold_revenue
FROM truth AS t
FULL OUTER JOIN platform.gold_daily_revenue AS g USING (order_date, country);
