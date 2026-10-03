-- silver -> gold, incremental and idempotent.
-- Parameters: @ingest_from, @ingest_to (DATE) - the ingest partitions just loaded.
--   daily:     both = the run date
--   backfill:  the whole range
--
-- A new ingest partition does not only contain today's orders: it carries late
-- arrivals (placed days earlier) and corrections to earlier orders. So:
--   1. find every order_date the new partitions touch        ("affected")
--   2. rebuild gold for exactly those dates from silver
--   3. leave every other date alone
--
-- Assumptions this relies on, both enforced by the source contract:
--   * an order is never ingested before it is placed  -> ingest_date >= order_date
--   * order_date never changes between versions of an order
-- Together they mean every version of an affected order lives in an ingest
-- partition >= MIN(affected), which is what bounds the scan below.

DECLARE affected ARRAY<DATE>;
DECLARE scan_from DATE;

SET affected = (
  SELECT ARRAY_AGG(DISTINCT order_date ORDER BY order_date)
  FROM platform.silver_orders
  WHERE ingest_date BETWEEN @ingest_from AND @ingest_to
);
SET scan_from = (SELECT MIN(d) FROM UNNEST(affected) AS d);

MERGE platform.gold_daily_revenue AS g
USING (
  SELECT
    order_date,
    country,
    COUNT(*)                                    AS orders,
    CAST(ROUND(SUM(amount), 2) AS NUMERIC)      AS revenue,
    CAST(ROUND(AVG(amount), 2) AS NUMERIC)      AS avg_order_value,
    CURRENT_TIMESTAMP()                         AS built_at
  FROM (
    -- latest version of each order, across every ingest partition that can hold one.
    -- No upper bound on ingest_date: re-running an old day must still see newer
    -- corrections, or a reprocess would roll gold back in time.
    SELECT order_id, order_date, country, amount
    FROM platform.silver_orders
    WHERE ingest_date >= scan_from
      AND order_date IN UNNEST(affected)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id
                               ORDER BY updated_at DESC, ingest_date DESC) = 1
  )
  GROUP BY order_date, country
) AS s
ON g.order_date = s.order_date AND g.country = s.country
WHEN MATCHED THEN UPDATE SET
  orders = s.orders, revenue = s.revenue,
  avg_order_value = s.avg_order_value, built_at = s.built_at
WHEN NOT MATCHED THEN INSERT
  (order_date, country, orders, revenue, avg_order_value, built_at)
VALUES
  (s.order_date, s.country, s.orders, s.revenue, s.avg_order_value, s.built_at)
WHEN NOT MATCHED BY SOURCE AND g.order_date IN UNNEST(affected) THEN DELETE;

-- Gold must agree with silver for every date it just rebuilt. If not, fail the
-- job loudly instead of publishing a number nobody can trust.
ASSERT (
  SELECT SUM(orders) FROM platform.gold_daily_revenue WHERE order_date IN UNNEST(affected)
) = (
  SELECT COUNT(DISTINCT order_id) FROM platform.silver_orders
  WHERE ingest_date >= scan_from AND order_date IN UNNEST(affected)
) AS 'gold order count does not match distinct silver orders for the affected dates';

SELECT
  ARRAY_LENGTH(affected)  AS affected_order_dates,
  scan_from               AS silver_scanned_from_ingest_date,
  (SELECT MIN(d) FROM UNNEST(affected) d) AS first_affected,
  (SELECT MAX(d) FROM UNNEST(affected) d) AS last_affected;
