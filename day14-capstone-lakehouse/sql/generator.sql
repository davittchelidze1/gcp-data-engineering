-- Synthetic order-event generator, evaluated entirely inside BigQuery.
--
-- Every attribute of order n is a deterministic hash of n, so any order can be
-- regenerated on demand. That is what makes corrections to *yesterday's* orders
-- possible, and what lets reconcile_truth.sql compute an independent ground truth.
--
-- Per ingest day, ~1,000,000 orders plus the mess real feeds carry:
--   3.0%  late arrivals   order placed 1-5 days before the day it is ingested
--   0.5%  exact replays   the same event delivered twice
--   2.0%  corrections     yesterday's order re-sent with a new amount (later updated_at)
--   1.0%  invalid rows    negative amount / zero items / missing timestamp / missing amount
--
-- Queries against these functions reference no tables, so they bill 0 bytes.

-- uniform [0, 1) from (n, salt); the double MOD keeps FARM_FINGERPRINT's sign out
CREATE OR REPLACE FUNCTION platform.u01(n INT64, salt STRING) AS (
  MOD(MOD(FARM_FINGERPRINT(CONCAT(CAST(n AS STRING), ':', salt)), 1000000) + 1000000, 1000000)
    / 1000000.0
);

-- The clean, canonical version of every order first ingested on ingest_d.
CREATE OR REPLACE TABLE FUNCTION platform.order_universe(ingest_d DATE) AS (
  SELECT
    n,
    FORMAT('ORD-%09d', n) AS order_id,
    order_date,
    FORMAT('CUST-%06d', CAST(FLOOR(platform.u01(n, 'cust') * 200000) AS INT64)) AS customer_id,
    ['GE', 'DE', 'US', 'GB', 'TR'][OFFSET(CAST(FLOOR(platform.u01(n, 'ctry') * 5) AS INT64))] AS country,
    1 + CAST(FLOOR(platform.u01(n, 'items') * 6) AS INT64) AS items,
    ROUND(5 + platform.u01(n, 'amt') * 495, 2) AS amount,
    TIMESTAMP_ADD(TIMESTAMP(order_date),
                  INTERVAL CAST(FLOOR(platform.u01(n, 'sec') * 86400) AS INT64) SECOND) AS order_ts,
    platform.u01(n, 'bad')  AS bad,
    platform.u01(n, 'dup')  AS dup,
    platform.u01(n, 'corr') AS corr
  FROM (
    SELECT
      n,
      IF(platform.u01(n, 'late') < 0.03,
         DATE_SUB(ingest_d, INTERVAL 1 + CAST(FLOOR(platform.u01(n, 'lag') * 5) AS INT64) DAY),
         ingest_d) AS order_date
    FROM UNNEST(GENERATE_ARRAY(
           DATE_DIFF(ingest_d, DATE '2026-08-01', DAY) * 1000000,
           DATE_DIFF(ingest_d, DATE '2026-08-01', DAY) * 1000000 + 999999)) AS n
  )
);

-- Exactly what lands in bronze for ingest day d, dirt included.
CREATE OR REPLACE TABLE FUNCTION platform.gen_orders(d DATE) AS (
  WITH base AS (
    SELECT
      order_id, customer_id, country,
      IF(bad >= 0.004 AND bad < 0.007, 0, items) AS items,
      CASE WHEN bad < 0.004                  THEN -amount
           WHEN bad >= 0.009 AND bad < 0.010 THEN NULL
           ELSE amount END AS amount,
      IF(bad >= 0.007 AND bad < 0.009, NULL, order_ts) AS order_ts,
      IF(bad >= 0.007 AND bad < 0.009, NULL, order_ts) AS updated_at,
      dup
    FROM platform.order_universe(d)
  ),
  corrections AS (
    SELECT
      order_id, customer_id, country, items,
      ROUND(amount * (0.8 + 0.4 * platform.u01(n, 'cadj')), 2) AS amount,
      order_ts,
      TIMESTAMP_ADD(TIMESTAMP(d),
                    INTERVAL CAST(FLOOR(platform.u01(n, 'csec') * 86400) AS INT64) SECOND) AS updated_at
    FROM platform.order_universe(DATE_SUB(d, INTERVAL 1 DAY))
    WHERE d > DATE '2026-08-01'
      AND corr < 0.02
      AND bad >= 0.010          -- only valid orders get corrected
  ),
  events AS (
    SELECT order_id, customer_id, country, items, amount, order_ts, updated_at FROM base
    UNION ALL
    SELECT order_id, customer_id, country, items, amount, order_ts, updated_at FROM base WHERE dup < 0.005
    UNION ALL
    SELECT order_id, customer_id, country, items, amount, order_ts, updated_at FROM corrections
  )
  SELECT
    order_id, customer_id, country, items, amount,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', order_ts)   AS order_ts,
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', updated_at) AS updated_at
  FROM events
);
