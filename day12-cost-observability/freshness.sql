-- Day 12: which tables have missed their freshness SLA?
-- __TABLES__ is per-dataset metadata: last_modified_time is epoch milliseconds.
SELECT
  table_id,
  TIMESTAMP_MILLIS(last_modified_time)                                           AS last_updated,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(last_modified_time), MINUTE) AS minutes_stale,
  IF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(last_modified_time), MINUTE) > 60,
     'STALE - ALERT', 'ok')                                                       AS status
FROM day6.__TABLES__
ORDER BY minutes_stale DESC;
