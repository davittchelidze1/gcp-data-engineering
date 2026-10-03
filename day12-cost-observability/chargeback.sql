-- Day 12: cost per pipeline, from job labels.
-- Jobs are labelled at submission: bq query --label=pipeline:orders_etl --label=owner:me ...
SELECT
  (SELECT value FROM UNNEST(labels) WHERE key = 'pipeline') AS pipeline,
  COUNT(*)                                                AS jobs,
  ROUND(SUM(total_bytes_billed) / POW(1024, 2), 1)       AS mb_billed,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 6) AS usd_on_demand,
  SUM(total_slot_ms)                                      AS slot_ms
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY' AND state = 'DONE'
  AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'pipeline')
GROUP BY pipeline
ORDER BY usd_on_demand DESC;
