-- Day 5: cost analysis from job metadata. `region-us` = where the jobs ran.

-- 1. my ten most expensive queries
SELECT
  FORMAT_TIMESTAMP('%m-%d %H:%M', creation_time)          AS run_at,
  ROUND(total_bytes_billed / POW(1024, 2), 1)            AS mb_billed,
  ROUND(total_bytes_billed / POW(1024, 4) * 6.25, 4)     AS usd_on_demand,
  total_slot_ms,
  cache_hit,
  SUBSTR(REGEXP_REPLACE(query, r'\s+', ' '), 1, 60)      AS query_start
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY' AND state = 'DONE' AND total_bytes_billed > 0
ORDER BY total_bytes_billed DESC
LIMIT 10;

-- 2. everything so far, against the 1 TiB monthly free tier
SELECT
  COUNT(*)                                                AS queries,
  ROUND(SUM(total_bytes_billed) / POW(1024, 3), 2)       AS gib_billed,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 100, 2) AS pct_of_free_tier,
  ROUND(SUM(total_slot_ms) / 1000 / 3600, 2)             AS slot_hours
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY' AND state = 'DONE';

-- 3. bytes billed vs compute: slot time per MB
SELECT
  SUBSTR(REGEXP_REPLACE(query, r'\s+', ' '), 1, 45)           AS q,
  ROUND(total_bytes_billed / POW(1024, 2))                    AS mb_billed,
  ROUND(total_slot_ms / 1000)                                 AS slot_sec,
  ROUND(total_slot_ms / NULLIF(total_bytes_billed / POW(1024, 2), 0)) AS slot_ms_per_mb
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY' AND state = 'DONE' AND total_bytes_billed > 100000000
ORDER BY slot_ms_per_mb DESC
LIMIT 6;

-- 4. exact vs approximate distinct count (compare slot time in the job stats)
SELECT COUNT(DISTINCT owner_user_id)        AS exact  FROM day4.t_plain;
SELECT APPROX_COUNT_DISTINCT(owner_user_id) AS approx FROM day4.t_plain;

-- 5. storage: logical vs physical, active vs long-term.
--    TABLE_STORAGE has to be switched on first (data backfills over ~a day):
ALTER PROJECT `gcp-learning-507108`
SET OPTIONS (`region-us.enable_info_schema_storage` = TRUE);

SELECT
  table_name,
  ROUND(active_logical_bytes    / POW(1024, 2), 1) AS active_logical_mb,
  ROUND(long_term_logical_bytes / POW(1024, 2), 1) AS longterm_mb,
  ROUND(total_physical_bytes    / POW(1024, 2), 1) AS physical_mb,
  ROUND(total_logical_bytes / NULLIF(total_physical_bytes, 0), 2) AS compression_x
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_schema = 'day4'
ORDER BY active_logical_mb DESC;

-- 6. join order: write it both ways, compare slot time
CREATE OR REPLACE TABLE day4.f_uni  AS SELECT id AS k, score FROM day4.t_plain;
CREATE OR REPLACE TABLE day4.f_skew AS SELECT IF(MOD(id, 10) > 0, 1, id) AS k, score FROM day4.t_plain;  -- 90% one key
CREATE OR REPLACE TABLE day4.dim_k  AS SELECT DISTINCT k FROM day4.f_skew;

SELECT COUNT(*) FROM day4.f_uni f JOIN day4.dim_k d ON f.k = d.k;
SELECT COUNT(*) FROM day4.dim_k d JOIN day4.f_uni f ON f.k = d.k;
