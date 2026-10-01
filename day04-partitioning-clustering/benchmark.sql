-- Day 4: the same three questions against each layout.
-- Run each with --nouse_cache, then read ACTUAL bytes from the job
-- (bq show -j JOB_ID -> statistics.query.totalBytesProcessed).
-- The dry-run estimate is exact for partition pruning but cannot see
-- clustering: it reported 126.9 MB for a query that really scanned 5.4 MB.

-- A: one month of dates  -> partition pruning, 128.2 MB -> 3.1 MB
SELECT COUNT(*) AS n, AVG(score) AS avg_score
FROM day4.t_part            -- swap: t_plain / t_part / t_part_clust / t_clust
WHERE creation_date >= TIMESTAMP('2017-03-01')
  AND creation_date <  TIMESTAMP('2017-04-01');

-- B: one user, no date filter  -> clustering, 126.9 MB -> 5.4 MB on t_clust
--    (and ~100x MORE slot time on t_part: it must open all 1,461 partitions)
SELECT COUNT(*) AS n, AVG(score) AS avg_score
FROM day4.t_clust
WHERE owner_user_id = 1223975;

-- C: a year + one user -> clustering inside daily partitions barely helps
--    (48.1 MB vs 47.7 MB): each partition is too small to hold many blocks
SELECT COUNT(*) AS n, AVG(score) AS avg_score
FROM day4.t_part_clust
WHERE creation_date >= TIMESTAMP('2017-01-01')
  AND creation_date <  TIMESTAMP('2018-01-01')
  AND owner_user_id = 1223975;
