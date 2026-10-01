-- Day 4: four identical copies of 8.4M Stack Overflow questions (2015-2018),
-- differing only in physical layout.
--
-- The `day4` dataset must be in the US multi-region: the public datasets live
-- there, and BigQuery cannot query across locations. (US-CENTRAL1 is a
-- different location from US - the error you get is a misleading "Access Denied".)

CREATE OR REPLACE TABLE day4.t_plain AS
SELECT id, creation_date, owner_user_id, score, answer_count, view_count, tags
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE creation_date >= TIMESTAMP('2015-01-01')
  AND creation_date <  TIMESTAMP('2019-01-01');

CREATE OR REPLACE TABLE day4.t_part
PARTITION BY DATE(creation_date)
AS SELECT * FROM day4.t_plain;

CREATE OR REPLACE TABLE day4.t_part_clust
PARTITION BY DATE(creation_date)
CLUSTER BY owner_user_id
AS SELECT * FROM day4.t_plain;

CREATE OR REPLACE TABLE day4.t_clust
CLUSTER BY owner_user_id
AS SELECT * FROM day4.t_plain;

-- the guardrail: refuse any query that would scan every partition
ALTER TABLE day4.t_part SET OPTIONS (require_partition_filter = TRUE);

-- how many partitions? (hard limit: 4,000 per table)
SELECT COUNT(*) AS partitions, MIN(partition_id) AS first, MAX(partition_id) AS last
FROM day4.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 't_part';

-- nested and repeated: tags as an ARRAY inside the row
CREATE OR REPLACE TABLE day4.t_nested AS
SELECT id, creation_date, owner_user_id, score, SPLIT(tags, '|') AS tags_arr
FROM day4.t_plain
WHERE tags IS NOT NULL;

-- UNNEST is a join with no shuffle: the list is already inside the row
SELECT tag, COUNT(*) AS n
FROM day4.t_nested, UNNEST(tags_arr) AS tag
GROUP BY tag
ORDER BY n DESC
LIMIT 8;
