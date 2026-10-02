-- Day 6: SCD Type 2 dimension maintained with one MERGE.

CREATE OR REPLACE TABLE day6.dim_customer (
  sk STRING, customer_id INT64, name STRING, city STRING, tier STRING,
  row_hash STRING, valid_from DATE, valid_to DATE, is_current BOOL
);

-- initial load from staging (day 1 file)
INSERT INTO day6.dim_customer
SELECT GENERATE_UUID(), customer_id, name, city, tier,
       TO_HEX(MD5(CONCAT(name, '|', city, '|', tier))),
       DATE '2026-01-01', DATE '9999-12-31', TRUE
FROM day6.stg_customers;

-- ...reload day6.stg_customers with the day 2 file, then:
MERGE day6.dim_customer AS d
USING (
  -- rows to INSERT as new versions (new customers + changed ones):
  -- a NULL join key guarantees they never match
  SELECT CAST(NULL AS INT64) AS join_key, s.customer_id, s.name, s.city, s.tier, s.h
  FROM (SELECT *, TO_HEX(MD5(CONCAT(name, '|', city, '|', tier))) AS h
        FROM day6.stg_customers) AS s
  LEFT JOIN day6.dim_customer AS c
    ON c.customer_id = s.customer_id AND c.is_current
  WHERE c.customer_id IS NULL OR c.row_hash != s.h

  UNION ALL

  -- rows whose CURRENT version must be EXPIRED: the real key, so they match
  SELECT s.customer_id AS join_key, s.customer_id, s.name, s.city, s.tier, s.h
  FROM (SELECT *, TO_HEX(MD5(CONCAT(name, '|', city, '|', tier))) AS h
        FROM day6.stg_customers) AS s
  JOIN day6.dim_customer AS c
    ON c.customer_id = s.customer_id AND c.is_current
  WHERE c.row_hash != s.h
) AS src
ON d.customer_id = src.join_key AND d.is_current
WHEN MATCHED THEN
  UPDATE SET valid_to = DATE '2026-01-02', is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (sk, customer_id, name, city, tier, row_hash, valid_from, valid_to, is_current)
  VALUES (GENERATE_UUID(), src.customer_id, src.name, src.city, src.tier, src.h,
          DATE '2026-01-02', DATE '9999-12-31', TRUE);

-- point in time: the dimension as it was on 2026-01-01
SELECT customer_id, name, city, tier
FROM day6.dim_customer
WHERE DATE '2026-01-01' >= valid_from AND DATE '2026-01-01' < valid_to
ORDER BY customer_id;
