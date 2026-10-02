-- Day 6: materialized view, SQL UDF, stored procedure.

CREATE MATERIALIZED VIEW day6.mv_tier_counts AS
SELECT tier, COUNT(*) AS n, MIN(valid_from) AS first_seen
FROM day6.dim_customer
GROUP BY tier;

CREATE OR REPLACE FUNCTION day6.tier_rank(t STRING) AS (
  CASE t WHEN 'PLATINUM' THEN 4 WHEN 'GOLD' THEN 3 WHEN 'SILVER' THEN 2 ELSE 1 END
);

SELECT name, tier, day6.tier_rank(tier) AS rank
FROM day6.dim_customer
WHERE is_current
ORDER BY rank DESC;

CREATE OR REPLACE PROCEDURE day6.sp_tier_summary(IN min_rank INT64)
BEGIN
  SELECT tier, COUNT(*) AS n
  FROM day6.dim_customer
  WHERE is_current AND day6.tier_rank(tier) >= min_rank
  GROUP BY tier
  ORDER BY tier;
END;

CALL day6.sp_tier_summary(3);   -- GOLD 1, PLATINUM 1
