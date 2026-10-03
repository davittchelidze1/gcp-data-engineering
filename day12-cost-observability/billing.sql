-- Day 12: what was actually spent, from the billing export (set up on day 0).
-- Replace the table suffix with your own billing account's export table.

-- by service: gross cost, trial credits applied, net
SELECT
  service.description AS service,
  ROUND(SUM(cost), 4) AS gross_usd,
  ROUND(SUM((SELECT IFNULL(SUM(c.amount), 0) FROM UNNEST(credits) c WHERE c.type = 'PROMOTION')), 4) AS trial_credits,
  ROUND(SUM(cost) + SUM((SELECT IFNULL(SUM(c.amount), 0) FROM UNNEST(credits) c)), 4) AS net_usd
FROM `billing_export.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX`
GROUP BY service
HAVING gross_usd > 0
ORDER BY gross_usd DESC;

-- by day: spend should drop to zero once everything is torn down
SELECT DATE(usage_start_time) AS day, ROUND(SUM(cost), 4) AS gross_usd
FROM `billing_export.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX`
GROUP BY day
HAVING gross_usd > 0
ORDER BY day;
