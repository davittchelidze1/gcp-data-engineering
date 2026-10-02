-- Day 6: row-level security and an authorized view.
-- Replace the principals with your own.

-- ---------- row-level security ----------
CREATE OR REPLACE ROW ACCESS POLICY batumi_only ON day6.dim_customer
GRANT TO ('serviceAccount:day1-analyst@gcp-learning-507108.iam.gserviceaccount.com')
FILTER USING (city = 'Batumi');

-- Once ANY row policy exists, everyone is filtered - including the owner, who
-- saw 0 rows until this one was added:
CREATE OR REPLACE ROW ACCESS POLICY owner_sees_all ON day6.dim_customer
GRANT TO ('user:you@example.com')
FILTER USING (TRUE);

GRANT `roles/bigquery.dataViewer` ON SCHEMA day6
TO 'serviceAccount:day1-analyst@gcp-learning-507108.iam.gserviceaccount.com';

-- result: owner sees 7 rows, the service account sees the 3 Batumi rows

-- ---------- authorized view ----------
-- the view lives in its own dataset; the analyst gets that dataset, not the raw one
CREATE OR REPLACE VIEW day6_views.v_customers AS
SELECT customer_id, name, tier
FROM day6.dim_customer
WHERE is_current;

REVOKE `roles/bigquery.dataViewer` ON SCHEMA day6
FROM 'serviceAccount:day1-analyst@gcp-learning-507108.iam.gserviceaccount.com';

GRANT `roles/bigquery.dataViewer` ON SCHEMA day6_views
TO 'serviceAccount:day1-analyst@gcp-learning-507108.iam.gserviceaccount.com';

-- Then authorize the view on the RAW dataset, so it reads on its own behalf.
-- That's an edit of day6's access list:
--
--   bq show --format=prettyjson PROJECT:day6 \
--     | jq '.access += [{"view": {"projectId": "PROJECT", "datasetId": "day6_views", "tableId": "v_customers"}}]' \
--     > day6.json
--   bq update --source=day6.json PROJECT:day6
--
-- result, as the service account: the view works, the base table is Access Denied
-- (the row policy still applies through the view: it returned Bob and Carol).
