#!/usr/bin/env bash
# Day 1: least-privilege service account, proven by impersonating it.
set -euo pipefail
PROJECT=gcp-learning-507108
ME="user:you@example.com"                                  # your own account
SA="day1-analyst@${PROJECT}.iam.gserviceaccount.com"

# ---- two datasets: one the analyst may read, one it may not ----
bq --location=us-central1 mk --dataset "$PROJECT:sales_data"
bq --location=us-central1 mk --dataset "$PROJECT:hr_data"
bq query --use_legacy_sql=false "CREATE OR REPLACE TABLE sales_data.orders AS
  SELECT 1 AS order_id, 'widget' AS item, 9.99 AS price
  UNION ALL SELECT 2, 'gadget', 19.99 UNION ALL SELECT 3, 'doohickey', 4.50"
bq query --use_legacy_sql=false "CREATE OR REPLACE TABLE hr_data.salaries AS
  SELECT 'alice' AS employee, 120000 AS salary UNION ALL SELECT 'bob', 95000"

gcloud storage buckets create "gs://$PROJECT-day1" --location=us-central1 --uniform-bucket-level-access
printf 'order_id,item\n1,widget\n' | gcloud storage cp - "gs://$PROJECT-day1/sample.csv"

# ---- the identity ----
gcloud iam service-accounts create day1-analyst --display-name="Day 1 least-privilege analyst"

# project: may run query jobs (grants no data access on its own)
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA" --role=roles/bigquery.jobUser --condition=None

# dataset: READER on sales_data only, through the dataset's access list
bq show --format=prettyjson "$PROJECT:sales_data" \
  | jq --arg sa "$SA" '.access += [{"role": "READER", "userByEmail": $sa}]' > /tmp/sales_data.json
bq update --source=/tmp/sales_data.json "$PROJECT:sales_data"

# bucket: read objects in this one bucket
gcloud storage buckets add-iam-policy-binding "gs://$PROJECT-day1" \
  --member="serviceAccount:$SA" --role=roles/storage.objectViewer

# the SA itself: allow me to impersonate it
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --member="$ME" --role=roles/iam.serviceAccountTokenCreator

# ---- become the service account and try both doors ----
gcloud config set auth/impersonate_service_account "$SA"
bq query --use_legacy_sql=false "SELECT item, price FROM sales_data.orders"   # works
bq query --use_legacy_sql=false "SELECT * FROM hr_data.salaries" || true      # Access Denied
gcloud storage cat "gs://$PROJECT-day1/sample.csv"                             # works
gcloud storage ls || true                                                       # 403: no storage.buckets.list
gcloud config unset auth/impersonate_service_account

# ---- audit: the four questions ----
gcloud projects get-iam-policy "$PROJECT" --format="table(bindings.role)"          # which roles exist here?
gcloud projects get-iam-policy "$PROJECT" --flatten="bindings[].members" \
  --filter="bindings.members:day1-analyst" --format="value(bindings.role)"        # what can the SA do?
gcloud iam service-accounts get-iam-policy "$SA"                                   # who can become it?
gcloud iam service-accounts keys list --iam-account="$SA" --managed-by=user        # downloadable keys? (none)
