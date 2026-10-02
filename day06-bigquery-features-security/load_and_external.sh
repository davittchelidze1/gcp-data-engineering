#!/usr/bin/env bash
# Day 6: a batch load (copies data in) vs an external table (queries it in place).
set -euo pipefail
PROJECT=gcp-learning-507108
B="gs://$PROJECT-day1/day6"

bq --location=us-central1 mk --dataset "$PROJECT:day6"

printf 'customer_id,name,city,tier,updated_at
1,Alice,Tbilisi,SILVER,2026-01-01
2,Bob,Batumi,GOLD,2026-01-01
3,Carol,Kutaisi,SILVER,2026-01-01
4,Dave,Rustavi,BRONZE,2026-01-01
' | gcloud storage cp - "$B/customers_day1.csv"

# load job: data is copied into BigQuery storage
bq load --source_format=CSV --autodetect --replace \
  "$PROJECT:day6.stg_customers" "$B/customers_day1.csv"

# external table: data stays in GCS, read at query time
bq query --use_legacy_sql=false "
CREATE OR REPLACE EXTERNAL TABLE day6.ext_customers
OPTIONS (format = 'CSV', uris = ['$B/customers_day1.csv'], skip_leading_rows = 1)"

bq query --use_legacy_sql=false "SELECT COUNT(*) FROM day6.ext_customers"
bq query --dry_run --use_legacy_sql=false "SELECT * FROM day6.ext_customers"   # can't be estimated: 0 bytes
