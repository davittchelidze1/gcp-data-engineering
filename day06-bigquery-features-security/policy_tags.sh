#!/usr/bin/env bash
# Day 6: column-level security with a policy tag.
# gcloud has no command to CREATE a taxonomy, so this uses the Data Catalog REST API.
set -euo pipefail
PROJECT=gcp-learning-507108
LOC=us-central1
ME="user:you@example.com"
API="https://datacatalog.googleapis.com/v1"
AUTH="Authorization: Bearer $(gcloud auth print-access-token)"

gcloud services enable datacatalog.googleapis.com

TAXONOMY=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  "$API/projects/$PROJECT/locations/$LOC/taxonomies" \
  -d '{"displayName": "pii-taxonomy", "activatedPolicyTypes": ["FINE_GRAINED_ACCESS_CONTROL"]}' \
  | jq -r .name)

TAG=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" \
  "$API/$TAXONOMY/policyTags" -d '{"displayName": "pii-name"}' | jq -r .name)

# attach the tag to the `name` column
bq show --format=prettyjson day6.dim_customer \
  | jq --arg tag "$TAG" '.schema.fields
      | map(if .name == "name" then . + {policyTags: {names: [$tag]}} else . end)' > schema.json
bq update --schema=schema.json day6.dim_customer

# even the project OWNER is now denied:
#   "User has neither fine-grained reader nor masked get permission ... on column day6.dim_customer.name"
bq query --use_legacy_sql=false "SELECT customer_id, name FROM day6.dim_customer LIMIT 2" || true
bq query --use_legacy_sql=false "SELECT customer_id, city FROM day6.dim_customer LIMIT 2"   # untagged: fine

# column access is its own permission
gcloud data-catalog taxonomies policy-tags add-iam-policy-binding "$TAG" --location="$LOC" \
  --member="$ME" --role=roles/datacatalog.categoryFineGrainedReader
