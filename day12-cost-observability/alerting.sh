#!/usr/bin/env bash
# Day 12: log-based metric -> alert policy -> email channel.
set -euo pipefail
PROJECT=gcp-learning-507108
EMAIL="you@example.com"
API="https://monitoring.googleapis.com/v3/projects/$PROJECT"
AUTH="Authorization: Bearer $(gcloud auth print-access-token)"

# 1. count BigQuery jobs that billed more than 100 MB
gcloud logging metrics create big_bq_queries \
  --description="BigQuery jobs billing over 100MB" \
  --log-filter='resource.type="bigquery_project" AND protoPayload.metadata.jobChange.job.jobStats.queryStats.totalBilledBytes > 100000000'

# 2. where alerts go (REST: gcloud's alpha/beta components need an interactive install)
CHANNEL=$(curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" "$API/notificationChannels" \
  -d "{\"type\": \"email\", \"displayName\": \"me\", \"labels\": {\"email_address\": \"$EMAIL\"}}" \
  | jq -r .name)

# 3. alert whenever the metric is above zero in a 5-minute window
curl -s -X POST -H "$AUTH" -H "Content-Type: application/json" "$API/alertPolicies" -d @- <<EOF
{
  "displayName": "Expensive BigQuery query detected",
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": ["$CHANNEL"],
  "conditions": [{
    "displayName": "BigQuery job billed over 100MB",
    "conditionThreshold": {
      "filter": "metric.type=\"logging.googleapis.com/user/big_bq_queries\" AND resource.type=\"bigquery_project\"",
      "comparison": "COMPARISON_GT",
      "thresholdValue": 0,
      "duration": "0s",
      "aggregations": [{"alignmentPeriod": "300s", "perSeriesAligner": "ALIGN_SUM"}]
    }
  }]
}
EOF
