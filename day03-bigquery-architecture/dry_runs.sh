#!/usr/bin/env bash
# Day 3: what each query WOULD scan. Dry runs are free and run nothing.
set -euo pipefail
T='`bigquery-public-data.stackoverflow.posts_questions`'

while IFS='|' read -r label sql; do
  bytes=$(bq --format=json query --dry_run --use_legacy_sql=false "$sql" \
          | jq -r '.statistics.totalBytesProcessed')
  printf '%-34s %8.2f GB\n' "$label" "$(echo "$bytes / 1073741824" | bc -l)"
done <<EOF
SELECT *|SELECT * FROM $T
SELECT * LIMIT 10|SELECT * FROM $T LIMIT 10
SELECT id|SELECT id FROM $T
SELECT id, title|SELECT id, title FROM $T
SELECT body|SELECT body FROM $T
COUNT(*)|SELECT COUNT(*) FROM $T
SELECT id WHERE score > 100|SELECT id FROM $T WHERE score > 100
EOF
