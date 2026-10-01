#!/usr/bin/env bash
# Day 3: run the join, then read its execution plan from the job metadata.
set -euo pipefail

bq query --use_legacy_sql=false --nouse_cache "$(cat join.sql)"
JOB=$(bq --format=json ls -j --max_results=1 | jq -r '.[0].jobReference.jobId')

bq --format=prettyjson show -j "$JOB" | jq '{
  bytes_processed: .statistics.query.totalBytesProcessed,
  slot_ms:         .statistics.query.totalSlotMs,
  wall_ms:         ((.statistics.endTime | tonumber) - (.statistics.startTime | tonumber)),
  stages: [.statistics.query.queryPlan[] | {
    name, workers: .parallelInputs,
    rows_in: .recordsRead, rows_out: .recordsWritten,
    shuffle_bytes: .shuffleOutputBytes,
    skew: (((.computeMsMax | tonumber) / ((.computeMsAvg | tonumber) + 0.0001)) * 10 | round / 10)
  }]
}'
