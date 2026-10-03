#!/usr/bin/env bash
# Day 10: same pipeline, local first, then Dataflow. Uses the day 9 Beam venv.
set -euo pipefail
PROJECT=gcp-learning-507108

# 1. local, free
python pipeline.py --input events.jsonl --dataset day6 \
  --runner DirectRunner --project "$PROJECT" --temp_location "gs://$PROJECT-dataproc/tmp"

# 2. Dataflow - BATCH, so it stops by itself
gcloud storage cp events.jsonl "gs://$PROJECT-lake/day10/events.jsonl"
python pipeline.py --input "gs://$PROJECT-lake/day10/events.jsonl" --dataset day6 \
  --runner DataflowRunner --project "$PROJECT" --region us-central1 \
  --temp_location "gs://$PROJECT-dataproc/tmp" --staging_location "gs://$PROJECT-dataproc/staging" \
  --job_name day10-deadletter --max_num_workers 2 --machine_type e2-standard-2 --save_main_session

# 3. confirm nothing is still running
gcloud dataflow jobs list --region=us-central1 --status=active

bq query --use_legacy_sql=false "SELECT raw_line, error FROM day6.df_events_dead_letter"
