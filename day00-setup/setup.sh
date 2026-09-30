#!/usr/bin/env bash
# Day 0: authenticate, point the CLI at the project, switch on the APIs used later.
set -euo pipefail
PROJECT=gcp-learning-507108
REGION=us-central1

gcloud auth login                         # credentials for me: gcloud, bq
gcloud auth application-default login     # credentials for code: Terraform, Python clients
gcloud auth application-default set-quota-project "$PROJECT"

gcloud config set project "$PROJECT"
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "${REGION}-a"

# enabling an API is free; you pay only for what you create
gcloud services enable \
  bigquery.googleapis.com bigquerystorage.googleapis.com storage.googleapis.com \
  compute.googleapis.com dataproc.googleapis.com dataflow.googleapis.com \
  pubsub.googleapis.com datastream.googleapis.com cloudbuild.googleapis.com \
  cloudresourcemanager.googleapis.com iam.googleapis.com

# billing export lands here (then: Console -> Billing -> Billing export -> BigQuery export)
bq --location="$REGION" mk --dataset "$PROJECT:billing_export"

# free: validates the query and reports the bytes it would scan, runs nothing
bq query --dry_run --use_legacy_sql=false \
  'SELECT word, SUM(word_count) AS n
   FROM `bigquery-public-data.samples.shakespeare`
   GROUP BY word ORDER BY n DESC LIMIT 10'
