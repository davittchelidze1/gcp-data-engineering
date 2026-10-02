#!/usr/bin/env bash
# Day 7: Spark on Dataproc Serverless, a TTL'd cluster, and the BigLake connection.
set -euo pipefail
PROJECT=gcp-learning-507108
REGION=us-central1
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# ---- prerequisites ----
gcloud compute networks subnets update default --region="$REGION" --enable-private-ip-google-access
gcloud storage buckets create "gs://$PROJECT-dataproc" --location="$REGION" --uniform-bucket-level-access
gcloud storage buckets create "gs://$PROJECT-lake"     --location="$REGION" --uniform-bucket-level-access
for role in roles/dataproc.worker roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$COMPUTE_SA" --role="$role" --condition=None
done

# ---- serverless: no cluster to create, nothing to forget ----
gcloud storage cp job.py "gs://$PROJECT-dataproc/jobs/job.py"
gcloud dataproc batches submit pyspark "gs://$PROJECT-dataproc/jobs/job.py" \
  --batch=day7-rollup-1 --region="$REGION" --version=2.2 \
  --deps-bucket="gs://$PROJECT-dataproc"

# ---- cluster mode: ALWAYS with auto-delete ----
gcloud dataproc clusters create day7-cluster --region="$REGION" --single-node \
  --master-machine-type=e2-standard-2 --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 --no-address --bucket="$PROJECT-dataproc" \
  --max-idle=30m --max-age=2h
gcloud dataproc jobs submit spark --cluster=day7-cluster --region="$REGION" \
  --class=org.apache.spark.examples.SparkPi \
  --jars=file:///usr/lib/spark/examples/jars/spark-examples.jar -- 200
gcloud dataproc clusters delete day7-cluster --region="$REGION" --quiet   # don't wait for the TTL

# ---- BigLake connection (it gets its own service account) ----
bq mk --connection --location="$REGION" --project_id="$PROJECT" \
  --connection_type=CLOUD_RESOURCE biglake-conn
CONN_SA=$(bq --format=json show --connection "$PROJECT.$REGION.biglake-conn" | jq -r .cloudResource.serviceAccountId)
gcloud storage buckets add-iam-policy-binding "gs://$PROJECT-lake" \
  --member="serviceAccount:$CONN_SA" --role=roles/storage.objectAdmin        # Iceberg writes too
# then run lakehouse.sql
