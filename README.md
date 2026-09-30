# GCP data engineering, hands-on

Moving from the Hadoop stack (HDFS, Hive, Spark, YARN, Airflow) to Google Cloud,
one topic per day. Every day runs something real on a live GCP project and
records what was measured — not what the docs say should happen.

The last day puts it all together: an incremental batch lakehouse
(GCS → Spark on Dataproc Serverless → BigQuery), orchestrated with Airflow,
provisioned with Terraform, and reconciled against ground truth on ~31M records.

Everything ran on the GCP free tier plus trial credits.

| Day | Topic | Hadoop equivalent | Key result |
|---|---|---|---|
| 00 | [Setup & cost guardrails](day00-setup/) | cluster provisioning | budget alerts, billing export, free dry runs |
| 01 | [IAM & service accounts](day01-iam/) | Ranger + Kerberos principals | service account reads one dataset, denied the other |
