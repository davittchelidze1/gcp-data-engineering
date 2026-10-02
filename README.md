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
| 02 | [Cloud Storage](day02-cloud-storage/) | HDFS | rename is a copy: 0.227 s per file to commit |
| 03 | [BigQuery architecture](day03-bigquery-architecture/) | Hive, with no cluster to size | `SELECT *` cost 217x one column |
| 04 | [Partitioning & clustering](day04-partitioning-clustering/) | Hive partitions & bucketing | 41x less scanned; wrong partition key 100x slower |
| 05 | [BigQuery cost](day05-bigquery-cost/) | YARN job history | approx distinct: 4.4x less compute, 0.049% error |
| 06 | [MERGE, recovery, security, ML](day06-bigquery-features-security/) | Hive MERGE + Ranger | SCD2 in one MERGE; row, column and view security |
| 07 | [Dataproc, BigLake, Iceberg](day07-dataproc/) | your Spark cluster | serverless Spark job in 135 s, no cluster |
| 08 | [Pub/Sub](day08-pubsub/) | Kafka | dead-lettered after 5 attempts; replay by timestamp |
