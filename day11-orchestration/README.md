# Day 11 — Orchestration, and choosing the right service

**Hadoop equivalent:** Airflow or Oozie on your cluster. Cloud Composer *is*
Airflow, hosted — and at ~$300/month, billing while idle, it's the fastest way to
burn trial credits. So I ran the same Airflow locally in Docker, for $0.

## The DAG — [dags/gcp_demo.py](dags/gcp_demo.py)
`start → build_daily_summary → quality_gate → end`, running against real
BigQuery with my own credentials mounted into the container:
- `BigQueryInsertJobOperator` builds a summary table — the operator you'd use on Composer
- a quality gate reads it back and fails the run if it's empty

All four tasks green. The one fix it needed: the BigQuery hook defaults to the
`US` location, and the dataset lives in `us-central1` — so every hook and
operator gets an explicit `location`.

```bash
docker run --rm \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT="google-cloud-platform://" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/.config/gcloud/application_default_credentials.json \
  -v "$PWD/dags:/opt/airflow/dags" \
  -v "$HOME/.config/gcloud:/home/airflow/.config/gcloud" \
  apache/airflow:2.10.5 \
  bash -c "airflow db migrate && airflow dags test gcp_daily_orders 2026-09-08"
```

A one-shot container like this is lighter than `airflow standalone`, which also
runs a webserver and scheduler — on my laptop, standalone eventually froze Docker.

## Choosing the service — [service-selection.md](service-selection.md)
The decision tree the plan calls "most of the interview", with the deciding
question for each branch. The short version:

| Need | Service | Hadoop world |
|---|---|---|
| SQL analytics over history | BigQuery | Hive |
| key lookups at huge scale | Bigtable | HBase |
| existing Spark code | Dataproc | your Spark cluster |
| event-time streaming | Dataflow | Flink / Spark Streaming |
| messaging | Pub/Sub | Kafka |
| database CDC | Datastream | Sqoop, but continuous |
| a schedule, one query | Cloud Scheduler | cron |
| real DAGs, backfills | Composer | Airflow |

**Batch unless freshness changes a decision.**
