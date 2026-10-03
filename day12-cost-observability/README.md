# Day 12 — Cost attribution and observability

**Hadoop equivalent:** YARN queue chargeback plus Ganglia/ResourceManager
dashboards — except here every signal is a table or a metric you can query and
alert on.

## Chargeback by pipeline — [chargeback.sql](chargeback.sql)
Tag every job with a label (`bq query --label=pipeline:orders_etl …`), then cost
is a `GROUP BY`:

| pipeline | jobs | MB billed | slot ms |
|---|---|---|---|
| orders_etl | 2 | 65.0 | 345 |
| customer_dim | 1 | 10.0 | 64 |

Two sources, two questions: **the billing export** says what you spent;
**`INFORMATION_SCHEMA.JOBS`** says why.

## What the whole 14 days cost — [billing.sql](billing.sql)
Measured from the billing export, not estimated:

| Service | Cost | Paid by |
|---|---|---|
| Dataproc | $0.152 | trial credits |
| Compute Engine | $0.009 | trial credits |
| Dataflow | $0.006 | trial credits |
| BigLake | $0.001 | trial credits |
| Networking | $0.063 | free-tier discount |
| **Card** | **$0.00** | |

Every BigQuery query, all storage, Pub/Sub, Beam and Airflow fell inside the
free tier. Dataproc was nearly all of it — and it stayed small because every
Spark job ran serverless, billed only while running.

## Freshness monitor — [freshness.sql](freshness.sql)
Flags any table that hasn't been written within its SLA (60 minutes here) — the
cloud version of checking that a `_SUCCESS` marker arrived on time. This is the
alert that actually pages someone.

## Alerting — [alerting.sh](alerting.sh)
A chain of three: a **log-based metric** counting BigQuery jobs that billed
over 100 MB → an **alert policy** on that metric → an **email notification
channel**. The channel and policy go through the Monitoring REST API, because
`gcloud alpha` components can't be installed non-interactively.
