# Day 14 — Capstone: an incremental batch lakehouse

**Hadoop equivalent:** the classic raw zone → Spark ETL → Hive tables → BI
pipeline, with HDFS replaced by Cloud Storage, the cluster by Dataproc
Serverless, and Hive by BigQuery.

Raw order events land in GCS partitioned by ingest date. Each day, Spark on
Dataproc Serverless reads **only that day's partition**, validates and
deduplicates it into Parquet, and BigQuery rebuilds **only the order dates the
new data touched**. Provisioned with Terraform, orchestrated with Airflow.

Measured on ~31M synthetic events (31 days × ~1M orders) with realistic defects:
late arrivals, corrections, replays, invalid rows, and one malformed producer file.

## Architecture

```
                 ┌──────────────── Airflow: orders_platform, @daily ─────────────────┐
                 │  logical date {{ ds }} = the ingest partition this run processes  │
                 └──┬─────────────┬────────────────┬──────────────┬───────────┬──────┘
               sensor          submit           ensure +        MERGE +      freshness
                 │                │             3 gates          ASSERT         SLA
                 ▼                ▼                 ▼               ▼
gs://…-bronze/orders/     Dataproc Serverless   BigLake table   gold_daily_revenue
  ingest_date=2026-08-31/ →  PySpark: that   →  silver_orders → partitioned by order_date
    part-*.json              partition only     partitioned by   clustered by country
  (append-only, raw)            │               ingest_date      only affected dates rebuilt
                                └──→ gs://…-silver/_quarantine/ingest_date=…/ (row + reason)
```

| Layer | Where | Partitioned by | Contents |
|---|---|---|---|
| bronze | `gs://…-bronze/orders/` | `ingest_date` | raw JSON exactly as received, never modified |
| silver | `gs://…-silver/orders/` (Parquet, BigLake table) | `ingest_date` | typed, valid, deduplicated within the day |
| quarantine | `gs://…-silver/_quarantine/orders/` | `ingest_date` | every rejected row with its `reject_reason` |
| gold | BigQuery `platform.gold_daily_revenue` | `order_date` | revenue by day × country, latest version of every order |

## Results (measured)

### Correctness — gold reconciled against independent ground truth

`sql/reconcile_truth.sql` recomputes what gold should contain directly from the
generator's definitions — never touching bronze, Spark, or silver — and compares
every cell:

| | |
|---|---|
| cells compared (36 order dates × 5 countries) | 180 |
| order-count mismatches | **0** |
| revenue mismatches over 1 cent | **0** (max difference 4 × 10⁻⁹) |
| orders | truth 30,689,827 = gold 30,689,827 |
| revenue | truth 7,750,082,488.76 = gold 7,750,082,488.76 |

### Backfill — Aug 1–30 in one Spark job

| | |
|---|---|
| rows in | 30,725,281 |
| rejected | 301,632 (0.98%) — negative amount 120,455 · zero items 90,654 · missing timestamp 60,204 · missing amount 30,319 |
| duplicates removed | 148,850 |
| silver rows | 30,274,799 |
| Spark | 300 s in-job · 402 s submit-to-finish · **1.121 DCU-hours** |
| gold MERGE | 35 order dates rebuilt · 13.5 s · 2,138 MB billed |

### Daily incremental run — Aug 31

| | |
|---|---|
| bronze read | **one partition, 166 MB** (of 5.4 GB) |
| rows in | 1,024,982 — incl. a 5-line broken producer file |
| rejected | 10,119 (0.99%) — incl. `malformed_json` 2, `bad_or_missing_items` 1 from the broken file |
| silver rows | 1,009,830 |
| Spark | 65 s in-job · 167 s batch · **0.283 DCU-hours** |
| order dates affected | **7** (Aug 25–31: late arrivals + corrections to Aug 30) |
| gold MERGE | 456 MB billed · 55.6 slot-seconds · scanned 7 of 31 silver partitions |
| gates + freshness | 63 MB billed |
| everything after Spark | 36 s |

### Old design vs new, per daily run

| | Old: full rescan | New: incremental |
|---|---|---|
| bronze read | all history — 5.4 GB and growing | one day — 166 MB |
| Spark compute | 1.121 DCU-h at 30 days of history | 0.283 DCU-h |
| gold rebuilt | every date | the 7 dates touched |
| cost as history grows | **grows linearly, forever** | **flat** |

Honest caveat: at 1M rows/day most of the daily job's 0.283 DCU-h is fixed
Serverless startup — inside the backfill, one day's marginal cost was
~0.04 DCU-h. At this volume the real win isn't today's bill; it's that the old
design got more expensive every single day and the new one doesn't.

### Idempotency

- Aug 1 was processed **twice** (test run, then inside the backfill): silver still holds exactly 990,012 rows — dynamic partition overwrite replaced it rather than appending.
- Re-running the Aug 31 gold MERGE left gold byte-for-byte identical in orders and revenue.
- A retried DAG run found the Spark batch already finished (`Batch with given id already exists`) and **did not pay for it again** — batch IDs are derived from the logical date.

## Design decisions, and what each one costs

**Bronze partitioned by ingest date, not event date.** An ingest partition is
immutable once written, so "process yesterday" is always exactly one directory.
Event-date partitioning would scatter every late arrival into old partitions,
and you'd need to rescan to find them — which is the bug this design removes.

**Read raw fields as strings, then `try_cast`.** Upstream encodings drift:
BigQuery's JSON export writes INT64 as strings (`"items":"4"`) but FLOAT64 as
numbers. A strict typed schema would fail on that silently. `try_cast` returns
NULL on bad input regardless of Spark's ANSI setting — a plain `cast` throws
under ANSI and one bad row would kill the whole job.

**Every row gets exactly one outcome.** A single `CASE` assigns a reject reason
or NULL. With two independent `filter()` calls — as in v1 — a row with a NULL
field can match neither and silently disappear.

**Circuit breaker at 5% rejects.** A feed that broken is an incident. The job
writes quarantine for diagnosis and exits non-zero without touching silver.

**Dedup within the day in Spark; across days in gold.** Silver stays a pure
function of one bronze partition, so it is identical no matter how days were
batched together. Corrections arriving on later days are resolved in the gold
MERGE with `QUALIFY ROW_NUMBER() … ORDER BY updated_at DESC`.

**Gold rebuilds only affected dates — with a provably bounded scan.** Two source
guarantees make it exact: an order is never ingested before it's placed
(`ingest_date >= order_date`), and `order_date` never changes between versions.
So every version of an affected order sits in an ingest partition
`>= MIN(affected)`. No upper bound is applied, so reprocessing an old day still
sees newer corrections and can't roll gold back in time.

**`ASSERT` after the MERGE.** Gold order counts must equal distinct silver orders
for the rebuilt dates, or the job fails before anyone reads a wrong number.

**`require_hive_partition_filter` on silver.** A query without an `ingest_date`
filter is refused. Every gate checks the new partition only, never all history.

**Sensor before compute; bulk backfill outside the DAG.** The DAG waits for the
bronze partition before submitting anything. Backfills run as one CLI job,
because each Serverless batch pays ~2 minutes of startup: one 30-day job cost
1.1 DCU-h, thirty daily jobs would cost ~8.5.

**Executor caps.** Serverless defaults to a ceiling of 1,000 executors. Daily
runs cap at 4, backfill at 10.

## Cost

**Measured, all of it:** 1.724 DCU-hours of Dataproc Serverless across three
batches. Generating 31M rows cost 0 BigQuery bytes (the generator reads no
tables). Gold backfill 2.1 GB, daily run ~0.5 GB.

**Estimated monthly at this volume** (1M orders/day), from the measured unit
costs at list price — *not yet confirmed against the billing export*:

| Component | Basis | ≈ $/month |
|---|---|---|
| Dataproc Serverless | 0.283 DCU-h × 30 runs at ~$0.06/DCU-h | ~0.50 |
| BigQuery | ~0.5 GB/day → 15 GB/month (inside the 1 TiB free tier) | 0 |
| GCS bronze | +5.2 GB/month of JSON; Nearline after 90 days | ~0.10, growing |
| GCS silver | +0.9 GB/month of Parquet | ~0.02, growing |
| **Total** | | **under $1** |
| Cloud Composer, if used | smallest environment, bills while idle | **~$300** |

The orchestrator is still the whole cost story: Composer would be several
hundred times everything else. For one daily DAG, Cloud Scheduler → Workflows or
a Cloud Run job does the same work for cents.

## What's still not done

- **Silver is an append log, not current state.** Cross-day dedup happens in the gold query. Iceberg with `MERGE INTO` would give a queryable current-state silver and atomic commits.
- **The external-table DDL runs inside the DAG.** It belongs in Terraform.
- **Volume gate thresholds are static** (500k–2M rows). They should be relative to a trailing average.
- **Quarantine partitions aren't cleared** if a re-run produces zero rejects for a day — dynamic overwrite only replaces partitions it writes.
- **The two source guarantees are enforced by the generator.** A real source would need a data contract and an alert when they're violated.
- **Malformed input was tested with 5 synthetic lines**, not a real broken producer.

## Reproducing

```bash
cd terraform && terraform init && terraform apply             # buckets, dataset, gold table, SA, IAM

python generate_bronze.py --from 2026-08-01 --to 2026-08-30   # 30.7M rows, ~1 min, 0 bytes billed

gcloud storage cp spark/bronze_to_silver.py gs://PROJECT-dataproc/jobs/
gcloud dataproc batches submit pyspark gs://PROJECT-dataproc/jobs/bronze_to_silver.py \
  --region=us-central1 --version=2.2 \
  --properties=spark.dynamicAllocation.maxExecutors=10 -- \
  --bronze=gs://PROJECT-bronze/orders --silver=gs://PROJECT-silver/orders \
  --quarantine=gs://PROJECT-silver/_quarantine/orders \
  --ingest_from=2026-08-01 --ingest_to=2026-08-30               # backfill

python tools/run_sql.py sql/silver_table.sql
python tools/run_sql.py sql/silver_to_gold.sql --param ingest_from=2026-08-01 --param ingest_to=2026-08-30

python generate_bronze.py --from 2026-08-31 --to 2026-08-31   # a new day arrives
airflow dags test orders_platform 2026-08-31                  # the daily run

python tools/run_sql.py sql/reconcile_truth.sql --param last_ingest=2026-08-31
```

## Layout

```
dags/orders_platform.py      the daily DAG
spark/bronze_to_silver.py    bronze -> silver + quarantine, one or many ingest dates
sql/generator.sql            deterministic synthetic source (BigQuery table functions)
sql/silver_table.sql         BigLake table over silver
sql/silver_to_gold.sql       incremental, idempotent gold MERGE + ASSERT
sql/reconcile_truth.sql      verification against independent ground truth
generate_bronze.py           exports one bronze partition per day, in parallel
tools/run_sql.py             runs a .sql file and reports bytes billed / slot time
terraform/                   infrastructure
```
