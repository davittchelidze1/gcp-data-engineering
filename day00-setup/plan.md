# GCP in 14 Days — for an experienced Hadoop/Spark/Airflow engineer

**Assumption:** you're strong in Spark, SQL, HDFS, Hive, Airflow. This plan skips all of that and covers only what GCP does differently.

**Time:** ~3 hours/day, or batch it into two solid weekends plus evenings.

**Goal:** be able to design and defend a GCP data platform, and pass a GCP-focused technical screen.

**Budget: $0 of your own money.** This plan is designed to run entirely on the Always Free tier plus the $300 trial credits. Read the box below before you touch anything.

---

# ⚠️ READ FIRST — How Not To Get Charged

## The four rules

**1. Never click "Activate full account" or "Upgrade account".**
This is the only button that can make GCP charge your card. Until you click it, your account is in trial mode: when credits run out, resources **pause** — they do not bill you. Google shows this prompt frequently and prominently. Ignore it every time, for all 90 days.

**2. Never create a Cloud Composer environment.**
~$300/month. Bills continuously even when idle, even with zero DAGs. This is the single fastest way to destroy $300 in credits. You already know Airflow — run it locally in Docker (Day 11).

**3. Never leave a Dataflow *streaming* job running.**
Batch jobs finish and stop. Streaming jobs run forever until you cancel them. A forgotten streaming job costs ~$5–15/day. After every Dataflow experiment: Console → Dataflow → Jobs → confirm nothing shows "Running".

**4. Always set auto-delete TTL on Dataproc clusters.**
`gcloud dataproc clusters create ... --max-idle=30m --max-age=2h`
A forgotten cluster bills by the minute. The TTL flag makes forgetting harmless.

## Why the card is safe

The $300 trial requires a payment method for identity verification. Debit cards generally work. **GCP does not auto-convert to paid billing** — this is genuinely different from AWS, where a forgotten resource quietly charges you. Your worst case in trial mode is exhausted credits and paused resources, not a bill.

## Understand what "free" means here

| Source | Amount | Runs out? |
|---|---|---|
| **Always Free tier** | 1 TiB BigQuery queries/mo, 10 GiB storage, 5 GB GCS, 10 GiB Pub/Sub | No — resets monthly, forever |
| **Trial credits** | $300 | Yes — 90 days or when spent |

Days 3–6 (BigQuery, the most important part) run **entirely on the Always Free tier**. Credits are only for Dataproc and one Dataflow run. Expected consumption: **$20–60 of the $300.**

The trap is that credits feel infinite until they aren't. Treat them as real money.

## End-of-session teardown (60 seconds, every single time)

- [ ] Dataproc → Clusters → **empty?**
- [ ] Dataflow → Jobs → **nothing "Running"?**
- [ ] Composer → Environments → **empty?** (should always be, per rule 2)
- [ ] Compute Engine → VM instances → **only the e2-micro, if any?**

## Billing checkpoints

- [ ] **Day 1** — Billing → Reports. Confirm $0 charged, credits intact. Confirm budget alerts fired a test.
- [ ] **Day 7** — Billing → Reports. Check credit burn rate. If you've used more than $30, find out what and kill it.
- [ ] **Day 14** — Final teardown. Delete the whole project when finished: `gcloud projects delete PROJECT_ID`

**If a budget alert ever fires:** stop, go to Billing → Reports → group by Service, find the line item, and delete that resource before continuing.

*Free-tier limits and trial terms change. Verify current numbers on Google's free-tier page rather than trusting this table.*

---

## Day 0 — Setup (30 minutes, do it now)

- [ ] New GCP project, free trial claimed ($300 / 90 days)
- [ ] Budget alerts at **$1 / $5 / $10** — you want to hear about spend immediately, not eventually
- [ ] **BigQuery → Query settings → max bytes billed = 100 GB.** Prevents the one expensive mistake everyone makes.
- [ ] Set all resources in a **US region** (`us-central1`) — the Always Free tier only applies there
- [ ] Enable billing export to a BigQuery dataset (you'll query it on Day 12)
- [ ] Install `gcloud`, `bq`, Terraform, Docker
- [ ] Enable APIs: BigQuery, Dataproc, Dataflow, Pub/Sub, Datastream
- [ ] Create a free **Google Cloud Skills Boost** account — you'll use its labs for the paid services

### The $0 rule
**Never click "Activate full account" / "Upgrade".** GCP does not auto-charge when the trial ends — it pauses your resources and waits for you to manually upgrade. The card on file is verification, not a billing risk. As long as you don't upgrade, your maximum possible spend is $0.

---

## The $0 Strategy

Three things stack to cover this entire plan for free.

### 1. Always Free tier (never expires)
Covers the whole BigQuery core of this plan:
- **BigQuery: 1 TiB query processing/month + 10 GiB storage** ← this is the important one
- GCS: 5 GB Standard storage, US regions only
- Pub/Sub: 10 GiB messages/month
- Compute Engine: 1× e2-micro, US regions
- Cloud Run / Cloud Functions: generous request tiers

1 TiB/month is far more than learning queries consume, especially with the 100 GB per-query cap.

### 2. $300 trial credits (90 days)
Reserve these for the services with no free tier. Spend them deliberately, not by accident.

### 3. Google Cloud Skills Boost labs
Labs run in a temporary sandbox project with pre-provisioned credentials — **you pay nothing for resources inside a lab**, including Composer, Dataflow, and Dataproc. Free credits come via the Cloud Innovators program, plus periodic free-access campaigns. This is your escape hatch for everything expensive.

### Services with NO free tier — and the free substitute

| Service | Why it costs | Free approach |
|---|---|---|
| **Cloud Composer** | ~$300/mo, bills even when idle | Run Airflow locally in Docker. You already know Airflow — Composer adds environment mechanics only. One Skills Boost lab covers it. **Do not create a Composer environment on your own project.** |
| **Dataflow** | Per-vCPU-hour, streaming jobs bill continuously | Learn Beam with the **local DirectRunner** — the entire model (windows, watermarks, triggers) works on your laptop at $0. Run on Dataflow once from trial credits, batch only, then stop it. |
| **Dataproc** | VM cost + premium | Single-node cluster from trial credits with **auto-delete TTL of 30 minutes**, or use Skills Boost labs. You know Spark; you're only learning submission mechanics. |
| **Datastream** | Per-GB processed | Skills Boost lab, or learn the CDC merge pattern conceptually and implement it against a fake change-log table in BigQuery. |

### Local substitutes worth setting up
- **Airflow**: `docker compose` with the official image
- **Beam**: `pip install apache-beam` → DirectRunner. Full model, zero cost.
- **Spark + Iceberg**: local Spark with Iceberg JARs against a local warehouse dir
- **GCS emulator**: `fake-gcs-server` for object-store API practice (though real GCS free tier is easier)

### Budget checkpoints
- **Day 7 and Day 14**: open Billing → Reports and confirm you're at $0 of your own money. Check for orphaned Dataproc clusters and running Dataflow jobs.
- If you ever see a running Dataflow streaming job you forgot about, that's the #1 way students burn credits.

---

# WEEK 1 — Storage, IAM, and BigQuery

## Day 1 — IAM and project model
The genuinely unfamiliar part. Ranger intuitions don't transfer.

- [ ] Resource hierarchy: org → folder → project → resource, and how policy inherits down
- [ ] Roles vs. permissions; primitive (`owner`/`editor`/`viewer`) vs. predefined vs. custom
- [ ] **Service accounts**: what they are, why they're the identity for every workload
- [ ] Service account **impersonation** (`--impersonate-service-account`) — the correct pattern
- [ ] Why you never download a JSON key, and what Workload Identity Federation replaces it with
- [ ] `gcloud` fluency: `config`, `auth`, `--format`, `--filter`, `--impersonate-service-account`

**Do:** create a service account with least-privilege access to one dataset and one bucket, then impersonate it and prove it can't read a second dataset.

---

## Day 2 — GCS, and where it isn't HDFS
- [ ] Storage classes: Standard / Nearline / Coldline / Archive; lifecycle rules; versioning
- [ ] **Object store semantics**: flat namespace, "directories" are a prefix illusion, no atomic rename
- [ ] Consequence for Spark: the commit protocol. Understand why `_temporary` rename-based commits are slow/unsafe on object stores and what GCS connector does instead.
- [ ] Strong consistency (GCS has it — unlike early S3; know this, it comes up)
- [ ] No data locality → your HDFS partition-sizing heuristics need re-tuning
- [ ] Listing cost at scale; why deep partition trees hurt more here than on HDFS
- [ ] Requester-pays, signed URLs, retention policies

**Do:** write a Spark job's output to GCS, inspect what actually lands, time it against your instinct.

---

## Day 3 — BigQuery: architecture and storage
This is the main event. Days 3–6 are the highest-value days in the plan.

- [ ] Separation of storage and compute — no cluster, no sizing decision
- [ ] Dremel execution model: tree architecture, leaf nodes, mixers
- [ ] What a **slot** is (a unit of compute, roughly a CPU thread + memory)
- [ ] Capacitor columnar format; why `SELECT *` is financially painful
- [ ] **Shuffle** — where queries die, same as Spark, but you can't tune executors. Understand shuffle spill to disk.
- [ ] Read: "Dremel: A Decade Later" (2020) — short, and it's the best 40 minutes you'll spend

**Do:** run a heavy join on a public dataset, open the execution graph, map each stage to what you'd expect in a Spark DAG.

---

## Day 4 — BigQuery physical design
- [ ] Partitioning: ingestion time, date/timestamp column, integer range
- [ ] Verify pruning by **bytes processed**, not by reading the plan
- [ ] Clustering: sorted column order, how it differs from Hive bucketing, when it does nothing
- [ ] `require_partition_filter` — set this on every large table
- [ ] Partition limits (4,000), partition expiration
- [ ] Nested/repeated: `ARRAY`, `STRUCT`, `UNNEST` — and when denormalizing beats joining (more often than in Hive)

**Do:** benchmark one query across unpartitioned / partitioned / partitioned+clustered. Write down bytes and seconds for each. Keep this table — it's interview material.

---

## Day 5 — BigQuery cost and optimization
The single most differentiating topic. Most candidates are vague here.

- [ ] On-demand ($/TB scanned) vs. **Editions** (Standard / Enterprise / Enterprise Plus)
- [ ] Slot reservations, baseline vs. autoscaling slots, commitment models
- [ ] Storage billing: logical vs. **physical**, active vs. long-term (90 days)
- [ ] Reading execution graphs: wait / read / compute / write time, records in vs. out
- [ ] Diagnosing skew: one stage where max compute time ≫ avg
- [ ] Join order — largest table first (opposite of some Hive habits)
- [ ] `APPROX_COUNT_DISTINCT` and HLL sketches
- [ ] Dry-run cost estimation: `bq query --dry_run`

**Do:** query `INFORMATION_SCHEMA.JOBS` to find your 10 most expensive queries. Then estimate the cost of 5 queries before running them and check your accuracy.

---

## Day 6 — BigQuery features, loading, security
- [ ] Loading: batch load jobs, **Storage Write API**, external tables, BigLake tables
- [ ] `MERGE` — your incremental workhorse
- [ ] Materialized views: automatic rewrite, refresh cost, what they can't do
- [ ] Table clones, snapshots, time travel (7 days), `FOR SYSTEM_TIME AS OF`
- [ ] Scheduled queries, stored procedures, UDFs, remote functions
- [ ] Authorized views and datasets; **policy tags** for column-level access; row-level security
- [ ] BigQuery ML — 30 minutes, just enough to know it exists and when it's the right answer

**Do:** build a small incremental `MERGE` pipeline with an SCD Type 2 dimension. You know the modeling; this is just learning BigQuery's syntax and semantics.

---

## Day 7 — Dataproc and the lakehouse layer
Easy day given your background. Mostly service mechanics.

- [ ] Dataproc cluster mode vs. **Dataproc Serverless** (default to serverless for batch)
- [ ] Cluster creation, autoscaling policies, preemptible/spot secondary workers, **auto-delete TTL** (always set it)
- [ ] Initialization actions, component gateway, custom images
- [ ] Dataproc Metastore — your managed Hive Metastore
- [ ] **BigQuery Spark connector**: direct read via Storage Read API, indirect write via GCS
- [ ] **Apache Iceberg** on GCS + BigLake managed Iceberg tables — if you're on Hive tables today, this is the migration target
- [ ] Dataplex: lakes/zones/assets, catalog, data quality scans, lineage (your Atlas/Ranger replacement)

**Do:** run one of your existing Spark jobs on Dataproc Serverless, unchanged. Then write its output as an Iceberg table and query it from BigQuery.

---

# WEEK 2 — Streaming, orchestration, production

## Day 8 — Pub/Sub
- [ ] Topics and subscriptions; pull vs. push vs. **BigQuery subscriptions** (direct, no Dataflow)
- [ ] Ack deadlines, retry policy, dead-letter topics
- [ ] **Ordering keys** — the Kafka-partition analogue, but not the same thing
- [ ] Message retention, seek/replay
- [ ] Delivery semantics: at-least-once default, "exactly-once delivery" subscriptions and their real limits
- [ ] **Mental model shift from Kafka:** no consumer-managed offsets, no partition count to size, no log compaction. Scaling is automatic; you lose some control in exchange.

**Do:** publish and consume with ordering keys; force a message into a dead-letter topic and recover it.

---

## Days 9–10 — Dataflow and the Beam model
The other genuinely new mental model. Give it two full days.

> **$0 note:** Day 9 is entirely local. `pip install apache-beam` and use the DirectRunner — the model is identical to what runs on Dataflow. Day 10 is the service layer; do it in a Skills Boost lab, or one short batch job on trial credits.

**Day 9 — the model**
- [ ] PCollections, PTransforms, ParDo, GroupByKey
- [ ] **Event time vs. processing time** (you know this from Spark Structured Streaming — Beam is stricter about it)
- [ ] **Windowing**: fixed, sliding, **session** (sessions are where Beam clearly beats Spark)
- [ ] **Watermarks**: how Beam estimates event-time completeness
- [ ] **Triggers**: early / on-time / late firings; accumulating vs. discarding mode
- [ ] Allowed lateness and what happens to data past it
- [ ] Read: Tyler Akidau's "Streaming 101 / 102". Non-optional — it's the source text for this model.

**Day 10 — the service**
- [ ] Dataflow runner: autoscaling, Streaming Engine, Dataflow Prime
- [ ] Drain vs. cancel, and why the difference matters in production
- [ ] Flex templates for parameterized reusable jobs
- [ ] Writing to BigQuery: Storage Write API vs. legacy streaming inserts
- [ ] Dead-letter patterns for unparseable records
- [ ] Dataflow SQL and templates for the simple cases (don't write Beam code you don't need)

**Do:** build a session-windowed aggregation with late data. Draw the watermark/trigger timeline for it and predict exactly what gets emitted when. Then verify.

---

## Day 11 — Composer, Datastream, and service selection
- [ ] **Cloud Composer** — it's managed Airflow. Half a day at most for you: environment model, GCS DAG folder, GCP operators (`BigQueryInsertJobOperator`, `DataprocCreateBatchOperator`, `DataflowTemplatedJobStartOperator`), Composer 2 vs. 3.
- [ ] **Do not create a Composer environment.** ~$300/month, bills while idle, and it's the single easiest way to destroy your credits. Use local Airflow in Docker plus one Skills Boost lab to see the environment model. You lose nothing — it's Airflow.
- [ ] **Datastream**: CDC from Cloud SQL/Postgres/MySQL/Oracle → BigQuery. Your Sqoop replacement, but continuous.
- [ ] The merge pattern for applying CDC changes, and handling deletes
- [ ] **Service selection decision tree** — memorize this, it's most of the interview:
  - Bigtable vs. Spanner vs. Firestore vs. Cloud SQL vs. BigQuery
  - Dataflow vs. Dataproc vs. BigQuery-native transforms
  - Pub/Sub vs. Pub/Sub Lite vs. managed Kafka
  - Composer vs. Workflows vs. Cloud Scheduler

**Do:** write out the decision tree in your own words. If you can't justify each branch, you don't know it yet.

---

## Day 12 — Cost engineering and observability
This is the senior-level differentiator. Don't skip it.

- [ ] Query your billing export (collected since Day 0): spend by service, by day
- [ ] **Labels** on jobs, datasets, and resources → per-pipeline chargeback
- [ ] Build a cost attribution query: spend by pipeline, by user, by table
- [ ] Decide on-demand vs. Editions with actual numbers from your usage
- [ ] Cloud Monitoring, Logging, log-based metrics, alerting policies
- [ ] Pipeline SLAs: freshness, volume, distribution anomalies
- [ ] Write a freshness monitor that alerts when a table hasn't updated on schedule

---

## Day 13 — Terraform and CI/CD
- [ ] Terraform GCP provider; GCS backend for state
- [ ] Build a reusable module: project + buckets + datasets + service accounts + IAM bindings
- [ ] dev/staging/prod project separation and promotion flow
- [ ] Cloud Build (or GitHub Actions): lint → test → `terraform plan` → deploy DAGs
- [ ] Deploying Composer DAGs via CI to the GCS DAG folder

**Do:** tear down your whole learning environment and rebuild it from Terraform in one command. If that works, you understand the platform.

---

## Day 14 — Build one thing end to end
Pick the shape closest to what you run at work today, and rebuild a slice of it on GCP:

**Batch:** GCS bronze → Dataproc Serverless Spark → Iceberg silver → BigQuery gold → Looker Studio. Airflow-orchestrated, Terraform-provisioned.

**Streaming:** Pub/Sub → Dataflow with session windowing and late-data handling → BigQuery via Storage Write API, with a replay path.

**CDC:** Cloud SQL → Datastream → BigQuery with SCD Type 2 merge and a backfill.

Write a short README: architecture, the trade-offs you chose, **monthly running cost**, what you'd do differently. The cost line is what makes it read as senior work.

---

## What this plan deliberately omits

- SQL, Spark, Airflow concepts, dimensional modeling — you have these
- dbt — worth a weekend later, but it's cloud-agnostic and not GCP knowledge
- Vertex AI beyond BigQuery ML basics — separate track
- The Professional Data Engineer cert — if you want it, add ~1 week of scenario practice after Day 14. The material is now all covered; the exam mostly tests service selection.

---

## Cost expectations

**Target: $0 of your own money.** Achievable if you follow three rules:

1. Never click "Activate full account" — GCP pauses rather than charges
2. Never create a Cloud Composer environment
3. Never leave a Dataflow **streaming** job running (batch jobs end themselves; streaming ones do not)

Expected trial credit consumption if you follow the plan: **$20–60 of the $300**, mostly Dataproc and one Dataflow run. Everything else fits in the Always Free tier or Skills Boost labs.

### If you can't get a card at all
The $300 trial requires card verification (debit usually works). If that's impossible, you can still do roughly 70% of this plan through Skills Boost labs alone, which need no payment method — but you lose the persistent project, so the Day 13–14 Terraform and end-to-end build won't work. In that case, Azure for Students and AWS Educate both grant credits without a card, which is a reason to reconsider platform if the card is a hard blocker.

*Free-tier terms and student programs change frequently — verify current limits on Google's pricing and free-tier pages rather than trusting this table.*

---

## Day-by-day tracker

| Day | Topic | Cost source | Done |
|---|---|---|---|
| 0 | Setup + guardrails | free | ☐ |
| 1 | IAM + service accounts | free | ☐ |
| 2 | GCS + object-store semantics | Always Free | ☐ |
| 3 | BigQuery architecture | Always Free | ☐ |
| 4 | BigQuery physical design | Always Free | ☐ |
| 5 | BigQuery cost + optimization | Always Free | ☐ |
| 6 | BigQuery features + security | Always Free | ☐ |
| 7 | Dataproc + Iceberg + Dataplex | **credits — set TTL** | ☐ |
| 8 | Pub/Sub | Always Free | ☐ |
| 9 | Beam model | local, $0 | ☐ |
| 10 | Dataflow service | **credits — batch only, verify stopped** | ☐ |
| 11 | Composer + Datastream + selection | local Airflow + Skills Boost | ☐ |
| 12 | Cost + observability | free | ☐ |
| 13 | Terraform + CI/CD | Always Free | ☐ |
| 14 | End-to-end build | credits | ☐ |

**Bold rows are the only two days that can cost you.** Run the teardown checklist at the end of both.

**If you're short on time, the irreducible core is Days 3–6 (BigQuery) and Days 9–10 (Beam).** Everything else you can pick up on the job in a week.
