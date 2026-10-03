# Service selection decision tree

The plan calls this "most of the interview." Each branch states the deciding question, not just the answer.

## 1. Where do I put the data?

**Deciding question: what is the access pattern?**

| Need | Service | Why not the others |
|---|---|---|
| Analytics over large history, SQL, scan-heavy | **BigQuery** | Columnar + separated compute. Terrible at single-row lookups by key. |
| Single-key lookups, huge write throughput, ms latency, no joins | **Bigtable** | Wide-column, no SQL, no secondary indexes. Choose when the access pattern is one row key. |
| Relational + global consistency + horizontal scale | **Spanner** | Expensive. Only when you genuinely need both ACID and scale beyond one machine. |
| Ordinary OLTP app, one region, Postgres/MySQL semantics | **Cloud SQL** | Vertically scaled, capped at one machine. The default until you outgrow it. |
| Mobile/web app documents, offline sync, realtime listeners | **Firestore** | Document store. Weak analytics; export to BigQuery for that. |
| Files, raw/landing zone, archives | **Cloud Storage** | No query engine. Pair with BigLake/external tables. |

**Rule of thumb:** OLTP → Cloud SQL, scale it → Spanner. OLAP → BigQuery. Key-value at scale → Bigtable.

## 2. How do I transform the data?

**Deciding question: can it be expressed as SQL over data already in BigQuery?**

- **Yes → BigQuery SQL** (scheduled queries, `MERGE`, dbt). No cluster, no code, cheapest to operate. *Default answer.*
- **No, and it's streaming or needs a real programming model → Dataflow (Beam).** Unified batch+streaming, autoscaling, event-time windowing. Pick this for session windows, watermarks, late data.
- **No, and I have existing Spark/Hadoop code → Dataproc** (prefer **Serverless** for batch). Choose it for migration, not for new work.

**Trap:** picking Dataproc for new pipelines because Spark is familiar. On GCP, the cheap default is SQL in BigQuery.

## 3. How do I move messages?

**Deciding question: do I need ordering/replay guarantees, and at what scale?**

- **Pub/Sub** — default. Global, autoscaling, no partitions to size. At-least-once; ordering only within an ordering key.
- **Pub/Sub Lite** — cheaper, zonal, you provision capacity yourself. Only for very high, predictable volume where cost dominates. (Being deprecated — verify before recommending.)
- **Managed Service for Kafka** — you need actual Kafka APIs, consumer-group semantics, log compaction, or an existing Kafka ecosystem.

**And the shortcut worth knowing:** Pub/Sub → **BigQuery subscription** writes straight to a table with *no Dataflow at all*. If the transform is trivial, don't build a pipeline.

## 4. What orchestrates it?

**Deciding question: how complex is the dependency graph?**

| Need | Service | Cost shape |
|---|---|---|
| Just run something on a schedule | **Cloud Scheduler** | ~free |
| A few steps, API calls, retries, no Python | **Workflows** | per-step, ~free at low volume |
| Real DAGs, backfills, sensors, dozens of tasks | **Cloud Composer** | **~$300/month, bills while idle** |

**Composer is the expensive default people reach for too early.** It's managed Airflow — nothing more. If the answer is "one query at 6am," it's Cloud Scheduler + a scheduled query, not a $300/month environment.

## 5. How does data get in?

- **Database CDC** → **Datastream** (Cloud SQL / Postgres / MySQL / Oracle → BigQuery). The continuous replacement for Sqoop. Apply changes with `MERGE`; handle deletes explicitly.
- **Files landing in GCS** → BigQuery load jobs, or external/BigLake tables if you want to leave them in place.
- **Streaming events** → Pub/Sub, then BigQuery subscription or Dataflow.
- **SaaS sources** → BigQuery Data Transfer Service before writing custom code.

## 6. Batch or streaming?

**Deciding question: does the business actually need sub-minute freshness?**

Usually no. Batch is cheaper, simpler, easier to backfill and reason about. Streaming buys freshness and costs you: continuous compute, watermark/late-data handling, harder testing, harder replay.

Choose streaming when the *decision* being made depends on freshness — fraud, alerting, live ops. Not because the data "arrives continuously."
