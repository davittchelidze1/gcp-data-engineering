# Day 7 — Dataproc, BigLake and Iceberg

**Hadoop equivalent:** this *is* your Hadoop/Spark cluster — same Spark, same
code. What's new is that you can skip the cluster entirely.

## Dataproc Serverless — no cluster exists before or after
[job.py](job.py) is an ordinary PySpark job, unchanged. Submitted as a
serverless batch it:
- read 20,000 rows straight from BigQuery (Storage Read API — no export step)
- wrote Parquet to GCS, partitioned by weekday — the "silver" layer
- wrote the aggregate back to BigQuery (staged through a temporary GCS bucket)

**135 s** submit-to-finish on Spark 3.5.3, billed only while it ran.

## Cluster mode — always with a TTL
A forgotten on-prem cluster just idles; a forgotten cloud cluster bills by the
minute. So the cluster was created with `--max-idle=30m --max-age=2h` (it
deletes itself), ran SparkPi, and was deleted by hand straight after anyway.

## The lakehouse layer — [lakehouse.sql](lakehouse.sql)
- **BigLake table** over the Parquet the Spark job wrote: queried from BigQuery
  with no load step. Hive-style `weekday=0/` folders need an explicit
  `WITH PARTITION COLUMNS`.
- **BigQuery-managed Iceberg table**: 500 rows inserted with SQL; on GCS it's just
  `metadata/v0.metadata.json` plus Parquet data files — an open format any engine
  can read.

## Gotchas
- Serverless needs **Private Google Access** on the subnet.
- The BigLake connection has its **own service account**; it needs read (and, for
  Iceberg, write) access to the bucket, and grants take a minute to apply.

Commands: [commands.sh](commands.sh)
