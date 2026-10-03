"""bronze -> silver for one or more ingest-date partitions. Runs on Dataproc Serverless.

Reads ONLY the requested bronze partitions:
    <bronze>/ingest_date=YYYY-MM-DD/*.json
and writes, per ingest date:
    <silver>/ingest_date=YYYY-MM-DD/       clean, typed, deduped rows    (Parquet)
    <quarantine>/ingest_date=YYYY-MM-DD/   rejected rows + reject_reason (Parquet)

    daily:     --ingest_date 2026-08-31
    backfill:  --ingest_from 2026-08-01 --ingest_to 2026-08-30

Dynamic partition overwrite replaces exactly the dates being processed, so a
re-run is idempotent and the rest of silver is never touched.
"""
import argparse
import datetime as dt
import json
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_FIELDS = ["order_id", "customer_id", "country", "items", "amount", "order_ts", "updated_at"]

# Bronze is read as raw strings. Upstream encodings drift (BigQuery exports INT64
# as JSON strings but FLOAT64 as numbers); typing happens explicitly below, where
# a failed cast becomes a quarantine reason instead of a silently-null column.
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_FIELDS]
                        + [StructField("_corrupt_record", StringType(), True)])


def run_dates(a):
    if a.ingest_date:
        return [a.ingest_date]
    d, end = dt.date.fromisoformat(a.ingest_from), dt.date.fromisoformat(a.ingest_to)
    out = []
    while d <= end:
        out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", required=True)
    ap.add_argument("--silver", required=True)
    ap.add_argument("--quarantine", required=True)
    ap.add_argument("--ingest_date")
    ap.add_argument("--ingest_from")
    ap.add_argument("--ingest_to")
    ap.add_argument("--max_reject_rate", type=float, default=0.05)
    a = ap.parse_args()
    if not (a.ingest_date or (a.ingest_from and a.ingest_to)):
        ap.error("pass --ingest_date, or both --ingest_from and --ingest_to")

    spark = (SparkSession.builder.appName("orders-bronze-to-silver")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")   # order_date must not drift with the cluster's zone
             .getOrCreate())
    t_start = time.time()

    dates = run_dates(a)
    paths = ["%s/ingest_date=%s" % (a.bronze.rstrip("/"), d) for d in dates]   # the only files read

    raw = (spark.read.schema(RAW_SCHEMA)
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "_corrupt_record")
           .json(paths))
    if "ingest_date" in raw.columns:
        raw = raw.drop("ingest_date")

    typed = (raw
             .withColumn("ingest_date", F.to_date(F.regexp_extract(
                 F.input_file_name(), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)))
             # try_* return NULL on bad input whether or not ANSI mode is on;
             # a plain cast would throw under ANSI and kill the job on one bad row
             .withColumn("items_i", F.expr("try_cast(items AS INT)"))
             .withColumn("amount_d", F.expr("try_cast(amount AS DOUBLE)"))
             .withColumn("order_ts_t", F.expr("try_to_timestamp(order_ts)"))
             .withColumn("updated_at_t", F.expr("try_to_timestamp(updated_at)")))

    # Every row gets exactly one outcome: a reject reason, or NULL = valid.
    # Nothing can fall between two filters and disappear.
    reason = (F.when(F.col("_corrupt_record").isNotNull(), "malformed_json")
              .when(F.col("order_id").isNull(), "missing_order_id")
              .when(F.col("country").isNull(), "missing_country")
              .when(F.col("order_ts_t").isNull(), "bad_or_missing_order_ts")
              .when(F.col("updated_at_t").isNull(), "bad_or_missing_updated_at")
              .when(F.col("amount_d").isNull(), "bad_or_missing_amount")
              .when(F.col("amount_d") < 0, "negative_amount")
              .when(F.col("items_i").isNull(), "bad_or_missing_items")
              .when(F.col("items_i") <= 0, "non_positive_items"))
    scored = typed.withColumn("reject_reason", reason)

    # ---- pass 1: outcome counts ----
    outcome = {r["reject_reason"]: r["count"]
               for r in scored.groupBy("reject_reason").count().collect()}
    n_valid = outcome.pop(None, 0)
    n_rejected = sum(outcome.values())
    n_in = n_valid + n_rejected
    if n_in == 0:
        raise SystemExit("no rows found under %s" % paths)
    reject_rate = n_rejected / n_in
    t_validated = time.time()

    rejected = (scored.filter(F.col("reject_reason").isNotNull())
                .select(*RAW_FIELDS, "_corrupt_record", "reject_reason", "ingest_date"))

    # ---- circuit breaker: a feed this broken is an incident, not a load ----
    if reject_rate > a.max_reject_rate:
        rejected.repartition("ingest_date").write.mode("overwrite") \
            .partitionBy("ingest_date").parquet(a.quarantine)
        raise SystemExit("reject rate %.2f%% exceeds %.2f%%; quarantine written, silver untouched"
                         % (100 * reject_rate, 100 * a.max_reject_rate))

    # ---- pass 2: silver. One task per ingest date -> one file per partition,
    # and the window's sort reuses that distribution (no second shuffle). ----
    latest = Window.partitionBy("ingest_date", "order_id").orderBy(F.col("updated_at_t").desc())
    silver = (scored.filter(F.col("reject_reason").isNull())
              .repartition("ingest_date")
              .withColumn("_rn", F.row_number().over(latest))
              .filter(F.col("_rn") == 1)
              .select("order_id", "customer_id", "country",
                      F.col("items_i").alias("items"),
                      F.col("amount_d").alias("amount"),
                      F.col("order_ts_t").alias("order_ts"),
                      F.col("updated_at_t").alias("updated_at"),
                      F.to_date("order_ts_t").alias("order_date"),
                      "ingest_date"))
    silver.write.mode("overwrite").partitionBy("ingest_date").parquet(a.silver)
    t_silver = time.time()

    # ---- pass 3: quarantine ----
    rejected.repartition("ingest_date").write.mode("overwrite") \
        .partitionBy("ingest_date").parquet(a.quarantine)
    t_quarantine = time.time()

    # read back what actually landed, rather than trusting the plan
    n_silver = (spark.read.parquet(a.silver)
                .where(F.col("ingest_date").isin(dates)).count())

    print("METRICS " + json.dumps({
        "ingest_dates": [dates[0], dates[-1]],
        "partitions": len(dates),
        "rows_in": n_in,
        "valid": n_valid,
        "rejected": n_rejected,
        "reject_rate_pct": round(100 * reject_rate, 3),
        "reject_reasons": outcome,
        "duplicates_removed": n_valid - n_silver,
        "silver_rows": n_silver,
        "seconds": {
            "validate": round(t_validated - t_start, 1),
            "write_silver": round(t_silver - t_validated, 1),
            "write_quarantine": round(t_quarantine - t_silver, 1),
            "total": round(time.time() - t_start, 1),
        },
    }))
    spark.stop()


if __name__ == "__main__":
    main()
