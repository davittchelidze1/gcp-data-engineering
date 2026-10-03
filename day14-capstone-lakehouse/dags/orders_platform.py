"""Orders platform - one daily run processes exactly one bronze partition.

  wait for bronze/ingest_date={{ ds }}/  ->  Spark (that partition only)  ->  silver
  -> gates on the new partition  ->  gold MERGE for the order dates it touched
  -> freshness SLA

Backfills are run in bulk from the CLI (see README), not as 30 daily DAG runs:
each Dataproc Serverless batch pays ~2 minutes of startup, so one 30-day job is
far cheaper than thirty 1-day jobs.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

PROJECT = "gcp-learning-507108"
REGION = "us-central1"
CONN = "google_cloud_default"
BRONZE_BUCKET = "%s-bronze" % PROJECT
SILVER = "gs://%s-silver" % PROJECT
LABELS = {"pipeline": "orders_platform"}
SILVER_TABLE = "`%s.platform.silver_orders`" % PROJECT

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql")


def sql(name):
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as f:
        return f.read()


def date_param(name):
    return {"name": name, "parameterType": {"type": "DATE"},
            "parameterValue": {"value": "{{ ds }}"}}


def bq_job(task_id, query, params=None):
    q = {"query": query, "useLegacySql": False}
    if params:
        q["parameterMode"] = "NAMED"
        q["queryParameters"] = params
    return BigQueryInsertJobOperator(task_id=task_id, location=REGION, gcp_conn_id=CONN,
                                     configuration={"query": q, "labels": LABELS})


def gate(task_id, predicate):
    """Checks run against the new partition only - never a scan of all history."""
    return BigQueryCheckOperator(
        task_id=task_id, location=REGION, gcp_conn_id=CONN, use_legacy_sql=False, labels=LABELS,
        sql="SELECT %s FROM %s WHERE ingest_date = '{{ ds }}'" % (predicate, SILVER_TABLE))


def _freshness(**_):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    hook = BigQueryHook(gcp_conn_id=CONN, use_legacy_sql=False, location=REGION)
    stale = hook.get_records(
        "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(built_at), MINUTE) "
        "FROM `%s.platform.gold_daily_revenue`" % PROJECT)[0][0]
    print("gold was rebuilt %s minutes ago" % stale)
    if stale is None or stale > 60:
        raise ValueError("gold is stale: %s minutes" % stale)


with DAG(
    dag_id="orders_platform",
    description="bronze -> silver -> gold, one ingest partition per run",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dato", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["day14", "platform"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_bronze = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_bronze_partition",
        bucket=BRONZE_BUCKET,
        prefix="orders/ingest_date={{ ds }}/",
        google_cloud_conn_id=CONN,
        mode="reschedule",          # frees the worker slot between pokes
        poke_interval=300,
        timeout=6 * 3600,
    )

    bronze_to_silver = DataprocCreateBatchOperator(
        task_id="bronze_to_silver",
        project_id=PROJECT,
        region=REGION,
        gcp_conn_id=CONN,
        batch_id="orders-{{ ds_nodash }}-{{ ti.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://%s-dataproc/jobs/bronze_to_silver.py" % PROJECT,
                "args": [
                    "--bronze=gs://%s/orders" % BRONZE_BUCKET,
                    "--silver=%s/orders" % SILVER,
                    "--quarantine=%s/_quarantine/orders" % SILVER,
                    "--ingest_date={{ ds }}",
                ],
            },
            "runtime_config": {
                "version": "2.2",
                # one day of data needs a few cores, not the default ceiling of 1,000 executors
                "properties": {"spark.dynamicAllocation.maxExecutors": "4"},
            },
            "environment_config": {"execution_config": {"staging_bucket": "%s-dataproc" % PROJECT}},
            "labels": LABELS,
        },
    )

    ensure_silver_table = bq_job("ensure_silver_table", sql("silver_table.sql"))

    gate_volume = gate("gate_batch_volume", "COUNT(*) BETWEEN 500000 AND 2000000")
    gate_valid = gate("gate_no_invalid_rows",
                      "COUNTIF(amount < 0 OR items <= 0 OR order_ts IS NULL OR order_id IS NULL) = 0")
    gate_unique = gate("gate_order_ids_unique", "COUNT(*) = COUNT(DISTINCT order_id)")

    merge_gold = bq_job("merge_gold", sql("silver_to_gold.sql"),
                        params=[date_param("ingest_from"), date_param("ingest_to")])

    freshness = PythonOperator(task_id="sla_freshness", python_callable=_freshness)

    end = EmptyOperator(task_id="end")

    (start >> wait_for_bronze >> bronze_to_silver >> ensure_silver_table
     >> [gate_volume, gate_valid, gate_unique] >> merge_gold >> freshness >> end)
