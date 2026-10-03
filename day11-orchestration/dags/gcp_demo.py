"""Day 11 - what Cloud Composer runs, running locally in Docker for $0.

Composer IS this. The only thing it adds is a managed environment,
a GCS-backed dags/ folder, and a ~$300/month bill.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

PROJECT = "gcp-learning-507108"

default_args = {
    "owner": "dato",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

with DAG(
    dag_id="gcp_daily_orders",
    description="Day 11: the GCP operators you would use on Composer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["day11", "gcp"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. run SQL in BigQuery - the workhorse operator
    build_summary = BigQueryInsertJobOperator(
        task_id="build_daily_summary",
        gcp_conn_id="google_cloud_default",
        location="us-central1",
        configuration={
            "query": {
                "query": (
                    "CREATE OR REPLACE TABLE `day6.airflow_summary` AS "
                    "SELECT tier, COUNT(*) AS customers, CURRENT_TIMESTAMP() AS built_at "
                    "FROM `day6.dim_customer` WHERE is_current GROUP BY tier"
                ),
                "useLegacySql": False,
            }
        },
    )

    # 2. a data-quality gate - fail the DAG rather than publish bad data
    def _check(**context):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, location="us-central1")
        rows = hook.get_records(
            "SELECT COUNT(*) FROM `%s.day6.airflow_summary`" % PROJECT
        )
        n = rows[0][0]
        print("summary row count = %s" % n)
        if n == 0:
            raise ValueError("summary table is empty - refusing to publish")
        return n

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_check,
    )

    end = EmptyOperator(task_id="end")

    start >> build_summary >> quality_gate >> end
