"""Airflow DAG for scheduling CDP ETL Spark job."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="cdp_orders_etl",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdp", "spark", "etl"],
) as dag:
    run_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            "spark-submit --master yarn "
            "--deploy-mode cluster "
            "--conf spark.sql.adaptive.enabled=true "
            "src/pipeline/job.py --config conf/pipeline.yml"
        ),
    )

    run_etl
