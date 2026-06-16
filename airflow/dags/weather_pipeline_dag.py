"""
Weather Pipeline DAG
Orchestrates: Ingestion → dbt staging → dbt marts → dbt tests
Schedule: Daily at 06:00 UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import subprocess

DEFAULT_ARGS = {
    "owner":            "dan",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

DBT_DIR = "/opt/dbt_project"
DBT_PROFILES_DIR = "/opt/dbt_project"

with DAG(
    dag_id="weather_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end weather data pipeline: API → Postgres → ClickHouse → dbt",
    schedule_interval="0 6 * * *",   # every day at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "pipeline", "assessment"],
) as dag:

    # ── Task 1: Ingest from API ──────────────────────────────────────────────
    ingest = BashOperator(
        task_id="ingest_weather_data",
        bash_command="cd /opt/ingestion && python ingest.py",
        env={
            "POSTGRES_HOST":     "postgres",
            "POSTGRES_PORT":     "5432",
            "POSTGRES_USER":     "pipeline_user",
            "POSTGRES_PASSWORD": "pipeline_pass",
            "POSTGRES_DB":       "weather_db",
            "CLICKHOUSE_HOST":   "clickhouse",
            "CLICKHOUSE_PORT":   "8123",
            "CLICKHOUSE_DB":     "weather_analytics",
            "CLICKHOUSE_USER":   "admin",
            "CLICKHOUSE_PASSWORD": "u66w8wdsd",
        },
    )

    # ── Task 2: dbt deps ─────────────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 3: dbt staging layer ────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select staging "
            f"--vars '{{\"start_date\": \"{{{{ ds }}}}\"}}'  "
        ),
    )

    # ── Task 4: dbt marts layer ──────────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select marts"
        ),
    )

    # ── Task 5: dbt tests ────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── DAG dependency chain ─────────────────────────────────────────────────
    ingest >> dbt_deps >> dbt_staging >> dbt_marts >> dbt_test