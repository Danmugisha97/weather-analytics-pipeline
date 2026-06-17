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

import os

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
            "POSTGRES_HOST":       os.getenv("POSTGRES_HOST",     "postgres"),
            "POSTGRES_PORT":       os.getenv("POSTGRES_PORT",     "5432"),
            "POSTGRES_USER":       os.getenv("POSTGRES_USER",     "pipeline_user"),
            "POSTGRES_PASSWORD":   os.getenv("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB":         os.getenv("POSTGRES_DB",       "weather_db"),
            "CLICKHOUSE_HOST":     os.getenv("CLICKHOUSE_HOST",   "clickhouse"),
            "CLICKHOUSE_PORT":     os.getenv("CLICKHOUSE_PORT",   "8123"),
            "CLICKHOUSE_DB":       os.getenv("CLICKHOUSE_DB",     "weather_analytics"),
            "CLICKHOUSE_USER":     os.getenv("CLICKHOUSE_USER",   "admin"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
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
    # Runs all models in the marts/ folder:
    #   - mart_daily_weather_summary
    #   - mart_city_weather_profile
    #   - mart_extreme_weather_events
    #   - mart_weekly_weather_trends
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