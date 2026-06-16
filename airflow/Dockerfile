FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

# Install dbt-clickhouse and ingestion deps inside Airflow image
COPY ./airflow/requirements.txt /tmp/airflow_requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow_requirements.txt