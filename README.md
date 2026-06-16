# Weather Data Pipeline — Assessment Submission

## Overview

An end-to-end analytics engineering pipeline that ingests real-time weather data
from the **Open-Meteo API** (free, no authentication required) for 5 African cities
(Kigali, Nairobi, Kampala, Dar es Salaam, Lagos), loads it through a layered data
architecture, and produces analytics-ready datasets in ClickHouse.

**Architecture:**
```
Open-Meteo API
      │
      ▼
  Python Ingestion Script
      │           │
      ▼           ▼
 PostgreSQL   ClickHouse
  (OLTP)       (OLAP raw)
                  │
                  ▼
              dbt staging
           (cleaned views)
                  │
                  ▼
              dbt marts
          (aggregated tables)
                  │
                  ▼
          Analytics / ML Ready
```

**Core Technologies:** Python · PostgreSQL · ClickHouse · dbt · Airflow · Docker Compose

---

## Data Source

| Detail | Value |
|--------|-------|
| API | Open-Meteo Forecast API |
| URL | https://api.open-meteo.com/v1/forecast |
| Authentication | None required |
| Docs | https://open-meteo.com/en/docs |
| Data | Hourly weather: temperature, humidity, precipitation, wind speed, WMO weather code |
| Cities | Kigali, Nairobi, Kampala, Dar es Salaam, Lagos |

---

## Prerequisites

- Docker ≥ 24.x
- Docker Compose ≥ 2.x
- Internet access (for Open-Meteo API)
- 4 GB RAM minimum recommended

---

## How to Run the Pipeline (One Command)

```bash
# 1. Clone / enter the project directory
cd weather-pipeline

# 2. Start everything (databases, ingestion, airflow)
docker compose up --build -d

# 3. Wait ~60 seconds for services to initialise, then trigger the pipeline
docker compose exec airflow-webserver airflow dags trigger weather_pipeline
```

You can also run ingestion standalone (without waiting for Airflow):
```bash
docker compose run --rm ingestion
```

---

## Airflow UI

| URL | http://localhost:8080 |
|-----|----------------------|
| Username | admin |
| Password | admin |

Navigate to **DAGs → weather_pipeline → Trigger DAG** to run the full pipeline manually.

---

## Validating Data at Each Stage

### 1. PostgreSQL (raw ingestion)
```bash
docker compose exec postgres psql -U pipeline_user -d weather_db -c \
  "SELECT city, count(*) AS rows, min(recorded_at), max(recorded_at)
   FROM raw_weather GROUP BY city ORDER BY city;"
```

### 2. ClickHouse (replicated raw)
```bash
docker compose exec clickhouse clickhouse-client --query \
  "SELECT city, count(*) AS rows FROM weather_analytics.raw_weather GROUP BY city;"
```

### 3. dbt staging view
```bash
docker compose exec clickhouse clickhouse-client --query \
  "SELECT city, weather_description, count(*) AS n
   FROM weather_analytics.stg_weather GROUP BY city, weather_description
   ORDER BY city, n DESC LIMIT 20;"
```

### 4. dbt mart (analytics-ready)
```bash
docker compose exec clickhouse clickhouse-client --query \
  "SELECT city, record_date, avg_temp_c, total_precipitation_mm, dominant_weather
   FROM weather_analytics.marts_mart_daily_weather_summary
   ORDER BY city, record_date DESC LIMIT 10;"
```

---

## Stopping the Stack

```bash
docker compose down -v   # -v removes volumes (clean reset)
```

---

## Scaling & Extension Notes

See the Design Report for full discussion. In brief:

- **Volume scaling:** Partition ClickHouse tables by month (already done). Add
  ClickHouse sharding / replication for multi-node setups.
- **More cities:** Add entries to the `CITIES` list in `ingest.py` — no schema changes needed.
- **Historical backfill:** Parameterise `START_DATE` in the DAG via Airflow variables.
- **ML readiness:** The `mart_daily_weather_summary` table (with heat index, dominant
  weather, daily aggregates) is directly usable as a feature store input.
- **Real-time:** Replace batch ingestion with Kafka + ClickHouse Kafka engine for
  streaming weather feeds.