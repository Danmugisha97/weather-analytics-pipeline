.DEFAULT_GOAL := help

.PHONY: help setup up down trigger logs validate clean

help:            ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup:           ## Copy .env.example → .env (first-time setup)
	cp -n .env.example .env || echo ".env already exists, skipping."
	@echo "👉  Edit .env and set your passwords before running 'make up'."

up:              ## Build images and start all services
	docker compose up --build -d
	@echo "⏳  Waiting 60 s for services to initialise..."
	sleep 60
	@echo "✅  Stack is up.  Airflow UI → http://localhost:8080  (admin / admin)"

down:            ## Stop all services and remove containers
	docker compose down

clean:           ## Stop services and delete all volumes (full reset)
	docker compose down -v

trigger:         ## Trigger the weather_pipeline DAG
	docker compose exec airflow-webserver \
		airflow dags trigger weather_pipeline
	@echo "✅  DAG triggered. Watch it at http://localhost:8080"

logs:            ## Tail Airflow scheduler logs
	docker compose logs -f airflow-scheduler

validate:        ## Query row counts at every layer (quick sanity check)
	@echo "\n── PostgreSQL raw_weather ──────────────────────────────"
	docker compose exec postgres psql -U $${POSTGRES_USER:-pipeline_user} \
		-d $${POSTGRES_DB:-weather_db} \
		-c "SELECT city, count(*) AS rows FROM raw_weather GROUP BY city ORDER BY city;"
	@echo "\n── ClickHouse raw_weather ──────────────────────────────"
	docker compose exec clickhouse-server clickhouse-client \
		--query "SELECT city, count() AS rows FROM weather_analytics.raw_weather GROUP BY city;"
	@echo "\n── ClickHouse mart (daily summary) ─────────────────────"
	docker compose exec clickhouse-server clickhouse-client \
		--query "SELECT city, min(record_date) AS from, max(record_date) AS to, count() AS days \
		         FROM weather_analytics.marts_mart_daily_weather_summary GROUP BY city ORDER BY city;"
