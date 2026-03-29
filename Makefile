.PHONY: help up down install test lint dbt-run run-etl logs clean

help:
	@echo "Available commands:"
	@echo "  make up         - Start all services (Docker)"
	@echo "  make down       - Stop all services"
	@echo "  make install    - Install Python dependencies locally"
	@echo "  make test       - Run all tests"
	@echo "  make lint       - Run linting & formatting"
	@echo "  make dbt-run    - Run dbt models"
	@echo "  make run-etl    - Trigger ETL pipeline via Airflow"
	@echo "  make logs       - Follow Docker logs"
	@echo "  make clean      - Remove containers & volumes"

up:
	docker compose up -d
	@echo "✅ Stack is running!"
	@echo "   Airflow UI  → http://localhost:8080  (admin/admin)"
	@echo "   Kafka UI    → http://localhost:8090"
	@echo "   Spark UI    → http://localhost:4040"

down:
	docker compose down

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --cov=. --cov-report=term-missing

lint:
	ruff check . --fix
	black .

dbt-run:
	cd dbt && dbt run --profiles-dir . --target dev

dbt-test:
	cd dbt && dbt test --profiles-dir . --target dev

run-etl:
	docker compose exec airflow-webserver airflow dags trigger etl_pipeline_dag

logs:
	docker compose logs -f

clean:
	docker compose down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
