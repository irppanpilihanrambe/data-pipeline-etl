#  Data Pipeline ETL

End-to-end data pipeline project using Python, Apache Spark, Apache Kafka, Apache Airflow, and dbt — running fully on-premise with Docker.

## Architecture

```
Kafka (Ingest) → Spark (Transform) → Postgres/DWH (Load) → dbt (Model) → Analytics
                        ↑
                   Airflow (Orchestrate)
```

## Project Structure

```
data-pipeline-etl/
├── dags/                    # Airflow DAG definitions
│   ├── etl_pipeline_dag.py
│   └── kafka_consumer_dag.py
├── spark_jobs/              # PySpark transformation scripts
│   ├── transform_raw.py
│   └── aggregate_metrics.py
├── kafka/                   # Kafka producer & consumer
│   ├── producer.py
│   └── consumer.py
├── dbt/                     # dbt models & tests
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
├── sql/                     # Raw SQL scripts
│   ├── ddl/
│   └── queries/
├── tests/                   # Unit & integration tests
├── docker/                  # Dockerfiles per service
│   ├── Dockerfile.airflow
│   └── Dockerfile.spark
├── docker-compose.yml       # Full local stack
├── requirements.txt
├── .env.example
├── Makefile
└── README.md
```

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11+ | Core language |
| Apache Airflow | 2.8+ | Orchestration |
| Apache Spark | 3.5+ | Batch transformation |
| Apache Kafka | 3.6+ | Streaming ingest |
| dbt-core | 1.7+ | Data modeling |
| PostgreSQL | 15+ | Data warehouse |
| Docker | 24+ | Containerization |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Make

### 1. Clone & setup environment

```bash
git clone https://github.com/<username>/data-pipeline-etl.git
cd data-pipeline-etl
cp .env.example .env
# Edit .env dengan konfigurasi lokal kamu
```

### 2. Jalankan seluruh stack

```bash
make up
```

### 3. Akses services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | — |
| Spark UI | http://localhost:4040 | — |

### 4. Trigger pipeline

```bash
make run-etl
```

## Development

```bash
# Install dependencies lokal
make install

# Jalankan tests
make test

# Run dbt models
make dbt-run

# Format & lint code
make lint
```

## Pipeline Flow

1. **Ingest** — Kafka producer mengirim data ke topic
2. **Consume** — Kafka consumer membaca dan simpan ke raw layer
3. **Transform** — Spark jobs membersihkan dan mentransformasi data
4. **Model** — dbt membuat staging dan mart models
5. **Orchestrate** — Airflow menjadwalkan seluruh pipeline

## Contributing

1. Fork repo ini
2. Buat feature branch: `git checkout -b feature/nama-fitur`
3. Commit changes: `git commit -m 'feat: tambah fitur X'`
4. Push & buat Pull Request

## 📄 License

MIT License
