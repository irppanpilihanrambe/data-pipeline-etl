"""
ETL Pipeline DAG — Orchestrates the full extract → transform → load flow.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_pipeline_dag",
    default_args=default_args,
    description="Full ETL: Kafka → Spark → PostgreSQL → dbt",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "pipeline"],
) as dag:

    def extract_from_kafka(**context):
        """Pull latest messages from Kafka and save to staging area."""
        from kafka import consume_messages  # noqa: F401
        logical_date = context["logical_date"].strftime("%Y-%m-%d")
        print(f"[Extract] Consuming Kafka messages for {logical_date}")
        # consume_messages(date=logical_date)

    extract = PythonOperator(
        task_id="extract_kafka",
        python_callable=extract_from_kafka,
    )

    transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/airflow/spark_jobs/transform_raw.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        name="etl-transform-{{ ds }}",
        verbose=False,
    )

    aggregate = SparkSubmitOperator(
        task_id="spark_aggregate",
        application="/opt/airflow/spark_jobs/aggregate_metrics.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        name="etl-aggregate-{{ ds }}",
        verbose=False,
    )

    load = PostgresOperator(
        task_id="load_to_dwh",
        postgres_conn_id="postgres_default",
        sql="sql/queries/upsert_processed.sql",
    )

    def run_dbt(**context):
        import subprocess
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", ".", "--target", "dev"],
            cwd="/opt/airflow/dbt",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt run failed:\n{result.stderr}")
        print(result.stdout)

    dbt = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
    )

    extract >> transform >> aggregate >> load >> dbt
