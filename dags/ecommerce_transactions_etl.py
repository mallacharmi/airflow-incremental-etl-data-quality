from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "alerts@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_transactions_etl",
    description="Incremental ETL pipeline with data quality gates",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce", "incremental"],
) as dag:

    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command="python /opt/airflow/scripts/generate_data.py",
    )

    ingest_staging = BashOperator(
        task_id="ingest_staging",
        bash_command="python /opt/airflow/scripts/ingest.py",
    )

    run_data_quality = BashOperator(
        task_id="run_data_quality",
        bash_command="python /opt/airflow/scripts/data_quality.py",
    )

    load_fact_table = BashOperator(
        task_id="load_fact_table",
        bash_command="python /opt/airflow/scripts/load_fact.py",
    )

    generate_source_data >> ingest_staging >> run_data_quality >> load_fact_table
