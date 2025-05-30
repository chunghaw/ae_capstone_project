from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os, sys

# Ensure project root is on PYTHONPATH
dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(dag_dir, "../scripts"))
sys.path.insert(0, os.path.join(dag_dir, ".."))

# Import your custom functions
from scripts.binance.fetch.crypto_iceberg_etl import main as ingest_to_iceberg
from scripts.load_to_snowflake import main as load_to_snowflake

# Default args
default_args = {
    'owner': 'chunghaw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_etl_and_dbt',
    default_args=default_args,
    start_date=datetime(2025, 5, 20),
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'iceberg', 'snowflake', 'dbt'],
) as dag:

    ingest = PythonOperator(
        task_id='ingest_to_iceberg',
        python_callable=ingest_to_iceberg,
    )

    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    # ✅ Replace DockerOperator with BashOperator for dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/local/airflow/dbt_project && dbt run --profiles-dir .',
    )

    # ✅ Replace DockerOperator with BashOperator for dbt test
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command='cd /usr/local/airflow/dbt_project && dbt test --profiles-dir .',
    )

    ingest >> load >> dbt_run >> dbt_tests