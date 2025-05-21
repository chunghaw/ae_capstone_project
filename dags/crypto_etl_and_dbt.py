# dags/crypto_etl_and_dbt.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import os, sys

# __file__ is e.g. .../AE_CAPSTONE_PROJECT/dags/crypto_etl_and_dbt.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# now we can import from binance.fetch
from scripts.binance.fetch.crypto_iceberg_etl import main as ingest_to_iceberg
from scripts.load_to_snowflake import main as load_to_snowflake

default_args = {
    'owner': 'chunghaw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_etl_and_dbt',
    default_args=default_args,
    start_date=datetime(2025,5,20),
    schedule_interval='@daily',
    catchup=False,
    tags=["crypto", "iceberg", "dbt"],
) as dag:

    # 1) Extract & load into Iceberg via your existing script
    ingest = PythonOperator(
        task_id="ingest_to_iceberg",
        python_callable=ingest_to_iceberg
    )

    load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staghing",
        env={
            "DBT_PROJECT_DIR": "/usr/local/airflow/dbt_project",
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_project",
            "DBT_TARGET": "dev",
            "DBT_SCHEMA": "chunghaw",
        },
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt deps && "
            "dbt run --models staging "
            "--profiles-dir $DBT_PROFILES_DIR "
            "--target $DBT_TARGET"
        ),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        env={
            "DBT_PROJECT_DIR": "/usr/local/airflow/dbt_project",
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_project",
            "DBT_TARGET": "dev",
            "DBT_SCHEMA": "chunghaw",
        },
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --models intermediate "
            "--profiles-dir $DBT_PROFILES_DIR "
            "--target $DBT_TARGET"
        ),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        env={
            "DBT_PROJECT_DIR": "/usr/local/airflow/dbt_project",
            "DBT_PROFILES_DIR": "/usr/local/airflow/dbt_project",
            "DBT_TARGET": "dev",
            "DBT_SCHEMA": "chunghaw",
        },
        bash_command=(
            "cd $DBT_PROJECT_DIR && "
            "dbt run --models marts "
            "--profiles-dir $DBT_PROFILES_DIR "
            "--target $DBT_TARGET"
        ),
    )

ingest >> load >> dbt_staging >> dbt_intermediate >> dbt_marts
