# dags/crypto_etl_and_dbt.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    tags=['crypto','iceberg','dbt']
) as dag:

    # 1) Extract & load into Iceberg via your existing script
    ingest = PythonOperator(
        task_id='ingest_crypto_to_iceberg',
        python_callable=lambda: __import__('src.fetch.crypto_iceberg_etl').crypto_iceberg_etl.main()
    )

    # 2) dbt run: build staging→marts in Snowflake
    dbt_run = BashOperator(
        task_id='dbt_run',
        env={
            'AIRFLOW_HOME': '/usr/local/airflow',
            'DBT_PROFILES_DIR': '/usr/local/airflow/dbt_project',
            'DBT_PROJECT_DIR': '/usr/local/airflow/dbt_project',
            'DBT_SCHEMA': 'chunghaw',
            'DBT_TARGET': 'dev'
        },
        bash_command=(
            'cd $DBT_PROJECT_DIR && '
            'dbt deps && '
            'dbt run '
            '--profiles-dir $DBT_PROFILES_DIR '
            '--target $DBT_TARGET '
            '--models +stg_binance_klines +stg_binance_24h_stats +marts/*'
        )
    )

    # 3) dbt test: sanity checks on your new models
    dbt_test = BashOperator(
        task_id='dbt_test',
        env={
            'DBT_PROFILES_DIR': '/usr/local/airflow/dbt_project',
            'DBT_PROJECT_DIR': '/usr/local/airflow/dbt_project',
            'DBT_SCHEMA': 'chunghaw',
            'DBT_TARGET': 'dev'
        },
        bash_command=(
            'cd $DBT_PROJECT_DIR && '
            'dbt test '
            '--profiles-dir $DBT_PROFILES_DIR '
            '--target $DBT_TARGET '
            '--models +stg_binance_klines +stg_binance_24h_stats +marts/*'
        )
    )

    ingest >> dbt_run >> dbt_test
