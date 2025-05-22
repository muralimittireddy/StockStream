from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from clickhouse_to_bq import ingest_data
from datetime import timedelta
import pendulum

local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': local_tz.datetime(2024, 1, 1, 16, 30),  # Start at 4:30 PM EST
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='clickhouse_to_bq_and_dbt',
    start_date=datetime(2025, 5, 3),
    schedule_interval='5 16 * * 1-5',
    catchup=False,
    tags=["market"]
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
    )

    initate_dbt_task = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /dbt && dbt deps --profiles-dir . --target prod'
    )

    execute_dbt_task = BashOperator(
        task_id='dbt_run_ohlcv_models',
        bash_command='cd /dbt && dbt run --select stg_ohlcv agg_daily_ohlcv --profiles-dir . --target prod'
    )

    ingest_task >> initate_dbt_task >> execute_dbt_task
