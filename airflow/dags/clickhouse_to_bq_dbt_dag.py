from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from clickhouse_to_bq import ingest_data
with DAG(
    dag_id='clickhouse_to_bq_and_dbt',
    start_date=datetime(2025, 5, 3),
    schedule_interval='5 * * * *',
    catchup=False,
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
