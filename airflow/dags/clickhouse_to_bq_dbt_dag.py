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

    # dbt = BashOperator(
    #     task_id='run_dbt_transforms',
    #     bash_command='/opt/scripts/dbt_runner.sh',
    # )

    ingest_task
