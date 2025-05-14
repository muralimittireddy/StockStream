from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='top_movers',
    start_date=datetime(2025, 5, 3),
    schedule_interval= "30 16 * * 1-5" ,
    catchup=False,
) as dag:

 


    execute_dbt_task = BashOperator(
        task_id='dbt_run_ohlcv_models',
        bash_command='cd /dbt && dbt run --select top_movers --profiles-dir . --target prod'
    )

    execute_dbt_task
