from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator
        )

os.environ['LC_ALL'] = 'C'

with DAG(
     'movies_dynamic_json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 12, 31),
    catchup=True,
    tags=['json', 'movies'],
) as dag:


    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    get_data = EmptyOperator(task_id='get.data')
    parsing_parquet = EmptyOperator(task_id='parsing.parquet')
    select_parquet = EmptyOperator(task_id='select.parquet')

    start >> get_data >> parsing_parquet >> select_parquet >> end
