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
    schedule="@yearly",
    start_date=datetime(2014, 1, 1),
    end_date=datetime(2021, 12, 31),
    catchup=True,
    tags=['json', 'movies'],
) as dag:
    

    def run(year):
        from movdata.ml_copy import save_movies, save_json
        import os
        home_path = os.path.expanduser("~")
        data = save_movies(year)
        file_path = f"{home_path}/data/movies_pagelimit/year={year}/data.json"
        d = save_json(data, file_path)
        print(data)
        return d

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=run,
            requirements=["git+https://github.com/hamsunwoo/movdata.git@0.2/movielist"],
            system_site_packages=False,
            op_args=["{{logical_date.strftime('%Y')}}"]
            )

    parsing_parquet = BashOperator(
            task_id='parsing.parquet',
            bash_command="""
                echo "parsing.parquet"
            """
            )

    select_parquet = BashOperator(
            task_id='select.parquet',
            bash_command="""
                echo "select.parquet"
            """
            )

    start >> get_data >> parsing_parquet >> select_parquet >> end
