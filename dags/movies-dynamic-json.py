import sys
from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,

)

with DAG(
        'movies_dynamic_json',
        default_args={
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(seconds=3)
        },
        max_active_runs=1,
        max_active_tasks=3,
        description='movie',
        schedule="0 0 * * *",
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2015, 5,1),
        catchup=True,
        tags=['api', 'movie', 'amt'],
) as dag:


    start = BashOperator(
        task_id="start",
        bash_command="echo 'start'"
    )

    def tmp():
        pass

    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=tmp,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    parsing_parquet = PythonVirtualenvOperator(
        task_id="parsing.parquet",
        python_callable=tmp,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    select_parquet = PythonVirtualenvOperator(
        task_id="select.parquet",
        python_callable=tmp,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.2.0/movie_flow"],
        system_site_packages=False,
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'end'",
		trigger_rule="one_success"
    )

    start >> get_data >> parsing_parquet >> select_parquet >> end
