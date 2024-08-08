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
        'movie_spark',
        default_args={
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(seconds=3)
        },
        max_active_runs=1,
        max_active_tasks=3,
        description='movie',
        schedule="0 0 * * *",
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 1, 5),
        catchup=True,
        tags=['api', 'movie', 'amt'],
) as dag:


    start = BashOperator(
        task_id="start",
        bash_command="echo 'start'"
    )

    def re_partition_func():
        pass

    def join_df_func():
        pass

    def agg_func():
        pass

    re_partition = PythonVirtualenvOperator(
        task_id="re.partition",
        python_callable=re_partition_func,
        # requirements=["git+https://github.com/DE32-Team-Two/Extract.git@d2.0.0/parquet"],
        system_site_packages=False,
        
    )
    
    join_df = PythonVirtualenvOperator(
        task_id="join.df",
        python_callable=join_df_func,
        # requirements=["git+https://github.com/DE32-Team-Two/Extract.git@d2.0.0/parquet"],
        system_site_packages=False,
    )

    agg = PythonVirtualenvOperator(
        task_id="agg",
        python_callable=agg_func,
        # requirements=["git+https://github.com/DE32-Team-Two/Extract.git@d2.0.0/parquet"],
        system_site_packages=False,
    )


    end = BashOperator(
        task_id="end",
        bash_command="echo 'end'",
		trigger_rule="one_success"
    )

    start >> re_partition >> join_df >> agg >> end
