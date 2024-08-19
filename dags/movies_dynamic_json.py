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
        schedule='@once',
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2015, 5,1),
        catchup=True,
        tags=['api', 'movie', 'amt'],
) as dag:


    start = BashOperator(
        task_id="start",
        bash_command="echo 'start'"
    )

    def _get_data_():
        from movdata.get_list_data import ListDataSave

        ild = ListDataSave()

        params = {
            "openStartDt": "2015",
            "openEndDt": "2015"
        }

        ild.set.base_url("https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json")
        ild.set.params(params)
        ild.set.max_page(30)
        ild.set.json_col_name('movieListResult','movieList')
        ild.set.partition(act='probing1', year='2015')

        ild.run()

    


    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=_get_data_,
        requirements=["git+https://github.com/Jeonghoon2/movdata.git@ver/0.5"],
        system_site_packages=False,
    )

    parsing_parquet_submit_file = "~/code/spark_flow/operator/parsing_parquet.py"
    read_json_file_path = "/Users/DE32/mov/datas/act=probing1/year=2015/data.json"
    parsing_parquet = BashOperator(
        task_id="parsing.parquet",
        bash_command=f"""
        $SPARK_HOME/bin/spark-submit {parsing_parquet_submit_file} {read_json_file_path}
        """
    )

    select_parquet_file = "~/code/spark_flow/operator/select_parquet.py"
    select_target_parquet = "~/mov/parquetdata"
    select_parquet = BashOperator(
        task_id="select.parquet",
        bash_command=f"""
        $SPARK_HOME/bin/spark-submit {select_parquet_file} {select_target_parquet}
        """
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'end'",
		trigger_rule="one_success"
    )

    start >> get_data >> parsing_parquet >> select_parquet >> end
