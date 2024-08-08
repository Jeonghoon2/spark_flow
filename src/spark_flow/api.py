import pandas as pd
import os
def repartition(load_dt, base_path='~/data2/extract'):
    home_dir = os.path.expanduser(base_path)
    read_dir = os.path.join(home_dir,f'load_dt={load_dt}')
    df = pd.read_parquet(read_dir)
    df[load_dt] = load_dt

    df.to_parquet(
        '~/data2/repartition',
        cols=['load_dt','multiMovieYn','repNationCd']
        )
    
    return df.size, read_dir

    
def join_df():
    # spark sql 을 활용(어제 작성한 제플린 코드) 하여 movie_join_df.py 을 $AIRFLOW_HOME/py 밑에 생성
    # spark-submit 을 사용 airflow bash operator 를 이용
    # 아래 pyspark 코드를 활용하여 HIVE 형식에 맞는 파티션 생성
    pass

def agg():
    # sparksql 을 사용하여 일별 독립영화 여부, 해외영화 여부에 대하여 각각 합을 구하기(누적은 제외 일별관객수, 수익 ... )
    # 위에서 구한 SUM 데이터를 "/home//data/movie/sum-multi", "/home//data/movie/sum-nation" 에 날짜를 파티션 하여 저장
    pass