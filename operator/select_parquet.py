from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import sys

spark : SparkSession = SparkSession.builder.appName("ParsingParquet").getOrCreate()

origin_df = spark.read.parquet(sys.argv[1])
origin_df.show()

# 감독별로 영화 수를 집계하고, 회사별로도 그룹화
df_grouped = origin_df.groupBy("director", "company").agg(count("*").alias("movie_count"))

# 결과를 출력하거나 저장
df_grouped.show()

spark.stop()

