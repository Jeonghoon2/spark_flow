from pyspark.sql import SparkSession
from pyspark.sql.functions import size

spark : SparkSession = SparkSession.builder.appName("ParsingParquet").getOrCreate()

df = spark.read.option("multiline","true").json("/Users/DE32/mov/datas/act=probing1/year=2015/data.json")

ccdf = df.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

df.show()

if len(ccdf.collect()) > 0 and len(ccdf.collect()[0]['companys']) > 0:
    companyCd = ccdf.collect()[0]['companys'][0]['companyCd']
    print(companyCd) 
else:
    print("No data available for companys")

from pyspark.sql.functions import explode, size
edf = df.withColumn("company", explode("companys"))

eedf = edf.withColumn("director", explode("directors"))

eedf.write.mode("overwrite").parquet("/Users/DE32/mov/parquetdata")

spark.stop()
