from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Print common patterns") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df_mapped_boroughs = spark.read.csv('hdfs://namenode:9000/consumption/common_patterns', inferSchema=True, header=True)
df_mapped_boroughs.createOrReplaceTempView('patterns')


query = """
  SELECT * FROM patterns ORDER BY BOROUGH
"""

result_df = spark.sql(query)

result_df.show(n=60)