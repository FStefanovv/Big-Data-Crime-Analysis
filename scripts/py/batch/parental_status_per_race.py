from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Criminal history in family by race") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.printSchema()

df.createOrReplaceTempView('prisoners')

query = """
  WITH race_and_parental_status AS (
    SELECT DISTINCT RACE, PARENTAL_STATUS,
    COUNT(*) OVER(PARTITION BY RACE, PARENTAL_STATUS) AS cnt,
    COUNT(*) OVER(PARTITION BY RACE) as race_cnt
    FROM prisoners
  ), ranked AS (
    SELECT RACE, PARENTAL_STATUS, ROUND(100*cnt/race_cnt, 2) AS percentage, 
    DENSE_RANK() OVER(PARTITION BY RACE ORDER BY cnt DESC) AS ranking
    FROM race_and_parental_status
  )

  SELECT * FROM ranked
  WHERE ranking <=5 
  ORDER BY RACE, ranking
"""

result_df = spark.sql(query)

result_df.show(n=40)

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='parental_status_per_race', properties=properties, mode='overwrite')