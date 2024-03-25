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
  WITH race_share AS (
    SELECT RACE, COUNT(*) AS cnt
    FROM prisoners
    GROUP BY RACE
  )

  SELECT RACE, ROUND(100 * COUNT(*)/cnt, 2) AS PERCENTAGE
  FROM prisoners NATURAL JOIN race_share
  WHERE FATHER_SERVED=1 OR MOTHER_SERVED=1 OR SIBLING_SERVED=1
  GROUP BY RACE, cnt
  ORDER BY PERCENTAGE DESC
"""

result_df = spark.sql(query)

result_df.show()

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='family_criminal_history', properties=properties, mode='overwrite')