from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Race share scaled by crime") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.createOrReplaceTempView('prisoners')


query = """
   WITH race_crime_counts AS (
      SELECT DISTINCT RACE, OFFENSE_TYPE, 
      COUNT(*) OVER(PARTITION BY RACE, OFFENSE_TYPE) AS cnt
      FROM prisoners
    ), crime_counts AS (
      SELECT DISTINCT OFFENSE_TYPE, COUNT(*) OVER(PARTITION BY OFFENSE_TYPE) AS cnt
      FROM prisoners
    ), race_share AS (
      SELECT RACE, ROUND(COUNT(*) / (SELECT COUNT(*) FROM prisoners), 2) AS share
      FROM prisoners
      GROUP BY RACE
    ), race_indexes AS (
      SELECT cc.OFFENSE_TYPE, rcc.RACE, ROUND((rcc.cnt/cc.cnt) / rs.share, 2) AS RACE_INDEX, cc.cnt AS cnt
      FROM crime_counts AS cc LEFT OUTER JOIN 
      race_crime_counts AS rcc ON cc.OFFENSE_TYPE=rcc.OFFENSE_TYPE
      LEFT OUTER JOIN race_share AS rs ON rs.RACE=rcc.RACE
    )

    SELECT OFFENSE_TYPE, RACE, RACE_INDEX
    FROM race_indexes
    ORDER BY cnt DESC, RACE_INDEX DESC
"""


result_df = spark.sql(query)

result_df.show(n=50)

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='race_share_scaled_by_crime', properties=properties, mode='overwrite')