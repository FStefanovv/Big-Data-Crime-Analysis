from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Top 3 crime types by race and their share") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.createOrReplaceTempView('prisoners')

query = """
    WITH grouped_by_race AS (
        SELECT RACE, OFFENSE_TYPE, COUNT(*) AS type_count
        FROM prisoners
        GROUP BY RACE, OFFENSE_TYPE
    ), total_per_race AS (
        SELECT RACE, COUNT(*) AS total_count
        FROM prisoners
        GROUP BY RACE
    ), ranked_per_race AS (
        SELECT RACE, OFFENSE_TYPE, type_count,
        DENSE_RANK() OVER(PARTITION BY RACE ORDER BY type_count DESC) AS RANKING
        FROM grouped_by_race
    )

    SELECT RACE, OFFENSE_TYPE, ROUND(100 * type_count / total_count, 2) AS CRIME_PERCENTAGE
    FROM ranked_per_race  NATURAL JOIN total_per_race
    WHERE RANKING <=3
    ORDER BY total_count DESC, RANKING ASC;
"""

result_df = spark.sql(query)

result_df.show()


properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='top_3_crimes_by_race', properties=properties, mode='overwrite')