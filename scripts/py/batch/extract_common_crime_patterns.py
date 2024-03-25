from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Extract common patterns") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df_mapped_boroughs = spark.read.csv('hdfs://namenode:9000/consumption/mapped_boroughs', inferSchema=True, header=True)
df_mapped_boroughs.createOrReplaceTempView('mapped_boroughs')

df_prisoners = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)
df_prisoners.createOrReplaceTempView('prisoners')

query = """
    WITH total_per_borough AS (
        SELECT BOROUGH, COUNT(*) AS cnt
        FROM mapped_boroughs
        GROUP BY BOROUGH
    ), crime_percentages AS (
        SELECT RACE, BOROUGH, OFFENSE_TYPE, ROUND(COUNT(*)/cnt, 2) AS percentage
        FROM mapped_boroughs NATURAL JOIN total_per_borough
        GROUP BY RACE, BOROUGH, OFFENSE_TYPE, cnt
    ), ranked AS (
        SELECT BOROUGH, RACE, OFFENSE_TYPE,
        DENSE_RANK() OVER(PARTITION BY BOROUGH ORDER BY percentage DESC) AS ranking
        FROM crime_percentages
    )

    SELECT * from ranked WHERE ranking<=4 ORDER BY BOROUGH, ranking
"""

result_df = spark.sql(query)

result_df.show(n=60)

result_df.write.csv(path='hdfs://namenode:9000/consumption/common_patterns', \
                    mode='overwrite', header=True)