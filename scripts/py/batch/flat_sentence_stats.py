from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Average flat sentence duration for crime type") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.printSchema()

df.createOrReplaceTempView('prisoners')

query = """
    WITH flat_sentences AS (
        SELECT OFFENSE_TYPE, (FLAT_YEARS * 365 + FLAT_MONTHS * 30 + FLAT_DAYS) AS duration_days
        FROM prisoners
        WHERE FLAT_DAYS IS NOT NULL
    )

    SELECT DISTINCT OFFENSE_TYPE, 
    AVG(duration_days) OVER(PARTITION BY OFFENSE_TYPE) AS AVG_DURATION_DAYS,
    MIN(duration_days) OVER(PARTITION BY OFFENSE_TYPE) AS MIN_DURATION_DAYS,
    MAX(duration_days) OVER(PARTITION BY OFFENSE_TYPE) AS MAX_DURATION_DAYS
    FROM flat_sentences
    WHERE duration_days>0
    ORDER BY MAX_DURATION_DAYS DESC
"""

result_df = spark.sql(query)

result_df.show()

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='flat_sentence_stats', properties=properties, mode='overwrite')