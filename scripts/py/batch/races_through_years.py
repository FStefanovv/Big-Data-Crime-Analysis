from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Race share through years") \
    .setMaster("spark://spark-master:7077") \
    
conf.set('spark.jars', '/spark/jars/postgresql-42.7.2.jar')
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.printSchema()

df.createOrReplaceTempView('prisoners')

query = """
    WITH counts_per_year AS (
        SELECT DISTINCT ADMISSION_YEAR, COUNT(*) OVER(PARTITION BY ADMISSION_YEAR) AS total_yearly_count
        FROM prisoners
        WHERE ADMISSION_YEAR BETWEEN 2008 AND 2016
    ), race_count_per_year AS (
        SELECT DISTINCT RACE, ADMISSION_YEAR, COUNT(*) OVER(PARTITION BY RACE, ADMISSION_YEAR) AS yearly_race_count
        FROM prisoners 
        WHERE ADMISSION_YEAR BETWEEN 2008 AND 2016
    ), race_share_yearly AS (
        SELECT RACE, ADMISSION_YEAR, ROUND(100 * yearly_race_count/total_yearly_count, 2) AS share
        FROM race_count_per_year NATURAL JOIN counts_per_year
    )

    SELECT ADMISSION_YEAR, RACE, yearly_race_count, share,
    COALESCE(ROUND(share - LAG(share) OVER(PARTITION BY RACE ORDER BY ADMISSION_YEAR), 2), 'N/A') AS CHANGE
    FROM race_share_yearly NATURAL JOIN race_count_per_year
    ORDER BY ADMISSION_YEAR, share DESC
"""


result_df = spark.sql(query)

result_df.show(n=70)

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='races_through_years', properties=properties, mode='overwrite')