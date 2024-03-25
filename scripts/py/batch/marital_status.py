from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Marital status") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.createOrReplaceTempView('prisoners')

query = """
    WITH prisoner_count AS (
        SELECT COUNT(*) FROM prisoners
    )

    SELECT MARITAL_STATUS AS Race, ROUND(100 * COUNT(*)/(SELECT * FROM prisoner_count), 2) AS Percentage
    FROM prisoners
    GROUP BY MARITAL_STATUS
    ORDER BY Percentage DESC
"""

result_df = spark.sql(query)

result_df.show()

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='marital_status', properties=properties, mode='overwrite')