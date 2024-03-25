import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_to_database_and_console(df, batch_id):
    df.write \
        .jdbc(url='jdbc:postgresql://citus-coordinator:5432/streaming_results', 
              table='complaints_per_hour_and_borough', 
              mode='append', 
              properties={"user": 'postgres', "password": 'postgres', "driver": "org.postgresql.Driver"})
    
    df.show(truncate=False)


spark = SparkSession \
    .builder \
    .appName("Complaints per hour and borough") \
    .getOrCreate()

quiet_logs(spark)

complaints =  spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka1:19092") \
                .option("subscribe", os.getenv('TOPIC', 'complaints')) \
                .option("startingOffsets", "earliest") \
                .load()

complaint_schema = StructType([
    StructField('CMPLNT_FR_DT', StringType(), True),
    StructField('CMPLNT_FR_TM', StringType(), True),
    StructField('BORO_NM', StringType(), True)
])

complaints = complaints.selectExpr("cast(value as string) as value")

complaints = complaints.withColumn("value", from_json(complaints["value"], complaint_schema)).select("value.*") 

complaints = complaints.withColumn("CMPLNT_FR_TM", col("CMPLNT_FR_TM").cast("timestamp"))

windowedCounts = complaints \
    .groupBy(window(col("CMPLNT_FR_TM"), "1 hour").alias("window"), col("boro_nm")) \
    .agg(count("*").alias("complaints_count"))\
    .withColumn("timestamp", current_timestamp())\
    .withColumn("window_str", col("window").cast("string"))\
    .select("window_str", "boro_nm", "complaints_count", "timestamp")\
    .orderBy('window_str', desc('complaints_count'))

query = windowedCounts \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_database_and_console)\
    .start()

query.awaitTermination()
