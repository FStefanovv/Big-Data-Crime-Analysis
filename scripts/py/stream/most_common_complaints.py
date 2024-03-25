import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType, LongType, FloatType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_to_database_and_console(df, batch_id):
    df.write \
        .jdbc(url='jdbc:postgresql://citus-coordinator:5432/streaming_results', 
              table='most_common_complaints', 
              mode='append', 
              properties={"user": 'postgres', "password": 'postgres', "driver": "org.postgresql.Driver"})
    
    df.show(truncate=False)

spark = SparkSession \
    .builder \
    .appName("Most frequent complaints") \
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
    StructField('CMPLNT_NUM', StringType(), True),
    StructField('CMPLNT_FR_DT', StringType(), True),
    StructField('CMPLNT_FR_TM', StringType(), True),
    StructField('CMPLNT_TO_DT', StringType(), True),
    StructField('CMPLNT_TO_TM', StringType(), True),
    StructField('ADDR_PCT_CD', LongType(), True),  
    StructField('RPT_DT', StringType(), True),
    StructField('KY_CD', LongType(), True),  
    StructField('OFNS_DESC', StringType(), True),
    StructField('PD_CD', FloatType(), True), 
    StructField('PD_DESC', StringType(), True),
    StructField('CRM_ATPT_CPTD_CD', StringType(), True),
    StructField('LAW_CAT_CD', StringType(), True),
    StructField('BORO_NM', StringType(), True),
    StructField('LOC_OF_OCCUR_DESC', StringType(), True),
    StructField('PREM_TYP_DESC', StringType(), True),
    StructField('JURIS_DESC', StringType(), True),
    StructField('JURISDICTION_CODE', LongType(), True), 
    StructField('PARKS_NM', StringType(), True),
    StructField('HADEVELOPT', StringType(), True),
    StructField('HOUSING_PSA', StringType(), True),
    StructField('X_COORD_CD', LongType(), True), 
    StructField('Y_COORD_CD', LongType(), True), 
    StructField('SUSP_AGE_GROUP', StringType(), True),
    StructField('SUSP_RACE', StringType(), True),
    StructField('SUSP_SEX', StringType(), True),
    StructField('TRANSIT_DISTRICT', FloatType(), True),  
    StructField('Latitude', FloatType(), True),  
    StructField('Longitude', FloatType(), True), 
    StructField('Lat_Lon', StringType(), True),
    StructField('PATROL_BORO', StringType(), True),
    StructField('STATION_NAME', StringType(), True),
    StructField('VIC_AGE_GROUP', StringType(), True),
    StructField('VIC_RACE', StringType(), True),
    StructField('VIC_SEX', StringType(), True)
])

complaints_casted = complaints.selectExpr("cast(value as string) as value")

complaints_final = complaints_casted.withColumn("value", from_json(complaints_casted["value"], complaint_schema)).select("value.*") 

windowedCounts = complaints_final \
    .withColumn("CMPLNT_FR_TM", col("CMPLNT_FR_TM").cast("timestamp")) \
    .groupBy(window(col("CMPLNT_FR_TM"), "1 hour").alias("window"), col("OFNS_DESC").alias("crime")) \
    .agg(count("*").alias("complaints_count")) \
    .withColumn("window_str", col("window").cast("string")) \
    .withColumn("timestamp", current_timestamp()) \
    .select("window_str", "crime", "complaints_count", "timestamp") \
    .orderBy('window_str', desc('complaints_count'))

query = windowedCounts.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_database_and_console) \
    .start()

query.awaitTermination()