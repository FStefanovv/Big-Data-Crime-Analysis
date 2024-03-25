import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, FloatType, LongType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)



spark = SparkSession \
    .builder \
    .appName("Save incoming complaints to datalake") \
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


complaints = complaints.selectExpr("cast(value as string) as value")

complaints = complaints.withColumn("value", from_json(complaints["value"], complaint_schema)).select("value.*") 

query = complaints.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/raw/complaints_stream") \
    .option("checkpointLocation", "hdfs://namenode:9000/sss_checkpoints/complaints_stream") \
    .start()

query.awaitTermination()