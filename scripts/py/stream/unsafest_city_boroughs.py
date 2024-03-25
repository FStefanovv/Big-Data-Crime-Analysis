import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType, LongType, FloatType

from pyspark.sql import functions as F


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_to_database_and_console(df, batch_id):
    df_with_window = df.withColumn("window_start", col("window.start")) \
                       .withColumn("window_end", col("window.end")) \
                       .withColumn("timestamp", current_timestamp())

    df_selected = df_with_window.select("window_start", "window_end", "Borough", "Felonies", "timestamp")

    df_selected.write \
        .jdbc(url='jdbc:postgresql://citus-coordinator:5432/streaming_results', 
              table='unsafest_boroughs', 
              mode='append', 
              properties={"user": 'postgres', "password": 'postgres', "driver": "org.postgresql.Driver"})
    
    df_with_window.show(truncate=False)


spark = SparkSession \
    .builder \
    .appName("Boroughs ranked by safety") \
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

#.where(col("OFNS_DESC").isin("MURDER & NON-NEGL. MANSLAUGHTER", "DANGEROUS WEAPONS", "SEX CRIMES", "FELONY ASSAULT")) \

complaints_final = complaints_final.withColumnRenamed("BORO_NM", "Borough")

complaints_final = complaints_final.select('Borough', 'LAW_CAT_CD', 'CMPLNT_FR_TM')

unsafest_boroughs = complaints_final \
    .where(col("LAW_CAT_CD") == "FELONY") \
    .where((col("Borough") != "UNKNOWN") & (col("Borough").isNotNull())) \
    .withColumn("CMPLNT_FR_TM", col("CMPLNT_FR_TM").cast("timestamp")) \
    .groupBy(window(col("CMPLNT_FR_TM"), "1 hour"), "Borough") \
    .agg(count("*").alias("Felonies")) \
    .withColumn("timestamp", current_timestamp()) \
    .orderBy("window", desc("Felonies"))\

query = unsafest_boroughs.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_database_and_console)\
    .start()

query.awaitTermination()
