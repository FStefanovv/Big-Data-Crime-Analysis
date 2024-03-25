from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, udf, when
from pyspark.sql.types import IntegerType, StringType
import random

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

@udf(StringType())
def roulette_selection(bronx_prob, brooklyn_prob, manhattan_prob, queens_prob, staten_island_prob, unknown_prob):
    random_num = random.random()

    take_random = random.random()

    if take_random <= 0.1:
        boroughs = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNKNOWN"]
        random_borough = random.choice(boroughs)

        return random_borough    
    
    bronx_prob = bronx_prob if bronx_prob is not None else 0.0
    brooklyn_prob = brooklyn_prob if brooklyn_prob is not None else 0.0
    manhattan_prob = manhattan_prob if manhattan_prob is not None else 0.0
    queens_prob = queens_prob if queens_prob is not None else 0.0
    staten_island_prob = staten_island_prob if staten_island_prob is not None else 0.0
    unknown_prob = unknown_prob if unknown_prob is not None else 0.0

    if random_num <= bronx_prob:
        return "BRONX"
    elif random_num <= bronx_prob + brooklyn_prob:
        return "BROOKLYN"
    elif random_num <= bronx_prob + brooklyn_prob + manhattan_prob:
        return "MANHATTAN"
    elif random_num <= bronx_prob + brooklyn_prob + manhattan_prob + queens_prob:
        return "QUEENS"
    elif random_num <= bronx_prob + brooklyn_prob + manhattan_prob + queens_prob + staten_island_prob:
        return "STATEN ISLAND"
    else:
        return "UNKNOWN"


conf = SparkConf() \
    .setAppName("Dropping suppressed columns or columns with a \
                 high percentage of missing values") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

prisoners_df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, 
                    header=True)

prisoners_df = prisoners_df.select('OFFENSE_TYPE', 'RACE')

boroughs_df = spark.read.csv('hdfs://namenode:9000/raw/cummulative_boroughs.csv', inferSchema=True, 
                    header=True)

joined_df = prisoners_df.join(boroughs_df,
                               (prisoners_df['OFFENSE_TYPE'] == boroughs_df['PD_DESC_Mapped']) &
                               (prisoners_df['RACE'] == boroughs_df['SUSP_RACE_Mapped']),
                               'left_outer')

joined_df = joined_df.withColumn("BRONX_cumulative_prob", 
                                 col("BRONX_cumulative_prob") / 100.0)
joined_df = joined_df.withColumn("BROOKLYN_cumulative_prob", 
                                 (col("BRONX_cumulative_prob") + col("BROOKLYN_cumulative_prob")) / 100.0)
joined_df = joined_df.withColumn("MANHATTAN_cumulative_prob", 
                                 (col("BROOKLYN_cumulative_prob") + col("MANHATTAN_cumulative_prob")) / 100.0)
joined_df = joined_df.withColumn("QUEENS_cumulative_prob", 
                                 (col("MANHATTAN_cumulative_prob") + col("QUEENS_cumulative_prob")) / 100.0)
joined_df = joined_df.withColumn("STATEN ISLAND_cumulative_prob", 
                                 (col("QUEENS_cumulative_prob") + col("STATEN ISLAND_cumulative_prob")) / 100.0)
joined_df = joined_df.withColumn("UNKNOWN_cumulative_prob", 
                                 (col("STATEN ISLAND_cumulative_prob") + col("UNKNOWN_cumulative_prob")) / 100.0)


joined_df = joined_df.withColumn("BOROUGH", 
                                 roulette_selection( 
                                                    joined_df['BRONX_cumulative_prob'],
                                                    joined_df['BROOKLYN_cumulative_prob'],
                                                    joined_df['MANHATTAN_cumulative_prob'],
                                                    joined_df['QUEENS_cumulative_prob'],
                                                    joined_df['STATEN ISLAND_cumulative_prob'],
                                                    joined_df['UNKNOWN_cumulative_prob']))

extracted_cols = joined_df.select('OFFENSE_TYPE', 'RACE', 'BOROUGH')

extracted_cols.write.csv('hdfs://namenode:9000/consumption/mapped_boroughs', header=True, mode='overwrite')

extracted_cols.show(n=10)