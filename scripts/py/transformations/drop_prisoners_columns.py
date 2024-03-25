from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
from pyspark.sql.types import IntegerType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Dropping suppressed columns or columns with a \
                 high percentage of missing values") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/raw/prisoners.tsv', inferSchema=True, 
                    header=True, sep='\t')

num_of_rows = df.count()

print('Determining columns with supressed or missing values...')
suppressed_cols = []
for column in df.columns:

    if df.schema[column].dataType == IntegerType():
       current_column  = df.select(col(column))
    else:
        current_column = df.select(col(column).cast(IntegerType()).alias(column))

    suppressed_or_missing_values = current_column.filter(
                            (col(column) == 9) |
                            (col(column) == 99) |
                            (col(column) == 999) |
                            (col(column) == 9999) |
                            isnan(col(column)) |
                            isnull(col(column))) \
                            .count()
    if suppressed_or_missing_values >= int(0.9 * float(num_of_rows)):
        suppressed_cols.append(column)
        print('added ', column)

print("Dropping columns:...")
df = df.drop(*suppressed_cols)

df.write.csv('hdfs://namenode:9000/curated/prisoners-df')
