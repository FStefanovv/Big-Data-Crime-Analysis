from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Feelings used to describe the experience of prisoners in the past 30 days") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.printSchema()

df.createOrReplaceTempView('prisoners')

query = """
    WITH feelings AS (
        SELECT 'NERVOUSNESS' AS FEELING, NERVOUSNESS AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY NERVOUSNESS
        UNION ALL
        SELECT 'EVERYTHING_AN_EFFORT' AS FEELING, EVERYTHING_AN_EFFORT AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY EVERYTHING_AN_EFFORT
        UNION ALL   
        SELECT 'WORTHLESSNESS' AS FEELING, WORTHLESSNESS AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY WORTHLESSNESS
        UNION ALL
        SELECT 'DEPRESSION' AS FEELING, DEPRESSION AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY DEPRESSION
        UNION ALL
        SELECT 'HOPELESSNESS' AS FEELING, HOPELESSNESS AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY HOPELESSNESS
        UNION ALL
        SELECT 'RESTLESSNESS' AS FEELING, RESTLESSNESS AS VALUE, COUNT(*) AS cnt FROM prisoners GROUP BY RESTLESSNESS
    ), 
    
    feeling_counts AS (
        SELECT 'NERVOUSNESS' AS FEELING, COUNT(*) AS feeling_cnt FROM prisoners WHERE NERVOUSNESS IS NOT NULL
        UNION ALL
        SELECT 'EVERYTHING_AN_EFFORT' AS FEELING, COUNT(*) AS feeling_cnt FROM prisoners WHERE EVERYTHING_AN_EFFORT IS NOT NULL
        UNION ALL   
        SELECT 'WORTHLESSNESS' AS FEELING,  COUNT(*) AS feeling_cnt FROM prisoners WHERE WORTHLESSNESS IS NOT NULL
        UNION ALL
        SELECT 'DEPRESSION' AS FEELING, COUNT(*) AS feeling_cnt FROM prisoners WHERE DEPRESSION IS NOT NULL
        UNION ALL
        SELECT 'HOPELESSNESS' AS FEELING, COUNT(*) AS feeling_cnt FROM prisoners WHERE HOPELESSNESS IS NOT NULL
        UNION ALL
        SELECT 'RESTLESSNESS' AS FEELING, COUNT(*) AS feeling_cnt FROM prisoners WHERE RESTLESSNESS IS NOT NULL
    ), 
    
    feeling_value_freqs AS (
        SELECT DISTINCT FEELING, VALUE, 1/VALUE * (cnt / feeling_cnt) as feeling_freq_scaled
        FROM feelings NATURAL JOIN feeling_counts
        WHERE value IS NOT NULL
        GROUP BY FEELING, VALUE, feeling_cnt, cnt
    )
   
    SELECT FEELING, ROUND(SUM(feeling_freq_scaled), 2) AS summed_score
    from feeling_value_freqs        
    GROUP BY FEELING
    ORDER BY summed_score DESC
"""

result_df = spark.sql(query)

result_df.show()

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='most_common_feelings', properties=properties, mode='overwrite')