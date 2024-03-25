from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf() \
    .setAppName("Victim stats") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

df = spark.read.csv('hdfs://namenode:9000/consumption/prisoners_batch_queries', inferSchema=True, header=True)

df.createOrReplaceTempView('prisoners')

query = """
    WITH violent_crimes AS (
        SELECT VIOLENT_TYPE, KNEW_VICTIM, VICTIM_SEX, VICTIM_AGE_GROUP
        FROM prisoners
        WHERE OFFENSE_TYPE='VIOLENT_OFFENSE' AND VIOLENT_TYPE IS NOT NULL
    ), knew_victim_and_counts AS (
        SELECT VIOLENT_TYPE, COUNT(*) AS cnt, ROUND(100 * AVG(KNEW_VICTIM), 2) AS knew_vict_perc
        FROM violent_crimes
        GROUP BY VIOLENT_TYPE
    ), age_grouped AS (
        SELECT VIOLENT_TYPE, VICTIM_SEX, VICTIM_AGE_GROUP, COUNT(*) AS age_group_cnt
        FROM violent_crimes
        GROUP BY VIOLENT_TYPE, VICTIM_SEX, VICTIM_AGE_GROUP
    ), age_grouped_max AS (
        SELECT VIOLENT_TYPE, VICTIM_SEX, VICTIM_AGE_GROUP
        FROM age_grouped a
        WHERE age_group_cnt = (SELECT MAX(age_group_cnt) FROM age_grouped ag WHERE a.VIOLENT_TYPE=ag.VIOLENT_TYPE AND a.VICTIM_SEX=ag.VICTIM_SEX)
    )

    SELECT v.VIOLENT_TYPE, COALESCE(v.VICTIM_SEX, 'unspecified') as vict_gender, ROUND(100 * COUNT(*)/cnt, 2) AS gender_share, knew_vict_perc, ag.VICTIM_AGE_GROUP
    FROM violent_crimes v NATURAL JOIN knew_victim_and_counts LEFT OUTER JOIN  age_grouped_max ag ON ag.VIOLENT_TYPE=v.VIOLENT_TYPE AND ag.VICTIM_SEX=v.VICTIM_SEX
    GROUP BY v.VIOLENT_TYPE, v.VICTIM_SEX, knew_vict_perc, cnt, ag.VICTIM_AGE_GROUP
    ORDER BY cnt DESC, gender_share DESC 
"""

result_df = spark.sql(query)

result_df.show()

properties = {"user":'postgres', "password":'postgres', "driver": "org.postgresql.Driver"}
result_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/prisoners', table='victim_stats', properties=properties, mode='overwrite')