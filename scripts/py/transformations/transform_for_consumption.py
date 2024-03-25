from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, when, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

race_mapping = {
    1: "HISPANIC",
    2: "WHITE",
    3: "BLACK",
    4: "AMERICAN_INDIAN",
    5: "ASIAN",
    6: "PACIFIC_ISLANDER",
    7: "OTHER"
}

def map_marital_status(value):
    marital_status_mapping = {
        1: "MARRIED",
        2: "WIDOWED",
        3: "DIVORCED",
        4: "SEPARATED",
        5: "NEVER_MARRIED"
    }
    return marital_status_mapping.get(value, None)

def map_offense_type(value):
    offense_mapping = {
        0: None,
        1: "VIOLENT_OFFENSE",
        2: "PROPERTY_OFFENSE",
        3: "DRUG_OFFENSE",
        4: "PUBLIC_ORDER_OFFENSE",
        5: "OTHER_OFFENSE"
    }
    return offense_mapping.get(value, None)

def map_offense_type_v0069(value):
    offense_mapping_v0069 = {
        1: "RAPE",
        2: "MURDER_MANSLAUGHTER_HOMICIDE",
        3: "OTHER_VIOLENT_OFFENSE"
    }
    return offense_mapping_v0069.get(value, None)

def map_sentence_type(value):
    sentence_type_mapping = {
        1: "FLAT",
        2: "RANGE"
    }
    return sentence_type_mapping.get(value, None)


def map_vicitm_sex(value):
    victim_sex_mapping = {
        1: "MALE",
        2: "FEMALE"
    }
    return victim_sex_mapping.get(value, None)


def map_flat_type(value):
    flat_type_mapping = {
        1: "LIFE",
        2: "LIFE_PLUS_ADDITIONAL_YEARS",
        3: "LIFE_WITHOUT_PAROLE",
        4: "DEATH",
        5: "INTERMITTENT",
        6: "SPECIFY_AMOUNT_OF_TIME"
    }
    return flat_type_mapping.get(value, None)

def map_education_level(value):
    education_mapping = {
        1: "ELEMENTARY_FIRST",
        2: "ELEMENTARY_SECOND",
        3: "ELEMENTARY_THIRD",
        4: "ELEMENTARY_FOURTH",
        5: "ELEMENTARY_FIFTH",
        6: "ELEMENTARY_SIXTH",
        7: "ELEMENTARY_SEVENTH",
        8: "ELEMENTARY_EIGHTH",
        9: "HIGH_SCHOOL_NINTH",
        10: "HIGH_SCHOOL_TENTH",
        11: "HIGH_SCHOOL_ELEVENTH",
        12: "HIGH_SCHOOL_TWELFTH",
        13: "COLLEGE_FRESHMAN",
        14: "COLLEGE_SOPHOMORE",
        15: "COLLEGE_JUNIOR",
        16: "COLLEGE_SENIOR",
        17: "GRADUATE_SCHOOL_ONE_YEAR",
        18: "GRADUATE_SCHOOL_TWO_OR_MORE_YEARS",
        30: "ATTENDED_SCHOOL_IN_ANOTHER_COUNTRY"
    }
    return education_mapping.get(value, None)

def map_parental_status(value):
    parental_status_mapping = {
        1: "Both Parents",
        2: "Mother",
        3: "Father",
        4: "Split Time Between Both Parents",
        5: "Foster Parent(s) / Foster Home",
        6: "Grandmother",
        7: "Grandfather",
        8: "Both Grandparents",
        9: "Other Relative(s)",
        10: "Friends",
        11: "Incarcerated",
        12: "Agency or Institution",
        13: "Someone Else"
    }
    return parental_status_mapping.get(value, None)

def map_age_group(value):
    age_group_mapping = {
        1: 'Under 12 Years',
        2: '12 to 17',
        3: '18 to 24',
        4: '25 to 34',
        5: '35 to 54',
        6: '55 or Older'
    }
    return age_group_mapping.get(value, None)



conf = SparkConf() \
    .setAppName("Transforming curated data into format ready for querying") \
    .setMaster("spark://spark-master:7077")  
ctx = SparkContext().getOrCreate(conf)
spark = SparkSession(ctx)

quiet_logs(spark)

original_df = spark.read.csv('hdfs://namenode:9000/curated/prisoners-df', inferSchema=True, header=True)


transformation_df = original_df.select(
    col("V0015").cast("int"),
    col("V0016").cast("int"),
    col("V0017").cast("int"),
    col("V0018").cast("int"),
    col("V0019").cast("int"),
    col("V0020").cast("int"),
    col("V0021").cast("int"),
    col("V0022").cast("int"),
    col("V0055Y").cast("int"), 
    col("V0056Y").cast("int"),
    col("V0062").cast("int"),
    col("V0069").cast("int"),
    col("V0390").cast("int"),  
    col("V0391").cast("int"),  
    col("V0392").cast("int"),    
    col("V0393").cast("int"),    
    col("V0394").cast("int"),    
    col("V0400").cast("int"),
    col("V0401").cast("int"),
    col("V0402").cast("int"),
    col("V0403").cast("int"),
    col("V0404").cast("int"),
    col("V0405").cast("int"),   
    col("V0406").cast("int"),   
    col("V0407").cast("int"),   
    col("V0408").cast("int"),   
    col("V0409").cast("int"), 
    col("V0410").cast("int"),  
    col("V0491").cast("int"),
    col("V1168").cast("int"),
    col("V1179").cast("int"),
    col("V1180").cast("int"),
    col("V1181").cast("int"),
    col("V1182").cast("int"),
    col("V1183").cast("int"),
    col("V1184").cast("int"),
    col("V1172").cast("int"),
    col("V1173").cast("int"),
    col("V1174").cast("int"),
    col("V0935").cast("int"),
    col("V0480").cast("int"),
    col("V0489").cast("int"),
    col("V0490").cast("int")
)

transformation_df = transformation_df.withColumn("RACE",
    when(col("V0015") == 1, race_mapping[1])
    .when(col("V0016") == 1, race_mapping[2])
    .when(col("V0017") == 1, race_mapping[3])
    .when(col("V0018") == 1, race_mapping[4])
    .when(col("V0019") == 1, race_mapping[5])
    .when(col("V0020") == 1, race_mapping[6])
    .when(col("V0021") == 1, race_mapping[7])
    .otherwise("OTHER")
)

map_marital_status_udf = udf(map_marital_status, StringType())
transformation_df = transformation_df.withColumn("MARITAL_STATUS", map_marital_status_udf(col("V0022")))

transformation_df = transformation_df.withColumnRenamed("V0055Y", "ARREST_YEAR")
transformation_df = transformation_df.withColumnRenamed("V0056Y", "ADMISSION_YEAR")
transformation_df = transformation_df.withColumnRenamed("V0390", "JAIL_TIME")
transformation_df = transformation_df.withColumnRenamed("V0480", "VICTIMS_NUM")
transformation_df = transformation_df.withColumnRenamed("V0491", "KNEW_VICTIM")

map_offense_type_udf = udf(map_offense_type, StringType())
transformation_df = transformation_df.withColumn("OFFENSE_TYPE", map_offense_type_udf(col("V0062")))

map_violent_type_udf = udf(map_offense_type_v0069, StringType())
transformation_df = transformation_df.withColumn("VIOLENT_TYPE", map_violent_type_udf(col("V0069")))

map_sentence_type_udf = udf(map_sentence_type, StringType())
transformation_df = transformation_df.withColumn("SENTENCE_TYPE", map_sentence_type_udf(col("V0400")))

map_flat_type_udf = udf(map_flat_type, StringType())
transformation_df = transformation_df.withColumn("FLAT_TYPE", map_flat_type_udf(col("V0401")))



map_victim_sex_udf = udf(map_vicitm_sex, StringType())
transformation_df = transformation_df.withColumn("VICTIM_SEX", map_victim_sex_udf(col("V0489")))


transformation_df = transformation_df.withColumn("JAIL_TIME_TOTAL_DAYS",
    col("V0391") * 365 + col("V0392") * 30 + col("V0393") * 7 + col("V0394")
)

transformation_df = transformation_df.withColumnRenamed("V0402", "FLAT_YEARS")
transformation_df = transformation_df.withColumnRenamed("V0403", "FLAT_MONTHS")
transformation_df = transformation_df.withColumnRenamed("V0404", "FLAT_DAYS")

cols_range_sentence = ['V0405', 'V0406', 'V0407', 'V0408', 'V0409', 'V0410']
new_column_names = ['MIN_YEARS_RANGE', 'MAX_YEARS_RANGE', 'MIN_MONTHS_RANGE', 'MAX_MONTHS_RANGE', 'MIN_DAYS_RANGE', 'MAX_DAYS_RANGE']
for old_col, new_col in zip(cols_range_sentence, new_column_names):
    transformation_df = transformation_df.withColumnRenamed(old_col, new_col)

transformation_df = transformation_df.withColumn("KNEW_VICTIM", when(col("KNEW_VICTIM") == 2, 0).otherwise(col("KNEW_VICTIM")))

map_parental_status_udf = udf(map_parental_status, StringType())
transformation_df = transformation_df.withColumn("PARENTAL_STATUS", map_parental_status_udf(col("V1168")))

map_age_group_udf = udf(map_age_group, StringType())
transformation_df = transformation_df.withColumn("VICTIM_AGE_GROUP", map_age_group_udf(col("V0490")))

transformation_df = transformation_df.withColumn("FATHER_SERVED", when(col("V1172") == 2, 0).otherwise(col("V1172")))
transformation_df = transformation_df.withColumn("MOTHER_SERVED", when(col("V1173") == 2, 0).otherwise(col("V1173")))
transformation_df = transformation_df.withColumn("SIBLING_SERVED", when(col("V1174") == 2, 0).otherwise(col("V1174")))

transformation_df = transformation_df.withColumnRenamed("V1179", "NERVOUSNESS") \
    .withColumnRenamed("V1180", "HOPELESSNESS") \
    .withColumnRenamed("V1181", "RESTLESSNESS") \
    .withColumnRenamed("V1182", "DEPRESSION") \
    .withColumnRenamed("V1183", "EVERYTHING_AN_EFFORT") \
    .withColumnRenamed("V1184", "WORTHLESSNESS")

map_education_level_udf = udf(map_education_level, StringType())
transformation_df = transformation_df.withColumn("EDUCATION_LEVEL", map_education_level_udf(col("V0935")))

columns_to_drop = [col_name for col_name in transformation_df.columns if col_name.startswith("V0") or col_name.startswith("V1")]

transformation_df = transformation_df.drop(*columns_to_drop)

for col_name in transformation_df.columns:
    transformation_df = transformation_df.withColumn(col_name,
                                                     when(col(col_name) < 0, '').otherwise(col(col_name)))


transformation_df.write.mode('overwrite').option('header', 'true').csv('hdfs://namenode:9000/consumption/prisoners_batch_queries')