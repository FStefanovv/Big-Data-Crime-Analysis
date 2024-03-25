import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType, LongType, FloatType

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_to_database(batch_df, batch_id):
    properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}
    batch_df.write.jdbc(url='jdbc:postgresql://citus-coordinator:5432/streaming_results',
                        table='uncommon_crimes',
                        properties=properties,
                        mode='append')
    
    batch_df.show(truncate=False)



@udf(StringType())
def map_offense_type(pd_desc):
    return offense_mapping.get(pd_desc, pd_desc)

@udf(StringType())
def map_race(susp_race):
    return race_mapping.get(susp_race, susp_race)


offense_mapping = {
    'HARASSMENT,SUBD 3,4,5': 'Violent_Offense',
    'HARASSMENT,SUBD 1,CIVILIAN': 'Violent_Offense',
    'LARCENY,PETIT FROM BUILDING,UNATTENDED, PACKAGE THEFT INSIDE': 'Property_Offense',
    'BURGLARY,RESIDENCE,DAY': 'Property_Offense',
    'MISCHIEF,CRIMINAL,    UNCL 2ND': 'Property_Offense',
    'MISCHIEF, CRIMINAL 4, OF MOTOR': 'Property_Offense',
    'CRIMINAL MISCHIEF,UNCLASSIFIED 4': 'Property_Offense',
    'CHILD, ENDANGERING WELFARE': 'Violent_Offense',
    'ROBBERY,OPEN AREA UNCLASSIFIED': 'Violent_Offense',
    'AGGRAVATED HARASSMENT 2': 'Violent_Offense',
    'LARCENY,PETIT FROM STORE-SHOPL': 'Property_Offense',
    'LARCENY,GRAND OF VEHICULAR/MOTORCYCLE ACCESSORIES': 'Property_Offense',
    'ASSAULT OTHER PUBLIC SERVICE EMPLOYEE': 'Violent_Offense',
    'LARCENY,PETIT FROM BUILDING,UN': 'Property_Offense',
    'LARCENY,GRAND OF AUTO': 'Property_Offense',
    'LARCENY,GRAND FROM STORE-SHOPL': 'Property_Offense',
    'LARCENY,GRAND FROM PERSON,PERSONAL ELECTRONIC DEVICE(SNATCH)': 'Property_Offense',
    'ASSAULT 3': 'Violent_Offense',
    'LARCENY,GRAND BY DISHONEST EMP': 'Property_Offense',
    'LARCENY,PETIT FROM BUILDING,UNATTENDED, PACKAGE THEFT OUTSIDE': 'Property_Offense',
    'THEFT OF SERVICES, UNCLASSIFIE': 'Property_Offense',
    'FORGERY,ETC.-MISD.': 'Other_Offense',
    'FORGERY,M.V. REGISTRATION': 'Other_Offense',
    'LARCENY,GRAND FROM EATERY, UNATTENDED': 'Property_Offense',
    'CONTROLLED SUBSTANCE,INTENT TO': 'Drug_Offense',
    'LARCENY,PETIT FROM AUTO': 'Property_Offense',
    'CONTROLLED SUBSTANCE, POSSESSI': 'Drug_Offense',
    'TRAFFIC,UNCLASSIFIED MISDEMEAN': 'Public_Order_Offense',
    'UNAUTH. SALE OF TRANS. SERVICE': 'Public_Order_Offense',
    'FORGERY,ETC.,UNCLASSIFIED-FELO': 'Other_Offense',
    'MISCHIEF, CRIMINAL 3 & 2, OF M': 'Property_Offense',
    'LARCENY,PETIT OF VEHICLE ACCES': 'Property_Offense',
    'LARCENY,PETIT FROM OPEN AREAS,': 'Property_Offense',
    'ASSAULT 2,1,UNCLASSIFIED': 'Violent_Offense',
    'CRIMINAL POSSESSION WEAPON': 'Violent_Offense',
    'LARCENY,PETIT OF LICENSE PLATE': 'Property_Offense',
    'ROBBERY,PERSONAL ELECTRONIC DEVICE': 'Violent_Offense',
    'LEAVING SCENE-ACCIDENT-PERSONA': 'Other_Offense',
    'LARCENY,PETIT BY CHECK USE': 'Property_Offense',
    'LARCENY,GRAND FROM OPEN AREAS, UNATTENDED': 'Property_Offense',
    'FORGERY,DRIVERS LICENSE': 'Other_Offense',
    'NY STATE LAWS,UNCLASSIFIED FEL': 'Other_Offense',
    'ROBBERY,CLOTHING': 'Violent_Offense',
    'BURGLARY,RESIDENCE,NIGHT': 'Property_Offense',
    'ASSAULT POLICE/PEACE OFFICER': 'Violent_Offense',
    'LARCENY,PETIT OF BICYCLE': 'Property_Offense',
    'LARCENY,GRAND FROM PERSON,LUSH WORKER(SLEEPING/UNCON VICTIM)': 'Property_Offense',
    'LARCENY,GRAND FROM RESIDENCE, UNATTENDED': 'Property_Offense',
    'WEAPONS POSSESSION 3': 'Violent_Offense',
    'LARCENY,GRAND BY IDENTITY THEFT-UNCLASSIFIED': 'Property_Offense',
    'CONTROLLED SUBSTANCE,POSSESS.': 'Drug_Offense',
    'LARCENY,PETIT BY CREDIT CARD U': 'Property_Offense',
    'LARCENY,GRAND BY BANK ACCT COMPROMISE-UNCLASSIFIED': 'Property_Offense',
    'BURGLARY,TRUCK DAY': 'Property_Offense',
    'LARCENY,GRAND BY FALSE PROMISE-NOT IN PERSON CONTACT': 'Property_Offense',
    'LARCENY,GRAND FROM BUILDING (NON-RESIDENCE) UNATTENDED': 'Property_Offense',
    'RECKLESS ENDANGERMENT 1': 'Violent_Offense',
    'LARCENY,GRAND BY EXTORTION': 'Property_Offense',
    'CRIMINAL CONTEMPT 1': 'Violent_Offense',
    'LARCENY,GRAND FROM PERSON,PICK': 'Property_Offense',
    'WEAPONS, POSSESSION, ETC': 'Violent_Offense',
    'FRAUD,UNCLASSIFIED-MISDEMEANOR': 'Other_Offense',
    'BURGLARY,COMMERCIAL,NIGHT': 'Property_Offense',
    'MENACING,UNCLASSIFIED': 'Violent_Offense',
    'LARCENY,GRAND FROM VEHICLE/MOTORCYCLE': 'Property_Offense',
    'TRESPASS 2, CRIMINAL': 'Public_Order_Offense',
    'ROBBERY,COMMERCIAL UNCLASSIFIED': 'Violent_Offense',
    'SODOMY 1': 'Violent_Offense',
    'NY STATE LAWS,UNCLASSIFIED MIS': 'Other_Offense',
    'OBSTR BREATH/CIRCUL': 'Violent_Offense',
    'BURGLARY,TRUCK NIGHT': 'Property_Offense',
    'ROBBERY,RESIDENTIAL COMMON AREA': 'Violent_Offense',
    'STRANGULATION 1ST': 'Violent_Offense',
    'UNAUTHORIZED USE VEHICLE 3': 'Property_Offense',
    'RECKLESS ENDANGERMENT 2': 'Violent_Offense',
    'CANNABIS SALE, 2&1': 'Drug_Offense',
    'LARCENY,GRAND BY THEFT OF CREDIT CARD': 'Property_Offense',
    'CONTROLLED SUBSTANCE, SALE 5': 'Drug_Offense',
    'VIOLATION OF ORDER OF PROTECTI': 'Violent_Offense',
    'CRIMINAL MISCHIEF 4TH, GRAFFIT': 'Property_Offense',
    'ROBBERY,BAR/RESTAURANT': 'Violent_Offense',
    'LARCENY,GRAND OF MOTORCYCLE': 'Property_Offense',
    'ROBBERY,BEGIN AS SHOPLIFTING': 'Violent_Offense',
    'FALSE REPORT UNCLASSIFIED': 'Other_Offense',
    'LARCENY,GRAND FROM PERSON,PURS': 'Property_Offense',
    'BURGLARY,COMMERCIAL,DAY': 'Property_Offense',
    'LARCENY,GRAND FROM RETAIL STORE, UNATTENDED': 'Property_Offense',
    'LARCENY,GRAND FROM PERSON, BAG OPEN/DIP': 'Property_Offense',
    'CONTROLLED SUBSTANCE,SALE 1': 'Drug_Offense',
    'INTOXICATED DRIVING,ALCOHOL': 'Other_Offense',
    'SEXUAL ABUSE 3,2': 'Violent_Offense',
    'ROBBERY,HOME INVASION': 'Violent_Offense',
    'N.Y.C. TRANSIT AUTH. R&R': 'Other_Offense',
    'AGGRAVATED HARASSMENT 1': 'Violent_Offense',
    'TRESPASS 4,CRIMINAL SUB 2': 'Public_Order_Offense',
    'FRAUD,UNCLASSIFIED-FELONY': 'Other_Offense',
    'ARSON 2,3,4': 'Violent_Offense',
    'CONTROLLED SUBSTANCE,SALE 3': 'Drug_Offense',
    'ROBBERY,POCKETBOOK/CARRIED BAG': 'Violent_Offense',
    'STOLEN PROPERTY 3,POSSESSION': 'Property_Offense',
    'LARCENY,GRAND FROM NIGHT CLUB, UNATTENDED': 'Property_Offense',
    'LARCENY,GRAND FROM RESIDENCE/BUILDING,UNATTENDED, PACKAGE THEFT OUTSIDE': 'Property_Offense',
    'AGGRAVATED SEXUAL ASBUSE': 'Violent_Offense',
    'ROBBERY,DWELLING': 'Violent_Offense',
    'CONTEMPT,CRIMINAL': 'Violent_Offense',
    'ROBBERY,CAR JACKING': 'Violent_Offense',
    'LARCENY,GRAND OF BICYCLE': 'Property_Offense',
    'RECKLESS ENDANGERMENT OF PROPE': 'Violent_Offense',
    'CRIMINAL MIS 2 & 3': 'Property_Offense',
    'STOLEN PROPERTY-MOTOR VEH 2ND,': 'Property_Offense',
    'MATERIAL              OFFENSIV': 'Other_Offense',
    'RESISTING ARREST': 'Violent_Offense',
    'LARCENY,GRAND BY BANK ACCT COMPROMISE-REPRODUCED CHECK': 'Property_Offense',
    'ARSON, MOTOR VEHICLE 1 2 3 & 4': 'Violent_Offense',
    'RECKLESS DRIVING': 'Violent_Offense',
    'LARCENY,GRAND BY BANK ACCT COMPROMISE-TELLER': 'Property_Offense',
    'FALSE REPORT BOMB': 'Other_Offense',
    'PERJURY 3,ETC.': 'Other_Offense',
    'ROBBERY,PUBLIC PLACE INSIDE': 'Violent_Offense',
    'ROBBERY,NECKCHAIN/JEWELRY': 'Violent_Offense',
    'LARCENY,PETIT BY ACQUIRING LOS': 'Property_Offense',
    'ADM.CODE,UNCLASSIFIED MISDEMEA': 'Other_Offense',
    'CONTROLLED SUBSTANCE, INTENT T': 'Drug_Offense',
    'LARCENY,GRAND BY ACQUIRING LOS': 'Property_Offense',
    'UNLAWFUL DISCLOSURE OF AN INTIMATE IMAGE': 'Violent_Offense',
    'UNAUTHORIZED USE VEHICLE 2': 'Property_Offense',
    'LARCENY,GRAND OF TRUCK': 'Property_Offense',
    'LEWDNESS,PUBLIC': 'Public_Order_Offense',
    '(null)': 'Other_Offense',
    'STOLEN PROPERTY 2,1,POSSESSION': 'Property_Offense',
    'ROBBERY,DELIVERY PERSON': 'Violent_Offense',
    'LARCENY,PETIT BY DISHONEST EMP': 'Property_Offense',
    'ROBBERY,GAS STATION': 'Violent_Offense',
    'PUBLIC ADMINISTATION,UNCLASS M': 'Other_Offense',
    'LARCENY,GRAND BY BANK ACCT COMPROMISE-UNAUTHORIZED PURCHASE': 'Property_Offense',
    'LARCENY,PETIT BY FALSE PROMISE': 'Property_Offense',
    'LARCENY,GRAND FROM PERSON,UNCL': 'Property_Offense',
    'LARCENY,GRAND BY OPEN/COMPROMISE CELL PHONE ACCT': 'Property_Offense',
    'LARCENY,GRAND FROM RESIDENCE/BUILDING,UNATTENDED, PACKAGE THEFT INSIDE': 'Property_Offense',
    'ROBBERY,BODEGA/CONVENIENCE STORE': 'Violent_Offense',
    'MISCHIEF, CRIMINAL 4, BY FIRE': 'Other_Offense',
    'BURGLARS TOOLS,UNCLASSIFIED': 'Other_Offense',
    'BURGLARY,COMMERCIAL,UNKNOWN TI': 'Property_Offense',
    'BURGLARY,UNCLASSIFIED,NIGHT': 'Property_Offense',
    'CHILD ABANDONMENT': 'Other_Offense',
    'COERCION 1': 'Other_Offense',
    'BURGLARY,RESIDENCE,UNKNOWN TIM': 'Property_Offense',
    'TAX LAW': 'Other_Offense',
    'CUSTODIAL INTERFERENCE 2': 'Other_Offense',
    'BURGLARY,UNCLASSIFIED,UNKNOWN': 'Property_Offense',
    'LARCENY,GRAND BY CREDIT CARD ACCT COMPROMISE-EXISTING ACCT': 'Property_Offense',
    'TRESPASS 3, CRIMINAL': 'Other_Offense',
    'LARCENY,GRAND BY OPEN CREDIT CARD (NEW ACCT)': 'Property_Offense',
    'RAPE 1': 'Violent_Offense',
    'LARCENY, GRAND OF MOPED': 'Property_Offense',
    'BAIL JUMPING 3': 'Other_Offense',
    'ROBBERY,LICENSED MEDALLION CAB': 'Violent_Offense',
    'INCOMPETENT PERSON,KNOWINGLY ENDANGERING': 'Other_Offense',
    'SEXUAL ABUSE': 'Violent_Offense',
    'BURGLARY, TRUCK UNKNOWN TIME': 'Property_Offense',
    'ADM.CODE,UNCLASSIFIED VIOLATIO': 'Other_Offense',
    'ASSAULT SCHOOL SAFETY AGENT': 'Violent_Offense',
    'RAPE 1,ATTEMPT': 'Violent_Offense',
    'LARCENY,GRAND BY ACQUIRING LOST CREDIT CARD': 'Property_Offense',
    'ABANDON ANIMAL': 'Other_Offense',
    'BRIBERY,PUBLIC ADMINISTRATION': 'Other_Offense',
    'ROBBERY,LICENSED FOR HIRE VEHICLE': 'Violent_Offense',
    'NY STATE LAWS,UNCLASSIFIED VIO': 'Other_Offense',
    'LARCENY,GRAND BY FALSE PROMISE-IN PERSON CONTACT': 'Property_Offense',
    'IMPERSONATION 2, PUBLIC SERVAN': 'Other_Offense',
    'LARCENY,PETIT OF AUTO': 'Property_Offense',
    'FORGERY-ILLEGAL POSSESSION,VEH': 'Other_Offense',
    'CAUSE SPI/KILL ANIMAL': 'Other_Offense',
    'LARCENY,GRAND FROM TRUCK, UNATTENDED': 'Property_Offense',
    'IMPRISONMENT 2,UNLAWFUL': 'Violent_Offense',
    'LARCENY,GRAND BY BANK ACCT COMPROMISE-ATM TRANSACTION': 'Property_Offense',
    'RAPE 3': 'Violent_Offense',
    'SALE SCHOOL GROUNDS 4': 'Other_Offense',
    'MISCHIEF, CRIMINAL 3&2, BY FIR': 'Other_Offense',
    'SODOMY 3': 'Violent_Offense',
    'ALCOHOLIC BEVERAGE CONTROL LAW': 'Other_Offense',
    'ROBBERY,UNLICENSED FOR HIRE VEHICLE': 'Violent_Offense',
    'PETIT LARCENY-CHECK FROM MAILB': 'Property_Offense',
    'MAKING TERRORISTIC THREAT': 'Violent_Offense',
    'BURGLARY,UNCLASSIFIED,DAY': 'Property_Offense',
    'AGGRAVATED CRIMINAL CONTEMPT': 'Other_Offense',
    'COURSE OF SEXUAL CONDUCT AGAIN': 'Violent_Offense',
    'TRESPASS 1,CRIMINAL': 'Other_Offense',
    'MENACING 1ST DEGREE (VICT NOT': 'Violent_Offense',
    'FALSE REPORT 1,FIRE': 'Other_Offense',
    'SUPP. ACT TERR 2ND': 'Other_Offense',
    'BAIL JUMPING 1 & 2': 'Other_Offense',
    'TAMPERING 3,2, CRIMINAL': 'Other_Offense',
    'LARCENY,PETIT FROM TRUCK': 'Property_Offense',
    'ESCAPE 3': 'Violent_Offense',
    'LARCENY, GRAND OF AUTO - ATTEM': 'Property_Offense',
    'LARCENY,GRAND BY OPEN BANK ACCT': 'Property_Offense',
    'STOLEN PROP-MOTOR VEHICLE 3RD,': 'Property_Offense',
    'LARCENY,GRAND PERSON,NECK CHAI': 'Property_Offense',
    'ROBBERY,ON BUS/ OR BUS DRIVER': 'Violent_Offense',
    'PETIT LARCENY OF ANIMAL': 'Property_Offense',
    'CANNABIS POSSESSION': 'Drug_Offense',
    'THEFT,RELATED OFFENSES,UNCLASS': 'Property_Offense',
    'GAMBLING, DEVICE, POSSESSION': 'Other_Offense',
    'CHILD,ALCOHOL SALE TO': 'Other_Offense',
    'RAPE 2': 'Violent_Offense',
    'CANNABIS SALE, 3': 'Drug_Offense',
    'INCOMPETENT PERSON,RECKLESSY ENDANGERING': 'Other_Offense',
    'CANNABIS POSSESSION, 2&1': 'Drug_Offense',
    'MENACING,PEACE OFFICER': 'Violent_Offense',
    'ROBBERY,PHARMACY': 'Violent_Offense',
    'CHECK,BAD': 'Other_Offense',
        'IMPERSONATION 1, POLICE OFFICE': 'Other_Offense',
    'DRUG PARAPHERNALIA,   POSSESSE': 'Drug_Offense',
    'PROSTITUTION': 'Other_Offense',
    'ACCOSTING,FRAUDULENT': 'Other_Offense',
    'AGRICULTURE & MARKETS LAW,UNCL': 'Other_Offense',
    'CRIMINAL DISPOSAL FIREARM 1': 'Other_Offense',
    'ROBBERY,ATM LOCATION': 'Violent_Offense',
    'ROBBERY,BICYCLE': 'Violent_Offense',
    'MENACING 1ST DEGREE (VICT PEAC': 'Violent_Offense',
    'TAMPERING 1,CRIMINAL': 'Other_Offense',
    'RECORDS,FALSIFY-TAMPER': 'Other_Offense',
    'OBSCENITY 1': 'Other_Offense',
    'LARCENY,PETIT FROM COIN MACHIN': 'Property_Offense',
    'PUBLIC ADMINISTRATION,UNCLASSI': 'Other_Offense',
    'GAMBLING 2,PROMOTING,UNCLASSIF': 'Other_Offense',
    'NEGLECT/POISON ANIMAL': 'Other_Offense',
    'ROBBERY, CHAIN STORE': 'Violent_Offense',
    'SODOMY 2': 'Violent_Offense',
    'INAPPROPIATE SHELTER DOG LEFT': 'Other_Offense',
    'SEXUAL MISCONDUCT,INTERCOURSE': 'Violent_Offense',
    'LARCENY,GRAND OF BOAT': 'Property_Offense',
    'IMPRISONMENT 1,UNLAWFUL': 'Other_Offense',
    'CHILD,OFFENSES AGAINST,UNCLASS': 'Other_Offense',
    'RIOT 2/INCITING': 'Other_Offense',
    'TERRORISM PROVIDE SUPPORT': 'Other_Offense',
    'CANNABIS POSSESSION, 3': 'Drug_Offense',
    'ROBBERY,BANK': 'Violent_Offense',
    'TORTURE/INJURE ANIMAL CRUELTY': 'Other_Offense',
    'PUBLIC HEALTH LAW,UNCLASSIFIED': 'Other_Offense',
    'ROBBERY,LIQUOR STORE': 'Violent_Offense',
    'MARIJUANA, POSSESSION 1, 2 & 3': 'Drug_Offense',
    'FACILITATION 4, CRIMINAL': 'Other_Offense',
    'EXPOSURE OF A PERSON': 'Other_Offense',
    'PROSTITUTION 3,PROMOTING BUSIN': 'Other_Offense',
    'PUBLIC HEALTH LAW,GLUE,UNLAWFU': 'Other_Offense',
    'SEXUAL MISCONDUCT,DEVIATE': 'Violent_Offense',
    'ROBBERY,CHECK CASHING BUSINESS': 'Violent_Offense',
    'MARIJUANA, POSSESSION 4 & 5': 'Drug_Offense',
    'PUBLIC SAFETY,UNCLASSIFIED MIS': 'Other_Offense',
    'LARCENY,PETIT OF MOTORCYCLE': 'Property_Offense',
    'CRIM POS WEAP 4': 'Other_Offense'
}
    
race_mapping = {
    "BLACK": "BLACK",
    "WHITE HISPANIC": "HISPANIC",
    "BLACK HISPANIC": "HISPANIC",
    "UNKNOWN": "UNKNOWN",
    "WHITE": "WHITE",
    "ASIAN / PACIFIC ISLANDER": "ASIAN",
    "AMERICAN INDIAN/ALASKAN NATIVE": "AMERICAN INDIAN"
}

spark = SparkSession \
    .builder \
    .appName("Uncommon crimes") \
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

common_patterns = spark.read.csv('hdfs://namenode:9000/consumption/common_patterns', header=True, inferSchema=True)

complaints_mapped = complaints \
    .withColumn("OFFENSE_TYPE", map_offense_type(col("PD_DESC"))) \
    .withColumn("RACE", map_race(col("SUSP_RACE")))

missing_complaints = complaints_mapped \
    .join(broadcast(common_patterns),
          (complaints_mapped.BORO_NM == common_patterns.BOROUGH) &
          (complaints_mapped.RACE == common_patterns.RACE) &
          (lower(complaints_mapped.OFFENSE_TYPE) == lower(common_patterns.OFFENSE_TYPE)),
          "left_anti") \
    .select("CMPLNT_NUM", "CMPLNT_FR_TM", "SUSP_RACE", "PD_DESC", "BORO_NM", "OFFENSE_TYPE")

query = missing_complaints \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_database) \
    .start()

query.awaitTermination()