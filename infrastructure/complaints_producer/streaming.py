#producer works properly, successful serialization and publishing to topic
import os
import pandas as pd
import time
from confluent_kafka import Producer
import json
import datetime

def transform_value(value):
    if isinstance(value, datetime.time):
        return value.isoformat()
    elif isinstance(value, (pd.Timestamp, pd.Timedelta, pd.Period)):
        return value.isoformat()
    else:
        return value

p = Producer({"bootstrap.servers": os.getenv('BOOTSTRAP_SERVER', 'kafka:9092')})

complaints = pd.read_csv("./data/complaints_streaming.csv")
complaints.drop('Unnamed: 0', axis=1, inplace=True)
print(complaints.columns)
complaints['CMPLNT_FR_TM'] = pd.to_datetime(complaints['CMPLNT_FR_TM'], format='%H:%M:%S').dt.time

complaints['time_seconds'] = complaints['CMPLNT_FR_TM'].map(lambda t: t.hour*60+t.minute)

group_ids = complaints['time_seconds'].unique()

differences = []

for i, group in enumerate(group_ids):
    if i != len(group_ids)-1:
        differences.append((group, group_ids[i+1]-group))
    else:
        differences.append(None)

for column in ['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'CMPLNT_TO_DT', 'CMPLNT_TO_TM', 'RPT_DT']:
    complaints[column] = complaints[column].apply(transform_value)

complaints_grouped = complaints.groupby('time_seconds')

for i in range(0, len(differences)):
        current_data = complaints_grouped.get_group(differences[i][0])
        for _, row in current_data.iterrows():
            p.produce(topic=os.getenv('TOPIC', 'complaints'), value=json.dumps(row.to_dict()))
            #p.flush()
            #print(f'sent complaint to kafka topic {os.getenv("TOPIC", "complaints")} - {row["CMPLNT_NUM"]}')
        if i!=len(differences)-1:
            time.sleep(differences[i][1])



