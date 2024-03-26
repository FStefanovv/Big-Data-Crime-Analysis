#!/bin/bash

run_spark_job() {
    echo "Starting job: $1"

    docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
     --jars /spark/jars/postgresql-42.7.2.jar /py_scripts/$1

    echo "Finished job: $1"
}

run_spark_job transformations/transform_for_consumption.py
run_spark_job transformations/add_borough_column.py
run_spark_job batch/extract_common_crime_patterns.py

#./run_batch.sh & 
./run_stream.sh &

while true; do
    sleep 60
done