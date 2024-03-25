#!/bin/bash

run_spark_job() {
    echo "Starting job: $1"

    docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
     --jars /spark/jars/postgresql-42.7.2.jar /py_scripts/$1

    echo "Finished job: $1"
}

run_spark_job transformations/transform_for_consumption.py

./run_batch.sh & 
./run_stream.sh &

run_spark_job transformations/add_borough_column.py
run_spark_job batch/extract_common_crime_patterns.py

./run_uncommon_crimes.sh

echo "All initial processing jobs finished"