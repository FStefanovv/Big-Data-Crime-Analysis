#!/bin/bash

run_streaming_job() {
    echo "Starting stream: $1"

    docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /spark/jars/postgresql-42.7.2.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /py_scripts/stream/$1 2> "$1.err"
}

run_streaming_job location_safety.py &
run_streaming_job most_common_complaints.py &
run_streaming_job unsafest_city_boroughs.py &
run_streaming_job complaints_per_hour_and_borough.py &
run_streaming_job save_to_datalake.py &
run_streaming_job uncommon_crimes.py &

while true; do
    sleep 60
done