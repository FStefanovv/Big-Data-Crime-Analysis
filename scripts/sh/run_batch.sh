#!/bin/bash

run_batch_job() {
    echo "Starting job: $1"

    docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /spark/jars/postgresql-42.7.2.jar /py_scripts/batch/$1 2> "$1.err"

    echo "Finished job: $1"
}

run_batch_job family_criminal_history_by_race.py &
run_batch_job flat_sentence_stats.py &
run_batch_job marital_status.py &
run_batch_job most_common_feelings.py &
run_batch_job parental_status_per_race.py &
run_batch_job race_share_scaled_by_crime.py &
run_batch_job races_through_years.py &
run_batch_job racial_structure.py &
run_batch_job top_3_crimes_by_race.py &
run_batch_job victim_stats.py &

while true; do
    sleep 60
done