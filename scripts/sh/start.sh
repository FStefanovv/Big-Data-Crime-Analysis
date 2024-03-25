#!/bin/bash

docker compose -f ../../infrastructure/docker/docker-compose.yml build
docker compose -f ../../infrastructure/docker/docker-compose.yml up -d

#delete all directories from hdfs
docker exec -it namenode bash -c "rm -r -f /input"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /raw"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /curated"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /consumption"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /sss_checkpoints"

#create hdfs directories
docker exec -it namenode bash -c "mkdir input"
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /raw"
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /curated"
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /consumption"
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /sss_checkpoints"

docker cp ../../data/prisoners.tsv namenode:/input/
docker exec -it namenode bash -c 'hdfs dfs -put input/prisoners.tsv  /raw/'

docker cp ../../data/cummulative_boroughs.csv namenode:/input/
docker exec -it namenode bash -c 'hdfs dfs -put input/cummulative_boroughs.csv /raw/'

docker exec -it namenode bash -c 'hdfs dfs -put spark-output/prisoners-df /curated/'
#docker exec -it namenode bash -c 'hdfs dfs -put spark-output/prisoners_batch_queries /consumption/'
#docker exec -it namenode bash -c 'hdfs dfs -put spark-output/common_patterns /consumption/'

docker cp ../../postgresql-42.7.2.jar spark-master:/spark/jars

docker exec -it citus-coordinator bash -c 'psql -U postgres -c "DROP DATABASE IF EXISTS streaming_results;"'
docker exec -it citus-coordinator bash -c 'psql -U postgres -c "CREATE DATABASE streaming_results;"'

docker exec -it citus-coordinator bash -c 'psql -U postgres -c "DROP DATABASE IF EXISTS prisoners;"'
docker exec -it citus-coordinator bash -c 'psql -U postgres -c "CREATE DATABASE prisoners;"'