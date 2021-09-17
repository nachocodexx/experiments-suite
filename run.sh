#!/bin/sh
docker run --name ex0 \
--network=mynet \
-d \
-e NODE_ID="ex0" \
-e MAX_NODES=4 \
-e MAX_EXPERIMENTS=2 \
-e EXPERIMENT_TIME_INTERVAL=5000 \
-e INTERARRIVAL_TIME=2000 \
-e WORKLOAD_PATH=/app/workloads \
-e WORKLOAD_FILENAME=workload_test.csv \
-e DOWNLOAD_FOLDER=/app/downloads \
-e WEBSERVER_URL="http://10.0.0.5" \
-e RABBITMQ_HOST="10.0.0.4" \
-e LOG_PATH=/app/logs \
-v /home/nacho/Programming/Scala/experiments-suite/target/workloads:/app/workloads \
-v /home/nacho/Programming/Scala/experiments-suite/target/downloads:/app/downloads \
-v /home/nacho/Programming/Scala/experiments-suite/target/logs:/app/logs \
nachocode/experiments-suite