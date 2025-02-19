#!/bin/bash

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.memory.fraction=0.75 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=10 \
  --conf spark.dynamicAllocation.maxExecutors=100 \
  --conf spark.sql.autoBroadcastJoinThreshold=52428800 \
  your_spark_script.py
