#!/bin/bash

$SPARK_HOME/bin/spark-submit \
  --class "at.ac.ait.SimpleApp" \
  --master local[4] \
  --conf spark.cassandra.connection.host=localhost \
  --packages datastax:spark-cassandra-connector:2.0.6-s_2.11 \
  target/scala-2.11/graphsense-transformation_2.11-0.3.2-SNAPSHOT.jar \
  --source_keyspace graphsense_raw \
  --target_keyspace graphsense_transformed_new \
  --max_blockgroup 4
