#!/bin/bash

/opt/spark-2.2/bin/spark-submit \
  --class "at.ac.ait.SimpleApp" \
  --master local[4] \
  --conf spark.cassandra.connection.host=spark1 \
  --packages datastax:spark-cassandra-connector:2.0.5-s_2.11 \
  target/scala-2.11/graphsense-transformation_2.11-*.jar
