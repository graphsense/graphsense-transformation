#!/bin/bash

echo -en "Starting Spark job ...\n" \
         "Config:\n" \
         "- Spark master:        $SPARK_MASTER\n" \
         "- Spark driver:        $SPARK_DRIVER_HOST\n" \
         "- Spark local dir:     $SPARK_LOCAL_DIR\n" \
         "- Cassandra host:      $CASSANDRA_HOST\n" \
         "- Executor memory:     $SPARK_EXECUTOR_MEMORY\n" \
         "Arguments:\n" \
         "- Currency:            $CURRENCY\n" \
         "- Raw keyspace:        $RAW_KEYSPACE\n" \
         "- Target keyspace:     $TGT_KEYSPACE\n" \
         "- HDFS Checkpoint dir: $CHECKPOINT_DIR\n"

"$SPARK_HOME"/bin/spark-submit \
  --class "info.graphsense.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.driver.bindAddress="0.0.0.0" \
  --conf spark.driver.host="$SPARK_DRIVER_HOST" \
  --conf spark.driver.port="$SPARK_DRIVER_PORT" \
  --conf spark.ui.port="$SPARK_UI_PORT" \
  --conf spark.blockManager.port="$SPARK_BLOCKMGR_PORT" \
  --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.default.parallelism=400 \
  --conf spark.driver.memory="92G" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,graphframes:graphframes:0.8.1-spark3.0-s_2.12,org.rogach:scallop_2.12:4.1.0,joda-time:joda-time:2.10.10 \
  target/scala-2.12/graphsense-transformation_2.12-0.5.2.jar \
  --currency "$CURRENCY" \
  --raw-keyspace "$RAW_KEYSPACE" \
  --target-keyspace "$TGT_KEYSPACE" \
  --bucket-size 25000 \
  --coinjoin-filtering \
  --bech32-prefix "$BECH32_PREFIX" \
  --checkpoint-dir "$CHECKPOINT_DIR"

exit $?
