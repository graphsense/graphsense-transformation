#!/bin/bash

echo -en "Starting Spark job ...\n" \
         "Config:\n" \
         "- Spark master: $SPARK_MASTER\n" \
         "- Spark driver: $SPARK_DRIVER_HOST\n" \
         "- Cassandra host: $CASSANDRA_HOST\n" \
         "- Executor memory: $SPARK_EXECUTOR_MEMORY\n" \
         "Arguments:\n" \
         "- Currency:        $CURRENCY\n" \
         "- Raw keyspace:    $RAW_KEYSPACE\n" \
         "- Tag keyspace:    $TAG_KEYSPACE\n" \
         "- Target keyspace: $TGT_KEYSPACE\n"

"$SPARK_HOME"/bin/spark-submit \
  --class "at.ac.ait.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.driver.bindAddress="0.0.0.0" \
  --conf spark.driver.host="$SPARK_DRIVER_HOST" \
  --conf spark.driver.port="$SPARK_DRIVER_PORT" \
  --conf spark.ui.port="$SPARK_UI_PORT" \
  --conf spark.blockManager.port="$SPARK_BLOCKMGR_PORT" \
  --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.default.parallelism=400 \
  --conf spark.driver.memory="64G" \
  --conf spark.local.dir="/var/cache/spark" \
  --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:2.4.2,org.rogach:scallop_2.12:3.4.0 \
  target/scala-2.12/graphsense-transformation_2.12-0.4.4-SNAPSHOT.jar \
  --currency "$CURRENCY" \
  --raw_keyspace "$RAW_KEYSPACE" \
  --tag_keyspace "$TAG_KEYSPACE" \
  --target_keyspace "$TGT_KEYSPACE" \
  --bucket_size 25000

exit $?
