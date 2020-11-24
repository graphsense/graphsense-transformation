/usr/local/spark/sbin/start-master.sh

SPARK_MASTER="local"
MEMORY="4g"
CASSANDRA_HOST="172.31.0.2"
CURRENCY="BTC"
#RAW_KEYSPACE="btc_raw15k"
RAW_KEYSPACE="btc_raw15k"
TAG_KEYSPACE="tagpacks"
TARGET_KEYSPACE="btc_transformed_append"
#TARGET_KEYSPACE="btc_transformed"
BUCKET_SIZE=25000

# Uncomment this line to enable debug mode.
# The script will pause here until a debugger is attached. You can attach one from for example Idea by creating a remote debug configuration and specifying localhost:7777 as address.
#export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777


/usr/local/spark/bin/spark-submit \
  --class "at.ac.ait.AppendJob" \
  --master "$SPARK_MASTER" \
  --conf spark.executor.memory="$MEMORY" \
  --conf spark.driver.memory="2g" \
  --conf spark.local.dir="./spark_store" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.memory.offHeap.enabled="true" \
  --conf spark.memory.offHeap.size="10G" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:2.4.2,org.rogach:scallop_2.12:3.4.0,com.codahale.metrics:metrics-core:3.0.2 \
  target/scala-2.12/graphsense-transformation.jar \
  --currency "$CURRENCY" \
  --raw_keyspace "$RAW_KEYSPACE" \
  --tag_keyspace "$TAG_KEYSPACE" \
  --target_keyspace "$TARGET_KEYSPACE" \
  --bucket_size "$BUCKET_SIZE" \
  --append_block_count "5000"
