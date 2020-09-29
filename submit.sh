#!/bin/bash

# default values
MEMORY="24g"
SPARK_MASTER="local[*]"
#CASSANDRA_HOST="localhost" # This is provided through docker-compose.yml

CURRENCY="BTC"
# All of these are provided through .env files
#RAW_KEYSPACE="btc_raw"
#TAG_KEYSPACE="tagpacks"
#TGT_KEYSPACE="btc_transformed"
#BUCKET_SIZE=25000


if [ -z "$SPARK_HOME" ] ; then
    echo "Cannot find Apache Spark. Set the SPARK_HOME environment variable." > /dev/stderr
    exit 1;
fi

EXEC=$(basename "$0")
USAGE="Usage: $EXEC [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER] [--currency CURRENCY] [--src_keyspace RAW_KEYSPACE] [--tag_keyspace TAG_KEYSPACE] [--tgt_keyspace TARGET_KEYSPACE] [--bucket_size BUCKET_SIZE]"

# parse command line options
args=$(getopt -o hc:m:s: --long raw_keyspace:,tag_keyspace:,tgt_keyspace:,bucket_size:,currency: -- "$@")
eval set -- "$args"

while true; do
    case "$1" in
        -h)
            echo "$USAGE"
            exit 0
        ;;
        -c)
            CASSANDRA_HOST="$2"
            shift 2
        ;;
        -m)
            MEMORY=$(printf "%dg" "$2")
            shift 2
        ;;
        -s)
            SPARK_MASTER="$2"
            shift 2
        ;;
        --currency)
            CURRENCY="$2"
            shift 2
        ;;
        --raw_keyspace)
            RAW_KEYSPACE="$2"
            shift 2
        ;;
        --tag_keyspace)
            TAG_KEYSPACE="$2"
            shift 2
        ;;
        --tgt_keyspace)
            TGT_KEYSPACE="$2"
            shift 2
        ;;
        --bucket_size)
            BUCKET_SIZE="$2"
            shift 2
        ;;
        --) # end of all options
            shift
            if [ "x$*" != "x" ] ; then
                echo "$EXEC: Error - unknown argument \"$*\"" >&2
                exit 1
            fi
            break
        ;;
        -*)
            echo "$EXEC: Unrecognized option \"$1\". Use -h flag for help." >&2
            exit 1
        ;;
        *) # no more options
             break
        ;;
    esac
done


echo -en "Starting on $CASSANDRA_HOST with master $SPARK_MASTER" \
         "and $MEMORY memory ...\n" \
         "- currency:        $CURRENCY\n" \
         "- raw keyspace:    $RAW_KEYSPACE\n" \
         "- tag keyspace:    $TAG_KEYSPACE\n" \
         "- target keyspace: $TARGET_KEYSPACE\n" \
         "- bucket size:     $BUCKET_SIZE\n"


"$SPARK_HOME"/bin/spark-submit \
  --class "at.ac.ait.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.executor.memory="$MEMORY" \
  --conf spark.driver.memory="24g" \
  --conf spark.local.dir="/spark_store" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.memory.offHeap.enabled="true" \
  --conf spark.memory.offHeap.size="10G" \
  --conf spark.unsafe.sorter.spill.read.ahead.enabled="false" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:2.4.2,org.rogach:scallop_2.12:3.4.0,com.codahale.metrics:metrics-core:3.0.2 \
  target/scala-2.12/graphsense-transformation_2.12-0.4.4-SNAPSHOT.jar \
  --currency "$CURRENCY" \
  --raw_keyspace "$RAW_KEYSPACE" \
  --tag_keyspace "$TAG_KEYSPACE" \
  --target_keyspace "$TARGET_KEYSPACE" \
  --bucket_size "$BUCKET_SIZE"

exit $?
