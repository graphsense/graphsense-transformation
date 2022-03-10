#!/bin/bash

# default values
MEMORY="4g"
SPARK_MASTER="local[*]"
CASSANDRA_HOST="localhost"

CURRENCY="BTC"
RAW_KEYSPACE="btc_raw"
TGT_KEYSPACE="btc_transformed"
BUCKET_SIZE=25000
BECH32_PREFIX=""
CHECKPOINT_DIR="file:///tmp/spark-checkpoint"  # hdfs:// in cluster mode


if [ -z "$SPARK_HOME" ] ; then
    echo "Cannot find Apache Spark. Set the SPARK_HOME environment variable." > /dev/stderr
    exit 1;
fi

EXEC=$(basename "$0")
USAGE="Usage: $EXEC [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER] [--currency CURRENCY] [--raw_keyspace RAW_KEYSPACE] [--tgt_keyspace TGT_KEYSPACE] [--bucket_size BUCKET_SIZE] [--bech32-prefix BECH32_PREFIX] [--checkpoint-dir CHECKPOINT_DIR] [--coinjoin-filtering]"

# parse command line options
args=$(getopt -o hc:m:s: --long raw_keyspace:,tgt_keyspace:,bucket_size:,currency:,bech32_prefix:,checkpoint_dir,coinjoin_filtering: -- "$@")
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
        --tgt_keyspace)
            TGT_KEYSPACE="$2"
            shift 2
        ;;
        --bucket_size)
            BUCKET_SIZE="$2"
            shift 2
        ;;
        --bech32_prefix)
            BECH32_PREFIX="$2"
            shift 2
        ;;
        --checkpoint_dir)
            CHECKPOINT_DIR="$2"
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
         "- target keyspace: $TGT_KEYSPACE\n" \
         "- bucket size:     $BUCKET_SIZE\n" \
         "- BECH32 prefix:   $BECH32_PREFIX\n" \
         "- checkpoint dir:  $CHECKPOINT_DIR\n"


"$SPARK_HOME"/bin/spark-submit \
  --class "info.graphsense.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.executor.memory="$MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,graphframes:graphframes:0.8.1-spark3.0-s_2.12,org.rogach:scallop_2.12:4.1.0,joda-time:joda-time:2.10.10 \
  target/scala-2.12/graphsense-transformation_2.12-0.5.2.jar \
  --currency "$CURRENCY" \
  --raw-keyspace "$RAW_KEYSPACE" \
  --target-keyspace "$TGT_KEYSPACE" \
  --bucket-size "$BUCKET_SIZE" \
  --coinjoin-filtering \
  --bech32-prefix "$BECH32_PREFIX" \
  --checkpoint-dir "$CHECKPOINT_DIR"

exit $?
