#!/bin/bash

# default values
MEMORY="4g"
SPARK_MASTER="local[*]"
CASSANDRA_HOST="localhost"

SRC_KEYSPACE="btc_raw"
TGT_KEYSPACE="btc_transformed"


if [ -z $SPARK_HOME ] ; then
    echo "Cannot find Apache Spark. Set the SPARK_HOME environment variable." > /dev/stderr
    exit 1;
fi

EXEC=$(basename "$0")
USAGE="Usage: $EXEC [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER] [--src_keyspace SRC_KEYSPACE] [--tgt_keyspace TGT_KEYSPACE]"

# parse command line options
args=`getopt -o hc:m:s: --long src_keyspace:,tgt_keyspace: -- "$@"`
eval set -- "$args"

while true; do
    case "$1" in
        -h)
            echo "$USAGE"
            exit 0
        ;;
        -c)
            CASSANDRA_HOST=$2
            shift 2
        ;;
        -m)
            MEMORY=`printf '%dg' $2`
            shift 2
        ;;
        -s)
            SPARK_MASTER=$2
            shift 2
        ;;
        --src_keyspace)
            SRC_KEYSPACE=$2
            shift 2
        ;;
        --tgt_keyspace)
            TGT_KEYSPACE=$2
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
         "- source keyspace: $SRC_KEYSPACE\n" \
         "- target keyspace: $TGT_KEYSPACE\n"


$SPARK_HOME/bin/spark-submit \
  --class "at.ac.ait.TransformationJob" \
  --master $SPARK_MASTER \
  --conf spark.executor.memory=$MEMORY \
  --conf spark.cassandra.connection.host=$CASSANDRA_HOST \
  --jars ~/.ivy2/local/at.ac.ait/graphsense-clustering_2.11/0.4.0/jars/graphsense-clustering_2.11.jar \
  --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 \
             target/scala-2.11/graphsense-transformation_2.11-0.4.0.jar \
  --source_keyspace $SRC_KEYSPACE \
  --target_keyspace $TGT_KEYSPACE

exit $?
