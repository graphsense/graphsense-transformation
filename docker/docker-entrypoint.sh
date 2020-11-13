#!/bin/sh
echo "Creating Cassandra keyspace ${TGT_KEYSPACE}"
python3 /opt/graphsense/scripts/create_keyspace.py \
    -d ${CASSANDRA_HOST} \
    -k ${TGT_KEYSPACE} \
    -s /opt/graphsense/scripts/schema_transformed.cql
exec "$@"
