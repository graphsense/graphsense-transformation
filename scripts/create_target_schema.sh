#!/bin/bash

echo "Creating target keyspace in Cassandra"
m4 -Dgraphsense=$TARGET_KEYSPACE ./scripts/schema_transformed.cql | cqlsh $CASSANDRA_HOST --cqlversion="3.4.4"

echo "Creating tagpacks keyspace in Cassandra"
m4 -Dtagkeyspace=$TAG_KEYSPACE ./scripts/schema_tagpacks.cql | cqlsh $CASSANDRA_HOST --cqlversion="3.4.4"
