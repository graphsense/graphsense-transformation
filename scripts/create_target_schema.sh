#!/bin/bash

echo "Creating target keyspace in Cassandra"
cqlsh cassandra --cqlversion="3.4.4" -f ./scripts/schema_transformed.cql
