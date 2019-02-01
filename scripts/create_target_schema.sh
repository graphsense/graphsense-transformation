#!/bin/bash

echo "Creating target keyspace in Cassandra"
cqlsh localhost -f ./scripts/schema_transformed.cql