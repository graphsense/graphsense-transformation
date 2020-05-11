[![Build Status](https://travis-ci.org/graphsense/graphsense-transformation.svg?branch=develop)](https://travis-ci.org/graphsense/graphsense-transformation)

# GraphSense Transformation Pipeline

The GraphSense Transformation Pipeline reads raw block data, which is
ingested into [Apache Cassandra][apache-cassandra]
by the [graphsense-blocksci][graphsense-blocksci] component, and
attribution tags provided by [graphsense-tagpacks][graphsense-tagpacks].
The transformation pipeline computes de-normalized views using
[Apache Spark][apache-spark], which are again stored in Cassandra.

Access to computed de-normalized views is subsequently provided by the
[GraphSense REST][graphsense-rest] interface, which is used by the
[graphsense-dashboard][graphsense-dashboard] component.

This component is implemented in Scala using [Apache Spark][apache-spark].

## Local Development Environment Setup

### Prerequisites

Make sure [Java 8][java] and [sbt >= 1.0][scala-sbt] is installed:

    java -version
    sbt about

Download, install, and run [Apache Spark][apache-spark] (version >= 2.4.0) in $SPARK_HOME:

    $SPARK_HOME/sbin/start-master.sh

Download, install, and run [Apache Cassandra][apache-cassandra] (version >= 3.1.1) in $CASSANDRA_HOME

    $CASSANDRA_HOME/bin/cassandra -f 

### Ingest Raw Block Data

Run the following script for ingesting raw block test data

    ./scripts/ingest_test_data.sh

This should create a keyspace `btc_raw` (tables `exchange_rates`,
`transaction`, `block`, `block_transactions`) and `tagpacks`
(table `tag_by_address`). Check as follows

    cqlsh localhost
    cqlsh> USE btc_raw;
    cqlsh:btc_raw> DESCRIBE tables;
    cqlsh:btc_raw> USE tagpacks;
    cqlsh:tagpacks> DESCRIBE tables;

## Execute Transformation Locally 

macOS only: make sure gnu-getopt is installed

    brew install gnu-getopt

Create the target keyspace for transformed data

    ./scripts/create_target_schema.sh

Compile and test the implementation

    sbt test

Package the transformation pipeline

    sbt package

Run the transformation pipeline on localhost

    ./submit.sh

Check the running job using the local Spark UI at http://localhost:4040/jobs

[graphsense-blocksci]: https://github.com/graphsense/graphsense-blocksci
[graphsense-tagpacks]: https://github.com/graphsense/graphsense-tagpacks
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[graphsense-rest]: https://github.com/graphsense/graphsense-rest

[java]: https://java.com
[scala-ide]: http://scala-ide.org/
[scala-lang]: https://www.scala-lang.org/
[scala-sbt]: http://www.scala-sbt.org
[sbteclipse]: https://github.com/typesafehub/sbteclipse

[apache-spark]: https://spark.apache.org/downloads.html
[apache-cassandra]: http://cassandra.apache.org/
