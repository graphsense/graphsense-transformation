# GraphSense Transformation Pipeline

An Apache Spark service for transforming raw data (blocks, tags, etc)
provided by the [graphsense-datafeed][graphsense-datafeed] component into
denormalized views needed by [graphsense-dashboard][graphsense-dashboard].


## Development Setup with Eclipse

Make sure [Scala][scala-lang] 2.11 and [scala-sbt][scala-sbt] is installed:

    sbt about

Install the [sbteclipse][sbteclipse] plugin. Use either

- the global file at `~/.sbt/SBTVERSION/plugins/plugins.sbt`
  (for `sbt` version 0.13 and up)
- the project-specific file `graphsense-transformation/project/plugins.sbt`

Download and install [Apache Spark][apache-spark] (version >= 2.1.0)
in `$SPARK_HOME`:

    $SPARK_HOME/bin/spark-shell

Create an eclipse project file using `sbt`

    cp eclipse.sbt.disabled eclipse.sbt
    sbt eclipse

Import project into the Scala-IDE via
`File > Import... > Existing Projects into Workspace`


## Run the Transformation Pipeline

Make sure raw data have been ingested into Apache Cassandra by
the [graphsense-datafeed][graphsense-datafeed] service.

Create a keyspace for the transformed data

    $CASSANDRA_HOME/bin/cqlsh -f cassandra/basicTransformation.cql

and a keyspace for multiple input clustering

    $CASSANDRA_HOME/bin/cqlsh -f cassandra/multipleInputClustering.cql


Compile and package the transformation pipeline

    sbt package

Run the transformation pipeline on the localhost

    ./bin/execute

The -m option specifies the Spark executor memory (defaults to 4GB Memory),
see

    ./bin/execute -h
    Usage: execute [-h] [-m MEMORY_GB] [-c CASSANDRA_HOST] [-s SPARK_MASTER]


Check the running job using the local Spark UI at http://localhost:4040/jobs


[graphsense-datafeed]: https://github.com/graphsense/graphsense-datafeed
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[scala-lang]: https://www.scala-lang.org/
[scala-sbt]: http://www.scala-sbt.org
[sbteclipse]: https://github.com/typesafehub/sbteclipse
[apache-spark]: https://spark.apache.org/downloads.html
