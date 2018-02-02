# GraphSense Transformation Pipeline

The GraphSense Transformation Pipeline reads raw data, which is ingested into [Cassandra][apache-cassandra]
by the [graphsense-datafeed][graphsense-datafeed] component, and computes de-normalized views, which are
again stored in [Cassandra][apache-cassandra].

Access to computed de-normalized views is subsequently provided by the [GraphSense REST][graphsense-rest]
interface, which is used by the [graphsense-dashboard][graphsense-dashboard] component.

This component is implemented using the [Apache Spark][apache-spark].

## Local Development Environment Setup

Make sure [Java 8][java] and [sbt > 1.0][scala-sbt]is installed:

    java -version
    sbt about

Install the [Scala IDE for Eclipse][http://scala-ide.org/].

Install the [sbteclipse][sbteclipse] plugin. Use either

- the global file at `~/.sbt/SBTVERSION/plugins/plugins.sbt`
  (for `sbt` version 0.13 and up)
- the project-specific file `graphsense-transformation/project/plugins.sbt`

Create an eclipse project file using `sbt`

    cp eclipse.sbt.disabled eclipse.sbt
    sbt eclipse

Import project into the Scala-IDE via
`File > Import... > Existing Projects into Workspace`

Download, install, and test Apache Spark (version >= 2.2.0) in $SPARK_HOME:

    $SPARK_HOME/bin/spark-shell

## Local Transformation Pipeline Execution 

Make sure raw data has been imported into a running Apache Cassandra
instance using the [graphsense-datafeed][graphsense-datafeed] service.


macOS only: make sure gnu-getopt is installed

    brew install gnu-getopt

Create a keyspace for the transformed data

    cqlsh -f schema_transformed.cql

Compile and test the implementation

    sbt test

Package the transformation pipeline

    sbt package

Run the transformation pipeline on the localhost

    bash submit.sh

Check the running job using the local Spark UI at http://localhost:4040/jobs

[graphsense-datafeed]: https://github.com/graphsense/graphsense-datafeed
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[graphsense-rest]: https://github.com/graphsense/graphsense-rest
[java]: https://java.com
[scala-lang]: https://www.scala-lang.org/
[scala-sbt]: http://www.scala-sbt.org
[sbteclipse]: https://github.com/typesafehub/sbteclipse
[apache-spark]: https://spark.apache.org/downloads.html
[apache-cassandra]: http://cassandra.apache.org/
[java]: https://java.com
[scala-ide]: http://scala-ide.org/
