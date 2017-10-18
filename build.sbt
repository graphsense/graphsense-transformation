name := "graphsense-transformation"

version := "0.3.2dev"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Ywarn-value-discard")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5")
