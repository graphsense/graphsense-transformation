ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "info.graphsense"
ThisBuild / version      := "0.5.2"


lazy val root = (project in file(".")).
  settings(
    name := "graphsense-transformation",
    fork := true,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    scalacOptions ++= List(
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
      "-Ywarn-value-discard"),
    resolvers += "SparkPackages" at "https://repos.spark-packages.org/",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.11" % Test,
      "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % Test,
      "graphframes" % "graphframes" % "0.8.1-spark3.0-s_2.12" % Provided,
      "org.rogach" %% "scallop" % "4.1.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.1.2" % Provided,
      "org.apache.spark" %% "spark-graphx" % "3.1.2" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0" % Provided)
  )
