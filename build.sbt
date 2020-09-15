ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "at.ac.ait"
ThisBuild / version      := "0.4.5"


lazy val root = (project in file(".")).
  settings(
    name := "graphsense-transformation",
    fork := true,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
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
    resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
    resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.2" % Test,
      "MrPowers" % "spark-fast-tests" % "0.21.1-s_2.12" % Test,
      "org.rogach" %% "scallop" % "3.4.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.5" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2" % Provided,
      "com.codahale.metrics" % "metrics-core" % "3.0.2" % Provided)
  )
