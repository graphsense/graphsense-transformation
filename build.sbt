ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "at.ac.ait"
ThisBuild / version      := "0.4.2-SNAPSHOT"


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
    resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.0" % Test,
      "MrPowers" % "spark-fast-tests" % "0.17.2-s_2.11" % Test,
      "org.rogach" %% "scallop" % "3.3.2" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.3" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0" % Provided,
      "at.ac.ait" %% "graphsense-clustering" % "0.4.1" % Provided)
  )
