ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "at.ac.ait"
ThisBuild / version      := "0.4.0-SNAPSHOT"


lazy val root = (project in file(".")).
  settings(
    name := "graphsense-transformation",
    fork := true,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
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
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % Test,
      "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0" % Provided,
      "at.ac.ait" %% "graphsense-clustering" % "0.3.3" % Provided)
  )
