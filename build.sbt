ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "at.ac.ait"
ThisBuild / version      := "0.5.0-SNAPSHOT"


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
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.7" % Test,
      "com.github.mrpowers" % "spark-fast-tests_2.12" % "0.23.0" % Test,
      "org.rogach" %% "scallop" % "4.0.2" % Provided,
      "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2" % Provided)
  )
