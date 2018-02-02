import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.11.8",
      version      := "0.3.2"
    )),
    name := "graphsense-transformation",
    fork := true,
    //javaOptions in Test ++= List("-Xms3G", "-Xmx3G"),
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
    libraryDependencies ++= List(
      scalaTest % Test,
      sparkSql % Provided,
      sparkCassandraConnector % Provided)
  )
