val packagename = "graphsense-transformation"
// used for local builds
val defaultVersion = "1.5.0"
// taken from https://alterationx10.com/2022/05/26/publish-to-github/
val tagWithQualifier: String => String => String =
  qualifier =>
    tagVersion => s"%s.%s.%s-${qualifier}%s".format(tagVersion.split("\\."): _*)

val tagAlpha: String => String = tagWithQualifier("a")
val tagBeta: String => String = tagWithQualifier("b")
val tagMilestone: String => String = tagWithQualifier("m")
val tagRC: String => String = tagWithQualifier("rc")

val versionFromTag: String = sys.env
  .get("GITHUB_REF_TYPE")
  .filter(_ == "tag")
  .flatMap(_ => sys.env.get("GITHUB_REF_NAME"))
  .flatMap { t =>
    t.headOption.map {
      case 'a' => tagAlpha(t.tail) // Alpha build, a1.2.3.4
      case 'b' => tagBeta(t.tail) // Beta build, b1.2.3.4
      case 'm' => tagMilestone(t.tail) // Milestone build, m1.2.3.4
      case 'r' => tagRC(t.tail) // RC build, r1.2.3.4
      case 'v' => t.tail // Production build, should be v1.2.3
      case _ => defaultVersion
    }
  }
  .getOrElse(defaultVersion)

ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "info.graphsense"
ThisBuild / version := versionFromTag
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / publishTo := Some(
  "GitHub Package Registry" at "https://maven.pkg.github.com/graphsense/" + packagename
)
ThisBuild / credentials += Credentials(
  "GitHub Package Registry", // realm
  "maven.pkg.github.com", // host
  "graphsense", // user
  sys.env.getOrElse("GITHUB_TOKEN", "thisisnottherealpassword") // password
)

lazy val root = (project in file(".")).
  settings(
    name := "graphsense-transformation",
    fork := true,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    Test / javaOptions += "-Xmx3G",
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
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % Test,
      "graphframes" % "graphframes" % "0.8.2-spark3.2-s_2.12" % Provided,
      "org.rogach" %% "scallop" % "4.1.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.2.4" % Provided,
      "org.apache.spark" %% "spark-graphx" % "3.2.4" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0" % Provided)
  )
