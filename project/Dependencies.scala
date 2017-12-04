import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.3"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val sparkCassandraConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"
}
