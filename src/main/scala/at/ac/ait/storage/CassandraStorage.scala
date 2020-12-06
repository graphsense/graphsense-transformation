package at.ac.ait.storage

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.{RowWriterFactory, WriteConf}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import at.ac.ait.AppendProgress
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag
import at.ac.ait.Util._
import org.apache.spark.rdd.RDD

class CassandraStorage(spark: SparkSession) {

  import spark.implicits._
  import com.datastax.spark.connector._

  def load[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](
      keyspace: String,
      tableName: String,
      columns: ColumnRef*
  ) = {
    spark.sparkContext.setJobDescription(s"Loading table ${tableName}")
    val table = spark.sparkContext.cassandraTable[T](keyspace, tableName)
    if (columns.isEmpty)
      table.toDS().as[T]
    else
      table.select(columns: _*).toDS().as[T]
  }

  def store[T <: Product: RowWriterFactory](
      keyspace: String,
      tableName: String,
      df: Dataset[T],
      ifNotExists: Boolean = false,
  ) = {
    storeRDD(keyspace, tableName, df.rdd)
  }

  def saveAppendProgress(targetKeyspace: String, progress: AppendProgress): Unit = {
    store[AppendProgress](targetKeyspace, "append_progress", spark.createDataset(Seq(progress)))
  }

  def storeRDD[T <: Product: RowWriterFactory](
    keyspace: String,
    tableName: String,
    rdd: RDD[T]
  ) = {
    spark.sparkContext.setJobDescription(s"Writing table ${tableName}")
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = LocalDateTime.now().format(dtf)
    println(s"[$timestamp] Writing table ${tableName}")
    val conf = WriteConf.fromSparkConf(spark.sparkContext.getConf)
    time { rdd.saveToCassandra(keyspace, tableName, writeConf = conf) }
  }
}
