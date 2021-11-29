package info.graphsense.storage

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.{RowWriterFactory}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import scala.reflect.ClassTag

import info.graphsense.Util._

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
      df: Dataset[T]
  ) = {

    spark.sparkContext.setJobDescription(s"Writing table ${tableName}")
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = LocalDateTime.now().format(dtf)
    println(s"[$timestamp] Writing table ${tableName}")
    time { df.rdd.saveToCassandra(keyspace, tableName) }
  }
}
