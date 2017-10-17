package at.ac.ait.storage

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions, BytesInBatch}
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.{RowWriterFactory, WriteConf}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import scala.reflect.ClassTag

import at.ac.ait.BasicTables

object CassandraStorage {

  val defaultWriteConf = WriteConf(
    batchSize = BytesInBatch(4096),
    parallelismLevel = 5,
    throughputMiBPS = 5,
    consistencyLevel = ConsistencyLevel.ONE)

}


class CassandraStorage(spark: SparkSession) {

  import CassandraStorage._

  import spark.implicits._

  import com.datastax.spark.connector._

  def load[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](
      keyspace: String,
      tableName: String,
      columns: ColumnRef*) = {
    spark.sparkContext.setJobDescription(s"Loading table $tableName")
    val table = spark.sparkContext.cassandraTable[T](keyspace, tableName)
    if (columns.isEmpty)
      table.toDS().as[T]
    else
      table.select(columns: _*).toDS().as[T]
  }

  def store[T <: Product: RowWriterFactory] (
      tableName: String,
      df: Dataset[T],
      writeConf: WriteConf = defaultWriteConf,
      count: Boolean = true) = {

    println(s"\n============= ${tableName} =============")
    if (count) println(s"Writing ${df.count} rows...")
    df.rdd.saveToCassandra(BasicTables.transformedKeyspace, tableName, writeConf = writeConf)
  }

  def delete[T <: Product: RowWriterFactory] (
      tableName: String,
      df: Dataset[T],
      writeConf: WriteConf = defaultWriteConf,
      count: Boolean = true) = {

    println(s"\n============= ${tableName} =============")
    spark.sparkContext.setJobDescription(tableName)
    if (count) println(s"Deleting ${df.count} rows...")
    df.rdd.deleteFromCassandra(BasicTables.transformedKeyspace, tableName, writeConf = writeConf)
  }
}
