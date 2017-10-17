package at.ac.ait

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import at.ac.ait.clustering._
import at.ac.ait.storage._

object TransformationControl {

  def main(args: Array[String]): Unit = {

    val startLocal = args.contains("local")

    val basicConf = new SparkConf()
      .setAppName("GraphSense Transformation")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[RawInput],
        classOf[RawOutput],
        classOf[RawBlock],
        classOf[RawTransaction],
        classOf[RawExchangeRates],
        classOf[RawTag],
        classOf[TxSummary],
        classOf[TxInputOutput],
        classOf[TxIdTime],
        classOf[Value],
        classOf[AddressSummary],
        classOf[ExchangeRates],
        classOf[Block],
        classOf[Transaction],
        classOf[BlockTransactions],
        classOf[AddressTransactions],
        classOf[Address],
        classOf[AddressOutgoingRelations],
        classOf[AddressIncomingRelations],
        classOf[TransactionInputs],
        classOf[InputRelation],
        classOf[ClusteringResult],
        classOf[AddressCluster],
        classOf[Cluster],
        classOf[ClusterAddresses],
        classOf[ClusterTags]
      ))

    val conf =
      if (startLocal)
        basicConf.setMaster("local[4]")
          .set("spark.cassandra.connection.host", "127.0.0.1")
          .set("spark.executor.memory", "1g")
      else basicConf

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cassandra = new CassandraStorage(spark)

    val basicTransformation = new BasicTransformation(spark, cassandra)
    basicTransformation.processBlocks()

    spark.stop()

    
  }

}
