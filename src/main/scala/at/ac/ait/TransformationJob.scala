package at.ac.ait

import com.datastax.spark.connector._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import at.ac.ait.{Fields => F}


case class BlockGroup(block_group: Int)

object TransformationJob {

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0)/1000 + "s")
    result
  }

  case class AppArgs(
      keyspace: String,
      maxBlocks: Int  // TODO
  )

  object AppArgs {
    def empty = new AppArgs("", 0)
  }

  def main(args: Array[String]) {

    val argsInstance: AppArgs = args.sliding(2, 1).toList.foldLeft(AppArgs.empty) {
      case (accumArgs, currArgs) => currArgs match {
        case Array("--keyspace", keyspace) => accumArgs.copy(keyspace = keyspace)
        case Array("--max_blocks", maxBlocks) => accumArgs.copy(maxBlocks = maxBlocks.toInt)
        case _ => accumArgs
      }
    }

    if (argsInstance.maxBlocks < 0 ||
        argsInstance.keyspace == "") {
      Console.err.println("Usage: spark-submit [...] graphsense-transformation.jar" +
        " --keyspace KEYSPACE" +
        " --max_blocks NUM_BLOCKS")
      sys.exit(1)
    }

    val spark = SparkSession.builder.appName("GraphSense Transformation [dev]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val keyspace = argsInstance.keyspace

    import spark.implicits._
    val transactions = spark.sparkContext.cassandraTable[Transaction](keyspace, "transaction").toDS()
    val exchangeRates = spark.sparkContext.cassandraTable[ExchangeRates](keyspace, "exchange_rates").toDS()
    val tags = spark.sparkContext.cassandraTable[Tag](keyspace, "tag").toDS()

    val transformation = new Transformation(spark, transactions, exchangeRates, tags)

    def save[A <: Product: ClassTag: TypeTag](table: Dataset[A], tableName: String) = {
      val description = "store " + tableName
      println(description)
      spark.sparkContext.setJobDescription(description)
      time{table.rdd.saveToCassandra(keyspace, tableName)}
      ()
    }


    save(transformation.addressTransactions, "address_transactions")
    save(transformation.addresses, "address")

    save(transformation.addressCluster, "address_cluster")
    save(transformation.filteredTags, "address_tags")
    save(transformation.clusterAddresses, "cluster_addresses")
    save(transformation.cluster, "cluster")
    save(transformation.clusterTags, "cluster_tags")

    save(transformation.addressRelations.sort(F.dstAddressPrefix), "address_incoming_relations")
    save(transformation.addressRelations.sort(F.srcAddressPrefix), "address_outgoing_relations")

    save(transformation.simpleClusterRelations.sort(F.srcCluster), "simple_cluster_relations")
    save(
      transformation.clusterRelations.sort(F.dstCluster, F.srcCluster),
      "cluster_incoming_relations")
    save(
      transformation.clusterRelations.sort(F.srcCluster, F.dstCluster),
      "cluster_outgoing_relations")

    spark.stop()
  }
}
