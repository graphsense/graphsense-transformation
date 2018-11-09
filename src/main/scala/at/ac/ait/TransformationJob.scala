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
      source_keyspace: String,
      target_keyspace: String
  )

  object AppArgs {
    def empty = new AppArgs("", "")
  }

  def main(args: Array[String]) {

    val argsInstance: AppArgs = args.sliding(2, 1).toList.foldLeft(AppArgs.empty) {
      case (accumArgs, currArgs) => currArgs match {
        case Array("--source_keyspace", source_keyspace) =>
          accumArgs.copy(source_keyspace = source_keyspace)
        case Array("--target_keyspace", target_keyspace) =>
          accumArgs.copy(target_keyspace = target_keyspace)
        case _ => accumArgs
      }
    }

    if (argsInstance.source_keyspace == "" || argsInstance.target_keyspace == "") {
      Console.err.println("Usage: spark-submit [...] graphsense-transformation.jar" +
        " --source_keyspace SOURCE_KEYSPACE" +
        " --target_keyspace TARGET_KEYSPACE")
      sys.exit(1)
    }

    val spark = SparkSession.builder.appName("GraphSense Transformation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val src_keyspace = argsInstance.source_keyspace
    val keyspace = argsInstance.target_keyspace
    println("Source keyspace: " + src_keyspace)
    println("Target keyspace: " + keyspace)

    import spark.implicits._
    val blocks =
      spark.sparkContext.cassandraTable[Block](src_keyspace, "block").toDS()
    val transactions =
      spark.sparkContext.cassandraTable[Transaction](src_keyspace, "transaction").toDS()
    val exchangeRates =
      spark.sparkContext.cassandraTable[ExchangeRates](src_keyspace, "exchange_rates").toDS()
    val tags = spark.sparkContext.cassandraTable[Tag](src_keyspace, "tag").toDS()

    val transformation = new Transformation(spark, blocks, transactions, exchangeRates, tags)

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

    save(transformation.plainClusterRelations.sort(F.srcCluster), "plain_cluster_relations")
    save(
      transformation.clusterRelations.sort(F.dstCluster, F.srcCluster),
      "cluster_incoming_relations")
    save(
      transformation.clusterRelations.sort(F.srcCluster, F.dstCluster),
      "cluster_outgoing_relations")

    save(transformation.summaryStatistics, "summary_statistics")

    spark.stop()
  }
}
