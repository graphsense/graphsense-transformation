package at.ac.ait

import org.apache.spark.sql.SparkSession

import at.ac.ait.{Fields => F}
import at.ac.ait.storage._

object TransformationJob {

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

    val src_keyspace = argsInstance.source_keyspace
    val keyspace = argsInstance.target_keyspace

    val spark = SparkSession.builder.appName(s"GraphSense Transformation [$keyspace]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Source keyspace: " + src_keyspace)
    println("Target keyspace: " + keyspace)

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val blocks = cassandra.load[Block](src_keyspace, "block")
    val transactions = cassandra.load[Transaction](src_keyspace, "transaction")
    val exchangeRates = cassandra.load[ExchangeRates](src_keyspace, "exchange_rates")
    val tags = cassandra.load[Tag](src_keyspace, "tag")

    val transformation = new Transformation(spark, transactions, exchangeRates, tags)

    cassandra.store(keyspace, "address_transactions", transformation.addressTransactions)
    cassandra.store(keyspace, "address_cluster", transformation.addressCluster)
    cassandra.store(keyspace, "address_tags", transformation.filteredTags)
    cassandra.store(keyspace, "cluster_tags", transformation.clusterTags)

    cassandra.store(
      keyspace,
      "address_incoming_relations",
      transformation.addressRelations.sort(F.dstAddressPrefix))
    cassandra.store(
      keyspace,
      "address_outgoing_relations",
      transformation.addressRelations.sort(F.srcAddressPrefix))
    cassandra.store(
      keyspace,
      "plain_cluster_relations",
      transformation.plainClusterRelations.sort(F.srcCluster))
    cassandra.store(
      keyspace,
      "cluster_incoming_relations",
      transformation.clusterRelations.sort(F.dstCluster, F.srcCluster))
    cassandra.store(
      keyspace,
      "cluster_outgoing_relations",
      transformation.clusterRelations.sort(F.srcCluster, F.dstCluster))

    cassandra.store(keyspace, "address", transformation.addresses)
    cassandra.store(keyspace, "cluster", transformation.cluster)
    cassandra.store(keyspace, "cluster_addresses", transformation.clusterAddresses)

    // table summary_statistics
    spark.sparkContext.setJobDescription("Compute summary statistics")
    val summaryStatistics =
      transformation.computeSummaryStatistics(blocks,
                                              transactions,
                                              transformation.basicAddresses,
                                              transformation.addressRelations,
                                              transformation.basicCluster)
    cassandra.store(keyspace, "summary_statistics", summaryStatistics)

    spark.stop()
  }
}
