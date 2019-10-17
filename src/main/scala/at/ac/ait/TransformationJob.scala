package at.ac.ait

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}
import org.rogach.scallop._

import at.ac.ait.{Fields => F}
import at.ac.ait.storage._

object TransformationJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val currency = opt[String](
      required = true,
      descr = "Cryptocurrency ticker symbol (e.g., BTC, BCH, LTC or ZEC)"
    )
    val rawKeyspace =
      opt[String]("raw_keyspace", required = true, descr = "Raw keyspace")
    val tagKeyspace =
      opt[String]("tag_keyspace", required = true, descr = "Tag keyspace")
    val targetKeyspace = opt[String](
      "target_keyspace",
      required = true,
      descr = "Transformed keyspace"
    )
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Currency:        " + conf.currency())
    println("Raw keyspace:    " + conf.rawKeyspace())
    println("Tag keyspace:    " + conf.tagKeyspace())
    println("Target keyspace: " + conf.targetKeyspace())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val summaryStatisticsRaw = cassandra
      .load[SummaryStatisticsRaw](conf.rawKeyspace(), "summary_statistics")
    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val tags = cassandra
      .load[Tag](conf.tagKeyspace(), "tag_by_address")

    val noBlocks = summaryStatisticsRaw.select(col("noBlocks")).first.getInt(0)
    val lastBlockTimestamp =
      summaryStatisticsRaw.select($"timestamp").first.getInt(0)
    val noTransactions =
      summaryStatisticsRaw.select(col("noTxs")).first.getLong(0)

    val transformation = new Transformation(spark)

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)

    println("Extracting transaction inputs")
    val regInputs = transformation.computeRegularInputs(transactions).persist()

    println("Extracting transaction outputs")
    val regOutputs =
      transformation.computeRegularOutputs(transactions).persist()

    println("Computing address transactions")
    val addressTransactions =
      transformation
        .computeAddressTransactions(transactions, regInputs, regOutputs)
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions",
      addressTransactions
    )

    val (inputs, outputs) =
      transformation.splitTransactions(addressTransactions)
    inputs.persist()
    outputs.persist()

    println("Computing address statistics")
    val basicAddresses =
      transformation
        .computeBasicAddresses(
          transactions,
          addressTransactions,
          inputs,
          outputs,
          exchangeRates
        )
        .persist()

    println("Computing address relations")
    val addressRelations =
      transformation
        .computeAddressRelations(
          inputs,
          outputs,
          regInputs,
          transactions,
          basicAddresses,
          exchangeRates
        )
        .persist()
    val noAddressRelations = addressRelations.count()
    cassandra.store(
      conf.targetKeyspace(),
      "address_incoming_relations",
      addressRelations.sort(F.dstAddressPrefix)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_outgoing_relations",
      addressRelations.sort(F.srcAddressPrefix)
    )

    println("Computing addresses")
    val addresses =
      transformation.computeAddresses(basicAddresses, addressRelations)
    val noAddresses = addresses.count()
    cassandra.store(conf.targetKeyspace(), "address", addresses)

    println("Computing address tags")
    val addressTags =
      transformation
        .computeAddressTags(basicAddresses, tags, conf.currency())
        .persist()
    val noAddressTags = addressTags
      .select(col("label"))
      .withColumn("label", lower(col("label")))
      .distinct()
      .count()
    cassandra.store(conf.targetKeyspace(), "address_tags", addressTags)

    spark.sparkContext.setJobDescription("Perform clustering")
    println("Computing address clusters")
    val addressCluster = transformation
      .computeAddressCluster(regInputs, regOutputs, true)
      .persist()
    cassandra.store(conf.targetKeyspace(), "address_cluster", addressCluster)

    println("Computing basic cluster addresses")
    val basicClusterAddresses =
      transformation
        .computeBasicClusterAddresses(basicAddresses, addressCluster)
        .persist()

    println("Computing cluster transactions")
    val clusterTransactions =
      transformation
        .computeClusterTransactions(
          inputs,
          outputs,
          transactions,
          addressCluster
        )
        .persist()

    val (clusterInputs, clusterOutputs) =
      transformation.splitTransactions(clusterTransactions)
    clusterInputs.persist()
    clusterOutputs.persist()

    println("Computing cluster statistics")
    val basicCluster =
      transformation
        .computeBasicCluster(
          transactions,
          basicClusterAddresses,
          clusterTransactions,
          clusterInputs,
          clusterOutputs,
          exchangeRates
        )
        .persist()

    println("Computing plain cluster relations")
    val plainClusterRelations =
      transformation
        .computePlainClusterRelations(
          clusterInputs,
          clusterOutputs
        )
        .persist()

    println("Computing cluster relations")
    val clusterRelations =
      transformation
        .computeClusterRelations(
          plainClusterRelations,
          basicCluster,
          basicAddresses,
          exchangeRates
        )
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_incoming_relations",
      clusterRelations.sort(F.dstCluster, F.srcCluster)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_outgoing_relations",
      clusterRelations.sort(F.srcCluster, F.dstCluster)
    )

    println("Computing cluster tags")
    val clusterTags =
      transformation.computeClusterTags(addressCluster, addressTags).persist()
    cassandra.store(conf.targetKeyspace(), "cluster_tags", clusterTags)

    println("Computing cluster")
    val cluster =
      transformation
        .computeCluster(basicCluster, clusterRelations, clusterTags)
        .persist()
    val noCluster = cluster.count()
    cassandra.store(conf.targetKeyspace(), "cluster", cluster)

    println("Computing cluster addresses")
    val clusterAddresses =
      transformation
        .computeClusterAddresses(addresses, basicClusterAddresses)
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_addresses",
      clusterAddresses
    )

    println("Compute summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noCluster,
        noAddressTags
      )
    summaryStatistics.show()
    cassandra.store(
      conf.targetKeyspace(),
      "summary_statistics",
      summaryStatistics
    )

    spark.stop()
  }
}
