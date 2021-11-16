package info.graphsense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}
import org.rogach.scallop._

import info.graphsense.{Fields => F}
import info.graphsense.storage._

object TransformationJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val currency: ScallopOption[String] = opt[String](
      required = true,
      descr = "Cryptocurrency (e.g. BTC, BCH, LTC, ZEC)"
    )
    val rawKeyspace: ScallopOption[String] =
      opt[String](
        "raw-keyspace",
        required = true,
        noshort = true,
        descr = "Raw keyspace"
      )
    val tagKeyspace: ScallopOption[String] =
      opt[String](
        "tag-keyspace",
        required = true,
        noshort = true,
        descr = "Tag keyspace"
      )
    val targetKeyspace: ScallopOption[String] = opt[String](
      "target-keyspace",
      required = true,
      noshort = true,
      descr = "Transformed keyspace"
    )
    val bucketSize: ScallopOption[Int] = opt[Int](
      "bucket-size",
      required = false,
      default = Some(25000),
      noshort = true,
      descr = "Bucket size for Cassandra partitions"
    )
    val addressPrefixLength: ScallopOption[Int] = opt[Int](
      "address-prefix-length",
      required = false,
      default = Some(4),
      noshort = true,
      descr = "Prefix length of address hashes for Cassandra partitioning keys"
    )
    val labelPrefixLength: ScallopOption[Int] = opt[Int](
      "label-prefix-length",
      required = false,
      default = Some(3),
      noshort = true,
      descr = "Prefix length of tag labels for Cassandra partitioning keys"
    )
    val coinjoinFilter: ScallopOption[Boolean] = toggle(
      "coinjoin-filtering",
      default = Some(true),
      noshort = true,
      prefix = "no-",
      descrYes = "Exclude coinJoin transactions from clustering",
      descrNo = "Include coinJoin transactions in clustering"
    )
    val bech32Prefix: ScallopOption[String] =
      opt[String](
        "bech32-prefix",
        default = Some(""),
        noshort = true,
        descr =
          "Bech32 address prefix (e.g. 'bc1' for Bitcoin or 'ltc1' for Litecoin)"
      )
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Currency:                      " + conf.currency())
    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Tag keyspace:                  " + conf.tagKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("Label prefix length:           " + conf.labelPrefixLength())
    println("CoinJoin Filtering enabled:    " + conf.coinjoinFilter())
    if (conf.bech32Prefix().length > 0) {
      println("Bech32 address prefix:         " + conf.bech32Prefix())
    }

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
    val addressTagsRaw = cassandra
      .load[AddressTagRaw](conf.tagKeyspace(), "address_tag_by_address")
    val clusterTagsRaw = cassandra
      .load[ClusterTagRaw](conf.tagKeyspace(), "entity_tag_by_id")

    val noBlocks = summaryStatisticsRaw.select(col("noBlocks")).first.getInt(0)
    val lastBlockTimestamp =
      summaryStatisticsRaw.select(col("timestamp")).first.getInt(0)
    val noTransactions =
      summaryStatisticsRaw.select(col("noTxs")).first.getLong(0)

    val transformation =
      new Transformation(spark, conf.bucketSize(), conf.addressPrefixLength())

    println("Store configuration")
    val configuration =
      transformation.configuration(
        conf.targetKeyspace(),
        conf.bucketSize(),
        conf.addressPrefixLength(),
        conf.bech32Prefix(),
        conf.labelPrefixLength(),
        conf.coinjoinFilter(),
        transformation.getFiatCurrencies(exchangeRatesRaw)
      )
    cassandra.store(
      conf.targetKeyspace(),
      "configuration",
      configuration
    )
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

    println("Computing address IDs")
    val addressIds =
      transformation.computeAddressIds(regOutputs)

    val addressByAddressPrefix = transformation.computeAddressByAddressPrefix(
      addressIds,
      bech32Prefix = conf.bech32Prefix()
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_prefix",
      addressByAddressPrefix
    )

    println("Computing address transactions")
    val addressTransactions =
      transformation
        .computeAddressTransactions(
          regInputs,
          regOutputs,
          addressIds
        )
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
          addressTransactions,
          inputs,
          outputs,
          exchangeRates
        )
        .persist()

    println("Computing address tags")
    val addressTags =
      transformation
        .computeAddressTags(
          addressTagsRaw,
          basicAddresses,
          addressIds,
          conf.currency()
        )
        .persist()
    cassandra.store(conf.targetKeyspace(), "address_tags", addressTags)
    val addressTagsByLabel =
      transformation.computeAddressTagsByLabel(
        addressTagsRaw,
        addressTags,
        conf.currency(),
        conf.labelPrefixLength()
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_tag_by_label",
      addressTagsByLabel
    )
    val noAddressTags = addressTags
      .select(col("label"))
      .withColumn("label", lower(col("label")))
      .distinct()
      .count()

    println("Computing plain address relations")
    val plainAddressRelations =
      transformation
        .computePlainAddressRelations(inputs, outputs, regInputs, transactions)

    println("Computing address relations")
    val addressRelations =
      transformation
        .computeAddressRelations(
          plainAddressRelations,
          exchangeRates,
          addressTags
        )
        .persist()
    val noAddressRelations = addressRelations.count()
    cassandra.store(
      conf.targetKeyspace(),
      "address_incoming_relations",
      addressRelations.sort(F.dstAddressIdGroup, F.dstAddressId)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_outgoing_relations",
      addressRelations.sort(F.srcAddressIdGroup, F.srcAddressId)
    )

    spark.sparkContext.setJobDescription("Perform clustering")
    println("Computing address clusters")
    val addressCluster = transformation
      .computeAddressCluster(regInputs, addressIds, conf.coinjoinFilter())
      .persist()

    println("Computing addresses")
    val addresses =
      transformation.computeAddresses(
        basicAddresses,
        addressCluster,
        addressRelations,
        addressIds
      )
    val noAddresses = addresses.count()
    cassandra.store(conf.targetKeyspace(), "address", addresses)

    println("Computing cluster addresses")
    val clusterAddresses =
      transformation
        .computeClusterAddresses(addressCluster)
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_addresses",
      clusterAddresses
    )

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
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_transactions",
      clusterTransactions
    )

    val (clusterInputs, clusterOutputs) =
      transformation.splitTransactions(clusterTransactions)
    clusterInputs.persist()
    clusterOutputs.persist()

    println("Computing cluster statistics")
    val basicCluster =
      transformation
        .computeBasicCluster(
          clusterAddresses,
          clusterTransactions,
          clusterInputs,
          clusterOutputs,
          exchangeRates
        )
        .persist()

    println("Computing cluster tags")
    val clusterTags =
      transformation
        .computeClusterTags(clusterTagsRaw, basicCluster, conf.currency())
        .persist()
    cassandra.store(conf.targetKeyspace(), "cluster_tags", clusterTags)
    val clusterTagsByLabel =
      transformation.computeClusterTagsByLabel(
        clusterTagsRaw,
        clusterTags,
        conf.currency(),
        conf.labelPrefixLength()
      )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_tag_by_label",
      clusterTagsByLabel
    )
    val clusterAddressTags =
      transformation.computeClusterAddressTags(addressCluster, addressTags)
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_address_tags",
      clusterAddressTags
    )

    println("Computing plain cluster relations")
    val plainClusterRelations =
      transformation
        .computePlainClusterRelations(clusterInputs, clusterOutputs)

    println("Computing cluster relations")
    val clusterRelations =
      transformation
        .computeClusterRelations(
          plainClusterRelations,
          exchangeRates,
          clusterTags
        )
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_incoming_relations",
      clusterRelations.sort(
        F.dstClusterIdGroup,
        F.dstClusterId,
        F.srcClusterId
      )
    )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_outgoing_relations",
      clusterRelations.sort(
        F.srcClusterIdGroup,
        F.srcClusterId,
        F.dstClusterId
      )
    )

    println("Computing cluster")
    val cluster =
      transformation
        .computeCluster(basicCluster, clusterRelations)
        .persist()
    val noCluster = cluster.count()
    cassandra.store(conf.targetKeyspace(), "cluster", cluster)

    println("Computing summary statistics")
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
