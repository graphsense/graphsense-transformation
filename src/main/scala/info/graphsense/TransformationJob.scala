package info.graphsense

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, max}
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
    val checkpointDir: ScallopOption[String] = opt[String](
      "checkpoint-dir",
      default = Some("file:///tmp/spark-checkpoint"),
      noshort = true,
      descr = "Spark checkpoint directory (HFDS in non-local mode)"
    )
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(conf.checkpointDir())

    println("Currency:                      " + conf.currency())
    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("CoinJoin Filtering enabled:    " + conf.coinjoinFilter())
    if (conf.bech32Prefix().length > 0) {
      println("Bech32 address prefix:         " + conf.bech32Prefix())
    }
    println("Spark checkpoint directory:    " + conf.checkpointDir())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block").persist()
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")

    val transformation =
      new Transformation(spark, conf.bucketSize(), conf.addressPrefixLength())

    println("Store configuration")
    val configuration =
      transformation.configuration(
        conf.targetKeyspace(),
        conf.bucketSize(),
        conf.addressPrefixLength(),
        conf.bech32Prefix(),
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

    val maxBlockExchangeRates =
      exchangeRates.select(max(col(F.blockId))).first.getInt(0)
    val transactionsFiltered =
      transactions.filter(col(F.blockId) <= maxBlockExchangeRates).persist()

    val maxBlock = blocks
      .filter(col(F.blockId) <= maxBlockExchangeRates)
      .select(
        max(col(F.blockId)).as("maxBlockId"),
        max(col(F.timestamp)).as("maxBlockTimestamp")
      )
      .withColumn("maxBlockDatetime", from_unixtime(col("maxBlockTimestamp")))
    val maxBlockTimestamp =
      maxBlock.select(col("maxBlockTimestamp")).first.getInt(0)
    val maxBlockDatetime =
      maxBlock.select(col("maxBlockDatetime")).first.getString(0)
    val maxTransactionId =
      transactionsFiltered.select(max(F.txId)).first.getLong(0)
    val noBlocks = maxBlockExchangeRates + 1
    val noTransactions = maxTransactionId + 1

    println(s"Max block timestamp: ${maxBlockDatetime}")
    println(s"Max block ID: ${maxBlockExchangeRates}")
    println(s"Max transaction ID: ${maxTransactionId}")

    println("Extracting transaction inputs")
    val regInputs =
      transformation.computeRegularInputs(transactionsFiltered).persist()

    println("Extracting transaction outputs")
    val regOutputs =
      transformation.computeRegularOutputs(transactionsFiltered).persist()

    println("Computing address IDs")
    val addressIds =
      transformation.computeAddressIds(regOutputs).persist()

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

    println("Computing plain address relations")
    val plainAddressRelations =
      transformation
        .computePlainAddressRelations(
          inputs,
          outputs,
          regInputs,
          transactionsFiltered
        )

    println("Computing address relations")
    val addressRelations =
      transformation
        .computeAddressRelations(
          plainAddressRelations,
          exchangeRates
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
          transactionsFiltered,
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
    val noCluster = basicCluster.count()

    println("Computing plain cluster relations")
    val plainClusterRelations =
      transformation
        .computePlainClusterRelations(plainAddressRelations, addressCluster)

    println("Computing cluster relations")
    val clusterRelations =
      transformation
        .computeClusterRelations(
          plainClusterRelations,
          exchangeRates
        )
        .persist()
    val noClusterRelations = clusterRelations.count()
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
    cassandra.store(conf.targetKeyspace(), "cluster", cluster)

    println("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        maxBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noCluster,
        noClusterRelations
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
