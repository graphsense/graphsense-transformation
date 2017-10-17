package at.ac.ait

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  col, collect_list, count, explode, hex, lit, lower,
  max, min, size, struct, substring, sum, udf}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.collection.SortedMap

import at.ac.ait.clustering._
import at.ac.ait.storage._

class BasicTransformation(spark: SparkSession, cassandra: CassandraStorage) {

  import spark.implicits._

  // compute EUR / USD exchange rates for each height
  def computeExchangeRate
      (rawBlock: Dataset[RawBlock], rawExchangeRates: Dataset[RawExchangeRates]) = {

    import Currency._

    val rawMarketPrices = {
      for (currency <- Currency.values.toList)
      yield {
        val column = currency.toString.toLowerCase
        val entries = rawExchangeRates.select("timestamp", column)
          .filter(col(column).isNotNull)
          .as[(Int, Double)]
          .collect()
        (currency, SortedMap(entries.to: _*))
      }
    }.toMap

    val determineMarketPrice: (Currency, Int) => Double = { (currency, timestamp) =>
      val priceBefore = rawMarketPrices(currency).to(timestamp).lastOption
      val priceAfter = rawMarketPrices(currency).from(timestamp).headOption
      if (priceBefore.isEmpty) 0
      else if (priceAfter.isEmpty) priceBefore.get._2
      else {
        val p1 = priceBefore.get
        val p2 = priceAfter.get
        p1._2 + (timestamp - p1._1) * (p2._2 - p1._2) / (p2._1 - p1._1)
      }
    }
    val eurPrice = udf(determineMarketPrice curried EUR)
    val usdPrice = udf(determineMarketPrice curried USD)
    rawBlock.select(
        $"height",
        eurPrice($"timestamp") as "eur",
        usdPrice($"timestamp") as "usd")
      .as[ExchangeRates]
  }

  // block/{height}
  def computeBlock(rawBlock: Dataset[RawBlock]) =
    rawBlock.select($"height", $"blockHash", $"timestamp", $"blockVersion", $"size",
      size($"txs") as "noTransactions").as[Block]

  // block/{height}/transactions
  def computeBlockTransactions(tx: Dataset[Transaction]) = {
    tx.select(
      $"height",
      struct($"txHash",
        size($"inputs") as "noInputs",
        size($"outputs") as "noOutputs",
        $"totalInput",
        $"totalOutput") as "tx")
      .groupBy($"height")
      .agg(collect_list("tx") as "txs")
      .as[BlockTransactions]
  }

  // transaction/{tx_hash}
  def computeTransaction(rawTx: Dataset[RawTransaction]) = {

    val outputDF = rawTx.withColumn("vout", explode($"vout"))
      .withColumn("address", explode($"vout.addresses"))
      .select($"txHash", $"vout.value", $"vout.n", $"address")
      .cache

    val inputDF = rawTx.withColumn("vin", explode($"vin"))
      .select($"txHash", $"vin.txid", $"vin.vout" as "n")

    val joinedInputDF = inputDF
      .join(outputDF.withColumnRenamed("txHash", "txid"), Seq("txid", "n"), "left_outer")
      .select("txHash", "n", "address", "value")
      .cache

    val totalOutput = outputDF.groupBy($"txHash")
      .agg(sum("value") as "totalOutput")

    val totalInput = joinedInputDF.groupBy($"txHash")
      .agg(sum("value") as "totalInput")

    val basicInputList = joinedInputDF
      .groupBy($"txHash", $"address")
      .agg(sum("value") as "value")
      .select($"txHash", struct($"address", $"value") as "input")

    val inputList =
      rawTx.select("txHash")
        .join(basicInputList, Seq("txHash"), "left_outer")
        .groupBy("txHash")
        .agg(collect_list("input") as "inputs")

    val outputList = outputDF
      .groupBy($"txHash", $"address")
      .agg(sum("value") as "value")
      .select($"txHash", struct($"address", $"value") as "output")
      .groupBy("txHash")
      .agg(collect_list("output") as "outputs")

    rawTx.select(
        substring(lower(hex($"txHash")), 0, 5) as "txPrefix",
        $"txHash", $"height", $"timestamp", $"coinbase")
      .join(totalInput, Seq("txHash"), "left_outer")
      .join(totalOutput, Seq("txHash"))
      .join(inputList, Seq("txHash"))
      .join(outputList, Seq("txHash"), "left_outer")
      .na.fill(0, List("totalInput"))
      .as[Transaction]

  }

  // address/{address}/transactions
  def computeAddressTransactions(tx: Dataset[Transaction]) = {

    val inputs = tx.withColumn("input", explode($"inputs"))
      .select($"txHash", $"input.address", -$"input.value" as "value")

    val outputs = tx.withColumn("output", explode($"outputs"))
      .select($"txHash", $"output.address", $"output.value")

    val addresses = inputs.union(outputs)
      .groupBy("address", "txHash")
      .agg(sum("value") as "value")
      .filter($"address".isNotNull)
      .withColumn("addressPrefix", substring($"address", 0, 5))

    addresses.join(tx.select($"txHash", $"height", $"timestamp"), Seq("txHash"), "left_outer")
      .as[AddressTransactions]
  }

  def incomeAndOutgo
      (addrTxs: Dataset[AddressTransactions], exchangeRates: Dataset[ExchangeRates]) = {
    val in = CurrencyConversion.convertBitcoinValues(
      exchangeRates,
      "height",
      addrTxs.filter($"value" > 0),
      List("value"))
    val out = CurrencyConversion.convertBitcoinValues(
      exchangeRates,
      "height",
      addrTxs.filter($"value" < 0).withColumn("value", -$"value"),
      List("value"))
    (in, out)
  }

  // address/{address}
  def computeAddress
      (addrTxs: Dataset[AddressTransactions], income: Dataset[Row], outgo: Dataset[Row]) = {
    val totalReceived = income.groupBy($"address")
      .agg(
        count("txHash").cast(IntegerType) as "noIncomingTxs",
        sum("value.satoshi") as "satoshi",
        sum("value.eur") as "eur",
        sum("value.usd") as "usd")

    val totalSpent = outgo.groupBy($"address")
      .agg(
        count("txHash").cast(IntegerType) as "noOutgoingTxs",
        sum("value.satoshi") as "satoshi",
        sum("value.eur") as "eur",
        sum("value.usd") as "usd")

    addrTxs.groupBy("address")
      .agg(
        min(struct($"height", $"txHash", $"timestamp")) as "firstTx",
        max(struct($"height", $"txHash", $"timestamp")) as "lastTx")
      .withColumn("addressPrefix", substring($"address", 0, 5))
      .join(totalReceived, Seq("address"), "left_outer")
      .na.fill(0, Seq("noIncomingTxs", "satoshi", "eur", "usd"))
      .withColumn("totalReceived", struct("satoshi", "eur", "usd"))
      .drop("satoshi", "eur", "usd")
      .join(totalSpent, Seq("address"), "left_outer")
      .na.fill(0, Seq("noOutgoingTxs", "satoshi", "eur", "usd"))
      .withColumn("totalSpent", struct("satoshi", "eur", "usd"))
      .drop("satoshi", "eur", "usd")
      .as[Address]
  }

  def computeRelations(
      tx: Dataset[Transaction],
      income: Dataset[Row], outgo: Dataset[Row],
      exchangeRates: Dataset[ExchangeRates]) = {

    val inputPart =
      outgo.select($"txHash", $"address" as "srcAddress", $"value.satoshi" as "inValue")
    val outputPart =
      income.select($"txHash", $"address" as "dstAddress", $"value.satoshi" as "outValue")

    val relations = inputPart.join(outputPart, "txHash")
      .join(tx.filter($"totalInput" > 0).select($"txHash", $"totalInput", $"height"), "txHash")
      .withColumn("estimatedValue", ($"inValue" / $"totalInput" * $"outValue").cast(LongType))

    CurrencyConversion.convertBitcoinValues(
        exchangeRates,
        "height",
        relations,
        List("estimatedValue"))
  }

  // address/{address}/outgoingRelations
  def computeAddressOutgoingRelations(
      relations: Dataset[Row],
      addr: Dataset[Address],
      rawTag: Dataset[RawTag]) = {
    val nodePropertiesDF =
      addr.select(
        $"address",
        udf(AddressSummary).apply($"totalReceived.satoshi", $"totalSpent.satoshi") as "properties")
      .join(
        rawTag.select($"address", lit(2).cast(IntegerType) as "category").dropDuplicates(),
        Seq("address"), "left_outer")
      .na.fill(0, Seq("category"))

    relations.groupBy($"srcAddress", $"dstAddress")
      .agg(
        count($"txHash").cast(IntegerType) as "noTransactions",
        udf(Value).apply(
          sum($"estimatedValue.satoshi"),
          sum($"estimatedValue.eur"),
          sum($"estimatedValue.usd")) as "estimatedValue")
      .join(
        nodePropertiesDF.select(
          $"address" as "srcAddress",
          $"properties" as "srcProperties",
          $"category" as "srcCategory"),
        "srcAddress")
      .join(
        nodePropertiesDF.select(
          $"address" as "dstAddress",
          $"properties" as "dstProperties",
          $"category" as "dstCategory"),
        "dstAddress")
      .withColumn("srcAddressPrefix", substring($"srcAddress", 0, 5))
      .withColumn("dstAddressPrefix", substring($"dstAddress", 0, 5))
  }


  def processBlocks() {

    val cache = Util.cache(spark) _

    val rawExchangeRate = cassandra
      .load[RawExchangeRates](BasicTables.rawKeyspace, BasicTables.rawExchangeRatesTable)
    val rawBlock = cassandra.load[RawBlock](BasicTables.rawKeyspace, BasicTables.rawBlockTable)
    val rawTx = cassandra
      .load[RawTransaction](BasicTables.rawKeyspace, BasicTables.rawTransactionTable)
    val rawTag = cassandra
      .load[RawTag](BasicTables.rawKeyspace, BasicTables.rawTagTable)

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.exchangeRates}")
    val exchangeRate = computeExchangeRate(rawBlock, rawExchangeRate)
    cache(exchangeRate, BasicTables.exchangeRates)
    Util.time(cassandra.store(BasicTables.exchangeRates, exchangeRate))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.block}")
    val block = computeBlock(rawBlock)
    cache(block, BasicTables.block)
    Util.time(cassandra.store(BasicTables.block, block))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.transaction}")
    val tx = computeTransaction(rawTx).sort($"txPrefix")
    cache(tx, BasicTables.transaction)
    Util.time(cassandra.store(BasicTables.transaction, tx))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.blockTransactions}")
    val blockTx = computeBlockTransactions(tx)
    Util.time(cassandra.store(BasicTables.blockTransactions, blockTx))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.addressTransactions}")
    val addrTxs = computeAddressTransactions(tx).sort($"addressPrefix")
    cache(addrTxs, BasicTables.addressTransactions)
    Util.time(cassandra.store(BasicTables.addressTransactions, addrTxs))

    val (in, out) = incomeAndOutgo(addrTxs, exchangeRate)

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.address}")
    val addr = computeAddress(addrTxs, in, out).sort($"addressPrefix")
    cache(addr, BasicTables.address)
    Util.time(cassandra.store(BasicTables.address, addr))

    val relations = computeRelations(tx, in, out, exchangeRate).persist()

    spark.sparkContext.setJobDescription("Computing address graph relations")
    val addrRelations = computeAddressOutgoingRelations(relations, addr, rawTag)
    cache(addrRelations, BasicTables.addressOutgoingRelations)

    val addrOutgoingRelations =
      addrRelations.select(
        $"srcAddressPrefix", $"srcAddress",
        $"dstAddress", $"dstCategory", $"dstProperties",
        $"noTransactions", $"estimatedValue").as[AddressOutgoingRelations]

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.addressOutgoingRelations}")
    Util.time(cassandra.store(
      BasicTables.addressOutgoingRelations,
      addrOutgoingRelations.sort($"srcAddressPrefix")))

    val addrIncomingRelations =
      addrRelations.select(
        $"dstAddressPrefix", $"dstAddress",
        $"srcAddress", $"srcCategory", $"srcProperties",
        $"noTransactions", $"estimatedValue").as[AddressIncomingRelations]

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.addressIncomingRelations}")
    Util.time(cassandra.store(
      BasicTables.addressIncomingRelations,
      addrIncomingRelations.sort($"dstAddressPrefix")))

    val mic = new MultipleInputClustering(spark)

    // already computed datasets stored in Cassandra
    val txInputs = cassandra.load[TransactionInputs](
      BasicTables.transformedKeyspace,
      BasicTables.transaction,
      "tx_hash", "inputs")

    spark.sparkContext.setJobDescription(s"Writing table ${ClusterTables.addressCluster}")
    val addressCluster = mic.computeAddressCluster(txInputs)
    cache(addressCluster, ClusterTables.addressCluster)
    Util.time(cassandra.store(ClusterTables.addressCluster, addressCluster))

    spark.sparkContext.setJobDescription(s"Writing table ${ClusterTables.clusterAddresses}")
    val clusterAddresses = mic.computeClusterAddresses(addressCluster, addr)
    cache(clusterAddresses, ClusterTables.clusterAddresses)
    Util.time(cassandra.store(ClusterTables.clusterAddresses, clusterAddresses))

    spark.sparkContext.setJobDescription(s"Writing table ${ClusterTables.cluster}")
    val cluster = mic.computeCluster(clusterAddresses)
    Util.time(cassandra.store(ClusterTables.cluster, cluster))

    spark.sparkContext.setJobDescription(s"Writing table ${ClusterTables.clusterTags}")
    val clusterTags = mic.computeClusterTags(addressCluster, rawTag)
    cache(clusterTags, ClusterTables.clusterTags)
    Util.time(cassandra.store(ClusterTables.clusterTags, clusterTags))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.addressIncomingRelations}")
    val updatedAddrIncomingRelations =
      mic.updateAddressIncomingRelations(addrIncomingRelations, addressCluster, clusterTags)
    cache(updatedAddrIncomingRelations, BasicTables.addressIncomingRelations)
    Util.time(cassandra.store(
      BasicTables.addressIncomingRelations,
      updatedAddrIncomingRelations.sort($"dstAddressPrefix")))

    val removedAddrIncomingRelations =
      updatedAddrIncomingRelations.withColumn("srcCategory", lit(0).cast(IntegerType))
        .as[AddressIncomingRelations]
    Util.time(cassandra.delete(
      BasicTables.addressIncomingRelations,
      removedAddrIncomingRelations))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.addressOutgoingRelations}")
    val updatedAddrOutgoingRelations =
      mic.updateAddressOutgoingRelations(addrOutgoingRelations, addressCluster, clusterTags)
    cache(updatedAddrOutgoingRelations, "updated" + BasicTables.addressOutgoingRelations)
    Util.time(cassandra.store(
      BasicTables.addressOutgoingRelations,
      updatedAddrOutgoingRelations.sort($"srcAddressPrefix")))

    val removedAddrOutgoingRelations =
      updatedAddrOutgoingRelations.withColumn("dstCategory", lit(0).cast(IntegerType))
        .as[AddressOutgoingRelations]
    Util.time(cassandra.delete(
      BasicTables.addressOutgoingRelations,
      removedAddrOutgoingRelations))

    spark.sparkContext.setJobDescription("Computing cluster graph relations")
    val clusterRelations =
      mic.computeClusterRelations(relations, cluster, clusterTags, addressCluster)
    cache(clusterRelations, BasicTables.clusterOutgoingRelations)

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.clusterOutgoingRelations}")
    Util.time(cassandra.store(
      BasicTables.clusterIncomingRelations,
      clusterRelations.sort("srcCluster")))

    spark.sparkContext.setJobDescription(s"Writing table ${BasicTables.clusterIncomingRelations}")
    Util.time(cassandra.store(
      BasicTables.clusterIncomingRelations,
      clusterRelations.sort("dstCluster")))
    ()

  }

}
