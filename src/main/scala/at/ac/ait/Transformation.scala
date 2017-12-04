package at.ac.ait

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.
  {col, collect_list, count, explode, hex, lit, lower, max, min, size, struct, substring, sum, udf}
import org.apache.spark.sql.types.IntegerType

import at.ac.ait.{Fields => F}

class Transformation(
    spark: SparkSession,
    rawTransactions: Dataset[RawTransaction],
    rawExchangeRates: Dataset[RawExchangeRates],
    rawTags: Dataset[RawTag]) {
  
  import spark.implicits._
  
  val t = new Transformator(spark)
  
  val rawOutputs =
    rawTransactions.withColumn(F.vout, explode(col(F.vout)))
      .select(F.txHash, F.txNumber, "vout.value", "vout.n", "vout.addresses")
  val rawInputs = {
    val outputReferences =
      rawTransactions.withColumn(F.vin, explode(col(F.vin)))
        .select(col(F.txHash), col(F.txNumber), $"vin.txid", $"vin.vout" as F.n)
    outputReferences
      .join(rawOutputs.drop(F.txNumber).withColumnRenamed(F.txHash, "txid"), List("txid", F.n))
      .select(F.txHash, F.txNumber, F.value, F.addresses)
  }.persist()
  
  def simplifyOutputs(rawOutputs: Dataset[Row]) =
    rawOutputs.filter(size(col(F.addresses)) === 1)
      .withColumn(F.address, explode(col(F.addresses)))
      .drop(F.addresses)
  val regularOutputs = simplifyOutputs(rawOutputs).as[RegularOutput].persist()
  val regularInputs = simplifyOutputs(rawInputs).as[RegularInput].persist()
  val addressTransactions =
    regularInputs.drop(F.txNumber)
      .withColumn(F.value, -col(F.value))
      .union(regularOutputs.drop(F.txNumber, F.n))
      .groupBy(F.txHash, F.address).agg(sum(F.value) as F.value)
      .join(rawTransactions.select(F.txHash, F.height, F.txNumber, F.timestamp), F.txHash)
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[AddressTransactions]
      .sort(F.addressPrefix)
      .persist()
  def inAndOutParts[A](tableWithValue: Dataset[A])(implicit evidence: Encoder[A]) = (
    tableWithValue.filter(col(F.value) < 0).withColumn(F.value, -col(F.value)).as[A],
    tableWithValue.filter(col(F.value) > 0)
  )
  val (inputs, outputs) = inAndOutParts(addressTransactions)
  val totalInput =
    rawInputs.groupBy(F.txHash).agg(sum(F.value) as F.totalInput).as[TotalInput].persist()
  val totalOutput = rawOutputs.groupBy(F.txHash).agg(sum(F.value) as F.totalOutput).persist()
  val transactions = {
    val groupedInputs =
      inputs.groupBy(F.txHash)
        .agg(collect_list(struct(col(F.address), col(F.value))) as F.inputs)
    val groupedOutputs =
      outputs.groupBy(F.txHash)
        .agg(collect_list(struct(col(F.address), col(F.value))) as F.outputs)
    rawTransactions.drop(F.vin, F.vout)
      .join(totalInput, List(F.txHash), "left_outer").na.fill(0)
      .join(totalOutput, F.txHash)
      .join(groupedInputs, List(F.txHash), "left_outer")
      .join(groupedOutputs, List(F.txHash), "left_outer")
      .withColumn(F.txPrefix, substring(lower(hex(col(F.txHash))), 0, 5))
      .as[Transaction]
      .sort(F.txPrefix)
  }
  val blockTransactions =
    rawTransactions.join(totalInput, List(F.txHash), "left_outer").na.fill(0)
      .join(totalOutput, F.txHash)
      .groupBy(F.height)
      .agg(collect_list(udf(TxSummary).apply(
        col(F.txHash),
        size(col(F.vin)),
        size(col(F.vout)),
        col(F.totalInput),
        col(F.totalOutput))) as F.txs)
      .as[BlockTransactions]
  val exchangeRates = t.exchangeRates(rawExchangeRates, rawTransactions).persist()
  def statistics[A](all: Dataset[A], in: Dataset[A], out: Dataset[A], idColumn: String) = {
    def statsPart(inOrOut: Dataset[_]) =
      t.toBitcoinDataFrame(exchangeRates, inOrOut, List(F.value)).groupBy(idColumn)
        .agg(
          count(F.txHash) cast IntegerType,
          udf(Bitcoin).apply(sum("value.satoshi"), sum("value.eur"), sum("value.usd")))
    val inStats = statsPart(out).toDF(idColumn, F.noIncomingTxs, F.totalReceived)
    val outStats = statsPart(in).toDF(idColumn, F.noOutgoingTxs, F.totalSpent)
    val firstTxNumber = "firstTxNumber"
    val lastTxNumber = "lastTxNumber"
    val txTimes = rawTransactions.select(col(F.txNumber), struct(F.height, F.txHash, F.timestamp))
    val zeroBtcIfNull = udf[Bitcoin, Row] { b =>
      if (b != null) Bitcoin(b.getAs[Long](0), b.getAs[Double](1), b.getAs[Double](2))
      else Bitcoin(0,0,0)
    }
    all.groupBy(idColumn)
      .agg(min(F.txNumber) as firstTxNumber, max(F.txNumber) as lastTxNumber)
      .join(txTimes.toDF(firstTxNumber, F.firstTx), firstTxNumber)
      .join(txTimes.toDF(lastTxNumber, F.lastTx), lastTxNumber)
      .drop(firstTxNumber, lastTxNumber)
      .join(inStats, idColumn)
      .join(outStats, List(idColumn), "left_outer").na.fill(0)
      .withColumn(F.totalSpent, zeroBtcIfNull(col(F.totalSpent)))
  }
  val addresses =
    statistics(addressTransactions, inputs, outputs, F.address)
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[Address]
      .sort(F.addressPrefix)
      .persist()
  val addressCluster =
    t.addressCluster(regularInputs, regularOutputs).sort(F.addressPrefix).persist()
  val clusterAddresses =
    addressCluster.join(addresses, F.address)
      .as[ClusterAddresses]
      .sort(F.cluster, F.address)
      .persist()
  val clusterTransactions = {
    val clusteredInputs =
      inputs.join(addressCluster, F.address).groupBy(F.txHash, F.cluster)
        .agg(sum(col(F.value)) as F.value)
    val clusteredOutputs =
      outputs.join(addressCluster, F.address).groupBy(F.txHash, F.cluster)
        .agg(sum(col(F.value)) as F.value)
    clusteredInputs.withColumn(F.value, -col(F.value))
      .union(clusteredOutputs)
      .groupBy(F.txHash, F.cluster).agg(sum(F.value) as F.value)
      .join(rawTransactions.select(F.txHash, F.height, F.txNumber, F.timestamp), F.txHash)
      .as[ClusterTransactions]
  }.persist()
  val (clusterInputs, clusterOutputs) = inAndOutParts(clusterTransactions)
  val cluster = {
    val noAddresses =
      clusterAddresses.groupBy(F.cluster).agg(count("*") cast IntegerType as F.noAddresses)
    statistics(clusterTransactions, clusterInputs, clusterOutputs, F.cluster)
      .join(noAddresses, F.cluster)
      .as[Cluster]
  }.persist()
  val clusterTags = addressCluster.join(rawTags, F.address).as[ClusterTags].persist()
  val explicitlyKnownAddresses =
    rawTags.select(col(F.address), lit(2) as F.category).dropDuplicates().as[KnownAddress].persist()
  val addressRelations = t.addressRelations(
    inputs, outputs, regularInputs, totalInput,
    explicitlyKnownAddresses, clusterTags, addresses, exchangeRates).persist()
  val clusterRelations = t.clusterRelations(
    clusterInputs, clusterOutputs, inputs, outputs, addressCluster,
    clusterTags, explicitlyKnownAddresses, cluster, addresses, exchangeRates).persist()
}
