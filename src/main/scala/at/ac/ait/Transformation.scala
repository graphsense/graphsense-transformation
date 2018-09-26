package at.ac.ait

import org.apache.spark.sql.{Encoder, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.
  {col, count, explode, lit, min, max, size, sum, struct, udf}
import org.apache.spark.sql.types.IntegerType

import at.ac.ait.{Fields => F}

import org.apache.spark.sql.functions.{collect_set, monotonically_increasing_id}
import collection.JavaConverters._
import linking.common._

class Transformation(
    spark: SparkSession,
    transactions: Dataset[Transaction],
    exchangeRates: Dataset[ExchangeRates],
    tags: Dataset[Tag]) {

  import spark.implicits._

  val t = new Transformator(spark)

  val regularInputs = transactions.withColumn("input", explode(col("inputs")))
                        .filter(size(col("input.address")) === 1)
                        .select(explode(col("input.address")) as "address",
                                col("input.value"),
                                col(F.txHash), col(F.height),
                                col(F.txIndex), col(F.timestamp))
                        .withColumn(F.addressPrefix, t.addressPrefixColumn)
                        .as[AddressTransactions]
                        .persist()

  val regularOutputs = transactions.withColumn("output", explode(col("outputs")))
                         .filter(size(col("output.address")) === 1)
                         .select(explode(col("output.address")) as "address",
                                 col("output.value"),
                                 col(F.txHash), col(F.height),
                                 col("txIndex"), col(F.timestamp))
                         .withColumn(F.addressPrefix, t.addressPrefixColumn)
                         .as[AddressTransactions]
                         .persist()

  // table address_transactions
  val addressTransactions = regularInputs.withColumn(F.value, -col(F.value))
                              .as[AddressTransactions]
                              .union(regularOutputs)
                              .groupBy(F.txHash, F.address)
                              .agg(sum(F.value) as F.value)
                              .join(
                                transactions.select(F.txHash, F.height, F.txIndex, F.timestamp).distinct(),
                                F.txHash)
                              .withColumn(F.addressPrefix, t.addressPrefixColumn)
                              .sort(F.addressPrefix)
                              .as[AddressTransactions]
                              .persist()

  def inAndOutParts[A](tableWithValue: Dataset[A])(implicit evidence: Encoder[A]) = (
    tableWithValue.filter(col(F.value) < 0).withColumn(F.value, -col(F.value)).as[A],
    tableWithValue.filter(col(F.value) > 0)
  )

  val (inputs, outputs) = inAndOutParts(addressTransactions)

  val totalInput = transactions
                     .withColumn("input", explode(col("inputs")))
                     .select(F.txHash, "input.value")
                     .groupBy(F.txHash).agg(sum(F.value) as F.totalInput)
                     .as[TotalInput]
                     .persist()

  def statistics[A](all: Dataset[A], in: Dataset[A], out: Dataset[A], idColumn: String) = {
    def statsPart(inOrOut: Dataset[_]) =
      t.toCurrencyDataFrame(exchangeRates, inOrOut, List(F.value)).groupBy(idColumn)
        .agg(
          count(F.txHash) cast IntegerType,
          udf(Currency).apply(sum("value.satoshi"), sum("value.eur"), sum("value.usd")))
    val inStats = statsPart(out).toDF(idColumn, F.noIncomingTxs, F.totalReceived)
    val outStats = statsPart(in).toDF(idColumn, F.noOutgoingTxs, F.totalSpent)
    val firstTxNumber = "firstTxNumber"
    val lastTxNumber = "lastTxNumber"
    val txTimes = transactions.select(col(F.txIndex), struct(F.height, F.txHash, F.timestamp))
    val zeroValueIfNull = udf[Currency, Row] { b =>
      if (b != null) Currency(b.getAs[Long](0), b.getAs[Double](1), b.getAs[Double](2))
      else Currency(0, 0, 0)
    }
    all.groupBy(idColumn)
      .agg(min(F.txIndex) as firstTxNumber,
           max(F.txIndex) as lastTxNumber)
      .join(txTimes.toDF(firstTxNumber, F.firstTx), firstTxNumber)
      .join(txTimes.toDF(lastTxNumber, F.lastTx), lastTxNumber)
      .drop(firstTxNumber, lastTxNumber)
      .join(inStats, idColumn)
      .join(outStats, List(idColumn), "left_outer").na.fill(0)
      .withColumn(F.totalSpent, zeroValueIfNull(col(F.totalSpent)))
  }

  // table address
  val addresses = statistics(addressTransactions, inputs, outputs, F.address)
    .withColumn(F.addressPrefix, t.addressPrefixColumn)
    .as[Address]
    .sort(F.addressPrefix)
    .persist()


  // clustering
  val addressId = regularOutputs
                    .select(F.address)
                    .distinct
                    .withColumn("id", monotonically_increasing_id)
                    .as[NormalizedAddress]
                    .persist()

  val inputIds = regularInputs
                   .join(addressId, F.address)
                   .select(F.txIndex, F.id)
                   .groupBy(col(F.txIndex))
                   .agg(collect_set(F.id) as "inputs")
                   .select(col("inputs"))
                   .filter(size($"inputs") > 1)
                   .as[InputIdSet]

  // table address_cluster
  val addressCluster = Clustering.getClustersMutable(inputIds.toLocalIterator.asScala)
                         .toList
                         .toDS
                         .join(addressId, F.id)
                         .withColumn(F.addressPrefix, t.addressPrefixColumn)
                         .select(col(F.addressPrefix), col(F.address), col(F.cluster))
                         .as[AddressCluster]
                         .sort(F.addressPrefix)
                         .persist()

  val clusterAddresses =
    addressCluster.join(addresses, F.address)
      .as[ClusterAddresses]
      .sort(F.cluster, F.address)
      .persist()

  val clusterTransactions = {
    val clusteredInputs = inputs.join(addressCluster, F.address)
    val clusteredOutputs = outputs.join(addressCluster, F.address)
    clusteredInputs.withColumn(F.value, -col(F.value))
      .union(clusteredOutputs)
      .groupBy(F.txHash, F.cluster).agg(sum(F.value) as F.value)
      .join(transactions.select(F.txHash, F.height, F.txIndex, F.timestamp), F.txHash)
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
  val clusterTags = addressCluster.join(tags, F.address).as[ClusterTags].persist()
  val filteredTags = tags.join(addresses, Seq(F.address), joinType="left_semi").as[Tag]

  val explicitlyKnownAddresses =
    tags.select(col(F.address), lit(2) as F.category).dropDuplicates().as[KnownAddress].persist()

  val addressRelations = t.addressRelations(
    inputs, outputs, regularInputs, totalInput,
    explicitlyKnownAddresses, clusterTags, addresses, exchangeRates).persist()

  val clusterRelations = t.clusterRelations(
    clusterInputs, clusterOutputs, inputs, outputs, addressCluster,
    clusterTags, explicitlyKnownAddresses, cluster, addresses, exchangeRates)
}
