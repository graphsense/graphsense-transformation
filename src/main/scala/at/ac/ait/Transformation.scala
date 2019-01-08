package at.ac.ait

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.
  {col, count, explode, lit, max, min, posexplode, size, struct, sum, udf}
import org.apache.spark.sql.types.{IntegerType, StringType}

import at.ac.ait.{Fields => F}


class Transformation(
    spark: SparkSession,
    transactions: Dataset[Transaction],
    exchangeRates: Dataset[ExchangeRates],
    tags: Dataset[Tag]) {

  import spark.implicits._

  val t = new Transformator(spark)

  def computeRegularInputs(tx: Dataset[Transaction]): Dataset[RegularInput] = {
    tx.withColumn("input", explode(col("inputs")))
      .filter(size(col("input.address")) === 1)
      .select(explode(col("input.address")) as "address",
              col("input.value") as "value",
              col(F.txHash),
              col(F.height),
              col(F.txIndex),
              col(F.timestamp))
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[RegularInput]
  }

  def computeRegularOutputs(tx: Dataset[Transaction]): Dataset[RegularOutput] = {
    tx.select(posexplode(col("outputs")) as Seq(F.n, "output"),
              col(F.txHash),
              col(F.height),
              col(F.txIndex),
              col(F.timestamp))
      .filter(size(col("output.address")) === 1)
      .select(explode(col("output.address")) as "address",
              col("output.value") as "value",
              col(F.txHash),
              col(F.height),
              col(F.txIndex),
              col(F.n),
              col(F.timestamp))
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[RegularOutput]
  }

  def computeAddressTransactions(
      tx: Dataset[Transaction],
      regInputs: Dataset[RegularInput],
      regOutputs: Dataset[RegularOutput]): Dataset[AddressTransactions] = {
    regInputs
      .withColumn(F.value, -col(F.value))
      .union(regOutputs.drop(F.n))
      .groupBy(F.txHash, F.address)
      .agg(sum(F.value) as F.value)
      .join(tx.select(F.txHash, F.height, F.txIndex, F.timestamp).distinct(),
            F.txHash)
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .sort(F.addressPrefix)
      .as[AddressTransactions]
  }

  def computeTotalInput(tx: Dataset[Transaction]): Dataset[TotalInput] = {
    tx.withColumn("input", explode(col("inputs")))
      .select(F.txHash, "input.value")
      .groupBy(F.txHash).agg(sum(F.value) as F.totalInput)
      .as[TotalInput]
  }

  def splitTransactions[A](txTable: Dataset[A])(implicit evidence: Encoder[A]) = (
    txTable.filter(col(F.value) < 0).withColumn(F.value, -col(F.value)).as[A],
    txTable.filter(col(F.value) > 0)
  )

  def computeStatistics[A](
      all: Dataset[A],
      in: Dataset[A],
      out: Dataset[A],
      idColumn: String,
      exchangeRates: Dataset[ExchangeRates]) = {
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

  def computeNodeDegrees(
      nodes: Dataset[_],
      edges: Dataset[_],
      srcCol: String,
      dstCol: String,
      joinCol: String) = {
    val outDegree = edges
      .groupBy(srcCol)
      .agg(count(dstCol) cast IntegerType as "outDegree" )
      .withColumnRenamed(srcCol, joinCol)
    val inDegree = edges
      .groupBy(dstCol)
      .agg(count(srcCol) cast IntegerType as "inDegree" )
      .withColumnRenamed(dstCol, joinCol)
    nodes
      .join(inDegree, Seq(joinCol), "outer")
      .join(outDegree, Seq(joinCol), "outer")
      .na.fill(0)
  }

  def computeSummaryStatistics(
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      addresses: Dataset[BasicAddress],
      addressRelations: Dataset[AddressRelations],
      cluster: Dataset[BasicCluster]) = {
    val noBlocks = blocks.count()
    val date = blocks.filter(col(F.height)===noBlocks-1)
                     .select(col(F.timestamp))
                     .first()
                     .getInt(0)
    Seq(SummaryStatistics(date,
                          noBlocks,
                          transactions.count(),
                          addresses.count(),
                          addressRelations.count(),
                          cluster.count()))
      .toDS()
 }


  spark.sparkContext.setJobDescription("Start transformation")

  val regularInputs = computeRegularInputs(transactions).persist()
  val regularOutputs = computeRegularOutputs(transactions).persist()
  // table address_transactions
  val addressTransactions =
    computeAddressTransactions(transactions, regularInputs, regularOutputs).persist()
  val totalInput = computeTotalInput(transactions).persist()
  val (inputs, outputs) = splitTransactions(addressTransactions)

  val basicAddresses =
    computeStatistics(addressTransactions, inputs, outputs, F.address, exchangeRates)
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[BasicAddress]
      .persist()

  // multiple input clustering
  println("Perform clustering")
  spark.sparkContext.setJobDescription("Perform clustering")
  // table address_cluster
  val addressCluster = t.addressCluster(regularInputs, regularOutputs).persist()

  val basicClusterAddresses =
    addressCluster.join(basicAddresses, F.address)
      .as[BasicClusterAddresses]
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

  val (clusterInputs, clusterOutputs) = splitTransactions(clusterTransactions)

  val basicCluster = {
    val noAddresses =
      basicClusterAddresses.groupBy(F.cluster).agg(count("*") cast IntegerType as F.noAddresses)
    computeStatistics(clusterTransactions, clusterInputs, clusterOutputs, F.cluster, exchangeRates)
      .join(noAddresses, F.cluster)
      .as[BasicCluster]
  }.persist()

  // table cluster_tags
  val clusterTags = addressCluster.join(tags, F.address).as[ClusterTags].persist()

  // table address_tags
  val filteredTags = tags.join(basicAddresses, Seq(F.address), joinType="left_semi").as[Tag]

  val explicitlyKnownAddresses =
    tags.select(col(F.address), lit(2) as F.category).dropDuplicates().as[KnownAddress].persist()

  // table address_incoming_relations/address_outgoing_relations
  val addressRelations =
    t.addressRelations(inputs,
                       outputs,
                       regularInputs,
                       totalInput,
                       explicitlyKnownAddresses,
                       clusterTags,
                       basicAddresses,
                       exchangeRates
                      ).persist()

  // table plain_cluster_relations
  val plainClusterRelations =
    t.plainClusterRelations(clusterInputs,
                            clusterOutputs,
                            inputs,
                            outputs,
                            addressCluster
                           ).persist()

  // table cluster_incoming_relations/cluster_outgoing_relations
  val clusterRelations =
    t.clusterRelations(plainClusterRelations,
                       clusterTags,
                       explicitlyKnownAddresses,
                       basicCluster,
                       basicAddresses,
                       exchangeRates
                      ).persist()

  // table addresses
  // compute in/out degrees for address graph
  spark.sparkContext.setJobDescription("Compute node degrees")
  val addresses =
    computeNodeDegrees(basicAddresses,
                       addressRelations.select(col(F.srcAddress), col(F.dstAddress)),
                       F.srcAddress,
                       F.dstAddress,
                       F.address)
      .sort(F.addressPrefix)
      .as[Address]
      .persist()

  // table cluster
  // compute in/out degrees for cluster graph
  // basicCluster contains only clusters of size > 1 with an integer ID
  // clusterRelations includes also cluster of size 1 (using the address string as ID)
  val cluster =
    computeNodeDegrees(basicCluster.withColumn("cluster", $"cluster" cast StringType),
                       clusterRelations.select(col(F.srcCluster), col(F.dstCluster)),
                       F.srcCluster,
                       F.dstCluster,
                       F.cluster)
      .join(basicCluster.select(col(F.cluster) cast StringType), Seq(F.cluster), "right")
      .withColumn(F.cluster, $"cluster" cast IntegerType)
      .as[Cluster]
      .persist()

  // table cluster_addresses
  val clusterAddresses =
    basicClusterAddresses
      .join(addresses.select(col(F.address), col("inDegree"), col("outDegree")),
            Seq(F.address),
            "left")
      .as[ClusterAddresses]
      .persist()
}
