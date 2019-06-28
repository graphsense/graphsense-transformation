package at.ac.ait

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.{
  col,
  count,
  explode,
  max,
  min,
  posexplode,
  size,
  struct,
  sum,
  udf
}
import org.apache.spark.sql.types.{IntegerType, StringType}

import at.ac.ait.{Fields => F}

class Transformation(spark: SparkSession) {

  import spark.implicits._

  val t = new Transformator(spark)

  def computeRegularInputs(tx: Dataset[Transaction]): Dataset[RegularInput] = {
    tx.withColumn("input", explode(col("inputs")))
      .filter(size(col("input.address")) === 1)
      .select(
        explode(col("input.address")) as "address",
        col("input.value") as "value",
        col(F.txHash)
      )
      .groupBy(F.txHash, F.address)
      .agg(sum(F.value) as F.value)
      .join(
        tx.select(
          col(F.txHash),
          col(F.height),
          col(F.txIndex),
          col(F.timestamp),
          col(F.coinjoin)
        ),
        Seq(F.txHash)
      )
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[RegularInput]
  }

  def computeRegularOutputs(
      tx: Dataset[Transaction]
  ): Dataset[RegularOutput] = {
    tx.select(
        posexplode(col("outputs")) as Seq(F.n, "output"),
        col(F.txHash),
        col(F.height),
        col(F.txIndex),
        col(F.timestamp),
        col(F.coinjoin)
      )
      .filter(size(col("output.address")) === 1)
      .select(
        col(F.txHash),
        explode(col("output.address")) as "address",
        col("output.value") as "value",
        col(F.height),
        col(F.txIndex),
        col(F.n),
        col(F.timestamp),
        col(F.coinjoin)
      )
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[RegularOutput]
  }

  def splitTransactions[A](txTable: Dataset[A])(implicit evidence: Encoder[A]) =
    (
      txTable.filter(col(F.value) < 0).withColumn(F.value, -col(F.value)).as[A],
      txTable.filter(col(F.value) > 0)
    )

  def computeStatistics[A](
      transactions: Dataset[Transaction],
      all: Dataset[A],
      in: Dataset[A],
      out: Dataset[A],
      idColumn: String,
      exchangeRates: Dataset[ExchangeRates]
  ) = {
    def statsPart(inOrOut: Dataset[_]) =
      t.toCurrencyDataFrame(exchangeRates, inOrOut, List(F.value))
        .groupBy(idColumn)
        .agg(
          count(F.txHash) cast IntegerType,
          udf(Currency)
            .apply(sum("value.satoshi"), sum("value.eur"), sum("value.usd"))
        )
    val inStats =
      statsPart(out).toDF(idColumn, F.noIncomingTxs, F.totalReceived)
    val outStats = statsPart(in).toDF(idColumn, F.noOutgoingTxs, F.totalSpent)
    val txTimes = transactions.select(
      col(F.txIndex),
      struct(F.height, F.txHash, F.timestamp)
    )
    val zeroValueIfNull = udf[Currency, Row] { b =>
      if (b != null)
        Currency(b.getAs[Long](0), b.getAs[Double](1), b.getAs[Double](2))
      else Currency(0, 0, 0)
    }
    all
      .groupBy(idColumn)
      .agg(min(F.txIndex) as "firstTxNumber", max(F.txIndex) as "lastTxNumber")
      .join(txTimes.toDF("firstTxNumber", F.firstTx), "firstTxNumber")
      .join(txTimes.toDF("lastTxNumber", F.lastTx), "lastTxNumber")
      .drop("firstTxNumber", "lastTxNumber")
      .join(inStats, idColumn)
      .join(outStats, List(idColumn), "left_outer")
      .na
      .fill(0)
      .withColumn(F.totalSpent, zeroValueIfNull(col(F.totalSpent)))
  }

  def computeNodeDegrees(
      nodes: Dataset[_],
      edges: Dataset[_],
      srcCol: String,
      dstCol: String,
      joinCol: String
  ) = {
    val outDegree = edges
      .groupBy(srcCol)
      .agg(count(dstCol) cast IntegerType as "outDegree")
      .withColumnRenamed(srcCol, joinCol)
    val inDegree = edges
      .groupBy(dstCol)
      .agg(count(srcCol) cast IntegerType as "inDegree")
      .withColumnRenamed(dstCol, joinCol)
    nodes
      .join(inDegree, Seq(joinCol), "outer")
      .join(outDegree, Seq(joinCol), "outer")
      .na
      .fill(0)
  }

  def computeAddressTransactions(
      tx: Dataset[Transaction],
      regInputs: Dataset[RegularInput],
      regOutputs: Dataset[RegularOutput]
  ): Dataset[AddressTransactions] = {
    regInputs
      .withColumn(F.value, -col(F.value))
      .union(regOutputs.drop(F.n))
      .groupBy(F.txHash, F.address)
      .agg(sum(F.value) as F.value)
      .join(
        tx.select(F.txHash, F.height, F.txIndex, F.timestamp).distinct(),
        F.txHash
      )
      .withColumn(F.addressPrefix, t.addressPrefixColumn)
      .sort(F.addressPrefix)
      .as[AddressTransactions]
  }

  def computeBasicAddresses(
      transactions: Dataset[Transaction],
      addressTransactions: Dataset[AddressTransactions],
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicAddress] = {
    computeStatistics(
      transactions,
      addressTransactions,
      inputs,
      outputs,
      F.address,
      exchangeRates
    ).withColumn(F.addressPrefix, t.addressPrefixColumn)
      .as[BasicAddress]
  }

  def computeAddressRelations(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[AddressRelations] = {
    t.addressRelations(
      inputs,
      outputs,
      regularInputs,
      transactions,
      addresses,
      exchangeRates
    )
  }

  def computeAddresses(
      basicAddresses: Dataset[BasicAddress],
      addressRelations: Dataset[AddressRelations]
  ): Dataset[Address] = {
    // compute in/out degrees for address graph
    computeNodeDegrees(
      basicAddresses,
      addressRelations.select(col(F.srcAddress), col(F.dstAddress)),
      F.srcAddress,
      F.dstAddress,
      F.address
    ).sort(F.addressPrefix)
      .as[Address]
  }

  def computeAddressTags(
      addresses: Dataset[BasicAddress],
      tags: Dataset[Tag],
      currency: String
  ): Dataset[AddressTags] = {
    tags
      .filter(col(F.currency) === currency)
      .drop(col(F.currency))
      .join(addresses, Seq(F.address), joinType = "left_semi")
      .as[AddressTags]
  }

  def computeAddressCluster(
      regularInputs: Dataset[RegularInput],
      regularOutputs: Dataset[RegularOutput],
      removeCoinJoin: Boolean
  ): Dataset[AddressCluster] = {
    t.addressCluster(regularInputs, regularOutputs, removeCoinJoin)
  }

  def computeBasicClusterAddresses(
      basicAddresses: Dataset[BasicAddress],
      addressCluster: Dataset[AddressCluster]
  ): Dataset[BasicClusterAddresses] = {
    addressCluster
      .join(basicAddresses, List(F.address, F.addressPrefix))
      .drop("addressPrefix")
      .as[BasicClusterAddresses]
      .sort(F.cluster, F.address)
  }

  def computeClusterTransactions(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      transactions: Dataset[Transaction],
      addressCluster: Dataset[AddressCluster]
  ): Dataset[ClusterTransactions] = {
    val clusteredInputs = inputs.join(addressCluster, F.address)
    val clusteredOutputs = outputs.join(addressCluster, F.address)
    clusteredInputs
      .withColumn(F.value, -col(F.value))
      .union(clusteredOutputs)
      .groupBy(F.txHash, F.cluster)
      .agg(sum(F.value) as F.value)
      .join(
        transactions.select(F.txHash, F.height, F.txIndex, F.timestamp),
        F.txHash
      )
      .as[ClusterTransactions]
  }

  def computeBasicCluster(
      transactions: Dataset[Transaction],
      basicClusterAddresses: Dataset[BasicClusterAddresses],
      clusterTransactions: Dataset[ClusterTransactions],
      clusterInputs: Dataset[ClusterTransactions],
      clusterOutputs: Dataset[ClusterTransactions],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicCluster] = {
    val noAddresses =
      basicClusterAddresses
        .groupBy(F.cluster)
        .agg(count("*") cast IntegerType as F.noAddresses)
    computeStatistics(
      transactions,
      clusterTransactions,
      clusterInputs,
      clusterOutputs,
      F.cluster,
      exchangeRates
    ).join(noAddresses, F.cluster)
      .as[BasicCluster]
  }

  def computePlainClusterRelations(
      clusterInputs: Dataset[ClusterTransactions],
      clusterOutputs: Dataset[ClusterTransactions]
  ): Dataset[PlainClusterRelations] = {
    t.plainClusterRelations(
      clusterInputs,
      clusterOutputs
    )
  }

  def computeClusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelations],
      cluster: Dataset[BasicCluster],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[ClusterRelations] = {
    t.clusterRelations(plainClusterRelations, cluster, addresses, exchangeRates)
  }

  def computeClusterAddresses(
      addresses: Dataset[Address],
      basicClusterAddresses: Dataset[BasicClusterAddresses]
  ): Dataset[ClusterAddresses] = {
    basicClusterAddresses
      .join(
        addresses.select(col(F.address), col("inDegree"), col("outDegree")),
        Seq(F.address),
        "left"
      )
      .as[ClusterAddresses]
  }

  def computeCluster(
      basicCluster: Dataset[BasicCluster],
      clusterRelations: Dataset[ClusterRelations]
  ): Dataset[Cluster] = {
    // compute in/out degrees for cluster graph
    // basicCluster contains only clusters of size > 1 with an integer ID
    // clusterRelations includes also cluster of size 1 (using the address string as ID)
    computeNodeDegrees(
      basicCluster.withColumn("cluster", $"cluster" cast StringType),
      clusterRelations.select(col(F.srcCluster), col(F.dstCluster)),
      F.srcCluster,
      F.dstCluster,
      F.cluster
    ).join(
        basicCluster.select(col(F.cluster) cast StringType),
        Seq(F.cluster),
        "right"
      )
      .withColumn(F.cluster, $"cluster" cast IntegerType)
      .as[Cluster]
  }

  def computeClusterTags(
      addressCluster: Dataset[AddressCluster],
      tags: Dataset[AddressTags]
  ): Dataset[ClusterTags] = {
    addressCluster.join(tags, F.address).as[ClusterTags]
  }

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Long,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long,
      noCluster: Long,
      noTags: Long
  ) = {
    Seq(
      SummaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noCluster,
        noTags
      )
    ).toDS()
  }
}
