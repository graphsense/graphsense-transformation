package at.ac.ait

import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  col,
  count,
  date_format,
  explode,
  from_unixtime,
  lit,
  lower,
  max,
  min,
  posexplode,
  regexp_replace,
  row_number,
  size,
  struct,
  substring,
  sum,
  to_date,
  udf,
  unix_timestamp
}
import org.apache.spark.sql.types.{IntegerType, StringType}

import at.ac.ait.{Fields => F}

class Transformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  val t = new Transformator(spark, bucketSize)

  def configuration(
      keyspaceName: String,
      bucketSize: Int,
      bech32Prefix: String,
      coinjoinFiltering: Boolean
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize,
        bech32Prefix,
        coinjoinFiltering
      )
    ).toDS()
  }

  def computeExchangeRates(
      blocks: Dataset[Block],
      exchangeRates: Dataset[ExchangeRatesRaw]
  ): Dataset[ExchangeRates] = {
    val blocksDate = blocks
      .withColumn(
        F.date,
        date_format(
          to_date(from_unixtime(col(F.timestamp), "yyyy-MM-dd")),
          "yyyy-MM-dd"
        )
      )
      .select(F.height, F.date)

    val lastDateExchangeRates =
      exchangeRates.select(max(F.date)).first.getString(0)
    val lastDateBlocks = blocksDate.select(max(F.date)).first.getString(0)
    if (lastDateExchangeRates < lastDateBlocks)
      println(
        "WARNING: exchange rates not available for all blocks, filling missing values with 0"
      )

    blocksDate
      .join(exchangeRates, Seq(F.date), "left")
      .na
      .fill(0)
      .drop(F.date)
      .sort(F.height)
      .as[ExchangeRates]
  }

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
      .as[RegularOutput]
  }

  def computeAddressIds(
      regularOutputs: Dataset[RegularOutput]
  ): Dataset[AddressId] = {
    // assign integer IDs to addresses
    // .withColumn("id", monotonically_increasing_id) could be used instead of zipWithIndex,
    // (assigns Long values instead of Int)
    val orderWindow = Window.partitionBy(F.address).orderBy(F.txIndex, F.n)
    regularOutputs
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort(F.txIndex, F.n)
      .select(F.address)
      .map(_ getString 0)
      .rdd
      .zipWithIndex()
      .map { case ((a, id)) => AddressId(a, id.toInt) }
      .toDS()
  }

  def computeAddressByIdGroups(
      addressIds: Dataset[AddressId]
  ): Dataset[AddressByIdGroup] = {
    addressIds
      .select(F.addressId, F.address)
      .transform(t.idGroup(F.addressId, F.addressIdGroup))
      .as[AddressByIdGroup]
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
          count(F.txHash).cast(IntegerType),
          udf(Currency)
            .apply(sum("value.value"), sum("value.eur"), sum("value.usd"))
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
        Currency(b.getAs[Long](0), b.getAs[Float](1), b.getAs[Float](2))
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
      .agg(count(dstCol).cast(IntegerType).as("outDegree"))
      .withColumnRenamed(srcCol, joinCol)
    val inDegree = edges
      .groupBy(dstCol)
      .agg(count(srcCol).cast(IntegerType).as("inDegree"))
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
      regOutputs: Dataset[RegularOutput],
      addressIds: Dataset[AddressId]
  ): Dataset[AddressTransaction] = {
    regInputs
      .withColumn(F.value, -col(F.value))
      .union(regOutputs.drop(F.n))
      .groupBy(F.txHash, F.address)
      .agg(sum(F.value) as F.value)
      .join(
        tx.select(F.txHash, F.height, F.txIndex, F.timestamp).distinct(),
        F.txHash
      )
      .join(addressIds, Seq(F.address))
      .drop(F.addressPrefix, F.address)
      .transform(t.idGroup(F.addressId, F.addressIdGroup))
      .sort(F.addressIdGroup, F.addressId)
      .as[AddressTransaction]
  }

  def computeBasicAddresses(
      transactions: Dataset[Transaction],
      addressTransactions: Dataset[AddressTransaction],
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicAddress] = {
    computeStatistics(
      transactions,
      addressTransactions,
      inputs,
      outputs,
      F.addressId,
      exchangeRates
    ).as[BasicAddress]
  }

  def computePlainAddressRelations(
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction]
  ): Dataset[PlainAddressRelation] = {
    t.plainAddressRelations(
      inputs,
      outputs,
      regularInputs,
      transactions
    )
  }

  def computeAddressRelations(
      plainAddressRelations: Dataset[PlainAddressRelation],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates],
      addressTags: Dataset[AddressTag],
      txLimit: Int = 100
  ): Dataset[AddressRelation] = {
    t.addressRelations(
      plainAddressRelations,
      addresses,
      exchangeRates,
      addressTags,
      txLimit
    )
  }

  def computeAddresses(
      basicAddresses: Dataset[BasicAddress],
      addressRelations: Dataset[AddressRelation],
      addressIds: Dataset[AddressId],
      bech32Prefix: String = ""
  ): Dataset[Address] = {
    // compute in/out degrees for address graph
    computeNodeDegrees(
      basicAddresses,
      addressRelations.select(col(F.srcAddressId), col(F.dstAddressId)),
      F.srcAddressId,
      F.dstAddressId,
      F.addressId
    ).join(addressIds, Seq(F.addressId))
      .transform(
        t.addressPrefix(F.address, F.addressPrefix, bech32Prefix = bech32Prefix)
      )
      .sort(F.addressPrefix)
      .as[Address]
  }

  def computeAddressTags(
      tags: Dataset[AddressTagRaw],
      addresses: Dataset[BasicAddress],
      addressIds: Dataset[AddressId],
      currency: String
  ): Dataset[AddressTag] = {
    tags
      .filter(col(F.currency) === currency)
      .drop(col(F.currency))
      .join(addressIds.drop(F.addressPrefix), Seq(F.address))
      .join(addresses, Seq(F.addressId), joinType = "left_semi")
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .transform(t.idGroup(F.addressId, F.addressIdGroup))
      .sort(F.addressIdGroup, F.addressId)
      .as[AddressTag]
  }

  def computeAddressCluster(
      regularInputs: Dataset[RegularInput],
      addressIds: Dataset[AddressId],
      removeCoinJoin: Boolean
  ): Dataset[AddressCluster] = {
    t.addressCluster(regularInputs, addressIds, removeCoinJoin)
  }

  def computeBasicClusterAddresses(
      basicAddresses: Dataset[BasicAddress],
      addressCluster: Dataset[AddressCluster]
  ): Dataset[BasicClusterAddress] = {
    addressCluster
      .join(basicAddresses, Seq(F.addressId))
      .as[BasicClusterAddress]
      .sort(F.cluster, F.addressId)
  }

  def computeClusterTransactions(
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      transactions: Dataset[Transaction],
      addressCluster: Dataset[AddressCluster]
  ): Dataset[ClusterTransaction] = {
    val clusteredInputs = inputs.join(addressCluster, F.addressId)
    val clusteredOutputs = outputs.join(addressCluster, F.addressId)
    clusteredInputs
      .withColumn(F.value, -col(F.value))
      .union(clusteredOutputs)
      .groupBy(F.txHash, F.cluster)
      .agg(sum(F.value) as F.value)
      .join(
        transactions.select(F.txHash, F.height, F.txIndex, F.timestamp),
        F.txHash
      )
      .as[ClusterTransaction]
  }

  def computeBasicCluster(
      transactions: Dataset[Transaction],
      basicClusterAddresses: Dataset[BasicClusterAddress],
      clusterTransactions: Dataset[ClusterTransaction],
      clusterInputs: Dataset[ClusterTransaction],
      clusterOutputs: Dataset[ClusterTransaction],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicCluster] = {
    val noAddresses =
      basicClusterAddresses
        .groupBy(F.cluster)
        .agg(count("*").cast(IntegerType).as(F.noAddresses))
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
      clusterInputs: Dataset[ClusterTransaction],
      clusterOutputs: Dataset[ClusterTransaction]
  ): Dataset[PlainClusterRelation] = {
    t.plainClusterRelations(
      clusterInputs,
      clusterOutputs
    )
  }

  def computeClusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelation],
      cluster: Dataset[BasicCluster],
      exchangeRates: Dataset[ExchangeRates],
      clusterTags: Dataset[ClusterTag],
      txLimit: Int = 100
  ): Dataset[ClusterRelation] = {
    t.clusterRelations(
      plainClusterRelations,
      cluster,
      exchangeRates,
      clusterTags,
      txLimit
    )
  }

  def computeClusterAddresses(
      addresses: Dataset[Address],
      basicClusterAddresses: Dataset[BasicClusterAddress]
  ): Dataset[ClusterAddress] = {
    basicClusterAddresses
      .join(
        addresses.select(col(F.addressId), col("inDegree"), col("outDegree")),
        Seq(F.addressId),
        "left"
      )
      .transform(t.idGroup(F.cluster, F.clusterGroup))
      .as[ClusterAddress]
  }

  def computeCluster(
      basicCluster: Dataset[BasicCluster],
      clusterRelations: Dataset[ClusterRelation]
  ): Dataset[Cluster] = {
    // compute in/out degrees for cluster graph
    // basicCluster contains only clusters of size > 1 with an integer ID
    // clusterRelations includes also cluster of size 1 (using the address string as ID)
    computeNodeDegrees(
      basicCluster.withColumn(F.cluster, col(F.cluster) cast StringType),
      clusterRelations.select(col(F.srcCluster), col(F.dstCluster)),
      F.srcCluster,
      F.dstCluster,
      F.cluster
    ).join(
        basicCluster.select(col(F.cluster) cast StringType),
        Seq(F.cluster),
        "right"
      )
      .withColumn(F.cluster, col(F.cluster).cast(IntegerType))
      .transform(t.idGroup(F.cluster, F.clusterGroup))
      .sort(F.clusterGroup, F.cluster)
      .as[Cluster]
  }

  def computeClusterTags(
      tags: Dataset[ClusterTagRaw],
      cluster: Dataset[BasicCluster],
      currency: String
  ): Dataset[ClusterTag] = {
    tags
      .filter(col(F.currency) === currency)
      .withColumnRenamed("entity", F.cluster)
      .drop(col(F.currency))
      .join(
        cluster,
        Seq(F.cluster),
        joinType = "left_semi"
      )
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .transform(t.idGroup(F.cluster, F.clusterGroup))
      .sort(F.clusterGroup, F.cluster)
      .as[ClusterTag]
  }

  def computeClusterAddressTags(
      addressCluster: Dataset[AddressCluster],
      tags: Dataset[AddressTag]
  ): Dataset[ClusterAddressTag] = {
    addressCluster
      .join(tags, F.addressId)
      .transform(t.idGroup(F.cluster, F.clusterGroup))
      .sort(F.clusterGroup, F.cluster)
      .as[ClusterAddressTag]
  }

  def computeAddressTagsByLabel(
      tagsRaw: Dataset[AddressTagRaw],
      addressTags: Dataset[AddressTag],
      currency: String,
      prefixLength: Int = 3
  ): Dataset[AddressTagByLabel] = {
    // check if addresses where used in transactions
    tagsRaw
      .filter(col(F.currency) === currency)
      .join(
        addressTags
          .select(col(F.address))
          .withColumn(F.active, lit(true)),
        Seq(F.address),
        "left"
      )
      .na
      .fill(false, Seq(F.active))
      // normalize labels
      .withColumn(
        F.labelNorm,
        lower(regexp_replace(col(F.label), "[\\W_]+", ""))
      )
      .withColumn(
        F.labelNormPrefix,
        substring(col(F.labelNorm), 0, prefixLength)
      )
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .as[AddressTagByLabel]
  }

  def computeClusterTagsByLabel(
      tagsRaw: Dataset[ClusterTagRaw],
      clusterTags: Dataset[ClusterTag],
      currency: String,
      prefixLength: Int = 3
  ): Dataset[ClusterTagByLabel] = {
    // check if addresses where used in transactions
    tagsRaw
      .filter(col(F.currency) === currency)
      .withColumnRenamed("entity", F.cluster)
      .join(
        clusterTags
          .select(col(F.cluster))
          .withColumn(F.active, lit(true)),
        Seq(F.cluster),
        "left"
      )
      .na
      .fill(false, Seq(F.active))
      // normalize labels
      .withColumn(
        F.labelNorm,
        lower(regexp_replace(col(F.label), "[\\W_]+", ""))
      )
      .withColumn(
        F.labelNormPrefix,
        substring(col(F.labelNorm), 0, prefixLength)
      )
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .as[ClusterTagByLabel]
  }

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Int,
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
