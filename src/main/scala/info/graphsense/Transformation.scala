package info.graphsense

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  count,
  date_format,
  explode,
  from_unixtime,
  lit,
  map_keys,
  map_values,
  max,
  min,
  posexplode,
  row_number,
  size,
  struct,
  sum,
  to_date,
  typedLit,
  when
}
import org.apache.spark.sql.types.{FloatType, IntegerType}

import info.graphsense.{Fields => F}

class Transformation(
    spark: SparkSession,
    bucketSize: Int,
    addressPrefixLength: Int
) {

  import spark.implicits._

  private var noFiatCurrencies: Option[Int] = None

  val t = new Transformator(spark, bucketSize)

  def configuration(
      keyspaceName: String,
      bucketSize: Int,
      addressPrefixLength: Int,
      bech32Prefix: String,
      coinjoinFiltering: Boolean,
      fiatCurrencies: Seq[String]
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize,
        addressPrefixLength,
        bech32Prefix,
        coinjoinFiltering,
        fiatCurrencies
      )
    ).toDS()
  }

  def getFiatCurrencies(
      exchangeRatesRaw: Dataset[ExchangeRatesRaw]
  ): Seq[String] = {
    val currencies =
      exchangeRatesRaw.select(map_keys(col(F.fiatValues))).distinct
    if (currencies.count() > 1L)
      throw new Exception("Non-unique map keys in raw exchange rates table")
    currencies.rdd.map(r => r(0).asInstanceOf[Seq[String]]).collect()(0)
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
      .select(F.blockId, F.date)

    val lastDateExchangeRates =
      exchangeRates.select(max(F.date)).first.getString(0)
    val lastDateBlocks = blocksDate.select(max(F.date)).first.getString(0)
    if (lastDateExchangeRates < lastDateBlocks)
      println(
        "WARNING: exchange rates not available for all blocks, filling missing values with 0"
      )

    noFiatCurrencies = Some(
      exchangeRates.select(size(col(F.fiatValues))).distinct.first.getInt(0)
    )

    blocksDate
      .join(exchangeRates, Seq(F.date), "left")
      // replace null values in column fiatValues
      .withColumn(F.fiatValues, map_values(col(F.fiatValues)))
      .withColumn(
        F.fiatValues,
        coalesce(
          col(F.fiatValues),
          typedLit(Array.fill[Float](noFiatCurrencies.get)(0))
        )
      )
      .drop(F.date)
      .sort(F.blockId)
      .as[ExchangeRates]
  }

  def computeRegularInputs(tx: Dataset[Transaction]): Dataset[RegularInput] = {
    tx.withColumn("input", explode(col("inputs")))
      .filter(size(col("input.address")) === 1)
      .select(
        explode(col("input.address")).as(F.address),
        col("input.value").as(F.value),
        col(F.txId)
      )
      .groupBy(F.txId, F.address)
      .agg(sum(F.value).as(F.value))
      .join(
        tx.select(
          col(F.txId),
          col(F.blockId),
          col(F.timestamp),
          col(F.coinjoin)
        ),
        Seq(F.txId)
      )
      .as[RegularInput]
  }

  def computeRegularOutputs(
      tx: Dataset[Transaction]
  ): Dataset[RegularOutput] = {
    tx.select(
        posexplode(col("outputs")).as(Seq(F.n, "output")),
        col(F.txId),
        col(F.blockId),
        col(F.timestamp),
        col(F.coinjoin)
      )
      .filter(size(col("output.address")) === 1)
      .select(
        col(F.txId),
        explode(col("output.address")).as(F.address),
        col("output.value").as(F.value),
        col(F.blockId),
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
    val orderWindow = Window.partitionBy(F.address).orderBy(F.txId, F.n)
    regularOutputs
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort(F.txId, F.n)
      .select(F.address)
      .map(_.getString(0))
      .rdd
      .zipWithIndex()
      .map { case ((a, id)) => AddressId(a, id.toInt) }
      .toDS()
  }

  def computeAddressByAddressPrefix(
      addressIds: Dataset[AddressId],
      prefixLength: Int = addressPrefixLength,
      bech32Prefix: String = ""
  ): Dataset[AddressByAddressPrefix] = {
    addressIds
      .select(F.addressId, F.address)
      .transform(
        t.withAddressPrefix(
          F.address,
          F.addressPrefix,
          prefixLength,
          bech32Prefix
        )
      )
      .as[AddressByAddressPrefix]
  }

  def splitTransactions[A](
      transactions: Dataset[A]
  )(implicit evidence: Encoder[A]) =
    (
      transactions
        .filter(col(F.value) < 0)
        .withColumn(F.value, -col(F.value))
        .as[A],
      transactions.filter(col(F.value) > 0)
    )

  def computeStatistics[A](
      all: Dataset[A],
      in: Dataset[A],
      out: Dataset[A],
      idColumn: String,
      exchangeRates: Dataset[ExchangeRates]
  ) = {

    def zeroValueIfNull(columnName: String)(df: DataFrame): DataFrame = {
      df.withColumn(
        columnName,
        coalesce(
          col(columnName),
          struct(
            lit(0).as(F.value),
            typedLit(Array.fill[Float](noFiatCurrencies.get)(0))
              .as(F.fiatValues)
          )
        )
      )
    }

    def statsPart(
        inOrOut: Dataset[_],
        exchangeRates: Dataset[ExchangeRates],
        length: Int
    ) = {
      inOrOut
        .join(exchangeRates, Seq(F.blockId), "left")
        .transform(t.toFiatCurrency(F.value, F.fiatValues))
        .groupBy(idColumn)
        .agg(
          count(F.txId).cast(IntegerType),
          struct(
            sum(col(F.value)).as(F.value),
            array(
              (0 until length)
                .map(i => sum(col(F.fiatValues).getItem(i)).cast(FloatType)): _*
            ).as(F.fiatValues)
          )
        )
    }
    val inStats = statsPart(out, exchangeRates, noFiatCurrencies.get)
      .toDF(idColumn, F.noIncomingTxs, F.totalReceived)
    val outStats = statsPart(in, exchangeRates, noFiatCurrencies.get)
      .toDF(idColumn, F.noOutgoingTxs, F.totalSpent)

    all
      .groupBy(idColumn)
      .agg(
        min(F.txId).as(F.firstTxId),
        max(F.txId).as(F.lastTxId)
      )
      .join(inStats, idColumn)
      .join(outStats, Seq(idColumn), "left_outer")
      .na
      .fill(0)
      .transform(zeroValueIfNull("totalReceived"))
      .transform(zeroValueIfNull("totalSpent"))
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
      regInputs: Dataset[RegularInput],
      regOutputs: Dataset[RegularOutput],
      addressIds: Dataset[AddressId]
  ): Dataset[AddressTransaction] = {
    regInputs
      .withColumn(F.value, -col(F.value))
      .union(regOutputs.drop(F.n))
      .groupBy(F.txId, F.address)
      // blockId is constant in each group, get single value with min
      .agg(sum(F.value).as(F.value), min(F.blockId).as(F.blockId))
      .join(addressIds, Seq(F.address))
      .drop(F.addressPrefix, F.address)
      .withColumn(
        F.isOutgoing,
        when(col(F.value) < 0, lit(true)).otherwise(lit(false))
      )
      .transform(t.withIdGroup(F.addressId, F.addressIdGroup))
      .sort(F.addressIdGroup, F.addressId)
      .as[AddressTransaction]
  }

  def computeBasicAddresses(
      addressTransactions: Dataset[AddressTransaction],
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicAddress] = {
    computeStatistics(
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
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[AddressRelation] = {
    t.addressRelations(
      plainAddressRelations,
      exchangeRates,
      noFiatCurrencies.get
    )
  }

  def computeAddresses(
      basicAddresses: Dataset[BasicAddress],
      addressCluster: Dataset[AddressCluster],
      addressRelations: Dataset[AddressRelation],
      addressIds: Dataset[AddressId]
  ): Dataset[Address] = {
    // compute in/out degrees for address graph
    computeNodeDegrees(
      basicAddresses,
      addressRelations.select(col(F.srcAddressId), col(F.dstAddressId)),
      F.srcAddressId,
      F.dstAddressId,
      F.addressId
    ).join(addressIds, Seq(F.addressId))
      .transform(t.withIdGroup(F.addressId, F.addressIdGroup))
      .join(addressCluster, Seq(F.addressId), "left")
      .sort(F.addressId)
      .as[Address]
  }

  def computeAddressCluster(
      regularInputs: Dataset[RegularInput],
      addressIds: Dataset[AddressId],
      removeCoinJoin: Boolean
  ): Dataset[AddressCluster] = {
    t.addressCluster(regularInputs, addressIds, removeCoinJoin)
  }

  def computeClusterAddresses(
      addressCluster: Dataset[AddressCluster]
  ): Dataset[ClusterAddress] = {
    addressCluster
      .transform(t.withIdGroup(F.clusterId, F.clusterIdGroup))
      .sort(F.clusterId, F.addressId)
      .as[ClusterAddress]
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
      .groupBy(F.txId, F.clusterId)
      .agg(sum(F.value).as(F.value))
      .join(
        transactions.select(F.txId, F.blockId, F.txId),
        F.txId
      )
      .withColumn(
        F.isOutgoing,
        when(col(F.value) < 0, lit(true)).otherwise(lit(false))
      )
      .transform(t.withIdGroup(F.clusterId, F.clusterIdGroup))
      .sort(F.clusterIdGroup, F.clusterId)
      .as[ClusterTransaction]
  }

  def computeBasicCluster(
      clusterAddresses: Dataset[ClusterAddress],
      clusterTransactions: Dataset[ClusterTransaction],
      clusterInputs: Dataset[ClusterTransaction],
      clusterOutputs: Dataset[ClusterTransaction],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[BasicCluster] = {
    val noAddresses =
      clusterAddresses
        .groupBy(F.clusterId)
        .agg(count("*").cast(IntegerType).as(F.noAddresses))
    computeStatistics(
      clusterTransactions,
      clusterInputs,
      clusterOutputs,
      F.clusterId,
      exchangeRates
    ).join(noAddresses, F.clusterId)
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
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[ClusterRelation] = {
    t.clusterRelations(
      plainClusterRelations,
      exchangeRates,
      noFiatCurrencies.get
    )
  }

  def computeCluster(
      basicCluster: Dataset[BasicCluster],
      clusterRelations: Dataset[ClusterRelation]
  ): Dataset[Cluster] = {
    // compute in/out degrees for cluster graph
    computeNodeDegrees(
      basicCluster,
      clusterRelations.select(F.srcClusterId, F.dstClusterId),
      F.srcClusterId,
      F.dstClusterId,
      F.clusterId
    ).join(
        basicCluster.select(F.clusterId),
        Seq(F.clusterId),
        "right"
      )
      .transform(t.withIdGroup(F.clusterId, F.clusterIdGroup))
      .sort(F.clusterIdGroup, F.clusterId)
      .as[Cluster]
  }

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Int,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long,
      noCluster: Long,
      noClusterRelations: Long
  ) = {
    Seq(
      SummaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noCluster,
        noClusterRelations
      )
    ).toDS()
  }
}
