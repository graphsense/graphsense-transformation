package at.ac.ait
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, collect_set, count, explode, floor, min,
  round, substring, sum, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.annotation.tailrec

import at.ac.ait.{Fields => F}
import at.ac.ait.clustering._

class Transformator(spark: SparkSession, bucketSize: Int) extends Serializable {

  import spark.implicits._

  def addressPrefix[T](
      addressColumn: String,
      prefixColumn: String,
      length: Int = 5,
      bech32Prefix: String = ""
  )(ds: Dataset[T]): DataFrame = {
    if (bech32Prefix.length == 0) {
      ds.withColumn(prefixColumn, substring(col(addressColumn), 0, length))
    } else {
      ds.withColumn(
        prefixColumn,
        when(
          substring(col(addressColumn), 0, bech32Prefix.length) === bech32Prefix,
          substring(
            col(addressColumn),
            bech32Prefix.length + 1,
            length
          )
        ).otherwise(substring(col(addressColumn), 0, length))
      )
    }
  }

  def idGroup[T](
      idColumn: String,
      idGroupColum: String,
      size: Int = bucketSize
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(idGroupColum, floor(col(idColumn) / size).cast("int"))
  }

  def toCurrencyDataFrame(
      exchangeRates: Dataset[ExchangeRates],
      tableWithHeight: Dataset[_],
      columns: List[String]
  ) = {
    val toCurrency = udf[Currency, Long, Float, Float] {
      (value, eurPrice, usdPrice) =>
        val convert: (Long, Float) => Float =
          (value, price) =>
            ((value * price / 1000000 + 0.5).toLong / 100.0).toFloat
        Currency(
          value,
          convert(value, eurPrice),
          convert(value, usdPrice)
        )
    }
    @tailrec
    def processColumns(df: DataFrame, cs: List[String]): DataFrame =
      if (cs.isEmpty) df
      else
        processColumns(
          df.withColumn(cs(0), toCurrency(col(cs(0)), col(F.eur), col(F.usd))),
          cs.tail
        )
    processColumns(tableWithHeight.join(exchangeRates, F.height), columns)
      .drop(F.eur, F.usd)
  }

  def toAddressSummary(received: Row, sent: Row) = {
    AddressSummary(
      Currency(
        received.getAs[Long]("value"),
        received.getAs[Float]("eur"),
        received.getAs[Float]("usd")
      ),
      Currency(
        sent.getAs[Long]("value"),
        sent.getAs[Float]("eur"),
        sent.getAs[Float]("usd")
      )
    )
  }

  def toClusterSummary(
      noAddresses: Int,
      received: Row,
      sent: Row
  ) = {
    ClusterSummary(
      noAddresses,
      Currency(
        received.getAs[Long]("value"),
        received.getAs[Float]("eur"),
        received.getAs[Float]("usd")
      ),
      Currency(
        sent.getAs[Long]("value"),
        sent.getAs[Float]("eur"),
        sent.getAs[Float]("usd")
      )
    )
  }

  def addressCluster(
      regularInputs: Dataset[RegularInput],
      addressIds: Dataset[AddressId],
      removeCoinJoin: Boolean
  ) = {

    def plainAddressCluster(
        basicTxInputAddresses: DataFrame,
        removeCoinJoin: Boolean
    ) = {

      val addressCount = count(F.addressId).over(Window.partitionBy(F.txIndex))

      // filter transactions with multiple input addresses
      val collectiveInputAddresses = (if (removeCoinJoin) {
        println("Clustering without coinjoin inputs")
        basicTxInputAddresses.filter(col(F.coinjoin) === false)
      } else {
        println("Clustering with coinjoin inputs")
        basicTxInputAddresses
      }).select(col(F.txIndex), col(F.addressId), addressCount as "count")
        .filter(col("count") > 1)
        .select(col(F.txIndex), col(F.addressId))

      // compute number of transactions per address
      val transactionCount =
        collectiveInputAddresses.groupBy(F.addressId).count()

      val basicAddressCluster = {
        // input for clustering algorithm
        // to optimize performance use only nontrivial addresses,
        // i.e., addresses which occur in multiple txes
        // (Spark errors were observed for BTC >= 480000 blocks)
        val inputGroups = transactionCount
          .filter(col("count") > 1)
          .select(F.addressId)
          .join(collectiveInputAddresses, F.addressId)
          .groupBy(col(F.txIndex))
          .agg(collect_set(F.addressId) as "inputs")
          .select(col("inputs"))
          .as[InputIdSet]
          .rdd
          .toLocalIterator

        spark.sparkContext
          .parallelize(
            MultipleInputClustering.getClustersMutable(inputGroups).toSeq
          )
          .toDS()
      }

      val reprAddrId = "reprAddrId"

      val initialRepresentative = {
        val addressMin = collectiveInputAddresses
          .groupBy(col(F.txIndex)).agg(min(F.addressId).as(reprAddrId))

        transactionCount
          .filter(col("count") === 1) // restrict to "trivial" addresses
          .select(F.addressId)
          .join(collectiveInputAddresses, F.addressId)
          .join(addressMin, F.txIndex)
          .select(F.addressId, reprAddrId)
      }

      val addressClusterRemainder =
        initialRepresentative
          .withColumnRenamed(F.addressId, "id")
          .join(
            basicAddressCluster.toDF(reprAddrId, F.cluster),
            List(reprAddrId),
            "left_outer"
          )
          .select(
            col("id"),
            coalesce(col(F.cluster), col(reprAddrId)) as F.cluster
          )
          .as[Result[Int]]

      basicAddressCluster.union(addressClusterRemainder).toDF()
    }

    val inputIds = regularInputs
      .join(addressIds, Seq(F.address))
      .select(F.txIndex, F.addressId, F.coinjoin)

    // perform multiple-input clustering
    val addressCluster = plainAddressCluster(inputIds, removeCoinJoin)

    val singleAddressCluster = addressIds
      .select(F.addressId)
      .distinct
      .join(
        addressCluster.withColumnRenamed("id", F.addressId),
        Seq(F.addressId),
        "left_anti"
      )
      .withColumn(F.cluster, col(F.addressId))

    singleAddressCluster
      .union(addressCluster)
      .join(addressIds, F.addressId)
      .select(F.addressId, F.cluster)
      .transform(idGroup(F.addressId, F.addressIdGroup))
      .as[AddressCluster]
  }

  def plainAddressRelations(
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction]
  ): Dataset[PlainAddressRelation] = {

    val regularInputSum =
      regularInputs.groupBy(F.txHash).agg(sum(F.value) as "regularSum")
    val addressInputSum =
      inputs.groupBy(F.height, F.txHash).agg(sum(F.value) as "addressSum")
    val totalInput = transactions
      .withColumn("input", explode(col("inputs")))
      .select(F.txHash, "input.value")
      .groupBy(F.txHash)
      .agg(sum(F.value) as F.totalInput)
    val reducedInputSum =
      addressInputSum
        .join(regularInputSum, F.txHash)
        .join(totalInput, F.txHash)
        .select(
          col(F.height),
          col(F.txHash),
          // regularSum == addressSum, unless input address is used as output in same tx
          col(F.totalInput) - col("regularSum") + col("addressSum") as F.totalInput
        )

    inputs
      .select(
        col(F.txHash),
        col(F.addressId) as F.srcAddressId,
        col(F.value) as "inValue"
      )
      .join(
        outputs.select(
          col(F.txHash),
          col(F.addressId) as F.dstAddressId,
          col(F.value) as "outValue"
        ),
        F.txHash
      )
      .join(reducedInputSum, F.txHash)
      .withColumn(
        F.estimatedValue,
        round(col("inValue") / col(F.totalInput) * col("outValue")) cast LongType
      )
      .drop(F.totalInput, "inValue", "outValue")
      .as[PlainAddressRelation]
  }

  def addressRelations(
      plainAddressRelations: Dataset[PlainAddressRelation],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates],
      addressTags: Dataset[AddressTag],
      txLimit: Int
  ): Dataset[AddressRelation] = {

    val props =
      addresses.select(
        col(F.addressId),
        udf(toAddressSummary _).apply($"totalReceived", $"totalSpent")
      )

    val addressLabels = addressTags
      .groupBy(F.addressId)
      .agg(collect_set(col(F.label)).as(F.label))

    val fullAddressRelations = toCurrencyDataFrame(
      exchangeRates,
      plainAddressRelations,
      List(F.estimatedValue)
    ).drop(F.height)
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("estimatedValue.value"),
          sum("estimatedValue.eur"),
          sum("estimatedValue.usd")
        ) as F.estimatedValue
      )
      .join(props.toDF(F.srcAddressId, F.srcProperties), F.srcAddressId)
      .join(props.toDF(F.dstAddressId, F.dstProperties), F.dstAddressId)
      .transform(idGroup(F.srcAddressId, F.srcAddressIdGroup))
      .transform(idGroup(F.dstAddressId, F.dstAddressIdGroup))
      .join(
        addressLabels.select(
          col(F.addressId).as(F.srcAddressId),
          col(F.label).as(F.srcLabels)
        ),
        Seq(F.srcAddressId),
        "left"
      )
      .join(
        addressLabels.select(
          col(F.addressId).as(F.dstAddressId),
          col(F.label).as(F.dstLabels)
        ),
        Seq(F.dstAddressId),
        "left"
      )

    val txList = plainAddressRelations
    // compute list column of transactions (only if #tx <= txLimit)
      .select(F.srcAddressId, F.dstAddressId, F.txHash)
      .join(
        fullAddressRelations
          .select(F.srcAddressId, F.dstAddressId, F.noTransactions),
        Seq(F.srcAddressId, F.dstAddressId),
        "full"
      )
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        collect_set(when(col(F.noTransactions) <= txLimit, col(F.txHash))) as F.txList
      )

    fullAddressRelations
      .join(txList, Seq(F.srcAddressId, F.dstAddressId), "left")
      .as[AddressRelation]
  }

  def plainClusterRelations(
      clusterInputs: Dataset[ClusterTransaction],
      clusterOutputs: Dataset[ClusterTransaction]
  ) = {
    clusterInputs
      .select(col(F.txHash), col(F.cluster) as F.srcCluster)
      .join(
        clusterOutputs
          .select(
            col(F.txHash),
            col(F.cluster) as F.dstCluster,
            col(F.value),
            col(F.height)
          ),
        Seq(F.txHash)
      )
      .as[PlainClusterRelation]
  }

  def clusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelation],
      cluster: Dataset[BasicCluster],
      exchangeRates: Dataset[ExchangeRates],
      clusterTags: Dataset[ClusterTag],
      txLimit: Int
  ) = {

    val props = cluster
      .select(
        col(F.cluster),
        udf(toClusterSummary _).apply(
          col(F.noAddresses),
          col("totalReceived"),
          col("totalSpent")
        )
      )

    val clusterLabels = clusterTags
      .groupBy(F.cluster)
      .agg(collect_set(col(F.label)).as(F.label))

    val fullClusterRelations =
      toCurrencyDataFrame(exchangeRates, plainClusterRelations, List(F.value))
        .drop(F.height)
        .groupBy(F.srcCluster, F.dstCluster)
        .agg(
          count(F.txHash) cast IntegerType as F.noTransactions,
          udf(Currency).apply(
            sum("value.value"),
            sum("value.eur"),
            sum("value.usd")
          ) as F.value
        )
        .join(props.toDF(F.srcCluster, F.srcProperties), F.srcCluster)
        .join(props.toDF(F.dstCluster, F.dstProperties), F.dstCluster)
        .transform(idGroup(F.srcCluster, F.srcClusterGroup))
        .transform(idGroup(F.dstCluster, F.dstClusterGroup))
        .join(
          clusterLabels.select(
            col(F.cluster).as(F.srcCluster),
            col(F.label).as(F.srcLabels)
          ),
          Seq(F.srcCluster),
          "left"
        )
        .join(
          clusterLabels.select(
            col(F.cluster).as(F.dstCluster),
            col(F.label).as(F.dstLabels)
          ),
          Seq(F.dstCluster),
          "left"
        )

    val txList = plainClusterRelations
    // compute list column of transactions (only if #tx <= txLimit)
      .select(F.srcCluster, F.dstCluster, F.txHash)
      .join(
        fullClusterRelations
          .select(F.srcCluster, F.dstCluster, F.noTransactions),
        Seq(F.srcCluster, F.dstCluster),
        "full"
      )
      .groupBy(F.srcCluster, F.dstCluster)
      .agg(
        collect_set(when(col(F.noTransactions) <= txLimit, col(F.txHash))) as F.txList
      )

    fullClusterRelations
      .join(txList, Seq(F.srcCluster, F.dstCluster), "left")
      .as[ClusterRelation]
  }
}
