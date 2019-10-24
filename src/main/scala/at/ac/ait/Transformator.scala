package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  collect_set,
  count,
  explode,
  floor,
  round,
  row_number,
  substring,
  sum,
  udf
}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.annotation.tailrec

import at.ac.ait.{Fields => F}
import at.ac.ait.clustering._

class Transformator(spark: SparkSession) {

  import spark.implicits._

  def addressPrefix[T](
      addressColumn: String,
      prefixColumn: String,
      length: Int = 5
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(prefixColumn, substring(col(addressColumn), 0, length))
  }

  def addressIdGroup[T](
      idColumn: String,
      idGroupColum: String,
      size: Int = 5000
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(idGroupColum, floor(col(idColumn) / size).cast("int"))
  }

  def toCurrencyDataFrame(
      exchangeRates: Dataset[ExchangeRates],
      tableWithHeight: Dataset[_],
      columns: List[String]
  ) = {
    val toCurrency = udf[Currency, Long, Double, Double] {
      (satoshi, eurPrice, usdPrice) =>
        val convert: (Long, Double) => Double =
          (satoshi, price) => (satoshi * price / 1000000 + 0.5).toLong / 100.0
        Currency(
          satoshi,
          convert(satoshi, eurPrice),
          convert(satoshi, usdPrice)
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
      val collectiveInputAddresses = if (removeCoinJoin) {
        println("Clustering without coinjoin inputs")
        basicTxInputAddresses.filter(col(F.coinjoin) === false)
      } else {
        println("Clustering with coinjoin inputs")
        basicTxInputAddresses
      }.select(col(F.txIndex), col(F.addressId), addressCount as "count")
        .filter(col("count") > 1)
        .select(col(F.txIndex), col(F.addressId))

      // compute number of transaction per address
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
        val transactionWindow =
          Window.partitionBy(F.txIndex).orderBy(col("count").desc)
        val rowNumber = row_number().over(transactionWindow)

        val addressMax = collectiveInputAddresses
          .join(transactionCount, F.addressId)
          .select(col(F.txIndex), col(F.addressId), rowNumber as "rank")
          .filter(col("rank") === 1)
          .select(col(F.txIndex), col(F.addressId) as reprAddrId)

        transactionCount
          .filter(col("count") === 1)
          .select(F.addressId)
          .join(collectiveInputAddresses, F.addressId)
          .join(addressMax, F.txIndex)
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
      .join(addressIds, Seq(F.addressPrefix, F.address))
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
      .as[AddressCluster]
  }

  def addressRelations(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[AddressRelations] = {
    val fullAddressRelations = {

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

      val plainAddressRelations =
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
      toCurrencyDataFrame(
        exchangeRates,
        plainAddressRelations,
        List(F.estimatedValue)
      ).drop(F.height)
    }

    val props =
      addresses.select(
        col(F.addressId),
        udf(AddressSummary)
          .apply(col("totalReceived.satoshi"), col("totalSpent.satoshi"))
      )

    fullAddressRelations
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("estimatedValue.satoshi"),
          sum("estimatedValue.eur"),
          sum("estimatedValue.usd")
        ) as F.estimatedValue
      )
      .join(props.toDF(F.srcAddressId, F.srcProperties), F.srcAddressId)
      .join(props.toDF(F.dstAddressId, F.dstProperties), F.dstAddressId)
      .transform(addressIdGroup(F.srcAddressId, F.srcAddressIdGroup))
      .transform(addressIdGroup(F.dstAddressId, F.dstAddressIdGroup))
      .as[AddressRelations]
  }

  def plainClusterRelations(
      clusterInputs: Dataset[ClusterTransactions],
      clusterOutputs: Dataset[ClusterTransactions]
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
      .as[PlainClusterRelations]
  }

  def clusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelations],
      cluster: Dataset[BasicCluster],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]
  ) = {
    val fullClusterRelations =
      toCurrencyDataFrame(exchangeRates, plainClusterRelations, List(F.value))
        .drop(F.height)

    val props = cluster
      .select(
        col(F.cluster),
        udf(ClusterSummary).apply(
          col(F.noAddresses),
          col("totalReceived.satoshi"),
          col("totalSpent.satoshi")
        )
      )

    fullClusterRelations
      .groupBy(F.srcCluster, F.dstCluster)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("value.satoshi"),
          sum("value.eur"),
          sum("value.usd")
        ) as F.value
      )
      .join(props.toDF(F.srcCluster, F.srcProperties), F.srcCluster)
      .join(props.toDF(F.dstCluster, F.dstProperties), F.dstCluster)
      .as[ClusterRelations]
  }
}
