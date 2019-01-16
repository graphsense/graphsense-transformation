package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.
  {col, collect_set, count, explode, lit, round, row_number, substring, sum, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.annotation.tailrec

import at.ac.ait.{Fields => F}
import at.ac.ait.clustering.common._


class Transformator(spark: SparkSession) {

  import spark.implicits._

  val addressPrefixColumn = substring(col(F.address), 0, 5) as F.addressPrefix

  def toCurrencyDataFrame(
      exchangeRates: Dataset[ExchangeRates],
      tableWithHeight: Dataset[_],
      columns: List[String]) = {
    val toCurrency = udf[Currency, Long, Double, Double] { (satoshi, eurPrice, usdPrice) =>
      val convert: (Long, Double) => Double = (satoshi, price) =>
        (satoshi * price / 1000000 + 0.5).toLong / 100.0
      Currency(satoshi, convert(satoshi, eurPrice), convert(satoshi, usdPrice))
    }
    @tailrec
    def processColumns(df: DataFrame, cs: List[String]): DataFrame =
      if (cs.isEmpty) df
      else processColumns(
        df.withColumn(cs(0), toCurrency(col(cs(0)), col(F.eur), col(F.usd))),
        cs.tail)
    processColumns(tableWithHeight.join(exchangeRates, F.height), columns).drop(F.eur, F.usd)
  }

  def addressCluster(
      regularInputs: Dataset[RegularInput],
      regularOutputs: Dataset[RegularOutput]) = {

    def plainAddressCluster(basicTxInputAddresses: DataFrame) = {

      val addrCount = count(F.addrId).over(Window.partitionBy(F.txIndex))

      // filter transactions with multiple input addresses
      val collectiveInputAddresses =
        basicTxInputAddresses.select(col(F.txIndex), col(F.addrId), addrCount as "count")
          .filter(col("count") > 1)
          .select(col(F.txIndex), col(F.addrId))

      // compute number of transaction per address
      val transactionCount =
        collectiveInputAddresses.groupBy(F.addrId).count()

      val basicAddressCluster = {
        // input for clustering algorithm
        // to optimize performance use only nontrivial addresses,
        // i.e. addresses which occur in multiple txes;
        // otherwise Spark errors were observed for BTC >= 480000 blocks
        val inputGroups = transactionCount
              .filter(col("count") > 1)
              .select(F.addrId)
              .join(collectiveInputAddresses, F.addrId)
              .groupBy(col(F.txIndex))
              .agg(collect_set(F.addrId) as "inputs")
              .select(col("inputs"))
              .as[InputIdSet]
              .rdd
              .toLocalIterator
        spark.sparkContext.parallelize(Clustering.getClustersMutable(inputGroups).toSeq).toDS()
      }

      val reprAddrId = "reprAddrId"

      val initialRepresentative = {
        val transactionWindow = Window.partitionBy(F.txIndex).orderBy(col("count").desc)
        val rowNumber = row_number().over(transactionWindow)

        val addressMax = collectiveInputAddresses
                           .join(transactionCount, F.addrId)
                           .select(col(F.txIndex), col(F.addrId), rowNumber as "rank")
                           .filter(col("rank") === 1)
                           .select(col(F.txIndex), col(F.addrId) as reprAddrId)

        transactionCount.filter(col("count") === 1)
          .select(F.addrId)
          .join(collectiveInputAddresses, F.addrId)
          .toDF(F.addrId, F.txIndex)
          .join(addressMax, F.txIndex)
          .select(F.addrId, reprAddrId)
      }

      val addressClusterRemainder =
        initialRepresentative.join(
            basicAddressCluster.toDF(reprAddrId, F.cluster), List(reprAddrId), "left_outer")
          .select(
            col(F.addrId),
            when(col(F.cluster).isNotNull, col(F.cluster)) otherwise col(reprAddrId) as F.cluster)
          .as[Result[Int]]

      basicAddressCluster.union(addressClusterRemainder)
    }

    // assign integer IDs to addresses
    // .withColumn("id", monotonically_increasing_id) could be used instead of zipWithIndex,
    // but this assigns Long values instead of Int
    val orderWindow = Window.partitionBy(F.address).orderBy(F.txIndex, F.n)
    val normalizedAddresses =
      regularOutputs.withColumn("rowNumber", row_number().over(orderWindow))
        .filter(col("rowNumber") === 1)
        .sort(F.txIndex, F.n)
        .select(F.address)
        .map(_ getString 0)
        .rdd
        .zipWithIndex()
        .map { case ((a, id)) => NormalizedAddress(id.toInt + 1, a) }
        .toDS()

    val inputIds = regularInputs
                     .join(normalizedAddresses, F.address)
                     .select(F.txIndex, F.addrId)

    // perform multiple-input clustering
    plainAddressCluster(inputIds).join(normalizedAddresses, F.addrId)
      .select(addressPrefixColumn, col(F.address), col(F.cluster))
      .as[AddressCluster]
}


  def addressRelations(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]) = {
    val fullAddressRelations = {

      val regularInputSum = regularInputs.groupBy(F.txHash).agg(sum(F.value) as "regularSum")
      val addressInputSum = inputs.groupBy(F.height, F.txHash).agg(sum(F.value) as "addressSum")
      val totalInput = transactions
        .withColumn("input", explode(col("inputs")))
        .select(F.txHash, "input.value")
        .groupBy(F.txHash).agg(sum(F.value) as F.totalInput)
      val reducedInputSum =
        addressInputSum.join(regularInputSum, F.txHash)
          .join(totalInput, F.txHash)
          .select(col(F.height),
                  col(F.txHash),
                  col(F.totalInput) - col("regularSum") + col("addressSum") as F.totalInput)
      reducedInputSum.show()
      val plainAddressRelations =
        inputs.select(col(F.txHash), col(F.address) as F.srcAddress, col(F.value) as "inValue")
          .join(outputs.select(col(F.txHash),
                               col(F.address) as F.dstAddress,
                               col(F.value) as "outValue"),
                F.txHash)
          .join(reducedInputSum, F.txHash)
          .withColumn(F.estimatedValue,
                      round(col("inValue")/col(F.totalInput)*col("outValue")) cast LongType)
          .drop(F.totalInput, "inValue", "outValue")
      // TODO exchangeRates needed?
      toCurrencyDataFrame(exchangeRates, plainAddressRelations, List(F.estimatedValue))
        .drop(F.height)
    }

    val props =
      addresses.select(
        col(F.address),
        udf(AddressSummary).apply(col("totalReceived.satoshi"), col("totalSpent.satoshi")))

    fullAddressRelations.groupBy(F.srcAddress, F.dstAddress)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("estimatedValue.satoshi"),
          sum("estimatedValue.eur"),
          sum("estimatedValue.usd")) as F.estimatedValue)
      .join(props.toDF(F.srcAddress, F.srcProperties), F.srcAddress)
      .join(props.toDF(F.dstAddress, F.dstProperties), F.dstAddress)
      .withColumn(F.srcAddressPrefix, substring(col(F.srcAddress), 0, 5))
      .withColumn(F.dstAddressPrefix, substring(col(F.dstAddress), 0, 5))
      .as[AddressRelations]
  }


  def plainClusterRelations(
      clusterInputs: Dataset[ClusterTransactions],
      clusterOutputs: Dataset[ClusterTransactions],
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      addressCluster: Dataset[AddressCluster]) = {
    val addressInputs =
      inputs.join(addressCluster, List(F.address), "left_anti")
        .select(col(F.txHash), col(F.address) as F.srcCluster)
    val addressOutputs =
      outputs.join(addressCluster, List(F.address), "left_anti")
        .select(col(F.txHash), col(F.address) as F.dstCluster, col(F.value), col(F.height))
    val allInputs =
      clusterInputs.select(col(F.txHash), col(F.cluster) as F.srcCluster)
        .union(addressInputs)
    val allOutputs =
      clusterOutputs
        .select(col(F.txHash), col(F.cluster) as F.dstCluster, col(F.value), col(F.height))
        .union(addressOutputs)
    allInputs.join(allOutputs, F.txHash).as[PlainClusterRelations]
  }


  def clusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelations],
      cluster: Dataset[BasicCluster],
      addresses: Dataset[BasicAddress],
      exchangeRates: Dataset[ExchangeRates]) = {
    val fullClusterRelations =
      toCurrencyDataFrame(exchangeRates, plainClusterRelations, List(F.value))
        .drop(F.height)
    val props = {
      val addressProps =
        addresses.select(
          col(F.address) as F.cluster,
          udf(ClusterSummary).apply(
            lit(1),
            col("totalReceived.satoshi"),
            col("totalSpent.satoshi")))
      cluster.select(
          col(F.cluster),
          udf(ClusterSummary).apply(
            col(F.noAddresses),
            col("totalReceived.satoshi"),
            col("totalSpent.satoshi")))
        .union(addressProps)
    }
    fullClusterRelations.groupBy(F.srcCluster, F.dstCluster)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("value.satoshi"),
          sum("value.eur"),
          sum("value.usd")) as F.value)
      .join(props.toDF(F.srcCluster, F.srcProperties), F.srcCluster)
      .join(props.toDF(F.dstCluster, F.dstProperties), F.dstCluster)
      .as[ClusterRelations]
  }
}
