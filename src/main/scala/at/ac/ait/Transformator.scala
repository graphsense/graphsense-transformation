package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import org.apache.spark.sql.functions.
  {col, count, lit, round, substring, sum, udf}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.annotation.tailrec

import at.ac.ait.{Fields => F}

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


  def addressRelations(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      regularInputs: Dataset[AddressTransactions],
      totalInput: Dataset[TotalInput],
      explicitlyKnownAddresses: Dataset[KnownAddress],
      clusterTags: Dataset[ClusterTags],
      addresses: Dataset[Address],
      exchangeRates: Dataset[ExchangeRates]) = {
    val fullAddressRelations = {
      val regularSum = "regularSum"
      val addressSum = "addressSum"
      val inValue = "inValue"
      val outValue = "outValue"
      val shortInputs =
        inputs.select(col(F.txHash), col(F.address) as F.srcAddress, col(F.value) as inValue)
      val shortOutputs =
        outputs.select(col(F.txHash), col(F.address) as F.dstAddress, col(F.value) as outValue)
      val regularInputSum = regularInputs.groupBy(F.txHash).agg(sum(F.value) as regularSum)
      val addressInputSum = inputs.groupBy(F.height, F.txHash).agg(sum(F.value) as addressSum)
      val reducedInputSum =
        addressInputSum.join(regularInputSum, F.txHash)
          .join(totalInput, F.txHash)
          .select(
            col(F.height),
            col(F.txHash),
            col(F.totalInput) - col(regularSum) + col(addressSum) as F.totalInput)
      val plainAddressRelations =
        shortInputs.join(shortOutputs, F.txHash)
          .join(reducedInputSum, F.txHash)
          .withColumn(
            F.estimatedValue,
            round(col(inValue) / col(F.totalInput) * col(outValue)) cast LongType)
          .drop(F.totalInput, inValue, outValue)
      toCurrencyDataFrame(exchangeRates, plainAddressRelations, List(F.estimatedValue))
        .drop(F.height)
    }
    val knownAddresses = {
      val implicitlyKnownAddresses =
        clusterTags.select(col(F.address), lit(1) as F.category).dropDuplicates().as[KnownAddress]
      explicitlyKnownAddresses.union(implicitlyKnownAddresses)
        .groupBy(F.address).max(F.category)
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
      .join(knownAddresses.toDF(F.srcAddress, F.srcCategory), List(F.srcAddress), "left_outer")
      .join(knownAddresses.toDF(F.dstAddress, F.dstCategory), List(F.dstAddress), "left_outer")
      .na.fill(0)
      .join(props.toDF(F.srcAddress, F.srcProperties), F.srcAddress)
      .join(props.toDF(F.dstAddress, F.dstProperties), F.dstAddress)
      .withColumn(F.srcAddressPrefix, substring(col(F.srcAddress), 0, 5))
      .withColumn(F.dstAddressPrefix, substring(col(F.dstAddress), 0, 5))
      .as[AddressRelations]
  }


  def clusterRelations(
      clusterInputs: Dataset[ClusterTransactions],
      clusterOutputs: Dataset[ClusterTransactions],
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      addressCluster: Dataset[AddressCluster],
      clusterTags: Dataset[ClusterTags],
      explicitlyKnownAddresses: Dataset[KnownAddress],
      cluster: Dataset[Cluster],
      addresses: Dataset[Address],
      exchangeRates: Dataset[ExchangeRates]) = {
    val fullClusterRelations = {
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
      val plainClusterRelations = allInputs.join(allOutputs, F.txHash)
      toCurrencyDataFrame(exchangeRates, plainClusterRelations, List(F.value)).drop(F.height)
    }
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
    val knownCluster =
      clusterTags.select(col(F.cluster), lit(2))
        .dropDuplicates()
        .union(explicitlyKnownAddresses.toDF(F.cluster, F.category))
    fullClusterRelations.groupBy(F.srcCluster, F.dstCluster)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Currency).apply(
          sum("value.satoshi"),
          sum("value.eur"),
          sum("value.usd")) as F.value)
      .join(knownCluster.toDF(F.srcCluster, F.srcCategory), List(F.srcCluster), "left_outer")
      .join(knownCluster.toDF(F.dstCluster, F.dstCategory), List(F.dstCluster), "left_outer")
      .na.fill(0)
      .join(props.toDF(F.srcCluster, F.srcProperties), F.srcCluster)
      .join(props.toDF(F.dstCluster, F.dstProperties), F.dstCluster)
      .as[ClusterRelations]
  }
}