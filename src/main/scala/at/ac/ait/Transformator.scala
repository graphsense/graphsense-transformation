package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.
  {col, count, lit, round, row_number, substring, sum, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType}
import scala.annotation.tailrec
import scala.collection.SortedMap

import Currency._
import at.ac.ait.{Fields => F}
import entity.AddressMapping

class Transformator(spark: SparkSession) {
  
  import spark.implicits._

  val addressPrefixColumn = substring(col(F.address), 0, 5) as F.addressPrefix
  
  def exchangeRates(
      rawExchangeRates: Dataset[RawExchangeRates],
      rawTransactions: Dataset[RawTransaction]) = {
    val blocks = rawTransactions.select(F.height, F.timestamp).dropDuplicates()
    val rawMarketPrices = {
      for (currency <- Currency.values.toList)
      yield {
        val column = currency.toString.toLowerCase
        val entries = rawExchangeRates.select(F.timestamp, column)
          .filter(col(column).isNotNull)
          .as[(Int, Double)]
          .collect()
        (currency, SortedMap(entries.to: _*))
      }
    }.toMap
    val determineMarketPrice: (Currency, Int) => Double = { (currency, timestamp) =>
      val priceBefore = rawMarketPrices(currency).to(timestamp).lastOption
      val priceAfter = rawMarketPrices(currency).from(timestamp + 1).headOption
      (priceBefore, priceAfter) match {
        case (None, _) => 0
        case (Some(p1), None) => p1._2
        case (Some(p1), Some(p2)) =>
          p1._2 + (timestamp - p1._1) * (p2._2 - p1._2) / (p2._1 - p1._1)
      }
    }
    val eurPrice = udf(determineMarketPrice curried EUR)
    val usdPrice = udf(determineMarketPrice curried USD)
    blocks.select(
        col(F.height),
        eurPrice(col(F.timestamp)) as F.eur,
        usdPrice(col(F.timestamp)) as F.usd)
      .as[ExchangeRates]
  }
  
  def toBitcoinDataFrame(
      exchangeRates: Dataset[ExchangeRates],
      tableWithHeight: Dataset[_],
      columns: List[String]) = {
    val toBitcoin = udf[Bitcoin, Long, Double, Double] { (satoshi, eurPrice, usdPrice) =>
      val convert: (Long, Double) => Double = (satoshi, price) =>
        (satoshi * price / 1000000 + 0.5).toLong / 100.0
      Bitcoin(satoshi, convert(satoshi, eurPrice), convert(satoshi, usdPrice))
    }
    @tailrec
    def processColumns(df: DataFrame, cs: List[String]): DataFrame =
      if (cs.isEmpty) df
      else processColumns(
        df.withColumn(cs(0), toBitcoin(col(cs(0)), col(F.eur), col(F.usd))),
        cs.tail)
    processColumns(tableWithHeight.join(exchangeRates, F.height), columns).drop(F.eur, F.usd)
  }
  
  def addressCluster(
      regularInputs: Dataset[RegularInput],
      regularOutputs: Dataset[RegularOutput]) = {
    def plainAddressCluster(basicTxInputAddresses: DataFrame) = {
      val addrCount = count(F.addrId).over(Window.partitionBy(F.txNumber))
      
      val collectiveInputAddresses =
        basicTxInputAddresses.select(col(F.txNumber), col(F.addrId), addrCount as "count")
          .filter($"count" > 1)
          .select(col(F.txNumber), col(F.addrId))
      
      val transactionCount =
        collectiveInputAddresses.groupBy(F.addrId).count()
      
      val reprAddrId = "reprAddrId"
      
      val initialRepresentative = {
        val transactionWindow = Window.partitionBy(F.txNumber).orderBy($"count".desc)
        val rowNumber = row_number().over(transactionWindow)
        
        val addressMax = {
          val rank = "rank"
          collectiveInputAddresses.join(transactionCount, F.addrId)
            .select(col(F.txNumber), col(F.addrId), rowNumber as rank)
            .filter(col(rank) === 1)
            .select(col(F.txNumber), col(F.addrId) as reprAddrId)
        }
      
        val tmp = transactionCount.filter($"count" === 1)
          .select(F.addrId)
          .join(collectiveInputAddresses, F.addrId)
          
        tmp.toDF(F.addrId, F.txNumber).join(addressMax, F.txNumber)
          .select(F.addrId, reprAddrId)
      }
      
      val basicAddressCluster = {
        val inputAddressesRdd = {
          val nonTrivialAddresses =
            transactionCount.filter($"count" > 1)
              .select(F.addrId)
              .join(collectiveInputAddresses, F.addrId)
              
          for (r <- nonTrivialAddresses)
          yield (r getInt 1, r getInt 0)
        }.rdd
        
        @tailrec
        def doGrouping(am: AddressMapping, iter: Iterator[Set[Int]]): AddressMapping = {
          if (iter.hasNext) {
            val addresses = iter.next()
            doGrouping(am.group(addresses), iter)
          } else am
        }
        
        val inputGroups =
          inputAddressesRdd.aggregateByKey[Set[Int]](Set.empty[Int])(_ + _, _ ++ _)
            .map(_._2).toLocalIterator
       
        val addressMapping = doGrouping(AddressMapping(Map.empty), inputGroups)
        
        spark.sparkContext.parallelize(addressMapping.collect.toSeq).toDF().persist()
      }
      
      val addressClusterRemainder =
        initialRepresentative.join(
            basicAddressCluster.toDF(reprAddrId, F.cluster), List(reprAddrId), "left_outer")
          .select(
            col(F.addrId),
            when(col(F.cluster).isNotNull, col(F.cluster)) otherwise col(reprAddrId) as F.cluster)
      
      basicAddressCluster.union(addressClusterRemainder)
    }
    val orderWindow = Window.partitionBy(F.address).orderBy(F.txNumber, F.n)
    val rowNumber = "rowNumber"
    val normalizedAddresses =
      regularOutputs.withColumn(rowNumber, row_number().over(orderWindow))
        .filter(col(rowNumber) === 1)
        .sort(F.txNumber, F.n)
        .select(F.address)
        .map(_ getString 0).rdd
        .zipWithIndex()
        .map { case ((a, id)) => NormalizedAddress(id.toInt + 1, a) }
        .toDS()
    val inputIds =
      regularInputs.join(normalizedAddresses, F.address)
        .select(F.txNumber, F.addrId)
    plainAddressCluster(inputIds).join(normalizedAddresses, F.addrId)
      .select(addressPrefixColumn, col(F.address), col(F.cluster))
      .as[AddressCluster]
  }
  
  def addressRelations(
      inputs: Dataset[AddressTransactions],
      outputs: Dataset[AddressTransactions],
      regularInputs: Dataset[RegularInput],
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
      toBitcoinDataFrame(exchangeRates, plainAddressRelations, List(F.estimatedValue))
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
        udf(AddressSummary).apply($"totalReceived.satoshi", $"totalSpent.satoshi"))
    fullAddressRelations.groupBy(F.srcAddress, F.dstAddress)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Bitcoin).apply(
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
      toBitcoinDataFrame(exchangeRates, plainClusterRelations, List(F.value)).drop(F.height)
    }
    val props = {
      val addressProps =
        addresses.select(
          col(F.address) as F.cluster,
          udf(ClusterSummary).apply(
            lit(1),
            $"totalReceived.satoshi",
            $"totalSpent.satoshi"))
      cluster.select(
          col(F.cluster),
          udf(ClusterSummary).apply(
            col(F.noAddresses),
            $"totalReceived.satoshi",
            $"totalSpent.satoshi"))
        .union(addressProps)
    }
    val knownCluster =
      clusterTags.select(col(F.cluster), lit(2))
        .dropDuplicates()
        .union(explicitlyKnownAddresses.toDF(F.cluster, F.category))
    fullClusterRelations.groupBy(F.srcCluster, F.dstCluster)
      .agg(
        count(F.txHash) cast IntegerType as F.noTransactions,
        udf(Bitcoin).apply(
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
