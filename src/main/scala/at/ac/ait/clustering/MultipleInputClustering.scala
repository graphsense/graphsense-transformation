package at.ac.ait.clustering

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  col, count, explode, lit, max, min, monotonically_increasing_id,
  row_number, size, substring, sum, udf, when}
import org.apache.spark.sql.types.IntegerType
import scala.annotation.tailrec

import at.ac.ait._

class MultipleInputClustering(spark: SparkSession) {
  import spark.implicits._

  private def clusterAddresses(basicTxInputAddresses: Dataset[InputRelation]) = {

    val addrCount = count("addrId").over(Window.partitionBy("txId"))

    val collectiveInputAddresses =
      basicTxInputAddresses.select($"txId", $"addrId", addrCount as "count")
        .filter($"count" > 1)
        .select($"txId", $"addrId")
        .persist()

    val transactionCount =
      collectiveInputAddresses.groupBy("addrId").count()

    val reprAddrId = "reprAddrId"

    val initialRepresentative = {
      val transactionWindow = Window.partitionBy("txId").orderBy($"count".desc)
      val rowNumber = row_number().over(transactionWindow)

      val addressMax =
        collectiveInputAddresses.join(transactionCount, "addrId")
          .select($"txId", $"addrId", rowNumber as "rank")
          .filter($"rank" === 1)
          .select($"txId", $"addrId" as reprAddrId)

      val tmp = transactionCount.filter($"count" === 1)
        .select($"addrId")
        .join(collectiveInputAddresses, "addrId")

      tmp.toDF("addrId", "txId").join(addressMax, "txId")
        .select("addrId", reprAddrId)
    }

    val basicAddressCluster = {
      val inputAddressesRdd = {
        val nonTrivialAddresses =
          transactionCount.filter($"count" > 1)
            .select($"addrId")
            .join(collectiveInputAddresses, "addrId")

        for (r <- nonTrivialAddresses)
        yield (r getLong 1, r getLong 0)
      }.rdd

      println(s"Sending ${inputAddressesRdd.count} rows to driver for clustering...")

      @tailrec
      def doGrouping(am: AddressMapping, iter: Iterator[Set[Long]]): AddressMapping = {
        if (iter.hasNext) {
          val addresses = iter.next()
          doGrouping(am.group(addresses), iter)
        } else am
      }

      val inputGroups =
        inputAddressesRdd.aggregateByKey[Set[Long]](Set.empty[Long])(_ + _, _ ++ _)
          .map(_._2).toLocalIterator

      val addressMapping = doGrouping(AddressMapping(Map.empty), inputGroups)

      spark.sparkContext.parallelize(addressMapping.collect.toSeq).toDF().persist()
    }

    val addressClusterRemainder =
      initialRepresentative.join(
          basicAddressCluster.toDF(reprAddrId, "entityId"), List(reprAddrId), "left_outer")
        .select(
          $"addrId",
          when($"entityId".isNotNull, $"entityId") otherwise $"reprAddrId" as "entityId")

    basicAddressCluster.union(addressClusterRemainder)
  }

  def computeAddressCluster(txInputs: Dataset[TransactionInputs]): Dataset[AddressCluster] = {
    val txHashAddress = txInputs.filter(size($"inputs") gt 0)
      .select($"txHash", explode($"inputs") as "input")
      .filter($"input.address".isNotNull)
      .select($"txHash", $"input.address" as "address")
    val indexedTxHashes = txHashAddress.select($"txHash").dropDuplicates()
      .withColumn("txId", monotonically_increasing_id())
    val indexedAddresses = txHashAddress.select($"address").dropDuplicates()
      .withColumn("addrId", monotonically_increasing_id())
      .persist()
    val indexedTxHashAddress = txHashAddress
      .join(indexedTxHashes, "txHash")
      .join(indexedAddresses, "address")
    val inputRelations = indexedTxHashAddress.select($"txId", $"addrId").as[InputRelation]
    val clusters = clusterAddresses(inputRelations)

    indexedAddresses.join(clusters, "addrId")
      .select(
        substring($"address", 0, 5) as "addressPrefix",
        $"address",
        $"reprAddrId" as "cluster")
      .as[AddressCluster]
  }

  def computeClusterAddresses(
      addressCluster: Dataset[AddressCluster],
      address: Dataset[Address]): Dataset[ClusterAddresses] =
    addressCluster.select($"cluster", $"address")
      .join(address, "address")
      .as[ClusterAddresses]

  def computeCluster(clusterAddress: Dataset[ClusterAddresses]): Dataset[Cluster] =
    clusterAddress.groupBy("cluster")
      .agg(
        count("address").cast(IntegerType) as "noAddresses",
        sum("noIncomingTxs").cast(IntegerType) as "noIncomingTxs",
        sum("noOutgoingTxs").cast(IntegerType) as "noOutgoingTxs",
        min("firstTx") as "firstTx",
        max("lastTx") as "lastTx",
        udf(Value).apply(
          sum("totalReceived.satoshi"),
          sum("totalReceived.eur"),
          sum("totalReceived.usd")) as "totalReceived",
        udf(Value).apply(
          sum("totalSpent.satoshi"),
          sum("totalSpent.eur"),
          sum("totalSpent.usd")) as "totalSpent")
      .as[Cluster]

  def computeClusterTags(
      addressCluster: Dataset[AddressCluster],
      rawTag: Dataset[RawTag]): Dataset[ClusterTags] =
    addressCluster.select($"cluster", $"address")
      .join(rawTag, "address")
      .as[ClusterTags]

  private def updateAddressRelations(
      addrRelations: DataFrame,
      addressCluster: Dataset[AddressCluster],
      clusterTags: Dataset[ClusterTags],
      category: String,
      address: String) =
    addrRelations.filter(col(category) === 0)
      .join(addressCluster.select($"address" as address, $"cluster"), address)
      .drop(category)
      .join(
        clusterTags.select($"cluster")
          .dropDuplicates()
          .withColumn(category, lit(1).cast(IntegerType)),
        "cluster")

  def updateAddressIncomingRelations(
      addrRelations: Dataset[AddressIncomingRelations],
      addressCluster: Dataset[AddressCluster],
      clusterTags: Dataset[ClusterTags]): Dataset[AddressIncomingRelations] =
    updateAddressRelations(
      addrRelations.toDF(),
      addressCluster,
      clusterTags,
      "srcCategory",
      "srcAddress").as[AddressIncomingRelations]

  def updateAddressOutgoingRelations(
      addrRelations: Dataset[AddressOutgoingRelations],
      addressCluster: Dataset[AddressCluster],
      clusterTags: Dataset[ClusterTags]): Dataset[AddressOutgoingRelations] =
    updateAddressRelations(
      addrRelations.toDF(),
      addressCluster,
      clusterTags,
      "dstCategory",
      "dstAddress").as[AddressOutgoingRelations]

  def computeClusterRelations(
      relations: DataFrame,
      cluster: Dataset[Cluster],
      clusterTags: Dataset[ClusterTags],
      addressCluster: Dataset[AddressCluster]): Dataset[ClusterRelations] = {
    val srcAddress = "srcAddress"
    val dstAddress = "dstAddress"
    val srcCluster = "srcCluster"
    val dstCluster = "dstCluster"
    val clusterCol = "cluster"

    val nodePropertiesDF =
      cluster.select(
        col(clusterCol),
        udf(AddressSummary).apply($"totalReceived.satoshi", $"totalSpent.satoshi") as "properties")
      .join(
        clusterTags.select($"cluster", lit(2).cast(IntegerType) as "category").dropDuplicates(),
        List(clusterCol), "left_outer")
      .na.fill(0, Seq("category"))
    val inCluster = addressCluster.select($"address" as srcAddress, col(clusterCol) as srcCluster)
    val outCluster = addressCluster.select($"address" as dstAddress, col(clusterCol) as dstCluster)
    relations.join(inCluster, srcAddress)
      .join(outCluster, dstAddress)
      .groupBy(srcCluster, dstCluster)
      .agg(
        count($"txHash").cast(IntegerType) as "noTransactions",
        udf(Value).apply(
          sum($"estimatedValue.satoshi"),
          sum($"estimatedValue.eur"),
          sum($"estimatedValue.usd")) as "estimatedValue")
      .join(
        nodePropertiesDF.select(
          col(clusterCol) as srcCluster,
          $"properties" as "srcProperties",
          $"category" as "srcCategory"),
        srcCluster)
      .join(
        nodePropertiesDF.select(
          col(clusterCol) as dstCluster,
          $"properties" as "dstProperties",
          $"category" as "dstCategory"),
        dstCluster).as[ClusterRelations]
  }

}
