package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite._

import Helpers.{readTestData, setNullableStateForAllColumns}
import info.graphsense.{Fields => F}

class TransformationTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def assertDataFrameEquality[A](
      actualDS: Dataset[A],
      expectedDS: Dataset[A]
  ): Unit = {
    val colOrder = expectedDS.columns map col
    assert(actualDS.columns.sorted sameElements expectedDS.columns.sorted)
    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.setCheckpointDir("file:///tmp/spark-checkpoint")

  import spark.implicits._

  val inputDir = "src/test/resources/"
  val refDir = "src/test/resources/reference/"

  val bucketSize: Int = 2
  val addressPrefixLength: Int = 5

  // input data
  val blocks = readTestData[Block](spark, inputDir + "/test_blocks.json")
  val transactions =
    readTestData[Transaction](spark, inputDir + "test_txs.json")
  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](spark, inputDir + "test_exchange_rates.json")

  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .filter(col(F.blockId) === noBlocks - 1)
    .select(col(F.timestamp))
    .first()
    .getInt(0)
  val noTransactions = transactions.count()

  // transformation pipeline
  val t = new Transformation(spark, bucketSize, addressPrefixLength)

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val regInputs = t.computeRegularInputs(transactions).persist()
  val regOutputs = t.computeRegularOutputs(transactions).persist()

  val addressIds = t.computeAddressIds(regOutputs)
  val addressByAddressPrefix = t.computeAddressByAddressPrefix(addressIds)

  val addressTransactions =
    t.computeAddressTransactions(
        regInputs,
        regOutputs,
        addressIds
      )
      .sort(F.addressId, F.blockId, F.value)
      .persist()

  val (inputs, outputs) = t.splitTransactions(addressTransactions)
  inputs.persist()
  outputs.persist()

  val basicAddresses =
    t.computeBasicAddresses(
        addressTransactions,
        inputs,
        outputs,
        exchangeRates
      )
      .sort(F.addressId)
      .persist()

  val plainAddressRelations =
    t.computePlainAddressRelations(inputs, outputs, regInputs, transactions)

  val addressRelations =
    t.computeAddressRelations(plainAddressRelations, exchangeRates)
      .sort(F.dstAddressId, F.srcAddressId)
      .persist()
  val noAddressRelations = addressRelations.count()

  val addressCluster =
    t.computeAddressCluster(regInputs, addressIds, true)
      .sort(F.addressId)
      .persist()

  val addresses =
    t.computeAddresses(
        basicAddresses,
        addressCluster,
        addressRelations,
        addressIds
      )
      .sort(F.addressId)
      .persist()
  val noAddresses = addresses.count()

  val addressClusterCoinjoin =
    t.computeAddressCluster(regInputs, addressIds, false)
      .sort(F.addressId)
      .persist()

  val clusterAddresses =
    t.computeClusterAddresses(addressClusterCoinjoin)
      .sort(F.clusterId, F.addressId)
      .persist()

  val clusterTransactions =
    t.computeClusterTransactions(
        inputs,
        outputs,
        transactions,
        addressClusterCoinjoin
      )
      .sort(F.clusterId, F.blockId, F.value)
      .persist()

  val (clusterInputs, clusterOutputs) = t.splitTransactions(clusterTransactions)
  clusterInputs.persist()
  clusterOutputs.persist()

  val basicCluster =
    t.computeBasicCluster(
        clusterAddresses,
        clusterTransactions,
        clusterInputs,
        clusterOutputs,
        exchangeRates
      )
      .sort(F.clusterId)
      .persist()

  val plainClusterRelations =
    t.computePlainClusterRelations(clusterInputs, clusterOutputs).persist()

  val clusterRelations =
    t.computeClusterRelations(plainClusterRelations, exchangeRates).persist()
  val noClusterRelations = clusterRelations.count()

  val cluster =
    t.computeCluster(basicCluster, clusterRelations)
      .sort(F.clusterId)
      .persist()
  val noCluster = cluster.count()

  val summaryStatistics =
    t.summaryStatistics(
      lastBlockTimestamp,
      noBlocks,
      noTransactions,
      noAddresses,
      noAddressRelations,
      noCluster,
      noClusterRelations
    )

  note("test address graph")

  test("addressIds") {
    val addressIdsRef =
      readTestData[AddressId](spark, refDir + "address_ids.json")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  test("addressByAddressPrefix") {
    val addressByAddressPrefixRef =
      readTestData[AddressByAddressPrefix](
        spark,
        refDir + "address_by_address_prefix.json"
      )
    assertDataFrameEquality(addressByAddressPrefix, addressByAddressPrefixRef)
  }

  test("regularInputs") {
    val regInputsRef =
      readTestData[RegularInput](spark, refDir + "regular_inputs.json")
        .sort(F.txId, F.address)
    val sortedInputs = regInputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedInputs, regInputsRef)
  }
  test("regularOutputs") {
    val regOutputsRef =
      readTestData[RegularOutput](spark, refDir + "regular_outputs.json")
        .sort(F.txId, F.address)
    val sortedOutput = regOutputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedOutput, regOutputsRef)
  }
  test("addressTransactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](spark, refDir + "address_txs.json")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }
  test("inputs") {
    val inputsRef =
      readTestData[AddressTransaction](spark, refDir + "inputs.json")
    assertDataFrameEquality(inputs, inputsRef)
  }
  test("outputs") {
    val outputsRef =
      readTestData[AddressTransaction](spark, refDir + "outputs.json")
    assertDataFrameEquality(outputs, outputsRef)
  }
  test("basicAddresses") {
    val basicAddressesRef =
      readTestData[BasicAddress](spark, refDir + "basic_addresses.json")
    assertDataFrameEquality(basicAddresses, basicAddressesRef)
  }
  test("addressRelations") {
    val addressRelationsRef =
      readTestData[AddressRelation](spark, refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }
  test("addresses") {
    val addressesRef = readTestData[Address](spark, refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  note("test cluster graph")

  test("addressCluster without coinjoin inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](spark, refDir + "address_cluster.json")
    assertDataFrameEquality(addressCluster, addressClusterRef)
  }
  test("addressCluster all inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](
        spark,
        refDir + "address_cluster_with_coinjoin.json"
      )
    assertDataFrameEquality(addressClusterCoinjoin, addressClusterRef)
  }
  test("clusterTransactions") {
    val clusterTransactionsRef =
      readTestData[ClusterTransaction](spark, refDir + "cluster_txs.json")
    assertDataFrameEquality(clusterTransactions, clusterTransactionsRef)
  }
  test("clusterInputs") {
    val clusterInputsRef =
      readTestData[ClusterTransaction](spark, refDir + "cluster_inputs.json")
    assertDataFrameEquality(clusterInputs, clusterInputsRef)
  }
  test("clusterOutputs") {
    val clusterOutputsRef =
      readTestData[ClusterTransaction](spark, refDir + "cluster_outputs.json")
    assertDataFrameEquality(clusterOutputs, clusterOutputsRef)
  }
  test("basicCluster") {
    val basicClusterRef =
      readTestData[BasicCluster](spark, refDir + "basic_cluster.json")
    assertDataFrameEquality(basicCluster, basicClusterRef)
  }
  test("plainClusterRelations") {
    val plainClusterRelationsRef =
      readTestData[PlainClusterRelation](
        spark,
        refDir + "plain_cluster_relations.json"
      ).sort(F.txId)
    val sortedRels = plainClusterRelations.sort(F.txId)
    assertDataFrameEquality(sortedRels, plainClusterRelationsRef)
  }
  test("clusterRelations") {
    val clusterRelationsRef =
      readTestData[ClusterRelation](spark, refDir + "cluster_relations.json")
        .sort(F.srcClusterId, F.dstClusterId)
    val sortedRelations = clusterRelations.sort(F.srcClusterId, F.dstClusterId)
    assertDataFrameEquality(sortedRelations, clusterRelationsRef)
  }
  test("clusters") {
    val clusterRef = readTestData[Cluster](spark, refDir + "cluster.json")
    assertDataFrameEquality(cluster, clusterRef)
  }
  test("clusterAdresses") {
    val clusterAddressesRef =
      readTestData[ClusterAddress](spark, refDir + "cluster_addresses.json")
    assertDataFrameEquality(clusterAddresses, clusterAddressesRef)
  }

  note("summary statistics for address and cluster graph")

  test("summary statistics") {
    val summaryStatisticsRef =
      readTestData[SummaryStatistics](spark, refDir + "summary_statistics.json")
    assertDataFrameEquality(summaryStatistics, summaryStatisticsRef)
  }
}
