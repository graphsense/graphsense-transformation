package at.ac.ait

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.funsuite._

import at.ac.ait.{Fields => F}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Transformation Test")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }
}

class TransformationTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def readJson[A: Encoder: TypeTag](file: String): Dataset[A] = {
    // https://docs.databricks.com/spark/latest/faq/schema-from-case-class.html
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val newSchema = DataType
      .fromJson(
        schema.json
          .replace("\"type\":\"binary\"", "\"type\":\"string\"")
          .replace("\"elementType\":\"binary\"", "\"elementType\":\"string\"")
      )
      .asInstanceOf[StructType]
    spark.read.schema(newSchema).json(file).as[A]
  }

  def setNullableStateForAllColumns[A](ds: Dataset[A]): DataFrame = {
    val df = ds.toDF()
    val schema =
      DataType
        .fromJson(
          df.schema.json.replace("\"nullable\":false", "\"nullable\":true")
        )
        .asInstanceOf[StructType]
    df.sqlContext.createDataFrame(df.rdd, schema)
  }

  def assertDataFrameEquality[A](
      actualDS: Dataset[A],
      expectedDS: Dataset[A]
  ): Unit = {
    val colOrder = expectedDS.columns map col
    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val inputDir = "src/test/resources/"
  val refDir = "src/test/resources/reference/"

  val bucketSize: Int = 2

  // input data
  val blocks = readJson[Block](inputDir + "/test_blocks.json")
  val transactions = readJson[Transaction](inputDir + "test_txs.json")
  val exchangeRatesRaw =
    readJson[ExchangeRatesRaw](inputDir + "test_exchange_rates.json")
  val addressTagsRaw =
    readJson[AddressTagRaw](inputDir + "test_address_tags.json")
  val clusterTagsRaw =
    readJson[ClusterTagRaw](inputDir + "test_cluster_tags.json")

  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .filter(col(F.height) === noBlocks - 1)
    .select(col(F.timestamp))
    .first()
    .getInt(0)
  val noTransactions = transactions.count()

  // transformation pipeline
  val t = new Transformation(spark, bucketSize)

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val regInputs = t.computeRegularInputs(transactions).persist()
  val regOutputs = t.computeRegularOutputs(transactions).persist()

  val addressIds = t.computeAddressIds(regOutputs)
  val addressByIdGroup = t.computeAddressByIdGroups(addressIds)

  val addressTransactions =
    t.computeAddressTransactions(
        transactions,
        regInputs,
        regOutputs,
        addressIds
      )
      .sort(F.addressId, F.height, F.value)
      .persist()

  val (inputs, outputs) = t.splitTransactions(addressTransactions)
  inputs.persist()
  outputs.persist()

  val basicAddresses =
    t.computeBasicAddresses(
        transactions,
        addressTransactions,
        inputs,
        outputs,
        exchangeRates
      )
      .sort(F.addressId)
      .persist()

  val addressTags =
    t.computeAddressTags(addressTagsRaw, basicAddresses, addressIds, "BTC")
      .sort(F.addressId)
      .persist()
  val noAddressTags = addressTags
    .select(col("label"))
    .withColumn("label", lower(col("label")))
    .distinct()
    .count()

  val plainAddressRelations =
    t.computePlainAddressRelations(inputs, outputs, regInputs, transactions)

  val addressRelationsLimit1 =
    t.computeAddressRelations(
        plainAddressRelations,
        exchangeRates,
        addressTags,
        1
      )
      .sort(F.dstAddressId, F.srcAddressId)
      .persist()

  val addressRelations =
    t.computeAddressRelations(
        plainAddressRelations,
        exchangeRates,
        addressTags
      )
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

  val basicClusterAddresses =
    t.computeBasicClusterAddresses(basicAddresses, addressClusterCoinjoin)
      .sort(F.cluster, F.addressId)
      .persist()

  val clusterTransactions =
    t.computeClusterTransactions(
        inputs,
        outputs,
        transactions,
        addressClusterCoinjoin
      )
      .sort(F.cluster, F.height, F.value)
      .persist()

  val (clusterInputs, clusterOutputs) = t.splitTransactions(clusterTransactions)
  clusterInputs.persist()
  clusterOutputs.persist()

  val basicCluster =
    t.computeBasicCluster(
        transactions,
        basicClusterAddresses,
        clusterTransactions,
        clusterInputs,
        clusterOutputs,
        exchangeRates
      )
      .sort(F.cluster)
      .persist()

  val clusterTags =
    t.computeClusterTags(clusterTagsRaw, basicCluster, "BTC")
      .sort(F.cluster)
      .persist()

  val clusterTagsByLabel =
    t.computeClusterTagsByLabel(clusterTagsRaw, clusterTags, "BTC").persist()

  val clusterAddressTags =
    t.computeClusterAddressTags(addressCluster, addressTags)
      .sort(F.cluster)
      .persist()

  val plainClusterRelations =
    t.computePlainClusterRelations(clusterInputs, clusterOutputs).persist()

  val clusterRelationsLimit1 =
    t.computeClusterRelations(
        plainClusterRelations,
        exchangeRates,
        clusterTags,
        1
      )
      .persist()

  val clusterRelations =
    t.computeClusterRelations(
        plainClusterRelations,
        exchangeRates,
        clusterTags
      )
      .persist()

  val cluster =
    t.computeCluster(basicCluster, clusterRelations)
      .sort(F.cluster)
      .persist()
  val noCluster = cluster.count()

  val clusterAddresses =
    t.computeClusterAddresses(addresses, basicClusterAddresses)
      .sort(F.cluster, F.addressId)
      .persist()

  val addressTagsByLabel =
    t.computeAddressTagsByLabel(addressTagsRaw, addressTags, "BTC").persist()

  val summaryStatistics =
    t.summaryStatistics(
      lastBlockTimestamp,
      noBlocks,
      noTransactions,
      noAddresses,
      noAddressRelations,
      noCluster,
      noAddressTags
    )

  note("test address graph")

  test("addressIds") {
    val addressIdsRef = readJson[AddressId](refDir + "address_ids.json")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  test("addressByIdGroup") {
    val addressByIdGroupRef =
      readJson[AddressByIdGroup](refDir + "address_by_id_group.json")
    assertDataFrameEquality(addressByIdGroup, addressByIdGroupRef)
  }

  test("regularInputs") {
    val regInputsRef = readJson[RegularInput](refDir + "regular_inputs.json")
      .sort(F.txHash, F.address)
    val sortedInputs = regInputs.sort(F.txHash, F.address)
    assertDataFrameEquality(sortedInputs, regInputsRef)
  }
  test("regularOutputs") {
    val regOutputsRef = readJson[RegularOutput](refDir + "regular_outputs.json")
      .sort(F.txHash, F.address)
    val sortedOutput = regOutputs.sort(F.txHash, F.address)
    assertDataFrameEquality(sortedOutput, regOutputsRef)
  }
  test("addressTransactions") {
    val addressTransactionsRef =
      readJson[AddressTransaction](refDir + "address_txs.json")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }
  test("inputs") {
    val inputsRef = readJson[AddressTransaction](refDir + "inputs.json")
    assertDataFrameEquality(inputs, inputsRef)
  }
  test("outputs") {
    val outputsRef = readJson[AddressTransaction](refDir + "outputs.json")
    assertDataFrameEquality(outputs, outputsRef)
  }
  test("basicAddresses") {
    val basicAddressesRef =
      readJson[BasicAddress](refDir + "basic_addresses.json")
    assertDataFrameEquality(basicAddresses, basicAddressesRef)
  }
  test("addressTags") {
    val addressTagsRef = readJson[AddressTag](refDir + "address_tags.json")
    assertDataFrameEquality(addressTags, addressTagsRef)
  }
  test("addressTagsByLabel") {
    val addressTagsByLabelRef =
      readJson[AddressTagByLabel](refDir + "address_tags_by_label.json")
    assertDataFrameEquality(addressTagsByLabel, addressTagsByLabelRef)
  }
  test("addressRelations") {
    val addressRelationsRef =
      readJson[AddressRelation](refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }
  test("addressRelations with txLimit=1") {
    val addressRelationsLimit1Ref =
      readJson[AddressRelation](refDir + "address_relations_limit1.json")
    assertDataFrameEquality(addressRelationsLimit1, addressRelationsLimit1Ref)
  }
  test("addresses") {
    val addressesRef = readJson[Address](refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  note("test cluster graph")

  test("addressCluster without coinjoin inputs") {
    val addressClusterRef =
      readJson[AddressCluster](refDir + "address_cluster.json")
    assertDataFrameEquality(addressCluster, addressClusterRef)
  }
  test("addressCluster all inputs") {
    val addressClusterRef =
      readJson[AddressCluster](refDir + "address_cluster_with_coinjoin.json")
    assertDataFrameEquality(addressClusterCoinjoin, addressClusterRef)
  }
  test("basicClusterAddresses") {
    val basicClusterAddressesRef =
      readJson[BasicClusterAddress](refDir + "basic_cluster_addresses.json")
    assertDataFrameEquality(basicClusterAddresses, basicClusterAddressesRef)
  }
  test("clusterTransactions") {
    val clusterTransactionsRef =
      readJson[ClusterTransaction](refDir + "cluster_txs.json")
    assertDataFrameEquality(clusterTransactions, clusterTransactionsRef)
  }
  test("clusterInputs") {
    val clusterInputsRef =
      readJson[ClusterTransaction](refDir + "cluster_inputs.json")
    assertDataFrameEquality(clusterInputs, clusterInputsRef)
  }
  test("clusterOutputs") {
    val clusterOutputsRef =
      readJson[ClusterTransaction](refDir + "cluster_outputs.json")
    assertDataFrameEquality(clusterOutputs, clusterOutputsRef)
  }
  test("basicCluster") {
    val basicClusterRef = readJson[BasicCluster](refDir + "basic_cluster.json")
    assertDataFrameEquality(basicCluster, basicClusterRef)
  }
  test("clusterTags") {
    val clusterTagsRef = readJson[ClusterTag](refDir + "cluster_tags.json")
    assertDataFrameEquality(clusterTags, clusterTagsRef)
  }
  test("clusterTagsByLabel") {
    val clusterTagsByLabelRef =
      readJson[ClusterTagByLabel](refDir + "cluster_tags_by_label.json")
    assertDataFrameEquality(clusterTagsByLabel, clusterTagsByLabelRef)
  }
  test("clusterAddressTags") {
    val clusterAddressTagsRef =
      readJson[ClusterAddressTag](refDir + "cluster_address_tags.json")
    assertDataFrameEquality(clusterAddressTags, clusterAddressTagsRef)
  }
  test("plainClusterRelations") {
    val plainClusterRelationsRef =
      readJson[PlainClusterRelation](refDir + "plain_cluster_relations.json")
        .sort(F.txHash)
    val sortedRels = plainClusterRelations.sort(F.txHash)
    assertDataFrameEquality(sortedRels, plainClusterRelationsRef)
  }
  test("clusterRelations") {
    val clusterRelationsRef =
      readJson[ClusterRelation](refDir + "cluster_relations.json")
        .sort(F.srcCluster, F.dstCluster)
    val sortedRelations = clusterRelations.sort(F.srcCluster, F.dstCluster)
    assertDataFrameEquality(sortedRelations, clusterRelationsRef)
  }
  test("clusterRelations with txLimit=1") {
    val clusterRelationsLimit1Ref =
      readJson[ClusterRelation](refDir + "cluster_relations_limit1.json")
        .sort(F.srcCluster, F.dstCluster)
    val sortedRelations =
      clusterRelationsLimit1.sort(F.srcCluster, F.dstCluster)
    assertDataFrameEquality(sortedRelations, clusterRelationsLimit1Ref)
  }
  test("clusters") {
    val clusterRef = readJson[Cluster](refDir + "cluster.json")
    assertDataFrameEquality(cluster, clusterRef)
  }
  test("clusterAdresses") {
    val clusterAddressesRef =
      readJson[ClusterAddress](refDir + "cluster_addresses.json")
    assertDataFrameEquality(clusterAddresses, clusterAddressesRef)
  }

  note("summary statistics for address and cluster graph")

  test("summary statistics") {
    val summaryStatisticsRef =
      readJson[SummaryStatistics](refDir + "summary_statistics.json")
    assertDataFrameEquality(summaryStatistics, summaryStatisticsRef)
  }
}
