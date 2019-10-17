package at.ac.ait

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest._

import at.ac.ait.{Fields => F}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Transformation Test")
      .getOrCreate()
  }
}

class TransformationTest
    extends FunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def readJson[A: Encoder: TypeTag](file: String): Dataset[A] = {
    // https://docs.databricks.com/spark/latest/faq/schema-from-case-class.html
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val newSchema = DataType
      .fromJson(
        schema.json.replace("\"type\":\"binary\"", "\"type\":\"string\"")
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

  // input data
  val blocks = readJson[Block](inputDir + "/test_blocks.json")
  val transactions = readJson[Transaction](inputDir + "test_txs.json")
  val exchangeRates =
    readJson[ExchangeRates](inputDir + "test_exchange_rates.json")
  val attributionTags = readJson[Tag](inputDir + "test_tags.json")

  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .filter(col(F.height) === noBlocks - 1)
    .select(col(F.timestamp))
    .first()
    .getInt(0)
  val noTransactions = transactions.count()

  // transformation pipeline
  val t = new Transformation(spark)

  val regInputs = t.computeRegularInputs(transactions).persist()
  val regOutputs = t.computeRegularOutputs(transactions).persist()

  val addressIds = t.computeAddressIds(regOutputs)

  val addressTransactions =
    t.computeAddressTransactions(transactions, regInputs, regOutputs).persist()

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
      .persist()

  val addressRelations =
    t.computeAddressRelations(
        inputs,
        outputs,
        regInputs,
        transactions,
        basicAddresses,
        exchangeRates
      )
      .persist()
  val noAddressRelations = addressRelations.count()

  val addresses = t.computeAddresses(basicAddresses, addressRelations).persist()
  val noAddresses = addresses.count()

  val addressTags =
    t.computeAddressTags(basicAddresses, attributionTags, "BTC").persist()
  val noAddressTags = addressTags.count()

  val addressCluster =
    t.computeAddressCluster(regInputs, addressIds, true).persist()

  val addressClusterCoinjoin =
    t.computeAddressCluster(regInputs, addressIds, false).persist()

  val basicClusterAddresses =
    t.computeBasicClusterAddresses(basicAddresses, addressClusterCoinjoin)
      .persist()

  val clusterTransactions =
    t.computeClusterTransactions(
        inputs,
        outputs,
        transactions,
        addressClusterCoinjoin
      )
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
      .persist()

  val plainClusterRelations =
    t.computePlainClusterRelations(clusterInputs, clusterOutputs).persist()

  val clusterRelations =
    t.computeClusterRelations(
        plainClusterRelations,
        basicCluster,
        basicAddresses,
        exchangeRates
      )
      .persist()

  val clusterTags =
    t.computeClusterTags(addressClusterCoinjoin, addressTags).persist()

  val cluster = t.computeCluster(basicCluster, clusterRelations, clusterTags).persist()
  val noCluster = cluster.count()

  val clusterAddresses =
    t.computeClusterAddresses(addresses, basicClusterAddresses).persist()

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

  test("regularInputs") {
    val regInputsRef = readJson[RegularInput](refDir + "regular_inputs.json")
    assertDataFrameEquality(regInputs, regInputsRef)
  }
  test("regularOutputs") {
    val regOutputsRef = readJson[RegularOutput](refDir + "regular_outputs.json")
    assertDataFrameEquality(regOutputs, regOutputsRef)
  }
  test("addressTransactions") {
    val addressTransactionsRef =
      readJson[AddressTransactions](refDir + "address_txs.json")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }
  test("inputs") {
    val inputsRef = readJson[AddressTransactions](refDir + "inputs.json")
    assertDataFrameEquality(inputs, inputsRef)
  }
  test("outputs") {
    val outputsRef = readJson[AddressTransactions](refDir + "outputs.json")
    assertDataFrameEquality(outputs, outputsRef)
  }
  test("basicAddresses") {
    val basicAddressesRef =
      readJson[BasicAddress](refDir + "basic_addresses.json")
    assertDataFrameEquality(basicAddresses, basicAddressesRef)
  }
  test("addressRelations") {
    val addressRelationsRef =
      readJson[AddressRelations](refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }
  test("addresses") {
    val addressesRef = readJson[Address](refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }
  test("addressTag") {
    val addressTagsRef = readJson[AddressTags](refDir + "address_tags.json")
    assertDataFrameEquality(addressTags, addressTagsRef)
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
      readJson[BasicClusterAddresses](refDir + "basic_cluster_addresses.json")
    assertDataFrameEquality(basicClusterAddresses, basicClusterAddressesRef)
  }
  test("clusterTransactions") {
    val clusterTransactionsRef =
      readJson[ClusterTransactions](refDir + "cluster_txs.json")
    assertDataFrameEquality(clusterTransactions, clusterTransactionsRef)
  }
  test("clusterInputs") {
    val clusterInputsRef =
      readJson[ClusterTransactions](refDir + "cluster_inputs.json")
    assertDataFrameEquality(clusterInputs, clusterInputsRef)
  }
  test("clusterOutputs") {
    val clusterOutputsRef =
      readJson[ClusterTransactions](refDir + "cluster_outputs.json")
    assertDataFrameEquality(clusterOutputs, clusterOutputsRef)
  }
  test("basicCluster") {
    val basicClusterRef = readJson[BasicCluster](refDir + "basic_cluster.json")
    assertDataFrameEquality(basicCluster, basicClusterRef)
  }
  test("plainClusterRelations") {
    val plainClusterRelationsRef =
      readJson[PlainClusterRelations](refDir + "plain_cluster_relations.json")
    assertDataFrameEquality(plainClusterRelations, plainClusterRelationsRef)
  }
  test("clusterRelations") {
    val clusterRelationsRef =
      readJson[ClusterRelations](refDir + "cluster_relations.json")
    assertDataFrameEquality(clusterRelations, clusterRelationsRef)
  }
  test("clusters") {
    val clusterRef = readJson[Cluster](refDir + "cluster.json")
    assertDataFrameEquality(cluster, clusterRef)
  }
  test("clusterAdresses") {
    val clusterAddressesRef =
      readJson[ClusterAddresses](refDir + "cluster_addresses.json")
    assertDataFrameEquality(clusterAddresses, clusterAddressesRef)
  }
  test("clusterTags") {
    val clusterTagsRef = readJson[ClusterTags](refDir + "cluster_tags.json")
    assertDataFrameEquality(clusterTags, clusterTagsRef)
  }

  note("summary statistics for address and cluster graph")

  test("summary statistics") {
    val summaryStatisticsRef =
      readJson[SummaryStatistics](refDir + "summary_statistics.json")
    assertDataFrameEquality(summaryStatistics, summaryStatisticsRef)
  }
}
