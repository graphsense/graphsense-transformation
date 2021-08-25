package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  Encoders,
  SparkSession
}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.{col, length, lit, lower, udf, when}
import org.apache.spark.sql.types.{
  ArrayType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite._

import info.graphsense.{Fields => F}

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

  def readTestData[T <: Product: Encoder: TypeTag](
      file: String
  ): Dataset[T] = {
    val schema = Encoders.product[T].schema
    // read BinaryType as StringType from JSON/CSV file and cast to ByteArray
    val newSchema = StructType(
      schema.map(
        x =>
          if (x.dataType.toString == "BinaryType")
            StructField(x.name, StringType, true)
          else StructField(x.name, x.dataType, true)
      )
    )

    val binaryColumns = schema.collect {
      case x if x.dataType.toString == "BinaryType" => x.name
    }

    val hexStringToByteArray = udf(
      (x: String) => x.grouped(2).toArray map { Integer.parseInt(_, 16).toByte }
    )

    val fileSuffix = file.toUpperCase.split("\\.").last
    val df =
      if (fileSuffix == "JSON") spark.read.schema(newSchema).json(file)
      else spark.read.schema(newSchema).option("header", true).csv(file)

    binaryColumns
      .foldLeft(df) { (curDF, colName) =>
        curDF.withColumn(
          colName,
          when(
            col(colName).isNotNull,
            hexStringToByteArray(
              col(colName).substr(lit(3), length(col(colName)) - 2)
            )
          )
        )
      }
      .as[T]
  }

  def setNullableStateForAllColumns[T](
      ds: Dataset[T],
      nullable: Boolean = true,
      containsNull: Boolean = true
  ): DataFrame = {
    def set(st: StructType): StructType = {
      StructType(st.map {
        case StructField(name, dataType, _, metadata) =>
          val newDataType = dataType match {
            case t: StructType          => set(t)
            case ArrayType(dataType, _) => ArrayType(dataType, containsNull)
            case _                      => dataType
          }
          StructField(name, newDataType, nullable = nullable, metadata)
      })
    }
    ds.sqlContext.createDataFrame(ds.toDF.rdd, set(ds.schema))
  }

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
  import spark.implicits._

  val inputDir = "src/test/resources/"
  val refDir = "src/test/resources/reference/"

  val bucketSize: Int = 2
  val addressPrefixLength: Int = 5

  // input data
  val blocks = readTestData[Block](inputDir + "/test_blocks.json")
  val transactions = readTestData[Transaction](inputDir + "test_txs.json")
  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](inputDir + "test_exchange_rates.json")
  val addressTagsRaw =
    readTestData[AddressTagRaw](inputDir + "test_address_tags.json")
  val clusterTagsRaw =
    readTestData[ClusterTagRaw](inputDir + "test_cluster_tags.json")

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

  val clusterTags =
    t.computeClusterTags(clusterTagsRaw, basicCluster, "BTC")
      .sort(F.clusterId)
      .persist()

  val clusterTagsByLabel =
    t.computeClusterTagsByLabel(clusterTagsRaw, clusterTags, "BTC").persist()

  val clusterAddressTags =
    t.computeClusterAddressTags(addressCluster, addressTags)
      .sort(F.clusterId)
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
      .sort(F.clusterId)
      .persist()
  val noCluster = cluster.count()

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
    val addressIdsRef = readTestData[AddressId](refDir + "address_ids.json")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  test("addressByAddressPrefix") {
    val addressByAddressPrefixRef =
      readTestData[AddressByAddressPrefix](
        refDir + "address_by_address_prefix.json"
      )
    assertDataFrameEquality(addressByAddressPrefix, addressByAddressPrefixRef)
  }

  test("regularInputs") {
    val regInputsRef =
      readTestData[RegularInput](refDir + "regular_inputs.json")
        .sort(F.txId, F.address)
    val sortedInputs = regInputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedInputs, regInputsRef)
  }
  test("regularOutputs") {
    val regOutputsRef =
      readTestData[RegularOutput](refDir + "regular_outputs.json")
        .sort(F.txId, F.address)
    val sortedOutput = regOutputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedOutput, regOutputsRef)
  }
  test("addressTransactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](refDir + "address_txs.json")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }
  test("inputs") {
    val inputsRef = readTestData[AddressTransaction](refDir + "inputs.json")
    assertDataFrameEquality(inputs, inputsRef)
  }
  test("outputs") {
    val outputsRef = readTestData[AddressTransaction](refDir + "outputs.json")
    assertDataFrameEquality(outputs, outputsRef)
  }
  test("basicAddresses") {
    val basicAddressesRef =
      readTestData[BasicAddress](refDir + "basic_addresses.json")
    assertDataFrameEquality(basicAddresses, basicAddressesRef)
  }
  test("addressTags") {
    val addressTagsRef = readTestData[AddressTag](refDir + "address_tags.json")
    assertDataFrameEquality(addressTags, addressTagsRef)
  }
  test("addressTagsByLabel") {
    val addressTagsByLabelRef =
      readTestData[AddressTagByLabel](refDir + "address_tags_by_label.json")
    assertDataFrameEquality(addressTagsByLabel, addressTagsByLabelRef)
  }
  test("addressRelations") {
    val addressRelationsRef =
      readTestData[AddressRelation](refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }
  test("addressRelations with txLimit=1") {
    val addressRelationsLimit1Ref =
      readTestData[AddressRelation](refDir + "address_relations_limit1.json")
    assertDataFrameEquality(addressRelationsLimit1, addressRelationsLimit1Ref)
  }
  test("addresses") {
    val addressesRef = readTestData[Address](refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  note("test cluster graph")

  test("addressCluster without coinjoin inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](refDir + "address_cluster.json")
    assertDataFrameEquality(addressCluster, addressClusterRef)
  }
  test("addressCluster all inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](
        refDir + "address_cluster_with_coinjoin.json"
      )
    assertDataFrameEquality(addressClusterCoinjoin, addressClusterRef)
  }
  test("clusterTransactions") {
    val clusterTransactionsRef =
      readTestData[ClusterTransaction](refDir + "cluster_txs.json")
    assertDataFrameEquality(clusterTransactions, clusterTransactionsRef)
  }
  test("clusterInputs") {
    val clusterInputsRef =
      readTestData[ClusterTransaction](refDir + "cluster_inputs.json")
    assertDataFrameEquality(clusterInputs, clusterInputsRef)
  }
  test("clusterOutputs") {
    val clusterOutputsRef =
      readTestData[ClusterTransaction](refDir + "cluster_outputs.json")
    assertDataFrameEquality(clusterOutputs, clusterOutputsRef)
  }
  test("basicCluster") {
    val basicClusterRef =
      readTestData[BasicCluster](refDir + "basic_cluster.json")
    assertDataFrameEquality(basicCluster, basicClusterRef)
  }
  test("clusterTags") {
    val clusterTagsRef = readTestData[ClusterTag](refDir + "cluster_tags.json")
    assertDataFrameEquality(clusterTags, clusterTagsRef)
  }
  test("clusterTagsByLabel") {
    val clusterTagsByLabelRef =
      readTestData[ClusterTagByLabel](refDir + "cluster_tags_by_label.json")
    assertDataFrameEquality(clusterTagsByLabel, clusterTagsByLabelRef)
  }
  test("clusterAddressTags") {
    val clusterAddressTagsRef =
      readTestData[ClusterAddressTag](refDir + "cluster_address_tags.json")
    assertDataFrameEquality(clusterAddressTags, clusterAddressTagsRef)
  }
  test("plainClusterRelations") {
    val plainClusterRelationsRef =
      readTestData[PlainClusterRelation](
        refDir + "plain_cluster_relations.json"
      ).sort(F.txId)
    val sortedRels = plainClusterRelations.sort(F.txId)
    assertDataFrameEquality(sortedRels, plainClusterRelationsRef)
  }
  test("clusterRelations") {
    val clusterRelationsRef =
      readTestData[ClusterRelation](refDir + "cluster_relations.json")
        .sort(F.srcClusterId, F.dstClusterId)
    val sortedRelations = clusterRelations.sort(F.srcClusterId, F.dstClusterId)
    assertDataFrameEquality(sortedRelations, clusterRelationsRef)
  }
  test("clusterRelations with txLimit=1") {
    val clusterRelationsLimit1Ref =
      readTestData[ClusterRelation](refDir + "cluster_relations_limit1.json")
        .sort(F.srcClusterId, F.dstClusterId)
    val sortedRelations =
      clusterRelationsLimit1.sort(F.srcClusterId, F.dstClusterId)
    assertDataFrameEquality(sortedRelations, clusterRelationsLimit1Ref)
  }
  test("clusters") {
    val clusterRef = readTestData[Cluster](refDir + "cluster.json")
    assertDataFrameEquality(cluster, clusterRef)
  }
  test("clusterAdresses") {
    val clusterAddressesRef =
      readTestData[ClusterAddress](refDir + "cluster_addresses.json")
    assertDataFrameEquality(clusterAddresses, clusterAddressesRef)
  }

  note("summary statistics for address and cluster graph")

  test("summary statistics") {
    val summaryStatisticsRef =
      readTestData[SummaryStatistics](refDir + "summary_statistics.json")
    assertDataFrameEquality(summaryStatistics, summaryStatisticsRef)
  }
}
