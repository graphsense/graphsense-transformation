package info.graphsense

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  collect_set,
  count,
  explode,
  floor,
  lit,
  round,
  row_number,
  substring,
  sum,
  struct,
  when
}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}

import info.graphsense.{Fields => F}
import info.graphsense.clustering._

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
    ds.withColumn(idGroupColum, floor(col(idColumn) / size).cast(IntegerType))
  }

  def toFiatCurrency(valueColumn: String, fiatValueColumn: String, length: Int)(
      df: DataFrame
  ) = {
    // see `transform_values` in Spark 3
    df.withColumn(
      fiatValueColumn,
      array(
        (0 until length)
          .map(
            i =>
              ((col(valueColumn) * col(fiatValueColumn)
                .getItem(i) / 1e6 + 0.5).cast(LongType) / 100.0)
                .cast(FloatType)
          ): _*
      )
    )
  }

  def plainAddressCluster(
      basicTxInputAddresses: DataFrame,
      removeCoinJoin: Boolean
  ) = {
    val addressCount = count(F.addressId).over(Window.partitionBy(F.txIndex))

    // filter transactions with multiple input addresses
    val collectiveInputAddresses = {
      if (removeCoinJoin) {
        println(
          "Clustering without coinjoin inputs"
        )
        basicTxInputAddresses.filter(
          col(F.coinjoin) === false
        )
      } else {
        println(
          "Clustering with coinjoin inputs"
        )
        basicTxInputAddresses
      }
    }.select(col(F.txIndex), col(F.addressId), addressCount.as("count"))
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
        .agg(collect_set(F.addressId).as("inputs"))
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

    // the initial representative must be one address per transaction, which also occurs
    // in other transactions (tx count >= 2 in collectiveInputAddresses);
    // choose address with highest tx count
    val initialRepresentative = {

      val transactionWindow = Window
        .partitionBy(F.txIndex)
        .orderBy(col("count").desc, col(F.addressId).asc)
      val rowNumber = row_number().over(transactionWindow)

      // for each tx, set address which is in most other transactions
      val addressMax = collectiveInputAddresses
        .join(transactionCount, F.addressId)
        .select(
          col(F.txIndex),
          col(F.addressId).as(reprAddrId),
          rowNumber.as("rank")
        )
        .filter(col("rank") === 1)
        .drop("rank")

      transactionCount
        .filter(col("count") === 1) // restrict to "trivial" addresses
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
          Seq(reprAddrId),
          "left_outer"
        )
        .select(
          col("id"),
          coalesce(col(F.cluster), col(reprAddrId)).as(F.cluster)
        )
        .as[Result[Int]]

    basicAddressCluster.union(addressClusterRemainder).toDF()
  }

  def addressCluster(
      regularInputs: Dataset[RegularInput],
      addressIds: Dataset[AddressId],
      removeCoinJoin: Boolean
  ) = {

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
      .as[AddressCluster]
  }

  def plainAddressRelations(
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction]
  ): Dataset[PlainAddressRelation] = {

    val regularInputSum =
      regularInputs.groupBy(F.txIndex).agg(sum(F.value).as("regularSum"))
    val addressInputSum =
      inputs.groupBy(F.height, F.txIndex).agg(sum(F.value).as("addressSum"))
    val totalInput = transactions
      .withColumn("input", explode(col("inputs")))
      .select(F.txIndex, "input.value")
      .groupBy(F.txIndex)
      .agg(sum(F.value).as(F.totalInput))
    val reducedInputSum =
      addressInputSum
        .join(regularInputSum, F.txIndex)
        .join(totalInput, F.txIndex)
        .select(
          col(F.height),
          col(F.txIndex),
          // regularSum == addressSum, unless input address is used as output in same tx
          (col(F.totalInput) - col("regularSum") + col("addressSum"))
            .as(F.totalInput)
        )

    inputs
      .select(
        col(F.txIndex),
        col(F.addressId).as(F.srcAddressId),
        col(F.value).as("inValue")
      )
      .join(
        outputs.select(
          col(F.txIndex),
          col(F.addressId).as(F.dstAddressId),
          col(F.value).as("outValue")
        ),
        F.txIndex
      )
      .join(reducedInputSum, F.txIndex)
      .withColumn(
        F.estimatedValue,
        round(col("inValue") / col(F.totalInput) * col("outValue")).cast(LongType)
      )
      .drop(F.totalInput, "inValue", "outValue")
      .as[PlainAddressRelation]
  }

  def addressRelations(
      plainAddressRelations: Dataset[PlainAddressRelation],
      exchangeRates: Dataset[ExchangeRates],
      addressTags: Dataset[AddressTag],
      noFiatCurrencies: Int,
      txLimit: Int
  ): Dataset[AddressRelation] = {

    val addressLabels = addressTags
      .select(F.addressId)
      .distinct
      .withColumn(F.hasLabels, lit(true))

    val fullAddressRelations = plainAddressRelations
      .join(exchangeRates, Seq("height"), "left")
      .transform(
        toFiatCurrency(F.estimatedValue, F.fiatValues, noFiatCurrencies)
      )
      .drop(F.height)
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        count(F.txIndex).cast(IntegerType).as(F.noTransactions),
        struct(
          sum(col(F.estimatedValue)).as(F.value),
          array(
            (0 until noFiatCurrencies)
              .map(i => sum(col(F.fiatValues).getItem(i)).cast(FloatType)): _*
          ).as(F.fiatValues)
        ).as(F.estimatedValue)
      )
      .transform(idGroup(F.srcAddressId, F.srcAddressIdGroup))
      .transform(idGroup(F.dstAddressId, F.dstAddressIdGroup))
      .join(
        addressLabels.select(
          col(F.addressId).as(F.srcAddressId),
          col(F.hasLabels).as(F.hasSrcLabels)
        ),
        Seq(F.srcAddressId),
        "left"
      )
      .join(
        addressLabels.select(
          col(F.addressId).as(F.dstAddressId),
          col(F.hasLabels).as(F.hasDstLabels)
        ),
        Seq(F.dstAddressId),
        "left"
      )

    val txList = plainAddressRelations
    // compute list column of transactions (only if #tx <= txLimit)
      .select(F.srcAddressId, F.dstAddressId, F.txIndex)
      .join(
        fullAddressRelations
          .select(F.srcAddressId, F.dstAddressId, F.noTransactions),
        Seq(F.srcAddressId, F.dstAddressId),
        "full"
      )
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        collect_set(when(col(F.noTransactions) <= txLimit, col(F.txIndex)))
          .as(F.txList)
      )

    fullAddressRelations
      .join(txList, Seq(F.srcAddressId, F.dstAddressId), "left")
      .na
      .fill(false, Seq(F.hasSrcLabels, F.hasDstLabels))
      .as[AddressRelation]
  }

  def plainClusterRelations(
      clusterInputs: Dataset[ClusterTransaction],
      clusterOutputs: Dataset[ClusterTransaction]
  ) = {
    clusterInputs
      .select(col(F.txIndex), col(F.cluster).as(F.srcCluster))
      .join(
        clusterOutputs
          .select(
            col(F.txIndex),
            col(F.cluster).as(F.dstCluster),
            col(F.value),
            col(F.height)
          ),
        Seq(F.txIndex)
      )
      .as[PlainClusterRelation]
  }

  def clusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelation],
      exchangeRates: Dataset[ExchangeRates],
      clusterTags: Dataset[ClusterTag],
      noFiatCurrencies: Int,
      txLimit: Int
  ) = {

    val clusterLabels = clusterTags
      .select(F.cluster)
      .withColumn(F.hasLabels, lit(true))

    val fullClusterRelations = plainClusterRelations
      .join(exchangeRates, Seq(F.height), "left")
      .transform(
        toFiatCurrency(F.value, F.fiatValues, noFiatCurrencies)
      )
      .drop(F.height)
      .groupBy(F.srcCluster, F.dstCluster)
      .agg(
        count(F.txIndex).cast(IntegerType).as(F.noTransactions),
        struct(
          sum(col(F.value)).as(F.value),
          array(
            (0 until noFiatCurrencies)
              .map(i => sum(col(F.fiatValues).getItem(i)).cast(FloatType)): _*
          ).as(F.fiatValues)
        ).as(F.value)
      )
      .transform(idGroup(F.srcCluster, F.srcClusterGroup))
      .transform(idGroup(F.dstCluster, F.dstClusterGroup))
      .join(
        clusterLabels.select(
          col(F.cluster).as(F.srcCluster),
          col(F.hasLabels).as(F.hasSrcLabels)
        ),
        Seq(F.srcCluster),
        "left"
      )
      .join(
        clusterLabels.select(
          col(F.cluster).as(F.dstCluster),
          col(F.hasLabels).as(F.hasDstLabels)
        ),
        Seq(F.dstCluster),
        "left"
      )

    val txList = plainClusterRelations
    // compute list column of transactions (only if #tx <= txLimit)
      .select(F.srcCluster, F.dstCluster, F.txIndex)
      .join(
        fullClusterRelations
          .select(F.srcCluster, F.dstCluster, F.noTransactions),
        Seq(F.srcCluster, F.dstCluster),
        "full"
      )
      .groupBy(F.srcCluster, F.dstCluster)
      .agg(
        collect_set(when(col(F.noTransactions) <= txLimit, col(F.txIndex)))
          .as(F.txList)
      )

    fullClusterRelations
      .join(txList, Seq(F.srcCluster, F.dstCluster), "left")
      .na
      .fill(false, Seq(F.hasSrcLabels, F.hasDstLabels))
      .as[ClusterRelation]
  }
}
