package at.ac.ait

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.funsuite.AnyFunSuite


class PlainAddressClusteringTest extends AnyFunSuite
  with SparkSessionTestWrapper with DataFrameComparer {
  val bucketSize: Int = 2
  val t = new Transformator(spark, bucketSize)

  spark.sparkContext.setLogLevel("ERROR")

  test(" 'simple' addresses that appear in only one transaction are not clustered here") {
    import spark.implicits._
    val tx = Seq(
      (666L, 1, false),
      (888L, 4, false))
      .toDF("txIndex", "addressId", "coinJoin")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assert(clusters.isEmpty)
  }

  test("each address is in only one transaction, but MULTIPLE-INPUT addresses present") {
    import spark.implicits._
    val tx = Seq(
        (100L, 1, false),
        (100L, 2, false),
        (404L, 3, false)) // adr 3 will not be clustered because it's not multiple-input
      .toDF("txIndex", "addressId", "coinJoin")

    val expected = Seq(
      (1, 1),
      (2, 1)).toDF("id", "cluster")


    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }

  test("multiple tx for address 2, all addresses are multiple-input") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false), // adr 2 appears in most transactions

      (404L, 2, false),
      (404L, 3, false))
      .toDF("txIndex", "addressId", "coinJoin")

    val expected = Seq(
      // address, cluster
      (1, 2),
      (2, 2),
      (3, 2),
    ).toDF("id", "cluster")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }

  test("multiple tx for address 2 and 3, all addresses are multiple-input") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),
      (100L, 3, false),

      (404L, 2, false),
      (404L, 3, false),
      (404L, 4, false))
      .toDF("txIndex", "addressId", "coinJoin")

    val expected = Seq(
      // address, cluster
      (1, 2),
      (2, 2),
      (3, 2),
      (4, 2)
    ).toDF("id", "cluster")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }

  test("two clusters are found") {
    import spark.implicits._
    val tx = Seq(
      (100L, 2, false), // adr 2 appears in most transactions in this cluster
      (100L, 3, false),

      (404L, 1, false),
      (404L, 2, false),

      (770L, 10, false), // separate cluster with id = min(10, 11)
      (770L, 11, false))
      .toDF("txIndex", "addressId", "coinJoin")

    val expected = Seq(
      (1, 2),
      (2, 2),
      (3, 2),
      (10, 10),
      (11, 10)
    ).toDF("id", "cluster")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }

  test("addresses 2 and 3 appear in the same # of tx, 2 < 3 so 2 is chosen as id") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),

      (404L, 2, false),
      (404L, 3, false),

      (770L, 3, false),
      (770L, 4, false))
      .toDF("txIndex", "addressId", "coinJoin")

    val expected = Seq(
      (1, 2),
      (2, 2),
      (3, 2),
      (4, 2),
    ).toDF("id", "cluster")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }

  ignore("address 1 and 2 are in multiple tx. 2 wins because it's in most transactions") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),

      (404L, 2, false),
      (404L, 3, false),

      (770L, 1, false),
      (770L, 2, false),
      (770L, 4, false))
     .toDF("txIndex", "addressId", "coinJoin")
    tx.show()
    val expected = Seq(
      (1, 2),
      (2, 2),
      (3, 2),
      (4, 2)
    ).toDF("id", "cluster")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true)
    assertSmallDataFrameEquality(clusters, expected, orderedComparison = false)
  }
}
