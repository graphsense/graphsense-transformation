package at.ac.ait

import java.io.{File, PrintWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest._
import scala.io.Source

import at.ac.ait.{Fields => F}

trait SparkEnvironment extends BeforeAndAfterAll { this: Suite =>

  val conf = new SparkConf()
    .setAppName("Transformation Test")
    .setMaster("local")
    .set("spark.sql.shuffle.partitions", "1")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def blockHash(n: Byte) = Array(n, n, n, n, n)
  def a(n: Int) = n + "address"

  val transactions: Dataset[Transaction] =
    Seq(Transaction("01010", blockHash(1), 23, 0, true, 0, 200000000, null,
                    Seq(TxInputOutput(Seq(a(1)), 100000000, 1),
                        TxInputOutput(Seq(a(2)), 100000000, 1)), 1),
        Transaction("02020", blockHash(2), 23, 0, false, 200000000, 200000000,
                    Seq(TxInputOutput(Seq(a(1)), 100000000, 1),
                        TxInputOutput(Seq(a(2)), 100000000, 1)),
                    Seq(TxInputOutput(Seq(a(3)), 200000000, 1)), 2))
      .toDS()

  val exchangeRates: Dataset[ExchangeRates] = Seq(ExchangeRates(23, 4, 5)).toDS()
  val tag: Dataset[Tag] = List(Tag(a(1), "tag1", "", "", "", "", "", 12)).toDS()
  val transformation = new Transformation(spark, transactions, exchangeRates, tag)

  override def afterAll() {
    spark.close()
  }
}

class TransformationSpec extends FlatSpec with Matchers with SparkEnvironment {

  def fileTest(name: String, dataset: Dataset[_]) {
    val filename = name + ".txt"
    def formattedString(a: Any, inProduct: Boolean = false): String =
      a match {
        case t: Traversable[_] => t.map(formattedString(_)).mkString("[", "; ", "]")
        case a: Array[_] => a.map(formattedString(_)).mkString
        case p: Product => {
          val parts = p.productIterator.map(formattedString(_, true))
          if (inProduct) parts.mkString("(", ", ", ")")
          else parts.mkString(", ")
        }
        case null => "null"
        case _ => a.toString()
      }

    val testDir = "tests/result"
    val referenceDir = "tests/reference"
    new File(testDir).mkdir()

    def printToFile() {
      val f = new File(testDir + File.separator + filename)
      val p = new PrintWriter(f)
      try {
        dataset.collect().foreach(a => p.println(formattedString(a)))
      } finally { p.close() }
    }

    def filesAreEqual() = {
      def fileContent(dir: String) = {
        val source = Source.fromFile(dir + File.separator + filename)
        try source.getLines().mkString finally source.close()
      }
      fileContent(testDir) == fileContent(referenceDir)
    }

    s"The table $name" should "be correct" in {
      printToFile()
      filesAreEqual() shouldEqual true
    }
  }

  fileTest("addresses", transformation.addresses)
  fileTest("addressTransactions", transformation.addressTransactions)
  fileTest("addressCluster", transformation.addressCluster)
  fileTest("clusterAddresses", transformation.clusterAddresses.sort(F.cluster, F.address))
  fileTest("cluster", transformation.cluster)
  fileTest("clusterTags", transformation.clusterTags)
  fileTest("addressRelations", transformation.addressRelations)
  fileTest("clusterRelations", transformation.clusterRelations)
}
