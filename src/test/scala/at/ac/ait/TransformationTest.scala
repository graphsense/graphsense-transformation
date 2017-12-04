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
    //.set("spark.memory.fraction", "0.8")
    //.set("spark.memory.storageFraction", "0.3")
  val spark = SparkSession.builder.config(conf).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  
  def tx(n: Byte) = Array(n, n, n, n, n, n)
  def a(n: Int) = n + "address"
  
  val bitcoin: Long = 10000 * 10000
  
  val rawTransactions = List(
    RawTransaction(
      0,
      1,
      tx(1),
      23,
      true,
      List.empty,
      List(RawOutput(bitcoin, 0, List(a(1))), RawOutput(bitcoin, 1, List(a(2))))),
    RawTransaction(
      0,
      2,
      tx(2),
      23,
      false,
      List(RawInput(tx(1), 0), RawInput(tx(1), 1)),
      List(RawOutput(2 * bitcoin, 0, List(a(3)))))).toDS()
  val rawExchangeRates = List(RawExchangeRates(23, Some(4), Some(5))).toDS()
  val rawTags = List(RawTag(a(1),"tag1","","","","","",12)).toDS()
  val transformation = new Transformation(spark, rawTransactions, rawExchangeRates, rawTags)
  
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
    
    val testDir = "last_test"
    val referenceDir = "test_success"
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
  
  fileTest("addressTransactions", transformation.addressTransactions)
  fileTest("transactions", transformation.transactions)
  fileTest("blockTransactions", transformation.blockTransactions)
  fileTest("exchangeRates", transformation.exchangeRates)
  fileTest("addresses", transformation.addresses)
  fileTest("addressCluster", transformation.addressCluster)
  fileTest("clusterAddresses", transformation.clusterAddresses.sort(F.cluster, F.address))
  fileTest("cluster", transformation.cluster)
  fileTest("clusterTags", transformation.clusterTags)
  fileTest("addressRelations", transformation.addressRelations)
  fileTest("clusterRelations", transformation.clusterRelations)
}
