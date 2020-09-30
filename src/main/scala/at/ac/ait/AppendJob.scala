package at.ac.ait

import at.ac.ait.storage.CassandraStorage
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.rogach.scallop.ScallopConf

object AppendJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val currency = opt[String](
      required = true,
      descr = "Cryptocurrency (e.g. BTC, BCH, LTC, ZEC)"
    )
    val rawKeyspace =
      opt[String]("raw_keyspace", required = true, descr = "Raw keyspace")
    val tagKeyspace =
      opt[String]("tag_keyspace", required = true, descr = "Tag keyspace")
    val targetKeyspace = opt[String](
      "target_keyspace",
      required = true,
      descr = "Transformed keyspace"
    )
    val bucketSize = opt[Int](
      "bucket_size",
      required = false,
      default = Some(25000),
      descr = "Bucket size for Cassandra partitions"
    )
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Currency:        " + conf.currency())
    println("Raw keyspace:    " + conf.rawKeyspace())
    println("Tag keyspace:    " + conf.tagKeyspace())
    println("Target keyspace: " + conf.targetKeyspace())
    println("Bucket size:     " + conf.bucketSize())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val summaryStatisticsRaw = cassandra
      .load[SummaryStatisticsRaw](conf.rawKeyspace(), "summary_statistics")
    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")

    val noBlocks = summaryStatisticsRaw.select(col("noBlocks")).first.getInt(0)
    val lastBlockTimestamp =
      summaryStatisticsRaw.select(col("timestamp")).first.getInt(0)
    val noTransactions =
      summaryStatisticsRaw.select(col("noTxs")).first.getLong(0)

    val processedBlockCount =
      cassandra.load[SummaryStatistics](conf.targetKeyspace(), "summary_statistics")
        .first().noBlocks

    val unprocessedBlocks = blocks.filter((b) => {b.height > processedBlockCount})
    println()
    println(s"Found ${unprocessedBlocks.count()} new blocks to process.")

    val transformation = new Transformation(spark, conf.bucketSize())

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(unprocessedBlocks, exchangeRatesRaw)
        .persist()

    assert(exchangeRates.count() == unprocessedBlocks.count())
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)
  }
}
