package at.ac.ait

import at.ac.ait.storage.CassandraStorage
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.{CassandraRow, ColumnName, SomeColumns, toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, floor, lit, lower, min, struct, substring, sum, udf, when}
import org.rogach.scallop.ScallopConf

import scala.reflect.ClassTag

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


  def compute(args: Array[String]): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")

    val lastProcessedBlock =
      cassandra.load[SummaryStatistics](conf.targetKeyspace(), "summary_statistics")
        .first().noBlocks - 1

    val unprocessedBlocks = blocks.filter((b) => {
      b.height > lastProcessedBlock
    })
    val transactionsDiff = transactions.filter((tx) => {
      tx.height > lastProcessedBlock
    })
    println()
    println("Computing unprocessed diff")
    println(s"New blocks:          ${unprocessedBlocks.count()}")
    println(s"New transactions:    ${transactionsDiff.count()}")
    println()

    val transformation = new Transformation(spark, conf.bucketSize())

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates.filter(r => r.height > lastProcessedBlock))

    println("Extracting transaction inputs")
    val regInputs = transformation.computeRegularInputs(transactions).persist()
    println("Extracting transaction outputs")
    val regOutputs = transformation.computeRegularOutputs(transactions).persist()

    println("Computing address IDs")
    val addressIds = transformation.computeAddressIds(regOutputs)

    val addressByIdGroup = transformation.computeAddressByIdGroups(addressIds)
    cassandra.store(
      conf.targetKeyspace(),
      "address_by_id_group",
      addressByIdGroup
    )

    val addressTransactionsDiff =
      transformation
        .computeAddressTransactions(
          transactionsDiff,
          regInputs,
          regOutputs,
          addressIds
        )
        .persist()

    println("Address transaction diff for address 9701: ")
    addressTransactionsDiff.filter(r => r.addressId == 9701).show()


    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions",
      addressTransactionsDiff,
      ifNotExists = true
    )

    val (inputsDiff, outputsDiff) =
      transformation.splitTransactions(addressTransactionsDiff)
    inputsDiff.persist()
    outputsDiff.persist()

    println("Computing address statistics")

    /**
     * All addresses affected by the newly added transactions.
     */
    val basicAddressesDiff: Dataset[BasicAddress] =
      transformation
        .computeBasicAddresses(
          transactionsDiff,
          addressTransactionsDiff,
          inputsDiff,
          outputsDiff,
          exchangeRates
        )
        .persist()

    println("Basic addresses diff for address 9701: ")
    basicAddressesDiff.filter(r => r.addressId == 9701).show()

    val addressTagsDiff: Dataset[AddressTags] =
      if (tagsRaw.count() > 0) {
        println("Computing address tags")
        val addressTagsDiff =
          transformation
            .computeAddressTags(
              tagsRaw,
              basicAddressesDiff,
              addressIds,
              conf.currency()
            )
            .persist()
        cassandra.store(conf.targetKeyspace(), "address_tags", addressTagsDiff)
        val noAddressTagsDiff = addressTagsDiff
          .select(col("label"))
          .withColumn("label", lower(col("label")))
          .distinct()
          .count()
        addressTagsDiff
      }
      else {
        println("No tags available, not computing address tags!")
        spark.emptyDataset
      }


    println("Computing plain address relations")
    /**
     * Plain address relations derived only from unprocessed transactions (transactionsDiff)
     */
    val plainAddressRelationsDiff =
      transformation
        .computePlainAddressRelations(
          inputsDiff,
          outputsDiff,
          regInputs,
          transactionsDiff
        )

    println("Plain relations for address 9701: ")
    plainAddressRelationsDiff.filter(r => r.srcAddressId == 9701).show()

    println("Computing address relations")
    val addressRelationsDiff: Dataset[AddressRelations] =
      transformation
        .computeAddressRelations(
          plainAddressRelationsDiff,
          basicAddressesDiff,
          exchangeRates,
          addressTagsDiff
        )
        .persist()

    println("Relations for address 9701: ")
    addressRelationsDiff.filter(r => r.srcAddressId == 9701).show()

    val mergeArrays = udf((a: Seq[String], b: Seq[String]) => (a ++ b).distinct)

    def sumCurrency: ((Currency, Currency) => Currency) = {
      (c1, c2) =>
        Currency(c1.value + c2.value, c1.eur + c2.eur, c1.usd + c2.usd)
    }

    def sumProperties: ((AddressSummary, AddressSummary) => AddressSummary) = {
      (a1, a2) =>
        AddressSummary(
          sumCurrency.apply(a1.totalReceived, a2.totalReceived),
          sumCurrency.apply(a1.totalSpent, a2.totalSpent)
        )
    }

    val bucketSize = conf.bucketSize.getOrElse(0)

    val addressPropsDiff = basicAddressesDiff
      .withColumn(Fields.addressIdGroup, floor(col(Fields.addressId) / lit(bucketSize)).cast("int"))
      .select(Fields.addressIdGroup, Fields.addressId, Fields.totalReceived, Fields.totalSpent)
      .withColumnRenamed(Fields.addressId, "address_id2")
      .join(addressIds, $"address_id2".equalTo(col(Fields.addressId)))
      .withColumn(Fields.addressPrefix, substring(col(Fields.address), 0, 5))
      .as[AddressProperties]

    /**
     * Total received and total spent by address, only for addresses affected by new transactions.
     */
    val addressProps = addressPropsDiff
      .as[AddressProperties]
      .rdd
      .joinWithCassandraTable[Address](conf.targetKeyspace(), "address")
      .on(SomeColumns("address_prefix", "address"))
      .map({
        case (diff, existing) =>
          val totalReceived = sumCurrency.apply(diff.totalReceived, existing.totalReceived)
          val totalSpent = sumCurrency.apply(diff.totalSpent, existing.totalSpent)
          diff.asInstanceOf[AddressProperties].copy(totalReceived = totalReceived, totalSpent = totalSpent)
      }).toDS()

    println("addressProps for addresses 744: ")
    addressProps.filter(r => r.addressId == 744).show()


    val newAndUpdatedIn = addressRelationsDiff.as[AddressIncomingRelations]
      .rdd.leftJoinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .on(SomeColumns("src_address_id", "dst_address_id", "dst_address_id_group"))
      .map(r => {
        val existingOpt = r._2
        val diff = r._1
        if (existingOpt.isDefined) {
          val existing = existingOpt.get
          val noTransactions = existing.noTransactions + diff.noTransactions
          val estimatedValue = sumCurrency.apply(existing.estimatedValue, diff.estimatedValue)
          val srcProperties = sumProperties.apply(existing.srcProperties, diff.srcProperties)
          val srcLabels = (Option(existing.srcLabels).getOrElse(Seq()) ++ Option(diff.srcLabels).getOrElse(Seq())).distinct
          existing.copy(noTransactions = noTransactions, estimatedValue = estimatedValue, srcLabels = srcLabels)
        } else {
          diff
        }
    })

    val newAndUpdatedOut = addressRelationsDiff.as[AddressOutgoingRelations]
      .rdd.leftJoinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
      .on(SomeColumns("src_address_id", "dst_address_id", "src_address_id_group"))
      .map(r => {
        val existingOpt = r._2
        val diff = r._1
        if (existingOpt.isDefined) {
          val existing = existingOpt.get
          val noTransactions = existing.noTransactions + diff.noTransactions
          val estimatedValue = sumCurrency.apply(existing.estimatedValue, diff.estimatedValue)
          val dstProperties = sumProperties.apply(existing.dstProperties, diff.dstProperties)
          val dstLabels = (Option(existing.dstLabels).getOrElse(Seq()) ++ Option(diff.dstLabels).getOrElse(Seq())).distinct
          val txList = ((Option(existing.txList).getOrElse(Seq())) ++ Option(diff.txList).getOrElse(Seq())).distinct
          existing.copy(noTransactions = noTransactions, estimatedValue = estimatedValue, dstLabels = dstLabels, txList = txList)
        } else {
          diff
        }
      })
    newAndUpdatedIn.saveToCassandra(conf.targetKeyspace(), "address_incoming_relations")
    newAndUpdatedOut.saveToCassandra(conf.targetKeyspace(), "address_outgoing_relations")


    val props: ((Currency, Currency) => AddressSummary) = { case(totalReceived, totalSpent) => AddressSummary(totalReceived, totalSpent) }
    val propsUdf = udf(props)

    val mergedIn = cassandra.load[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .join(addressProps, joinExprs = col(Fields.srcAddressId).equalTo(col(Fields.addressId)))
      .withColumn(Fields.srcProperties, propsUdf(col(Fields.totalReceived), col(Fields.totalSpent)))
      .as[AddressIncomingRelations]
    cassandra.store[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations", mergedIn)

    val mergedOut = cassandra.load[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
      .join(addressProps, joinExprs = col(Fields.dstAddressId).equalTo(col(Fields.addressId)))
      .withColumn(Fields.dstProperties, propsUdf(col(Fields.totalReceived), col(Fields.totalSpent)))
      .as[AddressOutgoingRelations]
    cassandra.store[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations", mergedOut)
  }

  def verify(args: Array[String]): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val verifyTables = List(
//      "address_by_id_group",
//      "address_transactions",
//      "address_incoming_relations",
      "address_outgoing_relations",
      "address"
    )


    def verifyTable[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](table: String, sortBy: String) {
      println(s"Calculating difference: $table")
      val byAppendJob = cassandra.load[T](conf.targetKeyspace(), table)
        .sort(sortBy)

      val byTransformJob = cassandra.load[T]("btc_transformed", table)
        .sort(sortBy)

      byAppendJob.union(byTransformJob).except(byAppendJob.intersect(byTransformJob)).as[T].show()
    }

    if (verifyTables contains "address_transactions") {
      verifyTable[AddressTransactions]("address_transactions", Fields.addressId)
    }

    if (verifyTables contains "address_by_id_group") {
      verifyTable[AddressByIdGroup]("address_by_id_group", Fields.addressId)
    }

    if (verifyTables contains "address_incoming_relations") {
      verifyTable[AddressIncomingRelations]("address_incoming_relations", Fields.dstAddressId)
    }

    if (verifyTables contains "address_outgoing_relations") {
      verifyTable[AddressOutgoingRelations]("address_outgoing_relations", Fields.srcAddressId)
    }

    if (verifyTables contains "address") {
      verifyTable[Address]("address", Fields.addressId)
    }
  }

  def setupCassandra(args: Array[String]): (CassandraStorage, Conf, SparkSession) = {
    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Iterative Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Currency:        " + conf.currency())
    println("Raw keyspace:    " + conf.rawKeyspace())
    println("Tag keyspace:    " + conf.tagKeyspace())
    println("Target keyspace: " + conf.targetKeyspace())
    println("Bucket size:     " + conf.bucketSize())

    val cassandra = new CassandraStorage(spark)
    (cassandra, conf, spark)
  }

  def restore(args: Array[String], fromKeyspace: String = "btc_transformed10k", toKeyspace: String = "btc_transformed_append"): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    var table = "address_incoming_relations"

    println(s"Restore: Writing table ${table} from keyspace ${fromKeyspace} to ${toKeyspace}")
    val addrInRelations = cassandra.load[AddressIncomingRelations](fromKeyspace, table)
    cassandra.store[AddressIncomingRelations](toKeyspace, table, addrInRelations)

    table = "address_outgoing_relations"
    val addrOutRelations = cassandra.load[AddressOutgoingRelations](fromKeyspace, table)
    cassandra.store[AddressOutgoingRelations](toKeyspace, table, addrOutRelations)
  }

  def main(args: Array[String]) {
    compute(args)
//    verify(args)
//    restore(args)
  }
}
