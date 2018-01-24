package at.ac.ait

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import at.ac.ait.{Fields => F}

case class BlockGroup(block_group: Int)

object TransformationJob {

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) / 1000 + "s")
    result
  }

  case class AppArgs(
      sourceKeyspace: String,
      targetKeyspace: String,
      maxBlockgroup: Int
  )

  object AppArgs {
    def empty = new AppArgs("", "", 0)
  }

  def main(args: Array[String]) {

    val argsInstance = args.sliding(2, 1).toList.foldLeft(AppArgs.empty) {
      case (accumArgs, currArgs) => currArgs match {
        case Array("--source_keyspace", sourceKeyspace) => accumArgs.copy(sourceKeyspace = sourceKeyspace)
        case Array("--target_keyspace", targetKeyspace) => accumArgs.copy(targetKeyspace = targetKeyspace)
        case Array("--max_blockgroup", maxBlockgroup) => accumArgs.copy(maxBlockgroup = maxBlockgroup.toInt)
        case _ => accumArgs
      }
    }

    if (argsInstance.maxBlockgroup < 0 ||
        argsInstance.sourceKeyspace == "" ||
        argsInstance.targetKeyspace == "") {
      Console.err.println("Usage: spark-submit [...] graphsense-transformation.jar" +
        " --source_keyspace SRC_KEYSPACE" +
        " --target_keyspace TGT_KEYSPACE" +
        " --max_blockgroup BLOCKGROUP")
      sys.exit(1)
    }

    val spark = SparkSession.builder.appName("GraphSense Transformation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val keyspace_raw = argsInstance.sourceKeyspace
    val keyspace = argsInstance.targetKeyspace

    val maxBlockGroup = {
      if (argsInstance.maxBlockgroup == 0) {
        val rs = CassandraConnector(spark.sparkContext.getConf).withSessionDo (
          session => session.execute(
            s"SELECT block_group FROM $keyspace_raw.transaction PER PARTITION LIMIT 1")
        )
        rs.all.asScala.map(_ getInt 0).max
      } else {
        argsInstance.maxBlockgroup
      }
    }

    val rawBlocks = spark.sparkContext.cassandraTable[RawBlock](keyspace_raw, "block").toDS()

    val transactionTable = "transaction"
    val rawTransactionsRelative =
      spark.sparkContext.parallelize(0.to(maxBlockGroup).map(BlockGroup))
        .repartitionByCassandraReplica(keyspace_raw, transactionTable, 50)
        .joinWithCassandraTable[RawTransaction](keyspace_raw, transactionTable)
        .values
        .toDS().persist()

    val rawExchangeRates =
      spark.sparkContext.cassandraTable[RawExchangeRates](keyspace_raw, "exchange_rates").toDS()

    val rawTags =
      spark.sparkContext.cassandraTable[RawTag](keyspace_raw, "tag").toDS()

    val rawTransactions =
      rawTransactionsRelative.sort(F.height, F.txNumber).rdd
        .zipWithIndex()
        .map { case ((t, id)) =>
          RawTransaction(t.height, id.toInt + 1, t.txHash, t.timestamp, t.coinbase, t.vin, t.vout)
        }.toDS().persist()

    val transformation = new Transformation(spark, rawBlocks, rawTransactions, rawExchangeRates, rawTags)

    def save[A <: Product: ClassTag: TypeTag](table: Dataset[A], tableName: String) = {
      val description = "store " + tableName
      println(description)
      spark.sparkContext.setJobDescription(description)
      time{table.rdd.saveToCassandra(keyspace, tableName)}
      ()
    }

    save(transformation.blocks, "block")
    save(transformation.addressTransactions, "address_transactions")
    save(transformation.transactions, "transaction")
    save(transformation.blockTransactions, "block_transactions")
    save(transformation.exchangeRates, "exchange_rates")
    save(transformation.addresses, "address")
    save(transformation.addressCluster, "address_cluster")
    save(transformation.clusterAddresses, "cluster_addresses")
    save(transformation.cluster, "cluster")
    save(transformation.clusterTags, "cluster_tags")
    save(transformation.addressRelations.sort(F.dstAddressPrefix), "address_incoming_relations")
    save(transformation.addressRelations.sort(F.srcAddressPrefix), "address_outgoing_relations")
    save(
      transformation.clusterRelations.sort(F.dstCluster, F.srcCluster),
      "cluster_incoming_relations")
    save(
      transformation.clusterRelations.sort(F.srcCluster, F.dstCluster),
      "cluster_outgoing_relations")
    spark.stop()
  }
}
