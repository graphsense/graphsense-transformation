package at.ac.ait

import com.datastax.spark.connector._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import at.ac.ait.{Fields => F}

case class BlockGroup(block_group: Int)

object SimpleApp {
  
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) / 1000 + "s")
    result
  }
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    val keyspace_raw_old = "graphsense_raw"
    val keyspace_raw = "graphsense_raw_new"
    val keyspace = "graphsense_transformed_new"
    val allBlocks = false
    
    //val rawBlocks = spark.sparkContext.cassandraTable[RawBlock](keyspace, "block").toDS()
    val transactionTable = "transaction"
    val rawTransactionsRelative = {
      if (allBlocks)
        spark.sparkContext.cassandraTable[RawTransaction](keyspace_raw, transactionTable)
      else
        spark.sparkContext.parallelize(0.to(1).map(BlockGroup))
          .repartitionByCassandraReplica(keyspace_raw, transactionTable, 50)
          .joinWithCassandraTable[RawTransaction](keyspace_raw, transactionTable)
          .values
    }.toDS().persist()
    val rawExchangeRates =
      spark.sparkContext.cassandraTable[RawExchangeRates](keyspace_raw, "exchange_rates").toDS()
    val rawTags =
      spark.sparkContext.cassandraTable[RawTag](keyspace_raw_old, "tag").toDS()
    
    val rawTransactions =
      rawTransactionsRelative.sort(F.height, F.txNumber).rdd
        .zipWithIndex()
        .map { case ((t, id)) =>
          RawTransaction(t.height, id.toInt + 1, t.txHash, t.timestamp, t.coinbase, t.vin, t.vout)
        }.toDS().persist()
    
    val transformation = new Transformation(spark, rawTransactions, rawExchangeRates, rawTags)
    
    def save[A <: Product: ClassTag: TypeTag](table: Dataset[A], tableName: String) = {
      val description = "store " + tableName
      println(description)
      spark.sparkContext.setJobDescription(description)
      time{table.rdd.saveToCassandra(keyspace, tableName)}
      ()
    }
    
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
