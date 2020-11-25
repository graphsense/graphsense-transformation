package at.ac.ait

import at.ac.ait.clustering.MultipleInputClustering
import at.ac.ait.storage.CassandraStorage
import com.datastax.driver.core.DataType
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{array_distinct, col, collect_list, collect_set, count, countDistinct, first, flatten, floor, lit, lower, size, struct, substring, sum, udf, when}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf
import sun.invoke.util.ValueConversions.cast

import scala.collection.{immutable, mutable}
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

    val appendBlockCount = opt[Int](
      "append_block_count",
      required = true,
      default = Some(1000),
      descr = "Maximum number of blocks the append job will process"
    )

    verify()
  }


  def compute(args: Array[String]): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val disableAddressTransactions = false
    val disableAddressIds = false
    val disableRelations = false

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")

    val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")
//    val tagsRaw = spark.emptyDataset[TagRaw]

    val lastProcessedBlock =
      cassandra.load[SummaryStatistics](conf.targetKeyspace(), "summary_statistics")
        .first().noBlocks - 1

    println()
    println("Computing unprocessed diff")
    val heightsToProcess = spark.range(start = lastProcessedBlock + 1, end = lastProcessedBlock + conf.appendBlockCount() + 1)
      .withColumnRenamed("id", Fields.height)
    val unprocessedBlocks = blocks.join(heightsToProcess, Seq(Fields.height), "left_semi").as[Block].persist()
    val transactionsDiff = transactions.join(heightsToProcess, Seq(Fields.height), "left_semi").as[Transaction].persist()
    println(s"New blocks:          ${unprocessedBlocks.count()}")
    println(s"New transactions:    ${transactionsDiff.count()}")
    println()

    val transformation = new Transformation(spark, conf.bucketSize())

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

        //todo uncommment
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates.filter(r => r.height > lastProcessedBlock))

    val regInputsDiff = transformation.computeRegularInputs(transactionsDiff).persist()
    println("Extracting transaction outputs")
    val regOutputs = transformation.computeRegularOutputs(transactions)
    val regOutputsDiff = transformation.computeRegularOutputs(transactionsDiff).persist()

    println("Computing address IDs")
    val addressIds = transformation.computeAddressIds(regOutputs)

    val addressByIdGroup = transformation.computeAddressByIdGroups(addressIds).persist()
    val totalAddressCount = addressByIdGroup.count()
    val currentSummary = spark.sparkContext.cassandraTable[SummaryStatistics](conf.targetKeyspace(), "summary_statistics").first()
    if (!disableAddressIds && currentSummary.noAddresses != totalAddressCount) {
      cassandra.store(
        conf.targetKeyspace(),
        "address_by_id_group",
        addressByIdGroup
      )

      cassandra.store(conf.targetKeyspace(), "summary_statistics", spark.createDataset(Seq(currentSummary.copy(noAddresses = totalAddressCount))))
    }

    val addressTransactionsDiff =
      transformation
        .computeAddressTransactions(
          transactionsDiff,
          regInputsDiff,
          regOutputsDiff,
          addressIds
        )
        .persist(StorageLevel.DISK_ONLY)

    if (!disableAddressTransactions) {
      cassandra.store(
        conf.targetKeyspace(),
        "address_transactions",
        addressTransactionsDiff.sort(Fields.addressId)
      )
    }

    val (inputsDiff, outputsDiff) =
      transformation.splitTransactions(addressTransactionsDiff)
    inputsDiff.persist(StorageLevel.DISK_ONLY)
    outputsDiff.persist(StorageLevel.DISK_ONLY)

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
        .persist(StorageLevel.DISK_ONLY)

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

        //todo uncommment
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

    val bucketSize = conf.bucketSize.getOrElse(0)



    println("Computing plain address relations")
    /**
     * Plain address relations derived only from unprocessed transactions (transactionsDiff)
     */
    val plainAddressRelationsDiff =
      transformation
        .computePlainAddressRelations(
          inputsDiff,
          outputsDiff,
          regInputsDiff,
          transactionsDiff
        )

    println("Computing address relations")
    val addressRelationsDiff: Dataset[AddressRelations] =
      transformation
        .computeAddressRelations(
          plainAddressRelationsDiff,
          basicAddressesDiff,
          exchangeRates,
          addressTagsDiff
        )
        .persist(StorageLevel.MEMORY_AND_DISK)

    val sumCurrency: ((Currency, Currency) => Currency) = { case(curr1, curr2) => Currency(curr1.value + curr2.value, curr1.eur + curr2.eur, curr1.usd + curr1.usd) }
    val sumUpCurrencies: (Seq[GenericRowWithSchema]) => Currency = {
      list =>
        val vals = list.map(r => (r.getLong(0), r.getFloat(1), r.getFloat(2))).reduce[Tuple3[Long, Float, Float]]({
          case ((value1: Long, eur1: Float, usd1: Float), (value2: Long, eur2: Float, usd2: Float)) => (
            value1 + value2,
            eur1 + eur2,
            usd1 + usd2
          )
        })
        Currency(vals._1, vals._2, vals._3)
    }

    def sumProperties: ((AddressSummary, AddressSummary) => AddressSummary) = {
      (a1, a2) =>
        AddressSummary(
          sumCurrency.apply(a1.totalReceived, a2.totalReceived),
          sumCurrency.apply(a1.totalSpent, a2.totalSpent)
        )
    }

    println("Updating address incoming relations")
    addressRelationsDiff.as[AddressIncomingRelations]
      .rdd
      .leftJoinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .on(SomeColumns("src_address_id", "dst_address_id", "dst_address_id_group"))
      .mapPartitions(list => {
        list.map({
          case (diff, existingOpt) =>
          if (existingOpt.isDefined) {
            val existing = existingOpt.get
            val noTransactions = existing.noTransactions + diff.noTransactions
            val estimatedValue = sumCurrency.apply(existing.estimatedValue, diff.estimatedValue)
            val srcLabels = (Option(existing.srcLabels).getOrElse(Seq()) ++ Option(diff.srcLabels).getOrElse(Seq())).distinct
            existing.copy(noTransactions = noTransactions, estimatedValue = estimatedValue, srcLabels = srcLabels)
          } else {
            diff
          }
        })
      })
      .sortBy(r => r.dstAddressId)
      .saveToCassandra(conf.targetKeyspace(), "address_incoming_relations")

    println("Updating address outgoing relations")
    addressRelationsDiff.as[AddressOutgoingRelations]
      .rdd
      .leftJoinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
      .on(SomeColumns("src_address_id", "dst_address_id", "src_address_id_group"))
      .mapPartitions(list => {
        list.map({
          case (diff, existingOpt) =>
            if (existingOpt.isDefined) {
              val existing = existingOpt.get
              val noTransactions = existing.noTransactions + diff.noTransactions
              val estimatedValue = sumCurrency.apply(existing.estimatedValue, diff.estimatedValue)
              val dstLabels = (Option(existing.dstLabels).getOrElse(Seq()) ++ Option(diff.dstLabels).getOrElse(Seq())).distinct
              val txList = ((Option(existing.txList).getOrElse(Seq())) ++ Option(diff.txList).getOrElse(Seq())).distinct
              existing.copy(noTransactions = noTransactions, estimatedValue = estimatedValue, dstLabels = dstLabels, txList = txList)
            } else {
              diff
            }
        })
      })
      .sortBy(r => r.srcAddressId)
      .saveToCassandra(conf.targetKeyspace(), "address_outgoing_relations")


    println("Computing addresses")
    val addressesDiff =
      transformation.computeAddresses(
        basicAddressesDiff,
        addressRelationsDiff,
        addressIds
      ).persist(StorageLevel.MEMORY_AND_DISK)


    val noChangedAddresses = addressesDiff.count()

    /**
     * Information about all affected addresses, merged with their previous state in cassandra.
     */
    val mergedAddresses = addressesDiff
      .rdd
      .leftJoinWithCassandraTable[Address](conf.targetKeyspace(), "address")
      .on(SomeColumns("address_prefix", "address"))
      .mapPartitions(list => {
            list.map({
              case (diff, existingOpt) =>
                if (existingOpt.isEmpty) {
                  diff
                } else {
                  val existing = existingOpt.get
                  existing.copy(
                    noIncomingTxs = existing.noIncomingTxs + diff.noIncomingTxs,
                    noOutgoingTxs = existing.noOutgoingTxs + diff.noOutgoingTxs,
                    firstTx = if (diff.firstTx.height < existing.firstTx.height) diff.firstTx else existing.firstTx,
                    lastTx = if (diff.lastTx.height > existing.lastTx.height) diff.lastTx else existing.lastTx,
                    totalReceived = sumCurrency.apply(existing.totalReceived, diff.totalReceived),
                    totalSpent = sumCurrency.apply(existing.totalSpent, diff.totalSpent),
                    inDegree = existing.inDegree + diff.inDegree,
                    outDegree = existing.outDegree + diff.outDegree
                  )
                }
            })
      })
      .toDS()
      .persist(StorageLevel.DISK_ONLY)

    val addressRelationPairs = addressesDiff
      .withColumnRenamed(Fields.addressId, Fields.dstAddressId)
      .withColumn(Fields.dstAddressIdGroup, floor(col(Fields.dstAddressId).divide(lit(bucketSize))))
      .select(Fields.dstAddressIdGroup, Fields.dstAddressId)
      .rdd
      .joinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .on(SomeColumns("dst_address_id_group", "dst_address_id"))
      .mapPartitions(list => {
            list.map({
              case (row, relations) =>
                val dstAddressIdGroup = row.getLong(0)
                val dstAddressId = row.getInt(1)
                AddressPair(relations.srcAddressId, -1, dstAddressId, dstAddressIdGroup.intValue())
            })
      })
      .toDS()
      .withColumn(Fields.srcAddressIdGroup, floor(col(Fields.srcAddressId) / lit(bucketSize)).cast("int"))
      .as[AddressPair]

    println("Updating srcProperties field of address_incoming_relations...")
    val inRelationsUpdated = mergedAddresses
      .join(addressRelationPairs, joinExprs = col(Fields.addressId) equalTo col(Fields.srcAddressId))
      .as[AddressPairWithProperties]
      .rdd
      .joinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .on(SomeColumns("dst_address_id_group", "dst_address_id", "src_address_id"))
      .mapPartitions(list => {
            list.map({
              case (addressPair, relations) =>
                relations.copy(
                  srcProperties = AddressSummary(
                    totalReceived = addressPair.totalReceived,
                    totalSpent = addressPair.totalSpent
                  )
                )
            })
      }).toDS()
    cassandra.store[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations", inRelationsUpdated.sort(Fields.dstAddressId))

    println("Updating dstProperties field of address_outgoing_relations...")
    val outRelationsUpdated = mergedAddresses
      .join(addressRelationPairs, joinExprs = col(Fields.addressId) equalTo col(Fields.dstAddressId))
      .as[AddressPairWithProperties]
      .rdd
      .joinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
      .on(SomeColumns("src_address_id_group", "src_address_id", "dst_address_id"))
      .mapPartitions(list => {
            list.map({
              case (addressPair, relations) =>
                relations.copy(
                  dstProperties = AddressSummary(
                    totalReceived = addressPair.totalReceived,
                    totalSpent = addressPair.totalSpent
                  )
                )
            })
      }).toDS()
    cassandra.store[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations", outRelationsUpdated.sort(Fields.srcAddressId))

    cassandra.store(conf.targetKeyspace(), "address", mergedAddresses)

    spark.sparkContext.setJobDescription("Perform clustering")

    println("Computing address clusters")
    val addressClusterDiff = transformation
      .computeAddressCluster(regInputsDiff, addressIds, true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val addressClusterExtDiff = transformation.extendBasicClusterAddresses(addressClusterDiff)

    // computeBasicClusterAddresses will perform a join operation between basicAddressesDiff and addressClusterDiff.
    // Since basicAddressesDiff will not contain addresses that were not in any transaction since `lastProcessedBlock`,
    // that operation will not return any rows for these addresses.
    println("Computing basic cluster addresses")
    val basicClusterAddressesDiff =
      transformation
        .computeBasicClusterAddressesExt(basicAddressesDiff, addressClusterExtDiff)
        .persist()

    println("Retrieving overlapping clusters from cassandra")
    val cassandraClusters = basicClusterAddressesDiff
      .rdd
      .repartitionByCassandraReplica(conf.targetKeyspace(), "address_cluster")
      .leftJoinWithCassandraTable[AddressCluster](conf.targetKeyspace(), "address_cluster")
      .on(SomeColumns("address_id_group", "address_id"))
      .groupBy(r => r._1.cluster) // Group by cluster id that we have just clustered. Let's call it local cluster
      .map({
        case (localClusterId, tuples) =>
          val addressList = tuples
            .flatMap({
              case (localCluster, cassandraCluster) =>
                // these two clusters (local, i.e. in spark memory and cassandra, i.e. stored in cassandra)
                // definitely have a shared address since we just joined on address id in `joinWithCassandraTable`!
                val addresses = mutable.MutableList(
                  CassandraAddressCluster(
                    addressIdGroup = localCluster.addressIdGroup,
                    addressId = localCluster.addressId,
                    cluster = None,
                    clusterGroup = None
                  ))

                if (cassandraCluster.isDefined)
                  addresses += CassandraAddressCluster(
                    addressId = cassandraCluster.get.addressId,
                    addressIdGroup = cassandraCluster.get.addressIdGroup,
                    clusterGroup = Some(Math.floorDiv(cassandraCluster.get.cluster, bucketSize)),
                    cluster = Some(cassandraCluster.get.cluster)
                  )
                addresses
            })

          val localClusterGroup = Math.floorDiv(localClusterId, bucketSize)
          LocalClusterPart(
            clusterGroup = localClusterGroup,
            cluster = localClusterId,
            addressList = addressList.toSet
          )
      }).toDS()

    println("Merging ...")
    val mergers = cassandraClusters
      .flatMap(local => {
        local.addressList.map(r => {
          ClusterMerger(local.clusterGroup, local.cluster, r.clusterGroup.getOrElse(-1), r.cluster.getOrElse(-1), r.addressIdGroup, r.addressId)
        })
      })

    val mergeResults = mergers
      .rdd
      .leftJoinWithCassandraTable[ClusterAddresses](conf.targetKeyspace(), "cluster_addresses")
      .on(SomeColumns("cluster_group", "cluster"))
      .flatMap({
        case (merger, relatedAddress) =>
          val mergedWithCassandra = ClusterMergeResult(
            merger.localClusterGroup,
            merger.localCluster,
            merger.clusterGroup,
            merger.cluster,
            if (relatedAddress.isDefined) Some(Math.floorDiv(relatedAddress.get.addressId, bucketSize)) else None,
            if (relatedAddress.isDefined) Some(relatedAddress.get.addressId) else None )

          val withAddressFromLocalCluster = ClusterMergeResult(
            merger.localClusterGroup,
            merger.localCluster,
            merger.clusterGroup,
            merger.cluster,
            Some(merger.addressIdGroup),
            Some(merger.addressId)
          )
          Seq(mergedWithCassandra, withAddressFromLocalCluster)
      })
      .toDS()
      .dropDuplicates("localCluster", Fields.cluster, Fields.addressId)
      .persist(StorageLevel.MEMORY_AND_DISK)

    import org.apache.spark.sql.functions.udf
    val sumCurrenciesUDF = udf(sumUpCurrencies)

    val clusterMapping = mergeResults.select(Fields.cluster, "localCluster").filter(r => r.getInt(0) != -1).persist()

    println("Collecting INCOMING cluster relations of overlapping clusters...")

    // Collect clusters incoming to any of the contained(existing) clusters
    val containedClusterRelIn = mergeResults
      .select(col("localClusterGroup"), col("localCluster"), col(Fields.clusterGroup) as Fields.dstClusterGroup, col(Fields.cluster) as Fields.dstCluster)
      .as[ClusterMergeResultIn]
      .rdd
      .joinWithCassandraTable[ClusterIncomingRelations](conf.targetKeyspace(), "cluster_incoming_relations")
      .on(SomeColumns("dst_cluster_group", "dst_cluster"))
      .map({
        case (reclustered, relations) =>
          SimpleClusterRelations(
            srcClusterGroup = Math.floorDiv(relations.srcCluster, bucketSize),
            srcCluster = relations.srcCluster,
            dstClusterGroup = reclustered.dstClusterGroup,
            dstCluster = reclustered.dstCluster,
            noTransactions = relations.noTransactions,
            value = relations.value
          )
      })
      .joinWithCassandraTable[ClusterOutgoingRelations](conf.targetKeyspace(), "cluster_outgoing_relations")
      .on(SomeColumns("src_cluster_group", "src_cluster"))
      .map({
        case (incomingRelation, outgoingRelation) =>
          // incomingRelation and outgoingRelation in fact represent the same relation, but from perspecitives of two different clusters
          // This join with cassandra is performed to copy txList from out relation to in relation since in relations don't store txLists.
          incomingRelation.copy(
            txList = outgoingRelation.txList
          )
      }).toDS()
      .join(clusterMapping, joinExprs = col(Fields.srcCluster) equalTo(Fields.cluster), "left")
      .withColumn(Fields.srcCluster, when(col("localCluster").isNotNull, col("localCluster")).otherwise(col(Fields.srcCluster)))
      .withColumn(Fields.srcClusterGroup, floor(col(Fields.srcCluster) / lit(bucketSize)).cast("int"))
      .groupBy(Fields.srcCluster, Fields.dstCluster)
      .agg(
        first(Fields.srcClusterGroup) as Fields.srcClusterGroup,
        first(Fields.dstClusterGroup) as Fields.dstClusterGroup,
        sum(Fields.noTransactions).cast("int") as Fields.noTransactions,
        sumCurrenciesUDF(collect_list(Fields.value)) as Fields.value,
        array_distinct(flatten(collect_set(Fields.txList))) as Fields.txList)
      .as[SimpleClusterRelations]
      .persist()

    val combineTxLists = udf((lists: Seq[Array[Byte]]) => { lists.flatten })

    println("Collecting OUTGOING cluster relations of overlapping clusters...")
    // Collect relations outgoing from all the existing clusters. Then, add up noTransactions and value
    val containedClusterRelOut = mergeResults
      .select(col("localClusterGroup"), col("localCluster"), col(Fields.clusterGroup) as Fields.srcClusterGroup, col(Fields.cluster) as Fields.srcCluster)
      .as[ClusterMergeResultOut]
      .rdd
      .joinWithCassandraTable[ClusterOutgoingRelations](conf.targetKeyspace(), "cluster_outgoing_relations")
      .on(SomeColumns("src_cluster_group", "src_cluster"))
      .map({
        case (row, relations) =>
          relations.copy(
            srcClusterGroup = row.localClusterGroup,
            srcCluster = row.localCluster
          )
      }).toDS()
      .join(clusterMapping, joinExprs = col(Fields.dstCluster) equalTo(Fields.cluster), "left")
      .withColumn(Fields.dstCluster, when(col("localCluster").isNotNull, col("localCluster")).otherwise(col(Fields.dstCluster)))
      .withColumn(Fields.dstClusterGroup, floor(col(Fields.dstCluster) / lit(bucketSize)).cast("int"))
      .groupBy(Fields.srcCluster, Fields.dstCluster)
      .agg(
        first(Fields.srcClusterGroup) as Fields.srcClusterGroup,
        first(Fields.dstClusterGroup) as Fields.dstClusterGroup,
        sum(Fields.noTransactions).cast("int") as Fields.noTransactions,
        sumCurrenciesUDF(collect_list(Fields.value)) as Fields.value,
        combineTxLists(collect_set(col(Fields.txList))).cast(ArrayType(BinaryType)) as Fields.txList // todo do the same with incoming relations?
      )
      .as[SimpleClusterRelations]
      .persist()

    val mergedAddressClusters = mergeResults
      .groupByKey(r => r.localCluster)
      .flatMapGroups({
        case (clusterId, addresses) =>
          addresses
            .filter(r => r.addressId.isDefined)
            .map(r => AddressCluster(r.addressIdGroup.get, r.addressId.get, clusterId))
      })

    cassandra.store[AddressCluster](conf.targetKeyspace(), "address_cluster", mergedAddressClusters)
    val obsoleteClusters = mergeResults.filter(r => r.cluster != r.localCluster).map(r => (r.clusterGroup, r.cluster))

    println(" === Mapping of old clusters to clusters they had been merged to === ")
    clusterMapping.distinct().show(Integer.MAX_VALUE, false)

    println("Combining cluster merge results")
    val clusterSizes = mergeResults
      .groupBy("localCluster")
      .agg(countDistinct(col(Fields.addressId)).cast("int") as Fields.noAddresses)
      .select(col("localCluster") as Fields.cluster, col(Fields.noAddresses))


    println("Computing cluster transactions")
    val clusterTransactionsDiff =
      transformation
        .computeClusterTransactions(
          inputsDiff,
          outputsDiff,
          transactionsDiff,
          mergedAddressClusters
        )
        .persist()

    val (clusterInputs, clusterOutputs) =
      transformation.splitTransactions(clusterTransactionsDiff)
    clusterInputs.persist()
    clusterOutputs.persist()

    println("Computing cluster statistics")
    val basicClusterDiff =
      transformation
        .computeBasicCluster(
          transactionsDiff,
          basicClusterAddressesDiff.as[BasicClusterAddresses],
          clusterTransactionsDiff,
          clusterInputs,
          clusterOutputs,
          exchangeRates
        )
        .persist()

    println("Computing plain cluster relations for new blocks only")
    val plainClusterRelationsDiff =
      transformation
        .computePlainClusterRelations(
          clusterInputs,
          clusterOutputs
        )

    println("Computing cluster relations for new blocks only")
    val clusterRelationsDiff =
      transformation
        .computeClusterRelations(
          plainClusterRelationsDiff,
          basicClusterDiff,
          exchangeRates,
          spark.emptyDataset
        )
        .persist()

    println("Retrieving cluster properties for existing clusters")
    val relatedClusterProperties = mergeResults
      .dropDuplicates("localCluster", Fields.cluster)
      .rdd
      .joinWithCassandraTable[Cluster](conf.targetKeyspace(), "cluster")
      .on(SomeColumns("cluster_group", "cluster"))
      .map({
        case (mergeResult, existingCluster) =>
          PropertiesOfRelatedCluster(mergeResult.localClusterGroup, mergeResult.localCluster,
            existingCluster.copy(clusterGroup = mergeResult.clusterGroup, cluster = mergeResult.cluster)
          )
      })
      .toDS()
      .as[PropertiesOfRelatedCluster]
      .persist()

    val propSets = relatedClusterProperties
      .union(
        basicClusterDiff.map(localCluster => {
          val props = Cluster(
            clusterGroup = Math.floorDiv(localCluster.cluster, bucketSize),
            cluster = localCluster.cluster,
            noAddresses = localCluster.noAddresses,
            noIncomingTxs = localCluster.noIncomingTxs,
            noOutgoingTxs = localCluster.noOutgoingTxs,
            firstTx = localCluster.firstTx,
            lastTx = localCluster.lastTx,
            totalReceived = localCluster.totalReceived,
            totalSpent = localCluster.totalSpent,
            inDegree = 0,
            outDegree = 0
          )
          PropertiesOfRelatedCluster(props.clusterGroup, props.cluster, props)
        })
      )

    println("Coputing final cluster properties")
    val clusterPropsMerged = propSets
      .groupByKey(r => { r.cluster })
      .mapGroups({
        case (clusterId, existingClusters_) =>
          val existingClusters = existingClusters_.map(r => r.props).toList
          Cluster(
            clusterGroup = Math.floorDiv(clusterId, bucketSize),
            cluster = clusterId,
            noAddresses = existingClusters.map(r => r.noAddresses).sum,
            noIncomingTxs = existingClusters.map(r => r.noIncomingTxs).sum,
            noOutgoingTxs = existingClusters.map(r => r.noOutgoingTxs).sum,
            firstTx = existingClusters.map(r => r.firstTx).minBy(txId => txId.height),
            lastTx = existingClusters.map(r => r.lastTx).maxBy(txId => txId.height),
            totalReceived = existingClusters.map(r => r.totalReceived).reduce(sumCurrency),
            totalSpent = existingClusters.map(r => r.totalSpent).reduce(sumCurrency),
            inDegree = 0,
            outDegree = 0
          )
      })
      .drop(Fields.noAddresses)
      .join(clusterSizes, Fields.cluster)
      .as[Cluster]
      .persist()

    println("Combining cluster relations")
    val completeClusterRelations =
      transformation.computeClusterRelationsWithProperties(
        containedClusterRelIn,
        containedClusterRelOut,
        clusterRelationsDiff,
        clusterPropsMerged
      ).persist()


    val clusterInDegrees = completeClusterRelations
      .groupBy(Fields.dstCluster)
      .agg(countDistinct(Fields.srcCluster).cast("int") as Fields.inDegree)

    val clusterOutDegrees = completeClusterRelations
      .groupBy(Fields.srcCluster)
      .agg(countDistinct(Fields.dstCluster).cast("int") as Fields.outDegree)

    val clusterPropsWithDegrees = clusterPropsMerged
      .drop(Fields.inDegree, Fields.outDegree)
      .join(clusterOutDegrees, col(Fields.srcCluster) equalTo col(Fields.cluster), "left_outer")
      .join(clusterInDegrees, col(Fields.dstCluster) equalTo col(Fields.cluster), "left_outer")
      .na
      .fill(0)
      .as[Cluster]

    val clusterPropsWithTotalDegrees = clusterPropsWithDegrees
      .rdd
      .leftJoinWithCassandraTable[Cluster](conf.targetKeyspace(), "cluster")
      .on(SomeColumns("cluster_group", "cluster"))
      .map({
        case (cluster, maybeCluster) =>
          if (maybeCluster.isDefined)
            cluster.copy(
              inDegree = cluster.inDegree + maybeCluster.get.inDegree,
              outDegree = cluster.outDegree + maybeCluster.get.outDegree
            )
          else cluster
      }).toDS()


    cassandra.store[Cluster](conf.targetKeyspace(), "cluster", clusterPropsWithTotalDegrees)

    val clusterAddressesMerged = mergedAddressClusters
      .withColumn(Fields.clusterGroup, floor(col(Fields.cluster) / lit(bucketSize)).cast("int"))
      .join(mergedAddresses, Fields.addressId)
      .as[ClusterAddresses]
      /*.rdd
      .leftJoinWithCassandraTable[ClusterAddresses](conf.targetKeyspace(), "cluster_addresses")
      .on(SomeColumns("cluster_group", "cluster"))
      .map({
        case (diff, maybeAddresses) =>
          if (maybeAddresses.isDefined) {
            val existing = maybeAddresses.get
            existing.copy(
              noIncomingTxs = existing.noIncomingTxs + diff.noIncomingTxs,
              noOutgoingTxs = existing.noOutgoingTxs + diff.noOutgoingTxs,
              firstTx = if (existing.firstTx.height < diff.firstTx.height) existing.firstTx else diff.firstTx,
              lastTx = if (existing.lastTx.height > diff.lastTx.height) existing.lastTx else diff.lastTx,
              totalReceived = sumCurrency(existing.totalReceived, diff.totalReceived),
              totalSpent = sumCurrency(existing.totalSpent, diff.totalSpent),
              inDegree = existing.tot
            )
          }
      })*/

    cassandra.store(
      conf.targetKeyspace(),
      "cluster_addresses",
      clusterAddressesMerged
    )


    cassandra.store(
      conf.targetKeyspace(),
      "cluster_incoming_relations",
      completeClusterRelations.sort(Fields.dstClusterGroup, Fields.dstCluster, Fields.srcCluster)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_outgoing_relations",
      completeClusterRelations.sort(Fields.srcClusterGroup, Fields.srcCluster, Fields.dstCluster)
    )

    println("Retrieving cluster relations pairs")
    val clusterPairs = clusterPropsMerged
      .select(
        col(Fields.clusterGroup) as Fields.dstClusterGroup,
        col(Fields.cluster) as Fields.dstCluster
      )
      .rdd
      .joinWithCassandraTable[ClusterIncomingRelations](conf.targetKeyspace(), "cluster_incoming_relations")
      .on(SomeColumns("dst_cluster_group", "dst_cluster"))
      .map({
        case (row, relations) =>
          ClusterPair(
            srcCluster = relations.srcCluster,
            srcClusterGroup = Math.floorDiv(relations.srcCluster, bucketSize),
            dstCluster = row.getInt(1),
            dstClusterGroup = row.getInt(0)
          )
      }).toDS().persist()

    println("Filling in srcProperties of cluster relations")
    clusterPropsMerged
      .join(clusterPairs, joinExprs = col(Fields.cluster) equalTo col(Fields.srcCluster))
      .as[ClusterPropsMergerIn]
      .rdd
      .joinWithCassandraTable[ClusterIncomingRelations](conf.targetKeyspace(), "cluster_incoming_relations")
      .on(SomeColumns("dst_cluster_group", "dst_cluster", "src_cluster"))
      .map({
        case (stats, relations) =>
          relations.copy(srcProperties = ClusterSummary(stats.noAddresses, stats.totalReceived, stats.totalSpent))
      }).saveToCassandra(conf.targetKeyspace(), "cluster_incoming_relations")

    println("Filling in dstProperties of cluster relations")
    clusterPropsMerged
      .join(clusterPairs, joinExprs = col(Fields.cluster) equalTo col(Fields.dstCluster))
      .as[ClusterPropsMergerOut]
      .rdd
      .joinWithCassandraTable[ClusterOutgoingRelations](conf.targetKeyspace(), "cluster_outgoing_relations")
      .on(SomeColumns("src_cluster_group", "src_cluster", "dst_cluster"))
      .map({
        case (stats, relations) =>
          relations.copy(dstProperties = ClusterSummary(stats.noAddresses, stats.totalReceived, stats.totalSpent))
      }).saveToCassandra(conf.targetKeyspace(), "cluster_outgoing_relations")

    println("Deleting clusters that have been merged")
    obsoleteClusters
      .rdd
      .deleteFromCassandra(conf.targetKeyspace(), "cluster_addresses", keyColumns = SomeColumns("cluster_group", "cluster"))

    obsoleteClusters
      .rdd
      .deleteFromCassandra(conf.targetKeyspace(), "cluster", keyColumns = SomeColumns("cluster_group", "cluster"))
  }

  def verify(args: Array[String]): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val verifyTables = List(
      "address_by_id_group",
      "address_transactions",
      "address_incoming_relations",
      "address_outgoing_relations",
      "address",
      "address_cluster",
      "cluster",
      "cluster_addresses",
      "cluster_incoming_relations",
      "cluster_outgoing_relations"
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

    if (verifyTables contains "address_cluster") {
      verifyTable[AddressCluster]("address_cluster", Fields.addressId)
    }

    if (verifyTables contains "cluster_addresses")
      verifyTable[ClusterAddresses]("cluster_addresses", Fields.cluster)

    if (verifyTables contains "cluster") {
      verifyTable[Cluster]("cluster", Fields.cluster)
    }

    if (verifyTables contains "cluster_incoming_relations")
      verifyTable[ClusterIncomingRelations]("cluster_incoming_relations", Fields.dstCluster)

    if (verifyTables contains "cluster_outgoing_relations")
      verifyTable[ClusterOutgoingRelations]("cluster_outgoing_relations", Fields.srcCluster)
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

  /**
   * Restores some of the tables of btc_transformed_append to the state it was in btc_tranaformed10k
   * NOTE: Make sure you truncate the tables before running, this method does not delete rows!
   * @param args
   * @param fromKeyspace
   * @param toKeyspace
   */
  def restore(args: Array[String], fromKeyspace: String = "btc_transformed10k", toKeyspace: String = "btc_transformed_append"): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val tablesToRestore = List(
      "address_incoming_relations",
      "address_outgoing_relations",
      "address",
      "address_transactions",
      "address_cluster",
      "cluster_addresses",
      "cluster_incoming_relations",
      "cluster_outgoing_relations",
      "cluster"
    )


    def restoreTable[T <: Product: ClassTag: RowReaderFactory: RowWriterFactory: ValidRDDType: Encoder](table: String): Unit = {
      println(s"Restore: Writing table ${table} from keyspace ${fromKeyspace} to ${toKeyspace}")
      val addrInRelations = cassandra.load[T](fromKeyspace, table)
      cassandra.store[T](toKeyspace, table, addrInRelations.as[T])
    }


    if (tablesToRestore contains "address_incoming_relations")
      restoreTable[AddressIncomingRelations]("address_incoming_relations")

    if (tablesToRestore contains "address_outgoing_relations")
      restoreTable[AddressOutgoingRelations]("address_outgoing_relations")

    if (tablesToRestore contains "address")
      restoreTable[Address]("address")

    if (tablesToRestore contains "address_transactions")
      restoreTable[AddressTransactions]("address_transactions")

    if (tablesToRestore contains "cluster")
      restoreTable[Cluster]("cluster")

    if (tablesToRestore contains "address_cluster")
      restoreTable[AddressCluster]("address_cluster")

    if (tablesToRestore contains "cluster_addresses")
      restoreTable[ClusterAddresses]("cluster_addresses")

    if (tablesToRestore contains "cluster_incoming_relations")
      restoreTable[ClusterIncomingRelations]("cluster_incoming_relations")

    if (tablesToRestore contains "cluster_outgoing_relations")
      restoreTable[ClusterOutgoingRelations]("cluster_outgoing_relations")

  }

  def main(args: Array[String]) {
//    verify(args)
//    restore(args)
    compute(args)
  }
}
