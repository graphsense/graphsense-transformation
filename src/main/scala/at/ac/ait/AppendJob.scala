package at.ac.ait

import at.ac.ait.clustering.MultipleInputClustering
import at.ac.ait.storage.CassandraStorage
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, count, countDistinct, first, floor, lit, lower, size, struct, sum}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
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

    val disableAddressTransactions = true
    val disableAddressIds = true
    val disableRelations = true

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")

    // todo uncomment
    /*val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")*/
    val tagsRaw = spark.emptyDataset[TagRaw]

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

    println("Extracting transaction inputs")
    val regInputs = transformation.computeRegularInputs(transactions).persist()
    val regInputsDiff = transformation.computeRegularInputs(transactionsDiff).persist()
    println("Extracting transaction outputs")
    val regOutputs = transformation.computeRegularOutputs(transactions)
    val regOutputsDiff = transformation.computeRegularOutputs(transactionsDiff).persist()

    println("Computing address IDs")
    val addressIds = transformation.computeAddressIds(regOutputs)

    //todo uncommment
    val addressByIdGroup = transformation.computeAddressByIdGroups(addressIds)
    if (!disableAddressIds) {
      cassandra.store(
        conf.targetKeyspace(),
        "address_by_id_group",
        addressByIdGroup
      )
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

    //todo uncommment
    if (!disableAddressTransactions) {
      cassandra.store(
        conf.targetKeyspace(),
        "address_transactions",
        addressTransactionsDiff
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

    if (!disableRelations) {

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

      val newAndUpdatedInRelations = addressRelationsDiff.as[AddressIncomingRelations]
        .rdd.leftJoinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
        .on(SomeColumns("src_address_id", "dst_address_id", "dst_address_id_group"))
        .map(r => {
          val existingOpt = r._2
          val diff = r._1
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

      val newAndUpdatedOutRelations = addressRelationsDiff.as[AddressOutgoingRelations]
        .rdd.leftJoinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
        .on(SomeColumns("src_address_id", "dst_address_id", "src_address_id_group"))
        .map(r => {
          val existingOpt = r._2
          val diff = r._1
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

      println("Updating address relations")
      cassandra.store[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations", newAndUpdatedInRelations.toDS())
      cassandra.store[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations", newAndUpdatedOutRelations.toDS())


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
        .map({
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
        .toDS()

      val addressRelationPairs = addressesDiff
        .withColumnRenamed(Fields.addressId, Fields.dstAddressId)
        .withColumn(Fields.dstAddressIdGroup, floor(col(Fields.dstAddressId).divide(lit(bucketSize))))
        .select(Fields.dstAddressIdGroup, Fields.dstAddressId)
        .rdd
        .joinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
        .on(SomeColumns("dst_address_id_group", "dst_address_id"))
        .map({
          case (row, relations) =>
            val dstAddressIdGroup = row.getLong(0)
            val dstAddressId = row.getInt(1)
            AddressPair(relations.srcAddressId, -1, dstAddressId, dstAddressIdGroup.intValue())
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
        .map({
          case (addressPair, relations) =>
            relations.copy(
              srcProperties = AddressSummary(
                totalReceived = addressPair.totalReceived,
                totalSpent = addressPair.totalSpent
              )
            )
        }).toDS()
      cassandra.store[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations", inRelationsUpdated)

      println("Updating dstProperties field of address_outgoing_relations...")
      val outRelationsUpdated = mergedAddresses
        .join(addressRelationPairs, joinExprs = col(Fields.addressId) equalTo col(Fields.dstAddressId))
        .as[AddressPairWithProperties]
        .rdd
        .joinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
        .on(SomeColumns("src_address_id_group", "src_address_id", "dst_address_id"))
        .map({
          case (addressPair, relations) =>
            relations.copy(
              dstProperties = AddressSummary(
                totalReceived = addressPair.totalReceived,
                totalSpent = addressPair.totalSpent
              )
            )
        }).toDS()
      cassandra.store[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations", outRelationsUpdated)

      cassandra.store(conf.targetKeyspace(), "address", mergedAddresses)
    }


    spark.sparkContext.setJobDescription("Perform clustering")

    println("Computing address clusters")
    val addressClusterDiff = transformation
      .computeAddressCluster(regInputsDiff, addressIds, true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("addressClusterDiff for address 10161: ")
    addressClusterDiff.filter(r => r.addressId == 10161).show(100, false)

    val addressClusterExtDiff = transformation.extendBasicClusterAddresses(addressClusterDiff)
    println("addressClusterExtDiff for address 10161: ")
    addressClusterExtDiff.filter(r => r.addressId == 10161).show(100, false)


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
      .persist(StorageLevel.MEMORY_AND_DISK)
    println("Merge results cluster 744: ")
    mergeResults.filter(r => r.localCluster == 744).show(1000, false)

    val sumCurrency: ((Currency, Currency) => Currency) = { case(curr1, curr2) => Currency(curr1.value + curr2.value, curr1.eur + curr2.eur, curr1.usd + curr1.usd) }
    val sumUpCurrencies: (Seq[Currency]) => Currency = {
      case list =>
        list.reduce(sumCurrency)
    }
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
        case (row, relations) =>
          relations.copy(
            dstClusterGroup = row.localClusterGroup,
            dstCluster = row.localCluster
          )
      }).toDS()
      .join(clusterMapping, joinExprs = col(Fields.srcCluster) equalTo(Fields.cluster))
      .drop(Fields.srcCluster)
      .withColumnRenamed("localCluster", Fields.srcCluster)
      .withColumn(Fields.srcClusterGroup, floor(col(Fields.srcCluster) / lit(bucketSize)).cast("int"))
      .groupBy(Fields.srcCluster, Fields.dstCluster)
      .agg(
        first(Fields.srcClusterGroup) as Fields.srcClusterGroup,
        first(Fields.dstClusterGroup) as Fields.dstClusterGroup,
        sum(Fields.noTransactions).cast("int") as Fields.noTransactions,
        sumCurrenciesUDF(collect_list(Fields.value)) as Fields.value)
      .as[SimpleClusterRelations]
      .persist()

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
      .join(clusterMapping, joinExprs = col(Fields.dstCluster) equalTo(Fields.cluster))
      .drop(Fields.dstCluster)
      .withColumnRenamed("localCluster", Fields.dstCluster)
      .withColumn(Fields.dstClusterGroup, floor(col(Fields.dstCluster) / lit(bucketSize)).cast("int"))
      .groupBy(Fields.srcCluster, Fields.dstCluster)
      .agg(
        first(Fields.srcClusterGroup) as Fields.srcClusterGroup,
        first(Fields.dstClusterGroup) as Fields.dstClusterGroup,
        sum(Fields.noTransactions).cast("int") as Fields.noTransactions,
        sumCurrenciesUDF(collect_list(Fields.value)) as Fields.value)
      .as[SimpleClusterRelations]
      .persist()

    containedClusterRelIn.show(100, false)
    containedClusterRelOut.show(100, false)
    println("contained clusters relations in for cluster 744: ")
    containedClusterRelIn.filter(r => r.dstCluster == 744).show(100, false)

    println("contained clusters relations out for cluster 744: ")
    containedClusterRelOut.filter(r => r.srcCluster == 744).show(100, false)

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
    obsoleteClusters
      .rdd
      .deleteFromCassandra(conf.targetKeyspace(), "cluster_addresses", keyColumns = SomeColumns("cluster_group", "cluster"))

    println(" === Mapping of old clusters to clusters they had been merged to === ")
    clusterMapping.distinct().show(Integer.MAX_VALUE, false)

    println("Combining cluster merge results")
    val clusterSizes = mergeResults
      .groupBy("localCluster")
      .agg(countDistinct(col(Fields.addressId)).cast("int") as Fields.noAddresses)
      .select(col("localCluster") as Fields.cluster, col(Fields.noAddresses))



    println("inputsDiff for addresses in cluster 9166: ")
    inputsDiff.join(mergedAddressClusters, Fields.addressId)
      .filter(col(Fields.cluster) equalTo lit(9166))
      .show(100, false)

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

    clusterTransactionsDiff.filter(r => r.cluster == 9166).show(100, false)

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

    println("Related cluster props for 9166: ")
    relatedClusterProperties.filter(r => r.cluster == 9166).show(100, false)

    println("Deleting clusters that have been merged")
    obsoleteClusters
      .rdd
      .deleteFromCassandra(conf.targetKeyspace(), "cluster", keyColumns = SomeColumns("cluster_group", "cluster"))


    /*val upToDateClusterProps =
      mergeResults
        .dropDuplicates("localCluster")
        .drop(Fields.cluster)
        .withColumnRenamed("localCluster", Fields.cluster)
        .join(basicClusterDiff, usingColumn=Fields.cluster)
        .as[BasicCluster]
        .persist()*/



    /*println("existingClusterProps cluster 10146: ")
    upToDateClusterProps.filter(r => r.cluster == 10146).show(100, false)

    println("upToDateClusterProps cluster 10146: ")
    upToDateClusterProps.filter(r => r.cluster == 10146).show(100, false)
*/
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

    val addressToClusterRelations = mergedAddressClusters
      .withColumnRenamed(Fields.addressIdGroup, Fields.dstAddressIdGroup)
      .withColumnRenamed(Fields.addressId, Fields.dstAddressId)
      .as[MergedClusterRelationsIn]
      .rdd
      .joinWithCassandraTable[AddressIncomingRelations](conf.targetKeyspace(), "address_incoming_relations")
      .on(SomeColumns("dst_address_id_group", "dst_address_id"))
      .map({
        case (row, relations) =>
          AddressToClusterRelation(
            addressIdGroup = Math.floorDiv(relations.srcAddressId, bucketSize),
            addressId = relations.srcAddressId,
            clusterGroup = Math.floorDiv(row.cluster, bucketSize),
            cluster = row.cluster,
            addressProperties = relations.srcProperties,
            noTransactions = relations.noTransactions,
            value = relations.estimatedValue,
            srcLabels = relations.srcLabels
          )
      }).toDS()


    val clusterToAddressRelations = mergedAddressClusters
      .withColumnRenamed(Fields.addressId, Fields.srcAddressId)
      .withColumnRenamed(Fields.addressIdGroup, Fields.srcAddressIdGroup)
      .as[MergedClusterRelationsOut]
      .rdd
      .joinWithCassandraTable[AddressOutgoingRelations](conf.targetKeyspace(), "address_outgoing_relations")
      .on(SomeColumns("src_address_id_group", "src_address_id"))
      .map({
        case (row, relations) =>
          ClusterToAddressRelation(
            addressIdGroup = Math.floorDiv(relations.srcAddressId, bucketSize),
            addressId = relations.srcAddressId,
            clusterGroup = Math.floorDiv(row.cluster, bucketSize),
            cluster = row.cluster,
            addressProperties = relations.dstProperties,
            noTransactions = relations.noTransactions,
            value = relations.estimatedValue,
            txList = relations.txList,
            dstLabels = relations.dstLabels
          )
      }).toDS()


    println("Cluster 9166 in relations: ")
    addressToClusterRelations.filter(r => r.cluster == 9166).show()

    println("Cluster 9166 out relations: ")
    clusterToAddressRelations.filter(r => r.cluster == 9166).show()

    val clusterRelations1 = addressToClusterRelations
      .as("rel")
      .joinWith(mergedAddressClusters.as("address"), col("rel.addressId") equalTo col("address.addressId"))
      .groupByKey(r => (r._1.cluster, r._2.cluster))
      .mapGroups({
        case ((dstCluster, srcCluster), srcObjects) =>
          val sources = srcObjects.toList
          ClusterRelations(
            srcClusterGroup = -1,
            srcCluster = srcCluster,
            dstClusterGroup = -1,
            dstCluster = dstCluster,
            srcProperties = ClusterSummary(0, Currency(0,0,0), Currency(0, 0, 0)), // Fill in later
            dstProperties = ClusterSummary(0, Currency(0,0,0), Currency(0, 0, 0)), // Fill in later
            srcLabels = Seq(),
            dstLabels = Seq(),
            noTransactions = sources.map(r => r._1.noTransactions).sum,
            value = sources.map(r => r._1.value).reduce(sumCurrency),
            txList = Seq()
          )
      })

    val clusterRelations2 = clusterToAddressRelations
      .as("rel")
      .joinWith(mergedAddressClusters.as("address"), col("rel.addressId") equalTo col("address.addressId"))
      .groupByKey(r => (r._1.cluster, r._2.cluster))
      .mapGroups({
        case ((srcCluster, dstCluster), srcObjects) =>
          val sources = srcObjects.toList
          ClusterRelations(
            srcClusterGroup = -1,
            srcCluster = srcCluster,
            dstClusterGroup = -1,
            dstCluster = dstCluster,
            srcProperties = ClusterSummary(0, Currency(0,0,0), Currency(0, 0, 0)), // Fill in later
            dstProperties = ClusterSummary(0, Currency(0,0,0), Currency(0, 0, 0)), // Fill in later
            srcLabels = Seq(),
            dstLabels = Seq(),
            noTransactions = sources.map(r => r._1.noTransactions).sum,
            value = sources.map(r => r._1.value).reduce(sumCurrency),
            txList = Seq()
          )
      })


    println("Filling in srcProperties and dstProperties of cluster relations")
    val allClusterRelations = clusterRelations1.union(clusterRelations2)
      .dropDuplicates(Fields.srcCluster, Fields.dstCluster)
      .withColumn(Fields.srcClusterGroup, floor(col(Fields.srcCluster) / lit(bucketSize)).cast("int"))
      .withColumn(Fields.dstClusterGroup, floor(col(Fields.dstCluster) / lit(bucketSize)).cast("int"))
      .as[ClusterRelations]
      .filter(col(Fields.srcCluster) notEqual col(Fields.dstCluster))
      .joinWith(clusterPropsMerged, col(Fields.cluster) equalTo col(Fields.srcCluster))
      .map({
        case (relations, cluster) =>
          relations.copy(
            srcProperties = ClusterSummary(cluster.noAddresses, cluster.totalReceived, cluster.totalSpent)
          )
      })
      .joinWith(clusterPropsMerged, col(Fields.cluster) equalTo col(Fields.dstCluster))
      .map({
        case (relations, cluster) =>
          relations.copy(
            dstProperties = ClusterSummary(cluster.noAddresses, cluster.totalReceived, cluster.totalSpent)
          )
      })


    val clusterInDegrees = allClusterRelations
      .groupBy(Fields.dstCluster)
      .agg(countDistinct(Fields.srcCluster).cast("int") as Fields.inDegree)

    val clusterOutDegrees = allClusterRelations
      .groupBy(Fields.srcCluster)
      .agg(countDistinct(Fields.dstCluster).cast("int") as Fields.outDegree)

    val clusterPropsWithDegrees = clusterPropsMerged
      .drop(Fields.inDegree, Fields.outDegree)
      .join(clusterOutDegrees, col(Fields.srcCluster) equalTo col(Fields.cluster), "left_outer")
      .join(clusterInDegrees, col(Fields.dstCluster) equalTo col(Fields.cluster), "left_outer")
      .na
      .fill(0)
      .as[Cluster]
      .persist()

    cassandra.store[Cluster](conf.targetKeyspace(), "cluster", clusterPropsWithDegrees)

    cassandra.store(
      conf.targetKeyspace(),
      "cluster_incoming_relations",
      allClusterRelations.sort(Fields.dstClusterGroup, Fields.dstCluster, Fields.srcCluster)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_outgoing_relations",
      allClusterRelations.sort(Fields.srcClusterGroup, Fields.srcCluster, Fields.dstCluster)
    )


  }

  def verify(args: Array[String]): Unit = {
    val (cassandra, conf, spark) = setupCassandra(args)
    import spark.implicits._

    val verifyTables = List(
//      "address_by_id_group",
//      "address_transactions",
//      "address_incoming_relations",
//      "address_outgoing_relations",
//      "address",
      "address_cluster",
      "cluster",
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

    if (verifyTables contains "cluster") {
      verifyTable[Cluster]("cluster", Fields.cluster)
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
//      "address_incoming_relations",
//      "address_outgoing_relations",
//      "address",
//      "address_transactions",
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
    restore(args)
    compute(args)
  }
}
