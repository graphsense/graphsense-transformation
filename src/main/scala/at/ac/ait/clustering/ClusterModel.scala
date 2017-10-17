package at.ac.ait.clustering

import at.ac.ait._

// NON-PERSISTENT TYPE DEFINTIONS

case class TransactionInputs(
    txHash: Array[Byte],
    inputs: Seq[TxInputOutput])

case class InputRelation(
    txId: Long,
    addrId: Long)

case class ClusteringResult(
    addrId: Long,
    reprAddrId: Long)

// PERSISTENT TYPE DEFINTIONS

case class AddressCluster(
    addressPrefix: String,
    address: String,
    cluster: Long)

case class Cluster(
    cluster: Long,
    noAddresses: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Value,
    totalSpent: Value)

case class ClusterAddresses(
    cluster: Long,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Value,
    totalSpent: Value)

case class ClusterTags(
    cluster: Long,
    address: String,
    tag: String,
    tagUri: String,
    description: String,
    actorCategory: String,
    source: String,
    sourceUri: String,
    timestamp: Int)

case class ClusterRelations(
    srcCluster: Long,
    dstCluster: Long,
    srcCategory: Int,
    dstCategory: Int,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    noTransactions: Int,
    estimatedValue: Value)
