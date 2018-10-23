package at.ac.ait

case class NormalizedAddress(id: Int, address: String)
case class InputIdSet(inputs: Seq[Int]) extends Iterable[Int] {
    override def iterator = inputs.iterator
}

case class TotalInput(txHash: Array[Byte], totalInput: Long)
case class KnownAddress(address: String, category: Int)

case class Tag(
    address: String,
    tag: String,
    tagUri: String,
    description: String,
    actorCategory: String,
    source: String,
    sourceUri: String,
    timestamp: Int)

case class ClusterTags(
    cluster: Int,
    address: String,
    tag: String,
    tagUri: String,
    description: String,
    actorCategory: String,
    source: String,
    sourceUri: String,
    timestamp: Int)

case class RegularInput(
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int)

case class RegularOutput(
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    n: Int,
    timestamp: Int)

// TRANSFORMED SCHEMA DATA TYPES

case class TxSummary(
    txHash: Array[Byte],
    noInputs: Int,
    noOutputs: Int,
    totalInput: Long,
    totalOutput: Long)

case class TxInputOutput(
    address: Seq[String],
    value: Long,
    txType: Byte)

case class TxIdTime(
    height: Int,
    txHash: Array[Byte],
    timestamp: Int)

case class Currency(
    satoshi: Long,
    eur: Double,
    usd: Double)

case class AddressSummary(
    totalReceived: Long,
    totalSpent: Long)

case class ClusterSummary(
    noAddresses: Int,
    totalReceived: Long,
    totalSpent: Long)

// SCHEMA TABLES

case class ExchangeRates(
    height: Int,
    eur: Double,
    usd: Double)

case class Block(
    height: Int,
    blockHash: Array[Byte],
    timestamp: Int,
    noTransactions: Int)

case class Transaction(
    txPrefix: String,
    txHash: Array[Byte],
    height: Int,
    timestamp: Int,
    coinbase: Boolean,
    totalInput: Long,
    totalOutput: Long,
    inputs: Seq[TxInputOutput],
    outputs: Seq[TxInputOutput],
    txIndex: Long)

case class BlockTransactions(
    height: Int,
    txs: Seq[TxSummary])

case class AddressTransactions(
    addressPrefix: String,
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int)

case class Address(
    addressPrefix: String,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency)

case class AddressCluster(
    addressPrefix: String,
    address: String,
    cluster: Int)

case class ClusterAddresses(
    cluster: Long,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency)

case class ClusterTransactions(
    cluster: Int,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int)

case class Cluster(
    cluster: Int,
    noAddresses: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency)

case class AddressRelations(
    srcAddressPrefix: String,
    dstAddressPrefix: String,
    srcAddress: String,
    dstAddress: String,
    srcCategory: Int,
    dstCategory: Int,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    noTransactions: Int,
    estimatedValue: Currency)

case class SimpleClusterRelations(
    txHash: Array[Byte],
    srcCluster: String,
    dstCluster: String,
    value: Long,
    height: Int)

case class ClusterRelations(
    srcCluster: String,
    dstCluster: String,
    srcCategory: Int,
    dstCategory: Int,
    srcProperties: ClusterSummary,
    dstProperties: ClusterSummary,
    noTransactions: Int,
    value: Currency)
