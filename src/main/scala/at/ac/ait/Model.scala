package at.ac.ait

case class NormalizedAddress(id: Long, address: String)
case class InputIdSet(inputs: Seq[Long]) extends Iterable[Long] {
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
    cluster: Long,
    address: String,
    tag: String,
    tagUri: String,
    description: String,
    actorCategory: String,
    source: String,
    sourceUri: String,
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
    cluster: Long)

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
    cluster: Long,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int)

case class Cluster(
    cluster: Long,
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

case class ClusterRelations(
    srcCluster: String,
    dstCluster: String,
    srcCategory: Int,
    dstCategory: Int,
    srcProperties: ClusterSummary,
    dstProperties: ClusterSummary,
    noTransactions: Int,
    value: Currency)
