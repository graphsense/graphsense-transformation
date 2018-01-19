package at.ac.ait

object Currency extends Enumeration {
  type Currency = Value
  val EUR, USD = Value
}

case class NormalizedAddress(addrId: Int, address: String)

case class TotalInput(txHash: Array[Byte], totalInput: Long)
case class KnownAddress(address: String, category: Int)

case class RegularInput(
    txHash: Array[Byte],
    txNumber: Int,
    value: Long,
    address: String)

case class RegularOutput(
    txHash: Array[Byte],
    txNumber: Int,
    value: Long,
    n: Int,
    address: String)

// RAW SCHEMA DATA TYPES

case class RawInput(
    txid: Array[Byte],
    vout: Int)

case class RawOutput(
    value: Long,
    n: Int,
    addresses: Seq[String])

// RAW SCHEMA TABLES

case class RawBlock(
    height: Int,
    blockHash: Array[Byte],
    timestamp: Int,
    blockVersion: Int,
    size: Int,
    txs: Seq[Array[Byte]])

case class RawTransaction(
    height: Int,
    txNumber: Int,
    txHash: Array[Byte],
    timestamp: Int,
    coinbase: Boolean,
    vin: Seq[RawInput],
    vout: Seq[RawOutput])

case class RawExchangeRates(
    timestamp: Int,
    eur: Option[Double],
    usd: Option[Double])

case class RawTag(
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
    address: String,
    value: Long)

case class TxIdTime(
    height: Int,
    txHash: Array[Byte],
    timestamp: Int)

case class Bitcoin(
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

// TRANSFORMED SCHEMA TABLES

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
    outputs: Seq[TxInputOutput])

case class BlockTransactions(
    height: Int,
    txs: Seq[TxSummary])

case class AddressTransactions(
    addressPrefix: String,
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txNumber: Int,
    timestamp: Int)

case class Address(
    addressPrefix: String,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Bitcoin,
    totalSpent: Bitcoin)

case class AddressCluster(
    addressPrefix: String,
    address: String,
    cluster: Int)

case class ClusterAddresses(
    cluster: Int,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Bitcoin,
    totalSpent: Bitcoin)

case class ClusterTransactions(
    cluster: Int,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txNumber: Int,
    timestamp: Int)

case class Cluster(
    cluster: Int,
    noAddresses: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Bitcoin,
    totalSpent: Bitcoin)

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
    estimatedValue: Bitcoin)

case class ClusterRelations(
    srcCluster: String,
    dstCluster: String,
    srcCategory: Int,
    dstCategory: Int,
    srcProperties: ClusterSummary,
    dstProperties: ClusterSummary,
    noTransactions: Int,
    value: Bitcoin)
