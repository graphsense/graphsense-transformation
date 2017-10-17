package at.ac.ait

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
    txHash: Array[Byte],
    height: Int,
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

// TRANSFORMED SCHEMA DATA TYPES

case class TxSummary(
    txHash: Array[Byte],
    noInputs: Int,
    noOutputs: Int,
    totalInput: Long,
    totalOutput: Long)

case class TxInputOutput(
    address: Option[String],
    value: Option[Long])

case class TxIdTime(
    height: Int,
    txHash: Array[Byte],
    timestamp: Int)

case class Value(
    satoshi: Long,
    eur: Double,
    usd: Double)

case class AddressSummary(
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
    value: Option[Long],
    height: Int,
    timestamp: Int)

case class Address(
    addressPrefix: String,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Value,
    totalSpent: Value)

case class AddressOutgoingRelations(
    srcAddressPrefix: String,
    srcAddress: String,
    dstAddress: String,
    dstCategory: Int,
    dstProperties: AddressSummary,
    noTransactions: Int,
    estimatedValue: Value)

case class AddressIncomingRelations(
    dstAddressPrefix: String,
    dstAddress: String,
    srcAddress: String,
    srcCategory: Int,
    srcProperties: AddressSummary,
    noTransactions: Int,
    estimatedValue: Value)
