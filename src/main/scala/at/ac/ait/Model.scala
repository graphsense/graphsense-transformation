package at.ac.ait

case class RegularInput(
    addressPrefix: String,
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int,
    coinJoin: Boolean
)

case class RegularOutput(
    addressPrefix: String,
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int,
    coinJoin: Boolean,
    n: Int
)

case class NormalizedAddress(id: Int, address: String)

case class InputIdSet(inputs: Seq[Int]) extends Iterable[Int] {
  override def iterator = inputs.iterator
}

// raw schema data types

case class TxInputOutput(address: Seq[String], value: Long, txType: Byte)

// transformed schema data types

case class TxSummary(
    txHash: Array[Byte],
    noInputs: Int,
    noOutputs: Int,
    totalInput: Long,
    totalOutput: Long
)

case class TxIdTime(height: Int, txHash: Array[Byte], timestamp: Int)

case class Currency(satoshi: Long, eur: Double, usd: Double)

case class AddressSummary(totalReceived: Long, totalSpent: Long)

case class ClusterSummary(
    noAddresses: Int,
    totalReceived: Long,
    totalSpent: Long
)

// raw schema tables

case class Block(
    height: Int,
    blockHash: Array[Byte],
    timestamp: Int,
    noTransactions: Int
)

case class Transaction(
    txPrefix: String,
    txHash: Array[Byte],
    height: Int,
    timestamp: Int,
    coinbase: Boolean,
    coinjoin: Boolean,
    totalInput: Long,
    totalOutput: Long,
    inputs: Seq[TxInputOutput],
    outputs: Seq[TxInputOutput],
    txIndex: Long
)

case class ExchangeRatesRaw(date: String, eur: Double, usd: Double)

case class Tag(
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
    category: String
)

case class SummaryStatisticsRaw(
    id: String,
    timestamp: Int,
    noBlocks: Int,
    noTxs: Long
)

// transformed schema tables

case class ExchangeRates(height: Int, eur: Double, usd: Double)

case class BlockTransactions(height: Int, txs: Seq[TxSummary])

case class AddressTransactions(
    addressPrefix: String,
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int
)

case class BasicAddress(
    addressPrefix: String,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)

case class Address(
    addressPrefix: String,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class AddressTags(
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String
)

case class AddressCluster(addressPrefix: String, address: String, cluster: Int)

case class ClusterAddresses(
    cluster: Int,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class ClusterTransactions(
    cluster: Int,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int
)

case class BasicCluster(
    cluster: Int,
    noAddresses: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)

case class BasicClusterAddresses(
    cluster: Int,
    address: String,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)

case class Cluster(
    cluster: Int,
    noAddresses: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class ClusterTags(
    cluster: Int,
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String
)

case class AddressRelations(
    srcAddressPrefix: String,
    dstAddressPrefix: String,
    srcAddress: String,
    dstAddress: String,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    noTransactions: Int,
    estimatedValue: Currency
)

case class PlainClusterRelations(
    txHash: Array[Byte],
    srcCluster: Int,
    dstCluster: Int,
    value: Long,
    height: Int
)

case class ClusterRelations(
    srcCluster: Int,
    dstCluster: Int,
    srcProperties: ClusterSummary,
    dstProperties: ClusterSummary,
    noTransactions: Int,
    value: Currency
)

case class SummaryStatistics(
    timestamp: Int,
    noBlocks: Int,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long,
    noClusters: Long,
    noTags: Long
)
