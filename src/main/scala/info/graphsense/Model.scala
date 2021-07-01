package info.graphsense

import java.sql.Date

case class RegularInput(
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int,
    coinJoin: Boolean
)

case class RegularOutput(
    address: String,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
    timestamp: Int,
    coinJoin: Boolean,
    n: Int
)

case class AddressId(
    address: String,
    addressId: Int
)

case class AddressIdByAddress(
    addressPrefix: String,
    address: String,
    addressId: Int
)

case class AddressByIdGroup(
    addressIdGroup: Int,
    addressId: Int,
    address: String
)

case class InputIdSet(inputs: Seq[Int]) extends Iterable[Int] {
  override def iterator = inputs.iterator
}

// raw schema data types

case class TxInputOutput(address: Seq[String], value: Long, addressType: Byte)

// transformed schema data types

case class TxSummary(
    txHash: Array[Byte],
    noInputs: Int,
    noOutputs: Int,
    totalInput: Long,
    totalOutput: Long
)

case class TxIdTime(height: Int, txIndex: Long, timestamp: Int)

case class Currency(value: Long, fiatValues: Seq[Float])

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

case class BlockTransactions(height: Int, txs: Seq[TxSummary])

case class ExchangeRatesRaw(
    date: String,
    fiatValues: Option[Map[String, Float]]
  )

case class AddressTagRaw(
    address: String,
    currency: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Date,
    category: Option[String],
    abuse: Option[String]
)

case class ClusterTagRaw(
    entity: Int,
    currency: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Date,
    category: Option[String],
    abuse: Option[String]
)

case class SummaryStatisticsRaw(
    id: String,
    timestamp: Int,
    noBlocks: Int,
    noTxs: Long
)

// transformed schema tables

case class ExchangeRates(height: Int, fiatValues: Seq[Float])

case class AddressTransaction(
    addressIdGroup: Int,
    addressId: Int,
    txIndex: Long,
    value: Long,
    height: Int,
    timestamp: Int
)

case class BasicAddress(
    addressId: Int,
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
    addressId: Int,
    cluster: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class AddressTag(
    addressIdGroup: Int,
    addressId: Int,
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class AddressTagByLabel(
    labelNormPrefix: String,
    labelNorm: String,
    label: String,
    address: String,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
    category: Option[String],
    abuse: Option[String],
    active: Boolean
)

case class AddressCluster(addressId: Int, cluster: Int)

case class ClusterAddress(
    clusterGroup: Int,
    cluster: Int,
    addressId: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class ClusterTransaction(
    cluster: Int,
    txIndex: Long,
    value: Long,
    height: Int,
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

case class BasicClusterAddress(
    cluster: Int,
    addressId: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)

case class Cluster(
    clusterGroup: Int,
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

case class ClusterTag(
    clusterGroup: Int,
    cluster: Int,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class ClusterAddressTag(
    clusterGroup: Int,
    cluster: Int,
    addressId: Int,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class ClusterTagByLabel(
    labelNormPrefix: String,
    labelNorm: String,
    label: String,
    cluster: Int,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
    category: Option[String],
    abuse: Option[String],
    active: Boolean
)

case class PlainAddressRelation(
    txIndex: Long,
    srcAddressId: Int,
    dstAddressId: Int,
    height: Int,
    estimatedValue: Long
)

case class AddressRelation(
    srcAddressIdGroup: Int,
    srcAddressId: Int,
    dstAddressIdGroup: Int,
    dstAddressId: Int,
    hasSrcLabels: Boolean,
    hasDstLabels: Boolean,
    noTransactions: Int,
    estimatedValue: Currency,
    txList: Seq[Long]
)

case class PlainClusterRelation(
    txIndex: Long,
    srcCluster: Int,
    dstCluster: Int,
    value: Long,
    height: Int
)

case class ClusterRelation(
    srcClusterGroup: Int,
    srcCluster: Int,
    dstClusterGroup: Int,
    dstCluster: Int,
    hasSrcLabels: Boolean,
    hasDstLabels: Boolean,
    noTransactions: Int,
    value: Currency,
    txList: Seq[Long]
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

case class Configuration(
    keyspaceName: String,
    bucketSize: Int,
    bech32Prefix: String,
    coinjoinFiltering: Boolean,
    fiatCurrencies: Seq[String]
)