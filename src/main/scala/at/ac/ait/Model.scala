package at.ac.ait

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

case class TxIdTime(height: Int, txHash: Array[Byte], timestamp: Int)

case class Currency(value: Long, eur: Float, usd: Float)

case class AddressSummary(totalReceived: Currency, totalSpent: Currency)

case class AddressPair(
    srcAddressId: Int,
    srcAddressIdGroup: Int,
    dstAddressId: Int,
    dstAddressIdGroup: Int)

case class AddressPairWithProperties(
    srcAddressId: Int,
    srcAddressIdGroup: Int,
    dstAddressId: Int,
    dstAddressIdGroup: Int,
    totalReceived: Currency,
    totalSpent: Currency)

case class ReclusteredAddress(
   addressIdGroup: Int,
   addressId: Int,
   cluster: Int, // newly calculated cluster
   clusterGroup: Int,
   clusterObj: BasicClusterAddressesExt,
   existingCluster: AddressCluster
)

/**
 * Describes an address and a cluster it belongs to, according to Cassandra
 * @param addressIdGroup
 * @param addressId
 * @param clusterGroup
 * @param cluster
 */
case class CassandraAddressCluster(
   addressIdGroup: Int,
   addressId: Int,
   clusterGroup: Int,
   cluster: Int
)

case class ClusterPart(
                      clusterGroup: Int,
                      cluster: Int,
                      addressList: Set[CassandraAddressCluster]
                      )


case class AddressProperties(
    addressIdGroup: Int,
    addressId: Int,
    address: String,
    addressPrefix: String,
    totalReceived: Currency,
    totalSpent: Currency
)

case class ClusterSummary(
    noAddresses: Int,
    totalReceived: Currency,
    totalSpent: Currency
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

case class BlockTransactions(height: Int, txs: Seq[TxSummary])

case class ExchangeRatesRaw(date: String, eur: Float, usd: Float)

case class TagRaw(
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
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

case class ExchangeRates(height: Int, eur: Float, usd: Float)

case class AddressTransactions(
    addressIdGroup: Int,
    addressId: Int,
    txHash: Array[Byte],
    value: Long,
    height: Int,
    txIndex: Long,
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
    addressId: Int,
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class AddressCluster(addressIdGroup: Int, addressId: Int, cluster: Int)

case class ClusterAddresses(
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
    addressId: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)

case class BasicClusterAddressesExt(
    clusterGroup: Int,
    cluster: Int,
    addressId: Int,
    addressIdGroup: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency
)


case class AddressClusterExt(
    addressIdGroup: Int, addressId: Int, clusterGroup: Int, cluster: Int
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

case class ClusterTags(
    clusterGroup: Int,
    cluster: Int,
    addressId: Int,
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class PlainAddressRelations(
    txHash: Array[Byte],
    srcAddressId: Int,
    dstAddressId: Int,
    height: Int,
    estimatedValue: Long
)

case class AddressRelations(
    srcAddressIdGroup: Int,
    srcAddressId: Int,
    dstAddressIdGroup: Int,
    dstAddressId: Int,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    srcLabels: Seq[String],
    dstLabels: Seq[String],
    noTransactions: Int,
    estimatedValue: Currency,
    txList: Seq[Array[Byte]]
)

case class AddressIncomingRelations(
    dstAddressIdGroup: Int,
    dstAddressId: Int,
    srcAddressId: Int,
    estimatedValue: Currency,
    noTransactions: Int,
    srcLabels: Seq[String],
    srcProperties: AddressSummary
)

case class AddressOutgoingRelations(
    srcAddressIdGroup: Int,
    dstAddressId: Int,
    srcAddressId: Int,
    estimatedValue: Currency,
    noTransactions: Int,
    dstLabels: Seq[String],
    dstProperties: AddressSummary,
    txList: Seq[Array[Byte]]
)

case class PlainClusterRelations(
    txHash: Array[Byte],
    srcCluster: Int,
    dstCluster: Int,
    value: Long,
    height: Int
)

case class ClusterRelations(
    srcClusterGroup: Int,
    srcCluster: Int,
    dstClusterGroup: Int,
    dstCluster: Int,
    srcProperties: ClusterSummary,
    dstProperties: ClusterSummary,
    srcLabels: Seq[String],
    dstLabels: Seq[String],
    noTransactions: Int,
    value: Currency,
    txList: Seq[Array[Byte]]
)

case class Tag(
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
    activeAddress: Boolean
)

case class SummaryStatistics(
    timestamp: Int,
    noBlocks: Int,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long,
    noClusters: Long,
    noTags: Long,
    bucketSize: Int
)
