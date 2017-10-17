package at.ac.ait

object BasicTables {

  val rawKeyspace = "graphsense_raw"

  val rawBlockTable = "block"
  val rawTransactionTable = "transaction"
  val rawExchangeRatesTable = "exchange_rates"
  val rawTagTable = "tag"

  val transformedKeyspace = "graphsense_transformed"

  val exchangeRates = "exchange_rates"
  val block = "block"
  val blockTransactions = "block_transactions"
  val transaction = "transaction"
  val addressTransactions = "address_transactions"
  val address = "address"
  val addressOutgoingRelations = "address_outgoing_relations"
  val addressIncomingRelations = "address_incoming_relations"

  val clusterOutgoingRelations = "cluster_outgoing_relations"
  val clusterIncomingRelations = "cluster_incoming_relations"

}
