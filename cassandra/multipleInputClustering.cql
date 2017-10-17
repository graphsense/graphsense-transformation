USE graphsense_transformed;

DROP TABLE IF EXISTS address_cluster;
DROP TABLE IF EXISTS cluster;
DROP TABLE IF EXISTS cluster_addresses;
DROP TABLE IF EXISTS cluster_tags;


// address/{address}/cluster
CREATE TABLE address_cluster (
	address_prefix text,
	address text,
	cluster bigint,
	PRIMARY KEY (address_prefix, address)
);

// cluster/{cluster}
CREATE TABLE cluster (
	cluster bigint PRIMARY KEY,
	no_addresses int,
	no_incoming_txs int,
	no_outgoing_txs int,
	first_tx FROZEN <tx_id_time>,
	last_tx FROZEN <tx_id_time>,
	total_received frozen <value>,
	total_spent frozen <value>
);

// cluster/{cluster}/addresses
CREATE TABLE cluster_addresses (
	cluster bigint,
	address text,
	no_incoming_txs int,
	no_outgoing_txs int,
	first_tx FROZEN <tx_id_time>,
	last_tx FROZEN <tx_id_time>,
	total_received frozen <value>,
	total_spent frozen <value>,
	PRIMARY KEY (cluster, address)
);

// cluster/{cluster}/tags
CREATE TABLE cluster_tags (
	cluster bigint,
	address text,
	tag text,
	tag_uri text,
	description text,
	actor_category text,
	source text,
	source_uri text,
	timestamp int,
	PRIMARY KEY (cluster, address, tag, source)
);

// cluster/{cluster}/outgoingRelations (
CREATE TABLE cluster_outgoing_relations (
	src_cluster bigint,
	dst_cluster bigint,
	dst_category int,  // 0 = unknown, 2 = tag, 3 = manually entered label
	dst_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (src_cluster, dst_category, estimated_value, dst_cluster)
) WITH CLUSTERING ORDER BY (dst_category DESC, estimated_value DESC);

// cluster/{cluster}/incomingRelations (
CREATE TABLE cluster_incoming_relations (
	dst_cluster bigint,
	src_cluster bigint,
	src_category int,  // 0 = unknown, 2 = tag, 3 = manually entered label
	src_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (dst_cluster, src_category, estimated_value, src_cluster)
) WITH CLUSTERING ORDER BY (src_category DESC, estimated_value DESC);
