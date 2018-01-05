DROP KEYSPACE IF EXISTS graphsense_transformed_new;

CREATE KEYSPACE graphsense_transformed_new WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE graphsense_transformed_new;

CREATE TYPE tx_id_time (
	height int,
	tx_hash blob,
	timestamp int
);

CREATE TYPE value (
	satoshi bigint,
	eur double,
	usd double
);

CREATE TABLE exchange_rates (
	height int PRIMARY KEY,
	eur double,
	usd double
);

CREATE TABLE block (
	height int PRIMARY KEY,
	block_hash blob,
	timestamp int,
	no_transactions int
);

CREATE TYPE tx_input_output (
	address text,
	value bigint
);

CREATE TABLE transaction (
	tx_prefix text,
	tx_hash blob,
	height int,
	timestamp int,
	coinbase boolean,
	total_input bigint,
	total_output bigint,
	inputs list<FROZEN<tx_input_output>>,
	outputs list<FROZEN<tx_input_output>>,
	PRIMARY KEY (tx_prefix, tx_hash)
);

CREATE TYPE tx_summary (
	tx_hash blob,
	no_inputs int,
	no_outputs int,
	total_input bigint,
	total_output bigint
);

CREATE TABLE block_transactions (
	height int PRIMARY KEY,
	txs list<FROZEN<tx_summary>>
);

CREATE TABLE address_transactions (
	address_prefix text,
	address text,
	tx_hash blob,
	value bigint,
	height int,
	tx_number int,
	timestamp int,
	PRIMARY KEY (address_prefix, address, height, tx_hash)
) WITH CLUSTERING ORDER BY (address ASC, height DESC);

CREATE TABLE address (
	address_prefix text,
	address text,
	no_incoming_txs int,
	no_outgoing_txs int,
	first_tx FROZEN <tx_id_time>,
	last_tx FROZEN <tx_id_time>,
	total_received frozen <value>,
	total_spent frozen <value>,
	PRIMARY KEY (address_prefix, address)
);

CREATE TYPE address_summary (
	total_received bigint,
	total_spent bigint
);

CREATE TABLE address_incoming_relations (
	dst_address_prefix text,
	dst_address text,
	src_address text,
	src_category int,				// 0 = unknown, 1 = implicit tag, 2 = explicit tag, 3 = manually entered label
	src_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (dst_address_prefix, dst_address, src_category, estimated_value, src_address)	
) WITH CLUSTERING ORDER BY (dst_address ASC, src_category DESC, estimated_value DESC);

CREATE TABLE address_outgoing_relations (
	src_address_prefix text,
	src_address text,
	dst_address text,
	dst_category int,				// 0 = unknown, 1 = implicit tag, 2 = explicit tag, 3 = manually entered label
	dst_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (src_address_prefix, src_address, dst_category, estimated_value, dst_address)
) WITH CLUSTERING ORDER BY (src_address ASC, dst_category DESC, estimated_value DESC);

CREATE TABLE address_cluster (
	address_prefix text,
	address text,
	cluster int,
	PRIMARY KEY (address_prefix, address)
);

CREATE TABLE cluster (
	cluster int PRIMARY KEY,
	no_addresses int,
	no_incoming_txs int,
	no_outgoing_txs int,
	first_tx FROZEN <tx_id_time>,
	last_tx FROZEN <tx_id_time>,
	total_received frozen <value>,
	total_spent frozen <value>
);

CREATE TABLE cluster_addresses (
	cluster int,
	address text,
	no_incoming_txs int,
	no_outgoing_txs int,
	first_tx FROZEN <tx_id_time>,
	last_tx FROZEN <tx_id_time>,
	total_received frozen <value>,
	total_spent frozen <value>,
	PRIMARY KEY (cluster, address)
);

CREATE TABLE cluster_tags (
	cluster int,
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

CREATE TYPE cluster_summary (
	no_addresses int,
	total_received bigint,
	total_spent bigint
);

CREATE TABLE cluster_incoming_relations (
	dst_cluster text,
	src_cluster text,
	src_category int,				// 0 = unknown, 2 = tag, 3 = manually entered label
	src_properties FROZEN <cluster_summary>,
	no_transactions int,
	value frozen <value>,
	PRIMARY KEY (dst_cluster, src_category, value, src_cluster)
) WITH CLUSTERING ORDER BY (src_category DESC, value DESC);

CREATE TABLE cluster_outgoing_relations (
	src_cluster text,
	dst_cluster text,
	dst_category int,				// 0 = unknown, 2 = tag, 3 = manually entered label
	dst_properties FROZEN <cluster_summary>,
	no_transactions int,
	value frozen <value>,
	PRIMARY KEY (src_cluster, dst_category, value, dst_cluster)
) WITH CLUSTERING ORDER BY (dst_category DESC, value DESC);
