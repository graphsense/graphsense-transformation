DROP KEYSPACE IF EXISTS graphsense_transformed;

CREATE KEYSPACE graphsense_transformed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE graphsense_transformed;

// TYPE DEFINITIONS

CREATE TYPE tx_summary (
	tx_hash blob,
	no_inputs int,
	no_outputs int,
	total_input bigint,
	total_output bigint
);

CREATE TYPE tx_input_output (
	address text,
	value bigint
);

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

CREATE TYPE tag (
	tag text,
	link text,
	source text
);

CREATE TYPE address_summary (
	total_received bigint,
	total_spent bigint
);


// VIEW TABLES MATCHING REST API METHODS

// exchangerate/{height}
CREATE TABLE exchange_rates (
	height int PRIMARY KEY,
	eur double,
	usd double
);

// block/{height}
CREATE TABLE block (
	height int PRIMARY KEY,
	block_hash blob,
	timestamp int,
	no_transactions int
);

// transaction/{tx_hash}
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

// block/{height}/transactions
CREATE TABLE block_transactions (
	height int PRIMARY KEY,
	txs list<FROZEN<tx_summary>>
);

// address/{address}/transactions
CREATE TABLE address_transactions (
	address_prefix text,
	address text,
	tx_hash blob,
	value bigint,
	height int,
	timestamp int,
	PRIMARY KEY (address_prefix, address, height, tx_hash)
) WITH CLUSTERING ORDER BY (address ASC, height DESC);

// address/{address}
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

// address/{address}/outgoingRelations (
CREATE TABLE address_outgoing_relations (
	src_address_prefix text,
	src_address text,
	dst_address text,
	dst_category int,  // 0 = unknown, 1 = implicit tag, 2 = explicit tag, 3 = manually entered label
	dst_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (src_address_prefix, src_address, dst_category, estimated_value, dst_address)
) WITH CLUSTERING ORDER BY (src_address ASC, dst_category DESC, estimated_value DESC);

// address/{address}/incomingRelations (
CREATE TABLE address_incoming_relations (
	dst_address_prefix text,
	dst_address text,
	src_address text,
	src_category int,  // 0 = unknown, 1 = implicit tag, 2 = explicit tag, 3 = manually entered label
	src_properties FROZEN <address_summary>,
	no_transactions int,
	estimated_value frozen <value>,
	PRIMARY KEY (dst_address_prefix, dst_address, src_category, estimated_value, src_address)
) WITH CLUSTERING ORDER BY (dst_address ASC, src_category DESC, estimated_value DESC);
