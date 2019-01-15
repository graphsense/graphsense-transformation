DROP KEYSPACE IF EXISTS btc_transformed;
CREATE KEYSPACE btc_transformed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE btc_transformed;

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

CREATE TABLE address_transactions (
    address_prefix text,
    address text,
    tx_hash blob,
    value bigint,
    height int,
    tx_index int,
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
    in_degree int,
    out_degree int,
    PRIMARY KEY (address_prefix, address)
);

CREATE TABLE address_tags (
    address text,
    tag text,
    tag_uri text,
    description text,
    actor_category text,
    source text,
    source_uri text,
    timestamp int,
    PRIMARY KEY (address, tag, source)
);

CREATE TYPE address_summary (
    total_received bigint,
    total_spent bigint
);

CREATE TABLE address_incoming_relations (
    dst_address_prefix text,
    dst_address text,
    src_address text,
    src_properties FROZEN <address_summary>,
    no_transactions int,
    estimated_value frozen <value>,
    PRIMARY KEY (dst_address_prefix, dst_address, estimated_value, src_address)
) WITH CLUSTERING ORDER BY (dst_address ASC, estimated_value DESC);

CREATE TABLE address_outgoing_relations (
    src_address_prefix text,
    src_address text,
    dst_address text,
    dst_properties FROZEN <address_summary>,
    no_transactions int,
    estimated_value frozen <value>,
    PRIMARY KEY (src_address_prefix, src_address, estimated_value, dst_address)
) WITH CLUSTERING ORDER BY (src_address ASC, estimated_value DESC);

CREATE TABLE address_cluster (
    address_prefix text,
    address text,
    cluster bigint,
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
    total_spent frozen <value>,
    in_degree int,
    out_degree int
);

CREATE TABLE cluster_addresses (
    cluster bigint,
    address text,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx FROZEN <tx_id_time>,
    last_tx FROZEN <tx_id_time>,
    total_received frozen <value>,
    total_spent frozen <value>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (cluster, address)
);

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

CREATE TABLE plain_cluster_relations (
    tx_hash blob,
    src_cluster text,
    dst_cluster text,
    value bigint,
    height int,
    PRIMARY KEY (height, src_cluster, value)
);

CREATE TYPE cluster_summary (
    no_addresses int,
    total_received bigint,
    total_spent bigint
);

CREATE TABLE cluster_incoming_relations (
    dst_cluster text,
    src_cluster text,
    src_properties FROZEN <cluster_summary>,
    no_transactions int,
    value frozen <value>,
    PRIMARY KEY (dst_cluster, value, src_cluster)
);

CREATE TABLE cluster_outgoing_relations (
    src_cluster text,
    dst_cluster text,
    dst_properties FROZEN <cluster_summary>,
    no_transactions int,
    value frozen <value>,
    PRIMARY KEY (src_cluster, value, dst_cluster)
);

CREATE TABLE summary_statistics (
    timestamp int,
    no_blocks bigint PRIMARY KEY,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    no_clusters bigint
);
