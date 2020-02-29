DROP KEYSPACE IF EXISTS btc_transformed;
CREATE KEYSPACE btc_transformed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE btc_transformed;

CREATE TYPE tx_id_time (
    height int,
    tx_hash blob,
    timestamp int
);

CREATE TYPE value (
    value bigint,
    eur float,
    usd float
);

CREATE TABLE exchange_rates (
    height int PRIMARY KEY,
    usd float,
    eur float
);

CREATE TABLE address_transactions (
    address_id_group int,
    address_id int,
    tx_hash blob,
    value bigint,
    height int,
    tx_index int,
    timestamp int,
    PRIMARY KEY (address_id_group, address_id, height, tx_hash)
);

CREATE TABLE address (
    address_prefix text,
    address text,
    address_id int,
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

CREATE TABLE address_by_id_group (
    address_id_group int,
    address_id int,
    address text,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE address_tags (
    address text,
    address_id int,
    label text,
    source text,
    tagpack_uri text,
    lastmod int,
    category text,
    abuse text,
    PRIMARY KEY (address, label, source, tagpack_uri)
);

CREATE TYPE address_summary (
    total_received FROZEN <value>,
    total_spent FROZEN <value>
);

CREATE TABLE address_incoming_relations (
    dst_address_id_group int,
    dst_address_id int,
    src_address_id int,
    src_properties FROZEN <address_summary>,
    src_labels list<text>,
    no_transactions int,
    estimated_value frozen <value>,
    PRIMARY KEY (dst_address_id_group, dst_address_id, src_address_id)
);

CREATE TABLE address_outgoing_relations (
    src_address_id_group int,
    src_address_id int,
    dst_address_id int,
    dst_properties FROZEN <address_summary>,
    dst_labels list<text>,
    no_transactions int,
    estimated_value frozen <value>,
    PRIMARY KEY (src_address_id_group, src_address_id, dst_address_id)
);

CREATE TABLE address_cluster (
    address_id_group int,
    address_id int,
    cluster int,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE cluster (
    cluster_group int,
    cluster int,
    no_addresses int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx FROZEN <tx_id_time>,
    last_tx FROZEN <tx_id_time>,
    total_received frozen <value>,
    total_spent frozen <value>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (cluster_group, cluster)
);

CREATE TABLE cluster_addresses (
    cluster_group int,
    cluster int,
    address_id int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx FROZEN <tx_id_time>,
    last_tx FROZEN <tx_id_time>,
    total_received frozen <value>,
    total_spent frozen <value>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (cluster_group, cluster, address_id)
);

CREATE TABLE cluster_tags (
    cluster_group int,
    cluster int,
    address text,
    address_id int,
    label text,
    source text,
    tagpack_uri text,
    lastmod int,
    category text,
    abuse text,
    PRIMARY KEY (cluster_group, cluster, address, label, source, tagpack_uri)
);

CREATE TYPE cluster_summary (
    no_addresses int,
    total_received FROZEN <value>,
    total_spent FROZEN <value>
);

CREATE TABLE cluster_incoming_relations (
    dst_cluster_group int,
    dst_cluster int,
    src_cluster int,
    src_properties FROZEN <cluster_summary>,
    src_labels list<text>,
    no_transactions int,
    value frozen <value>,
    PRIMARY KEY (dst_cluster_group, dst_cluster, src_cluster)
);

CREATE TABLE cluster_outgoing_relations (
    src_cluster_group int,
    src_cluster int,
    dst_cluster int,
    dst_properties FROZEN <cluster_summary>,
    dst_labels list<text>,
    no_transactions int,
    value frozen <value>,
    PRIMARY KEY (src_cluster_group, src_cluster, dst_cluster)
);

CREATE TABLE tag_by_label (
    label_norm_prefix text,
    label_norm text,
    label text,
    address text,
    source text,
    tagpack_uri text,
    currency text,
    lastmod int,
    category text,
    abuse text,
    active_address boolean,
    PRIMARY KEY (label_norm_prefix, label_norm, label, address, currency, source, tagpack_uri)
);

CREATE TABLE summary_statistics (
    timestamp int,
    no_blocks bigint PRIMARY KEY,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    no_clusters bigint,
    no_tags bigint,
    bucket_size int
);
