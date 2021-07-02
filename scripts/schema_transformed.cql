CREATE KEYSPACE IF NOT EXISTS btc_transformed
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE btc_transformed;

CREATE TYPE tx_id_time (
    height int,
    tx_index bigint,
    timestamp int
);

CREATE TYPE currency (
    value bigint,
    fiat_values list<float>
);

CREATE TABLE exchange_rates (
    height int PRIMARY KEY,
    fiat_values list<float>
);

CREATE TABLE address_transactions (
    address_id_group int,
    address_id int,
    tx_index bigint,
    value bigint,
    height int,
    timestamp int,
    PRIMARY KEY (address_id_group, address_id, tx_index)
);

CREATE TABLE address (
    address_id_group int,
    address_id int,
    address text,
    cluster_id int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx FROZEN <tx_id_time>,
    last_tx FROZEN <tx_id_time>,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE address_by_address_prefix (
    address_prefix text,
    address text,
    address_id int,
    PRIMARY KEY (address_prefix, address)
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

CREATE TABLE address_tag_by_label (
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
    active boolean,
    PRIMARY KEY (label_norm_prefix, label_norm, label, address, currency, source, tagpack_uri)
);

CREATE TABLE address_incoming_relations (
    dst_address_id_group int,
    dst_address_id int,
    src_address_id int,
    has_src_labels boolean,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (dst_address_id_group, dst_address_id, src_address_id)
);

CREATE TABLE address_outgoing_relations (
    src_address_id_group int,
    src_address_id int,
    dst_address_id int,
    has_dst_labels boolean,
    no_transactions int,
    estimated_value FROZEN <currency>,
    tx_list list<bigint>,
    PRIMARY KEY (src_address_id_group, src_address_id, dst_address_id)
);

CREATE TABLE cluster (
    cluster_id_group int,
    cluster_id int,
    no_addresses int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx FROZEN <tx_id_time>,
    last_tx FROZEN <tx_id_time>,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (cluster_id_group, cluster_id)
);

CREATE TABLE cluster_addresses (
    cluster_id_group int,
    cluster_id int,
    address_id int,
    PRIMARY KEY (cluster_id_group, cluster_id, address_id)
);

CREATE TABLE cluster_tags (
    cluster_id_group int,
    cluster_id int,
    label text,
    source text,
    tagpack_uri text,
    lastmod int,
    category text,
    abuse text,
    PRIMARY KEY (cluster_id_group, cluster_id, label, source, tagpack_uri)
);

CREATE TABLE cluster_address_tags (
    cluster_id_group int,
    cluster_id int,
    address_id int,
    label text,
    source text,
    tagpack_uri text,
    lastmod int,
    category text,
    abuse text,
    PRIMARY KEY (cluster_id_group, cluster_id, address_id, label, source, tagpack_uri)
);

CREATE TABLE cluster_tag_by_label (
    label_norm_prefix text,
    label_norm text,
    label text,
    cluster_id int,
    source text,
    tagpack_uri text,
    currency text,
    lastmod int,
    category text,
    abuse text,
    active boolean,
    PRIMARY KEY (label_norm_prefix, label_norm, label, cluster_id, currency, source, tagpack_uri)
);

CREATE TABLE cluster_incoming_relations (
    dst_cluster_id_group int,
    dst_cluster_id int,
    src_cluster_id int,
    has_src_labels boolean,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (dst_cluster_id_group, dst_cluster_id, src_cluster_id)
);

CREATE TABLE cluster_outgoing_relations (
    src_cluster_id_group int,
    src_cluster_id int,
    dst_cluster_id int,
    has_dst_labels boolean,
    no_transactions int,
    estimated_value FROZEN <currency>,
    tx_list list<bigint>,
    PRIMARY KEY (src_cluster_id_group, src_cluster_id, dst_cluster_id)
);

CREATE TABLE summary_statistics (
    timestamp int,
    no_blocks bigint PRIMARY KEY,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    no_clusters bigint,
    no_tags bigint
);

CREATE TABLE configuration (
    keyspace_name text PRIMARY KEY,
    bucket_size int,
    bech_32_prefix text,
    coinjoin_filtering boolean,
    fiat_currencies list<text>
);
