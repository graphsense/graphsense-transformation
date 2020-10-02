CREATE KEYSPACE IF NOT EXISTS tagkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE tagkeyspace;

CREATE TABLE tag_by_address (
    address text,
    label text,
    source text,
    tagpack_uri text,
    currency text,
    lastmod int,
    category text,
    abuse text,
    PRIMARY KEY (address, label, source, tagpack_uri)
);
CREATE TABLE tagpack_by_uri (
    uri                 text PRIMARY KEY,
    title               text,
    description         text,
    creator             text
);

CREATE TABLE tag_by_category (
    category            text,
    currency            text,
    abuse               text,
    label_norm          text,
    label               text,
    address             text,
    source              text,
    tagpack_uri         text,
    lastmod             int,
    PRIMARY KEY (category, currency, address, source, tagpack_uri)
);

CREATE TABLE tag_by_label (
    label_norm_prefix   text,
    label_norm          text,
    label               text,
    address             text,
    currency            text,
    source              text,
    tagpack_uri         text,
    lastmod             int,
    category            text,
    abuse               text,
    PRIMARY KEY (label_norm_prefix, label_norm, label, address, currency, source, tagpack_uri)
);

CREATE TABLE taxonomy_by_key (
    key                 text,
    uri                 text,
    PRIMARY KEY (key)
);

CREATE TABLE concept_by_taxonomy_id (
    taxonomy            text,
    id                  text,
    uri                 text,
    label               text,
    description         text,
    PRIMARY KEY (taxonomy, id)
);
