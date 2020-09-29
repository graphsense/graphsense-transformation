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
