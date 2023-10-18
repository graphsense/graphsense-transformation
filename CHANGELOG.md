# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased
### Changed
- Upgrade to Spark 3.2.4

## [23.06/1.5.0] 2023-09-21
### Added
- Added coinbase transactions to network representations
  (address = 'coinbase', address_id = 0)
- Added new columns (`total_received_adj`, `total_spent_adj`) to `cluster`
  table (see graphsense/graphsense-transformation#34)

## [23.06/1.4.1] 2023-06-12
### Fixed
- Fix division by zero on zero fee and value txs

## [23.03/1.4.0] 2023-03-29
### Changed
- Upgrade to Spark 3.2.3
- Changed computation of `plainClusterRelations`
- Changed handling of missing exchange rates values; don't fill with zeros,
  remove blocks/txs instead.

## [23.01/1.3.0] 2023-01-30
### Added
- sbt scalafmt and scala style plugin
- Standardized dev-makefile

## [22.11] 2022-11-24
### Added
- Added columns to `summary_statistics` table

## [22.10] 2022-10-10
### Changed
- Upgraded to Spark 3.2.1
- Update Spark Cassandra connector to version 3.2.0
### Fixed
- Fixed test data ingest script

## [1.0.0] 2022-07-11

## [0.5.2] 2022-03-10
### Changed
- GraphFrames based address clustering
### Removed
- Removed tag handling (see graphsense/graphsense-tagpack-tool)

## [0.5.1] 2021-11-29
### Changed
- Upgrade to Spark 3
- Improved Cassandra schema
- Changed package name
### Added
- Added command-line arguments to spark submit script

## [0.5.0] 2021-06-02
### Added
- Add command-line arguments for coinjoin filtering and address prefixes
### Changed
- Adapted TagPack schema
- Changed schema of address/cluster relation tables
- Speed-up tests
- Made clustering deterministic wrt number of partitions

## [0.4.5] - 2020-11-19
### Added
- Added Dockerfile for `spark-submit` job

## [0.4.4] - 2020-06-12
### Changed
- Updated dependencies
### Fixed
- Fixed tests

## [0.4.3] - 2020-05-11
### Changed
- Updated data model, field name changed in raw Cassandra schema
  (graphsense/graphsense-blocksci@c418dab)
- Changed primary key of `address_transactions` table
- Added list of transactions (max 100) to address/cluster relations
- Added tag labels to address/cluster relations table
  (graphsense/graphsense-transformation#15)
- Store table `tag_by_label` in transformed keyspace
  (graphsense/graphsense-dashboard#98)
- Reintegrated clustering library
- Upgraded Scala (2.12)/Spark (2.4.5) + dependencies
- Allow NULL values in tag categories

## [0.4.2] - 2019-12-19
### Changed
- Exchange rates (by height) are stored in transformed keyspace
- Added new columns (`category`, `abuse`) to `cluster_tags` and `cluster` table
- Upgraded dependencies (scallop, scalatest, spark-fast-tests)
### Fixed
- Adjusted Cassandra schema, use integers for cluster IDs in cluster relations

## [0.4.1] - 2019-06-28
### Changed
- Changed model of cluster graph; every entity has now integer IDs
### Added
- Parser for command-line arguments (scallop)
### Fixed
- Fixed bug in cluster graph computation

## [0.4.0] - 2019-02-01
### Changed
- Update to Apache Spark 2.4
- Removed `srcCategory`/`dstCategory` in address/cluster relations
- Fixed data types in case classes/Cassandra schema
### Added
- Added argument to remove CoinJoin inputs in clustering
- Added `spark-fast-tests` library
- Added Travis CI

## [0.3.3] - 2018-11-30
### Changed
- Changed GraphSense backend to `graphsense-blocksci`
- Moved clustering code to library `graphsense-clustering`
### Added
- Added in/out degrees to tables `address`/`cluster`/`cluster_addresses`
- Added keyspace summary statistics (table `summary_statistics`)
- Added table `plain_cluster_relations`
