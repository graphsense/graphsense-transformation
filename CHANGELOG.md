# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Fixed
- Adjust Cassandra schema, use integers for cluster IDs in cluster relations

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
