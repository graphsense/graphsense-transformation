# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed
- Update to Apache Spark 2.4

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
