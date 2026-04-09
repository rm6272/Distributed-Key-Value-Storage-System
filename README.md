# Distributed Key-Value Store

## Overview

This project is a distributed key-value storage system implemented in Java, designed for high throughput, low latency, and strong consistency. It uses Multi-Raft consensus, sharding, and a hybrid storage engine to support scalable and reliable data access.

## Features

* Multi-Raft consensus for replication and consistency
* Consistent hashing with virtual nodes for load balancing
* Dynamic shard rebalancing
* Hybrid storage: RocksDB (LSM-tree) + in-memory B+ Tree
* MVCC snapshot isolation
* Read optimizations (ReadIndex, FollowerRead)
* Batched asynchronous writes

## Performance

* ~50K QPS throughput
* <10ms P99 latency
* 99.9% availability

## My Contributions

* Designed and implemented the system architecture
* Built consistent hashing and shard rebalancing
* Implemented storage layer with MVCC
* Optimized read/write paths for performance

## Technologies

* Java
* Distributed systems (replication, consensus, sharding)
* RocksDB
