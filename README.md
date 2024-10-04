# VortexNetFS 🌪️

**VortexNetFS** is a distributed file system built entirely from scratch in Go using low-level TCP networking. It leverages a decentralized architecture with consistent hashing, a distributed hash table (DHT), and a robust replication strategy to ensure scalable, reliable, and fault-tolerant file storage across multiple nodes. VortexNetFS offers flexibility in data distribution, supporting both ring-based and all-to-all topologies.

## Overview

VortexNetFS operates as a fully decentralized system where each file is split into multiple chunks and distributed across a network of nodes. Nodes communicate using TCP, with a Write-Ahead Log (WAL) used to ensure data consistency and recovery in the event of a failure. The distributed hash table (DHT) manages file metadata and mappings between chunks and nodes, while consistent hashing ensures an even distribution of data. The system also supports node health monitoring and automatic recovery mechanisms for nodes that fail during operation.

## Architectural Diagram

![Architectural Diagram](/documentation/VortexNetFS-init.png)

### Features
- **Built from Scratch**: Designed entirely in Go using raw TCP networking for node communication.
- **Distributed File Storage**: Files are chunked and distributed across multiple nodes, supporting large-scale data storage.
- **Write-Ahead Log (WAL)**: Ensures recovery and consistency in the event of failures.
- **DHT (Distributed Hash Table)**: Efficient management of file metadata and chunk-node mappings.
- **Consistent Hashing**: Balanced distribution of data across nodes, supporting both ring and all-to-all topologies.
- **Data Replication**: Fault tolerance via chunk replication across multiple nodes.
- **Node Health Monitoring**: Heartbeat mechanism to detect node failures and manage recovery.
- **Chunking Engine**: Splits large files into manageable chunks for distributed storage.

### Functionalities
- File storage with metadata and chunk management.
- File retrieval and chunk reconstruction from distributed nodes.
- WAL-based recovery for node failures or crashes.
- Heartbeat-driven node failure detection and recovery.
- DHT-based file and chunk distribution.
- Consistent hashing for efficient chunk placement.
- **Funny Test**: A small test script can be run to test basic functionality (storing/retrieving files between nodes).

### Todo's
- **Rebalance Manager**: Improve Auto-rebalancing of chunks on node addition/removal.
- **Replication Manager**: Improve data replication management for enhanced fault tolerance.
- **Disaster Recovery**: Add recovery strategies for catastrophic node failures.
- **SDK Development**: Provide a REST and gRPC SDK for easier integration with external applications.
- **Protocol Improvements**: Migration to efficient communication protocols

---

More detailed documentation, usage guides, and extended functionality will be added upon project completion. Stay tuned!
