# VortexNetFS Proposal

### Initial Idea

I started brainstorming **VortexNetFS** with the idea of creating a fully decentralized, distributed file system from scratch. The goal was to ensure reliable file storage across multiple nodes, but without relying on centralized services. I wanted to build everything from the ground up, focusing on low-level control using Go and TCP. The inspiration came from wanting to control every part of the system, from file chunking to how nodes communicate and replicate data.

### Key Concepts

- **Decentralization**: No single point of failure. Nodes interact directly using TCP, distributing files across the network.
- **File Chunking**: Files are split into chunks and distributed across multiple nodes for efficient storage.
- **Consistent Hashing**: I’ll use consistent hashing to make sure that data is evenly distributed across nodes and minimize disruption when nodes join or leave.
- **Replication**: Each file chunk will be replicated across nodes to handle failures.
- **Write-Ahead Logging (WAL)**: I’ve integrated WAL to ensure data can be recovered if a node crashes.


### Current Progress

- I’ve implemented basic node setup, file chunking, and node-to-node communication using TCP. 
- I’ve added WAL for reliability, and a heartbeat system is in place for detecting node failures.
- The system is designed to handle file storage and retrieval, with files being broken into chunks and distributed across nodes.

### What’s Next

For now, I'm focusing on improving the core functionalities:
- Expanding the **replication strategy** to ensure redundancy.
- Enhancing the **failure recovery** process when nodes go offline.
- Creating better testing strategies and maybe adding a **gRPC-based SDK** in the future to make the system easier to use and extend.

### Overall Goal

My aim with **VortexNetFS** is to keep things simple but powerful—something that’s easy to understand but also resilient and scalable. This is still a work in progress, but I’m getting closer to making it a fully functional distributed file system.

This is just the beginning, and I’ll be refining things as I go.