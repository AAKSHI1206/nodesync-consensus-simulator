# Distributed E-Commerce Order Management System Using Raft

A C++-based distributed e-commerce order management simulator that demonstrates how multiple backend servers maintain consistent order and inventory information using the Raft consensus algorithm.

## Project Overview

Modern e-commerce systems may run across multiple backend servers to improve availability and handle failures. When several servers maintain the same order and inventory data, they need to agree on which operations should be accepted and in what order.

For example, if only one laptop is available and two customers place orders at nearly the same time, different servers could independently accept both requests, resulting in inconsistent inventory.

This project simulates this problem using a small cluster of virtual backend nodes. The nodes communicate through a simulated network and use Raft to coordinate state-changing operations.

## Proposed Solution

The system will contain multiple virtual backend nodes. One node acts as the Raft leader while the remaining nodes act as followers.

A typical operation follows this flow:

Client Request → Leader → Log Entry → Replication → Majority Acknowledgement → Commit → Update Order/Inventory State

If the leader fails, the remaining nodes can start a new election. A new leader is selected when a majority of nodes vote for it, allowing the system to continue when enough nodes are available.

## Main Features

- Raft leader election and heartbeats
- Log replication and majority-based commitment
- Distributed order creation and inventory reservation
- Simulated node and leader failures
- Message delay and message loss
- Network partition simulation
- Event logging and performance metrics
- Comparison of different network and failure scenarios

## DSA and C++ Concepts

The project uses DSA and OOP concepts as part of the actual system implementation.

- Graphs / adjacency lists for representing the network topology
- Queues for handling pending messages
- Priority queues for scheduling delayed messages and events
- Vectors for logs and dynamic collections
- Hash maps for fast node, order and inventory lookup
- BFS/DFS for network connectivity analysis
- Optional DSU for detecting disconnected network components
- C++ OOP for modelling Node, Message, Network, RaftEngine, Simulator and other components

## Technology Stack

- C++17
- Standard Template Library (STL)
- Raft Consensus Algorithm
- CMake
- Git & GitHub
- Console-based interface
- CSV/Text files for logs and experiment results

## System Architecture

The simulator is divided into the following major layers:

1. **E-Commerce Layer**  
   Handles orders and inventory operations.

2. **Raft Consensus Layer**  
   Handles leader election, heartbeats, log replication and commitment.

3. **Network Simulation Layer**  
   Models node connections, message delivery, delays, message loss and partitions.

4. **Failure & Monitoring Layer**  
   Injects failures and records system behaviour and performance metrics.

## Expected Outcome

The final system will provide a working simulation of a distributed e-commerce backend and demonstrate how Raft maintains a consistent replicated state under different failure and network conditions.

The project will also demonstrate the practical use of C++ OOP, STL, Data Structures and Algorithms in a real-world distributed-systems scenario.

## Project Status

 **Currently under development**

The implementation will be developed incrementally, beginning with the virtual node and network structure, followed by message handling, Raft consensus, e-commerce operations, failure simulation, logging, metrics and visualization.
