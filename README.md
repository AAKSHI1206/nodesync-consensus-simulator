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

## Project Status

**Currently under development.** The foundation layers (data structures, storage, nodes, messages and the simulated network) are implemented and tested. Raft consensus is the next stage.

### Completed

| Component | Location | Description |
|---|---|---|
| Linked List | `dsa_c/` | Singly linked list in C; backs the Raft log |
| Queue | `dsa_c/` | FIFO queue in C; used as a message channel |
| Raft Log Structure | `dsa_c/` | Log entries (index, term, command) with append, lookup, last index/term and truncation |
| Storage | `cpp/` | C++ wrapper over the C log with safe ownership (move-only) |
| Node | `cpp/` | Node identity and lifecycle (start / stop / active state) |
| Message | `cpp/` | Raft message model: sender, receiver, term, type, payload. Types: `REQUEST_VOTE`, `VOTE_RESPONSE`, `APPEND_ENTRIES`, `APPEND_RESPONSE`, `CLIENT_COMMAND` |
| Network | `cpp/` | Simulated network: node registry, per-node mailboxes, direct send, broadcast, FIFO receive; messages to inactive nodes are dropped. No real sockets or threads |
| Integration demo | `cpp/src/main.cpp` | Phase 1: storage + queue. Phase 2: 5-node message flow showing a vote request, vote responses and heartbeats (scripted, not yet driven by Raft logic) |

### Roadmap

- [x] Core data structures (linked list, queue, Raft log)
- [x] Storage abstraction
- [x] Node, Message and simulated Network
- [x] Multi-node communication demo
- [ ] Load balancer for routing client requests to backend servers *(in progress)*
- [ ] Raft leader election: roles, terms, voting, election timeouts, heartbeats *(in progress)*
- [ ] Log replication and majority-based commitment
- [ ] Distributed order creation and inventory reservation
- [ ] Failure simulation: node/leader failure, message delay and loss, network partitions
- [ ] Event logging, performance metrics and scenario comparison

## DSA and C++ Concepts

**Used so far**

- Linked list (C) for storing Raft log entries
- Queue (C) for pending messages
- `std::map` and `std::queue` for the node registry and per-node mailboxes
- C++ OOP (encapsulation, RAII, move semantics) for Node, Message, Network and Storage
- Mixed C/C++ build with `extern "C"` linkage

**Planned**

- Graphs / adjacency lists for network topology
- Priority queues for scheduling delayed messages and events
- Hash maps for order and inventory lookup
- BFS/DFS (or DSU) for detecting disconnected network components during partitions

## Technology Stack

- C11 and C++17
- Standard Template Library (STL)
- CMake (3.16+)
- Git & GitHub
- Console-based interface

## Repository Structure

```
dsa_c/          C data structures (linked list, queue, Raft log)
cpp/include/    C++ headers (Node, Message, Network, Storage, ...)
cpp/src/        C++ sources and main.cpp demo
cpp/tests/      Automated tests for the C++ components
tests/          Interactive CLI testers for the C data structures and Storage
docs/           Phase 1 report and documentation
config/         Simulation configuration (placeholder)
simulation/     Failure and network scenarios (placeholder)
```

## Build and Run

```bash
cmake -S . -B build
cmake --build build
cd build && ctest --output-on-failure
```

On Windows with MinGW, add `-G "MinGW Makefiles"` to the first command.

> **Note:** `CMakeLists.txt` already lists the upcoming Server, LoadBalancer and load-balancer test files. Until those files are pushed, a fresh clone will not configure.

## Tests

**Automated (run by CTest):**

| Test | Checks |
|---|---|
| `test_node` | Node ID, initial state, start/stop lifecycle |
| `test_message` | Message fields, all message types, type-to-string conversion |
| `test_network` | Registration, direct delivery, broadcast, FIFO order, error and inactive-node cases |
| `test_communication` | End-to-end multi-node send, broadcast and receive |

**Interactive (menu-driven CLI):** `test_queue`, `test_linked_list`, `test_log_structure` and `test_storage` let you exercise each data structure by hand.

## Expected Outcome

The final system will provide a working simulation of a distributed e-commerce backend and demonstrate how Raft maintains a consistent replicated state under different failure and network conditions.

The project will also demonstrate the practical use of C++ OOP, STL, Data Structures and Algorithms in a real-world distributed-systems scenario.