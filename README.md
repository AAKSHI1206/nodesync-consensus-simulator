# Distributed E-Commerce Order Management System Using Raft

A C++-based distributed e-commerce order management simulator that demonstrates how multiple backend servers maintain consistent order and inventory information using the Raft consensus algorithm.

## Project Overview

Modern e-commerce systems may run across multiple backend servers to improve availability and handle failures. When several servers maintain the same order and inventory data, they need to agree on which operations should be accepted and in what order.

For example, if only one laptop is available and two customers place orders at nearly the same time, different servers could independently accept both requests, resulting in inconsistent inventory.

This project simulates this problem using a small cluster of virtual backend nodes. The nodes communicate through a simulated network and use Raft to coordinate state-changing operations.

## Proposed Solution

The system contains multiple virtual backend nodes. One node acts as the Raft leader while the remaining nodes act as followers.

A typical operation follows this flow:

Client Request → Leader → Log Entry → Replication → Majority Acknowledgement → Commit → Update Order/Inventory State

If the leader fails, the remaining nodes start a new election. A new leader is selected when a majority of nodes vote for it, allowing the system to continue when enough nodes are available.

## Project Status

**Currently under development.** The foundation layers, a load balancer, Raft leader election and Raft log replication with majority commitment are implemented and tested. The next stage is the order/inventory state machine that consumes committed log entries.

### Completed

| Component | Location | Description |
|---|---|---|
| Linked List | `dsa_c/` | Singly linked list in C; backs the Raft log |
| Queue | `dsa_c/` | FIFO queue in C; used as a message and client-request channel |
| Raft Log Structure | `dsa_c/` | Log entries (index, term, command) with append, lookup, last index/term and truncation |
| Storage | `cpp/` | C++ wrapper over the C log with safe ownership (move-only) |
| Node | `cpp/` | Node identity and lifecycle (start / stop / active state) |
| Message | `cpp/` | Raft message model: sender, receiver, term, type, payload. Types: `REQUEST_VOTE`, `VOTE_RESPONSE`, `APPEND_ENTRIES`, `APPEND_RESPONSE`, `CLIENT_COMMAND` |
| Network | `cpp/` | Simulated network: node registry, per-node mailboxes, direct send, broadcast, FIFO receive; messages to inactive nodes are dropped. No real sockets or threads |
| Server | `cpp/` | Backend server abstraction: a Node plus its own Storage, with simple request handling |
| Load Balancer | `cpp/` | Routes client requests to Servers using **round-robin** or **least-loaded** (log size) policy; skips stopped servers |
| Raft Leader Election | `cpp/RaftEngine` | Follower / candidate / leader roles, terms, one vote per term, up-to-date log check before granting a vote, deterministic election timeouts, heartbeats, stepping down on a higher term |
| Raft Log Replication | `cpp/RaftEngine` | Leader appends client commands and sends `APPEND_ENTRIES` (prevLogIndex, prevLogTerm, entries, leaderCommit). Followers check log matching and truncate conflicting entries. Leader tracks `nextIndex` / `matchIndex` per peer and commits by majority (current-term rule). Committed entries are passed to an apply callback exactly once, in log order |
| Cluster Simulation | `cpp/RaftCluster` | Tick-based, deterministic simulation clock. Each tick delivers pending messages and advances every active engine. Supports stopping a node (crash), restarting it with its term, vote and log intact, and submitting commands to the current leader |
| Integration demo | `cpp/src/main.cpp` | Phase 1: storage + queue. Phase 2: 5-node Raft election, then leader failure and re-election. Phase 3: load balancer routing requests to 3 servers, including a server failure |

### Known Gaps

- The load balancer currently routes to `Server` objects, which are **not yet connected** to `RaftEngine`. Requests routed this way are appended to a single server's log and are not replicated through Raft. Connecting the load balancer to the Raft leader is still to be done.
- The apply callback is in place, but there is no order/inventory state machine behind it yet, so committed commands are not yet interpreted as orders or stock changes.
- `main.cpp` does not yet demonstrate log replication; it is covered by the automated tests.
- `RaftNode.h`, `RaftState.h`, `RaftNode.cpp`, `config/` and `simulation/scenarios/` are empty placeholders.

### Roadmap

- [x] Core data structures (linked list, queue, Raft log)
- [x] Storage abstraction
- [x] Node, Message and simulated Network
- [x] Multi-node communication demo
- [x] Load balancer with round-robin and least-loaded routing
- [x] Raft leader election: roles, terms, voting, election timeouts, heartbeats
- [x] Deterministic tick-based cluster simulation
- [x] Log replication and majority-based commitment
- [x] Node crash and restart with persistent Raft state; follower catch-up
- [ ] Connect the load balancer to the Raft leader
- [ ] Distributed order creation and inventory reservation (state machine on the apply callback)
- [ ] Failure simulation: message delay and loss, network partitions
- [ ] Event logging, performance metrics and scenario comparison

## DSA and C++ Concepts

**Used so far**

- Linked list (C) for storing Raft log entries
- Queue (C) for pending messages and client requests
- `std::map` and `std::queue` for the node registry and per-node mailboxes
- `std::map` for per-peer `nextIndex` / `matchIndex`, `std::set` for counting votes
- `std::vector<std::unique_ptr<...>>` for owning cluster nodes, storages and engines
- `std::function` callback for applying committed entries
- C++ OOP (encapsulation, RAII, move semantics) for Node, Message, Network, Storage, Server and RaftEngine
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
cpp/include/    C++ headers (Node, Message, Network, Storage, Server,
                LoadBalancer, RaftEngine, RaftCluster)
cpp/src/        C++ sources and main.cpp demo
cpp/tests/      Automated tests for the C++ components and Raft
tests/          Phase 1 test and interactive CLI testers for the C data structures and Storage
docs/           Phase 1 report and documentation
config/         Simulation configuration (placeholder)
simulation/     Failure and network scenarios (placeholder)
```

## Build and Run

```bash
cmake -S . -B build
cmake --build build
cd build && ctest --output-on-failure
./nodesync
```

On Windows with MinGW, add `-G "MinGW Makefiles"` to the first command and run `nodesync.exe`. If the build fails inside CMake's compiler check, the build path is probably too long; use a shorter path for the build directory.

## Tests

**Automated (run by CTest, 14 tests):**

| Test | Checks |
|---|---|
| `test_phase1` | Phase 1 data structures and Storage |
| `test_node` | Node ID, initial state, start/stop lifecycle |
| `test_message` | Message fields, all message types, type-to-string conversion |
| `test_network` | Registration, direct delivery, broadcast, FIFO order, error and inactive-node cases |
| `test_communication` | End-to-end multi-node send, broadcast and receive |
| `test_loadbalancer` | Round-robin and least-loaded routing, skipping stopped servers |
| `test_raft_engine` | Raft roles, term handling, voting rules and heartbeats on a single engine |
| `test_election` | 3- and 5-node elections, one leader per term, majority required, leader replacement, minority cannot elect, deterministic results |
| `test_storage_metadata` | Last log index/term used for vote decisions |
| `test_simulation` | Deterministic simulation clock |
| `test_log_storage` | Log operations and conflict resolution (truncate and overwrite) |
| `test_append_entries` | `APPEND_ENTRIES` protocol, log matching, progress tracking and commitment |
| `test_replication_messages` | Stale, duplicate and out-of-date messages; bursts of proposals; deterministic replication |
| `test_log_replication` | Cluster-level replication, majority commit, no commit without majority, follower catch-up, old leader rejoining, restart keeping persistent state, 5-node quorum |

**Interactive (menu-driven CLI):** `test_queue`, `test_linked_list`, `test_log_structure` and `test_storage` let you exercise each data structure by hand.

## Expected Outcome

The final system will provide a working simulation of a distributed e-commerce backend and demonstrate how Raft maintains a consistent replicated state under different failure and network conditions.

The project will also demonstrate the practical use of C++ OOP, STL, Data Structures and Algorithms in a real-world distributed-systems scenario.
