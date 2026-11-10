# Distributed E-Commerce Order Management System Using Raft

A C++-based distributed e-commerce order management simulator that demonstrates how multiple backend servers maintain consistent order and inventory information using the Raft consensus algorithm.

## Project Overview

Modern e-commerce systems may run across multiple backend servers to improve availability and handle failures. When several servers maintain the same order and inventory data, they need to agree on which operations should be accepted and in what order.

For example, if only one laptop is available and two customers place orders at nearly the same time, different servers could independently accept both requests, resulting in inconsistent inventory.

This project simulates this problem using a small cluster of virtual backend nodes. The nodes communicate through a simulated network and use Raft to coordinate state-changing operations.

## Proposed Solution

The system contains multiple virtual backend nodes. One node acts as the Raft leader while the remaining nodes act as followers.

Every request enters through one load balancer, which routes it by type:

- **Write** (place an order): Client → Load Balancer → **Leader** → Log Entry → Replication → Majority Acknowledgement → Commit → Update Order/Inventory State on every node
- **Read** (check stock or an order's status): Client → Load Balancer (**consistent hashing** on the customer ID) → that customer's Server → answer from the server's own copy, marked "may be stale" if the server is behind the leader

If the leader fails, the remaining nodes start a new election. A new leader is selected when a majority of nodes vote for it, allowing the system to continue when enough nodes are available.

## Project Status

**Currently under development.** The foundation layers, a load balancer, Raft leader election, Raft log replication with majority commitment, and an order/inventory state machine fed by committed log entries are implemented and tested. Client requests enter through a load balancer: writes (orders) go to the Raft leader, and every node's inventory changes only when the order is committed; reads (stock, order status) go to the server picked by consistent hashing on the customer ID and are answered from that server's copy, with an honest "may be stale" flag. The next stage is simulating message delay, loss and network partitions.

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
| Server | `cpp/` | Backend server abstraction. **Raft-backed mode** (`attachToCluster`): server N is the front door of Raft node N; writes go through `OrderService` to the leader and succeed only when committed; reads are answered from node N's own `OrderBook`; stopping the server crashes that node. Original stand-alone mode (own Node + Storage) still available |
| Load Balancer | `cpp/` | Single entry point. With Raft-backed servers (policy `CONSISTENT_HASH`): **writes go to the leader's server** (it asks the servers which one is the leader; no leader → the write is refused and can be retried), **reads go to the server chosen by consistent hashing** on the customer ID. Original **round-robin** and **least-loaded** policies kept; stopped servers are skipped |
| Consistent Hash Ring | `cpp/ConsistentHashRing` | Ring of 2^32 positions stored in a sorted `std::map`; each server placed 50 times (virtual nodes) using a hand-written FNV-1a hash; lookup is the first server clockwise (`lower_bound`, O(log n)), skipping stopped servers. The same customer always reaches the same server, and a server going down moves only its own customers |
| Raft Leader Election | `cpp/RaftEngine` | Follower / candidate / leader roles, terms, one vote per term, up-to-date log check before granting a vote, deterministic election timeouts, heartbeats, stepping down on a higher term |
| Raft Log Replication | `cpp/RaftEngine` | Leader appends client commands and sends `APPEND_ENTRIES` (prevLogIndex, prevLogTerm, entries, leaderCommit). Followers check log matching and truncate conflicting entries. Leader tracks `nextIndex` / `matchIndex` per peer and commits by majority (current-term rule). Committed entries are passed to an apply callback exactly once, in log order |
| Cluster Simulation | `cpp/RaftCluster` | Tick-based, deterministic simulation clock. Each tick delivers pending messages and advances every active engine. Supports stopping a node (crash), restarting it with its term, vote and log intact, and submitting commands to the current leader |
| Order & Inventory | `cpp/OrderBook` | One state machine per node. Applies committed `CREATE_ORDER\|<id>\|<product>\|<qty>` entries in log order, never applies the same log index twice, rejects orders with insufficient stock (`REJECTED`, stock unchanged), unknown products or a reused order ID |
| Order Service | `cpp/OrderService` | Writes: validates the request, forwards it to the leader if it arrived at a follower, proposes it through Raft and reports the result only after the entry is committed and applied (`COMMITTED`, `NOT_COMMITTED`, `INVALID_REQUEST`, `NODE_DOWN`, `NO_LEADER`). Reads (`GET_STOCK\|<product>`, `GET_ORDER\|<id>`): answered from one node's `OrderBook` without a Raft round; the answer is marked **possibly stale** if that node has applied less than the leader has committed, if there is no leader, or if a newly elected leader has not yet committed an entry of its own term (so it cannot vouch for the commit point) |
| Interactive CLI | `cpp/src/main.cpp` | Menu to place orders (writes) and check stock or an order (reads) as a customer, through the load balancer; view inventory, orders, cluster state and routing; stop and restart servers; run a scripted self-checking demo. The original Phase 1-3 demos (storage + queue, 5-node election and failover, load balancer) remain available as menu option 10 or `nodesync --foundations` |

### Known Gaps

- One Raft group holds all data: consistent hashing spreads the **reads** over the servers, but every write goes through the single leader. Spreading writes would need sharding (several Raft groups).
- Reads are served from one server's copy and can be stale (they are flagged, not blocked). Linearizable reads (e.g. Raft's ReadIndex: confirm leadership with a majority before answering) are not implemented.
- There is only one load balancer, so it is a single point of failure.
- Client → load balancer → server hops are direct function calls; only Raft messages travel over the simulated network.
- A new leader does not append a no-op entry, so entries left from an earlier term are committed together with the next new order.
- Initial stock is configuration set identically on every node, not a replicated command.
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
- [x] Order creation with stock checks, applied only after commitment (state machine on the apply callback)
- [x] Interactive CLI and scripted demo
- [x] Load balancer connected to the Raft leader through Raft-backed servers
- [x] Consistent hashing in the load balancer (customer ID → server)
- [x] Read/write split: writes to the leader, reads to the hashed server with a staleness flag
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
- `std::map` for each node's inventory and orders (`OrderBook`)
- Consistent hashing: a sorted `std::map` as the hash ring, `lower_bound` (binary search) for the clockwise lookup, hand-written FNV-1a hash, virtual nodes
- C++ OOP (encapsulation, RAII, move semantics) for Node, Message, Network, Storage, Server, RaftEngine and OrderBook
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
                LoadBalancer, RaftEngine, RaftCluster, OrderBook, OrderService)
cpp/src/        C++ sources and main.cpp (interactive CLI)
cpp/tests/      Automated tests for the C++ components, Raft and orders
tests/          Phase 1 test and interactive CLI testers for the C data structures and Storage
docs/           Phase 1 and Phase 2 reports, architecture, workflow and algorithm notes
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

On Windows with MinGW, add `-G "MinGW Makefiles"` (or `-G Ninja`) to the first command and run `nodesync.exe`. If the build fails inside CMake's compiler check, the build path is probably too long; use a shorter path for the build directory. If a `build/` folder was copied from another location, delete it and configure again (CMake stores absolute paths). On MinGW the executables are linked statically, so they also run from Git Bash.

`nodesync` has three modes:

```bash
./nodesync               # interactive menu
./nodesync --demo        # scripted order demo that checks its own results
./nodesync --foundations # original Phase 1-3 demos
```

## CLI Demo

The interactive menu starts 5 Raft-backed servers (majority = 3) behind a consistent-hashing load balancer, with `LAPTOP: 2` and `MOUSE: 3`, then offers:

1. Create order as a customer (e.g. `C1`) — write: load balancer → leader → Raft
2. Check stock or an order's status — read: load balancer → the customer's hashed server
3. View inventory on all nodes
4. View orders
5. View Raft cluster and log status (role, term, log, commit and applied index per node)
6. Show routing: where writes go (the leader) and which server each customer `C1`-`C9` reads from
7. Stop a server (crashes its Raft node)
8. Restart stopped servers (they catch up)
9. Run the scripted demo on a fresh cluster
10. Run the original Phase 1-3 demos

Each order prints what actually happened: the leader the load balancer chose, the appended log entry, follower acknowledgements, the majority and commit index, which nodes applied it, and the result. Each read prints which server answered, the answer, and whether it is up to date or may be stale. The scripted demo (3 nodes) covers: writes going to the leader; a follower read right after a write (stale until the next heartbeat); an order rejected for lack of stock; a server crash that moves only that server's customers; a restarted server answering stale until it catches up; a leader crash during which writes are refused but reads still answered; re-election; and the old leader catching up. It reports a mismatch if any expected result does not happen.

See `docs/workflow.md` for the order rules and `docs/architecture.md` for the request path.

## Tests

**Automated (run by CTest, 19 tests):**

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
| `test_order_book` | Stock checks, order statuses, duplicate protection, input validation, replicas reaching the same state |
| `test_order_service` | Orders committed through Raft; no majority means no inventory change; invalid requests never reach the log |
| `test_order_failover` | Follower forwarding, leader failure and re-election, restart catch-up without double application, scripted demo |
| `test_consistent_hash` | Same key → same server, even spread over 3 servers, a stopped server moves only its own keys, adding a server moves about a quarter of keys |
| `test_routed_orders` | Through the load balancer: writes go to the leader and commit on every node; reads go to the hashed server and are not logged; stale flag after a write, after a restart and right after a new leader is elected; server crash moves only its customers; leader crash (write refused, read answered); no majority; last-laptop race; malformed and unknown reads |

**Interactive (menu-driven CLI):** `test_queue`, `test_linked_list`, `test_log_structure` and `test_storage` let you exercise each data structure by hand.

## Expected Outcome

The final system will provide a working simulation of a distributed e-commerce backend and demonstrate how Raft maintains a consistent replicated state under different failure and network conditions.

The project will also demonstrate the practical use of C++ OOP, STL, Data Structures and Algorithms in a real-world distributed-systems scenario.
