# Architecture

## Layers

| Layer | Modules | Responsibility |
|---|---|---|
| C data structures | `dsa_c/` linked list, queue, log structure | Raw storage of log entries and queued items |
| Storage | `Storage` | C++ wrapper over the C log (append, get, truncate, last index/term) |
| Network simulation | `Node`, `Message`, `Network` | Node lifecycle, per-node FIFO mailboxes; messages to stopped nodes are dropped |
| Consensus | `RaftEngine` (one per node), `RaftCluster` | Election, log replication, majority commit, apply callback; tick-based simulation loop |
| Application | `OrderBook` (one per node), `OrderService` | Inventory and orders, changed only by committed entries; request validation, forwarding and waiting for commit |
| Request routing | `LoadBalancer`, `ConsistentHashRing`, `Server` (Raft-backed) | Writes → the leader's server; reads → the server picked by consistent hashing on the customer ID; server N is the front door of Raft node N |
| Presentation | `cpp/src/main.cpp` | Interactive CLI; reads state, never changes inventory itself |

`Server` and `LoadBalancer` also keep their original stand-alone mode (each server with its own log, round-robin / least-loaded), used by `test_loadbalancer` and the Phase 3 foundation demo.

## Write path (place an order)

```
client (customer C1, order O101)
  |  LoadBalancer::routeRequest  -- policy CONSISTENT_HASH, command is a write
  |  LoadBalancer::leaderServer()  -> the running server whose node is LEADER
  |  (no leader -> refused, client retries)
  v
Server L (leader, Raft-backed)::handleRequest
  |  OrderService::submitOrder(L, id, product, qty)   (no forwarding needed)
  v
leader
                                          |  RaftEngine::propose(command)
                                          |  entry appended to the leader's Storage (NOT committed)
                                          v
                          APPEND_ENTRIES over Network -> followers append, reply APPEND_RESPONSE
                                          |
                          leader: majority holds the entry (current term) -> commitIndex advances
                                          |
                          every node: apply callback -> OrderBook::apply(index, command)
                                          v
                          result read from the leader's OrderBook -> printed by the CLI
```

## Read path (check stock / an order)

```
client (customer C1, GET_STOCK|LAPTOP)
  |  LoadBalancer::routeRequest  -- command is a read
  |  ConsistentHashRing::lookup(hash("C1"))  -> first running server clockwise
  v
Server N (Raft-backed)::handleRequest
  |  OrderService::read(N, command)
  v
answer from node N's own OrderBook  (not logged, no Raft round)
  + freshness: compared with the leader's commit index -> "up to date" or "may be stale"
```

Who decides what:

- **The load balancer** only decides which server receives a request (writes: the leader; reads: the hashed server); it never decides commitment.
- **Raft** (`RaftEngine`) decides whether and when an entry is committed.
- **OrderBook** decides whether a committed order is ACCEPTED or REJECTED (stock check).
- **OrderService** only validates the request format, finds the leader, and runs simulation ticks until the result is known.
- **The CLI** only displays results.

Each node has its own `OrderBook`, fed only by its own `RaftEngine`. Nodes that apply the same committed entries in the same order end up with the same inventory.
