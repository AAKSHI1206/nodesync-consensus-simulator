# Order Workflow

## Command format

One Raft log entry per order:

```
CREATE_ORDER|<orderId>|<product>|<quantity>
CREATE_ORDER|O101|LAPTOP|1
```

## Steps for one order

0. **Load balancer.** An order is a write, so `LoadBalancer` (policy `CONSISTENT_HASH`) sends it to the server whose Raft node is the leader (it asks the running servers; if there is no leader, for example during an election, the write is refused and the client can retry). That Raft-backed `Server` hands it to `OrderService` and reports success only if the order is committed. A malformed command is refused here (`INVALID_REQUEST`).
1. **Validate.** `OrderService` rejects malformed requests (empty ID or product, `|` inside a field, quantity not a whole number from 1 to 1,000,000). These never reach the log: outcome `INVALID_REQUEST`.
2. **Route.** If the receiving node is stopped: `NODE_DOWN`. If there is no leader: `NO_LEADER`. If the receiving node is a follower (only when `OrderService` is called directly, not through the load balancer), it forwards the request to the leader.
3. **Append.** The leader calls `RaftEngine::propose()`. The entry is in the leader's log but **not committed**; inventory is unchanged.
4. **Replicate and commit.** Followers append the entry and acknowledge. When a majority (2 of 3 in `--demo`, 3 of 5 in the interactive menu) holds it, the leader's commit index advances; followers learn it from the next message.
5. **Apply.** On every node, the apply callback hands the committed entry to that node's `OrderBook`:
   - unknown product: `REJECTED` ("unknown product")
   - order ID already used: `REJECTED` ("duplicate order ID"), first order kept
   - not enough stock: `REJECTED` ("insufficient stock"), stock unchanged
   - otherwise: `ACCEPTED`, stock reduced by the quantity
6. **Result.** `OrderService` waits (up to 50 ticks) for the leader to apply the entry and returns the real result (`COMMITTED` + order status). If there is no majority in time, the outcome is `NOT_COMMITTED` and inventory is unchanged; the entry may still commit later if enough nodes come back.

## Rules that always hold

- Inventory changes only when a committed entry is applied, never on request or append.
- Stock never goes below zero.
- Each node applies each log index at most once (`OrderBook` ignores an index it has already applied).
- A rejected order is still a committed entry: it is recorded as `REJECTED` and changes no stock.
- Initial stock (`LAPTOP: 2`, `MOUSE: 3` in the demo) is set identically on every node before any order.
- The same customer always reads from the same server while it is running; if it is stopped, only its customers move to the next server on the ring.

## Steps for one read

Reads (`GET_STOCK|<product>`, `GET_ORDER|<orderId>`) never enter the Raft log.

1. **Load balancer.** The customer ID is hashed onto the ring; the read goes to the first running server clockwise.
2. **Answer.** That server answers from its own `OrderBook` (`OK`, `NOT_FOUND`, `INVALID_REQUEST`, `NODE_DOWN`).
3. **Freshness.** The answer is marked **possibly stale** when:
   - the server has applied fewer entries than the leader has committed (a follower learns a new commit index with the next AppendEntries; a restarted server still has to catch up);
   - there is no leader to compare with (election in progress);
   - the leader was just elected and has not yet committed an entry of its own term, so it cannot be sure which earlier entries are committed.

Reads keep working while there is no leader (availability), at the price of possibly stale answers. A stale answer is flagged, never silently presented as current.
