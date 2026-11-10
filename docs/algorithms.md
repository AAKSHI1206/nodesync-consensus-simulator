# Algorithms

## Raft leader election (`RaftEngine`)

- Each node starts as a **follower**. If it hears nothing from a leader before its election timeout, it becomes a **candidate**: term + 1, votes for itself, sends `REQUEST_VOTE`.
- A node grants at most one vote per term, and only to a candidate whose log is at least as up to date (higher last term, or same last term and at least as long).
- A candidate with votes from a majority (`floor(N/2) + 1`) becomes **leader** and sends heartbeats.
- Any message with a higher term makes a node step down to follower.
- Timeouts are deterministic (`base + nodeId * spread`), so runs are reproducible.

## Log replication and commitment

- The leader sends `APPEND_ENTRIES` with `prevLogIndex`, `prevLogTerm`, new entries and its commit index.
- A follower accepts only if it holds `prevLogIndex` with `prevLogTerm`. It deletes conflicting entries that are not committed, appends the new ones, and replies with how far its log now matches.
- The leader tracks `nextIndex` and `matchIndex` per follower; on a rejection it moves `nextIndex` back (using the follower's hint) and retries.
- An entry is **committed** when a majority holds it **and** it is from the leader's current term; earlier entries commit with it.
- Committed entries are handed to the apply callback once each, in log order (`lastApplied` follows `commitIndex`).
- A restarted node keeps its term, vote and log, and catches up from the leader.

## Order state machine (`OrderBook`)

Applying entry `i` with command `CREATE_ORDER|id|product|qty`:

```
if i <= lastAppliedIndex: ignore (already applied)
lastAppliedIndex = i
if id already used:            REJECTED (duplicate order ID)
else if product not in stock:  REJECTED (unknown product)
else if stock[product] < qty:  REJECTED (insufficient stock)
else: stock[product] -= qty;   ACCEPTED
```

Every node runs the same steps on the same committed entries in the same order, so the result is the same everywhere. That is how two customers ordering the last laptop are resolved: the order whose entry comes first in the log gets it, on every node.

## Consistent hashing (`ConsistentHashRing`, `LoadBalancer`)

- The ring has 2^32 positions. Each server is placed on it 50 times ("virtual nodes": the strings `S1#0` ... `S1#49` are hashed), which spreads the servers evenly.
- Hash: 32-bit FNV-1a (start at 2166136261; for each byte, XOR it in and multiply by 16777619), followed by a standard mixing step so that similar strings land far apart.
- The ring is a sorted `std::map<position, serverId>`. To route a customer: hash the customer ID, `lower_bound` to the first position at or after it (wrapping to the start), and take that server; if it is stopped, keep going clockwise. Lookup is O(log n).

```
route(customerId):
    p = hash(customerId)
    walk the ring clockwise from the first position >= p
    return the first server that is running   (-1 if none)
```

Properties (checked by `test_consistent_hash`):

- the same customer always maps to the same server;
- 3000 customers spread about evenly over 3 servers;
- when a server stops, only its customers move (to the next server); when it restarts, they move back;
- adding a fourth server moves only about a quarter of the customers, all to the new server.

The ring is used for **reads** only. Writes skip it: the load balancer sends every order to the server whose Raft node is the leader, because only the leader may append to the log. Commitment is still decided by Raft.

## Read freshness

A read is answered from one server's copy without a Raft round, so it can lag behind the leader. `OrderService::read` marks it possibly stale unless the leader knows its commit point *and* the server has applied up to it:

```
leaderSure = leader exists and
             (leader.lastLogIndex == leader.commitIndex            // nothing pending
              or term(leader.log[commitIndex]) == leader.currentTerm)
stale      = not leaderSure or server.lastApplied < leader.commitIndex
```

Why the `leaderSure` part: a newly elected leader may hold entries from an earlier term that the old leader had already committed, but Raft's current-term rule only lets it count them after it commits an entry of its own term. Until then its commit index can be too low, and comparing against it would wrongly call an old answer "up to date". `test_routed_orders` covers this case (leader crashes right after a commit).

Trade-off: these reads stay available during an election, but are not linearizable. A linearizable alternative is Raft's ReadIndex (leader confirms its leadership with a majority, then serves the read once it has applied up to its commit index), at the cost of a round of messages per read.
