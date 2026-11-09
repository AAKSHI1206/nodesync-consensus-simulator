#include "OrderService.h"
#include "LoadBalancer.h"
#include <iostream>
#include <memory>
#include <streambuf>
#include <vector>

/*
 * OrderService.cpp
 * ================
 * Client orders -> Raft leader -> commit -> OrderBook on every node.
 */

// Discards std::cout output while it exists.  Used while the cluster
// runs, so the "[Network] Warning: receiver X is inactive" lines for
// stopped nodes do not flood the demo.
class QuietCout {
public:
    QuietCout() : old_(std::cout.rdbuf(&sink_)) {}
    ~QuietCout() { std::cout.rdbuf(old_); }

private:
    struct NullBuffer : std::streambuf {
        int overflow(int c) override { return c; }
    };
    NullBuffer      sink_;   // declared first: old_ needs it
    std::streambuf* old_;
};

std::string submitOutcomeToString(SubmitOutcome outcome)
{
    switch (outcome) {
        case SubmitOutcome::COMMITTED:       return "COMMITTED";
        case SubmitOutcome::NOT_COMMITTED:   return "NOT_COMMITTED";
        case SubmitOutcome::INVALID_REQUEST: return "INVALID_REQUEST";
        case SubmitOutcome::NODE_DOWN:       return "NODE_DOWN";
        case SubmitOutcome::NO_LEADER:       return "NO_LEADER";
        default:                             return "UNKNOWN";
    }
}

std::string readOutcomeToString(ReadOutcome outcome)
{
    switch (outcome) {
        case ReadOutcome::OK:              return "OK";
        case ReadOutcome::NOT_FOUND:       return "NOT_FOUND";
        case ReadOutcome::INVALID_REQUEST: return "INVALID_REQUEST";
        case ReadOutcome::NODE_DOWN:       return "NODE_DOWN";
        default:                           return "UNKNOWN";
    }
}

// ── Setup ──────────────────────────────────────────────────────────

OrderService::OrderService(RaftCluster& cluster)
    : cluster_(cluster),
      books_(cluster.getClusterSize())
{
    // Each node's committed entries go to that node's own OrderBook.
    for (int id = 1; id <= cluster_.getClusterSize(); id++) {
        cluster_.getEngine(id)->setApplyCallback(
            [this, id](int index, int, const std::string& command) {
                onCommittedEntry(id, index, command);
            });
    }
}

OrderService::~OrderService()
{
    // The engines must not call back into a destroyed service.
    for (int id = 1; id <= cluster_.getClusterSize(); id++) {
        cluster_.getEngine(id)->setApplyCallback(nullptr);
    }
}

void OrderService::setStock(const std::string& product, int quantity)
{
    for (OrderBook& book : books_) {
        book.setStock(product, quantity);
    }
}

void OrderService::setVerbose(bool verbose) { verbose_ = verbose; }
void OrderService::setMaxWaitTicks(int ticks) { maxWaitTicks_ = ticks; }

const OrderBook& OrderService::getBook(int nodeId) const
{
    return books_[nodeId - 1];
}

bool OrderService::replicasAgree() const
{
    for (const OrderBook& book : books_) {
        if (!book.sameStateAs(books_[0])) return false;
    }
    return true;
}

// ── Apply callback (called by RaftEngine for committed entries) ────

void OrderService::onCommittedEntry(int nodeId, int index,
                                    const std::string& command)
{
    ApplyResult result = books_[nodeId - 1].apply(index, command);
    if (!result.applied || !result.isOrder) return;

    std::string line = "Node " + std::to_string(nodeId) + " applied #" +
                       std::to_string(index) + ": " + result.order.id + " " +
                       orderStatusToString(result.order.status);
    if (!result.order.reason.empty()) line += " (" + result.order.reason + ")";
    applyEvents_.push_back(line);

    if (nodeId == waitNode_ && index == waitIndex_) {
        waitDone_  = true;
        waitOrder_ = result.order;
    }
}

// ── Simulation helpers ─────────────────────────────────────────────

void OrderService::runTicks(int ticks)
{
    QuietCout quiet;
    cluster_.runFor(ticks);
}

int OrderService::waitForLeader(int maxTicks)
{
    QuietCout quiet;
    for (int t = 0; t < maxTicks && cluster_.findLeader() == -1; t++) {
        cluster_.runTick();
    }
    return cluster_.findLeader();
}

// ── Submit an order ────────────────────────────────────────────────

SubmitResult OrderService::submitOrder(int receivingNode,
                                       const std::string& orderId,
                                       const std::string& product,
                                       int quantity)
{
    SubmitResult r;
    r.receivingNode = receivingNode;
    std::string command = OrderBook::makeCreateOrder(orderId, product, quantity);

    if (verbose_) {
        std::cout << "[ORDER] " << orderId << " (" << product << " x"
                  << quantity << ") sent to Node " << receivingNode << "\n";
    }

    // 1. Reject malformed requests before they reach the log.
    std::string id, prod;
    int qty = 0;
    if (!OrderBook::parseCreateOrder(command, id, prod, qty) ||
        id != orderId || prod != product) {
        r.outcome = SubmitOutcome::INVALID_REQUEST;
        r.message = "malformed order (need ID, product, quantity >= 1, no '|')";
        if (verbose_) std::cout << "[REJECTED] " << r.message << "\n";
        return r;
    }

    // 2. The receiving node must be running.
    Node* node = cluster_.getNode(receivingNode);
    if (node == nullptr || !node->isActive()) {
        r.outcome = SubmitOutcome::NODE_DOWN;
        r.message = "Node " + std::to_string(receivingNode) + " is not running";
        if (verbose_) std::cout << "[FAILED] " << r.message << "\n";
        return r;
    }

    // 3. Find the leader; a follower forwards the request to it.
    int leader = cluster_.findLeader();
    if (leader == -1) {
        r.outcome = SubmitOutcome::NO_LEADER;
        r.message = "no leader elected right now";
        if (verbose_) std::cout << "[FAILED] " << r.message << "\n";
        return r;
    }
    r.leaderNode = leader;
    r.forwarded  = (receivingNode != leader);
    if (verbose_) {
        if (r.forwarded) {
            std::cout << "[NODE " << receivingNode << "] I am a "
                      << raftRoleToString(cluster_.getEngine(receivingNode)->getRole())
                      << " -> forwarding to leader Node " << leader << "\n";
        } else {
            std::cout << "[NODE " << receivingNode << "] I am the LEADER\n";
        }
    }

    // 4. The leader appends the command (not committed yet).
    RaftEngine* engine = cluster_.getEngine(leader);
    int commitBefore = engine->getCommitIndex();
    {
        QuietCout quiet;
        r.logIndex = engine->propose(command);
    }
    if (r.logIndex == -1) {
        r.outcome = SubmitOutcome::NO_LEADER;
        r.message = "leader refused the command";
        if (verbose_) std::cout << "[FAILED] " << r.message << "\n";
        return r;
    }
    r.term = engine->getCurrentTerm();
    if (verbose_) {
        std::cout << "[LEADER " << leader << "] Appended log entry #"
                  << r.logIndex << " (term " << r.term << "): " << command
                  << "\n[LEADER " << leader << "] commitIndex = " << commitBefore
                  << " -> entry is NOT committed yet\n";
    }

    // 5. Let the cluster run until the leader has applied the entry.
    waitNode_  = leader;
    waitIndex_ = r.logIndex;
    waitDone_  = false;
    applyEvents_.clear();
    {
        QuietCout quiet;
        while (!waitDone_ && r.ticksWaited < maxWaitTicks_) {
            cluster_.runTick();
            r.ticksWaited++;
        }
    }
    waitNode_ = 0;

    // 6. Report what actually happened.
    if (verbose_) {
        int holders = 1;   // the leader's own copy
        std::cout << "[RAFT] Replication after " << r.ticksWaited << " tick(s):\n";
        for (int peer = 1; peer <= cluster_.getClusterSize(); peer++) {
            if (peer == leader) continue;
            int match = engine->getMatchIndex(peer);
            bool has = match >= r.logIndex;
            if (has) holders++;
            std::cout << "         Node " << peer << ": "
                      << (has ? "acknowledged" : "no acknowledgement")
                      << " (matchIndex " << match << ")"
                      << (cluster_.getNode(peer)->isActive() ? "" : " [stopped]")
                      << "\n";
        }
        std::cout << "[RAFT] " << holders << " of " << cluster_.getClusterSize()
                  << " nodes hold entry #" << r.logIndex << " (majority = "
                  << engine->getMajority() << "), commitIndex " << commitBefore
                  << " -> " << engine->getCommitIndex() << "\n";
        for (const std::string& line : applyEvents_) {
            std::cout << "[APPLY] " << line << "\n";
        }
    }

    if (!waitDone_) {
        r.outcome = SubmitOutcome::NOT_COMMITTED;
        r.message = "entry #" + std::to_string(r.logIndex) +
                    " has no majority yet; inventory unchanged";
        if (verbose_) std::cout << "[RESULT] " << orderId << " NOT COMMITTED: "
                                << r.message << "\n";
        return r;
    }

    r.outcome = SubmitOutcome::COMMITTED;
    r.order   = waitOrder_;
    r.message = orderStatusToString(r.order.status);
    if (verbose_) {
        std::cout << "[RESULT] " << orderId << " "
                  << orderStatusToString(r.order.status);
        if (!r.order.reason.empty()) std::cout << " (" << r.order.reason << ")";
        std::cout << " | " << product << " stock now "
                  << books_[leader - 1].getStock(product) << "\n";
    }
    return r;
}

// ── Read requests ──────────────────────────────────────────────────

static const std::string kGetStock = "GET_STOCK|";
static const std::string kGetOrder = "GET_ORDER|";

static bool startsWith(const std::string& text, const std::string& prefix)
{
    return text.compare(0, prefix.size(), prefix) == 0;
}

bool OrderService::isReadCommand(const std::string& command)
{
    return startsWith(command, kGetStock) || startsWith(command, kGetOrder);
}

std::string OrderService::makeGetStock(const std::string& product) { return kGetStock + product; }
std::string OrderService::makeGetOrder(const std::string& orderId) { return kGetOrder + orderId; }

ReadResult OrderService::read(int servingNode, const std::string& command)
{
    ReadResult r;
    r.servingNode = servingNode;

    // 1. Which read, and is it well-formed?  ("GET_STOCK|LAPTOP")
    bool stockRead = startsWith(command, kGetStock);
    std::string arg = isReadCommand(command)
                          ? command.substr(stockRead ? kGetStock.size() : kGetOrder.size())
                          : "";
    if (arg.empty() || arg.find('|') != std::string::npos) {
        r.outcome = ReadOutcome::INVALID_REQUEST;
        r.message = "malformed read (use GET_STOCK|<product> or GET_ORDER|<orderId>)";
        if (verbose_) std::cout << "[READ] Rejected: " << r.message << "\n";
        return r;
    }

    // 2. The serving node must be running.
    Node* node = cluster_.getNode(servingNode);
    if (node == nullptr || !node->isActive()) {
        r.outcome = ReadOutcome::NODE_DOWN;
        r.message = "Node " + std::to_string(servingNode) + " is not running";
        if (verbose_) std::cout << "[READ] Failed: " << r.message << "\n";
        return r;
    }

    // 3. Answer from this node's own copy of the state (no Raft round).
    const OrderBook& book = books_[servingNode - 1];
    if (stockRead) {
        if (book.getInventory().count(arg) == 0) {
            r.outcome = ReadOutcome::NOT_FOUND;
            r.message = "unknown product " + arg;
        } else {
            r.outcome = ReadOutcome::OK;
            r.stock   = book.getStock(arg);
            r.message = arg + " stock = " + std::to_string(r.stock);
        }
    } else {
        const Order* order = book.getOrder(arg);
        if (order == nullptr) {
            r.outcome = ReadOutcome::NOT_FOUND;
            r.message = "no order " + arg + " on this server";
        } else {
            r.outcome = ReadOutcome::OK;
            r.order   = *order;
            r.message = arg + " is " + orderStatusToString(order->status);
            if (!order->reason.empty()) r.message += " (" + order->reason + ")";
        }
    }

    // 4. How fresh is that copy?  Compare with what the leader has committed.
    //    A newly elected leader does not yet know the full commit point:
    //    entries from earlier terms in its log may already be committed,
    //    but Raft only lets it count them once it commits an entry of its
    //    OWN term (current-term rule).  Until then its commit index can be
    //    too low, so the comparison would wrongly say "up to date".
    r.appliedIndex = book.getLastAppliedIndex();
    int leader = cluster_.findLeader();
    bool leaderSure = false;
    if (leader != -1) {
        RaftEngine* engine = cluster_.getEngine(leader);
        const Storage* log = cluster_.getStorage(leader);
        r.leaderCommit = engine->getCommitIndex();
        const LogEntry* atCommit = log->getEntry(r.leaderCommit);
        leaderSure = log->getLastLogIndex() == r.leaderCommit ||   // nothing pending
                     (atCommit != nullptr && atCommit->term == engine->getCurrentTerm());
    }
    r.possiblyStale = !leaderSure || r.appliedIndex < r.leaderCommit;

    if (verbose_) {
        std::cout << "[READ] Node " << servingNode << " answered from its own copy: "
                  << r.message << "\n";
        std::cout << "[READ] Freshness: applied up to #" << r.appliedIndex;
        if (leader == -1)                         std::cout << ", no leader to compare with -> MAY BE STALE";
        else if (!leaderSure)                     std::cout << ", leader has not committed an entry of its own term yet, so it cannot confirm the commit point -> MAY BE STALE";
        else if (r.appliedIndex < r.leaderCommit) std::cout << ", leader committed #" << r.leaderCommit << " -> STALE (not caught up yet)";
        else                                      std::cout << ", leader committed #" << r.leaderCommit << " -> up to date";
        std::cout << "\n";
    }
    return r;
}

// ── Printing ───────────────────────────────────────────────────────

void OrderService::printInventory(int nodeId) const
{
    std::cout << "Inventory on Node " << nodeId << ":\n";
    for (const auto& item : getBook(nodeId).getInventory()) {
        std::cout << "  " << item.first << ": " << item.second << "\n";
    }
}

void OrderService::printOrders(int nodeId) const
{
    const auto& orders = getBook(nodeId).getOrders();
    std::cout << "Orders on Node " << nodeId << ":\n";
    if (orders.empty()) std::cout << "  (none)\n";
    for (const auto& entry : orders) {
        const Order& o = entry.second;
        std::cout << "  " << o.id << " | " << o.product << " x" << o.quantity
                  << " | " << orderStatusToString(o.status);
        if (!o.reason.empty()) std::cout << " (" << o.reason << ")";
        std::cout << " | log #" << o.logIndex << "\n";
    }
}

// ── Scripted demo ──────────────────────────────────────────────────

// The full path through the load balancer:
//   write: customer -> LoadBalancer -> leader's Server -> Raft -> commit
//          -> every node's OrderBook
//   read:  customer -> LoadBalancer (hash ring) -> that Server, which
//          answers from its own OrderBook
bool runOrderDemo(bool verbose)
{
    std::unique_ptr<QuietCout> quiet;
    if (!verbose) quiet = std::make_unique<QuietCout>();   // tests: no output

    bool ok = true;
    auto expect = [&](bool condition, const std::string& what) {
        if (verbose) std::cout << (condition ? "  [OK] " : "  [MISMATCH] ") << what << "\n";
        if (!condition) ok = false;
    };
    auto section = [&](const std::string& title) {
        if (verbose) std::cout << "\n--- " << title << " ---\n";
    };

    RaftCluster cluster(3, 50, 20, 10);
    OrderService service(cluster);
    service.setVerbose(verbose);
    service.setStock("LAPTOP", 2);
    service.setStock("MOUSE", 3);

    // Three Raft-backed servers behind one load balancer.
    Network unusedNet;
    std::vector<std::unique_ptr<Server>> servers;
    LoadBalancer lb(RoutingPolicy::CONSISTENT_HASH);
    for (int id = 1; id <= 3; id++) {
        servers.push_back(std::make_unique<Server>(id, &unusedNet));
        servers.back()->attachToCluster(&cluster, &service);
        lb.registerServer(servers.back().get());
    }
    auto customerOn = [&](int serverId) {
        for (int n = 1; n < 1000; n++) {
            std::string c = "C" + std::to_string(n);
            if (lb.serverFor(c) == serverId) return c;
        }
        return std::string();
    };
    auto order = [&](const std::string& customer, const std::string& id,
                     const std::string& product, int qty) {
        bool routed = lb.routeRequest(ClientRequest{id, OrderBook::makeCreateOrder(id, product, qty), customer});
        if (lb.getLastServerId() == -1) {
            SubmitResult none;
            none.message = routed ? "" : "refused by the load balancer";
            return none;
        }
        return servers[lb.getLastServerId() - 1]->getLastResult();
    };
    int readNo = 1;
    auto read = [&](const std::string& customer, const std::string& command) {
        lb.routeRequest(ClientRequest{"R" + std::to_string(readNo++), command, customer});
        if (lb.getLastServerId() == -1) return ReadResult();
        return servers[lb.getLastServerId() - 1]->getLastRead();
    };
    auto routingTable = [&]() {
        if (!verbose) return;
        std::cout << "Reads (hash ring on customer ID):";
        for (int n = 1; n <= 6; n++) {
            std::string c = "C" + std::to_string(n);
            int s = lb.serverFor(c);
            std::cout << "  " << c << "->" << (s == -1 ? std::string("none") : "S" + std::to_string(s));
        }
        int l = lb.leaderServer();
        std::cout << "\nWrites: always -> " << (l == -1 ? std::string("(no leader)") : "leader Server " + std::to_string(l)) << "\n";
    };

    section("1. Start 3 servers and elect a Raft leader");
    cluster.startAll();
    int leader = service.waitForLeader(500);
    expect(leader != -1, "a leader was elected");
    if (leader == -1) return false;
    if (verbose) {
        cluster.printState();
        service.printInventory(leader);
    }
    routingTable();
    int follower = (leader % 3) + 1;
    std::string onFollower = customerOn(follower);
    std::string onLeader = customerOn(leader);

    section("2. Customer " + onFollower + " orders 1 laptop (a write goes to the leader)");
    SubmitResult r1 = order(onFollower, "O101", "LAPTOP", 1);
    expect(lb.getLastServerId() == leader, "load balancer sent the write to leader Server " + std::to_string(leader));
    expect(!r1.forwarded, "no forwarding needed: it arrived at the leader");
    expect(r1.outcome == SubmitOutcome::COMMITTED &&
           r1.order.status == OrderStatus::ACCEPTED, "O101 committed by a majority and accepted");
    expect(service.getBook(leader).getStock("LAPTOP") == 1, "LAPTOP stock 2 -> 1 on the leader");

    section("3. " + onFollower + " checks the laptop stock (a read goes to its hashed server)");
    ReadResult q1 = read(onFollower, OrderService::makeGetStock("LAPTOP"));
    expect(lb.getLastServerId() == follower, "hash ring sent the read to Server " + std::to_string(follower));
    expect(q1.outcome == ReadOutcome::OK, "Server " + std::to_string(follower) + " answered from its own copy");
    // Right after the commit the follower may not have heard the new commit
    // index yet (it arrives with the next AppendEntries): then the answer is
    // the old value and is marked stale.  Either way it must be consistent.
    expect(q1.possiblyStale ? q1.stock == 2 : q1.stock == 1,
           q1.possiblyStale ? "answer 2 is marked STALE (follower has not applied O101 yet)"
                            : "answer 1 is up to date");
    service.runTicks(20);
    ReadResult q2 = read(onFollower, OrderService::makeGetStock("LAPTOP"));
    expect(q2.outcome == ReadOutcome::OK && !q2.possiblyStale && q2.stock == 1,
           "after the next heartbeat the follower answers 1, up to date");

    section("4. Customer " + onLeader + " orders the last laptop");
    SubmitResult r2 = order(onLeader, "O102", "LAPTOP", 1);
    expect(r2.outcome == SubmitOutcome::COMMITTED &&
           r2.order.status == OrderStatus::ACCEPTED, "O102 accepted");
    expect(service.getBook(leader).getStock("LAPTOP") == 0, "LAPTOP stock 1 -> 0");

    section("5. " + onFollower + " orders 1 more laptop, but none are left");
    SubmitResult r3 = order(onFollower, "O103", "LAPTOP", 1);
    expect(lb.getLastServerId() == leader, "the write again went to the leader");
    expect(r3.outcome == SubmitOutcome::COMMITTED &&
           r3.order.status == OrderStatus::REJECTED, "O103 committed but rejected (insufficient stock)");
    service.runTicks(20);
    ReadResult q3 = read(onFollower, OrderService::makeGetOrder("O103"));
    expect(q3.outcome == ReadOutcome::OK && q3.order.status == OrderStatus::REJECTED,
           onFollower + " reads O103's status from Server " + std::to_string(follower) + ": REJECTED");

    section("6. Server " + std::to_string(follower) + " crashes; only its customers' reads move");
    std::string kept = customerOn(leader);
    servers[follower - 1]->stop();
    routingTable();
    int moved = lb.serverFor(onFollower);
    expect(moved != follower && moved != -1, onFollower + "'s reads now go to Server " + std::to_string(moved));
    expect(lb.serverFor(kept) == leader, kept + " still reads from Server " + std::to_string(leader));
    SubmitResult r4 = order(onFollower, "O104", "MOUSE", 1);
    expect(r4.outcome == SubmitOutcome::COMMITTED &&
           r4.order.status == OrderStatus::ACCEPTED, "O104 committed with 2 of 3 servers");

    section("7. Server " + std::to_string(follower) + " restarts: a read before it catches up is stale");
    servers[follower - 1]->start();
    expect(lb.serverFor(onFollower) == follower, onFollower + " is back on Server " + std::to_string(follower));
    ReadResult q4 = read(onFollower, OrderService::makeGetStock("MOUSE"));
    expect(q4.possiblyStale && q4.stock == 3,
           "it still says MOUSE 3 and marks it STALE (it missed O104 while down)");
    service.runTicks(50);
    ReadResult q5 = read(onFollower, OrderService::makeGetStock("MOUSE"));
    expect(!q5.possiblyStale && q5.stock == 2, "after catching up: MOUSE 2, up to date");

    section("8. The leader crashes: writes wait for a new leader, reads keep working");
    servers[leader - 1]->stop();
    SubmitResult r5 = order(onFollower, "O105", "MOUSE", 1);
    expect(lb.getLastServerId() == -1, "no leader yet: the load balancer refuses the write");
    ReadResult q6 = read(onFollower, OrderService::makeGetStock("MOUSE"));
    expect(q6.outcome == ReadOutcome::OK && q6.possiblyStale,
           "the read is still answered, marked 'may be stale' (no leader to compare with)");
    int newLeader = service.waitForLeader(500);
    expect(newLeader != -1 && newLeader != leader, "Raft elected a different leader");
    if (newLeader == -1) return false;
    if (verbose) cluster.printState();
    routingTable();
    SubmitResult r6 = order(onLeader, "O105", "MOUSE", 2);
    expect(lb.getLastServerId() == newLeader, "writes now go to the new leader Server " + std::to_string(newLeader));
    expect(r6.outcome == SubmitOutcome::COMMITTED &&
           r6.order.status == OrderStatus::ACCEPTED, "O105 accepted under the new leader");
    expect(service.getBook(newLeader).getStock("MOUSE") == 0, "MOUSE stock 2 -> 0");
    (void)r5;

    section("9. The old leader restarts and catches up");
    servers[leader - 1]->start();
    service.runTicks(50);
    expect(service.getBook(leader).getStock("MOUSE") == 0,
           "old leader applied every order after restarting");
    expect(service.replicasAgree(), "all 3 nodes have identical inventory and orders");
    if (verbose) {
        cluster.printState();
        service.printInventory(newLeader);
        service.printOrders(newLeader);
        std::cout << "Requests received per server:";
        for (auto& s : servers) std::cout << "  S" << s->getId() << "=" << s->getLoad();
        std::cout << "\n\nDemo " << (ok ? "completed: every result matched."
                                        : "found a MISMATCH (see above).") << "\n";
    }
    return ok;
}
