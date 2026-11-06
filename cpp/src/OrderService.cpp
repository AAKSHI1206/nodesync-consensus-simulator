#include "OrderService.h"
#include <iostream>
#include <streambuf>

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
        std::cout << "[CLIENT] Order " << orderId << " (" << product << " x"
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

bool runOrderDemo(bool verbose)
{
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

    section("1. Start 3 nodes and elect a leader");
    cluster.startAll();
    int leader = service.waitForLeader(500);
    expect(leader != -1, "a leader was elected");
    if (leader == -1) return false;
    if (verbose) {
        cluster.printState();
        service.printInventory(leader);
    }
    int follower = (leader % 3) + 1;

    section("2. O101: 1 laptop, sent to a follower");
    SubmitResult r1 = service.submitOrder(follower, "O101", "LAPTOP", 1);
    expect(r1.forwarded, "follower forwarded the order to the leader");
    expect(r1.outcome == SubmitOutcome::COMMITTED &&
           r1.order.status == OrderStatus::ACCEPTED, "O101 accepted");
    expect(service.getBook(leader).getStock("LAPTOP") == 1, "LAPTOP stock 2 -> 1");

    section("3. O102: 1 laptop");
    SubmitResult r2 = service.submitOrder(leader, "O102", "LAPTOP", 1);
    expect(r2.outcome == SubmitOutcome::COMMITTED &&
           r2.order.status == OrderStatus::ACCEPTED, "O102 accepted");
    expect(service.getBook(leader).getStock("LAPTOP") == 0, "LAPTOP stock 1 -> 0");

    section("4. O103: 1 laptop, but none left");
    SubmitResult r3 = service.submitOrder(leader, "O103", "LAPTOP", 1);
    expect(r3.outcome == SubmitOutcome::COMMITTED &&
           r3.order.status == OrderStatus::REJECTED, "O103 rejected (insufficient stock)");
    expect(service.getBook(leader).getStock("LAPTOP") == 0, "LAPTOP stock stays 0");

    section("5. Leader fails; a new leader is elected");
    if (verbose) std::cout << "Stopping leader Node " << leader << "\n";
    cluster.stopNode(leader);
    int newLeader = service.waitForLeader(500);
    expect(newLeader != -1 && newLeader != leader, "a different node became leader");
    if (newLeader == -1) return false;
    if (verbose) cluster.printState();

    section("6. O104: 2 mice, handled by the new leader");
    SubmitResult r4 = service.submitOrder(newLeader, "O104", "MOUSE", 2);
    expect(r4.outcome == SubmitOutcome::COMMITTED &&
           r4.order.status == OrderStatus::ACCEPTED, "O104 accepted by the new leader");
    expect(service.getBook(newLeader).getStock("MOUSE") == 1, "MOUSE stock 3 -> 1");

    section("7. Old leader restarts and catches up");
    cluster.startNode(leader);
    service.runTicks(50);
    expect(service.getBook(leader).getStock("MOUSE") == 1,
           "old leader applied O104 after restarting");
    expect(service.replicasAgree(), "all 3 nodes have identical inventory and orders");
    if (verbose) {
        cluster.printState();
        service.printInventory(newLeader);
        service.printOrders(newLeader);
        std::cout << "\nDemo " << (ok ? "completed: every result matched."
                                      : "found a MISMATCH (see above).") << "\n";
    }
    return ok;
}
