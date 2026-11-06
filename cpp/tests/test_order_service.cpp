/*
 * test_order_service.cpp
 * ======================
 * Orders through the real Raft path: an order becomes a log entry on
 * the leader, is applied only after commitment, and its result comes
 * from the OrderBook — never from the request itself.
 */

#include <string>
#include "OrderService.h"
#include "replication_test_utils.h"

// 3-node cluster with a leader and the demo inventory.
struct OrderFixture {
    RaftCluster  cluster{3, 50, 20, 10};
    OrderService service{cluster};
    int leader = -1;

    OrderFixture()
    {
        service.setStock("LAPTOP", 2);
        service.setStock("MOUSE", 3);
        cluster.startAll();
        leader = service.waitForLeader(500);
    }

    int follower(int n = 1) const { return ((leader + n - 1) % 3) + 1; }
};

// Applied progress never runs ahead of commitment, on any node.
bool appliedWithinCommit(RaftCluster& cluster, OrderService& service)
{
    for (int id = 1; id <= cluster.getClusterSize(); id++) {
        if (service.getBook(id).getLastAppliedIndex() >
            cluster.getEngine(id)->getCommitIndex()) {
            return false;
        }
    }
    return true;
}

// ── Order enters the Raft log ──────────────────────────────────────

void test_order_goes_through_raft()
{
    std::cout << "\n--- Test: Order Becomes a Committed Raft Entry ---\n";

    OrderFixture f;
    check(f.leader != -1, "Leader elected");

    SubmitResult r = f.service.submitOrder(f.leader, "O101", "LAPTOP", 1);
    const LogEntry* entry = f.cluster.getStorage(f.leader)->getEntry(r.logIndex);

    check(r.outcome == SubmitOutcome::COMMITTED, "Outcome COMMITTED");
    check(r.order.status == OrderStatus::ACCEPTED, "Order ACCEPTED");
    check(entry && std::string(entry->command) == "CREATE_ORDER|O101|LAPTOP|1",
          "Leader's Raft log holds the order command");
    check(f.cluster.getEngine(f.leader)->getCommitIndex() >= r.logIndex,
          "Leader's commitIndex covers the entry");
    check(f.service.getBook(f.leader).getStock("LAPTOP") == 1,
          "Leader's inventory updated after commit");
    check(appliedWithinCommit(f.cluster, f.service), "Applied <= commit on every node");
}

// ── Nothing is applied without a majority ──────────────────────────

void test_no_majority_no_effect()
{
    std::cout << "\n--- Test: No Majority, No Inventory Change ---\n";

    OrderFixture f;
    f.cluster.stopNode(f.follower(1));
    f.cluster.stopNode(f.follower(2));

    SubmitResult r = f.service.submitOrder(f.leader, "O101", "LAPTOP", 1);

    check(r.outcome == SubmitOutcome::NOT_COMMITTED,
          "Outcome NOT_COMMITTED (never reported as success)");
    check(f.cluster.getStorage(f.leader)->getLastLogIndex() == r.logIndex,
          "Entry is in the leader's log ...");
    check(f.cluster.getEngine(f.leader)->getCommitIndex() < r.logIndex,
          "... but not committed");
    bool untouched = true;
    for (int id = 1; id <= 3; id++) {
        if (f.service.getBook(id).getStock("LAPTOP") != 2 ||
            !f.service.getBook(id).getOrders().empty()) untouched = false;
    }
    check(untouched, "Inventory and orders unchanged on every node");
    check(appliedWithinCommit(f.cluster, f.service), "Applied <= commit on every node");

    // A follower comes back: the same entry commits and is applied once.
    f.cluster.startNode(f.follower(1));
    f.service.runTicks(50);
    const Order* o = f.service.getBook(f.leader).getOrder("O101");
    check(o && o->status == OrderStatus::ACCEPTED,
          "After the majority returns, O101 commits and is ACCEPTED");
    check(f.service.getBook(f.leader).getStock("LAPTOP") == 1,
          "Stock deducted exactly once");
}

// ── Malformed and unroutable requests ──────────────────────────────

void test_invalid_requests_never_reach_log()
{
    std::cout << "\n--- Test: Invalid Requests Never Reach the Log ---\n";

    OrderFixture f;
    int logBefore = f.cluster.getStorage(f.leader)->getLastLogIndex();

    SubmitResult zero  = f.service.submitOrder(f.leader, "O1", "LAPTOP", 0);
    SubmitResult minus = f.service.submitOrder(f.leader, "O2", "LAPTOP", -3);
    SubmitResult pipe  = f.service.submitOrder(f.leader, "O|3", "LAPTOP", 1);
    SubmitResult empty = f.service.submitOrder(f.leader, "", "LAPTOP", 1);

    check(zero.outcome  == SubmitOutcome::INVALID_REQUEST &&
          minus.outcome == SubmitOutcome::INVALID_REQUEST &&
          pipe.outcome  == SubmitOutcome::INVALID_REQUEST &&
          empty.outcome == SubmitOutcome::INVALID_REQUEST,
          "Quantity 0, negative quantity, '|' in ID, empty ID: all INVALID_REQUEST");
    check(f.cluster.getStorage(f.leader)->getLastLogIndex() == logBefore,
          "No log entries were added");
    check(f.service.getBook(f.leader).getStock("LAPTOP") == 2, "Stock unchanged");

    SubmitResult unknown = f.service.submitOrder(9, "O4", "LAPTOP", 1);
    check(unknown.outcome == SubmitOutcome::NODE_DOWN, "Unknown node: NODE_DOWN");
}

// ── Log order decides the race for the last items ──────────────────

void test_log_order_decides()
{
    std::cout << "\n--- Test: Log Order Decides Who Gets the Last Laptops ---\n";

    OrderFixture f;
    SubmitResult a = f.service.submitOrder(f.follower(1), "O1", "LAPTOP", 1);
    SubmitResult b = f.service.submitOrder(f.follower(2), "O2", "LAPTOP", 1);
    SubmitResult c = f.service.submitOrder(f.leader,      "O3", "LAPTOP", 1);
    f.service.runTicks(30);   // followers learn the last commit index

    check(a.logIndex < b.logIndex && b.logIndex < c.logIndex,
          "Entries appended in submission order");
    check(a.order.status == OrderStatus::ACCEPTED &&
          b.order.status == OrderStatus::ACCEPTED &&
          c.order.status == OrderStatus::REJECTED,
          "First two accepted, third rejected (stock 2)");
    check(f.service.replicasAgree(), "All nodes reached the same state");
    bool stockZero = true;
    for (int id = 1; id <= 3; id++) {
        if (f.service.getBook(id).getStock("LAPTOP") != 0) stockZero = false;
    }
    check(stockZero, "LAPTOP stock 0 on every node, never negative");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Order Service (Raft Integration) Tests\n";
    std::cout << "========================================\n";

    test_order_goes_through_raft();
    test_no_majority_no_effect();
    test_invalid_requests_never_reach_log();
    test_log_order_decides();

    return finish("Order Service (Raft Integration) Tests");
}
