#include <map>
#include <string>
#include "OrderService.h"
#include "replication_test_utils.h"

struct FailoverFixture {
    RaftCluster  cluster{3, 50, 20, 10};
    OrderService service{cluster};
    int leader = -1;

    FailoverFixture()
    {
        service.setStock("LAPTOP", 2);
        service.setStock("MOUSE", 3);
        cluster.startAll();
        leader = service.waitForLeader(500);
    }
};

// ── A follower forwards to the leader ──────────────────────────────

void test_follower_forwards()
{
    std::cout << "\n--- Test: Follower Forwards to the Leader ---\n";

    FailoverFixture f;
    int follower = (f.leader % 3) + 1;
    SubmitResult r = f.service.submitOrder(follower, "O101", "LAPTOP", 1);

    check(r.forwarded && r.receivingNode == follower && r.leaderNode == f.leader,
          "Request received by a follower and forwarded to the leader");
    check(f.cluster.getStorage(follower)->getLastLogIndex() >= r.logIndex,
          "Entry replicated back to the follower");
    check(r.outcome == SubmitOutcome::COMMITTED &&
          r.order.status == OrderStatus::ACCEPTED, "Order committed and accepted");

    SubmitResult direct = f.service.submitOrder(f.leader, "O102", "MOUSE", 1);
    check(!direct.forwarded, "Request sent to the leader is not forwarded");
}

// ── Leader failure, re-election, then a new order ──────────────────

void test_order_after_leader_failure()
{
    std::cout << "\n--- Test: Order After Leader Failure ---\n";

    FailoverFixture f;
    f.service.submitOrder(f.leader, "O101", "LAPTOP", 1);
    int oldLeader = f.leader;
    int oldTerm   = f.cluster.getEngine(oldLeader)->getCurrentTerm();

    f.cluster.stopNode(oldLeader);
    SubmitResult toDown = f.service.submitOrder(oldLeader, "O102", "LAPTOP", 1);
    check(toDown.outcome == SubmitOutcome::NODE_DOWN,
          "Order sent to the stopped leader: NODE_DOWN");

    int newLeader = f.service.waitForLeader(500);
    check(newLeader != -1 && newLeader != oldLeader, "A new leader was elected");
    if (newLeader == -1) return;
    check(f.cluster.getEngine(newLeader)->getCurrentTerm() > oldTerm,
          "New leader has a higher term");

    SubmitResult r = f.service.submitOrder(newLeader, "O103", "LAPTOP", 1);
    check(r.outcome == SubmitOutcome::COMMITTED &&
          r.order.status == OrderStatus::ACCEPTED,
          "New leader commits O103 with 2 of 3 nodes");
    check(f.service.getBook(newLeader).getStock("LAPTOP") == 0,
          "Both O101 (old leader) and O103 (new leader) counted: stock 0");
}

// ── No leader: request is refused, not faked ───────────────────────

void test_no_leader()
{
    std::cout << "\n--- Test: No Leader Available ---\n";

    FailoverFixture f;
    int survivor = (f.leader % 3) + 1;
    for (int id = 1; id <= 3; id++) {
        if (id != survivor) f.cluster.stopNode(id);
    }
    f.service.waitForLeader(300);   // one node alone cannot win

    SubmitResult r = f.service.submitOrder(survivor, "O101", "LAPTOP", 1);
    check(r.outcome == SubmitOutcome::NO_LEADER ||
          r.outcome == SubmitOutcome::NOT_COMMITTED,
          "Lone node cannot accept the order (no leader / no majority)");
    check(f.service.getBook(survivor).getStock("LAPTOP") == 2 &&
          f.service.getBook(survivor).getOrders().empty(),
          "Inventory unchanged");
}

// ── Restarted node catches up, applies each entry once ─────────────

void test_restart_catch_up_once()
{
    std::cout << "\n--- Test: Restarted Node Catches Up Without Double Apply ---\n";

    FailoverFixture f;
    int follower = (f.leader % 3) + 1;
    f.service.submitOrder(f.leader, "O101", "LAPTOP", 1);
    f.service.runTicks(30);

    f.cluster.stopNode(follower);
    f.service.submitOrder(f.leader, "O102", "MOUSE", 2);
    f.service.submitOrder(f.leader, "O103", "LAPTOP", 1);
    check(f.service.getBook(follower).getOrders().size() == 1,
          "Stopped follower only has O101");

    f.cluster.startNode(follower);
    f.service.runTicks(50);

    const OrderBook& book = f.service.getBook(follower);
    check(book.getOrders().size() == 3, "Follower now has all 3 orders");
    check(book.getStock("LAPTOP") == 0 && book.getStock("MOUSE") == 1,
          "Follower stock: LAPTOP 0, MOUSE 1 (each order applied once)");
    check(f.service.replicasAgree(), "All nodes identical after catch-up");
}

// ── The scripted demo checks its own results ───────────────────────

void test_scripted_demo()
{
    std::cout << "\n--- Test: Scripted Demo Scenario ---\n";

    check(runOrderDemo(false), "Scripted demo: every expected result happened");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Order Failover Tests\n";
    std::cout << "========================================\n";

    test_follower_forwards();
    test_order_after_leader_failure();
    test_no_leader();
    test_restart_catch_up_once();
    test_scripted_demo();

    return finish("Order Failover Tests");
}