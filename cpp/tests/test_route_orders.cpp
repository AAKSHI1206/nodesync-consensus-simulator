/*
 * test_routed_orders.cpp
 * ======================
 * Requests through the load balancer (policy CONSISTENT_HASH):
 *   write: customer -> LoadBalancer -> leader's Server -> Raft commit
 *          -> every node's OrderBook
 *   read:  customer -> LoadBalancer (hash ring on the customer ID)
 *          -> that Server, which answers from its own OrderBook
 */

#include <memory>
#include <string>
#include <vector>
#include "LoadBalancer.h"
#include "replication_test_utils.h"

// 3-node Raft cluster fronted by 3 Raft-backed servers and one load
// balancer.
struct Frontend {
    RaftCluster  cluster{3, 50, 20, 10};
    OrderService service{cluster};
    Network      unusedNet;   // Server's legacy network (not used when attached)
    std::vector<std::unique_ptr<Server>> servers;
    LoadBalancer lb{RoutingPolicy::CONSISTENT_HASH};
    int leader = -1;
    int nextRead = 1;

    Frontend()
    {
        service.setStock("LAPTOP", 2);
        service.setStock("MOUSE", 3);
        for (int id = 1; id <= 3; id++) {
            servers.push_back(std::make_unique<Server>(id, &unusedNet));
            servers.back()->attachToCluster(&cluster, &service);
            lb.registerServer(servers.back().get());
        }
        cluster.startAll();
        leader = service.waitForLeader(500);
    }

    Server& server(int id) { return *servers[id - 1]; }
    int follower() const { return (leader % 3) + 1; }

    // First customer C1, C2, ... that the ring sends to `serverId`.
    std::string customerOn(int serverId)
    {
        for (int n = 1; n < 1000; n++) {
            std::string c = "C" + std::to_string(n);
            if (lb.serverFor(c) == serverId) return c;
        }
        return "";
    }

    bool order(const std::string& customer, const std::string& id,
               const std::string& product, int qty)
    {
        return lb.routeRequest(ClientRequest{id, OrderBook::makeCreateOrder(id, product, qty), customer});
    }

    bool read(const std::string& customer, const std::string& command)
    {
        return lb.routeRequest(ClientRequest{"R" + std::to_string(nextRead++), command, customer});
    }

    // Result held by the server that handled the last request (an empty
    // result if the load balancer gave it to no server).
    const SubmitResult& lastResult()
    {
        static const SubmitResult none;
        int id = lb.getLastServerId();
        return id < 1 ? none : server(id).getLastResult();
    }
    const ReadResult& lastRead()
    {
        static const ReadResult none;
        int id = lb.getLastServerId();
        return id < 1 ? none : server(id).getLastRead();
    }

    bool allStock(const std::string& product, int expected)
    {
        for (int id = 1; id <= 3; id++) {
            if (service.getBook(id).getStock(product) != expected) return false;
        }
        return true;
    }
};

// ── Writes go to the leader ────────────────────────────────────────

void test_write_goes_to_leader()
{
    std::cout << "\n--- Test: Write Goes to the Leader ---\n";

    Frontend f;
    check(f.leader != -1, "Leader elected");
    check(f.lb.leaderServer() == f.leader, "Load balancer finds the leader's server");
    std::string customer = f.customerOn(f.follower());
    check(!customer.empty(), "Found a customer whose hashed server is a follower");

    bool ok = f.order(customer, "O101", "LAPTOP", 1);
    const SubmitResult& r = f.lastResult();
    check(ok, "routeRequest reports success");
    check(f.lb.getLastServerId() == f.leader, "Write sent to the leader, not to the customer's hashed server");
    check(!r.forwarded, "No forwarding needed");
    check(r.outcome == SubmitOutcome::COMMITTED && r.order.status == OrderStatus::ACCEPTED,
          "Order committed by a majority and accepted");

    f.service.runTicks(30);
    check(f.allStock("LAPTOP", 1), "LAPTOP 2 -> 1 on every node");
    check(f.server(f.leader).getLoad() == 1 && f.server(f.follower()).getLoad() == 0,
          "The leader's server counted the write; the follower received nothing");
}

// ── Reads use the hash ring ────────────────────────────────────────

void test_read_uses_hash_ring()
{
    std::cout << "\n--- Test: Read Goes to the Customer's Hashed Server ---\n";

    Frontend f;
    std::string customer = f.customerOn(f.follower());
    int logBefore = f.cluster.getStorage(f.leader)->getLastLogIndex();

    bool ok = f.read(customer, OrderService::makeGetStock("LAPTOP"));
    const ReadResult& r = f.lastRead();
    check(ok, "routeRequest reports success");
    check(f.lb.getLastServerId() == f.follower(), "Read served by the hashed server (a follower)");
    check(r.outcome == ReadOutcome::OK && r.stock == 2 && r.servingNode == f.follower(),
          "Answer LAPTOP = 2 from the follower's own copy");
    check(!r.possiblyStale, "Nothing committed yet, so the answer is up to date");
    check(f.cluster.getStorage(f.leader)->getLastLogIndex() == logBefore,
          "Reads are not written to the Raft log");

    bool same = true;
    for (int i = 0; i < 3; i++) {
        f.read(customer, OrderService::makeGetStock("MOUSE"));
        if (f.lb.getLastServerId() != f.follower()) same = false;
    }
    check(same, "The same customer always reads from the same server");
}

void test_read_after_write_freshness()
{
    std::cout << "\n--- Test: Follower Read Right After a Write ---\n";

    Frontend f;
    std::string customer = f.customerOn(f.follower());
    f.order(customer, "O101", "LAPTOP", 1);

    // The follower learns the new commit index with the next AppendEntries,
    // so right now it may still answer the old value.  The answer must be
    // honest about it.
    f.read(customer, OrderService::makeGetStock("LAPTOP"));
    const ReadResult& now = f.lastRead();
    check(now.outcome == ReadOutcome::OK, "Read answered");
    check(now.possiblyStale == (now.appliedIndex < now.leaderCommit),
          "Stale flag = follower applied less than the leader committed");
    check(now.possiblyStale ? now.stock == 2 : now.stock == 1,
          "Answer matches what the follower has applied");

    f.service.runTicks(30);
    f.read(customer, OrderService::makeGetStock("LAPTOP"));
    check(!f.lastRead().possiblyStale && f.lastRead().stock == 1,
          "After a heartbeat the follower answers 1, up to date");

    f.read(customer, OrderService::makeGetOrder("O101"));
    check(f.lastRead().outcome == ReadOutcome::OK &&
          f.lastRead().order.status == OrderStatus::ACCEPTED, "Order status readable: ACCEPTED");
}

void test_stale_read_after_restart()
{
    std::cout << "\n--- Test: Read From a Server That Has Not Caught Up ---\n";

    Frontend f;
    int fol = f.follower();
    std::string customer = f.customerOn(fol);

    f.server(fol).stop();
    check(f.order(customer, "O201", "MOUSE", 1), "Write commits with 2 of 3 nodes");
    f.server(fol).start();   // back, but missed O201

    f.read(customer, OrderService::makeGetStock("MOUSE"));
    check(f.lb.getLastServerId() == fol, "Customer is back on its server");
    check(f.lastRead().possiblyStale && f.lastRead().stock == 3,
          "Answer MOUSE = 3 is marked STALE (it missed O201)");

    f.service.runTicks(50);
    f.read(customer, OrderService::makeGetStock("MOUSE"));
    check(!f.lastRead().possiblyStale && f.lastRead().stock == 2,
          "After catching up: MOUSE = 2, up to date");
}

// ── Failures ───────────────────────────────────────────────────────

void test_server_crash_moves_only_its_reads()
{
    std::cout << "\n--- Test: Server Crash Moves Only Its Customers ---\n";

    Frontend f;
    int fol = f.follower();
    std::string customer = f.customerOn(fol);

    std::vector<std::string> others;
    std::vector<int> othersServer;
    for (int n = 1; n <= 200; n++) {
        std::string c = "C" + std::to_string(n);
        if (f.lb.serverFor(c) != fol) { others.push_back(c); othersServer.push_back(f.lb.serverFor(c)); }
    }

    f.server(fol).stop();
    check(!f.cluster.getNode(fol)->isActive(), "Stopping the server crashes its Raft node");
    int reroute = f.lb.serverFor(customer);
    check(reroute != fol && reroute != -1, "Customer's reads moved to another running server");

    bool othersKept = true;
    for (size_t i = 0; i < others.size(); i++) {
        if (f.lb.serverFor(others[i]) != othersServer[i]) othersKept = false;
    }
    check(othersKept, "Every other customer kept their server");

    f.read(customer, OrderService::makeGetStock("LAPTOP"));
    check(f.lb.getLastServerId() == reroute && f.lastRead().outcome == ReadOutcome::OK,
          "Read answered by the next server on the ring");

    f.server(fol).start();
    f.service.runTicks(50);
    check(f.lb.serverFor(customer) == fol, "After restart the customer returns to its server");
}

void test_leader_crash()
{
    std::cout << "\n--- Test: Leader Crashes ---\n";

    Frontend f;
    int oldLeader = f.leader;
    std::string customer = f.customerOn(f.follower());
    f.server(oldLeader).stop();

    check(!f.order(customer, "O301", "MOUSE", 1), "Write refused while there is no leader");
    check(f.lb.getLastServerId() == -1 && f.lb.leaderServer() == -1, "No server was given the write");
    check(f.read(customer, OrderService::makeGetStock("MOUSE")), "Read still answered without a leader");
    check(f.lastRead().possiblyStale && f.lastRead().leaderCommit == -1,
          "...and marked 'may be stale' (no leader to compare with)");

    int newLeader = f.service.waitForLeader(500);
    check(newLeader != -1 && newLeader != oldLeader, "New leader elected");
    bool ok = f.order(customer, "O301", "MOUSE", 2);
    check(ok && f.lb.getLastServerId() == newLeader, "Retried write goes to the new leader");
    check(f.lastResult().order.status == OrderStatus::ACCEPTED, "Order accepted");
}

void test_read_from_new_leader()
{
    std::cout << "\n--- Test: Read Right After a New Leader Is Elected ---\n";

    Frontend f;
    int oldLeader = f.leader;
    check(f.order("C1", "O501", "LAPTOP", 1), "O501 committed under the first leader");
    // Crash the leader at once: the followers hold O501 but have not yet
    // heard (next AppendEntries) that it is committed.
    f.server(oldLeader).stop();
    int newLeader = f.service.waitForLeader(500);
    check(newLeader != -1 && newLeader != oldLeader, "New leader elected");
    check(f.cluster.getStorage(newLeader)->getLastLogIndex() == 1 &&
          f.cluster.getEngine(newLeader)->getCommitIndex() == 0,
          "New leader holds O501 but its commit index is still 0");

    // O501 IS committed (stock really is 1), but the new leader may only
    // count it once it commits an entry of its own term.  So it cannot
    // vouch for any read yet: the old answer must not be called up to date.
    std::string onNew = f.customerOn(newLeader);
    f.read(onNew, OrderService::makeGetStock("LAPTOP"));
    check(f.lastRead().outcome == ReadOutcome::OK && f.lastRead().stock == 2,
          "Read answered from the new leader's copy (still LAPTOP = 2)");
    check(f.lastRead().possiblyStale,
          "Marked 'may be stale': the new leader has not confirmed the commit point yet");

    check(f.order("C1", "O502", "MOUSE", 1), "First write of the new term commits");
    f.service.runTicks(30);
    f.read(onNew, OrderService::makeGetStock("LAPTOP"));
    check(!f.lastRead().possiblyStale && f.lastRead().stock == 1,
          "After that, reads are up to date again");
}

void test_no_majority()
{
    std::cout << "\n--- Test: No Majority ---\n";

    Frontend f;
    for (int id = 1; id <= 3; id++) {
        if (id != f.leader) f.server(id).stop();
    }
    bool ok = f.order("C1", "O401", "LAPTOP", 1);
    check(!ok, "routeRequest reports failure");
    check(f.lb.getLastServerId() == f.leader, "Write reached the (isolated) leader");
    check(f.lastResult().outcome == SubmitOutcome::NOT_COMMITTED, "Outcome NOT_COMMITTED");
    check(f.allStock("LAPTOP", 2), "Stock unchanged on every node");
}

void test_last_laptop_race()
{
    std::cout << "\n--- Test: Last-Laptop Race From Two Customers ---\n";

    Frontend f;
    int a = (f.leader % 3) + 1, b = ((f.leader + 1) % 3) + 1;   // the two followers
    std::string alice = f.customerOn(a), bob = f.customerOn(b);
    bool okA = f.order(alice, "ALICE", "LAPTOP", 2);
    OrderStatus sa = f.lastResult().order.status;
    bool okB = f.order(bob, "BOB", "LAPTOP", 2);
    OrderStatus sb = f.lastResult().order.status;
    f.service.runTicks(30);

    check(okA && okB, "Both orders were committed (both went to the leader)");
    check((sa == OrderStatus::ACCEPTED) != (sb == OrderStatus::ACCEPTED),
          "Exactly one of the two customers got the laptops");
    check(f.allStock("LAPTOP", 0) && f.service.replicasAgree(),
          "Stock 0 and the same orders on every node");
}

void test_malformed_requests()
{
    std::cout << "\n--- Test: Malformed Requests ---\n";

    Frontend f;
    int before = f.cluster.getStorage(f.leader)->getLastLogIndex();
    check(!f.lb.routeRequest(ClientRequest{"X1", "garbage", "C1"}), "Malformed write rejected");
    check(f.lastResult().outcome == SubmitOutcome::INVALID_REQUEST, "Outcome INVALID_REQUEST");
    check(f.cluster.getStorage(f.leader)->getLastLogIndex() == before, "Nothing reached the Raft log");

    check(!f.read("C1", "GET_STOCK|"), "Read with no product rejected");
    check(f.lastRead().outcome == ReadOutcome::INVALID_REQUEST, "Outcome INVALID_REQUEST");
    check(!f.read("C1", OrderService::makeGetOrder("NOPE")), "Read of an unknown order");
    check(f.lastRead().outcome == ReadOutcome::NOT_FOUND, "Outcome NOT_FOUND");
    check(!f.read("C1", OrderService::makeGetStock("PHONE")), "Read of an unknown product");
    check(f.lastRead().outcome == ReadOutcome::NOT_FOUND, "Outcome NOT_FOUND");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Routed Request Tests (LB -> Server -> Raft)\n";
    std::cout << "========================================\n";

    test_write_goes_to_leader();
    test_read_uses_hash_ring();
    test_read_after_write_freshness();
    test_stale_read_after_restart();
    test_server_crash_moves_only_its_reads();
    test_leader_crash();
    test_read_from_new_leader();
    test_no_majority();
    test_last_laptop_race();
    test_malformed_requests();

    return finish("Routed Request Tests");
}
