/*
 * test_network_delay.cpp
 * ======================
 * Simulated message delay.
 *
 * Network level: messages wait a random number of ticks in a priority
 * queue, may arrive out of order, are never lost or duplicated, and the
 * same seed gives the same run.
 *
 * Raft level (delays of several ticks, many seeds): Raft's safety rules
 * must still hold on every tick -
 *   - at most one leader per term (Election Safety);
 *   - once an entry is committed, every node that commits that index has
 *     the same entry, and it never changes (State Machine Safety);
 * and the cluster must still elect leaders and commit orders.
 */

#include <map>
#include <string>
#include <vector>
#include "OrderService.h"
#include "RaftCluster.h"
#include "replication_test_utils.h"

// ── Raft safety checker, run after every tick ──────────────────────

struct SafetyMonitor {
    std::map<int, int> leaderOfTerm;                       // term -> node
    std::map<int, std::pair<int, std::string>> committed;  // index -> (term, command)
    std::string violation;                                 // first problem seen

    void observe(RaftCluster& cluster)
    {
        for (int id = 1; id <= cluster.getClusterSize(); id++) {
            RaftEngine* engine = cluster.getEngine(id);
            if (cluster.getNode(id)->isActive() && engine->getRole() == RaftRole::LEADER) {
                int term = engine->getCurrentTerm();
                auto it = leaderOfTerm.find(term);
                if (it == leaderOfTerm.end()) leaderOfTerm[term] = id;
                else if (it->second != id && violation.empty())
                    violation = "two leaders in term " + std::to_string(term);
            }
            const Storage* log = cluster.getStorage(id);
            for (int index = 1; index <= engine->getCommitIndex(); index++) {
                const LogEntry* entry = log->getEntry(index);
                if (entry == nullptr) {
                    if (violation.empty()) violation = "node " + std::to_string(id) +
                                                       " committed an index it does not have";
                    continue;
                }
                std::pair<int, std::string> value(entry->term, entry->command);
                auto it = committed.find(index);
                if (it == committed.end()) committed[index] = value;
                else if (it->second != value && violation.empty())
                    violation = "committed entry #" + std::to_string(index) + " differs between nodes";
            }
        }
    }
    bool ok() const { return violation.empty(); }
};

// Run `ticks` ticks, checking safety after each one.
void runChecked(RaftCluster& cluster, SafetyMonitor& monitor, int ticks)
{
    for (int t = 0; t < ticks; t++) {
        cluster.runTick();
        monitor.observe(cluster);
    }
}

int waitLeader(RaftCluster& cluster, SafetyMonitor& monitor, int maxTicks)
{
    for (int t = 0; t < maxTicks && cluster.findLeader() == -1; t++) {
        cluster.runTick();
        monitor.observe(cluster);
    }
    return cluster.findLeader();
}

// Every running node has committed the same entries up to `index`.
bool allCommittedUpTo(RaftCluster& cluster, int index)
{
    for (int id = 1; id <= cluster.getClusterSize(); id++) {
        if (!cluster.getNode(id)->isActive()) continue;
        if (cluster.getEngine(id)->getCommitIndex() < index) return false;
    }
    return true;
}

// ── Network level ──────────────────────────────────────────────────

struct TwoNodes {
    Node a{1}, b{2};
    Network net;
    TwoNodes()
    {
        a.start(); b.start();
        net.registerNode(&a);
        net.registerNode(&b);
    }
    void send(int n) { net.sendMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES, std::to_string(n))); }
    std::vector<int> drain()
    {
        std::vector<int> got;
        Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");
        while (net.receiveMessage(2, msg)) got.push_back(std::stoi(msg.getPayload()));
        return got;
    }
};

void test_no_delay_by_default()
{
    std::cout << "\n--- Test: No Delay by Default ---\n";
    TwoNodes t;
    t.send(7);
    check(t.net.getPendingCount(2) == 1, "Message is in the mailbox at once");
    check(t.net.getInFlightCount() == 0 && t.net.getStats().delayed == 0, "Nothing held back");
}

void test_fixed_delay()
{
    std::cout << "\n--- Test: Fixed Delay of 3 Ticks ---\n";
    TwoNodes t;
    t.net.setDelay(3, 3);
    t.net.advanceTo(10);
    t.send(1);
    check(t.net.getPendingCount(2) == 0 && t.net.getInFlightCount() == 1, "Sent at tick 10: still travelling");
    t.net.advanceTo(12);
    check(t.net.getPendingCount(2) == 0, "Not there at tick 12");
    t.net.advanceTo(13);
    check(t.net.getPendingCount(2) == 1 && t.net.getInFlightCount() == 0, "Arrives at tick 13");
}

void test_random_delay_range_and_reordering()
{
    std::cout << "\n--- Test: Random Delays Stay in Range and Reorder ---\n";
    TwoNodes t;
    t.net.setDelay(1, 5);
    t.net.setSeed(42);
    std::map<int, int> sentAt;
    std::map<int, int> arrivedAt;
    int n = 0;
    for (int tick = 1; tick <= 40; tick++) {
        t.net.advanceTo(tick);
        for (int got : t.drain()) arrivedAt[got] = tick;
        for (int k = 0; k < 5; k++) { sentAt[n] = tick; t.send(n); n++; }
    }
    for (int tick = 41; tick <= 50; tick++) {
        t.net.advanceTo(tick);
        for (int got : t.drain()) arrivedAt[got] = tick;
    }

    bool inRange = true, allOnce = arrivedAt.size() == sentAt.size();
    for (const auto& s : sentAt) {
        auto it = arrivedAt.find(s.first);
        if (it == arrivedAt.end()) { allOnce = false; continue; }
        int d = it->second - s.second;
        if (d < 1 || d > 5) inRange = false;
    }
    check(allOnce, "All 200 messages arrived exactly once (no loss, no duplicates)");
    check(inRange, "Every delay was between 1 and 5 ticks");

    bool reordered = false;
    for (int k = 1; k < n; k++) {
        if (arrivedAt[k] < arrivedAt[k - 1]) reordered = true;
    }
    check(reordered, "Some later messages arrived before earlier ones (reordering)");
    check(t.net.getStats().sent == n && t.net.getStats().delayed == n, "Counters: 200 sent, 200 delayed");
}

void test_same_seed_same_run()
{
    std::cout << "\n--- Test: Same Seed, Same Run ---\n";
    auto run = [](unsigned int seed) {
        TwoNodes t;
        t.net.setDelay(0, 6);
        t.net.setSeed(seed);
        std::vector<int> order;
        for (int tick = 1; tick <= 30; tick++) {
            t.net.advanceTo(tick);
            for (int got : t.drain()) order.push_back(got);
            for (int k = 0; k < 3; k++) t.send(tick * 10 + k);
        }
        return order;
    };
    check(run(7) == run(7), "Seed 7 twice: identical arrival order");
    check(run(7) != run(8), "Seed 8: a different arrival order");
}

void test_receiver_down_on_arrival()
{
    std::cout << "\n--- Test: Receiver Stops While the Message Travels ---\n";
    TwoNodes t;
    t.net.setDelay(2, 2);
    t.send(1);
    t.b.stop();
    t.net.advanceTo(2);
    check(t.net.getPendingCount(2) == 0, "Message dropped: receiver was down when it arrived");
    check(t.net.getStats().droppedReceiverDown == 1, "Counted as dropped");
}

// ── Raft level ─────────────────────────────────────────────────────

void test_election_under_delay()
{
    std::cout << "\n--- Test: Elections With Delays of 1-5 Ticks (20 seeds, 5 nodes) ---\n";
    bool allElected = true, allSafe = true;
    std::string firstProblem;
    for (unsigned int seed = 1; seed <= 20; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(1, 5);
        cluster.getNetwork()->setSeed(seed);
        SafetyMonitor monitor;
        cluster.startAll();
        if (waitLeader(cluster, monitor, 1500) == -1) allElected = false;
        runChecked(cluster, monitor, 500);
        if (!monitor.ok()) {
            allSafe = false;
            if (firstProblem.empty()) firstProblem = "seed " + std::to_string(seed) + ": " + monitor.violation;
        }
    }
    check(allElected, "A leader was elected for every seed");
    check(allSafe, "Never two leaders in one term" + (firstProblem.empty() ? "" : " (" + firstProblem + ")"));
}

void test_replication_under_delay()
{
    std::cout << "\n--- Test: Commands Commit and Logs Agree Under Delay (20 seeds) ---\n";
    bool allCommitted = true, allSafe = true;
    std::string firstProblem;
    for (unsigned int seed = 1; seed <= 20; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(1, 5);
        cluster.getNetwork()->setSeed(seed);
        SafetyMonitor monitor;
        cluster.startAll();
        waitLeader(cluster, monitor, 1500);

        int submitted = 0;
        for (int k = 0; k < 10; k++) {
            if (cluster.submitCommand("CMD-" + std::to_string(k)) != -1) submitted++;
            runChecked(cluster, monitor, 7);       // new commands while others are in flight
        }
        runChecked(cluster, monitor, 300);

        int leader = cluster.findLeader();
        int commit = leader == -1 ? 0 : cluster.getEngine(leader)->getCommitIndex();
        if (submitted < 10 || commit < 10 || !allCommittedUpTo(cluster, commit)) allCommitted = false;
        if (!monitor.ok()) {
            allSafe = false;
            if (firstProblem.empty()) firstProblem = "seed " + std::to_string(seed) + ": " + monitor.violation;
        }
    }
    check(allCommitted, "All 10 commands committed on every node, for every seed");
    check(allSafe, "Committed entries identical everywhere and never changed" +
                   (firstProblem.empty() ? "" : " (" + firstProblem + ")"));
}

void test_leader_crash_under_delay()
{
    std::cout << "\n--- Test: Leader Crash and Recovery Under Delay (20 seeds) ---\n";
    bool reElected = true, caughtUp = true, allSafe = true;
    std::string firstProblem;
    for (unsigned int seed = 1; seed <= 20; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(1, 5);
        cluster.getNetwork()->setSeed(seed);
        SafetyMonitor monitor;
        cluster.startAll();
        int first = waitLeader(cluster, monitor, 1500);
        for (int k = 0; k < 5; k++) { cluster.submitCommand("A-" + std::to_string(k)); runChecked(cluster, monitor, 5); }
        runChecked(cluster, monitor, 100);

        cluster.stopNode(first);                   // crash with messages still in flight
        runChecked(cluster, monitor, 3);
        int second = waitLeader(cluster, monitor, 1500);
        if (second == -1 || second == first ||
            cluster.getEngine(second)->getCurrentTerm() <= cluster.getEngine(first)->getCurrentTerm())
            reElected = false;
        for (int k = 0; k < 5; k++) { cluster.submitCommand("B-" + std::to_string(k)); runChecked(cluster, monitor, 5); }

        cluster.startNode(first);
        runChecked(cluster, monitor, 400);
        int leader = cluster.findLeader();
        int commit = leader == -1 ? 0 : cluster.getEngine(leader)->getCommitIndex();
        if (commit < 10 || !allCommittedUpTo(cluster, commit)) caughtUp = false;
        if (!monitor.ok()) {
            allSafe = false;
            if (firstProblem.empty()) firstProblem = "seed " + std::to_string(seed) + ": " + monitor.violation;
        }
    }
    check(reElected, "A new leader with a higher term was elected for every seed");
    check(caughtUp, "After the restart all 5 nodes committed all 10 commands");
    check(allSafe, "Safety held on every tick" + (firstProblem.empty() ? "" : " (" + firstProblem + ")"));
}

void test_crash_with_entries_in_flight()
{
    std::cout << "\n--- Test: Leader Crashes While Its New Entries Are Still Travelling (20 seeds) ---\n";
    // The leader appends an entry and crashes 1-8 ticks later, while
    // copies and acknowledgements are still travelling.  The entry may be
    // lost (if it was never committed) but a committed entry must never
    // be replaced.
    bool allSafe = true, recovered = true;
    std::string firstProblem;
    for (unsigned int seed = 1; seed <= 20; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(1, 5);
        cluster.getNetwork()->setSeed(seed);
        SafetyMonitor monitor;
        cluster.startAll();
        for (int round = 0; round < 3; round++) {
            int leader = waitLeader(cluster, monitor, 1500);
            if (leader == -1) { recovered = false; break; }
            cluster.submitCommand("R" + std::to_string(round));
            runChecked(cluster, monitor, 1 + static_cast<int>(seed + round) % 8);   // crash 1-8 ticks later
            cluster.stopNode(leader);
            waitLeader(cluster, monitor, 1500);
            cluster.submitCommand("AFTER" + std::to_string(round));
            runChecked(cluster, monitor, 30);
            cluster.startNode(leader);
            runChecked(cluster, monitor, 200);
        }
        int leader = cluster.findLeader();
        int commit = leader == -1 ? 0 : cluster.getEngine(leader)->getCommitIndex();
        if (commit < 3 || !allCommittedUpTo(cluster, commit)) recovered = false;
        if (!monitor.ok()) {
            allSafe = false;
            if (firstProblem.empty()) firstProblem = "seed " + std::to_string(seed) + ": " + monitor.violation;
        }
    }
    check(allSafe, "No committed entry was ever replaced" + (firstProblem.empty() ? "" : " (" + firstProblem + ")"));
    check(recovered, "The cluster kept committing and all nodes caught up");
}

void test_safety_when_network_too_slow()
{
    std::cout << "\n--- Test: Network Slower Than the Election Timeout ---\n";
    // Delays (40-90 ticks) longer than the election timeout (50-70):
    // elections may keep failing, but Raft must never become unsafe.
    bool allSafe = true;
    std::string firstProblem;
    for (unsigned int seed = 1; seed <= 10; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(40, 90);
        cluster.getNetwork()->setSeed(seed);
        SafetyMonitor monitor;
        cluster.startAll();
        for (int k = 0; k < 20; k++) {
            cluster.submitCommand("S-" + std::to_string(k));
            runChecked(cluster, monitor, 100);
        }
        if (!monitor.ok()) {
            allSafe = false;
            if (firstProblem.empty()) firstProblem = "seed " + std::to_string(seed) + ": " + monitor.violation;
        }
    }
    check(allSafe, "Safety held for 2000 ticks on every seed" + (firstProblem.empty() ? "" : " (" + firstProblem + ")"));
}

void test_orders_under_delay()
{
    std::cout << "\n--- Test: Orders and the Last-Laptop Race Under Delay ---\n";
    bool allOk = true;
    for (unsigned int seed = 1; seed <= 10; seed++) {
        RaftCluster cluster(5, 50, 20, 10);
        cluster.getNetwork()->setDelay(1, 5);
        cluster.getNetwork()->setSeed(seed);
        OrderService service(cluster);
        service.setStock("LAPTOP", 2);
        service.setMaxWaitTicks(300);
        cluster.startAll();
        int leader = service.waitForLeader(1500);
        if (leader == -1) { allOk = false; continue; }

        SubmitResult a = service.submitOrder(leader, "ALICE", "LAPTOP", 2);
        leader = service.waitForLeader(1500);
        SubmitResult b = service.submitOrder(leader, "BOB", "LAPTOP", 2);
        service.runTicks(300);

        bool exactlyOne = a.outcome == SubmitOutcome::COMMITTED && b.outcome == SubmitOutcome::COMMITTED &&
                          ((a.order.status == OrderStatus::ACCEPTED) != (b.order.status == OrderStatus::ACCEPTED));
        if (!exactlyOne || !service.replicasAgree() || service.getBook(1).getStock("LAPTOP") != 0) allOk = false;
    }
    check(allOk, "For every seed: both orders committed, exactly one accepted, all 5 nodes agree");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Network Delay Tests\n";
    std::cout << "========================================\n";

    test_no_delay_by_default();
    test_fixed_delay();
    test_random_delay_range_and_reordering();
    test_same_seed_same_run();
    test_receiver_down_on_arrival();

    test_election_under_delay();
    test_replication_under_delay();
    test_leader_crash_under_delay();
    test_crash_with_entries_in_flight();
    test_safety_when_network_too_slow();
    test_orders_under_delay();

    return finish("Network Delay Tests");
}
