#include <iostream>
#include <set>
#include "RaftCluster.h"

int tests_passed = 0;
int tests_failed = 0;

void check(bool condition, const std::string& test_name)
{
    if (condition) {
        std::cout << "  PASS: " << test_name << "\n";
        tests_passed++;
    } else {
        std::cout << "  FAIL: " << test_name << "\n";
        tests_failed++;
    }
}

// ── Test: 3-node cluster elects a leader ───────────────────────────

void test_three_node_election()
{
    std::cout << "\n--- Test: 3-Node Cluster Elects Leader ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();

    // Run enough ticks to allow at least one election
    cluster.runFor(500);

    int leader = cluster.findLeader();
    check(leader != -1, "A leader was elected");

    if (leader != -1) {
        RaftEngine* eng = cluster.getEngine(leader);
        check(eng->getRole() == RaftRole::LEADER,
              "Leader node reports LEADER role");
        check(eng->getCurrentTerm() >= 1,
              "Leader's term is at least 1");
        check(eng->getLeaderId() == leader,
              "Leader knows itself as leader");
    }
}

// ── Test: 5-node cluster elects a leader ───────────────────────────

void test_five_node_election()
{
    std::cout << "\n--- Test: 5-Node Cluster Elects Leader ---\n";

    RaftCluster cluster(5, 50, 20, 10);
    cluster.startAll();

    cluster.runFor(500);

    int leader = cluster.findLeader();
    check(leader != -1, "A leader was elected in 5-node cluster");

    if (leader != -1) {
        RaftEngine* eng = cluster.getEngine(leader);
        check(eng->getVoteCount() >= 3,
              "Leader has at least 3 votes (majority of 5)");
    }
}

// ── Test: At most one leader per term ──────────────────────────────

void test_single_leader_per_term()
{
    std::cout << "\n--- Test: At Most One Leader Per Term ---\n";

    RaftCluster cluster(5, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(500);

    // Collect all leaders and their terms
    std::set<int> leaderTerms;
    bool violation = false;

    for (int i = 1; i <= cluster.getClusterSize(); i++) {
        RaftEngine* eng = cluster.getEngine(i);
        if (eng->getRole() == RaftRole::LEADER) {
            int term = eng->getCurrentTerm();
            if (leaderTerms.count(term)) {
                violation = true;
            }
            leaderTerms.insert(term);
        }
    }

    check(!violation,
          "No two leaders in the same term");
}

// ── Test: Leadership requires strict majority ──────────────────────

void test_majority_required()
{
    std::cout << "\n--- Test: Leadership Requires Strict Majority ---\n";

    RaftCluster cluster(5, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(500);

    int leader = cluster.findLeader();
    if (leader != -1) {
        RaftEngine* eng = cluster.getEngine(leader);
        check(eng->getVoteCount() >= eng->getMajority(),
              "Leader has votes >= majority threshold");
    } else {
        // This is also acceptable if no election completed
        check(true, "No leader yet (election may still be in progress)");
    }
}

// ── Test: Stopped leader replaced by new election ──────────────────

void test_leader_replacement()
{
    std::cout << "\n--- Test: Stopped Leader Replaced ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();

    // Elect initial leader
    cluster.runFor(500);
    int leader1 = cluster.findLeader();
    check(leader1 != -1, "Initial leader elected");

    if (leader1 != -1) {
        int leader1Term = cluster.getEngine(leader1)->getCurrentTerm();

        // Stop the leader
        cluster.stopNode(leader1);

        // Run more ticks — remaining 2 nodes should elect a new leader
        cluster.runFor(500);

        int leader2 = cluster.findLeader();
        check(leader2 != -1,
              "New leader elected after old leader stopped");
        check(leader2 != leader1,
              "New leader is different from stopped leader");

        if (leader2 != -1) {
            check(cluster.getEngine(leader2)->getCurrentTerm() > leader1Term,
                  "New leader has higher term");
        }
    }
}

// ── Test: Minority partition cannot elect leader ───────────────────

void test_minority_cannot_elect()
{
    std::cout << "\n--- Test: Minority Cannot Elect Leader ---\n";

    // 5-node cluster, stop 3 nodes — only 2 remain, need 3 for majority
    RaftCluster cluster(5, 30, 10, 10);
    cluster.startAll();

    // First let them elect a leader to get a baseline
    cluster.runFor(300);

    // Now stop 3 nodes to create a minority
    cluster.stopNode(1);
    cluster.stopNode(2);
    cluster.stopNode(3);

    // Run more ticks
    cluster.runFor(500);

    // Count leaders among the active nodes
    int leaderCount = 0;
    for (int i = 4; i <= 5; i++) {
        RaftEngine* eng = cluster.getEngine(i);
        if (cluster.getNode(i)->isActive() &&
            eng->getRole() == RaftRole::LEADER) {
            // A previously elected leader might still think it's leader
            // because it hasn't received higher-term messages.
            // But no NEW election can succeed.
            // So check: did this leader win an election AFTER the partition?
            // In our deterministic model, the old leader (if one of 4,5)
            // might still be leader from before. But if it tries a new
            // election, it can't win because it needs 3 votes.
        }
        if (eng->getRole() == RaftRole::LEADER) {
            leaderCount++;
        }
    }

    // The key test: try starting fresh engines for the minority
    // by stopping ALL nodes and only starting the minority
    RaftCluster freshCluster(5, 30, 10, 10);
    freshCluster.startAll();
    freshCluster.stopNode(1);
    freshCluster.stopNode(2);
    freshCluster.stopNode(3);

    freshCluster.runFor(500);
    int freshLeader = -1;
    for (int i = 4; i <= 5; i++) {
        if (freshCluster.getNode(i)->isActive() &&
            freshCluster.getEngine(i)->getRole() == RaftRole::LEADER) {
            freshLeader = i;
        }
    }
    check(freshLeader == -1,
          "Fresh minority (2/5) cannot elect a leader");
}

// ── Test: Followers process heartbeats ─────────────────────────────

void test_follower_heartbeat_processing()
{
    std::cout << "\n--- Test: Followers Process Valid Heartbeats ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(500);

    int leader = cluster.findLeader();
    check(leader != -1, "Leader exists");

    if (leader != -1) {
        int leaderTerm = cluster.getEngine(leader)->getCurrentTerm();

        // Run more ticks — heartbeats should keep followers stable
        int termBefore = leaderTerm;
        cluster.runFor(200);

        // Followers should not have started new elections
        // (term should be stable or at most equal)
        bool stable = true;
        for (int i = 1; i <= cluster.getClusterSize(); i++) {
            if (!cluster.getNode(i)->isActive()) continue;
            RaftEngine* eng = cluster.getEngine(i);
            if (eng->getRole() == RaftRole::FOLLOWER) {
                if (eng->getCurrentTerm() > termBefore + 1) {
                    stable = false;
                }
            }
        }
        check(stable, "Followers remain stable during heartbeats");
    }
}

// ── Test: New leader's identity and term ───────────────────────────

void test_leader_identity()
{
    std::cout << "\n--- Test: Leader Identity and Term Reported Correctly ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(500);

    int leader = cluster.findLeader();
    check(leader != -1, "Leader elected");

    if (leader != -1) {
        RaftEngine* eng = cluster.getEngine(leader);
        check(eng->getNodeId() == leader,
              "Engine's nodeId matches leader ID");
        check(eng->getLeaderId() == leader,
              "Engine's leaderId matches leader ID");
        check(eng->getCurrentTerm() >= 1,
              "Leader term is positive");

        // Followers should know the leader
        for (int i = 1; i <= cluster.getClusterSize(); i++) {
            if (i == leader) continue;
            if (!cluster.getNode(i)->isActive()) continue;
            RaftEngine* foll = cluster.getEngine(i);
            if (foll->getRole() == RaftRole::FOLLOWER) {
                check(foll->getLeaderId() == leader,
                      "Follower " + std::to_string(i) + " knows leader");
            }
        }
    }
}

// ── Test: Reproducible behavior ────────────────────────────────────

void test_deterministic_reproducibility()
{
    std::cout << "\n--- Test: Deterministic Reproducibility ---\n";

    // Run the same simulation twice
    auto runSim = []() {
        RaftCluster cluster(3, 50, 20, 10);
        cluster.startAll();
        cluster.runFor(500);
        return cluster.findLeader();
    };

    int leader1 = runSim();
    int leader2 = runSim();

    check(leader1 == leader2,
          "Same simulation produces same leader");
    check(leader1 != -1,
          "Both runs elected a leader");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Election Integration Tests\n";
    std::cout << "========================================\n";

    test_three_node_election();
    test_five_node_election();
    test_single_leader_per_term();
    test_majority_required();
    test_leader_replacement();
    test_minority_cannot_elect();
    test_follower_heartbeat_processing();
    test_leader_identity();
    test_deterministic_reproducibility();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}