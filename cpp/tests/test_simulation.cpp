/*
 * test_simulation.cpp
 * ===================
 * Tests for deterministic simulation time and message processing
 * in the Raft cluster.
 *
 * Validates that:
 * - Simulated time advances correctly.
 * - Messages are processed deterministically.
 * - Inactive nodes don't receive messages.
 * - The simulation is reproducible.
 * - No real sleeps are used.
 */

#include <iostream>
#include <cassert>
#include <chrono>
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

// ── Test: Initial simulation time ──────────────────────────────────

void test_initial_time()
{
    std::cout << "\n--- Test: Initial Simulation Time ---\n";

    RaftCluster cluster(3);

    check(cluster.getCurrentTime() == 0,
          "Initial simulated time is 0");
}

// ── Test: Time advances correctly ──────────────────────────────────

void test_time_advancement()
{
    std::cout << "\n--- Test: Time Advances Correctly ---\n";

    RaftCluster cluster(3);
    cluster.startAll();

    cluster.runTick();
    check(cluster.getCurrentTime() == 1,
          "After 1 tick, time is 1");

    cluster.runFor(9);
    check(cluster.getCurrentTime() == 10,
          "After 10 total ticks, time is 10");

    cluster.runFor(90);
    check(cluster.getCurrentTime() == 100,
          "After 100 total ticks, time is 100");
}

// ── Test: Reproducible event progression ───────────────────────────

void test_reproducible_progression()
{
    std::cout << "\n--- Test: Reproducible Event Progression ---\n";

    // Run the same simulation twice and check that the leader
    // is the same and the term is the same.
    auto runSim = []() -> std::pair<int, int> {
        RaftCluster cluster(3, 50, 20, 10);
        cluster.startAll();
        cluster.runFor(300);
        int leader = cluster.findLeader();
        int term = (leader != -1)
            ? cluster.getEngine(leader)->getCurrentTerm()
            : -1;
        return {leader, term};
    };

    auto [leader1, term1] = runSim();
    auto [leader2, term2] = runSim();

    check(leader1 == leader2,
          "Same leader in both runs");
    check(term1 == term2,
          "Same term in both runs");
}

// ── Test: Messages processed correctly ─────────────────────────────

void test_message_processing()
{
    std::cout << "\n--- Test: Correct Message Processing ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();

    // After enough ticks, at least one node should have started
    // an election and broadcast REQUEST_VOTE messages
    cluster.runFor(200);

    // Verify that engines have progressed (terms > 0)
    bool anyTermAdvanced = false;
    for (int i = 1; i <= cluster.getClusterSize(); i++) {
        if (cluster.getEngine(i)->getCurrentTerm() > 0) {
            anyTermAdvanced = true;
            break;
        }
    }
    check(anyTermAdvanced,
          "At least one engine advanced its term via messages");
}

// ── Test: Multiple messages in single tick ──────────────────────────

void test_multiple_messages_per_tick()
{
    std::cout << "\n--- Test: Multiple Messages Processed Per Tick ---\n";

    // With 5 nodes, a broadcast produces 4 messages at once.
    // They should all be processed within the same simulation tick.
    RaftCluster cluster(5, 30, 10, 10);
    cluster.startAll();
    cluster.runFor(300);

    // Verify that a leader emerged (which requires processing
    // multiple vote responses from a single broadcast)
    int leader = cluster.findLeader();
    check(leader != -1,
          "Leader elected (multiple messages processed successfully)");
}

// ── Test: Inactive node handling ───────────────────────────────────

void test_inactive_node_handling()
{
    std::cout << "\n--- Test: Inactive Node Not Processed ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();

    // Stop node 3 before any ticks
    cluster.stopNode(3);

    // Run simulation
    cluster.runFor(300);

    // Node 3 should still be in its initial state
    RaftEngine* eng3 = cluster.getEngine(3);
    check(eng3->getRole() == RaftRole::FOLLOWER,
          "Stopped node remains FOLLOWER");
    // Term may have been set by start(), but no further advancement
    check(eng3->getCurrentTerm() == 0,
          "Stopped node's term did not advance");
}

// ── Test: No real sleeps ───────────────────────────────────────────

void test_no_real_sleeps()
{
    std::cout << "\n--- Test: No Real Sleeps Used ---\n";

    auto start = std::chrono::steady_clock::now();

    // Run a full 1000-tick simulation
    RaftCluster cluster(5, 100, 20, 10);
    cluster.startAll();
    cluster.runFor(1000);

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - start).count();

    // A 1000-tick simulation should complete in well under 1 second
    // if no real sleeps are used (each tick is just CPU work)
    check(elapsed < 2000,
          "1000 ticks completed in <2s (no real sleeps)");

    check(cluster.getCurrentTime() == 1000,
          "Simulated time reached 1000");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Simulation Tests\n";
    std::cout << "========================================\n";

    test_initial_time();
    test_time_advancement();
    test_reproducible_progression();
    test_message_processing();
    test_multiple_messages_per_tick();
    test_inactive_node_handling();
    test_no_real_sleeps();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}
