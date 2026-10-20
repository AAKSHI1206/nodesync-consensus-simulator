/*
 * test_log_replication.cpp
 * ========================
 * End-to-end log replication through RaftCluster and the simulated
 * Network: replication, majority commitment, no-majority, follower
 * catch-up and an old leader rejoining.
 */

#include <map>
#include <vector>
#include "RaftCluster.h"
#include "replication_test_utils.h"

// Records every apply on every node of a cluster.
struct ApplyLog {
    std::map<int, std::vector<std::string>> commands;   // node -> commands

    void attach(RaftCluster& cluster)
    {
        for (int id = 1; id <= cluster.getClusterSize(); id++) {
            cluster.getEngine(id)->setApplyCallback(
                [this, id](int, int, const std::string& command) {
                    commands[id].push_back(command);
                });
        }
    }
};

// Start a cluster and run until it has a leader.
int electLeader(RaftCluster& cluster)
{
    cluster.startAll();
    cluster.runFor(200);
    return cluster.findLeader();
}

// ── Test A: successful replication ─────────────────────────────────

void test_successful_replication()
{
    std::cout << "\n--- Test A: Successful Replication (3 nodes) ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    ApplyLog applied;
    applied.attach(cluster);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    int term  = cluster.getEngine(leader)->getCurrentTerm();
    int index = cluster.submitCommand("ORDER-1 laptop x1");
    check(index == 1, "Command appended at index 1");
    check(cluster.getEngine(leader)->getCommitIndex() == 0,
          "Not reported committed before replication");

    cluster.runFor(30);

    for (int id = 1; id <= 3; id++) {
        const LogEntry* entry = cluster.getStorage(id)->getEntry(1);
        check(entry && entry->index == 1 && entry->term == term &&
              std::string(entry->command) == "ORDER-1 laptop x1",
              "Node " + std::to_string(id) + " holds entry 1 (same term, command)");
        check(cluster.getEngine(id)->getCommitIndex() == 1,
              "Node " + std::to_string(id) + " commitIndex = 1");
        check(applied.commands[id] == std::vector<std::string>{"ORDER-1 laptop x1"},
              "Node " + std::to_string(id) + " applied it once");
    }
    for (int peer = 1; peer <= 3; peer++) {
        if (peer == leader) continue;
        check(cluster.getEngine(leader)->getMatchIndex(peer) == 1 &&
              cluster.getEngine(leader)->getNextIndex(peer) == 2,
              "Leader progress for node " + std::to_string(peer) +
              ": matchIndex 1, nextIndex 2");
    }
}

// ── Test B: commit with one follower down ──────────────────────────

void test_majority_commitment()
{
    std::cout << "\n--- Test B: Majority Commitment (1 of 3 down) ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    int down = (leader % 3) + 1;
    int up   = 6 - leader - down;
    cluster.stopNode(down);

    cluster.submitCommand("ORDER-2");
    cluster.runFor(30);

    check(cluster.getEngine(leader)->getCommitIndex() == 1,
          "Leader committed with itself + one follower");
    check(cluster.getEngine(up)->getCommitIndex() == 1,
          "Active follower learned the commit");
    check(cluster.getStorage(down)->getLastLogIndex() == 0 &&
          cluster.getEngine(down)->getCommitIndex() == 0,
          "Stopped follower has nothing");
}

// ── Test C: no majority, no commit ─────────────────────────────────

void test_no_majority()
{
    std::cout << "\n--- Test C: No Majority (leader alone) ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    ApplyLog applied;
    applied.attach(cluster);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    for (int id = 1; id <= 3; id++) {
        if (id != leader) cluster.stopNode(id);
    }

    int index = cluster.submitCommand("ORDER-3");
    cluster.runFor(200);

    check(index == 1 && cluster.getStorage(leader)->getLastLogIndex() == 1,
          "Entry appended to the leader's log");
    check(cluster.getEngine(leader)->getCommitIndex() == 0,
          "Entry NOT committed without a majority");
    check(applied.commands[leader].empty(),
          "Application state unchanged");

    // Restoring one follower restores the majority.
    int back = (leader % 3) + 1;
    cluster.startNode(back);
    cluster.runFor(30);
    check(cluster.getEngine(leader)->getCommitIndex() == 1,
          "Committed once a majority is back");
    check(applied.commands[leader] == std::vector<std::string>{"ORDER-3"},
          "Applied exactly once after commit");
}

// ── Test E: lagging follower catches up ────────────────────────────

void test_follower_catch_up()
{
    std::cout << "\n--- Test E: Lagging Follower Catches Up ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    ApplyLog applied;
    applied.attach(cluster);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    int lagging = (leader % 3) + 1;
    cluster.submitCommand("ORDER-1");
    cluster.runFor(30);
    cluster.stopNode(lagging);

    for (int i = 2; i <= 6; i++) {
        cluster.submitCommand("ORDER-" + std::to_string(i));
    }
    cluster.runFor(30);
    check(cluster.getStorage(lagging)->getLastLogIndex() == 1,
          "Stopped follower is 5 entries behind");

    cluster.startNode(lagging);
    cluster.runFor(30);

    check(logSignature(*cluster.getStorage(lagging)) ==
          logSignature(*cluster.getStorage(leader)),
          "Lagging follower's log converged with the leader's");
    check(cluster.getEngine(lagging)->getCommitIndex() == 6,
          "Lagging follower commitIndex = 6");
    check(applied.commands[lagging].size() == 6,
          "Lagging follower applied 6 entries, none twice");
}

// ── Old leader rejoins with an uncommitted entry ───────────────────

void test_old_leader_rejoins()
{
    std::cout << "\n--- Test: Old Leader Rejoins With Stale Entry ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    ApplyLog applied;
    applied.attach(cluster);
    int oldLeader = electLeader(cluster);
    check(oldLeader != -1, "Leader elected");
    if (oldLeader == -1) return;

    cluster.submitCommand("ORDER-1");
    cluster.runFor(30);

    // The leader appends an entry, then crashes before it is sent.
    cluster.submitCommand("LOST");
    for (int id = 1; id <= 3; id++) {
        if (id == oldLeader) continue;
        Message drop(0, 0, 0, MessageType::REQUEST_VOTE, "");
        while (cluster.getNetwork()->receiveMessage(id, drop)) {}
    }
    cluster.stopNode(oldLeader);

    cluster.runFor(300);
    int newLeader = cluster.findLeader();
    check(newLeader != -1 && newLeader != oldLeader, "New leader elected");
    if (newLeader == -1) return;

    cluster.submitCommand("ORDER-2");
    cluster.runFor(30);

    cluster.startNode(oldLeader);
    cluster.runFor(50);

    check(cluster.getEngine(oldLeader)->getRole() == RaftRole::FOLLOWER,
          "Old leader rejoins as FOLLOWER");
    check(logSignature(*cluster.getStorage(oldLeader)) ==
          logSignature(*cluster.getStorage(newLeader)),
          "Old leader's uncommitted entry replaced; logs identical");
    bool lostApplied = false;
    for (auto& node : applied.commands) {
        for (auto& command : node.second) {
            if (command == "LOST") lostApplied = true;
        }
    }
    check(!lostApplied, "Uncommitted entry was never applied anywhere");
    check(applied.commands[oldLeader] ==
          std::vector<std::string>({"ORDER-1", "ORDER-2"}),
          "Old leader applied exactly the committed commands");
}

// ── Restart keeps term, vote and log; re-arms the timer from now ───

void test_recover_keeps_persistent_state()
{
    std::cout << "\n--- Test: Restart Keeps Term, Vote and Log ---\n";

    ReplicationHarness h(3);
    RaftEngine& node2 = h.engine(2);

    // Node 2 votes for node 1 in term 5 and receives one entry.
    node2.handleMessage(Message(1, 2, 5, MessageType::REQUEST_VOTE,
                                "lastLogIndex=0;lastLogTerm=0"));
    node2.handleMessage(Message(1, 2, 5, MessageType::APPEND_ENTRIES,
                                appendEntriesPayload(0, 0, 1, {{5, "A"}})));
    h.dropAll();
    check(node2.getVotedFor() == 1 && node2.getCommitIndex() == 1,
          "Before crash: term 5, voted for 1, entry 1 committed");

    node2.recover(5000);

    check(node2.getCurrentTerm() == 5, "Term survives restart");
    check(node2.getVotedFor() == 1, "Vote survives restart");
    check(h.log(2).getLastLogIndex() == 1, "Log survives restart");
    check(node2.getLastApplied() == 1 && node2.getCommitIndex() == 1,
          "Applied progress kept (no re-apply)");
    check(node2.getRole() == RaftRole::FOLLOWER && node2.getLeaderId() == -1,
          "Comes back as FOLLOWER with no known leader");
    check(!node2.isElectionTimedOut(5000 + 100000 - 1) &&
          node2.isElectionTimedOut(5000 + 100000),
          "Election timer counted from the restart time");

    // Same-term vote request from another candidate: must be refused.
    node2.handleMessage(Message(3, 2, 5, MessageType::REQUEST_VOTE,
                                "lastLogIndex=9;lastLogTerm=5"));
    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    bool refused = h.pop(3, reply) && reply.getPayload() == "granted=false";
    check(refused, "No second vote in term 5 after restart");
}

// ── Cluster restart helpers are safe to repeat ─────────────────────

void test_restart_helpers_idempotent()
{
    std::cout << "\n--- Test: startNode/startAll on Running Nodes ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    ApplyLog applied;
    applied.attach(cluster);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    cluster.submitCommand("A");
    cluster.submitCommand("B");
    cluster.runFor(30);
    int term = cluster.getEngine(leader)->getCurrentTerm();

    cluster.startNode(leader);   // already running
    check(cluster.findLeader() == leader,
          "startNode on a running leader leaves it LEADER");

    cluster.startAll();          // everything already running
    cluster.runFor(30);
    check(cluster.findLeader() == leader &&
          cluster.getEngine(leader)->getCurrentTerm() == term,
          "startAll again: same leader, same term");
    bool once = true;
    for (int id = 1; id <= 3; id++) {
        if (applied.commands[id] != std::vector<std::string>({"A", "B"})) once = false;
    }
    check(once, "No entry applied twice");
}

// ── Quorum follows cluster size ────────────────────────────────────

void test_five_node_quorum()
{
    std::cout << "\n--- Test: Five-Node Quorum ---\n";

    RaftCluster cluster(5, 50, 20, 10);
    int leader = electLeader(cluster);
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    std::vector<int> followers;
    for (int id = 1; id <= 5; id++) {
        if (id != leader) followers.push_back(id);
    }

    cluster.stopNode(followers[0]);
    cluster.stopNode(followers[1]);
    cluster.submitCommand("X");
    cluster.runFor(30);
    check(cluster.getEngine(leader)->getCommitIndex() == 1,
          "3 of 5 alive: committed");

    cluster.stopNode(followers[2]);
    cluster.submitCommand("Y");
    cluster.runFor(100);
    check(cluster.getEngine(leader)->getCommitIndex() == 1,
          "2 of 5 alive: second entry not committed");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Log Replication Cluster Tests\n";
    std::cout << "========================================\n";

    test_successful_replication();
    test_majority_commitment();
    test_no_majority();
    test_follower_catch_up();
    test_old_leader_rejoins();
    test_recover_keeps_persistent_state();
    test_restart_helpers_idempotent();
    test_five_node_quorum();

    return finish("Log Replication Cluster Tests");
}
