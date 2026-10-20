/*
 * test_replication_messages.cpp
 * =============================
 * Message-processing tests for log replication: stale terms, stale
 * and duplicate responses, several queued messages per node, and
 * deterministic, bounded processing in the simulated network.
 */

#include <chrono>
#include <map>
#include <sstream>
#include <vector>
#include "RaftCluster.h"
#include "replication_test_utils.h"

// ── Stale-term AppendEntries is rejected ───────────────────────────

void test_stale_append_entries()
{
    std::cout << "\n--- Test: Stale-Term AppendEntries Rejected ---\n";

    ReplicationHarness h(3);
    RaftEngine& follower = h.engine(2);
    follower.handleMessage(Message(1, 2, 3, MessageType::APPEND_ENTRIES,
                                   appendEntriesPayload(0, 0, 0, {{3, "a"}})));
    h.drop(1);
    std::string before = logSignature(h.log(2));

    // Old term-2 leader (node 3) still sending.
    follower.handleMessage(Message(3, 2, 2, MessageType::APPEND_ENTRIES,
        appendEntriesPayload(0, 0, 1, {{2, "old"}})));

    check(logSignature(h.log(2)) == before, "Log unchanged");
    check(follower.getLeaderId() == 1, "Leader still node 1");
    check(follower.getCurrentTerm() == 3, "Term still 3");
    check(follower.getCommitIndex() == 0, "Commit index unchanged");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    bool got = h.pop(3, reply);
    check(got && reply.getTerm() == 3 &&
          payloadField(reply.getPayload(), "success") == 0,
          "Stale leader told about term 3 (rejection)");
}

// ── Stale response from an older term is ignored ───────────────────

void test_stale_response_ignored()
{
    std::cout << "\n--- Test: Response From Obsolete Term Ignored ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 2);
    RaftEngine& leader = h.engine(1);
    leader.propose("ORDER-1");
    h.dropAll();

    // Delayed acknowledgement from term 1.
    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 1, 0, 1)));

    check(leader.getRole() == RaftRole::LEADER, "Still LEADER");
    check(leader.getMatchIndex(2) == 0, "matchIndex unchanged");
    check(leader.getCommitIndex() == 0, "Commit index unchanged");
}

// ── Stale rejection within the same term is ignored ────────────────

void test_stale_rejection_ignored()
{
    std::cout << "\n--- Test: Out-of-Date Rejection Ignored ---\n";

    ReplicationHarness h(3);
    for (int i = 1; i <= 4; i++) h.log(1).appendEntry(i, 1, "c");
    h.makeLeader(1, 2);
    RaftEngine& leader = h.engine(1);
    int nextBefore = leader.getNextIndex(2);   // 5

    // Rejection for prevLogIndex 2, but we would now send prevLogIndex 4.
    leader.handleMessage(Message(2, 1, 2, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(false, 0, 2, 0)));
    check(leader.getNextIndex(2) == nextBefore,
          "nextIndex not moved by a rejection of an older request");
    check(h.network().getPendingCount(2) == 0, "No retry sent");
}

// ── Bogus acknowledgement beyond the leader's log ──────────────────

void test_ack_beyond_log_ignored()
{
    std::cout << "\n--- Test: Acknowledgement Beyond Leader Log Ignored ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    RaftEngine& leader = h.engine(1);
    leader.propose("ORDER-1");
    h.dropAll();

    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 9, 0, 9)));
    check(leader.getMatchIndex(2) == 0, "matchIndex not set to 9");
    check(leader.getCommitIndex() == 0, "Nothing committed");
}

// ── Higher-term response makes the leader step down ────────────────

void test_higher_term_response_steps_down()
{
    std::cout << "\n--- Test: Higher-Term Response Forces Step-Down ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    RaftEngine& leader = h.engine(1);
    leader.propose("ORDER-1");
    h.dropAll();

    leader.handleMessage(Message(2, 1, 5, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(false, 0, 0, 0)));
    check(leader.getRole() == RaftRole::FOLLOWER, "Stepped down");
    check(leader.getCurrentTerm() == 5, "Adopted term 5");

    // A late success for its old term must not commit anything.
    leader.handleMessage(Message(3, 1, 1, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 1, 0, 1)));
    check(leader.getCommitIndex() == 0, "Late success ignored after step-down");

    // The deposed leader's election timer runs again.
    check(leader.isElectionTimedOut(100000),
          "Deposed leader's election timer re-armed");
    leader.tick(100000);
    check(leader.getRole() == RaftRole::CANDIDATE,
          "Deposed leader can campaign after timing out");
}

// ── Delayed lower acknowledgement after progress ───────────────────

void test_older_ack_does_not_regress()
{
    std::cout << "\n--- Test: Older Acknowledgement Does Not Regress Progress ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    RaftEngine& leader = h.engine(1);
    leader.propose("A");
    leader.propose("B");
    leader.propose("C");
    h.dropAll();

    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 3, 0, 3)));
    check(leader.getMatchIndex(2) == 3 && leader.getCommitIndex() == 3,
          "Progress at 3, committed 3");

    // The acknowledgement of an earlier, smaller request arrives late.
    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 1, 0, 1)));
    check(leader.getMatchIndex(2) == 3, "matchIndex stays 3");
    check(leader.getNextIndex(2) == 4, "nextIndex stays 4");
    check(leader.getCommitIndex() == 3, "commitIndex unchanged");
}

// ── Large burst of proposals stays linear ──────────────────────────

void test_burst_of_proposals()
{
    std::cout << "\n--- Test: Burst of 1000 Proposals ---\n";

    auto start = std::chrono::steady_clock::now();

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(200);
    int leader = cluster.findLeader();
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    for (int i = 1; i <= 1000; i++) {
        cluster.submitCommand("ORDER-" + std::to_string(i));
    }
    cluster.runFor(30);

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    bool allCommitted = true;
    for (int id = 1; id <= 3; id++) {
        if (cluster.getEngine(id)->getCommitIndex() != 1000) allCommitted = false;
    }
    check(allCommitted, "All 1000 entries committed on every node");
    check(elapsed < 5000, "Completed in under 5 s (no quadratic resend)");
}

// ── Several requests queued for one follower ───────────────────────

void test_multiple_queued_requests()
{
    std::cout << "\n--- Test: Multiple Queued Requests ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    std::map<int, std::vector<int>> applied;
    for (int id = 1; id <= 3; id++) {
        h.engine(id).setApplyCallback(
            [&, id](int index, int, const std::string&) {
                applied[id].push_back(index);
            });
    }

    // Three proposals before anything is delivered.  Only the first is
    // sent at once (B and C ride on later messages); a heartbeat then
    // queues a second, overlapping request carrying entries 1-3.
    h.engine(1).propose("A");
    h.engine(1).propose("B");
    h.engine(1).propose("C");
    check(h.network().getPendingCount(2) == 1,
          "Proposals batched: one request queued, not one per proposal");
    h.engine(1).tick(100000);
    check(h.network().getPendingCount(2) == 2,
          "Two overlapping requests queued for node 2");

    int delivered = h.deliverAll();
    check(delivered > 0, "Messages delivered without hitting the round limit");

    // One more heartbeat (the previous one moved the deadline to
    // 200000) carries the final commit index to followers.
    h.engine(1).tick(200000);
    h.deliverAll();

    check(logSignature(h.log(2)) == logSignature(h.log(1)) &&
          logSignature(h.log(3)) == logSignature(h.log(1)),
          "All logs identical (no duplicated entries)");
    check(h.log(2).getLastLogIndex() == 3, "Follower log has exactly 3 entries");
    bool exactlyOnce = true;
    for (int id = 1; id <= 3; id++) {
        exactlyOnce = exactlyOnce && applied[id] == std::vector<int>{1, 2, 3};
    }
    check(exactlyOnce, "Every node applied 1, 2, 3 exactly once, in order");
}

// ── Duplicate delivery of the same request and response ────────────

void test_duplicate_messages()
{
    std::cout << "\n--- Test: Duplicate Messages ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    int appliedOnFollower = 0;
    h.engine(2).setApplyCallback(
        [&](int, int, const std::string&) { appliedOnFollower++; });

    h.engine(1).propose("A");
    h.deliverAll();
    h.engine(1).tick(100000);   // heartbeat carries commit = 1
    h.deliverAll();
    check(appliedOnFollower == 1, "Follower applied entry 1 once");

    // Re-deliver an identical request and an identical acknowledgement.
    std::string request = appendEntriesPayload(0, 0, 1, {{1, "A"}});
    h.network().sendMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES, request));
    h.network().sendMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES, request));
    h.deliverAll();

    check(h.log(2).getLastLogIndex() == 1, "No duplicate log entries");
    check(appliedOnFollower == 1, "No duplicate application");
    check(h.engine(1).getMatchIndex(2) == 1 && h.engine(1).getCommitIndex() == 1,
          "Leader progress unchanged by duplicate acknowledgements");
}

// ── Catch-up inside one tick is bounded ────────────────────────────

void test_bounded_catch_up_in_one_tick()
{
    std::cout << "\n--- Test: Bounded Catch-Up Within One Tick ---\n";

    RaftCluster cluster(3, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(200);
    int leader = cluster.findLeader();
    check(leader != -1, "Leader elected");
    if (leader == -1) return;

    int lagging = (leader % 3) + 1;
    cluster.stopNode(lagging);
    for (int i = 1; i <= 30; i++) {
        cluster.submitCommand("ORDER-" + std::to_string(i));
    }
    cluster.runFor(5);
    cluster.startNode(lagging);

    // runTick() delivers queued messages first, then ticks the engines,
    // so messages sent during the tick phase (heartbeats) wait one tick.
    // The whole request/rejection/retry exchange for the catch-up must
    // still finish inside a single delivery phase, with no backlog.
    bool noBacklog = true;
    int firstProgressTick = -1;
    int caughtUpTick = -1;
    for (int t = 0; t < 15; t++) {
        cluster.runTick();
        for (int id = 1; id <= 3; id++) {
            // At most one fresh message per other node.
            if (cluster.getNetwork()->getPendingCount(id) > 2) noBacklog = false;
        }
        int lagLog = cluster.getStorage(lagging)->getLastLogIndex();
        if (lagLog > 0 && firstProgressTick == -1) {
            firstProgressTick = cluster.getCurrentTime();
        }
        if (lagLog == 30 && caughtUpTick == -1) {
            caughtUpTick = cluster.getCurrentTime();
        }
    }
    check(noBacklog, "No message backlog builds up between ticks");
    check(cluster.getStorage(lagging)->getLastLogIndex() == 30,
          "Lagging follower caught up on all 30 entries");
    check(caughtUpTick != -1 && caughtUpTick == firstProgressTick,
          "Backtracking and catch-up completed within one tick");
}

// ── Same scenario, same outcome ────────────────────────────────────

std::string runScriptedScenario()
{
    RaftCluster cluster(5, 50, 20, 10);
    cluster.startAll();
    cluster.runFor(200);
    cluster.submitCommand("A");
    cluster.runFor(20);
    int oldLeader = cluster.findLeader();
    cluster.stopNode(oldLeader);
    cluster.runFor(300);
    cluster.submitCommand("B");
    cluster.submitCommand("C");
    cluster.runFor(50);
    cluster.startNode(oldLeader);
    cluster.runFor(100);

    std::ostringstream state;
    for (int id = 1; id <= 5; id++) {
        RaftEngine* eng = cluster.getEngine(id);
        state << id << ":" << raftRoleToString(eng->getRole())
              << ":t" << eng->getCurrentTerm()
              << ":c" << eng->getCommitIndex()
              << ":a" << eng->getLastApplied()
              << ":" << logSignature(*cluster.getStorage(id)) << "\n";
    }
    return state.str();
}

void test_deterministic_replication()
{
    std::cout << "\n--- Test: Deterministic Replication ---\n";

    std::string first  = runScriptedScenario();
    std::string second = runScriptedScenario();
    check(first == second, "Two identical runs end in identical state");
    check(first.find("A|") != std::string::npos &&
          first.find("C|") != std::string::npos,
          "Scenario actually replicated commands");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Replication Message Processing Tests\n";
    std::cout << "========================================\n";

    test_stale_append_entries();
    test_stale_response_ignored();
    test_stale_rejection_ignored();
    test_ack_beyond_log_ignored();
    test_higher_term_response_steps_down();
    test_older_ack_does_not_regress();
    test_multiple_queued_requests();
    test_duplicate_messages();
    test_bounded_catch_up_in_one_tick();
    test_burst_of_proposals();
    test_deterministic_replication();

    return finish("Replication Message Processing Tests");
}
