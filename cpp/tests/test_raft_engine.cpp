/*
 * test_raft_engine.cpp
 * ====================
 * Unit tests for the RaftEngine — validates election state transitions,
 * voting rules, majority calculation, term management, log freshness,
 * heartbeat handling, and step-down behavior.
 */

#include <iostream>
#include <cassert>
#include "RaftEngine.h"
#include "Network.h"
#include "Storage.h"
#include "Node.h"

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

// ── Test: Initial follower state ───────────────────────────────────

void test_initial_state()
{
    std::cout << "\n--- Test: Initial Follower State ---\n";

    Network net;
    Storage storage;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    check(engine.getRole() == RaftRole::FOLLOWER,
          "Engine starts as FOLLOWER");
    check(engine.getCurrentTerm() == 0,
          "Initial term is 0");
    check(engine.getVotedFor() == -1,
          "Not voted for anyone initially");
    check(engine.getLeaderId() == -1,
          "No leader known initially");
    check(engine.getNodeId() == 1,
          "Node ID is 1");
    check(engine.getClusterSize() == 3,
          "Cluster size is 3");
}

// ── Test: Candidate transition on election timeout ─────────────────

void test_election_timeout_transition()
{
    std::cout << "\n--- Test: Candidate Transition on Timeout ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Small timeout so we can trigger it quickly
    RaftEngine engine(1, 3, &net, &storage, 10, 5, 50);
    engine.start();

    check(engine.getRole() == RaftRole::FOLLOWER,
          "Starts as FOLLOWER");

    // Tick past the election timeout (10 + 1*5 = 15)
    for (int t = 1; t <= 20; t++) {
        engine.tick(t);
    }

    check(engine.getRole() == RaftRole::CANDIDATE,
          "Transitions to CANDIDATE after timeout");
    check(engine.getCurrentTerm() == 1,
          "Term incremented to 1");
}

// ── Test: Self-voting ──────────────────────────────────────────────

void test_self_voting()
{
    std::cout << "\n--- Test: Self-Voting ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage, 10, 5, 50);
    engine.start();
    engine.startElection(0);

    check(engine.getVotedFor() == 1,
          "Candidate votes for itself");
    check(engine.getVoteCount() == 1,
          "Self-vote counted (1 vote)");
}

// ── Test: Term increment on election ───────────────────────────────

void test_term_increment()
{
    std::cout << "\n--- Test: Term Increment on Election ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    check(engine.getCurrentTerm() == 0, "Initial term is 0");

    engine.startElection(0);
    check(engine.getCurrentTerm() == 1, "Term is 1 after first election");

    engine.startElection(200);
    check(engine.getCurrentTerm() == 2, "Term is 2 after second election");
}

// ── Test: Vote granted once per term ───────────────────────────────

void test_vote_once_per_term()
{
    std::cout << "\n--- Test: Vote Granted Once Per Term ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Engine for node 1 as a follower
    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    // Node 2 requests vote from node 1 at term 1
    Message req1(2, 1, 1, MessageType::REQUEST_VOTE,
                 "lastLogIndex=0;lastLogTerm=0");
    engine.handleMessage(req1);

    check(engine.getVotedFor() == 2,
          "Voted for node 2");

    // Node 3 requests vote from node 1 at same term 1
    Message req2(3, 1, 1, MessageType::REQUEST_VOTE,
                 "lastLogIndex=0;lastLogTerm=0");
    engine.handleMessage(req2);

    check(engine.getVotedFor() == 2,
          "Still voted for node 2 (denied node 3)");

    // Verify node 3 got a rejection by checking the network
    Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");
    // Node 2 should have received a "granted=true" response
    bool found2 = net.receiveMessage(2, recv);
    check(found2, "Node 2 received a vote response");
    if (found2) {
        check(recv.getPayload().find("granted=true") != std::string::npos,
              "Node 2's response was granted=true");
    }

    // Node 3 should have received a "granted=false" response
    bool found3 = net.receiveMessage(3, recv);
    check(found3, "Node 3 received a vote response");
    if (found3) {
        check(recv.getPayload().find("granted=false") != std::string::npos,
              "Node 3's response was granted=false");
    }
}

// ── Test: Vote rejection for stale terms ───────────────────────────

void test_stale_term_rejection()
{
    std::cout << "\n--- Test: Stale Term Rejection ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    // Advance engine to term 3 by seeing a higher-term message
    Message highTerm(2, 1, 3, MessageType::APPEND_ENTRIES, "heartbeat");
    engine.handleMessage(highTerm);

    check(engine.getCurrentTerm() == 3,
          "Engine updated to term 3");

    // Now a REQUEST_VOTE at term 1 should be rejected
    Message staleReq(2, 1, 1, MessageType::REQUEST_VOTE,
                     "lastLogIndex=0;lastLogTerm=0");
    engine.handleMessage(staleReq);

    check(engine.getVotedFor() == -1,
          "Did not vote for stale-term candidate");

    // Verify rejection was sent
    Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");
    // Drain any messages for node 2
    bool gotReply = false;
    while (net.receiveMessage(2, recv)) {
        if (recv.getType() == MessageType::VOTE_RESPONSE) {
            check(recv.getPayload().find("granted=false") != std::string::npos,
                  "Stale request got rejection");
            gotReply = true;
        }
    }
    check(gotReply, "A rejection response was sent");
}

// ── Test: Candidate log-freshness rules ────────────────────────────

void test_log_freshness()
{
    std::cout << "\n--- Test: Log Freshness Check ---\n";

    Network net;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Node 1 has log entries at index 1 (term 1) and index 2 (term 2)
    Storage storage;
    storage.appendEntry(1, 1, "cmd1");
    storage.appendEntry(2, 2, "cmd2");

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    // A candidate with lastLogIndex=1, lastLogTerm=1 is outdated
    Message outdated(2, 1, 1, MessageType::REQUEST_VOTE,
                     "lastLogIndex=1;lastLogTerm=1");
    engine.handleMessage(outdated);
    check(engine.getVotedFor() == -1,
          "Rejected candidate with outdated log (lower term)");

    // A candidate with lastLogIndex=2, lastLogTerm=2 is up-to-date
    Message uptodate(3, 1, 1, MessageType::REQUEST_VOTE,
                     "lastLogIndex=2;lastLogTerm=2");
    engine.handleMessage(uptodate);
    check(engine.getVotedFor() == 3,
          "Accepted candidate with up-to-date log");
}

// ── Test: Log freshness — same term, shorter log ───────────────────

void test_log_freshness_same_term()
{
    std::cout << "\n--- Test: Log Freshness — Same Term, Shorter Log ---\n";

    Network net;
    Node n1(1), n2(2);
    n1.start(); n2.start();
    net.registerNode(&n1);
    net.registerNode(&n2);

    // Node 1 has 3 entries all in term 1
    Storage storage;
    storage.appendEntry(1, 1, "cmd1");
    storage.appendEntry(2, 1, "cmd2");
    storage.appendEntry(3, 1, "cmd3");

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();

    // Candidate has same last term but shorter log
    Message shorter(2, 1, 1, MessageType::REQUEST_VOTE,
                    "lastLogIndex=1;lastLogTerm=1");
    engine.handleMessage(shorter);
    check(engine.getVotedFor() == -1,
          "Rejected candidate with shorter log (same term)");
}

// ── Test: Duplicate vote rejection ─────────────────────────────────

void test_duplicate_vote_rejection()
{
    std::cout << "\n--- Test: Duplicate Vote Rejection ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getVoteCount() == 1, "1 self-vote initially");

    // Receive a vote from node 2
    Message v1(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    engine.handleMessage(v1);
    check(engine.getVoteCount() == 2, "2 votes after node 2 grants");

    // Receive a DUPLICATE vote from node 2
    Message v2(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    engine.handleMessage(v2);
    check(engine.getVoteCount() == 2,
          "Still 2 votes (duplicate rejected)");
}

// ── Test: Majority calculation ─────────────────────────────────────

void test_majority_calculation()
{
    std::cout << "\n--- Test: Majority Calculation ---\n";

    Network net;
    Storage storage;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    RaftEngine eng3(1, 3, &net, &storage);
    check(eng3.getMajority() == 2, "3-node majority is 2");

    RaftEngine eng5(1, 5, &net, &storage);
    check(eng5.getMajority() == 3, "5-node majority is 3");

    RaftEngine eng1(1, 1, &net, &storage);
    check(eng1.getMajority() == 1, "1-node majority is 1");

    RaftEngine eng4(1, 4, &net, &storage);
    check(eng4.getMajority() == 3, "4-node majority is 3");
}

// ── Test: Leadership transition after majority ─────────────────────

void test_leadership_on_majority()
{
    std::cout << "\n--- Test: Leadership After Majority ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getRole() == RaftRole::CANDIDATE,
          "Engine is CANDIDATE after election start");
    check(engine.getVoteCount() == 1,
          "Has 1 self-vote");

    // Receive vote from node 2 — now has majority (2/3)
    Message v1(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    engine.handleMessage(v1);

    check(engine.getRole() == RaftRole::LEADER,
          "Becomes LEADER after majority");
    check(engine.getLeaderId() == 1,
          "Leader ID is self (1)");
    check(engine.getVoteCount() == 2,
          "Vote count is 2");
}

// ── Test: No leadership without majority ───────────────────────────

void test_no_leadership_without_majority()
{
    std::cout << "\n--- Test: No Leadership Without Majority ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3), n4(4), n5(5);
    n1.start(); n2.start(); n3.start(); n4.start(); n5.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);
    net.registerNode(&n4);
    net.registerNode(&n5);

    // 5-node cluster, majority = 3
    RaftEngine engine(1, 5, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getVoteCount() == 1, "Self-vote only");

    // Get 1 vote — not enough
    Message v1(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    engine.handleMessage(v1);

    check(engine.getRole() == RaftRole::CANDIDATE,
          "Still CANDIDATE with 2/5 votes");

    // Get a rejection — still not enough
    Message v2(3, 1, 1, MessageType::VOTE_RESPONSE, "granted=false");
    engine.handleMessage(v2);

    check(engine.getRole() == RaftRole::CANDIDATE,
          "Still CANDIDATE after rejection");
    check(engine.getVoteCount() == 2,
          "Vote count still 2 (rejection doesn't add)");
}

// ── Test: Higher-term step-down ────────────────────────────────────

void test_higher_term_stepdown()
{
    std::cout << "\n--- Test: Higher-Term Step-Down ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getRole() == RaftRole::CANDIDATE, "Is CANDIDATE");
    check(engine.getCurrentTerm() == 1, "Term is 1");

    // Receive a message with higher term
    Message higherTerm(2, 1, 5, MessageType::APPEND_ENTRIES, "heartbeat");
    engine.handleMessage(higherTerm);

    check(engine.getRole() == RaftRole::FOLLOWER,
          "Stepped down to FOLLOWER");
    check(engine.getCurrentTerm() == 5,
          "Term updated to 5");
    check(engine.getVotedFor() == -1,
          "Vote cleared after step-down");
}

// ── Test: Leader step-down on higher term ──────────────────────────

void test_leader_stepdown()
{
    std::cout << "\n--- Test: Leader Step-Down on Higher Term ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(1, 3, &net, &storage);
    engine.start();
    engine.startElection(0);

    // Grant majority
    Message v1(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    engine.handleMessage(v1);
    check(engine.getRole() == RaftRole::LEADER, "Is LEADER");

    // Higher-term message arrives
    Message higherTerm(3, 1, 10, MessageType::APPEND_ENTRIES, "heartbeat");
    engine.handleMessage(higherTerm);

    check(engine.getRole() == RaftRole::FOLLOWER,
          "Leader stepped down to FOLLOWER");
    check(engine.getCurrentTerm() == 10,
          "Term updated to 10");
}

// ── Test: Basic heartbeat handling ─────────────────────────────────

void test_heartbeat_handling()
{
    std::cout << "\n--- Test: Heartbeat Handling ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Node 2 is a follower
    RaftEngine follower(2, 3, &net, &storage, 100, 10, 50);
    follower.start();

    check(follower.getLeaderId() == -1, "No leader known initially");

    // Receive heartbeat from node 1
    Message hb(1, 2, 1, MessageType::APPEND_ENTRIES, "heartbeat");
    follower.handleMessage(hb);

    check(follower.getLeaderId() == 1,
          "Leader recognized as node 1 after heartbeat");
    check(follower.getRole() == RaftRole::FOLLOWER,
          "Remains FOLLOWER after heartbeat");
    check(follower.getCurrentTerm() == 1,
          "Term updated to match heartbeat");
}

// ── Test: Heartbeat resets election timer ───────────────────────────

void test_heartbeat_resets_timer()
{
    std::cout << "\n--- Test: Heartbeat Resets Election Timer ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Node 2 with short election timeout
    RaftEngine follower(2, 3, &net, &storage, 20, 0, 50);
    follower.start();

    // Tick close to timeout but not past it
    for (int t = 1; t <= 15; t++) {
        follower.tick(t);
    }
    check(follower.getRole() == RaftRole::FOLLOWER,
          "Still FOLLOWER before timeout");

    // Receive heartbeat — should reset timer
    Message hb(1, 2, 1, MessageType::APPEND_ENTRIES, "heartbeat");
    follower.handleMessage(hb);

    // Continue ticking — should NOT time out because timer was reset
    for (int t = 16; t <= 30; t++) {
        follower.tick(t);
    }
    check(follower.getRole() == RaftRole::FOLLOWER,
          "Still FOLLOWER after heartbeat reset timer");
}

// ── Test: Stale heartbeat does not reset timer ─────────────────────

void test_stale_heartbeat_rejected()
{
    std::cout << "\n--- Test: Stale Heartbeat Does Not Reset Timer ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine follower(2, 3, &net, &storage, 20, 0, 50);
    follower.start();

    // First, accept a term-5 heartbeat to get to term 5
    Message hb5(1, 2, 5, MessageType::APPEND_ENTRIES, "heartbeat");
    follower.handleMessage(hb5);
    check(follower.getCurrentTerm() == 5, "Term is 5");

    // Now send a stale heartbeat at term 2
    Message staleHb(1, 2, 2, MessageType::APPEND_ENTRIES, "heartbeat");
    follower.handleMessage(staleHb);

    // The leader ID should still be from the valid heartbeat, not reset
    check(follower.getCurrentTerm() == 5,
          "Term unchanged from stale heartbeat");
}

// ── Test: Candidate steps down on valid leader in same term ────────

void test_candidate_stepdown_on_leader()
{
    std::cout << "\n--- Test: Candidate Steps Down on Leader Heartbeat ---\n";

    Network net;
    Storage storage;
    Node n1(1), n2(2), n3(3);
    n1.start(); n2.start(); n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    RaftEngine engine(2, 3, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getRole() == RaftRole::CANDIDATE, "Is CANDIDATE");
    int electionTerm = engine.getCurrentTerm();

    // Node 1 sends a heartbeat in the same term — it already won
    Message leaderHb(1, 2, electionTerm, MessageType::APPEND_ENTRIES, "heartbeat");
    engine.handleMessage(leaderHb);

    check(engine.getRole() == RaftRole::FOLLOWER,
          "Stepped down to FOLLOWER on leader heartbeat");
    check(engine.getLeaderId() == 1,
          "Recognized node 1 as leader");
}

// ── Test: Single-node cluster elects itself ────────────────────────

void test_single_node_election()
{
    std::cout << "\n--- Test: Single-Node Cluster ---\n";

    Network net;
    Storage storage;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    RaftEngine engine(1, 1, &net, &storage);
    engine.start();
    engine.startElection(0);

    check(engine.getRole() == RaftRole::LEADER,
          "Single-node cluster: immediate LEADER");
    check(engine.getLeaderId() == 1,
          "Leader is self");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  RaftEngine Tests\n";
    std::cout << "========================================\n";

    test_initial_state();
    test_election_timeout_transition();
    test_self_voting();
    test_term_increment();
    test_vote_once_per_term();
    test_stale_term_rejection();
    test_log_freshness();
    test_log_freshness_same_term();
    test_duplicate_vote_rejection();
    test_majority_calculation();
    test_leadership_on_majority();
    test_no_leadership_without_majority();
    test_higher_term_stepdown();
    test_leader_stepdown();
    test_heartbeat_handling();
    test_heartbeat_resets_timer();
    test_stale_heartbeat_rejected();
    test_candidate_stepdown_on_leader();
    test_single_node_election();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}
