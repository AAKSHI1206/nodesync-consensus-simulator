/*
 * test_append_entries.cpp
 * =======================
 * Unit tests for the AppendEntries protocol in RaftEngine:
 * follower log matching, responses, leader nextIndex/matchIndex
 * tracking, backtracking, majority commitment and the current-term
 * commit rule.
 */

#include <vector>
#include "replication_test_utils.h"

// ── Follower: accepts entries and replies ──────────────────────────

void test_follower_appends_entries()
{
    std::cout << "\n--- Test: Follower Appends Entries ---\n";

    ReplicationHarness h(3);
    RaftEngine& follower = h.engine(2);

    // Commands contain the payload's own delimiters on purpose.
    std::string payload = appendEntriesPayload(
        0, 0, 0, {{1, "ORDER A"}, {1, "ORDER;B:x,y"}});
    follower.handleMessage(
        Message(1, 2, 1, MessageType::APPEND_ENTRIES, payload));

    const LogEntry* e1 = h.log(2).getEntry(1);
    const LogEntry* e2 = h.log(2).getEntry(2);
    check(h.log(2).getLastLogIndex() == 2, "Follower log has 2 entries");
    check(e1 && e1->term == 1 && std::string(e1->command) == "ORDER A",
          "Entry 1 stored with term and command");
    check(e2 && std::string(e2->command) == "ORDER;B:x,y",
          "Command with ';' ':' ',' survives encoding");
    check(follower.getCurrentTerm() == 1 && follower.getLeaderId() == 1,
          "Follower adopted leader's term and identity");
    check(follower.getCommitIndex() == 0,
          "Nothing committed (leaderCommit = 0)");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    bool got = h.pop(1, reply);
    check(got && reply.getType() == MessageType::APPEND_RESPONSE,
          "APPEND_RESPONSE sent back to leader");
    check(payloadField(reply.getPayload(), "success") == 1 &&
          payloadField(reply.getPayload(), "matchIndex") == 2,
          "Response: success, matchIndex = 2");
}

// ── Follower: rejects a missing previous entry ─────────────────────

void test_follower_rejects_missing_prev()
{
    std::cout << "\n--- Test: Follower Rejects Missing prevLogIndex ---\n";

    ReplicationHarness h(3);
    h.log(2).appendEntry(1, 1, "a");

    std::string payload = appendEntriesPayload(3, 1, 0, {{1, "d"}});
    h.engine(2).handleMessage(
        Message(1, 2, 1, MessageType::APPEND_ENTRIES, payload));

    check(h.log(2).getLastLogIndex() == 1, "Log unchanged on rejection");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(1, reply);
    check(payloadField(reply.getPayload(), "success") == 0,
          "Response: failure");
    check(payloadField(reply.getPayload(), "prevLogIndex") == 3,
          "Response echoes prevLogIndex = 3");
    check(payloadField(reply.getPayload(), "hintIndex") == 1,
          "Hint = follower's last index (1)");
}

// ── Follower: rejects a previous-term mismatch ─────────────────────

void test_follower_rejects_prev_term_mismatch()
{
    std::cout << "\n--- Test: Follower Rejects prevLogTerm Mismatch ---\n";

    ReplicationHarness h(3);
    h.log(2).appendEntry(1, 1, "a");
    h.log(2).appendEntry(2, 1, "b");

    std::string before = logSignature(h.log(2));
    std::string payload = appendEntriesPayload(2, 2, 0, {{2, "c"}});
    h.engine(2).handleMessage(
        Message(1, 2, 2, MessageType::APPEND_ENTRIES, payload));

    check(logSignature(h.log(2)) == before,
          "Rejection does not truncate or append");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(1, reply);
    check(payloadField(reply.getPayload(), "success") == 0,
          "Response: failure");
    check(payloadField(reply.getPayload(), "hintIndex") == 1,
          "Hint = prevLogIndex - 1");
}

// ── Follower: commit index bounded by the last new entry ───────────

void test_follower_commit_bounded()
{
    std::cout << "\n--- Test: Follower Commit Bounded by Last New Entry ---\n";

    ReplicationHarness h(3);
    std::vector<int> applied;
    h.engine(2).setApplyCallback(
        [&](int index, int, const std::string&) { applied.push_back(index); });

    // Leader claims commit 5 but only sends entry 1.
    std::string payload = appendEntriesPayload(0, 0, 5, {{1, "a"}});
    h.engine(2).handleMessage(
        Message(1, 2, 1, MessageType::APPEND_ENTRIES, payload));

    check(h.engine(2).getCommitIndex() == 1,
          "commitIndex = min(leaderCommit, last new entry) = 1");
    check(applied.size() == 1 && applied[0] == 1,
          "Entry 1 applied exactly once");

    // A heartbeat repeating the same information applies nothing new.
    payload = appendEntriesPayload(1, 1, 5, {});
    h.engine(2).handleMessage(
        Message(1, 2, 1, MessageType::APPEND_ENTRIES, payload));
    check(applied.size() == 1, "Repeated heartbeat does not re-apply");
    check(h.engine(2).getLastApplied() == 1, "lastApplied = 1");
}

// ── Follower: unverified tail entries are not committed ────────────

void test_commit_ignores_unverified_tail()
{
    std::cout << "\n--- Test: Commit Ignores Unverified Tail Entries ---\n";

    ReplicationHarness h(3);
    for (int i = 1; i <= 4; i++) {
        h.log(2).appendEntry(i, 1, "cmd" + std::to_string(i));
    }
    std::vector<int> applied;
    h.engine(2).setApplyCallback(
        [&](int index, int, const std::string&) { applied.push_back(index); });

    // Leader vouches only for entries 1-2 but reports commit 4.
    h.engine(2).handleMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES,
                                      appendEntriesPayload(2, 1, 4, {})));

    check(h.engine(2).getCommitIndex() == 2,
          "commitIndex = prevLogIndex + entries (2), not lastLogIndex (4)");
    check(applied == std::vector<int>({1, 2}), "Only entries 1-2 applied");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(1, reply);
    check(payloadField(reply.getPayload(), "matchIndex") == 2,
          "Acknowledged matchIndex = 2, not the unverified 4");
}

// ── Candidate keeps its own vote when it steps down ────────────────

void test_candidate_keeps_vote_on_step_down()
{
    std::cout << "\n--- Test: Candidate Keeps Its Vote After Stepping Down ---\n";

    ReplicationHarness h(3);
    RaftEngine& node2 = h.engine(2);
    node2.startElection(0);
    int term = node2.getCurrentTerm();
    h.dropAll();

    node2.handleMessage(Message(1, 2, term, MessageType::APPEND_ENTRIES,
                                appendEntriesPayload(0, 0, 0, {})));
    check(node2.getRole() == RaftRole::FOLLOWER, "Stepped down to FOLLOWER");
    check(node2.getVotedFor() == 2, "Still records its self-vote for this term");

    // A third node asking for a vote in the same term must be refused.
    node2.handleMessage(Message(3, 2, term, MessageType::REQUEST_VOTE,
                                "lastLogIndex=0;lastLogTerm=0"));
    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    bool refused = false;
    while (h.pop(3, reply)) {
        if (reply.getType() == MessageType::VOTE_RESPONSE &&
            reply.getPayload() == "granted=false") {
            refused = true;
        }
    }
    check(refused, "Second vote in the same term refused");
}

// ── Follower: legacy "heartbeat" payload stays compatible ──────────

void test_legacy_heartbeat_compatible()
{
    std::cout << "\n--- Test: Legacy Heartbeat Payload ---\n";

    ReplicationHarness h(3);
    h.engine(2).handleMessage(
        Message(1, 2, 1, MessageType::APPEND_ENTRIES, "heartbeat"));

    check(h.engine(2).getLeaderId() == 1, "Leader recognised");
    check(h.log(2).getLastLogIndex() == 0, "Log untouched");
    check(h.network().getPendingCount(1) == 0,
          "No response (no log metadata to answer)");
}

// ── Leader: progress initialised on election ───────────────────────

void test_leader_initialises_progress()
{
    std::cout << "\n--- Test: Leader Initialises nextIndex/matchIndex ---\n";

    ReplicationHarness h(3);
    h.log(1).appendEntry(1, 1, "a");
    h.log(1).appendEntry(2, 1, "b");
    h.makeLeader(1, 2);

    check(h.engine(1).getRole() == RaftRole::LEADER, "Node 1 is LEADER");
    check(h.engine(1).getNextIndex(2) == 3 && h.engine(1).getNextIndex(3) == 3,
          "nextIndex = lastLogIndex + 1 for every peer");
    check(h.engine(1).getMatchIndex(2) == 0 && h.engine(1).getMatchIndex(3) == 0,
          "matchIndex = 0 for every peer");
    check(h.engine(2).getNextIndex(1) == -1,
          "Followers expose no replication progress");
}

// ── Leader: successful acknowledgement updates progress ────────────

void test_leader_tracks_success()
{
    std::cout << "\n--- Test: Leader Tracks Successful Replication ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    RaftEngine& leader = h.engine(1);

    int index = leader.propose("ORDER-1");
    check(index == 1, "propose() returns log index 1");
    check(leader.getCommitIndex() == 0,
          "Not committed before any acknowledgement");

    // Deliver only to node 2, then its reply to the leader.
    Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");
    while (h.pop(2, msg)) h.engine(2).handleMessage(msg);
    while (h.pop(1, msg)) leader.handleMessage(msg);

    check(leader.getMatchIndex(2) == 1, "matchIndex[2] = 1");
    check(leader.getNextIndex(2) == 2, "nextIndex[2] = 2");
    check(leader.getMatchIndex(3) == 0, "matchIndex[3] still 0");
    check(leader.getCommitIndex() == 1,
          "Committed: leader + node 2 = 2 of 3");
}

// ── Leader: one acknowledgement is not a majority of five ──────────

void test_majority_of_five()
{
    std::cout << "\n--- Test: Majority in a 5-Node Cluster ---\n";

    ReplicationHarness h(5);
    h.makeLeader(1, 1);
    RaftEngine& leader = h.engine(1);
    leader.propose("ORDER-1");
    h.dropAll();   // acknowledgements are injected by hand below

    std::string ok = appendResponsePayload(true, 1, 0, 1);
    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE, ok));
    check(leader.getCommitIndex() == 0,
          "Leader + 1 follower (2 of 5) does not commit");

    // The same follower acknowledging twice still counts once.
    leader.handleMessage(Message(2, 1, 1, MessageType::APPEND_RESPONSE, ok));
    check(leader.getCommitIndex() == 0,
          "Duplicate acknowledgement does not count twice");

    leader.handleMessage(Message(3, 1, 1, MessageType::APPEND_RESPONSE, ok));
    check(leader.getCommitIndex() == 1,
          "Leader + 2 followers (3 of 5) commits");
}

// ── Leader: current-term commit rule ───────────────────────────────

void test_current_term_rule()
{
    std::cout << "\n--- Test: Current-Term Commit Rule ---\n";

    ReplicationHarness h(3);
    h.log(1).appendEntry(1, 1, "old");   // entry from an earlier term
    h.makeLeader(1, 2);
    RaftEngine& leader = h.engine(1);

    std::vector<int> applied;
    leader.setApplyCallback(
        [&](int index, int, const std::string&) { applied.push_back(index); });

    // Node 2 confirms it holds index 1 (term 1).
    leader.handleMessage(Message(2, 1, 2, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 1, 1, 1)));
    check(leader.getMatchIndex(2) == 1, "matchIndex[2] = 1");
    check(leader.getCommitIndex() == 0,
          "Majority holds a term-1 entry, but it is NOT committed in term 2");
    check(applied.empty(), "Nothing applied");

    // A term-2 entry replicated to the same majority commits both.
    leader.propose("new");
    h.dropAll();
    leader.handleMessage(Message(2, 1, 2, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(true, 2, 1, 2)));
    check(leader.getCommitIndex() == 2,
          "Term-2 entry committed; term-1 entry committed with it");
    check(applied.size() == 2 && applied[0] == 1 && applied[1] == 2,
          "Applied in order: 1, 2");
}

// ── Leader: backtracking to a lagging follower ─────────────────────

void test_backtracking_catch_up()
{
    std::cout << "\n--- Test: nextIndex Backtracking ---\n";

    ReplicationHarness h(3);
    for (int i = 1; i <= 3; i++) {
        h.log(1).appendEntry(i, 1, "cmd" + std::to_string(i));
    }
    h.makeLeader(1, 2);   // nextIndex[2] = 4, but node 2's log is empty

    h.node(3).stop();     // keep this test to the leader and node 2
    h.engine(1).propose("cmd4");
    int delivered = h.deliverAll();

    check(delivered > 0 && delivered < 20,
          "Catch-up completes in a bounded number of messages");
    check(logSignature(h.log(2)) == logSignature(h.log(1)),
          "Follower log equals leader log after backtracking");
    check(h.engine(1).getMatchIndex(2) == 4, "matchIndex[2] = 4");
    check(h.engine(1).getNextIndex(2) == 5, "nextIndex[2] = 5");
    check(h.engine(1).getCommitIndex() == 4,
          "All entries committed via the term-2 entry");
}

// ── Leader: the follower's hint moves nextIndex in one step ────────

void test_hint_jumps_next_index()
{
    std::cout << "\n--- Test: Rejection Hint Moves nextIndex in One Step ---\n";

    ReplicationHarness h(3);
    for (int i = 1; i <= 10; i++) h.log(1).appendEntry(i, 1, "c");
    h.makeLeader(1, 2);
    RaftEngine& leader = h.engine(1);
    check(leader.getNextIndex(2) == 11, "nextIndex[2] starts at 11");

    // Follower only has 3 entries.
    leader.handleMessage(Message(2, 1, 2, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(false, 0, 10, 3)));
    check(leader.getNextIndex(2) == 4,
          "One rejection with hint 3 sets nextIndex to 4 (not 10)");

    Message retry(0, 0, 0, MessageType::REQUEST_VOTE, "");
    bool got = h.pop(2, retry);
    check(got && payloadField(retry.getPayload(), "prevLogIndex") == 3,
          "Retry sent with prevLogIndex = 3");

    // A malformed, huge hint must not overflow: falls back to one step.
    leader.handleMessage(Message(2, 1, 2, MessageType::APPEND_RESPONSE,
                                 appendResponsePayload(false, 0, 3, 2147483647)));
    check(leader.getNextIndex(2) == 3,
          "Huge hint handled safely (nextIndex 4 -> 3)");
}

// ── Leader: commands the C log cannot store are refused ────────────

void test_propose_rejects_nul()
{
    std::cout << "\n--- Test: propose() Rejects Embedded NUL ---\n";

    ReplicationHarness h(3);
    h.makeLeader(1, 1);
    std::string bad("ORDER\0HIDDEN", 12);
    check(h.engine(1).propose(bad) == -1, "propose() returns -1");
    check(h.log(1).getLastLogIndex() == 0, "Nothing appended");
    check(h.engine(2).propose("x") == -1, "Followers cannot propose");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  AppendEntries Protocol Tests\n";
    std::cout << "========================================\n";

    test_follower_appends_entries();
    test_follower_rejects_missing_prev();
    test_follower_rejects_prev_term_mismatch();
    test_follower_commit_bounded();
    test_commit_ignores_unverified_tail();
    test_candidate_keeps_vote_on_step_down();
    test_legacy_heartbeat_compatible();
    test_leader_initialises_progress();
    test_leader_tracks_success();
    test_majority_of_five();
    test_current_term_rule();
    test_backtracking_catch_up();
    test_hint_jumps_next_index();
    test_propose_rejects_nul();

    return finish("AppendEntries Protocol Tests");
}
