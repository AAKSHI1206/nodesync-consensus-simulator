/*
 * test_log_storage.cpp
 * ====================
 * Storage behaviour that log replication relies on (append, lookup,
 * truncate, index/term invariants over the C linked-list log) and
 * follower conflict resolution on top of it.
 */

#include <string>
#include "replication_test_utils.h"

// ── Storage: truncate in the middle keeps the prefix ───────────────

void test_truncate_keeps_prefix()
{
    std::cout << "\n--- Test: Truncate Keeps Earlier Entries ---\n";

    Storage s;
    s.appendEntry(1, 1, "a");
    s.appendEntry(2, 1, "b");
    s.appendEntry(3, 2, "c");
    s.appendEntry(4, 2, "d");

    int removed = s.truncateFrom(3);
    check(removed == 2, "truncateFrom(3) removes 2 entries");
    check(s.getLastLogIndex() == 2 && s.getLastLogTerm() == 1,
          "Last index/term now 2/1");
    check(s.getEntry(2) && std::string(s.getEntry(2)->command) == "b",
          "Entry 2 intact");
    check(s.getEntry(3) == nullptr, "Entry 3 gone");
}

// ── Storage: append after truncate stays contiguous ────────────────

void test_append_after_truncate()
{
    std::cout << "\n--- Test: Append After Truncate Stays Contiguous ---\n";

    Storage s;
    s.appendEntry(1, 1, "a");
    s.appendEntry(2, 1, "b");
    s.appendEntry(3, 1, "c");
    s.truncateFrom(2);
    s.appendEntry(2, 3, "B");
    s.appendEntry(3, 3, "C");

    check(logIsContiguous(s), "Index equals position for every entry");
    check(s.getEntry(2)->term == 3 &&
          std::string(s.getEntry(2)->command) == "B",
          "Lookup by index returns the new entry");
    check(s.getLastLogTerm() == 3, "Last term updated to 3");
}

// ── Storage: edge cases used by the protocol ───────────────────────

void test_storage_edge_cases()
{
    std::cout << "\n--- Test: Storage Edge Cases ---\n";

    Storage s;
    check(s.getEntry(0) == nullptr, "getEntry(0) is null (index 0 = empty prefix)");
    check(s.getLastLogIndex() == 0 && s.getLastLogTerm() == 0,
          "Empty log: last index/term 0/0");

    s.appendEntry(1, 1, "a");
    s.appendEntry(2, 1, "b");
    check(s.getEntry(3) == nullptr, "getEntry(last + 1) is null");
    check(s.truncateFrom(5) == 0, "Truncate past the end removes nothing");
    check(s.truncateFrom(0) == -1, "truncateFrom(0) is rejected");
    check(s.truncateFrom(1) == 2 && s.getLogSize() == 0,
          "truncateFrom(1) empties the log");

    std::string command = "ORDER-1";
    s.appendEntry(1, 1, command);
    command[0] = 'X';
    check(std::string(s.getEntry(1)->command) == "ORDER-1",
          "Storage keeps its own copy of the command");
}

// ── Conflict: follower replaces uncommitted conflicting entries ────

void test_conflict_resolution()
{
    std::cout << "\n--- Test: Conflicting Entries Replaced ---\n";

    ReplicationHarness h(3);
    Storage& log = h.log(2);
    log.appendEntry(1, 1, "a");
    log.appendEntry(2, 1, "b");
    log.appendEntry(3, 2, "stale-x");   // written by an old leader
    log.appendEntry(4, 2, "stale-y");

    // Term-2 leader told node 2 that index 2 is committed.
    h.engine(2).handleMessage(Message(1, 2, 2, MessageType::APPEND_ENTRIES,
                                      appendEntriesPayload(2, 1, 2, {})));
    check(h.engine(2).getCommitIndex() == 2, "Entries 1-2 committed");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(1, reply);
    check(payloadField(reply.getPayload(), "matchIndex") == 2,
          "Acknowledges only what the request covered (2), not 4");

    // New term-3 leader overwrites from index 3.
    h.engine(2).handleMessage(Message(3, 2, 3, MessageType::APPEND_ENTRIES,
        appendEntriesPayload(2, 1, 2, {{3, "c"}, {3, "d"}, {3, "e"}})));

    check(std::string(log.getEntry(1)->command) == "a" &&
          std::string(log.getEntry(2)->command) == "b",
          "Matching earlier entries retained");
    check(log.getEntry(3)->term == 3 &&
          std::string(log.getEntry(3)->command) == "c",
          "Conflicting entry 3 replaced");
    check(log.getLastLogIndex() == 5 && log.getLastLogTerm() == 3,
          "Log now ends at index 5, term 3");
    check(logIsContiguous(log), "Index/position invariant holds");
}

// ── Conflict: matching entries are never truncated ─────────────────

void test_matching_entries_not_truncated()
{
    std::cout << "\n--- Test: Matching Entries Not Truncated ---\n";

    ReplicationHarness h(3);
    Storage& log = h.log(2);
    for (int i = 1; i <= 5; i++) {
        log.appendEntry(i, 1, "cmd" + std::to_string(i));
    }
    std::string before = logSignature(log);

    // A delayed request carrying only the first two (matching) entries.
    h.engine(2).handleMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES,
        appendEntriesPayload(0, 0, 0, {{1, "cmd1"}, {1, "cmd2"}})));

    check(logSignature(log) == before,
          "Entries 3-5 kept: no truncation when incoming entries match");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(1, reply);
    check(payloadField(reply.getPayload(), "success") == 1 &&
          payloadField(reply.getPayload(), "matchIndex") == 2,
          "Reply: success, matchIndex = 2 (prev + entries), not 5");
}

// ── Conflict: committed entries are never overwritten ──────────────

void test_committed_entry_protected()
{
    std::cout << "\n--- Test: Committed Entries Never Overwritten ---\n";

    ReplicationHarness h(3);
    Storage& log = h.log(2);
    h.engine(2).handleMessage(Message(1, 2, 1, MessageType::APPEND_ENTRIES,
        appendEntriesPayload(0, 0, 2, {{1, "a"}, {1, "b"}})));
    check(h.engine(2).getCommitIndex() == 2, "Entries 1-2 committed");
    h.drop(1);

    // A (buggy) leader tries to replace committed entry 2.
    h.engine(2).handleMessage(Message(3, 2, 4, MessageType::APPEND_ENTRIES,
        appendEntriesPayload(1, 1, 2, {{4, "evil"}})));

    check(std::string(log.getEntry(2)->command) == "b" &&
          log.getEntry(2)->term == 1,
          "Committed entry 2 unchanged");
    check(log.getLastLogIndex() == 2, "Nothing appended");

    Message reply(0, 0, 0, MessageType::REQUEST_VOTE, "");
    h.pop(3, reply);
    check(payloadField(reply.getPayload(), "success") == 0,
          "Request rejected");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Log Storage & Conflict Tests\n";
    std::cout << "========================================\n";

    test_truncate_keeps_prefix();
    test_append_after_truncate();
    test_storage_edge_cases();
    test_conflict_resolution();
    test_matching_entries_not_truncated();
    test_committed_entry_protected();

    return finish("Log Storage & Conflict Tests");
}
