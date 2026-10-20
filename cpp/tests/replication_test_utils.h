#ifndef REPLICATION_TEST_UTILS_H
#define REPLICATION_TEST_UTILS_H

/*
 * replication_test_utils.h
 * ========================
 * Shared helpers for the log replication tests.
 *
 * ReplicationHarness wires N RaftEngines to one real Network and one
 * real Storage each (no RaftCluster), with very long timeouts so the
 * tests decide exactly when messages are delivered.
 */

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "Network.h"
#include "Node.h"
#include "RaftEngine.h"
#include "Storage.h"

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

int finish(const std::string& title)
{
    std::cout << "\n========================================\n";
    std::cout << "  " << title << ": " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";
    return tests_failed > 0 ? 1 : 0;
}

// Build an APPEND_ENTRIES payload in the RaftEngine wire format.
std::string appendEntriesPayload(
    int prevLogIndex, int prevLogTerm, int leaderCommit,
    const std::vector<std::pair<int, std::string>>& entries)
{
    std::ostringstream oss;
    oss << "prevLogIndex=" << prevLogIndex
        << ";prevLogTerm=" << prevLogTerm
        << ";leaderCommit=" << leaderCommit
        << ";entries=" << entries.size() << ";";
    for (const auto& entry : entries) {
        oss << entry.first << "," << entry.second.size() << ":"
            << entry.second;
    }
    return oss.str();
}

// Build an APPEND_RESPONSE payload in the RaftEngine wire format.
std::string appendResponsePayload(bool success, int matchIndex,
                                  int prevLogIndex, int hintIndex)
{
    std::ostringstream oss;
    oss << "success=" << (success ? 1 : 0)
        << ";matchIndex=" << matchIndex
        << ";prevLogIndex=" << prevLogIndex
        << ";hintIndex=" << hintIndex << ";";
    return oss.str();
}

// Read "<key>=<int>" from a payload (-1 if absent).
int payloadField(const std::string& payload, const std::string& key)
{
    std::string prefix = key + "=";
    auto pos = payload.find(prefix);
    if (pos == std::string::npos) return -1;
    return std::stoi(payload.substr(pos + prefix.size()));
}

// "<index>:<term>:<command>|..." for comparing whole logs.
std::string logSignature(const Storage& storage)
{
    std::ostringstream oss;
    for (int i = 1; i <= storage.getLastLogIndex(); i++) {
        const LogEntry* entry = storage.getEntry(i);
        if (entry == nullptr) {
            oss << "<missing " << i << ">|";
            continue;
        }
        oss << entry->index << ":" << entry->term << ":"
            << entry->command << "|";
    }
    return oss.str();
}

// Every entry sits at the position matching its index (1..N).
bool logIsContiguous(const Storage& storage)
{
    if (static_cast<int>(storage.getLogSize()) != storage.getLastLogIndex()) {
        return false;
    }
    for (int i = 1; i <= storage.getLastLogIndex(); i++) {
        const LogEntry* entry = storage.getEntry(i);
        if (entry == nullptr || entry->index != i) {
            return false;
        }
    }
    return true;
}

class ReplicationHarness {
public:
    explicit ReplicationHarness(int size) : size_(size)
    {
        for (int id = 1; id <= size; id++) {
            nodes_.push_back(std::make_unique<Node>(id));
            nodes_.back()->start();
            network_.registerNode(nodes_.back().get());
            logs_.push_back(std::make_unique<Storage>());
            // Huge timeouts: the tests drive everything explicitly.
            engines_.push_back(std::make_unique<RaftEngine>(
                id, size, &network_, logs_.back().get(), 100000, 0, 100000));
            engines_.back()->start();
        }
    }

    RaftEngine& engine(int id) { return *engines_[id - 1]; }
    Storage&    log(int id)    { return *logs_[id - 1]; }
    Node&       node(int id)   { return *nodes_[id - 1]; }
    Network&    network()      { return network_; }

    // Pop the next message queued for `id` (false if none).
    bool pop(int id, Message& out) { return network_.receiveMessage(id, out); }

    // Discard everything queued for `id`.
    int drop(int id)
    {
        Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");
        int dropped = 0;
        while (network_.receiveMessage(id, msg)) dropped++;
        return dropped;
    }

    void dropAll()
    {
        for (int id = 1; id <= size_; id++) drop(id);
    }

    // Deliver all messages queued for active nodes, repeating until the
    // network is quiet.  Returns the number delivered, or -1 if the
    // round limit was hit (a sign of an unbounded message loop).
    int deliverAll(int maxRounds = 1000)
    {
        int delivered = 0;
        for (int round = 0; round < maxRounds; round++) {
            bool any = false;
            for (int id = 1; id <= size_; id++) {
                if (!nodes_[id - 1]->isActive()) continue;
                Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");
                while (network_.receiveMessage(id, msg)) {
                    engines_[id - 1]->handleMessage(msg);
                    delivered++;
                    any = true;
                }
            }
            if (!any) return delivered;
        }
        return -1;
    }

    // Make `id` leader of `term` (>= 1) through a real election, then
    // clear the network so tests start from a quiet state.
    void makeLeader(int id, int term)
    {
        int other = (id % size_) + 1;
        if (term > 1) {
            // Adopt term - 1 first so the election lands on `term`.
            engine(id).handleMessage(Message(
                other, id, term - 1, MessageType::APPEND_ENTRIES, "heartbeat"));
        }
        engine(id).startElection(0);
        for (int peer = 1; peer <= size_; peer++) {
            if (peer == id || engine(id).getRole() == RaftRole::LEADER) continue;
            engine(id).handleMessage(Message(
                peer, id, term, MessageType::VOTE_RESPONSE, "granted=true"));
        }
        dropAll();
    }

private:
    int size_;
    Network network_;
    std::vector<std::unique_ptr<Node>>       nodes_;
    std::vector<std::unique_ptr<Storage>>    logs_;
    std::vector<std::unique_ptr<RaftEngine>> engines_;
};

#endif
