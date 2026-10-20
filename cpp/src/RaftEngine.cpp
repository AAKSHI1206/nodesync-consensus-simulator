#include "RaftEngine.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <cstdint>
#include <climits>
#include <utility>

/*
 * RaftEngine.cpp
 * ==============
 * Implementation of the Raft leader election engine.
 *
 * Implements:
 *   - Follower / Candidate / Leader role transitions
 *   - Term management and monotonic increments
 *   - Election initiation on timeout
 *   - REQUEST_VOTE generation with log metadata
 *   - Vote granting with Raft safety rules (one vote per term,
 *     log-freshness check)
 *   - Distinct vote counting and strict majority requirement
 *   - Higher-term step-down
 *   - Leader heartbeats via APPEND_ENTRIES
 *   - Deterministic simulated-time election and heartbeat timers
 *   - Log replication: APPEND_ENTRIES with prevLogIndex/prevLogTerm,
 *     follower log matching and conflict resolution, APPEND_RESPONSE
 *   - Per-peer nextIndex / matchIndex tracking and backtracking
 *   - Majority commitment (current-term rule) and in-order apply
 *
 * Reuses existing: Message, Network, Storage.
 */

// ── Utility ────────────────────────────────────────────────────────

std::string raftRoleToString(RaftRole role)
{
    switch (role) {
        case RaftRole::FOLLOWER:  return "FOLLOWER";
        case RaftRole::CANDIDATE: return "CANDIDATE";
        case RaftRole::LEADER:    return "LEADER";
        default:                  return "UNKNOWN";
    }
}

// Encode a RequestVote payload:
//   "lastLogIndex=<I>;lastLogTerm=<T>"
static std::string encodeVoteRequest(int lastLogIndex, int lastLogTerm)
{
    std::ostringstream oss;
    oss << "lastLogIndex=" << lastLogIndex
        << ";lastLogTerm=" << lastLogTerm;
    return oss.str();
}

// Decode a RequestVote payload.
static bool decodeVoteRequest(const std::string& payload,
                              int& lastLogIndex, int& lastLogTerm)
{
    // Expected format: "lastLogIndex=<I>;lastLogTerm=<T>"
    auto posIdx = payload.find("lastLogIndex=");
    auto posTrm = payload.find("lastLogTerm=");
    if (posIdx == std::string::npos || posTrm == std::string::npos) {
        return false;
    }
    lastLogIndex = std::stoi(payload.substr(posIdx + 13));
    lastLogTerm  = std::stoi(payload.substr(posTrm + 12));
    return true;
}

// Encode a VoteResponse payload:
//   "granted=true" or "granted=false"
static std::string encodeVoteResponse(bool granted)
{
    return granted ? "granted=true" : "granted=false";
}

// Decode a VoteResponse payload.
static bool decodeVoteResponse(const std::string& payload)
{
    return payload.find("granted=true") != std::string::npos;
}

// ── AppendEntries payload codec ────────────────────────────────────
//
// APPEND_ENTRIES payload:
//   "prevLogIndex=<P>;prevLogTerm=<T>;leaderCommit=<C>;entries=<N>;"
//   followed by N entries, each "<term>,<length>:<command>".
//   The command is length-prefixed so it may contain the delimiters
//   (any character except '\0', which the C log cannot store).
//   At most kMaxEntriesPerMessage entries are sent per message.
//
// APPEND_RESPONSE payload:
//   "success=<0|1>;matchIndex=<M>;prevLogIndex=<P>;hintIndex=<H>;"
//   matchIndex   - on success, last index known to match the leader
//   prevLogIndex - echoed from the request (to spot stale rejections)
//   hintIndex    - on failure, highest index the follower might match

static const int kMaxEntriesPerMessage = 64;

struct AppendEntriesArgs {
    int prevLogIndex = 0;
    int prevLogTerm  = 0;
    int leaderCommit = 0;
    std::vector<std::pair<int, std::string>> entries;   // (term, command)
};

struct AppendResponseArgs {
    bool success      = false;
    int  matchIndex   = 0;
    int  prevLogIndex = 0;
    int  hintIndex    = 0;
};

// Read a non-negative integer at pos that ends with `terminator`.
static bool readInt(const std::string& s, std::size_t& pos,
                    char terminator, int& out)
{
    std::size_t start = pos;
    long long value = 0;
    while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') {
        value = value * 10 + (s[pos] - '0');
        if (value > INT32_MAX) {
            return false;
        }
        pos++;
    }
    if (pos == start || pos >= s.size() || s[pos] != terminator) {
        return false;
    }
    pos++;   // skip the terminator
    out = static_cast<int>(value);
    return true;
}

// Read "<key>=<int>;" at pos.
static bool readField(const std::string& s, std::size_t& pos,
                      const std::string& key, int& out)
{
    std::string prefix = key + "=";
    if (s.compare(pos, prefix.size(), prefix) != 0) {
        return false;
    }
    pos += prefix.size();
    return readInt(s, pos, ';', out);
}

// Decode an APPEND_ENTRIES payload.  Returns false for anything that
// is not in the format above (e.g. the legacy "heartbeat" payload).
static bool decodeAppendEntries(const std::string& payload,
                                AppendEntriesArgs& args)
{
    std::size_t pos = 0;
    int count = 0;
    if (!readField(payload, pos, "prevLogIndex", args.prevLogIndex) ||
        !readField(payload, pos, "prevLogTerm",  args.prevLogTerm)  ||
        !readField(payload, pos, "leaderCommit", args.leaderCommit) ||
        !readField(payload, pos, "entries",      count)) {
        return false;
    }

    args.entries.clear();
    for (int i = 0; i < count; i++) {
        int term = 0;
        int length = 0;
        if (!readInt(payload, pos, ',', term) ||
            !readInt(payload, pos, ':', length) ||
            static_cast<std::size_t>(length) > payload.size() - pos) {
            return false;
        }
        args.entries.emplace_back(term, payload.substr(pos, length));
        pos += length;
    }
    return pos == payload.size();
}

static std::string encodeAppendResponse(bool success, int matchIndex,
                                        int prevLogIndex, int hintIndex)
{
    std::ostringstream oss;
    oss << "success=" << (success ? 1 : 0)
        << ";matchIndex=" << matchIndex
        << ";prevLogIndex=" << prevLogIndex
        << ";hintIndex=" << hintIndex << ";";
    return oss.str();
}

static bool decodeAppendResponse(const std::string& payload,
                                 AppendResponseArgs& resp)
{
    std::size_t pos = 0;
    int success = 0;
    if (!readField(payload, pos, "success",      success)          ||
        !readField(payload, pos, "matchIndex",   resp.matchIndex)  ||
        !readField(payload, pos, "prevLogIndex", resp.prevLogIndex) ||
        !readField(payload, pos, "hintIndex",    resp.hintIndex)) {
        return false;
    }
    resp.success = (success == 1);
    return pos == payload.size();
}

// ── Constructor ────────────────────────────────────────────────────

RaftEngine::RaftEngine(int nodeId,
                       int clusterSize,
                       Network* network,
                       Storage* storage,
                       int electionTimeoutBase,
                       int electionTimeoutSpread,
                       int heartbeatInterval)
    : currentTerm_(0),
      votedFor_(-1),
      role_(RaftRole::FOLLOWER),
      leaderId_(-1),
      electionTerm_(-1),
      commitIndex_(0),
      lastApplied_(0),
      nodeId_(nodeId),
      clusterSize_(clusterSize),
      electionTimeoutBase_(electionTimeoutBase),
      electionTimeoutSpread_(electionTimeoutSpread),
      heartbeatInterval_(heartbeatInterval),
      electionTimeout_(electionTimeoutBase + nodeId * electionTimeoutSpread),
      electionDeadline_(0),
      heartbeatDeadline_(0),
      lastKnownTime_(0),
      network_(network),
      storage_(storage)
{
}

// ── Lifecycle ──────────────────────────────────────────────────────

void RaftEngine::start()
{
    role_         = RaftRole::FOLLOWER;
    currentTerm_  = 0;
    votedFor_     = -1;
    leaderId_     = -1;
    electionTerm_ = -1;
    lastKnownTime_ = 0;
    votesReceived_.clear();
    commitIndex_  = 0;
    lastApplied_  = 0;
    nextIndex_.clear();
    matchIndex_.clear();
    // Arm the election timer from tick 0
    electionDeadline_ = electionTimeout_;
}

void RaftEngine::recover(int currentTime)
{
    // currentTerm_, votedFor_ and the log are persistent in Raft and
    // survive the crash.  lastApplied_ survives too because the state
    // machine it describes lives outside the engine; every applied
    // entry was committed, so it is a safe value for commitIndex_.
    role_          = RaftRole::FOLLOWER;
    leaderId_      = -1;
    electionTerm_  = -1;
    votesReceived_.clear();
    nextIndex_.clear();
    matchIndex_.clear();
    commitIndex_   = lastApplied_;
    lastKnownTime_ = currentTime;
    heartbeatDeadline_ = 0;
    resetElectionTimer(currentTime);
}

// ── Simulation clock ───────────────────────────────────────────────

void RaftEngine::tick(int currentTime)
{
    lastKnownTime_ = currentTime;
    if (role_ == RaftRole::LEADER) {
        // Leaders send heartbeats when the deadline arrives.
        if (currentTime >= heartbeatDeadline_) {
            sendHeartbeats();
            heartbeatDeadline_ = currentTime + heartbeatInterval_;
        }
    } else {
        // Followers and candidates check the election timer.
        if (currentTime >= electionDeadline_) {
            startElection(currentTime);
        }
    }
}

// ── Election ───────────────────────────────────────────────────────

void RaftEngine::startElection(int currentTime)
{
    // Transition to CANDIDATE
    currentTerm_++;
    role_     = RaftRole::CANDIDATE;
    votedFor_ = nodeId_;
    leaderId_ = -1;

    // Clear old vote set and record self-vote
    votesReceived_.clear();
    votesReceived_.insert(nodeId_);

    // Record the election term so we can reject stale responses.
    electionTerm_ = currentTerm_;

    // Reset election timer (in case this election fails / split vote)
    resetElectionTimer(currentTime);

    // Check immediate majority (single-node cluster)
    if (static_cast<int>(votesReceived_.size()) >= getMajority()) {
        becomeLeader();
        return;
    }

    // Build the REQUEST_VOTE payload with log metadata
    int lastLogIndex = storage_ ? storage_->getLastLogIndex() : 0;
    int lastLogTerm  = storage_ ? storage_->getLastLogTerm()  : 0;

    std::string payload = encodeVoteRequest(lastLogIndex, lastLogTerm);

    // Broadcast REQUEST_VOTE to all peers through the existing Network
    if (network_) {
        network_->broadcast(nodeId_, currentTerm_,
                            MessageType::REQUEST_VOTE, payload);
    }
}

// ── Message processing ─────────────────────────────────────────────

void RaftEngine::handleMessage(const Message& msg, int currentTime)
{
    if (currentTime >= 0) {
        lastKnownTime_ = currentTime;
    }

    int msgTerm = msg.getTerm();

    // Rule: If any message has a higher term, step down immediately.
    if (msgTerm > currentTerm_) {
        becomeFollower(msgTerm);
    }

    switch (msg.getType()) {
        case MessageType::REQUEST_VOTE:
            handleRequestVote(msg);
            break;
        case MessageType::VOTE_RESPONSE:
            handleVoteResponse(msg);
            break;
        case MessageType::APPEND_ENTRIES:
            handleAppendEntries(msg);
            break;
        case MessageType::APPEND_RESPONSE:
            handleAppendResponse(msg);
            break;
        default:
            // CLIENT_COMMAND — clients go through propose() instead.
            break;
    }
}

// ── REQUEST_VOTE handler ───────────────────────────────────────────

void RaftEngine::handleRequestVote(const Message& msg)
{
    int candidateId   = msg.getSenderId();
    int candidateTerm = msg.getTerm();

    // 1. Reject stale-term requests.
    if (candidateTerm < currentTerm_) {
        if (network_) {
            Message reply(nodeId_, candidateId, currentTerm_,
                          MessageType::VOTE_RESPONSE,
                          encodeVoteResponse(false));
            network_->sendMessage(reply);
        }
        return;
    }

    // 2. Grant at most one vote per term.
    if (votedFor_ != -1 && votedFor_ != candidateId) {
        if (network_) {
            Message reply(nodeId_, candidateId, currentTerm_,
                          MessageType::VOTE_RESPONSE,
                          encodeVoteResponse(false));
            network_->sendMessage(reply);
        }
        return;
    }

    // 3. Check candidate log freshness.
    int candLastLogIndex = 0;
    int candLastLogTerm  = 0;
    decodeVoteRequest(msg.getPayload(), candLastLogIndex, candLastLogTerm);

    int myLastLogIndex = storage_ ? storage_->getLastLogIndex() : 0;
    int myLastLogTerm  = storage_ ? storage_->getLastLogTerm()  : 0;

    bool logOk = false;
    if (candLastLogTerm > myLastLogTerm) {
        logOk = true;
    } else if (candLastLogTerm == myLastLogTerm) {
        logOk = (candLastLogIndex >= myLastLogIndex);
    }

    if (!logOk) {
        if (network_) {
            Message reply(nodeId_, candidateId, currentTerm_,
                          MessageType::VOTE_RESPONSE,
                          encodeVoteResponse(false));
            network_->sendMessage(reply);
        }
        return;
    }

    // 4. Grant the vote.
    votedFor_ = candidateId;

    if (network_) {
        Message reply(nodeId_, candidateId, currentTerm_,
                      MessageType::VOTE_RESPONSE,
                      encodeVoteResponse(true));
        network_->sendMessage(reply);
    }

    // Reset election timer when granting a vote (Raft spec).
    resetElectionTimer(lastKnownTime_);
}

// ── VOTE_RESPONSE handler ──────────────────────────────────────────

void RaftEngine::handleVoteResponse(const Message& msg)
{
    // Only candidates count votes.
    if (role_ != RaftRole::CANDIDATE) {
        return;
    }

    // Reject stale-term responses.
    if (msg.getTerm() != currentTerm_) {
        return;
    }

    // Reject responses for an earlier election.
    if (currentTerm_ != electionTerm_) {
        return;
    }

    bool granted = decodeVoteResponse(msg.getPayload());
    if (!granted) {
        return;
    }

    // Record the distinct voter.
    int voterId = msg.getSenderId();
    votesReceived_.insert(voterId);

    // Check majority.
    if (static_cast<int>(votesReceived_.size()) >= getMajority()) {
        becomeLeader();
    }
}

// ── APPEND_ENTRIES handler (heartbeat + replication) ───────────────

void RaftEngine::handleAppendEntries(const Message& msg)
{
    int leaderTerm = msg.getTerm();
    int leaderId   = msg.getSenderId();

    AppendEntriesArgs args;
    bool structured = decodeAppendEntries(msg.getPayload(), args);

    // 1. Reject stale leaders.  A structured request gets a reply that
    //    carries our newer term, so the stale leader steps down.
    if (leaderTerm < currentTerm_) {
        if (structured) {
            sendAppendResponse(leaderId, false, 0, args.prevLogIndex,
                               storage_ ? storage_->getLastLogIndex() : 0);
        }
        return;
    }

    // 2. Valid leader for this term (a higher term was already adopted
    //    in handleMessage).  A candidate steps down; its vote for this
    //    term stays recorded so it cannot vote a second time.
    role_     = RaftRole::FOLLOWER;
    leaderId_ = leaderId;

    // Reset election timer — leader is alive.
    resetElectionTimer(lastKnownTime_);

    // Legacy "heartbeat" payload carries no log metadata: it only
    // signals leader contact.
    if (!structured || storage_ == nullptr) {
        return;
    }

    // 3. Log matching: we must hold prevLogIndex with prevLogTerm.
    int lastIndex = storage_->getLastLogIndex();
    if (args.prevLogIndex > lastIndex) {
        sendAppendResponse(leaderId, false, 0, args.prevLogIndex, lastIndex);
        return;
    }
    if (termAt(args.prevLogIndex) != args.prevLogTerm) {
        sendAppendResponse(leaderId, false, 0, args.prevLogIndex,
                           args.prevLogIndex - 1);
        return;
    }

    // 4. Skip entries we already hold, truncate at the first conflict,
    //    append the rest.  Matching entries are never truncated.
    int index = args.prevLogIndex;
    for (const auto& entry : args.entries) {
        index++;
        int existingTerm = termAt(index);
        if (existingTerm == entry.first) {
            continue;
        }
        if (existingTerm != -1) {
            // Same index, different term.  A committed entry must never
            // be replaced (cannot happen with a correct leader).
            if (index <= commitIndex_) {
                std::cout << "[RAFT] Node " << nodeId_
                          << " refused to overwrite committed entry "
                          << index << "\n";
                sendAppendResponse(leaderId, false, 0, args.prevLogIndex,
                                   commitIndex_);
                return;
            }
            storage_->truncateFrom(index);
        }
        if (!storage_->appendEntry(index, entry.first, entry.second)) {
            sendAppendResponse(leaderId, false, 0, args.prevLogIndex,
                               storage_->getLastLogIndex());
            return;
        }
    }

    // 5. Learn the commit index, bounded by the last entry this request
    //    vouched for (anything after it may be from an older leader).
    int lastNewIndex = args.prevLogIndex + static_cast<int>(args.entries.size());
    if (args.leaderCommit > commitIndex_) {
        commitIndex_ = std::max(commitIndex_,
                                std::min(args.leaderCommit, lastNewIndex));
        applyCommittedEntries();
    }

    sendAppendResponse(leaderId, true, lastNewIndex, args.prevLogIndex,
                       lastNewIndex);
}

// ── APPEND_RESPONSE handler (leader) ───────────────────────────────

void RaftEngine::handleAppendResponse(const Message& msg)
{
    // Only the leader of the response's term uses it.  Responses from
    // an obsolete term are ignored; a newer term already made us step
    // down in handleMessage().
    if (role_ != RaftRole::LEADER || msg.getTerm() != currentTerm_) {
        return;
    }

    AppendResponseArgs resp;
    if (!decodeAppendResponse(msg.getPayload(), resp)) {
        return;
    }

    int peer = msg.getSenderId();
    auto nextIt  = nextIndex_.find(peer);
    auto matchIt = matchIndex_.find(peer);
    if (nextIt == nextIndex_.end() || matchIt == matchIndex_.end()) {
        return;
    }

    if (resp.success) {
        // A peer cannot match entries we do not have.
        if (storage_ == nullptr ||
            resp.matchIndex > storage_->getLastLogIndex()) {
            return;
        }
        // matchIndex only moves forward, so duplicate or reordered
        // acknowledgements are harmless.
        matchIt->second = std::max(matchIt->second, resp.matchIndex);
        nextIt->second  = std::max(nextIt->second, matchIt->second + 1);

        advanceCommitIndex();

        // Send whatever the peer is still missing.
        if (role_ == RaftRole::LEADER &&
            nextIt->second <= storage_->getLastLogIndex()) {
            sendAppendEntries(peer);
        }
        return;
    }

    // Rejection.  Only the answer to the request we would send now is
    // meaningful; rejections of earlier, already-backtracked requests
    // are stale.
    if (resp.prevLogIndex != nextIt->second - 1) {
        return;
    }

    // min(prevLogIndex, hintIndex + 1), written so hintIndex + 1 cannot
    // overflow on a malformed hint.
    int candidate = (resp.hintIndex >= resp.prevLogIndex)
                        ? resp.prevLogIndex
                        : resp.hintIndex + 1;
    int newNext   = std::max(matchIt->second + 1, std::max(1, candidate));

    // Retry only if we actually moved back; otherwise wait for the next
    // heartbeat.  This bounds the retries within one simulation tick.
    if (newNext < nextIt->second) {
        nextIt->second = newNext;
        sendAppendEntries(peer);
    }
}

// ── Role transitions ───────────────────────────────────────────────

void RaftEngine::becomeFollower(int newTerm)
{
    bool wasLeader = (role_ == RaftRole::LEADER);

    currentTerm_ = newTerm;
    role_        = RaftRole::FOLLOWER;
    votedFor_    = -1;
    leaderId_    = -1;
    votesReceived_.clear();
    nextIndex_.clear();
    matchIndex_.clear();

    // A leader's election timer is disabled; a deposed leader must be
    // able to time out and campaign like any other follower.
    if (wasLeader) {
        resetElectionTimer(lastKnownTime_);
    }
}

void RaftEngine::becomeLeader()
{
    role_     = RaftRole::LEADER;
    leaderId_ = nodeId_;

    // Stop election timer (not relevant for leaders).
    electionDeadline_ = INT32_MAX;

    // Fresh replication progress: assume every peer is up to date and
    // let rejections walk nextIndex back where needed.
    nextIndex_.clear();
    matchIndex_.clear();
    int lastIndex = storage_ ? storage_->getLastLogIndex() : 0;
    for (int peer : peerIds()) {
        nextIndex_[peer]  = lastIndex + 1;
        matchIndex_[peer] = 0;
    }

    // Send initial heartbeats immediately.
    sendHeartbeats();
    heartbeatDeadline_ = lastKnownTime_ + heartbeatInterval_;
}

// ── Timer helpers ──────────────────────────────────────────────────

void RaftEngine::resetElectionTimer(int currentTime)
{
    electionDeadline_ = currentTime + electionTimeout_;
}

void RaftEngine::sendHeartbeats()
{
    if (!network_) return;

    // A heartbeat is an AppendEntries carrying whatever each peer is
    // missing (empty when it is up to date).  This also retransmits
    // entries that were dropped while a peer was down.
    for (int peer : peerIds()) {
        sendAppendEntries(peer);
    }
}

// ── Replication helpers ────────────────────────────────────────────

std::vector<int> RaftEngine::peerIds() const
{
    std::vector<int> peers;
    for (int id = 1; id <= clusterSize_; id++) {
        if (id != nodeId_) {
            peers.push_back(id);
        }
    }
    return peers;
}

int RaftEngine::termAt(int index) const
{
    if (index == 0) {
        return 0;
    }
    const LogEntry* entry = storage_ ? storage_->getEntry(index) : nullptr;
    return entry ? entry->term : -1;
}

void RaftEngine::sendAppendEntries(int peerId)
{
    if (!network_ || !storage_ || role_ != RaftRole::LEADER) return;
    if (!network_->isRegistered(peerId)) return;

    auto it = nextIndex_.find(peerId);
    if (it == nextIndex_.end()) return;

    int lastIndex    = storage_->getLastLogIndex();
    int next         = std::max(1, std::min(it->second, lastIndex + 1));
    int prevLogIndex = next - 1;
    int prevLogTerm  = termAt(prevLogIndex);

    // Bounded batch; the rest follows when this one is acknowledged.
    int lastToSend = std::min(lastIndex, prevLogIndex + kMaxEntriesPerMessage);

    std::ostringstream oss;
    oss << "prevLogIndex=" << prevLogIndex
        << ";prevLogTerm=" << prevLogTerm
        << ";leaderCommit=" << commitIndex_
        << ";entries=" << (lastToSend - prevLogIndex) << ";";
    for (int i = next; i <= lastToSend; i++) {
        const LogEntry* entry = storage_->getEntry(i);
        std::string command = (entry && entry->command) ? entry->command : "";
        oss << (entry ? entry->term : 0) << "," << command.size() << ":"
            << command;
    }

    network_->sendMessage(Message(nodeId_, peerId, currentTerm_,
                                  MessageType::APPEND_ENTRIES, oss.str()));
}

void RaftEngine::sendAppendResponse(int leaderId, bool success,
                                    int matchIndex, int prevLogIndex,
                                    int hintIndex)
{
    if (!network_) return;

    Message reply(nodeId_, leaderId, currentTerm_,
                  MessageType::APPEND_RESPONSE,
                  encodeAppendResponse(success, std::max(0, matchIndex),
                                       std::max(0, prevLogIndex),
                                       std::max(0, hintIndex)));
    network_->sendMessage(reply);
}

void RaftEngine::advanceCommitIndex()
{
    if (role_ != RaftRole::LEADER || storage_ == nullptr) return;

    // Highest index stored on a majority: sort the leader's own last
    // index and every peer's matchIndex, take the majority-th largest.
    std::vector<int> matched;
    matched.push_back(storage_->getLastLogIndex());   // leader's own copy
    for (const auto& progress : matchIndex_) {
        matched.push_back(progress.second);
    }
    int majority = getMajority();
    if (static_cast<int>(matched.size()) < majority) return;
    std::sort(matched.begin(), matched.end(), std::greater<int>());
    int n = matched[majority - 1];

    // Current-term rule: only an entry from this term is committed by
    // counting replicas; earlier entries commit along with it.  (Terms
    // never increase going backwards, so if entry n is from an older
    // term, no lower index qualifies either.)
    if (n > commitIndex_ && termAt(n) == currentTerm_) {
        commitIndex_ = n;
    }

    applyCommittedEntries();
}

void RaftEngine::applyCommittedEntries()
{
    if (storage_ == nullptr) return;

    // lastApplied_ is advanced before the callback runs, so even a
    // re-entrant call cannot apply the same entry twice.
    while (lastApplied_ < commitIndex_) {
        const LogEntry* entry = storage_->getEntry(lastApplied_ + 1);
        if (entry == nullptr) {
            break;   // committed entries are always present locally
        }
        lastApplied_++;
        if (applyCallback_) {
            std::string command = entry->command ? entry->command : "";
            applyCallback_(entry->index, entry->term, command);
        }
    }
}

// ── Log replication API ────────────────────────────────────────────

int RaftEngine::propose(const std::string& command)
{
    if (role_ != RaftRole::LEADER || storage_ == nullptr) {
        return -1;
    }
    // The C log stores NUL-terminated strings; refuse what it would cut.
    if (command.find('\0') != std::string::npos) {
        return -1;
    }

    int index = storage_->getLastLogIndex() + 1;
    if (!storage_->appendEntry(index, currentTerm_, command)) {
        return -1;
    }

    // A single-node cluster commits immediately; otherwise this is a
    // no-op until followers acknowledge.
    advanceCommitIndex();

    // Send now only to peers with nothing outstanding.  A peer that is
    // still behind gets this entry batched with the next acknowledgement
    // or heartbeat, instead of a growing copy of the backlog per call.
    for (int peer : peerIds()) {
        if (nextIndex_[peer] == index) {
            sendAppendEntries(peer);
        }
    }
    return index;
}

void RaftEngine::setApplyCallback(ApplyCallback callback)
{
    applyCallback_ = std::move(callback);
}

int RaftEngine::getCommitIndex() const { return commitIndex_; }
int RaftEngine::getLastApplied() const { return lastApplied_; }

int RaftEngine::getNextIndex(int peerId) const
{
    if (role_ != RaftRole::LEADER) return -1;
    auto it = nextIndex_.find(peerId);
    return it != nextIndex_.end() ? it->second : -1;
}

int RaftEngine::getMatchIndex(int peerId) const
{
    if (role_ != RaftRole::LEADER) return -1;
    auto it = matchIndex_.find(peerId);
    return it != matchIndex_.end() ? it->second : -1;
}

// ── State accessors ────────────────────────────────────────────────

RaftRole RaftEngine::getRole() const        { return role_; }
int      RaftEngine::getCurrentTerm() const { return currentTerm_; }
int      RaftEngine::getVotedFor() const    { return votedFor_; }
int      RaftEngine::getLeaderId() const    { return leaderId_; }
int      RaftEngine::getNodeId() const      { return nodeId_; }
int      RaftEngine::getClusterSize() const { return clusterSize_; }
int      RaftEngine::getVoteCount() const   { return static_cast<int>(votesReceived_.size()); }

bool RaftEngine::isElectionTimedOut(int currentTime) const
{
    return currentTime >= electionDeadline_;
}

int RaftEngine::getMajority() const
{
    return (clusterSize_ / 2) + 1;
}
