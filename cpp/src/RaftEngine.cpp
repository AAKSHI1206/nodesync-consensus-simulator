#include "RaftEngine.h"
#include <iostream>
#include <sstream>
#include <cstdint>
#include <climits>

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
    // Arm the election timer from tick 0
    electionDeadline_ = electionTimeout_;
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
        default:
            // CLIENT_COMMAND, APPEND_RESPONSE — not handled in this feature.
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

// ── APPEND_ENTRIES handler (heartbeat) ─────────────────────────────

void RaftEngine::handleAppendEntries(const Message& msg)
{
    int leaderTerm = msg.getTerm();

    // Reject stale heartbeats.
    if (leaderTerm < currentTerm_) {
        return;
    }

    // Valid leader message — accept it.
    // If we're a candidate in this term, step down.
    if (role_ == RaftRole::CANDIDATE && leaderTerm >= currentTerm_) {
        role_ = RaftRole::FOLLOWER;
        votedFor_ = -1;   // clear vote for next term use
    }

    role_     = RaftRole::FOLLOWER;
    leaderId_ = msg.getSenderId();

    // Reset election timer — leader is alive.
    resetElectionTimer(lastKnownTime_);
}

// ── Role transitions ───────────────────────────────────────────────

void RaftEngine::becomeFollower(int newTerm)
{
    currentTerm_ = newTerm;
    role_        = RaftRole::FOLLOWER;
    votedFor_    = -1;
    leaderId_    = -1;
    votesReceived_.clear();
}

void RaftEngine::becomeLeader()
{
    role_     = RaftRole::LEADER;
    leaderId_ = nodeId_;

    // Stop election timer (not relevant for leaders).
    electionDeadline_ = INT32_MAX;

    // Send initial heartbeats immediately.
    sendHeartbeats();
    heartbeatDeadline_ = heartbeatInterval_;  // relative to start
}

// ── Timer helpers ──────────────────────────────────────────────────

void RaftEngine::resetElectionTimer(int currentTime)
{
    electionDeadline_ = currentTime + electionTimeout_;
}

void RaftEngine::sendHeartbeats()
{
    if (!network_) return;

    // Use APPEND_ENTRIES with "heartbeat" payload (empty heartbeat).
    network_->broadcast(nodeId_, currentTerm_,
                        MessageType::APPEND_ENTRIES, "heartbeat");
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
