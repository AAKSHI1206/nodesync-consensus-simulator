#ifndef RAFT_ENGINE_H
#define RAFT_ENGINE_H

#include "Network.h"
#include "Storage.h"
#include <set>
#include <string>
#include <vector>

/*
 * RaftEngine.h
 * ============
 * Core Raft leader election engine.
 *
 * Each node in the cluster owns one independent RaftEngine that
 * manages its role, term, voting state, election timeouts, and
 * leader heartbeats.  Uses deterministic simulated time (tick-based)
 * rather than wall-clock delays.
 *
 * Reuses existing: Node, Message, Network, Storage.
 */

// Raft roles
enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

std::string raftRoleToString(RaftRole role);

class RaftEngine {
public:
    /*
     * Construct a RaftEngine for the given node.
     *
     * @param nodeId        This node's unique identifier.
     * @param clusterSize   Total number of nodes in the fixed cluster.
     * @param network       Pointer to the shared Network (not owned).
     * @param storage       Pointer to this node's Storage (not owned).
     * @param electionTimeoutBase  Base election timeout in ticks.
     * @param electionTimeoutSpread  Additional per-node spread added
     *                               deterministically (nodeId * spread).
     * @param heartbeatInterval  Ticks between leader heartbeats.
     */
    RaftEngine(int nodeId,
               int clusterSize,
               Network* network,
               Storage* storage,
               int electionTimeoutBase = 150,
               int electionTimeoutSpread = 30,
               int heartbeatInterval = 50);

    // ── Lifecycle ──────────────────────────────────────────────────

    // Start the engine (begin as FOLLOWER, arm election timer).
    void start();

    // ── Simulation clock ───────────────────────────────────────────

    // Advance the engine by one tick of simulated time.
    // Checks election/heartbeat deadlines and acts accordingly.
    void tick(int currentTime);

    // ── Message processing ─────────────────────────────────────────

    // Process an incoming message (REQUEST_VOTE, VOTE_RESPONSE,
    // APPEND_ENTRIES, APPEND_RESPONSE).
    void handleMessage(const Message& msg, int currentTime = -1);

    // ── State accessors ────────────────────────────────────────────

    RaftRole    getRole()        const;
    int         getCurrentTerm() const;
    int         getVotedFor()    const;
    int         getLeaderId()    const;
    int         getNodeId()      const;
    int         getClusterSize() const;
    int         getVoteCount()   const;

    // Check whether this engine's election timer has expired given
    // the current simulated time.
    bool isElectionTimedOut(int currentTime) const;

    // The majority threshold for the configured cluster.
    int getMajority() const;

    // ── Election (exposed for testing) ─────────────────────────────

    // Manually initiate an election (normally triggered by timeout).
    void startElection(int currentTime);

private:
    // ── Internal message handlers ──────────────────────────────────

    void handleRequestVote(const Message& msg);
    void handleVoteResponse(const Message& msg);
    void handleAppendEntries(const Message& msg);

    // ── Role transitions ───────────────────────────────────────────

    void becomeFollower(int newTerm);
    void becomeLeader();

    // ── Timer helpers ──────────────────────────────────────────────

    void resetElectionTimer(int currentTime);
    void sendHeartbeats();

    // ── Raft persistent state ──────────────────────────────────────

    int  currentTerm_;
    int  votedFor_;       // -1 means "not voted this term"

    // ── Raft volatile state ────────────────────────────────────────

    RaftRole role_;
    int      leaderId_;   // -1 means "unknown"

    // Election bookkeeping
    std::set<int> votesReceived_;   // set of node IDs that granted votes
    int           electionTerm_;    // term in which the current election
                                    // was started (to reject stale responses)

    // ── Configuration (fixed for this cluster) ─────────────────────

    int nodeId_;
    int clusterSize_;

    // Timer configuration
    int electionTimeoutBase_;
    int electionTimeoutSpread_;
    int heartbeatInterval_;

    // Computed per-node election timeout
    int electionTimeout_;

    // Timer state (simulated ticks)
    int electionDeadline_;   // tick at which election times out
    int heartbeatDeadline_;  // tick at which next heartbeat is due
    int lastKnownTime_;      // latest observed tick timestamp

    // ── External references (not owned) ────────────────────────────

    Network* network_;
    Storage* storage_;
};

#endif
