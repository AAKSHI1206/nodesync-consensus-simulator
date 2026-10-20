#ifndef RAFT_ENGINE_H
#define RAFT_ENGINE_H

#include "Network.h"
#include "Storage.h"
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

/*
 * RaftEngine.h
 * ============
 * Core Raft engine: leader election and log replication.
 *
 * Each node in the cluster owns one independent RaftEngine that
 * manages its role, term, voting state, election timeouts, and
 * leader heartbeats.  Uses deterministic simulated time (tick-based)
 * rather than wall-clock delays.
 *
 * Log replication (Feature 4):
 *   - The leader appends client commands to its Storage and sends
 *     APPEND_ENTRIES (prevLogIndex, prevLogTerm, entries,
 *     leaderCommit) to every peer.
 *   - Followers check log matching, resolve conflicts and reply
 *     with APPEND_RESPONSE.
 *   - The leader tracks nextIndex / matchIndex per peer and
 *     advances commitIndex by majority (current-term rule).
 *   - Committed entries are handed to the apply callback exactly
 *     once, in log order (commitIndex vs lastApplied).
 *
 * Membership: node IDs are 1..clusterSize (as created by RaftCluster).
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

    // Restart after a crash.  Keeps the persistent Raft state
    // (currentTerm, votedFor, the log in Storage) and the applied
    // progress; resets volatile state and re-arms the election timer
    // relative to currentTime.
    void recover(int currentTime);

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

    // ── Log replication ────────────────────────────────────────────

    // Leader only: append a client command to the local log and start
    // replicating it.  Returns the new entry's log index, or -1 if
    // this node is not the leader (the command is NOT committed yet).
    int propose(const std::string& command);

    // Called once per committed entry, in index order.  This is the
    // boundary where the order/inventory state machine plugs in.
    using ApplyCallback =
        std::function<void(int index, int term, const std::string& command)>;
    void setApplyCallback(ApplyCallback callback);

    int getCommitIndex() const;
    int getLastApplied() const;

    // Leader's replication progress for a peer (-1 if not leader or
    // unknown peer).
    int getNextIndex(int peerId)  const;
    int getMatchIndex(int peerId) const;

private:
    // ── Internal message handlers ──────────────────────────────────

    void handleRequestVote(const Message& msg);
    void handleVoteResponse(const Message& msg);
    void handleAppendEntries(const Message& msg);
    void handleAppendResponse(const Message& msg);

    // ── Replication helpers ────────────────────────────────────────

    void sendAppendEntries(int peerId);
    void sendAppendResponse(int leaderId, bool success, int matchIndex,
                            int prevLogIndex, int hintIndex);
    void advanceCommitIndex();
    void applyCommittedEntries();
    std::vector<int> peerIds() const;

    // Term of the entry at index (0 for index 0, -1 if missing).
    int termAt(int index) const;

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

    // Replication state
    int commitIndex_;   // highest log index known to be committed
    int lastApplied_;   // highest log index handed to applyCallback_

    // Leader only (reinitialised on election)
    std::map<int, int> nextIndex_;   // peer ID -> next index to send
    std::map<int, int> matchIndex_;  // peer ID -> highest index known
                                     //            to match on that peer

    ApplyCallback applyCallback_;

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