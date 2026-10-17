#ifndef RAFT_CLUSTER_H
#define RAFT_CLUSTER_H

#include "Node.h"
#include "Network.h"
#include "Storage.h"
#include "RaftEngine.h"
#include <vector>
#include <memory>

/*
 * RaftCluster.h
 * =============
 * Multi-node cluster orchestrator for Raft leader election simulation.
 *
 * Manages a set of nodes, each with their own RaftEngine, Storage,
 * and shared Network.  Provides a tick-based simulation loop that
 * advances time, delivers messages, and lets engines react.
 */

class RaftCluster {
public:
    /*
     * Construct a cluster of the given size.
     *
     * @param size  Number of nodes (IDs will be 1..size).
     * @param electionTimeoutBase   Base election timeout in ticks.
     * @param electionTimeoutSpread Per-node timeout spread.
     * @param heartbeatInterval     Heartbeat interval in ticks.
     */
    RaftCluster(int size,
                int electionTimeoutBase = 150,
                int electionTimeoutSpread = 30,
                int heartbeatInterval = 50);

    // ── Simulation ─────────────────────────────────────────────────

    // Start all nodes and their engines.
    void startAll();

    // Stop a specific node (simulates crash).
    void stopNode(int nodeId);

    // Start a specific (previously stopped) node.
    void startNode(int nodeId);

    // Run the simulation for the given number of ticks.
    // Each tick: advance time → deliver all pending messages →
    //            tick each active engine.
    void runFor(int ticks);

    // Run a single tick of the simulation.
    void runTick();

    // ── Query ──────────────────────────────────────────────────────

    // Get the current simulated time.
    int getCurrentTime() const;

    // Find the current leader ID (-1 if none).
    int findLeader() const;

    // Get the RaftEngine for a specific node (1-indexed).
    RaftEngine* getEngine(int nodeId);

    // Get the Node for a specific node (1-indexed).
    Node* getNode(int nodeId);

    // Get the cluster size.
    int getClusterSize() const;

    // Print the state of all nodes (for debugging).
    void printState() const;

private:
    // Deliver all pending messages in the network to the correct engines.
    void deliverMessages();

    int clusterSize_;
    int currentTime_;

    Network network_;

    // Per-node objects (indexed 0..size-1, node IDs are 1..size)
    std::vector<std::unique_ptr<Node>>       nodes_;
    std::vector<std::unique_ptr<Storage>>    storages_;
    std::vector<std::unique_ptr<RaftEngine>> engines_;
};

#endif
