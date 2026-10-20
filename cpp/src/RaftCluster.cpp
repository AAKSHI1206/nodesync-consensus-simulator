#include "RaftCluster.h"
#include <iostream>

/*
 * RaftCluster.cpp
 * ===============
 * Implementation of the multi-node Raft cluster orchestrator.
 *
 * Creates N nodes (IDs 1..N), each with:
 *   - A Node  (identity + lifecycle)
 *   - A Storage (replicated log)
 *   - A RaftEngine (election state machine)
 *
 * All nodes share a single Network for message delivery.
 *
 * The simulation loop:
 *   1. Advance currentTime_
 *   2. Deliver all pending messages to the correct engines
 *   3. Tick each active engine
 */

RaftCluster::RaftCluster(int size,
                         int electionTimeoutBase,
                         int electionTimeoutSpread,
                         int heartbeatInterval)
    : clusterSize_(size),
      currentTime_(0)
{
    for (int i = 1; i <= size; i++) {
        nodes_.push_back(std::make_unique<Node>(i));
        storages_.push_back(std::make_unique<Storage>());
        engines_.push_back(std::make_unique<RaftEngine>(
            i, size, &network_, storages_.back().get(),
            electionTimeoutBase, electionTimeoutSpread,
            heartbeatInterval));
    }
}

// ── Lifecycle ──────────────────────────────────────────────────────

void RaftCluster::startAll()
{
    // Same rules as startNode(): fresh start the first time, recovery
    // for a stopped node, nothing for one that is already running.
    for (int id = 1; id <= clusterSize_; id++) {
        startNode(id);
    }
}

void RaftCluster::stopNode(int nodeId)
{
    if (nodeId < 1 || nodeId > clusterSize_) return;
    nodes_[nodeId - 1]->stop();
}

void RaftCluster::startNode(int nodeId)
{
    if (nodeId < 1 || nodeId > clusterSize_) return;
    if (nodes_[nodeId - 1]->isActive()) return;   // already running
    nodes_[nodeId - 1]->start();

    if (!network_.isRegistered(nodeId)) {
        // Never started before: fresh start.
        network_.registerNode(nodes_[nodeId - 1].get());
        engines_[nodeId - 1]->start();
    } else {
        // Restart after a crash: persistent Raft state survives.
        engines_[nodeId - 1]->recover(currentTime_);
    }
}

int RaftCluster::submitCommand(const std::string& command)
{
    int leader = findLeader();
    if (leader == -1) return -1;
    return engines_[leader - 1]->propose(command);
}

// ── Simulation ─────────────────────────────────────────────────────

void RaftCluster::runFor(int ticks)
{
    for (int t = 0; t < ticks; t++) {
        runTick();
    }
}

void RaftCluster::runTick()
{
    currentTime_++;

    // First: deliver all pending messages to engines
    deliverMessages();

    // Second: tick each active engine
    for (int i = 0; i < clusterSize_; i++) {
        if (nodes_[i]->isActive()) {
            engines_[i]->tick(currentTime_);
        }
    }
}

// ── Message delivery ───────────────────────────────────────────────

void RaftCluster::deliverMessages()
{
    // Process messages for each active node.
    // Loop until no more messages are pending for any node in this tick.
    bool anyDelivered = true;
    while (anyDelivered) {
        anyDelivered = false;
        for (int i = 0; i < clusterSize_; i++) {
            if (!nodes_[i]->isActive()) continue;

            int nodeId = nodes_[i]->getId();
            Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");

            while (network_.receiveMessage(nodeId, msg)) {
                engines_[i]->handleMessage(msg, currentTime_);
                anyDelivered = true;
            }
        }
    }
}

// ── Query ──────────────────────────────────────────────────────────

int RaftCluster::getCurrentTime() const
{
    return currentTime_;
}

int RaftCluster::findLeader() const
{
    for (int i = 0; i < clusterSize_; i++) {
        if (nodes_[i]->isActive() &&
            engines_[i]->getRole() == RaftRole::LEADER) {
            return nodes_[i]->getId();
        }
    }
    return -1;
}

RaftEngine* RaftCluster::getEngine(int nodeId)
{
    if (nodeId < 1 || nodeId > clusterSize_) return nullptr;
    return engines_[nodeId - 1].get();
}

Node* RaftCluster::getNode(int nodeId)
{
    if (nodeId < 1 || nodeId > clusterSize_) return nullptr;
    return nodes_[nodeId - 1].get();
}

const Storage* RaftCluster::getStorage(int nodeId) const
{
    if (nodeId < 1 || nodeId > clusterSize_) return nullptr;
    return storages_[nodeId - 1].get();
}

Network* RaftCluster::getNetwork()
{
    return &network_;
}

int RaftCluster::getClusterSize() const
{
    return clusterSize_;
}

void RaftCluster::printState() const
{
    std::cout << "\n--- Cluster State (tick " << currentTime_ << ") ---\n";
    for (int i = 0; i < clusterSize_; i++) {
        int id = nodes_[i]->getId();
        bool active = nodes_[i]->isActive();
        std::cout << "  Node " << id
                  << " | Active: " << (active ? "yes" : "no")
                  << " | Role: " << raftRoleToString(engines_[i]->getRole())
                  << " | Term: " << engines_[i]->getCurrentTerm()
                  << " | VotedFor: " << engines_[i]->getVotedFor()
                  << " | Votes: " << engines_[i]->getVoteCount()
                  << " | Leader: " << engines_[i]->getLeaderId()
                  << " | Log: " << storages_[i]->getLastLogIndex()
                  << " | Commit: " << engines_[i]->getCommitIndex()
                  << " | Applied: " << engines_[i]->getLastApplied()
                  << "\n";
    }
    std::cout << "---\n";
}
