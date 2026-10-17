#include "Server.h"
#include <iostream>

/*
 * Server.cpp
 * ==========
 * Implementation of the backend Server class.
 *
 * Acts as the entry point for client requests routed by the
 * LoadBalancer.  Simulates Raft leader behavior by appending
 * entries to local Storage and broadcasting APPEND_ENTRIES
 * to peers via the existing Network.
 */

Server::Server(int id, Network *network)
    : node_(id), storage_(), network_(network), currentTerm_(1) {}

void Server::start() { node_.start(); }

void Server::stop() { node_.stop(); }

bool Server::isRunning() const { return node_.isActive(); }

int Server::getId() const { return node_.getId(); }

Node *Server::getNode() { return &node_; }

bool Server::handleRequest(const std::string &orderId,
                           const std::string &command) {
  if (!isRunning()) {
    std::cout << "[SERVER " << getId() << "] Not running — cannot handle "
              << orderId << "\n";
    return false;
  }

  std::cout << "[SERVER " << getId() << "] Received request " << orderId
            << "\n";
  std::cout << "[SERVER " << getId() << "] Forwarding request to RaftNode "
            << getId() << "\n";

  // Simulate Raft leader: append entry to local log
  int nextIndex = storage_.getLastLogIndex() + 1;

  storage_.appendEntry(nextIndex, currentTerm_, command);
  std::cout << "[RAFT] Node " << getId() << " appended entry: " << command
            << " (index " << nextIndex << ", term " << currentTerm_ << ")\n";

  // Simulate replication: broadcast APPEND_ENTRIES to peers
  if (network_) {
    int delivered =
        network_->broadcast(getId(), currentTerm_, MessageType::APPEND_ENTRIES,
                            orderId + "|" + command);
    std::cout << "[RAFT] Node " << getId() << " replicated to " << delivered
              << " peer(s)\n";
  }

  std::cout << "[SERVER " << getId() << "] Request " << orderId
            << " committed to log\n";

  return true;
}

int Server::getLogSize() const {
  return static_cast<int>(storage_.getLogSize());
}

int Server::getLoad() const { return getLogSize(); }

int Server::getCurrentTerm() const { return currentTerm_; }
