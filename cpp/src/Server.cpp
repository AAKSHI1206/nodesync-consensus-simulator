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
 *
 * In Raft-backed mode (attachToCluster) writes go through OrderService
 * to the real Raft leader, and reads are answered from this node's own
 * OrderBook.
 */

Server::Server(int id, Network *network)
    : node_(id), storage_(), network_(network), currentTerm_(1) {}

void Server::attachToCluster(RaftCluster *cluster, OrderService *service) {
  cluster_ = cluster;
  service_ = service;
}

bool Server::isAttached() const { return cluster_ != nullptr && service_ != nullptr; }

const SubmitResult &Server::getLastResult() const { return lastResult_; }

const ReadResult &Server::getLastRead() const { return lastRead_; }

bool Server::isLeader() const {
  if (!isAttached() || !isRunning()) return false;
  return cluster_->getEngine(getId())->getRole() == RaftRole::LEADER;
}

void Server::start() {
  if (isAttached()) cluster_->startNode(getId());
  else node_.start();
}

void Server::stop() {
  if (isAttached()) cluster_->stopNode(getId());
  else node_.stop();
}

bool Server::isRunning() const {
  if (isAttached()) {
    Node *node = cluster_->getNode(getId());
    return node != nullptr && node->isActive();
  }
  return node_.isActive();
}

int Server::getId() const { return node_.getId(); }

Node *Server::getNode() {
  return isAttached() ? cluster_->getNode(getId()) : &node_;
}

bool Server::handleRequest(const std::string &orderId,
                           const std::string &command) {
  if (isAttached()) {
    requestsReceived_++;

    // Read: answered here, from this node's own copy of the orders.
    if (OrderService::isReadCommand(command)) {
      std::cout << "[SERVER " << getId() << "] Received read " << orderId << " ("
                << command << ")\n";
      lastRead_ = service_->read(getId(), command);
      return lastRead_.outcome == ReadOutcome::OK;
    }

    // Write: OrderService validates, proposes to the leader and waits
    // for commitment.  "true" only when committed.
    std::cout << "[SERVER " << getId() << "] Received request " << orderId << "\n";
    std::string id, product;
    int qty = 0;
    if (!OrderBook::parseCreateOrder(command, id, product, qty)) {
      lastResult_ = SubmitResult();
      lastResult_.outcome = SubmitOutcome::INVALID_REQUEST;
      lastResult_.receivingNode = getId();
      lastResult_.message = "malformed order command";
      std::cout << "[SERVER " << getId() << "] Rejected: " << lastResult_.message << "\n";
      return false;
    }
    lastResult_ = service_->submitOrder(getId(), id, product, qty);
    return lastResult_.outcome == SubmitOutcome::COMMITTED;
  }

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
  if (isAttached()) return cluster_->getStorage(getId())->getLastLogIndex();
  return static_cast<int>(storage_.getLogSize());
}

int Server::getLoad() const {
  // Under Raft every log is the same, so load = requests received.
  return isAttached() ? requestsReceived_ : getLogSize();
}

int Server::getCurrentTerm() const {
  if (isAttached()) return cluster_->getEngine(getId())->getCurrentTerm();
  return currentTerm_;
}
