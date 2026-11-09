#ifndef SERVER_H
#define SERVER_H

#include "Node.h"
#include "Storage.h"
#include "Network.h"
#include "OrderService.h"

#include <string>

/*
 * Server.h
 * ========
 * Backend server abstraction for the distributed cluster.
 *
 * Each Server wraps an existing Node (identity + lifecycle) with a
 * Storage (replicated log) and provides request handling that
 * simulates Raft leader behavior.
 *
 * Conceptually:
 *   Server 1 → Node 1 + Storage 1
 *   Server 2 → Node 2 + Storage 2
 *   Server 3 → Node 3 + Storage 3
 *
 * The Server does NOT implement Raft consensus itself.
 * It delegates to the existing Storage for log management
 * and uses the existing Network for replication broadcasts.
 *
 * Raft-backed mode (attachToCluster): the Server becomes the front door
 * of Raft node <id>; start/stop start and stop that Raft node.
 *   - write (CREATE_ORDER): handed to OrderService, which proposes it to
 *     the Raft leader and returns only after commitment (the load balancer
 *     sends writes to the leader's server, so no forwarding is needed);
 *   - read (GET_STOCK / GET_ORDER): answered from this node's own
 *     OrderBook, without a Raft round (see ReadResult::possiblyStale).
 */

class Server {
public:
  Server(int id, Network* network);

  // Raft-backed mode: server <id> fronts Raft node <id> of the cluster.
  void attachToCluster(RaftCluster* cluster, OrderService* service);
  bool isAttached() const;

  // Results of the last write / read handled in Raft-backed mode.
  const SubmitResult& getLastResult() const;
  const ReadResult&   getLastRead() const;

  // Raft-backed: true if this server's Raft node is running and is the
  // leader (the load balancer sends writes here).  Always false otherwise.
  bool isLeader() const;

  // Lifecycle (delegates to internal Node)
  void start();
  void stop();
  bool isRunning() const;

  // Identity
  int getId() const;
  Node* getNode();

  // Handle a client request (simulated Raft leader)
  // Appends to local log and broadcasts replication to peers
  bool handleRequest(const std::string& orderId,
                     const std::string& command);

  // Log inspection
  int getLogSize() const;
  int getLoad() const;   // Raft-backed: number of requests received
  int getCurrentTerm() const;

private:
  Node node_;
  Storage storage_;
  Network* network_;   // shared network, not owned
  int currentTerm_;

  // Raft-backed mode (both null when not attached; not owned)
  RaftCluster*  cluster_ = nullptr;
  OrderService* service_ = nullptr;
  int requestsReceived_ = 0;
  SubmitResult lastResult_;
  ReadResult   lastRead_;
};

#endif
