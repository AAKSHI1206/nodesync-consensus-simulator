#ifndef SERVER_H
#define SERVER_H

#include "Node.h"
#include "Storage.h"
#include "Network.h"

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
 */

class Server {
public:
  Server(int id, Network* network);

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
  int getLoad() const;
  int getCurrentTerm() const;

private:
  Node node_;
  Storage storage_;
  Network* network_;   // shared network, not owned
  int currentTerm_;
};

#endif
