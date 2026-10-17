/*
 * main.cpp
 * ========
 * NodeSync — Entry point and integration demo.
 *
 * Phase 1: Demonstrates Linked List, Queue, Log Structure, Storage.
 * Phase 2: Demonstrates Node + Message + Network multi-node communication.
 *
 * Updated by: Sanjam (Phase 2 integration)
 */

#include <iostream>
#include <string>

#include "Storage.h"
#include "Node.h"
#include "Message.h"
#include "Network.h"
#include "Server.h"
#include "LoadBalancer.h"
#include "RaftCluster.h"

extern "C" {
#include "queue.h"
}

int main() {
  std::cout << "========================================\n";
  std::cout << "  NodeSync — Phase 1 Integration Demo\n";
  std::cout << "========================================\n\n";

  /* ── 1. Storage (C++ wrapper over C Log + Linked List) ────────── */

  std::cout << "[Storage] Creating storage...\n";
  Storage storage;

  storage.appendEntry(1, 1, "ADD_PRODUCT Laptop 50");
  storage.appendEntry(2, 1, "UPDATE_STOCK Laptop 40");
  storage.appendEntry(3, 2, "ADD_PRODUCT Mouse 100");

  std::cout << "[Storage] Appended 3 entries.\n";
  std::cout << "[Storage] Log size : " << storage.getLogSize() << "\n";
  std::cout << "[Storage] Last idx : " << storage.getLastLogIndex() << "\n";
  std::cout << "[Storage] Last term: " << storage.getLastLogTerm() << "\n";

  std::cout << "\n[Storage] Entries:\n";
  for (std::size_t i = 1; i <= storage.getLogSize(); i++) {
    const LogEntry *e = storage.getEntry(static_cast<int>(i));
    if (e) {
      std::cout << "  Index " << e->index << "  Term " << e->term
                << "  Cmd: " << e->command << "\n";
    }
  }

  /* ── 2. Queue (simulated message channel) ─────────────────────── */

  std::cout << "\n[Queue] Creating message queue...\n";
  Queue *msgQueue = queue_create();

  /* Simulate enqueueing command strings as messages */
  const char *msg1 = "ADD_PRODUCT Keyboard 200";
  const char *msg2 = "UPDATE_STOCK Laptop 35";
  const char *msg3 = "ADD_PRODUCT Monitor 30";

  queue_enqueue(msgQueue, (void *)msg1);
  queue_enqueue(msgQueue, (void *)msg2);
  queue_enqueue(msgQueue, (void *)msg3);

  std::cout << "[Queue] Enqueued 3 messages. Size: " << queue_size(msgQueue)
            << "\n";

  /* Process messages in FIFO order and apply them to Storage */
  std::cout << "\n[Integration] Processing messages into storage:\n";
  int next_index = storage.getLastLogIndex() + 1;
  int current_term = storage.getLastLogTerm();

  while (!queue_is_empty(msgQueue)) {
    const char *cmd = (const char *)queue_dequeue(msgQueue);
    storage.appendEntry(next_index, current_term, cmd);
    std::cout << "  Applied index " << next_index << ": " << cmd << "\n";
    next_index++;
  }

  std::cout << "\n[Storage] Final log size : " << storage.getLogSize() << "\n";
  std::cout << "[Storage] Final last idx : " << storage.getLastLogIndex()
            << "\n";
  std::cout << "[Storage] Final last term: " << storage.getLastLogTerm()
            << "\n";

  std::cout << "\n[Storage] Full log:\n";
  for (std::size_t i = 1; i <= storage.getLogSize(); i++) {
    const LogEntry *e = storage.getEntry(static_cast<int>(i));
    if (e) {
      std::cout << "  [" << e->index << "] Term " << e->term << " | "
                << e->command << "\n";
    }
  }

  /* ── 3. Truncation demo ───────────────────────────────────────── */

  std::cout << "\n[Storage] Truncating from index 4...\n";
  int removed = storage.truncateFrom(4);
  std::cout << "[Storage] Removed " << removed << " entries.\n";
  std::cout << "[Storage] Log size after truncation: " << storage.getLogSize()
            << "\n";
  std::cout << "[Storage] Last idx : " << storage.getLastLogIndex() << "\n";

  /* Cleanup */
  queue_destroy(msgQueue);

  std::cout << "\n========================================\n";
  std::cout << "  Phase 1 complete. All components OK.\n";
  std::cout << "========================================\n";

  /* ================================================================
   * Phase 2 — Raft Leader Election Demo (real consensus)
   * ================================================================ */

  std::cout << "\n\n========================================\n";
  std::cout << "  NodeSync — Phase 2 Raft Election Demo\n";
  std::cout << "========================================\n\n";

  {
    // Create a 5-node cluster with deterministic election timeouts
    RaftCluster cluster(5, 50, 20, 10);
    cluster.startAll();

    std::cout << "[Cluster] Created and started 5 nodes.\n";
    std::cout << "[Cluster] Running Raft election simulation...\n\n";

    // Run the simulation for enough ticks to elect a leader
    cluster.runFor(500);

    // Display results
    cluster.printState();

    int leader = cluster.findLeader();
    if (leader != -1) {
      std::cout << "\n[RESULT] Leader elected: Node " << leader
                << " (term " << cluster.getEngine(leader)->getCurrentTerm()
                << ", votes: " << cluster.getEngine(leader)->getVoteCount()
                << ")\n";
    } else {
      std::cout << "\n[RESULT] No leader elected yet.\n";
    }

    // === Leader Failure and Re-Election ===
    std::cout << "\n--- Leader Failure Simulation ---\n\n";

    if (leader != -1) {
      std::cout << "[Cluster] Stopping leader Node " << leader << "...\n";
      cluster.stopNode(leader);

      // Run more ticks to allow re-election
      cluster.runFor(500);

      cluster.printState();

      int newLeader = cluster.findLeader();
      if (newLeader != -1) {
        std::cout << "\n[RESULT] New leader elected: Node " << newLeader
                  << " (term "
                  << cluster.getEngine(newLeader)->getCurrentTerm()
                  << ")\n";
      } else {
        std::cout << "\n[RESULT] No new leader elected.\n";
      }
    }
  }

  std::cout << "\n========================================\n";
  std::cout << "  Phase 2 complete. Raft Election OK.\n";
  std::cout << "========================================\n";

  /* ================================================================
   * Phase 3 — Load Balancer Integration Demo
   * ================================================================ */

  std::cout << "\n\n========================================\n";
  std::cout << "  NodeSync — Phase 3 Load Balancer Demo\n";
  std::cout << "========================================\n\n";

  // Create shared network
  Network sharedNet;

  // Create Servers (which wrap Nodes internally)
  Server s1(1, &sharedNet);
  Server s2(2, &sharedNet);
  Server s3(3, &sharedNet);

  // Start servers and register their underlying nodes with the network
  s1.start();
  s2.start();
  s3.start();

  sharedNet.registerNode(s1.getNode());
  sharedNet.registerNode(s2.getNode());
  sharedNet.registerNode(s3.getNode());

  std::cout << "[Servers] Created and registered 3 servers.\n";

  // Setup Load Balancer with Load-Aware (Least-Loaded) routing policy
  LoadBalancer lb(RoutingPolicy::LEAST_LOADED);
  lb.registerServer(&s1);
  lb.registerServer(&s2);
  lb.registerServer(&s3);

  std::cout << "[Load Balancer] Registered " << lb.getServerCount()
            << " servers (Policy: Load-Aware / Least-Loaded).\n\n";
  std::cout << "--- Processing Client Requests ---\n\n";

  // Using the existing C Queue for client requests
  Queue *clientQueue = queue_create();

  // We have to allocate dynamically to pass into the void* queue
  ClientRequest* req1 = new ClientRequest{"O101", "ADD Laptop 1"};
  ClientRequest* req2 = new ClientRequest{"O102", "ADD Mouse 5"};
  ClientRequest* req3 = new ClientRequest{"O103", "ADD Keyboard 2"};
  ClientRequest* req4 = new ClientRequest{"O104", "ADD Monitor 1"};

  queue_enqueue(clientQueue, req1);
  queue_enqueue(clientQueue, req2);
  queue_enqueue(clientQueue, req3);
  queue_enqueue(clientQueue, req4);

  std::cout << "[Client] Enqueued 4 requests to the queue.\n\n";

  // Process the queue
  while (!queue_is_empty(clientQueue)) {
      ClientRequest* req = static_cast<ClientRequest*>(queue_dequeue(clientQueue));
      
      lb.routeRequest(*req);
      std::cout << "\n";
      
      delete req;
  }

  std::cout << "--- Server States After Requests ---\n\n";
  std::cout << "Server 1 Log Size: " << s1.getLogSize() << "\n";
  std::cout << "Server 2 Log Size: " << s2.getLogSize() << "\n";
  std::cout << "Server 3 Log Size: " << s3.getLogSize() << "\n";

  // Simulate a server failure
  std::cout << "\n--- Simulating Server Failure ---\n\n";
  std::cout << "Stopping Server 2...\n";
  s2.stop();

  ClientRequest req5{"O105", "ADD Desk 1"};
  ClientRequest req6{"O106", "ADD Chair 2"};

  lb.routeRequest(req5);
  std::cout << "\n";
  lb.routeRequest(req6);
  std::cout << "\n";

  std::cout << "Server 1 Log Size: " << s1.getLogSize() << "\n";
  std::cout << "Server 2 Log Size: " << s2.getLogSize() << " (Stopped)\n";
  std::cout << "Server 3 Log Size: " << s3.getLogSize() << "\n";

  queue_destroy(clientQueue);

  std::cout << "\n========================================\n";
  std::cout << "  Phase 3 complete. Load Balancer OK.\n";
  std::cout << "========================================\n";

  return 0;
}
