/*
 * main.cpp
 * ========
 * NodeSync GÇö Entry point and integration demo.
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

extern "C" {
#include "queue.h"
}

int main() {
  std::cout << "========================================\n";
  std::cout << "  NodeSync GÇö Phase 1 Integration Demo\n";
  std::cout << "========================================\n\n";

  /* GöÇGöÇ 1. Storage (C++ wrapper over C Log + Linked List) GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ */

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

  /* GöÇGöÇ 2. Queue (simulated message channel) GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ */

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

  /* GöÇGöÇ 3. Truncation demo GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ */

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
   * Phase 2 GÇö Multi-Node Communication Demo
   * ================================================================ */

  std::cout << "\n\n========================================\n";
  std::cout << "  NodeSync GÇö Phase 2 Network Demo\n";
  std::cout << "========================================\n\n";

  // Create a 5-node cluster
  Node n1(1), n2(2), n3(3), n4(4), n5(5);

  // Start all nodes
  n1.start();
  n2.start();
  n3.start();
  n4.start();
  n5.start();

  std::cout << "[Nodes] Created and started 5 nodes.\n";

  // Create the network
  Network net;
  net.registerNode(&n1);
  net.registerNode(&n2);
  net.registerNode(&n3);
  net.registerNode(&n4);
  net.registerNode(&n5);

  std::cout << "[Network] Registered " << net.getNodeCount() << " nodes.\n\n";

  // === Election Simulation ===
  std::cout << "--- Election Phase ---\n\n";

  // Node 1 requests votes from all others
  int votes = net.broadcast(1, 1, MessageType::REQUEST_VOTE, "candidate=1");
  std::cout << "Node 1 -> ALL : REQUEST_VOTE (delivered to "
            << votes << " nodes)\n";

  // Nodes 2, 3, 4 grant votes back to Node 1
  net.sendMessage(Message(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true"));
  std::cout << "Node 2 -> Node 1 : VOTE_RESPONSE (granted)\n";

  net.sendMessage(Message(3, 1, 1, MessageType::VOTE_RESPONSE, "granted=true"));
  std::cout << "Node 3 -> Node 1 : VOTE_RESPONSE (granted)\n";

  net.sendMessage(Message(4, 1, 1, MessageType::VOTE_RESPONSE, "granted=true"));
  std::cout << "Node 4 -> Node 1 : VOTE_RESPONSE (granted)\n";

  // Node 5 denies vote
  net.sendMessage(Message(5, 1, 1, MessageType::VOTE_RESPONSE, "granted=false"));
  std::cout << "Node 5 -> Node 1 : VOTE_RESPONSE (denied)\n";

  // Node 1 receives and counts votes
  std::cout << "\n[Node 1] Processing vote responses...\n";
  int grantedVotes = 1;  // self-vote
  Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");

  while (net.receiveMessage(1, recv)) {
    if (recv.getType() == MessageType::VOTE_RESPONSE) {
      std::cout << "  Received " << messageTypeToString(recv.getType())
                << " from Node " << recv.getSenderId()
                << " : " << recv.getPayload() << "\n";
      if (recv.getPayload() == "granted=true") {
        grantedVotes++;
      }
    }
  }
  std::cout << "[Node 1] Total votes: " << grantedVotes << "/5 GÇö "
            << (grantedVotes > 2 ? "ELECTED LEADER" : "ELECTION FAILED") << "\n";

  // === Heartbeat Simulation ===
  std::cout << "\n--- Heartbeat Phase ---\n\n";

  int hb = net.broadcast(1, 1, MessageType::APPEND_ENTRIES, "heartbeat");
  std::cout << "Node 1 -> ALL : APPEND_ENTRIES (heartbeat to "
            << hb << " nodes)\n";

  // Each follower acknowledges
  for (int id = 2; id <= 5; id++) {
    Message hbMsg(0, 0, 0, MessageType::REQUEST_VOTE, "");
    if (net.receiveMessage(id, hbMsg)) {
      std::cout << "  Node " << id << " received heartbeat from Node "
                << hbMsg.getSenderId() << "\n";
    }
  }

  std::cout << "\nAll messages delivered successfully.\n";

  std::cout << "\n========================================\n";
  std::cout << "  Phase 2 complete. Network OK.\n";
  std::cout << "========================================\n";

  /* ================================================================
   * Phase 3 GÇö Load Balancer Integration Demo
   * ================================================================ */

  std::cout << "\n\n========================================\n";
  std::cout << "  NodeSync GÇö Phase 3 Load Balancer Demo\n";
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

  // Setup Load Balancer
  LoadBalancer lb;
  lb.registerServer(&s1);
  lb.registerServer(&s2);
  lb.registerServer(&s3);

  std::cout << "[Load Balancer] Registered " << lb.getServerCount() << " servers.\n\n";
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
