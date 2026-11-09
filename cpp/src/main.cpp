/*
 * main.cpp
 * ========
 * NodeSync — Entry point and integration demo.
 *
 * Phase 1: Demonstrates Linked List, Queue, Log Structure, Storage.
 * Phase 2: Demonstrates Node + Message + Network multi-node communication.
 *
 * Updated by: Sanjam (Phase 2 integration)
 *
 * Order simulator (PBL review 2/3): main() now opens an interactive
 * menu over OrderService.  The original demos above are unchanged and
 * run from menu option 8 (or `nodesync --foundations`).
 *   nodesync               interactive menu
 *   nodesync --demo        scripted order demo, then exit
 *   nodesync --foundations original Phase 1-3 demos, then exit
 */

#include <cctype>
#include <iostream>
#include <string>
#include <vector>

#include "Storage.h"
#include "Node.h"
#include "Message.h"
#include "Network.h"
#include "Server.h"
#include "LoadBalancer.h"
#include "RaftCluster.h"
#include "OrderService.h"

extern "C" {
#include "queue.h"
}

// Original integration demos (body unchanged; was main()).
static int runFoundationDemos() {
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

/* ================================================================
 * Interactive order simulator (presentation only: all state lives in
 * RaftCluster and OrderService)
 * ================================================================ */

static const int kClusterSize = 5;   // interactive menu (the scripted --demo keeps its own 3-node cluster)

static std::string toUpper(std::string text) {
  for (char& c : text) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return text;
}

// Read one line; false on end of input.
static bool ask(const std::string& prompt, std::string& answer) {
  std::cout << prompt;
  return static_cast<bool>(std::getline(std::cin, answer));
}

// Whole positive number, or 0 for anything else.
static int toQuantity(const std::string& text) {
  if (text.empty() || text.size() > 7) return 0;
  for (char c : text) {
    if (!std::isdigit(static_cast<unsigned char>(c))) return 0;
  }
  return std::stoi(text);
}

static void printInventoryTable(const OrderService& service) {
  std::cout << "Inventory      ";
  for (int id = 1; id <= kClusterSize; id++) std::cout << "Node " << id << "  ";
  std::cout << "\n";
  for (const auto& item : service.getBook(1).getInventory()) {
    size_t pad = item.first.size() < 13 ? 13 - item.first.size() : 1;
    std::cout << "  " << item.first << std::string(pad, ' ');
    for (int id = 1; id <= kClusterSize; id++) {
      std::cout << service.getBook(id).getStock(item.first) << "       ";
    }
    std::cout << "\n";
  }
  std::cout << (service.replicasAgree() ? "All nodes agree.\n"
                                        : "Nodes differ (a stopped node has not caught up yet).\n");
}

static void printLeader(RaftCluster& cluster) {
  int leader = cluster.findLeader();
  if (leader == -1) {
    std::cout << "Leader: none (no majority of nodes running)\n";
  } else {
    std::cout << "Leader: Node " << leader << " (term "
              << cluster.getEngine(leader)->getCurrentTerm() << ")\n";
  }
}

// Write: client -> LoadBalancer -> leader's Server -> Raft (majority commit)
static void createOrder(LoadBalancer& lb, int& nextOrder) {
  std::string customer, product, qty;
  if (!ask("Customer ID (e.g. C1): ", customer)) return;
  if (!ask("Product (e.g. LAPTOP): ", product)) return;
  if (!ask("Quantity: ", qty)) return;

  std::string id = "O" + std::to_string(nextOrder++);
  std::cout << "\n";
  lb.routeRequest(ClientRequest{id, OrderBook::makeCreateOrder(id, toUpper(product), toQuantity(qty)),
                                toUpper(customer)});
}

// Read: client -> LoadBalancer (hash ring on the customer) -> that Server,
// which answers from its own copy (it says if the copy may be stale).
static void readRequest(LoadBalancer& lb, int& nextRead) {
  std::string customer, what, value;
  if (!ask("Customer ID (e.g. C1): ", customer)) return;
  if (!ask("Read what? 1 = stock of a product, 2 = status of an order: ", what)) return;
  std::string command;
  if (what == "1") {
    if (!ask("Product (e.g. LAPTOP): ", value)) return;
    command = OrderService::makeGetStock(toUpper(value));
  } else if (what == "2") {
    if (!ask("Order ID (e.g. O101): ", value)) return;
    command = OrderService::makeGetOrder(toUpper(value));
  } else {
    std::cout << "Unknown choice.\n";
    return;
  }
  std::cout << "\n";
  lb.routeRequest(ClientRequest{"R" + std::to_string(nextRead++), command, toUpper(customer)});
}

// Where requests go right now.
static void showRouting(LoadBalancer& lb, RaftCluster& cluster) {
  int leader = lb.leaderServer();
  std::cout << "Writes (orders): always -> "
            << (leader == -1 ? "no leader right now (writes are refused until one is elected)"
                             : "leader Server " + std::to_string(leader)) << "\n";
  std::cout << "Reads (stock / order status): each customer ID is hashed onto a ring of servers.\n";
  for (int n = 1; n <= 9; n++) {
    std::string c = "C" + std::to_string(n);
    int s = lb.serverFor(c);
    std::cout << "  " << c << " -> " << (s == -1 ? "no server running" : "Server " + std::to_string(s));
    if (s != -1 && s == cluster.findLeader()) std::cout << " (leader)";
    std::cout << "\n";
  }
}

static void stopServer(RaftCluster& cluster, OrderService& service, std::vector<Server>& servers) {
  std::string answer;
  int leader = cluster.findLeader();
  if (!ask("Server to stop (Enter = current leader): ", answer)) return;
  int id = answer.empty() ? leader : toQuantity(answer);
  if (id < 1 || id > kClusterSize || !servers[id - 1].isRunning()) {
    std::cout << "That server is not running.\n";
    return;
  }
  servers[id - 1].stop();
  std::cout << "Server " << id << " stopped. Running the cluster...\n";
  service.waitForLeader(500);
  service.runTicks(20);
  printLeader(cluster);
}

static void restartServers(RaftCluster& cluster, OrderService& service, std::vector<Server>& servers) {
  int restarted = 0;
  for (Server& s : servers) {
    if (!s.isRunning()) {
      s.start();
      std::cout << "Server " << s.getId() << " restarted.\n";
      restarted++;
    }
  }
  if (restarted == 0) {
    std::cout << "All servers are already running.\n";
    return;
  }
  service.runTicks(50);
  std::cout << "Catch-up done.\n";
  printLeader(cluster);
  printInventoryTable(service);
}

static void printMenu() {
  std::cout << "\n------------- NodeSync -------------\n"
            << " 1. Create order    (write: load balancer -> leader -> Raft)\n"
            << " 2. Check stock or an order (read: load balancer -> hashed server)\n"
            << " 3. View inventory (all nodes)\n"
            << " 4. View orders\n"
            << " 5. View Raft cluster and log status\n"
            << " 6. Show routing (writes and reads)\n"
            << " 7. Stop a server (simulate failure)\n"
            << " 8. Restart stopped servers\n"
            << " 9. Run scripted demo (fresh cluster)\n"
            << "10. Run original Phase 1-3 demos\n"
            << " 0. Exit\n";
}

int main(int argc, char* argv[]) {
  std::string mode = argc > 1 ? argv[1] : "";
  if (mode == "--demo") return runOrderDemo(true) ? 0 : 1;
  if (mode == "--foundations") return runFoundationDemos();

  RaftCluster cluster(kClusterSize, 50, 20, 10);
  OrderService service(cluster);
  service.setVerbose(true);
  service.setStock("LAPTOP", 2);
  service.setStock("MOUSE", 3);

  // One Raft-backed server per node, behind a consistent-hashing load balancer.
  Network unusedNet;   // Server's own network; not used in Raft-backed mode
  std::vector<Server> servers;
  servers.reserve(kClusterSize);   // no reallocation: the load balancer keeps pointers
  for (int id = 1; id <= kClusterSize; id++) servers.emplace_back(id, &unusedNet);
  LoadBalancer lb(RoutingPolicy::CONSISTENT_HASH);
  for (Server& s : servers) {
    s.attachToCluster(&cluster, &service);
    lb.registerServer(&s);
  }
  int nextOrder = 101;
  int nextRead = 1;

  std::cout << "========================================\n"
            << "  NodeSync - Raft Order Simulator\n"
            << "========================================\n"
            << "Cluster size: " << kClusterSize << " servers (majority = "
            << kClusterSize / 2 + 1 << ")\n"
            << "Writes: customer -> load balancer -> leader -> Raft (majority commit)\n"
            << "Reads:  customer -> load balancer (consistent hashing) -> that server's own copy\n";
  cluster.startAll();
  service.waitForLeader(500);
  service.runTicks(20);
  cluster.printState();
  printLeader(cluster);
  printInventoryTable(service);

  std::string choice;
  while (true) {
    printMenu();
    if (!ask("Choice: ", choice)) break;
    std::cout << "\n";
    if (choice == "1") {
      createOrder(lb, nextOrder);
    } else if (choice == "2") {
      readRequest(lb, nextRead);
    } else if (choice == "3") {
      printInventoryTable(service);
    } else if (choice == "4") {
      int leader = cluster.findLeader();
      service.printOrders(leader != -1 ? leader : 1);
    } else if (choice == "5") {
      cluster.printState();
      printLeader(cluster);
    } else if (choice == "6") {
      showRouting(lb, cluster);
    } else if (choice == "7") {
      stopServer(cluster, service, servers);
    } else if (choice == "8") {
      restartServers(cluster, service, servers);
    } else if (choice == "9") {
      runOrderDemo(true);
    } else if (choice == "10") {
      runFoundationDemos();
    } else if (choice == "0") {
      break;
    } else {
      std::cout << "Unknown option.\n";
    }
  }
  std::cout << "Goodbye.\n";
  return 0;
}
