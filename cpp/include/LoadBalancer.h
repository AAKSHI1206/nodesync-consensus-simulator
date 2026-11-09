#ifndef LOADBALANCER_H
#define LOADBALANCER_H

#include "ConsistentHashRing.h"
#include "Server.h"
#include <vector>
#include <string>

/*
 * LoadBalancer.h
 * ==============
 * Load Balancer for routing client requests to backend servers.
 *
 * Supports three routing policies:
 *   1. ROUND_ROBIN: Cycles through available servers sequentially.
 *   2. LEAST_LOADED (Load-Aware): Inspects the current workload/log size
 *      of active servers and directs requests to the least-burdened server.
 *   3. CONSISTENT_HASH (used with Raft-backed servers):
 *      - reads (GET_STOCK / GET_ORDER): the customer ID is hashed onto a
 *        ring of servers, so the same customer always reaches the same
 *        server; if that server is down, only its customers move to the
 *        next server on the ring;
 *      - writes (CREATE_ORDER): always sent to the Raft leader's server,
 *        because only the leader may append to the log.
 *
 * If a server is inactive/stopped, it is automatically skipped.
 */

enum class RoutingPolicy {
    ROUND_ROBIN,
    LEAST_LOADED,    // Load-aware routing
    CONSISTENT_HASH  // Customer -> server via ConsistentHashRing
};

// Simple representation of an incoming client request
struct ClientRequest {
    std::string orderId;
    std::string command;
    std::string customerId = "";   // key for CONSISTENT_HASH (falls back to orderId)
};

class LoadBalancer {
public:
    explicit LoadBalancer(RoutingPolicy policy = RoutingPolicy::ROUND_ROBIN);

    // Register a server with the load balancer
    void registerServer(Server* server);

    // Policy management
    void setRoutingPolicy(RoutingPolicy policy);
    RoutingPolicy getRoutingPolicy() const;

    // Route a single client request using the active policy
    bool routeRequest(const ClientRequest& request);

    // Explicit routing algorithms
    bool routeRequestRoundRobin(const ClientRequest& request);
    bool routeRequestLeastLoaded(const ClientRequest& request);
    bool routeRequestConsistentHash(const ClientRequest& request);

    // Get the number of registered servers
    int getServerCount() const;

    // Server the ring picks for a key right now (skips stopped servers),
    // or -1 if none is running.
    int serverFor(const std::string& key) const;

    // Running server whose Raft node is the leader, or -1 (no leader /
    // servers not Raft-backed).  Writes go here under CONSISTENT_HASH.
    int leaderServer() const;

    // ID of the server that handled the last routed request (-1 if none).
    int getLastServerId() const;

private:
    Server* findServer(int id) const;

    std::vector<Server*> servers_;
    int nextServerIndex_;
    RoutingPolicy policy_;
    ConsistentHashRing ring_;
    int lastServerId_ = -1;
};

#endif
