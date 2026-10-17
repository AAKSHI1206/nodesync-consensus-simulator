#ifndef LOADBALANCER_H
#define LOADBALANCER_H

#include "Server.h"
#include <vector>
#include <string>

/*
 * LoadBalancer.h
 * ==============
 * Load Balancer for routing client requests to backend servers.
 *
 * Supports two routing policies:
 *   1. ROUND_ROBIN: Cycles through available servers sequentially.
 *   2. LEAST_LOADED (Load-Aware): Inspects the current workload/log size
 *      of active servers and directs requests to the least-burdened server.
 *
 * If a server is inactive/stopped, it is automatically skipped.
 */

enum class RoutingPolicy {
    ROUND_ROBIN,
    LEAST_LOADED    // Load-aware routing
};

// Simple representation of an incoming client request
struct ClientRequest {
    std::string orderId;
    std::string command;
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

    // Get the number of registered servers
    int getServerCount() const;

private:
    std::vector<Server*> servers_;
    int nextServerIndex_;
    RoutingPolicy policy_;
};

#endif
