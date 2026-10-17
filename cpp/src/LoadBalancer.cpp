#include "LoadBalancer.h"
#include <iostream>

/*
 * LoadBalancer.cpp
 * ================
 * Implementation of the LoadBalancer with both Round-Robin and
 * Load-Aware (Least-Loaded) routing strategies.
 */

LoadBalancer::LoadBalancer(RoutingPolicy policy)
    : nextServerIndex_(0), policy_(policy)
{
}

void LoadBalancer::registerServer(Server* server)
{
    if (server) {
        servers_.push_back(server);
    }
}

void LoadBalancer::setRoutingPolicy(RoutingPolicy policy)
{
    policy_ = policy;
}

RoutingPolicy LoadBalancer::getRoutingPolicy() const
{
    return policy_;
}

bool LoadBalancer::routeRequest(const ClientRequest& request)
{
    if (policy_ == RoutingPolicy::LEAST_LOADED) {
        return routeRequestLeastLoaded(request);
    }
    return routeRequestRoundRobin(request);
}

bool LoadBalancer::routeRequestRoundRobin(const ClientRequest& request)
{
    if (servers_.empty()) {
        std::cout << "[LOAD BALANCER] Error: No servers registered.\n";
        return false;
    }

    std::cout << "[CLIENT] Request " << request.orderId << " submitted\n";

    int attempts = 0;
    int numServers = static_cast<int>(servers_.size());

    while (attempts < numServers) {
        Server* server = servers_[nextServerIndex_];
        nextServerIndex_ = (nextServerIndex_ + 1) % numServers;

        if (server && server->isRunning()) {
            std::cout << "[LOAD BALANCER] [Round-Robin] Routing " << request.orderId 
                      << " to Server " << server->getId() << "\n";
            return server->handleRequest(request.orderId, request.command);
        }

        attempts++;
    }

    std::cout << "[LOAD BALANCER] Error: All servers are currently unavailable.\n";
    return false;
}

bool LoadBalancer::routeRequestLeastLoaded(const ClientRequest& request)
{
    if (servers_.empty()) {
        std::cout << "[LOAD BALANCER] Error: No servers registered.\n";
        return false;
    }

    std::cout << "[CLIENT] Request " << request.orderId << " submitted\n";

    Server* bestServer = nullptr;
    int minLoad = -1;

    // Scan all active servers to find the one with the lowest load
    for (Server* server : servers_) {
        if (!server || !server->isRunning()) {
            continue;
        }

        int currentLoad = server->getLoad();
        if (bestServer == nullptr || currentLoad < minLoad) {
            minLoad = currentLoad;
            bestServer = server;
        }
    }

    if (!bestServer) {
        std::cout << "[LOAD BALANCER] Error: All servers are currently unavailable.\n";
        return false;
    }

    std::cout << "[LOAD BALANCER] [Load-Aware] Routing " << request.orderId 
              << " to Server " << bestServer->getId() 
              << " (Current Load: " << minLoad << ")\n";

    return bestServer->handleRequest(request.orderId, request.command);
}

int LoadBalancer::getServerCount() const
{
    return static_cast<int>(servers_.size());
}
