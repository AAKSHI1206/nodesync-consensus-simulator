#include "LoadBalancer.h"
#include <iostream>

/*
 * LoadBalancer.cpp
 * ================
 * Implementation of the LoadBalancer with Round-Robin, Load-Aware
 * (Least-Loaded) and Consistent-Hash routing strategies.
 */

LoadBalancer::LoadBalancer(RoutingPolicy policy)
    : nextServerIndex_(0), policy_(policy)
{
}

void LoadBalancer::registerServer(Server* server)
{
    if (server) {
        servers_.push_back(server);
        ring_.addServer(server->getId());
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
    lastServerId_ = -1;   // set again by whichever route handles it
    if (policy_ == RoutingPolicy::LEAST_LOADED) {
        return routeRequestLeastLoaded(request);
    }
    if (policy_ == RoutingPolicy::CONSISTENT_HASH) {
        return routeRequestConsistentHash(request);
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
            lastServerId_ = server->getId();
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

    lastServerId_ = bestServer->getId();
    return bestServer->handleRequest(request.orderId, request.command);
}

// Reads and writes take different routes:
//   write (CREATE_ORDER ...)       -> the Raft leader's server: only the
//                                     leader may append to the log
//   read  (GET_STOCK / GET_ORDER)  -> the server the hash ring picks for
//                                     the customer, which answers from
//                                     its own copy
bool LoadBalancer::routeRequestConsistentHash(const ClientRequest& request)
{
    if (servers_.empty()) {
        std::cout << "[LOAD BALANCER] Error: No servers registered.\n";
        return false;
    }

    bool isRead = OrderService::isReadCommand(request.command);
    std::cout << "[CLIENT] " << (isRead ? "Read " : "Request ") << request.orderId << " submitted"
              << (request.customerId.empty() ? "" : " by customer " + request.customerId)
              << "\n";

    if (!isRead) {
        Server* leader = findServer(leaderServer());
        if (!leader) {
            std::cout << "[LOAD BALANCER] [Write] " << request.orderId
                      << " refused: no leader right now (election in progress), try again\n";
            return false;
        }
        std::cout << "[LOAD BALANCER] [Write] " << request.orderId
                  << " -> leader Server " << leader->getId() << "\n";
        lastServerId_ = leader->getId();
        return leader->handleRequest(request.orderId, request.command);
    }

    // The ring skips stopped servers, so a crashed server's customers
    // move to the next server clockwise; everyone else keeps theirs.
    const std::string& key = request.customerId.empty() ? request.orderId
                                                        : request.customerId;
    Server* server = findServer(serverFor(key));
    if (!server) {
        std::cout << "[LOAD BALANCER] Error: All servers are currently unavailable.\n";
        return false;
    }

    std::cout << "[LOAD BALANCER] [Read] [Consistent-Hash] key " << key
              << " -> Server " << server->getId() << "\n";

    lastServerId_ = server->getId();
    return server->handleRequest(request.orderId, request.command);
}

int LoadBalancer::leaderServer() const
{
    // Ask the running servers which one is the Raft leader.  If two claim
    // it (an old leader that has not heard of the new term yet), trust
    // the higher term.
    Server* best = nullptr;
    for (Server* server : servers_) {
        if (server && server->isLeader() &&
            (best == nullptr || server->getCurrentTerm() > best->getCurrentTerm())) {
            best = server;
        }
    }
    return best ? best->getId() : -1;
}

int LoadBalancer::serverFor(const std::string& key) const
{
    return ring_.lookup(key, [this](int id) {
        Server* server = findServer(id);
        return server != nullptr && server->isRunning();
    });
}

int LoadBalancer::getLastServerId() const
{
    return lastServerId_;
}

Server* LoadBalancer::findServer(int id) const
{
    for (Server* server : servers_) {
        if (server && server->getId() == id) {
            return server;
        }
    }
    return nullptr;
}

int LoadBalancer::getServerCount() const
{
    return static_cast<int>(servers_.size());
}
