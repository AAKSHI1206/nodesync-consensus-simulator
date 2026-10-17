/*
 * test_loadbalancer.cpp
 * =====================
 * Tests for LoadBalancer and Server routing.
 */

#include <iostream>
#include <cassert>
#include "LoadBalancer.h"
#include "Server.h"
#include "Network.h"

int tests_passed = 0;
int tests_failed = 0;

void check(bool condition, const std::string& test_name)
{
    if (condition) {
        std::cout << "  PASS: " << test_name << "\n";
        tests_passed++;
    } else {
        std::cout << "  FAIL: " << test_name << "\n";
        tests_failed++;
    }
}

void test_server_wrapper()
{
    std::cout << "\n--- Test: Server Wrapper ---\n";
    Network net;
    Server s1(1, &net);
    
    check(!s1.isRunning(), "Server starts inactive");
    s1.start();
    check(s1.isRunning(), "Server becomes active");
    check(s1.getId() == 1, "Server exposes Node ID");
    check(s1.getNode() != nullptr, "Server returns Node pointer");
}

void test_loadbalancer_registration()
{
    std::cout << "\n--- Test: Load Balancer Registration ---\n";
    Network net;
    Server s1(1, &net), s2(2, &net);
    
    LoadBalancer lb;
    check(lb.getServerCount() == 0, "LB starts empty");
    
    lb.registerServer(&s1);
    lb.registerServer(&s2);
    check(lb.getServerCount() == 2, "LB registers servers");
}

void test_round_robin_routing()
{
    std::cout << "\n--- Test: Round Robin Routing ---\n";
    Network net;
    Server s1(1, &net), s2(2, &net);
    
    // Need to start servers and register nodes to network so broadcast works
    s1.start();
    s2.start();
    net.registerNode(s1.getNode());
    net.registerNode(s2.getNode());
    
    LoadBalancer lb;
    lb.registerServer(&s1);
    lb.registerServer(&s2);
    
    ClientRequest req1{"O101", "ADD Laptop"};
    check(lb.routeRequest(req1), "Route first request");
    check(s1.getLogSize() == 1, "Server 1 processed first request");
    check(s2.getLogSize() == 0, "Server 2 unaffected");
    
    ClientRequest req2{"O102", "ADD Mouse"};
    check(lb.routeRequest(req2), "Route second request");
    check(s1.getLogSize() == 1, "Server 1 unaffected");
    check(s2.getLogSize() == 1, "Server 2 processed second request");
    
    ClientRequest req3{"O103", "ADD Keyboard"};
    check(lb.routeRequest(req3), "Route third request (wrap around)");
    check(s1.getLogSize() == 2, "Server 1 processed third request");
}

void test_skip_inactive_server()
{
    std::cout << "\n--- Test: Skip Inactive Server ---\n";
    Network net;
    Server s1(1, &net), s2(2, &net);
    
    // Only start server 2
    s2.start();
    net.registerNode(s1.getNode());
    net.registerNode(s2.getNode());
    
    LoadBalancer lb;
    lb.registerServer(&s1);
    lb.registerServer(&s2);
    
    ClientRequest req{"O201", "ADD Monitor"};
    check(lb.routeRequest(req), "Route request with inactive server");
    
    check(s1.getLogSize() == 0, "Inactive Server 1 skipped");
    check(s2.getLogSize() == 1, "Active Server 2 processed request");
}

void test_load_aware_routing()
{
    std::cout << "\n--- Test: Load Aware (Least-Loaded) Routing ---\n";
    Network net;
    Server s1(1, &net), s2(2, &net), s3(3, &net);

    s1.start();
    s2.start();
    s3.start();
    net.registerNode(s1.getNode());
    net.registerNode(s2.getNode());
    net.registerNode(s3.getNode());

    // Artificially give s1 load 2 and s2 load 1
    s1.handleRequest("PRE1", "ADD Pre1");
    s1.handleRequest("PRE2", "ADD Pre2");
    s2.handleRequest("PRE3", "ADD Pre3");

    check(s1.getLoad() == 2, "Server 1 initial load is 2");
    check(s2.getLoad() == 1, "Server 2 initial load is 1");
    check(s3.getLoad() == 0, "Server 3 initial load is 0");

    LoadBalancer lb(RoutingPolicy::LEAST_LOADED);
    lb.registerServer(&s1);
    lb.registerServer(&s2);
    lb.registerServer(&s3);

    // First request must go to Server 3 (load 0 is lowest)
    ClientRequest req1{"O301", "ADD Item1"};
    check(lb.routeRequest(req1), "Route request via least-loaded");
    check(s3.getLoad() == 1, "Server 3 received request (load increased to 1)");
    check(s1.getLoad() == 2, "Server 1 unaffected");
    check(s2.getLoad() == 1, "Server 2 unaffected");

    // Second request: s2 and s3 both have load 1, s1 has load 2
    ClientRequest req2{"O302", "ADD Item2"};
    check(lb.routeRequest(req2), "Route second request via least-loaded");
    check(s1.getLoad() == 2, "Server 1 skipped because load 2 > 1");

    // Test with inactive server: stop Server 3
    s3.stop();
    ClientRequest req3{"O303", "ADD Item3"};
    check(lb.routeRequest(req3), "Route when a server is stopped");
    check(!s3.isRunning(), "Server 3 is stopped");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Load Balancer Tests\n";
    std::cout << "========================================\n";

    test_server_wrapper();
    test_loadbalancer_registration();
    test_round_robin_routing();
    test_skip_inactive_server();
    test_load_aware_routing();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}
