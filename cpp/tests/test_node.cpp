
#include <iostream>
#include <cassert>
#include "Node.h"

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

void test_node_id()
{
    std::cout << "\n--- Test: Node ID ---\n";

    Node n1(1);
    check(n1.getId() == 1, "Node 1 has ID 1");

    Node n2(42);
    check(n2.getId() == 42, "Node 42 has ID 42");

    Node n3(0);
    check(n3.getId() == 0, "Node 0 has ID 0");
}

void test_initial_state()
{
    std::cout << "\n--- Test: Initial State ---\n";

    Node n(1);
    check(!n.isActive(), "New node starts inactive");
}

void test_start()
{
    std::cout << "\n--- Test: Start ---\n";

    Node n(1);
    n.start();
    check(n.isActive(), "Node is active after start()");
}

void test_stop()
{
    std::cout << "\n--- Test: Stop ---\n";

    Node n(1);
    n.start();
    n.stop();
    check(!n.isActive(), "Node is inactive after stop()");
}

void test_repeated_start()
{
    std::cout << "\n--- Test: Repeated Start ---\n";

    Node n(1);
    n.start();
    n.start();
    n.start();
    check(n.isActive(), "Node stays active after multiple start() calls");
}

void test_repeated_stop()
{
    std::cout << "\n--- Test: Repeated Stop ---\n";

    Node n(1);
    n.stop();
    check(!n.isActive(), "Stopping an already inactive node stays inactive");

    n.start();
    n.stop();
    n.stop();
    n.stop();
    check(!n.isActive(), "Node stays inactive after multiple stop() calls");
}

void test_start_stop_cycle()
{
    std::cout << "\n--- Test: Start/Stop Cycle ---\n";

    Node n(5);
    check(!n.isActive(), "Initially inactive");

    n.start();
    check(n.isActive(), "Active after start");

    n.stop();
    check(!n.isActive(), "Inactive after stop");

    n.start();
    check(n.isActive(), "Active again after restart");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Node Tests — Sidharth\n";
    std::cout << "========================================\n";

    test_node_id();
    test_initial_state();
    test_start();
    test_stop();
    test_repeated_start();
    test_repeated_stop();
    test_start_stop_cycle();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}
