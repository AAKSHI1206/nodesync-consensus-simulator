/*
 * test_consistent_hash.cpp
 * ========================
 * Tests for ConsistentHashRing: stable mapping, even spread, and the
 * consistent-hashing property (a server going down or being added only
 * moves the keys that have to move).
 */

#include <map>
#include <set>
#include <string>
#include <vector>
#include "ConsistentHashRing.h"
#include "replication_test_utils.h"

std::vector<std::string> customers(int n)
{
    std::vector<std::string> keys;
    for (int i = 1; i <= n; i++) keys.push_back("C" + std::to_string(i));
    return keys;
}

ConsistentHashRing threeServers()
{
    ConsistentHashRing ring(50);
    ring.addServer(1);
    ring.addServer(2);
    ring.addServer(3);
    return ring;
}

// ── Basics ─────────────────────────────────────────────────────────

void test_empty_and_basic()
{
    std::cout << "\n--- Test: Empty Ring and Basic Lookup ---\n";

    ConsistentHashRing ring;
    check(ring.lookup("C1") == -1, "Empty ring returns -1");

    ring.addServer(7);
    check(ring.lookup("C1") == 7 && ring.lookup("anything") == 7,
          "Single server receives every key");
    check(ring.pointsFor(7) == 50, "Server placed 50 times (virtual nodes)");

    ring.addServer(7);
    check(ring.getServerCount() == 1 && ring.pointsFor(7) == 50,
          "Adding the same server twice changes nothing");

    ring.removeServer(7);
    check(ring.lookup("C1") == -1 && ring.getServerCount() == 0,
          "Removing the last server empties the ring");
}

void test_deterministic()
{
    std::cout << "\n--- Test: Same Key, Same Server ---\n";

    ConsistentHashRing a = threeServers();
    ConsistentHashRing b = threeServers();
    bool same = true;
    for (const std::string& key : customers(200)) {
        if (a.lookup(key) != a.lookup(key) || a.lookup(key) != b.lookup(key)) same = false;
    }
    check(same, "Every customer maps to the same server, every time, in every ring");
    check(ConsistentHashRing::hash("C1") == ConsistentHashRing::hash("C1") &&
          ConsistentHashRing::hash("C1") != ConsistentHashRing::hash("C2"),
          "Hash is deterministic and distinguishes keys");
}

void test_even_spread()
{
    std::cout << "\n--- Test: Load Spread Over Three Servers ---\n";

    ConsistentHashRing ring = threeServers();
    std::map<int, int> count;
    for (const std::string& key : customers(3000)) count[ring.lookup(key)]++;

    bool balanced = true;
    for (int id = 1; id <= 3; id++) {
        std::cout << "    Server " << id << ": " << count[id] << " customers\n";
        if (count[id] < 600 || count[id] > 1400) balanced = false;   // 20%..47%
    }
    check(count.size() == 3, "All three servers receive customers");
    check(balanced, "Each server gets between 20% and 47% of 3000 customers");
}

// ── The consistent-hashing property ────────────────────────────────

void test_server_down_moves_only_its_keys()
{
    std::cout << "\n--- Test: A Server Goes Down ---\n";

    ConsistentHashRing ring = threeServers();
    auto all = customers(1000);
    std::map<std::string, int> before;
    for (const auto& key : all) before[key] = ring.lookup(key);

    auto up = [](int id) { return id != 2; };   // server 2 crashed
    int moved = 0, wrongly = 0;
    for (const auto& key : all) {
        int now = ring.lookup(key, up);
        if (before[key] == 2) {
            if (now == 2 || now == -1) wrongly++;
            moved++;
        } else if (now != before[key]) {
            wrongly++;
        }
    }
    check(moved > 0, "Server 2 had customers to move");
    check(wrongly == 0, "Only server 2's customers moved, each to a running server");

    bool back = true;
    for (const auto& key : all) if (ring.lookup(key) != before[key]) back = false;
    check(back, "When server 2 is back, every customer returns to its server");

    check(ring.lookup("C1", [](int) { return false; }) == -1, "All servers down: -1");
}

void test_adding_server_moves_a_fraction()
{
    std::cout << "\n--- Test: Adding a Fourth Server ---\n";

    ConsistentHashRing ring = threeServers();
    auto all = customers(2000);
    std::map<std::string, int> before;
    for (const auto& key : all) before[key] = ring.lookup(key);

    ring.addServer(4);
    int moved = 0, movedElsewhere = 0;
    for (const auto& key : all) {
        int now = ring.lookup(key);
        if (now != before[key]) {
            moved++;
            if (now != 4) movedElsewhere++;
        }
    }
    double share = 100.0 * moved / all.size();
    std::cout << "    " << moved << " of 2000 customers moved (" << share << "%)\n";
    check(moved > 0 && share < 40.0, "Roughly a quarter of customers move (less than 40%)");
    check(movedElsewhere == 0, "Every customer that moved went to the new server");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Consistent Hash Ring Tests\n";
    std::cout << "========================================\n";

    test_empty_and_basic();
    test_deterministic();
    test_even_spread();
    test_server_down_moves_only_its_keys();
    test_adding_server_moves_a_fraction();

    return finish("Consistent Hash Ring Tests");
}
