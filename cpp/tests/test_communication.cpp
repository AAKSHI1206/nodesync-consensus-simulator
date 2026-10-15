/*
 * test_communication.cpp
 * ======================
 * Multi-node communication simulation and end-to-end tests.
 * Integrates Node + Message + Network into a working demo.
 *
 * Tests by: Sanjam
 */

#include <iostream>
#include <vector>
#include "Node.h"
#include "Message.h"
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

// ── Simulation: 5-node cluster communication ────────────────────────

void run_simulation()
{
    std::cout << "\n=== Raft Network Simulation ===\n\n";

    // Create 5 nodes
    Node n1(1), n2(2), n3(3), n4(4), n5(5);

    // Start all nodes
    n1.start();
    n2.start();
    n3.start();
    n4.start();
    n5.start();

    // Create the network and register all nodes
    Network net;
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);
    net.registerNode(&n4);
    net.registerNode(&n5);

    std::cout << "Registered " << net.getNodeCount() << " nodes.\n\n";

    // Node 1 sends REQUEST_VOTE to Node 2 and Node 3
    Message m1(1, 2, 1, MessageType::REQUEST_VOTE, "candidate=1");
    Message m2(1, 3, 1, MessageType::REQUEST_VOTE, "candidate=1");

    net.sendMessage(m1);
    std::cout << "Node 1 -> Node 2 : " << messageTypeToString(m1.getType())
              << "\n";

    net.sendMessage(m2);
    std::cout << "Node 1 -> Node 3 : " << messageTypeToString(m2.getType())
              << "\n";

    // Node 2 responds with VOTE_RESPONSE to Node 1
    Message m3(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    net.sendMessage(m3);
    std::cout << "Node 2 -> Node 1 : " << messageTypeToString(m3.getType())
              << "\n";

    // Node 3 responds with VOTE_RESPONSE to Node 1
    Message m4(3, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");
    net.sendMessage(m4);
    std::cout << "Node 3 -> Node 1 : " << messageTypeToString(m4.getType())
              << "\n";

    // Node 1 broadcasts APPEND_ENTRIES to all (as new leader)
    std::cout << "\nNode 1 broadcasts APPEND_ENTRIES (heartbeat):\n";
    int delivered = net.broadcast(1, 1, MessageType::APPEND_ENTRIES,
                                  "entries=[]");
    std::cout << "  Delivered to " << delivered << " nodes.\n";

    std::cout << "\nAll messages delivered successfully.\n";
    std::cout << "\n=== Simulation Complete ===\n";
}

// ── End-to-end Tests ────────────────────────────────────────────────

void test_multi_node_direct()
{
    std::cout << "\n--- Test: Multi-Node Direct Communication ---\n";

    Node n1(1), n2(2), n3(3);
    n1.start();
    n2.start();
    n3.start();

    Network net;
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Send messages between nodes
    check(net.sendMessage(Message(1, 2, 1, MessageType::REQUEST_VOTE, "v1")),
          "Node 1 -> Node 2");
    check(net.sendMessage(Message(1, 3, 1, MessageType::REQUEST_VOTE, "v2")),
          "Node 1 -> Node 3");
    check(net.sendMessage(Message(2, 3, 1, MessageType::VOTE_RESPONSE, "ok")),
          "Node 2 -> Node 3");

    // Verify reception
    Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");

    check(net.receiveMessage(2, recv), "Node 2 receives message");
    check(recv.getSenderId() == 1, "  Sender is Node 1");
    check(recv.getPayload() == "v1", "  Payload is 'v1'");

    check(net.receiveMessage(3, recv), "Node 3 receives first message");
    check(recv.getSenderId() == 1, "  Sender is Node 1");

    check(net.receiveMessage(3, recv), "Node 3 receives second message");
    check(recv.getSenderId() == 2, "  Sender is Node 2");
}

void test_broadcast_and_receive()
{
    std::cout << "\n--- Test: Broadcast and Receive ---\n";

    Node n1(1), n2(2), n3(3), n4(4);
    n1.start();
    n2.start();
    n3.start();
    n4.start();

    Network net;
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);
    net.registerNode(&n4);

    int delivered = net.broadcast(1, 2, MessageType::APPEND_ENTRIES, "heartbeat");
    check(delivered == 3, "Broadcast reached 3 nodes");

    // Each receiver should have exactly 1 message
    Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");

    check(net.receiveMessage(2, recv), "Node 2 receives broadcast");
    check(recv.getTerm() == 2, "  Term is 2");
    check(recv.getType() == MessageType::APPEND_ENTRIES, "  Type is APPEND_ENTRIES");
    check(recv.getPayload() == "heartbeat", "  Payload is 'heartbeat'");

    check(net.receiveMessage(3, recv), "Node 3 receives broadcast");
    check(recv.getSenderId() == 1, "  Sender is Node 1");

    check(net.receiveMessage(4, recv), "Node 4 receives broadcast");
    check(recv.getReceiverId() == 4, "  Receiver is Node 4");
}

void test_message_correctness()
{
    std::cout << "\n--- Test: End-to-End Message Correctness ---\n";

    Node n1(1), n2(2);
    n1.start();
    n2.start();

    Network net;
    net.registerNode(&n1);
    net.registerNode(&n2);

    // Send multiple types
    net.sendMessage(Message(1, 2, 1, MessageType::REQUEST_VOTE, "candidate=1"));
    net.sendMessage(Message(1, 2, 2, MessageType::APPEND_ENTRIES, "data=x"));
    net.sendMessage(Message(1, 2, 3, MessageType::CLIENT_COMMAND, "ADD item"));

    Message recv(0, 0, 0, MessageType::REQUEST_VOTE, "");

    // First message
    net.receiveMessage(2, recv);
    check(recv.getSenderId() == 1, "Msg 1: sender correct");
    check(recv.getReceiverId() == 2, "Msg 1: receiver correct");
    check(recv.getTerm() == 1, "Msg 1: term correct");
    check(recv.getType() == MessageType::REQUEST_VOTE, "Msg 1: type correct");
    check(recv.getPayload() == "candidate=1", "Msg 1: payload correct");

    // Second message
    net.receiveMessage(2, recv);
    check(recv.getTerm() == 2, "Msg 2: term correct");
    check(recv.getType() == MessageType::APPEND_ENTRIES, "Msg 2: type correct");
    check(recv.getPayload() == "data=x", "Msg 2: payload correct");

    // Third message
    net.receiveMessage(2, recv);
    check(recv.getTerm() == 3, "Msg 3: term correct");
    check(recv.getType() == MessageType::CLIENT_COMMAND, "Msg 3: type correct");
    check(recv.getPayload() == "ADD item", "Msg 3: payload correct");
}

void test_inactive_node_handling()
{
    std::cout << "\n--- Test: Inactive Node in Multi-Node Setup ---\n";

    Node n1(1), n2(2), n3(3);
    n1.start();
    n2.start();
    // n3 stays inactive (simulates a crashed node)

    Network net;
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    // Broadcast should skip inactive n3
    int delivered = net.broadcast(1, 1, MessageType::APPEND_ENTRIES, "data");
    check(delivered == 1, "Broadcast only delivered to 1 active node (skipped inactive)");

    check(net.getPendingCount(2) == 1, "Node 2 has 1 message");
    check(net.getPendingCount(3) == 0, "Node 3 (inactive) has 0 messages");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Communication Tests — Sanjam\n";
    std::cout << "========================================\n";

    // Run the visual simulation first
    run_simulation();

    // Run the end-to-end tests
    test_multi_node_direct();
    test_broadcast_and_receive();
    test_message_correctness();
    test_inactive_node_handling();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    std::cout << "\nCommunication test: "
              << (tests_failed == 0 ? "PASSED" : "FAILED") << "\n";

    return tests_failed > 0 ? 1 : 0;
}
