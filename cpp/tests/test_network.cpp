/*
 * test_network.cpp
 * ================
 * Tests for Network routing and message delivery.
 *
 * Tests by: Astik
 */

#include <iostream>
#include <cassert>
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

void test_registration()
{
    std::cout << "\n--- Test: Node Registration ---\n";

    Network net;
    Node n1(1), n2(2), n3(3);

    check(net.registerNode(&n1), "Register Node 1");
    check(net.registerNode(&n2), "Register Node 2");
    check(net.registerNode(&n3), "Register Node 3");

    check(net.getNodeCount() == 3, "3 nodes registered");

    check(net.isRegistered(1), "Node 1 is registered");
    check(net.isRegistered(2), "Node 2 is registered");
    check(net.isRegistered(3), "Node 3 is registered");
    check(!net.isRegistered(99), "Node 99 is not registered");
}

void test_duplicate_registration()
{
    std::cout << "\n--- Test: Duplicate Registration ---\n";

    Network net;
    Node n1(1);

    check(net.registerNode(&n1), "First registration succeeds");
    check(!net.registerNode(&n1), "Duplicate registration fails");
    check(net.getNodeCount() == 1, "Still only 1 node");
}

void test_null_registration()
{
    std::cout << "\n--- Test: Null Registration ---\n";

    Network net;
    check(!net.registerNode(nullptr), "Cannot register null");
}

void test_direct_message()
{
    std::cout << "\n--- Test: Direct Message ---\n";

    Network net;
    Node n1(1), n2(2);
    n1.start();
    n2.start();
    net.registerNode(&n1);
    net.registerNode(&n2);

    Message msg(1, 2, 1, MessageType::REQUEST_VOTE, "candidate=1");
    check(net.sendMessage(msg), "Message sent from 1 to 2");

    check(net.getPendingCount(2) == 1, "Node 2 has 1 pending message");

    Message received(0, 0, 0, MessageType::REQUEST_VOTE, "");
    check(net.receiveMessage(2, received), "Node 2 receives message");

    check(received.getSenderId() == 1, "Sender is Node 1");
    check(received.getReceiverId() == 2, "Receiver is Node 2");
    check(received.getTerm() == 1, "Term is 1");
    check(received.getType() == MessageType::REQUEST_VOTE, "Type is REQUEST_VOTE");
    check(received.getPayload() == "candidate=1", "Payload matches");
}

void test_multiple_messages()
{
    std::cout << "\n--- Test: Multiple Messages ---\n";

    Network net;
    Node n1(1), n2(2), n3(3);
    n1.start();
    n2.start();
    n3.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);

    Message m1(1, 2, 1, MessageType::REQUEST_VOTE, "vote1");
    Message m2(1, 3, 1, MessageType::REQUEST_VOTE, "vote2");
    Message m3(2, 3, 1, MessageType::VOTE_RESPONSE, "granted");

    check(net.sendMessage(m1), "Node 1 → Node 2");
    check(net.sendMessage(m2), "Node 1 → Node 3");
    check(net.sendMessage(m3), "Node 2 → Node 3");

    check(net.getPendingCount(2) == 1, "Node 2 has 1 message");
    check(net.getPendingCount(3) == 2, "Node 3 has 2 messages");
}

void test_broadcast()
{
    std::cout << "\n--- Test: Broadcast ---\n";

    Network net;
    Node n1(1), n2(2), n3(3), n4(4);
    n1.start();
    n2.start();
    n3.start();
    n4.start();
    net.registerNode(&n1);
    net.registerNode(&n2);
    net.registerNode(&n3);
    net.registerNode(&n4);

    int delivered = net.broadcast(1, 1, MessageType::REQUEST_VOTE, "candidate=1");

    check(delivered == 3, "Broadcast delivered to 3 nodes");
    check(net.getPendingCount(1) == 0, "Sender has no messages");
    check(net.getPendingCount(2) == 1, "Node 2 has 1 message");
    check(net.getPendingCount(3) == 1, "Node 3 has 1 message");
    check(net.getPendingCount(4) == 1, "Node 4 has 1 message");
}

void test_send_to_unregistered()
{
    std::cout << "\n--- Test: Send to Unregistered Node ---\n";

    Network net;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    Message msg(1, 99, 1, MessageType::REQUEST_VOTE, "test");
    check(!net.sendMessage(msg), "Cannot send to unregistered node");
}

void test_send_from_unregistered()
{
    std::cout << "\n--- Test: Send from Unregistered Node ---\n";

    Network net;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    Message msg(99, 1, 1, MessageType::REQUEST_VOTE, "test");
    check(!net.sendMessage(msg), "Cannot send from unregistered node");
}

void test_send_to_inactive()
{
    std::cout << "\n--- Test: Send to Inactive Node ---\n";

    Network net;
    Node n1(1), n2(2);
    n1.start();
    // n2 stays inactive
    net.registerNode(&n1);
    net.registerNode(&n2);

    Message msg(1, 2, 1, MessageType::APPEND_ENTRIES, "data");
    check(!net.sendMessage(msg), "Cannot send to inactive node");
}

void test_receive_empty()
{
    std::cout << "\n--- Test: Receive from Empty Mailbox ---\n";

    Network net;
    Node n1(1);
    n1.start();
    net.registerNode(&n1);

    Message msg(0, 0, 0, MessageType::REQUEST_VOTE, "");
    check(!net.receiveMessage(1, msg), "No message to receive");
}

void test_receive_order()
{
    std::cout << "\n--- Test: FIFO Message Order ---\n";

    Network net;
    Node n1(1), n2(2);
    n1.start();
    n2.start();
    net.registerNode(&n1);
    net.registerNode(&n2);

    Message m1(1, 2, 1, MessageType::REQUEST_VOTE, "first");
    Message m2(1, 2, 2, MessageType::APPEND_ENTRIES, "second");
    net.sendMessage(m1);
    net.sendMessage(m2);

    Message received(0, 0, 0, MessageType::REQUEST_VOTE, "");

    net.receiveMessage(2, received);
    check(received.getPayload() == "first", "First message received first");

    net.receiveMessage(2, received);
    check(received.getPayload() == "second", "Second message received second");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Network Tests — Astik\n";
    std::cout << "========================================\n";

    test_registration();
    test_duplicate_registration();
    test_null_registration();
    test_direct_message();
    test_multiple_messages();
    test_broadcast();
    test_send_to_unregistered();
    test_send_from_unregistered();
    test_send_to_inactive();
    test_receive_empty();
    test_receive_order();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}
