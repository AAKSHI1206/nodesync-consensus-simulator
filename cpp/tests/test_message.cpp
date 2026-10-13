/*
 * test_message.cpp
 * ================
 * Tests for Raft Message creation and types.
 *
 * Tests by: Aakshi
 */

#include <iostream>
#include <cassert>
#include "Message.h"

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

void test_message_fields()
{
    std::cout << "\n--- Test: Message Fields ---\n";

    Message msg(1, 2, 3, MessageType::REQUEST_VOTE, "candidate=1");

    check(msg.getSenderId() == 1, "Sender ID is 1");
    check(msg.getReceiverId() == 2, "Receiver ID is 2");
    check(msg.getTerm() == 3, "Term is 3");
    check(msg.getType() == MessageType::REQUEST_VOTE, "Type is REQUEST_VOTE");
    check(msg.getPayload() == "candidate=1", "Payload is 'candidate=1'");
}

void test_request_vote()
{
    std::cout << "\n--- Test: REQUEST_VOTE ---\n";

    Message msg(1, 2, 1, MessageType::REQUEST_VOTE, "candidate=1");

    check(msg.getType() == MessageType::REQUEST_VOTE, "Type is REQUEST_VOTE");
    check(messageTypeToString(msg.getType()) == "REQUEST_VOTE",
          "String is 'REQUEST_VOTE'");
}

void test_vote_response()
{
    std::cout << "\n--- Test: VOTE_RESPONSE ---\n";

    Message msg(2, 1, 1, MessageType::VOTE_RESPONSE, "granted=true");

    check(msg.getType() == MessageType::VOTE_RESPONSE, "Type is VOTE_RESPONSE");
    check(messageTypeToString(msg.getType()) == "VOTE_RESPONSE",
          "String is 'VOTE_RESPONSE'");
}

void test_append_entries()
{
    std::cout << "\n--- Test: APPEND_ENTRIES ---\n";

    Message msg(1, 3, 2, MessageType::APPEND_ENTRIES, "entries=[]");

    check(msg.getType() == MessageType::APPEND_ENTRIES, "Type is APPEND_ENTRIES");
    check(messageTypeToString(msg.getType()) == "APPEND_ENTRIES",
          "String is 'APPEND_ENTRIES'");
}

void test_append_response()
{
    std::cout << "\n--- Test: APPEND_RESPONSE ---\n";

    Message msg(3, 1, 2, MessageType::APPEND_RESPONSE, "success=true");

    check(msg.getType() == MessageType::APPEND_RESPONSE,
          "Type is APPEND_RESPONSE");
    check(messageTypeToString(msg.getType()) == "APPEND_RESPONSE",
          "String is 'APPEND_RESPONSE'");
}

void test_client_command()
{
    std::cout << "\n--- Test: CLIENT_COMMAND ---\n";

    Message msg(0, 1, 0, MessageType::CLIENT_COMMAND, "ADD_PRODUCT Laptop 50");

    check(msg.getType() == MessageType::CLIENT_COMMAND,
          "Type is CLIENT_COMMAND");
    check(messageTypeToString(msg.getType()) == "CLIENT_COMMAND",
          "String is 'CLIENT_COMMAND'");
    check(msg.getPayload() == "ADD_PRODUCT Laptop 50",
          "Payload matches command");
}

void test_empty_payload()
{
    std::cout << "\n--- Test: Empty Payload ---\n";

    Message msg(1, 2, 1, MessageType::APPEND_ENTRIES, "");

    check(msg.getPayload().empty(), "Empty payload is empty");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Message Tests — Aakshi\n";
    std::cout << "========================================\n";

    test_message_fields();
    test_request_vote();
    test_vote_response();
    test_append_entries();
    test_append_response();
    test_client_command();
    test_empty_payload();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}