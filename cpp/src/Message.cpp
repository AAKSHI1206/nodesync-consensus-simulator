#include "Message.h"

/*
 * Message.cpp
 * ===========
 * Implementation of the Raft Message class.
 *
 * Implemented by: Aakshi
 */

// Convert MessageType to a readable string
std::string messageTypeToString(MessageType type)
{
    switch (type) {
        case MessageType::REQUEST_VOTE:    return "REQUEST_VOTE";
        case MessageType::VOTE_RESPONSE:   return "VOTE_RESPONSE";
        case MessageType::APPEND_ENTRIES:  return "APPEND_ENTRIES";
        case MessageType::APPEND_RESPONSE: return "APPEND_RESPONSE";
        case MessageType::CLIENT_COMMAND:  return "CLIENT_COMMAND";
        default:                           return "UNKNOWN";
    }
}

// Constructor
Message::Message(int sender, int receiver, int term, MessageType type,
                 const std::string& payload)
    : senderId(sender),
      receiverId(receiver),
      term(term),
      type(type),
      payload(payload)
{
}

// Getters
int Message::getSenderId() const
{
    return senderId;
}

int Message::getReceiverId() const
{
    return receiverId;
}

int Message::getTerm() const
{
    return term;
}

MessageType Message::getType() const
{
    return type;
}

std::string Message::getPayload() const
{
    return payload;
}
