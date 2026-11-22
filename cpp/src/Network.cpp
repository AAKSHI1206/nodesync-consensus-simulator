#include "Network.h"
#include <iostream>

/*
 * Network.cpp
 * ===========
 * Implementation of the simulated Network class.
 *
 * Uses a simple mailbox approach: each registered node gets a queue.
 * sendMessage() puts a message into the receiver's mailbox.
 * receiveMessage() pops a message from the node's mailbox.
 *
 * Optional simulated delay (setDelay): a sent message waits in a
 * priority queue ordered by arrival tick; advanceTo(tick) moves the
 * messages that have arrived into their mailboxes.
 */

Network::Network()
{
}

// Register a node so it can send and receive messages
bool Network::registerNode(Node* node)
{
    if (node == nullptr) {
        return false;
    }

    int id = node->getId();

    // Don't register the same node twice
    if (nodes.find(id) != nodes.end()) {
        return false;
    }

    nodes[id] = node;
    mailboxes[id] = std::queue<Message>();
    return true;
}

// Send a message to a specific node
bool Network::sendMessage(const Message& msg)
{
    int senderId = msg.getSenderId();
    int receiverId = msg.getReceiverId();

    // Check that sender is registered
    if (nodes.find(senderId) == nodes.end()) {
        std::cout << "[Network] Error: sender " << senderId
                  << " is not registered.\n";
        return false;
    }

    // Check that receiver is registered
    if (nodes.find(receiverId) == nodes.end()) {
        std::cout << "[Network] Error: receiver " << receiverId
                  << " is not registered.\n";
        return false;
    }

    stats_.sent++;

    // Simulated delay: hold the message back for a random number of
    // ticks.  Whether the receiver is running is checked when it arrives.
    if (maxDelay_ > 0) {
        int span  = maxDelay_ - minDelay_ + 1;
        int delay = minDelay_ + static_cast<int>(rng_() % static_cast<unsigned int>(span));
        if (delay > 0) {
            inFlight_.push(InFlight{now_ + delay, nextOrder_++, msg});
            stats_.delayed++;
            return true;
        }
    }

    return deliverNow(msg);
}

// Put a message into the receiver's mailbox (or drop it if the receiver
// is not running)
bool Network::deliverNow(const Message& msg)
{
    int receiverId = msg.getReceiverId();

    // Check that receiver is active
    if (!nodes[receiverId]->isActive()) {
        std::cout << "[Network] Warning: receiver " << receiverId
                  << " is inactive. Message dropped.\n";
        stats_.droppedReceiverDown++;
        return false;
    }

    // Deliver the message to the receiver's mailbox
    mailboxes[receiverId].push(msg);
    return true;
}

// ── Simulated message delay ────────────────────────────────────────

void Network::setDelay(int minTicks, int maxTicks)
{
    if (minTicks < 0) minTicks = 0;
    if (maxTicks < minTicks) maxTicks = minTicks;
    minDelay_ = minTicks;
    maxDelay_ = maxTicks;
}

int Network::getMinDelay() const { return minDelay_; }
int Network::getMaxDelay() const { return maxDelay_; }

void Network::setSeed(unsigned int seed)
{
    rng_.seed(seed);
}

void Network::advanceTo(int tick)
{
    now_ = tick;
    // Earliest arrival first; messages arriving in the same tick keep
    // the order they were sent in.
    while (!inFlight_.empty() && inFlight_.top().arriveAt <= now_) {
        Message msg = inFlight_.top().msg;
        inFlight_.pop();
        deliverNow(msg);
    }
}

int Network::getCurrentTick() const { return now_; }

int Network::getInFlightCount() const
{
    return static_cast<int>(inFlight_.size());
}

const Network::Stats& Network::getStats() const
{
    return stats_;
}

// Broadcast a message from sender to all other registered nodes
int Network::broadcast(int senderId, int term, MessageType type,
                       const std::string& payload)
{
    // Check that sender is registered
    if (nodes.find(senderId) == nodes.end()) {
        std::cout << "[Network] Error: sender " << senderId
                  << " is not registered.\n";
        return 0;
    }

    int delivered = 0;

    for (auto& pair : nodes) {
        int receiverId = pair.first;

        // Don't send to yourself
        if (receiverId == senderId) {
            continue;
        }

        Message msg(senderId, receiverId, term, type, payload);

        if (sendMessage(msg)) {
            delivered++;
        }
    }

    return delivered;
}

// Receive the next message for a given node
bool Network::receiveMessage(int nodeId, Message& msg)
{
    // Check that node is registered
    if (mailboxes.find(nodeId) == mailboxes.end()) {
        return false;
    }

    // Check if there are any messages
    if (mailboxes[nodeId].empty()) {
        return false;
    }

    // Pop the next message
    msg = mailboxes[nodeId].front();
    mailboxes[nodeId].pop();
    return true;
}

// Check if a node is registered
bool Network::isRegistered(int nodeId) const
{
    return nodes.find(nodeId) != nodes.end();
}

// Get the number of registered nodes
int Network::getNodeCount() const
{
    return static_cast<int>(nodes.size());
}

// Check how many messages are pending for a node
int Network::getPendingCount(int nodeId) const
{
    auto it = mailboxes.find(nodeId);
    if (it == mailboxes.end()) {
        return 0;
    }
    return static_cast<int>(it->second.size());
}
