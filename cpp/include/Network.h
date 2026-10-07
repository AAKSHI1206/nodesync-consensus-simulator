#ifndef NETWORK_H
#define NETWORK_H

#include <map>
#include <queue>
#include <vector>

#include "Message.h"
#include "Node.h"

/*
 * Network.h
 * =========
 * Simulated network communication layer.
 * Manages registered nodes and routes messages between them.
 *
 * Messages are stored in per-node mailboxes (queues).
 * No real sockets or threads — this is a simulation.
 *
 * Implemented by: Astik
 */

class Network {
public:
  Network();

  // Register a node with the network
  bool registerNode(Node *node);

  // Send a message from one node to another
  // Returns true if delivery was successful
  bool sendMessage(const Message &msg);

  // Broadcast a message from sender to all other registered nodes
  // Returns the number of nodes that received the message
  int broadcast(int senderId, int term, MessageType type,
                const std::string &payload);

  // Receive the next message for a given node
  // Returns true if a message was available, and fills 'msg'
  bool receiveMessage(int nodeId, Message &msg);

  // Check if a node is registered
  bool isRegistered(int nodeId) const;

  // Get the number of registered nodes
  int getNodeCount() const;

  // Check how many messages are pending for a node
  int getPendingCount(int nodeId) const;

private:
  // Map of node ID -> pointer to the Node object
  std::map<int, Node *> nodes;

  // Map of node ID -> mailbox (queue of incoming messages)
  std::map<int, std::queue<Message>> mailboxes;
};

#endif
