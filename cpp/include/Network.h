#ifndef NETWORK_H
#define NETWORK_H

#include <map>
#include <queue>
#include <random>
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

  // ── Simulated message delay (off by default) ─────────────────────
  // With a delay range set, every message waits a random number of
  // ticks in [minTicks, maxTicks] before it reaches the receiver's
  // mailbox.  Messages can therefore arrive in a different order than
  // they were sent, as on a real network.  (0, 0) = no delay.
  void setDelay(int minTicks, int maxTicks);
  int getMinDelay() const;
  int getMaxDelay() const;

  // Seed for the random delays: the same seed always gives the same run.
  void setSeed(unsigned int seed);

  // Move the network clock to `tick` and put every delayed message whose
  // arrival tick has come into its receiver's mailbox (earliest arrival
  // first).  RaftCluster calls this at the start of every tick.
  void advanceTo(int tick);
  int getCurrentTick() const;

  // Messages still travelling: sent, but not yet in a mailbox.
  int getInFlightCount() const;

  // Counters since the network was created.
  struct Stats {
    int sent = 0;                  // messages accepted by sendMessage()
    int delayed = 0;               // of those, held back for >= 1 tick
    int droppedReceiverDown = 0;   // receiver was stopped when it arrived
  };
  const Stats &getStats() const;

private:
  // Put a message into its receiver's mailbox now (dropped if the
  // receiver is stopped).
  bool deliverNow(const Message &msg);

  // A delayed message and when it arrives.  `order` keeps messages that
  // arrive in the same tick in the order they were sent.
  struct InFlight {
    int arriveAt;
    long long order;
    Message msg;
  };
  // Min-heap: the message that arrives first is on top.
  struct ArrivesLater {
    bool operator()(const InFlight &a, const InFlight &b) const {
      if (a.arriveAt != b.arriveAt) return a.arriveAt > b.arriveAt;
      return a.order > b.order;
    }
  };
  std::priority_queue<InFlight, std::vector<InFlight>, ArrivesLater> inFlight_;

  int minDelay_ = 0;
  int maxDelay_ = 0;
  int now_ = 0;
  long long nextOrder_ = 0;
  std::mt19937 rng_{1};
  Stats stats_;

  // Map of node ID -> pointer to the Node object
  std::map<int, Node *> nodes;

  // Map of node ID -> mailbox (queue of incoming messages)
  std::map<int, std::queue<Message>> mailboxes;
};

#endif
