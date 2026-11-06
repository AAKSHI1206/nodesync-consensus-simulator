#ifndef ORDER_SERVICE_H
#define ORDER_SERVICE_H

#include <string>
#include <vector>

#include "OrderBook.h"
#include "RaftCluster.h"

/*
 * OrderService.h
 * ==============
 * Connects client orders to the existing Raft cluster.
 *
 * Request path (nothing here decides commitment — Raft does):
 *
 *   client -> receiving node -> (forward if follower) -> leader
 *          -> RaftEngine::propose()       entry appended, NOT committed
 *          -> replication + majority      done by RaftEngine/RaftCluster
 *          -> apply callback              committed entry -> OrderBook
 *          -> result read back from the leader's OrderBook
 *
 * Each node has its own OrderBook, fed only by that node's
 * RaftEngine apply callback.
 */

enum class SubmitOutcome {
    COMMITTED,        // committed and applied: see order.status
    NOT_COMMITTED,    // appended but no majority within the wait limit
    INVALID_REQUEST,  // malformed: never sent to Raft
    NODE_DOWN,        // receiving node is stopped / unknown
    NO_LEADER         // no leader available to take the order
};

std::string submitOutcomeToString(SubmitOutcome outcome);

struct SubmitResult {
    SubmitOutcome outcome = SubmitOutcome::NO_LEADER;
    std::string   message;
    int  receivingNode = 0;
    int  leaderNode    = 0;
    bool forwarded     = false;   // receiving node was a follower
    int  logIndex      = 0;       // where the leader appended it
    int  term          = 0;
    int  ticksWaited   = 0;
    Order order;                  // valid when outcome == COMMITTED
};

class OrderService {
public:
    // The cluster must outlive this service.
    explicit OrderService(RaftCluster& cluster);
    ~OrderService();

    OrderService(const OrderService&) = delete;
    OrderService& operator=(const OrderService&) = delete;

    // Same initial stock on every node (call before any orders).
    void setStock(const std::string& product, int quantity);

    // Send an order to `receivingNode` and wait (up to the limit) for
    // Raft to commit and apply it.
    SubmitResult submitOrder(int receivingNode, const std::string& orderId,
                             const std::string& product, int quantity);

    // Simulation helpers.  Network warnings about stopped nodes are
    // hidden while these run, to keep the demo output readable.
    void runTicks(int ticks);
    int  waitForLeader(int maxTicks);   // leader ID, or -1

    // Print each step of submitOrder() (default: off).
    void setVerbose(bool verbose);
    void setMaxWaitTicks(int ticks);

    const OrderBook& getBook(int nodeId) const;
    bool replicasAgree() const;          // every node has the same state

    void printInventory(int nodeId) const;
    void printOrders(int nodeId) const;

private:
    void onCommittedEntry(int nodeId, int index, const std::string& command);

    RaftCluster&           cluster_;
    std::vector<OrderBook> books_;       // books_[nodeId - 1]
    bool verbose_      = false;
    int  maxWaitTicks_ = 50;

    // The entry submitOrder() is waiting for, and what applying it did.
    int   waitNode_  = 0;
    int   waitIndex_ = 0;
    bool  waitDone_  = false;
    Order waitOrder_;
    std::vector<std::string> applyEvents_;
};

// Scripted Phase 2 demo on a fresh 3-node cluster.  Prints every step
// when verbose; returns true only if every expected result happened.
bool runOrderDemo(bool verbose);

#endif
