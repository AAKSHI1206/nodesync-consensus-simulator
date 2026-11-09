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

// ── Read requests ───────────────────────────────────────────────────
// Reads never go into the Raft log.  The server that receives one
// answers from its OWN OrderBook:
//     GET_STOCK|<product>     units of a product in stock
//     GET_ORDER|<orderId>     an order's status
// A follower can be behind the leader (it learns new commits on the next
// AppendEntries, or after a restart it still has to catch up), so the
// answer says whether it may be stale.
enum class ReadOutcome {
    OK,               // answered (see stock / order)
    NOT_FOUND,        // unknown product or order ID
    INVALID_REQUEST,  // malformed read command
    NODE_DOWN         // serving node is stopped / unknown
};

std::string readOutcomeToString(ReadOutcome outcome);

struct ReadResult {
    ReadOutcome outcome = ReadOutcome::INVALID_REQUEST;
    std::string message;          // the answer in words
    int  servingNode   = 0;
    int  appliedIndex  = 0;       // log entries this node has applied
    int  leaderCommit  = -1;      // leader's commit index (-1: no leader)
    bool possiblyStale = false;   // behind the leader, or no leader to compare with
    int   stock = 0;              // GET_STOCK answer
    Order order;                  // GET_ORDER answer (when found)
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

    // Answer a read from `servingNode`'s own OrderBook (no Raft round).
    ReadResult read(int servingNode, const std::string& command);

    // Read command helpers.  isReadCommand() only looks at the prefix,
    // so a malformed read is still recognised (and rejected) as a read.
    static bool        isReadCommand(const std::string& command);
    static std::string makeGetStock(const std::string& product);
    static std::string makeGetOrder(const std::string& orderId);

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
