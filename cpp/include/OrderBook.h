#ifndef ORDER_BOOK_H
#define ORDER_BOOK_H

#include <map>
#include <string>

/*
 * OrderBook.h
 * ===========
 * Order and inventory state machine (one instance per node).
 *
 * It changes ONLY through apply(), which RaftEngine calls for each
 * committed log entry, in log order.  Every node applies the same
 * committed commands in the same order, so every node ends up with the
 * same inventory and the same orders.
 *
 * Command format (one log entry):
 *     CREATE_ORDER|<orderId>|<product>|<quantity>
 *     e.g. CREATE_ORDER|O101|LAPTOP|1
 *
 * Policy:
 *   - Malformed requests are rejected before they reach the log
 *     (OrderService checks them with parseCreateOrder()).
 *   - A well-formed order is committed first; stock is checked when it
 *     is applied.  If there is not enough stock, the committed order is
 *     recorded as REJECTED and the stock does not change.
 */

enum class OrderStatus {
    ACCEPTED,
    REJECTED
};

std::string orderStatusToString(OrderStatus status);

struct Order {
    std::string id;
    std::string product;
    int         quantity = 0;
    OrderStatus status   = OrderStatus::REJECTED;
    std::string reason;       // why it was rejected (empty if accepted)
    int         logIndex = 0; // Raft log index it was applied from
};

// What apply() did with one committed entry.
struct ApplyResult {
    bool        applied = false;  // false = duplicate index, ignored
    bool        isOrder = false;  // true if the entry was a valid order
    Order       order;            // the order as recorded (if isOrder)
};

class OrderBook {
public:
    // Initial catalogue.  Call before the cluster processes any orders,
    // with the same values on every node (it is configuration, not a
    // replicated command).
    void setStock(const std::string& product, int quantity);

    // Apply one committed log entry.  Indexes at or below the last one
    // applied are ignored, so an entry can never be applied twice.
    ApplyResult apply(int logIndex, const std::string& command);

    // Queries
    int          getStock(const std::string& product) const;  // 0 if unknown
    const Order* getOrder(const std::string& orderId) const;  // null if none
    int          getLastAppliedIndex() const;

    const std::map<std::string, int>&   getInventory() const;
    const std::map<std::string, Order>& getOrders() const;

    // Same inventory and same orders (used to check replicas agree).
    bool sameStateAs(const OrderBook& other) const;

    // Build / parse the command string.  parseCreateOrder() returns
    // false for anything malformed (wrong format, quantity <= 0, ...).
    static std::string makeCreateOrder(const std::string& orderId,
                                       const std::string& product,
                                       int quantity);
    static bool parseCreateOrder(const std::string& command,
                                 std::string& orderId,
                                 std::string& product,
                                 int& quantity);

private:
    std::map<std::string, int>   inventory_;   // product -> units in stock
    std::map<std::string, Order> orders_;      // order ID -> order
    int lastAppliedIndex_ = 0;
};

#endif
