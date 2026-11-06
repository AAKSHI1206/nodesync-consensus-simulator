#include "OrderBook.h"
#include <sstream>
#include <vector>

/*
 * OrderBook.cpp
 * =============
 * Order and inventory state machine.  See OrderBook.h for the rules.
 */

std::string orderStatusToString(OrderStatus status)
{
    return status == OrderStatus::ACCEPTED ? "ACCEPTED" : "REJECTED";
}

// ── Setup ──────────────────────────────────────────────────────────

void OrderBook::setStock(const std::string& product, int quantity)
{
    inventory_[product] = quantity < 0 ? 0 : quantity;
}

// ── Command format ─────────────────────────────────────────────────

std::string OrderBook::makeCreateOrder(const std::string& orderId,
                                       const std::string& product,
                                       int quantity)
{
    return "CREATE_ORDER|" + orderId + "|" + product + "|" +
           std::to_string(quantity);
}

bool OrderBook::parseCreateOrder(const std::string& command,
                                 std::string& orderId,
                                 std::string& product,
                                 int& quantity)
{
    // Split on '|' into exactly 4 parts.
    std::vector<std::string> parts;
    std::stringstream ss(command);
    std::string part;
    while (std::getline(ss, part, '|')) {
        parts.push_back(part);
    }
    if (parts.size() != 4 || parts[0] != "CREATE_ORDER") {
        return false;
    }
    if (parts[1].empty() || parts[2].empty() || parts[3].empty()) {
        return false;
    }

    // Quantity: digits only, 1 .. 1000000 (keeps it far from overflow).
    if (parts[3].size() > 7) {
        return false;
    }
    for (char c : parts[3]) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    int value = std::stoi(parts[3]);
    if (value < 1 || value > 1000000) {
        return false;
    }

    orderId  = parts[1];
    product  = parts[2];
    quantity = value;
    return true;
}

// ── Apply one committed entry ──────────────────────────────────────

ApplyResult OrderBook::apply(int logIndex, const std::string& command)
{
    ApplyResult result;

    // Never apply the same (or an older) log entry twice.
    if (logIndex <= lastAppliedIndex_) {
        return result;                 // applied = false
    }
    lastAppliedIndex_ = logIndex;
    result.applied = true;

    Order order;
    if (!parseCreateOrder(command, order.id, order.product, order.quantity)) {
        return result;                 // not an order: nothing changes
    }
    order.logIndex = logIndex;
    result.isOrder = true;

    // An order ID can only be used once; the first one stays.
    if (orders_.count(order.id) > 0) {
        order.status = OrderStatus::REJECTED;
        order.reason = "duplicate order ID";
        result.order = order;
        return result;
    }

    auto stock = inventory_.find(order.product);
    if (stock == inventory_.end()) {
        order.status = OrderStatus::REJECTED;
        order.reason = "unknown product";
    } else if (stock->second < order.quantity) {
        order.status = OrderStatus::REJECTED;
        order.reason = "insufficient stock";
    } else {
        stock->second -= order.quantity;   // never goes below 0
        order.status = OrderStatus::ACCEPTED;
    }

    orders_[order.id] = order;
    result.order = order;
    return result;
}

// ── Queries ────────────────────────────────────────────────────────

int OrderBook::getStock(const std::string& product) const
{
    auto it = inventory_.find(product);
    return it != inventory_.end() ? it->second : 0;
}

const Order* OrderBook::getOrder(const std::string& orderId) const
{
    auto it = orders_.find(orderId);
    return it != orders_.end() ? &it->second : nullptr;
}

int OrderBook::getLastAppliedIndex() const
{
    return lastAppliedIndex_;
}

const std::map<std::string, int>& OrderBook::getInventory() const
{
    return inventory_;
}

const std::map<std::string, Order>& OrderBook::getOrders() const
{
    return orders_;
}

bool OrderBook::sameStateAs(const OrderBook& other) const
{
    if (inventory_ != other.inventory_ || orders_.size() != other.orders_.size()) {
        return false;
    }
    for (const auto& entry : orders_) {
        const Order* theirs = other.getOrder(entry.first);
        if (theirs == nullptr ||
            theirs->product  != entry.second.product  ||
            theirs->quantity != entry.second.quantity ||
            theirs->status   != entry.second.status   ||
            theirs->logIndex != entry.second.logIndex) {
            return false;
        }
    }
    return true;
}
