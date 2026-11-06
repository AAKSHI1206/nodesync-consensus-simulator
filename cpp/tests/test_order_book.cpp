#include <string>
#include <vector>
#include "OrderBook.h"
#include "replication_test_utils.h"

OrderBook makeBook()
{
    OrderBook book;
    book.setStock("LAPTOP", 2);
    book.setStock("MOUSE", 3);
    return book;
}

// ── Accept, decrement, reject ──────────────────────────────────────

void test_accept_and_reject()
{
    std::cout << "\n--- Test: Accept Until Stock Runs Out ---\n";

    OrderBook book = makeBook();
    ApplyResult r1 = book.apply(1, "CREATE_ORDER|O101|LAPTOP|1");
    ApplyResult r2 = book.apply(2, "CREATE_ORDER|O102|LAPTOP|1");
    ApplyResult r3 = book.apply(3, "CREATE_ORDER|O103|LAPTOP|1");

    check(r1.applied && r1.isOrder && r1.order.status == OrderStatus::ACCEPTED,
          "O101 accepted");
    check(r2.order.status == OrderStatus::ACCEPTED, "O102 accepted");
    check(r3.order.status == OrderStatus::REJECTED &&
          r3.order.reason == "insufficient stock",
          "O103 rejected: insufficient stock");
    check(book.getStock("LAPTOP") == 0, "LAPTOP stock 2 -> 0, never negative");
    check(book.getStock("MOUSE") == 3, "Other products untouched");

    const Order* o103 = book.getOrder("O103");
    check(o103 && o103->status == OrderStatus::REJECTED && o103->logIndex == 3,
          "Rejected order is recorded with its log index");
}

void test_large_order_rejected()
{
    std::cout << "\n--- Test: Order Larger Than Stock ---\n";

    OrderBook book = makeBook();
    ApplyResult r = book.apply(1, "CREATE_ORDER|O1|MOUSE|4");
    check(r.order.status == OrderStatus::REJECTED, "4 mice with 3 in stock: rejected");
    check(book.getStock("MOUSE") == 3, "Stock unchanged");

    r = book.apply(2, "CREATE_ORDER|O2|MOUSE|3");
    check(r.order.status == OrderStatus::ACCEPTED && book.getStock("MOUSE") == 0,
          "Exactly the remaining stock: accepted, stock 0");
}

void test_unknown_product()
{
    std::cout << "\n--- Test: Unknown Product ---\n";

    OrderBook book = makeBook();
    ApplyResult r = book.apply(1, "CREATE_ORDER|O1|PHONE|1");
    check(r.order.status == OrderStatus::REJECTED &&
          r.order.reason == "unknown product", "Rejected: unknown product");
    check(book.getInventory().count("PHONE") == 0, "Unknown product not added");
}

// ── Never apply twice ──────────────────────────────────────────────

void test_duplicate_index_ignored()
{
    std::cout << "\n--- Test: Same Log Entry Applied Twice ---\n";

    OrderBook book = makeBook();
    book.apply(1, "CREATE_ORDER|O101|LAPTOP|1");
    ApplyResult again = book.apply(1, "CREATE_ORDER|O101|LAPTOP|1");
    ApplyResult older = book.apply(0, "CREATE_ORDER|O999|LAPTOP|1");

    check(!again.applied, "Re-applying index 1 is ignored");
    check(!older.applied, "An older index is ignored");
    check(book.getStock("LAPTOP") == 1, "Stock deducted only once");
    check(book.getOrders().size() == 1, "Only one order recorded");
    check(book.getLastAppliedIndex() == 1, "lastAppliedIndex = 1");
}

void test_duplicate_order_id()
{
    std::cout << "\n--- Test: Reused Order ID ---\n";

    OrderBook book = makeBook();
    book.apply(1, "CREATE_ORDER|O101|LAPTOP|1");
    ApplyResult r = book.apply(2, "CREATE_ORDER|O101|MOUSE|1");

    check(r.order.status == OrderStatus::REJECTED &&
          r.order.reason == "duplicate order ID", "Second O101 rejected");
    check(book.getStock("MOUSE") == 3, "No stock taken by the duplicate");
    check(book.getOrder("O101")->product == "LAPTOP", "First O101 kept");
}

// ── Input validation ───────────────────────────────────────────────

void test_parse_validation()
{
    std::cout << "\n--- Test: Command Validation ---\n";

    std::string id, product;
    int qty = 0;
    check(OrderBook::parseCreateOrder("CREATE_ORDER|O1|LAPTOP|2", id, product, qty) &&
          id == "O1" && product == "LAPTOP" && qty == 2, "Valid command parsed");

    std::vector<std::string> bad = {
        "CREATE_ORDER|O1|LAPTOP|0",        // zero
        "CREATE_ORDER|O1|LAPTOP|-1",       // negative
        "CREATE_ORDER|O1|LAPTOP|two",      // not a number
        "CREATE_ORDER|O1|LAPTOP|99999999", // too large
        "CREATE_ORDER|O1|LAPTOP",          // missing field
        "CREATE_ORDER||LAPTOP|1",          // empty ID
        "CREATE_ORDER|O1|LAP|TOP|1",       // extra field
        "DELETE_ORDER|O1|LAPTOP|1",        // unknown command
        "",
    };
    bool allRejected = true;
    for (const std::string& command : bad) {
        if (OrderBook::parseCreateOrder(command, id, product, qty)) {
            std::cout << "    accepted by mistake: " << command << "\n";
            allRejected = false;
        }
    }
    check(allRejected, "Every malformed command rejected");
    check(OrderBook::makeCreateOrder("O7", "MOUSE", 3) == "CREATE_ORDER|O7|MOUSE|3",
          "makeCreateOrder builds the expected format");
}

void test_malformed_entry_changes_nothing()
{
    std::cout << "\n--- Test: Malformed Committed Entry ---\n";

    OrderBook book = makeBook();
    ApplyResult r = book.apply(1, "CREATE_ORDER|O1|LAPTOP|-5");
    check(r.applied && !r.isOrder, "Entry consumed but not treated as an order");
    check(book.getStock("LAPTOP") == 2 && book.getOrders().empty(), "State unchanged");
    check(book.getLastAppliedIndex() == 1, "Index still counted as applied");
}

// ── Replicas ───────────────────────────────────────────────────────

void test_replicas_agree()
{
    std::cout << "\n--- Test: Same Commands, Same State ---\n";

    std::vector<std::string> log = {
        "CREATE_ORDER|O1|LAPTOP|1", "CREATE_ORDER|O2|MOUSE|2",
        "CREATE_ORDER|O3|LAPTOP|2", "CREATE_ORDER|O4|LAPTOP|1",
    };
    OrderBook a = makeBook();
    OrderBook b = makeBook();
    for (int i = 0; i < static_cast<int>(log.size()); i++) {
        a.apply(i + 1, log[i]);
        b.apply(i + 1, log[i]);
        b.apply(i + 1, log[i]);   // b sees every entry twice
    }
    check(a.sameStateAs(b), "Identical state despite duplicate deliveries");
    check(a.getOrder("O3")->status == OrderStatus::REJECTED &&
          a.getOrder("O4")->status == OrderStatus::ACCEPTED,
          "Log order decides: O3 (2 laptops) rejected, O4 (1 laptop) accepted");

    OrderBook c = makeBook();
    c.apply(1, log[0]);
    check(!a.sameStateAs(c), "A replica that is behind is detected as different");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  OrderBook State Machine Tests\n";
    std::cout << "========================================\n";

    test_accept_and_reject();
    test_large_order_rejected();
    test_unknown_product();
    test_duplicate_index_ignored();
    test_duplicate_order_id();
    test_parse_validation();
    test_malformed_entry_changes_nothing();
    test_replicas_agree();

    return finish("OrderBook State Machine Tests");
}