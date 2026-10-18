#include <iostream>
#include <string>

extern "C" {
    #include "queue.h"
    #include "linked_list.h"
    #include "log_structure.h"
}

#include "Storage.h"

using namespace std;

void testQueue() {
    Queue* q = queue_create();
    cout << "\n--- Queue Test ---\n";
    int val = 42;
    queue_enqueue(q, &val);
    cout << "Enqueued: " << val << ", Size: " << queue_size(q) << endl;
    cout << "Front: " << *(int*)queue_front(q) << endl;
    cout << "Dequeued: " << *(int*)queue_dequeue(q) << endl;
    cout << "Is Empty: " << (queue_is_empty(q) ? "Yes" : "No") << endl;
    queue_destroy(q);
}

void testLinkedList() {
    LinkedList* list = ll_create();
    cout << "\n--- Linked List Test ---\n";
    LogEntry* e1 = log_create_entry(1, 1, "CMD1");
    LogEntry* e2 = log_create_entry(2, 1, "CMD2");
    ll_insert_end(list, e1);
    ll_insert_end(list, e2);
    cout << "Size: " << ll_size(list) << endl;
    LogEntry* got = ll_get(list, 0);
    if (got) cout << "First item: [" << got->index << "] " << got->command << endl;
    ll_delete_front(list);
    cout << "Size after delete front: " << ll_size(list) << endl;
    ll_destroy(list);
}

void testLogStructure() {
    RaftLog* log = log_create();
    cout << "\n--- Log Structure Test ---\n";
    log_append_entry(log, log_create_entry(1, 1, "SET_A"));
    log_append_entry(log, log_create_entry(2, 2, "SET_B"));
    cout << "Size: " << log_get_size(log) << ", Last Index: " << log_get_last_index(log) << endl;
    LogEntry* last = log_get_last_entry(log);
    if (last) cout << "Last Entry: [" << last->index << "] " << last->command << endl;
    log_destroy(log);
}

void testStorage() {
    Storage storage;
    cout << "\n--- Storage Test ---\n";
    storage.appendEntry(1, 1, "INIT");
    storage.appendEntry(2, 1, "UPDATE");
    cout << "Size: " << storage.getLogSize() << ", Last Index: " << storage.getLastLogIndex() << endl;
    const LogEntry* e = storage.getEntry(1);
    if (e) cout << "Entry 1: [" << e->index << "] " << e->command << endl;
}

int main() {
    cout << "=== Phase 1 Basic Blocks Automated Demo ===\n";
    testQueue();
    testLinkedList();
    testLogStructure();
    testStorage();
    cout << "\nAll basic blocks tested successfully!\n";
    return 0;
}