    #include <iostream>
#include <string>

extern "C" {
#include "linked_list.h"
#include "log_structure.h"
}

using namespace std;

void displayList(const LinkedList *list) {
  cout << "List (size " << ll_size(list) << "): ";
  if (ll_is_empty(list)) {
    cout << "Empty\n";
    return;
  }
  for (size_t i = 0; i < ll_size(list); i++) {
    LogEntry *e = ll_get(list, i);
    if (e)
      cout << "[" << e->index << ":" << e->command << "] -> ";
  }
  cout << "NULL\n";
}

int main() {
  LinkedList *list = ll_create();
  cout << "=== Linked List Tester ===\n";

  int choice = -1;
  while (choice != 0) {
    cout << "\n1. Insert Front  2. Insert End  3. Delete Front  4. Delete End  "
            "5. Display  0. Exit\nChoice: ";
    if (!(cin >> choice))
      break;

    if (choice == 1 || choice == 2) {
      int idx, term;
      string cmd;
      cout << "Enter index, term, command: ";
      cin >> idx >> term >> cmd;
      LogEntry *entry = log_create_entry(idx, term, cmd.c_str());

      if (choice == 1)
        ll_insert_front(list, entry);
      else
        ll_insert_end(list, entry);

      cout << "Inserted successfully!\n";
    } else if (choice == 3) {
      if (ll_delete_front(list) == 0)
        cout << "Deleted front item.\n";
      else
        cout << "List is empty!\n";
    } else if (choice == 4) {
      if (ll_delete_end(list) == 0)
        cout << "Deleted end item.\n";
      else
        cout << "List is empty!\n";
    } else if (choice == 5) {
      displayList(list);
    }
  }

  ll_destroy(list);
  return 0;
}

