#include "Storage.h"
#include <iostream>
#include <limits>
#include <string>

using namespace std;

void clearInput() {
  cin.clear();
  cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

int main() {
  Storage storage;
  
  int choice = -1;

  while (choice != 0) {
    cout << "\n--- Storage Operations ---\n";
    cout << "1. Append New Entry\n";
    cout << "2. Get Entry by Index\n";
    cout << "3. Get Last Entry\n";
    cout << "4. Truncate Log from Index\n";
    cout << "5. View Storage Stats (Size, Last Index, Last Term)\n";
    cout << "6. Display All Log Entries in Storage\n";
    cout << "7. Clear Storage\n";
    cout << "0. Exit\n";
    cout << "Enter your choice: ";

    if (!(cin >> choice)) {
      cout << "Invalid input. Please enter a number.\n";
      clearInput();
      continue;
    }

    switch (choice) {
    case 1: {
      int index, term;
      string command;

      cout << "Enter entry index (e.g. 1): ";
      if (!(cin >> index)) {
        clearInput();
        break;
      }

      cout << "Enter entry term (e.g. 1): ";
      if (!(cin >> term)) {
        clearInput();
        break;
      }

      cout << "Enter command (e.g. KEY=VAL): ";
      cin >> command;

      if (storage.appendEntry(index, term, command)) {
        cout << "-> Successfully appended entry to Storage!\n";
        cout << "-> Current size: " << storage.getLogSize() << "\n";
        cout << "-> Last index: " << storage.getLastLogIndex()
             << ", Last term: " << storage.getLastLogTerm() << "\n";
      } else {
        cout << "-> Failed to append entry to Storage.\n";
      }
      break;
    }

    case 2: {
      int query_index;

      cout << "Enter index to retrieve: ";
      if (!(cin >> query_index)) {
        clearInput();
        break;
      }

      const LogEntry *entry = storage.getEntry(query_index);

      if (entry) {
        cout << "-> Entry found in Storage:\n";
        cout << "   Index:   " << entry->index << "\n";
        cout << "   Term:    " << entry->term << "\n";
        cout << "   Command: \"" << entry->command << "\"\n";
      } else {
        cout << "-> Entry with index " << query_index
             << " not found in Storage.\n";
      }
      break;
    }

    case 3: {
      const LogEntry *last = storage.getLastEntry();

      if (last) {
        cout << "-> Last Entry in Storage:\n";
        cout << "   Index:   " << last->index << "\n";
        cout << "   Term:    " << last->term << "\n";
        cout << "   Command: \"" << last->command << "\"\n";
      } else {
        cout << "-> Storage log is currently empty.\n";
      }
      break;
    }

    case 4: {
      int from_index;

      cout << "Enter index to truncate from: ";
      if (!(cin >> from_index)) {
        clearInput();
        break;
      }

      int removed = storage.truncateFrom(from_index);

      if (removed >= 0) {
        cout << "-> Truncation successful. Removed " << removed
             << " entries.\n";
        cout << "-> New size: " << storage.getLogSize() << "\n";
        cout << "-> New last index: " << storage.getLastLogIndex() << "\n";
      } else {
        cout << "-> Truncation failed (invalid index).\n";
      }
      break;
    }

    case 5:
      cout << "--- Storage Stats ---\n";
      cout << "-> Total Log Size: " << storage.getLogSize() << "\n";
      cout << "-> Last Log Index: " << storage.getLastLogIndex() << "\n";
      cout << "-> Last Log Term:  " << storage.getLastLogTerm() << "\n";
      break;

    case 6: {
      size_t total = storage.getLogSize();

      cout << "--- All Entries in Storage (" << total << " total) ---\n";

      if (total == 0) {
        cout << "  (Storage is empty)\n";
      } else {
        for (size_t i = 1; i <= total; i++) {
          const LogEntry *e = storage.getEntry((int)i);

          if (e) {
            cout << "  [" << i << "] Index: " << e->index
                 << ", Term: " << e->term << ", Command: \"" << e->command
                 << "\"\n";
          }
        }
      }
      break;
    }

    case 7:
      storage.clear();
      cout << "-> Storage cleared. Size is now: " << storage.getLogSize()
           << "\n";
      break;

    case 0:
      cout << "Exiting Storage Tester...\n";
      break;

    default:
      cout << "Invalid choice! Please select 0 to 7.\n";
    }
  }

  cout << "Storage instance destroyed automatically. Goodbye!\n";

  return 0;
}