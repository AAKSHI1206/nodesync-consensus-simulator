
#include <iostream>
#include <limits>
#include <string>

using namespace std;

extern "C" {
#include "log_structure.h"
}

void clearInput() {
  cin.clear();
  cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

int main() {
  RaftLog *log = log_create();

  if (!log) {
    cout << "Error: Failed to create log structure!\n";
    return 1;
  }

  cout << "======================================\n";
  cout << "   LOG STRUCTURE INTERACTIVE TESTER   \n";
  cout << "======================================\n";
  cout << "Raft Log initialized successfully.\n";

  int choice = -1;

  while (choice != 0) {
    cout << "\n--- Log Structure Operations ---\n";
    cout << "1. Append New Log Entry\n";
    cout << "2. Get Log Entry by Index\n";
    cout << "3. Get Last Log Entry\n";
    cout << "4. Truncate Log from Index (removes entries from index onwards)\n";
    cout << "5. Check if Index is Valid\n";
    cout << "6. View Log Metadata (Size, Last Index, Last Term)\n";
    cout << "7. Display All Log Entries\n";
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

      cout << "Enter command (e.g. SET_VAL=100): ";
      cin >> command;

      LogEntry *entry = log_create_entry(index, term, command.c_str());

      if (!entry) {
        cout << "-> Failed to allocate log entry.\n";
        break;
      }

      if (log_append_entry(log, entry) == 0) {
        cout << "-> Successfully appended log entry!\n";
        cout << "-> Current log size: " << log_get_size(log) << "\n";
        cout << "-> Last index: " << log_get_last_index(log)
             << ", Last term: " << log_get_last_term(log) << "\n";
      } else {
        cout << "-> Failed to append log entry.\n";
        log_entry_destroy(entry);
      }
      break;
    }

    case 2: {
      int query_index;

      cout << "Enter log index to retrieve: ";
      if (!(cin >> query_index)) {
        clearInput();
        break;
      }

      LogEntry *entry = log_get_entry(log, query_index);

      if (entry) {
        cout << "-> Found entry at index " << query_index << ":\n";
        cout << "   Index:   " << entry->index << "\n";
        cout << "   Term:    " << entry->term << "\n";
        cout << "   Command: \"" << entry->command << "\"\n";
      } else {
        cout << "-> No log entry found with index " << query_index << ".\n";
      }
      break;
    }

    case 3: {
      LogEntry *last = log_get_last_entry(log);

      if (last) {
        cout << "-> Last Log Entry:\n";
        cout << "   Index:   " << last->index << "\n";
        cout << "   Term:    " << last->term << "\n";
        cout << "   Command: \"" << last->command << "\"\n";
      } else {
        cout << "-> The log is currently empty.\n";
      }
      break;
    }

    case 4: {
      int from_index;

      cout << "Enter index to truncate from (deletes this and all subsequent "
              "entries): ";
      if (!(cin >> from_index)) {
        clearInput();
        break;
      }

      int removed = log_truncate_from(log, from_index);

      if (removed >= 0) {
        cout << "-> Truncation complete. Removed " << removed << " entries.\n";
        cout << "-> New log size: " << log_get_size(log) << "\n";
        cout << "-> New last index: " << log_get_last_index(log) << "\n";
      } else {
        cout << "-> Truncation failed (invalid index).\n";
      }
      break;
    }

    case 5: {
      int check_idx;

      cout << "Enter index to check: ";
      if (!(cin >> check_idx)) {
        clearInput();
        break;
      }

      if (log_is_valid_index(log, check_idx)) {
        cout << "-> Index " << check_idx << " is VALID in current log.\n";
      } else {
        cout << "-> Index " << check_idx
             << " is INVALID (out of bounds or empty).\n";
      }
      break;
    }

    case 6:
      cout << "--- Log Metadata ---\n";
      cout << "-> Total Size:  " << log_get_size(log) << "\n";
      cout << "-> Last Index:  " << log_get_last_index(log) << "\n";
      cout << "-> Last Term:   " << log_get_last_term(log) << "\n";
      break;

    case 7: {
      size_t total = log_get_size(log);

      cout << "--- All Log Entries (" << total << " total) ---\n";

      if (total == 0) {
        cout << "  (Log is empty)\n";
      } else {
        for (size_t i = 1; i <= total; i++) {
          LogEntry *e = log_get_entry(log, (int)i);

          if (e) {
            cout << "  [" << i << "] Index: " << e->index
                 << ", Term: " << e->term << ", Command: \"" << e->command
                 << "\"\n";
          }
        }
      }
      break;
    }

    case 0:
      cout << "Exiting Log Structure Tester...\n";
      break;

    default:
      cout << "Invalid choice! Please select 0 to 7.\n";
    }
  }

  log_destroy(log);

  cout << "Log structure destroyed. Goodbye!\n";

  return 0;
}
