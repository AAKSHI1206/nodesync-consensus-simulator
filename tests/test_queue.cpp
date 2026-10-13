#include <iostream>
#include <limits>

using namespace std;

extern "C" {
#include "queue.h"
}

void clearInput() {
  cin.clear();
  cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

int main() {
  Queue *q = queue_create();

  if (!q) {
    cout << "Error: Failed to create queue!\n";
    return 1;
  }

  cout << "======================================\n";
  cout << "       QUEUE INTERACTIVE TESTER       \n";
  cout << "======================================\n";
  cout << "Queue successfully created.\n";

  int choice = -1;

  while (choice != 0) {
    cout << "\n--- Queue Operations ---\n";
    cout << "1. Enqueue (Add integer to rear)\n";
    cout << "2. Dequeue (Remove integer from front)\n";
    cout << "3. View Front element\n";
    cout << "4. Get Queue Size\n";
    cout << "5. Check if Queue is Empty\n";
    cout << "6. Clear Queue\n";
    cout << "0. Exit\n";
    cout << "Enter your choice: ";

    if (!(cin >> choice)) {
      cout << "Invalid input. Please enter a number.\n";
      clearInput();
      continue;
    }

    switch (choice) {
    case 1: {
      int val;

      cout << "Enter integer value to enqueue: ";

      if (cin >> val) {
        int *data = new int(val);

        if (queue_enqueue(q, data) == 0) {
          cout << "-> Successfully enqueued: " << val << "\n";
          cout << "-> Current size: " << queue_size(q) << "\n";
        } else {
          cout << "-> Failed to enqueue element.\n";
          delete data;
        }
      } else {
        cout << "Invalid integer input.\n";
        clearInput();
      }
      break;
    }

    case 2: {
      if (queue_is_empty(q)) {
        cout << "-> Queue is empty. Cannot dequeue.\n";
      } else {
        int *data = (int *)queue_dequeue(q);

        if (data) {
          cout << "-> Dequeued value: " << *data << "\n";
          delete data;
        }

        cout << "-> Remaining size: " << queue_size(q) << "\n";
      }
      break;
    }

    case 3: {
      if (queue_is_empty(q)) {
        cout << "-> Queue is empty. No front element.\n";
      } else {
        int *data = (int *)queue_front(q);

        if (data) {
          cout << "-> Front element value: " << *data << "\n";
        }
      }
      break;
    }

    case 4:
      cout << "-> Current Queue size: " << queue_size(q) << "\n";
      break;

    case 5:
      if (queue_is_empty(q)) {
        cout << "-> Queue is currently EMPTY.\n";
      } else {
        cout << "-> Queue is NOT empty (contains " << queue_size(q)
             << " items).\n";
      }
      break;

    case 6:
      while (!queue_is_empty(q)) {
        int *data = (int *)queue_dequeue(q);
        delete data;
      }

      cout << "-> Queue has been cleared.\n";
      cout << "-> Current size: " << queue_size(q) << "\n";
      break;

    case 0:
      cout << "Exiting Queue Tester...\n";
      break;

    default:
      cout << "Invalid choice! Please select 0 to 6.\n";
    }
  }

  while (!queue_is_empty(q)) {
    int *data = (int *)queue_dequeue(q);
    delete data;
  }

  queue_destroy(q);

  cout << "Queue destroyed. Goodbye!\n";

  return 0;
}