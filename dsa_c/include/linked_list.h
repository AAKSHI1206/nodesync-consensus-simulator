#ifndef LINKED_LIST_H
#define LINKED_LIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

struct LogEntry;

typedef struct LLNode {
    struct LogEntry *data;
    struct LLNode *next;
} LLNode;

typedef struct LinkedList {
    LLNode *head;
    size_t size;
} LinkedList;

/* Creation and Destruction */

LinkedList *ll_create(void);

LLNode *ll_create_node(struct LogEntry *entry);

void ll_destroy(LinkedList *list);

/* Insertion */

int ll_insert_front(LinkedList *list, struct LogEntry *entry);

int ll_insert_end(LinkedList *list, struct LogEntry *entry);

int ll_insert_at(LinkedList *list, struct LogEntry *entry, size_t index);

/* Deletion */

int ll_delete_front(LinkedList *list);

int ll_delete_end(LinkedList *list);

int ll_delete_at(LinkedList *list, size_t index);

/* Access and Search */

struct LogEntry *ll_get(const LinkedList *list, size_t index);

size_t ll_search(const LinkedList *list, int log_index);

/* Traversal and Utility */

void ll_traverse(const LinkedList *list, void (*visit)(const struct LogEntry *));

size_t ll_size(const LinkedList *list);

int ll_is_empty(const LinkedList *list);

#ifdef __cplusplus
}
#endif

#endif
