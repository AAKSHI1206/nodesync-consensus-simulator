#include "linked_list.h"
#include "log_structure.h"
#include <stdlib.h>
#include <stddef.h>

LinkedList *ll_create(void)
{
    LinkedList *list = malloc(sizeof(LinkedList));
    if (!list) return NULL;

    list->head = NULL;
    list->size = 0;
    return list;
}

LLNode *ll_create_node(LogEntry *entry)
{
    LLNode *node = malloc(sizeof(LLNode));
    if (!node) return NULL;

    node->data = entry;
    node->next = NULL;
    return node;
}

void ll_destroy(LinkedList *list)
{
    if (!list) return;

    LLNode *current = list->head;

    while (current) {
        LLNode *next = current->next;
        log_entry_destroy(current->data);
        free(current);
        current = next;
    }

    free(list);
}

/* Insertion */

int ll_insert_front(LinkedList *list, LogEntry *entry)
{
    if (!list) return -1;

    LLNode *node = ll_create_node(entry);
    if (!node) return -1;

    node->next = list->head;
    list->head = node;
    list->size++;

    return 0;
}

int ll_insert_end(LinkedList *list, LogEntry *entry)
{
    if (!list) return -1;

    LLNode *node = ll_create_node(entry);
    if (!node) return -1;

    if (!list->head) {
        list->head = node;
    } else {
        LLNode *current = list->head;

        while (current->next) {
            current = current->next;
        }

        current->next = node;
    }

    list->size++;
    return 0;
}

int ll_insert_at(LinkedList *list, LogEntry *entry, size_t index)
{
    if (!list || index > list->size) return -1;

    if (index == 0) return ll_insert_front(list, entry);
    if (index == list->size) return ll_insert_end(list, entry);

    LLNode *node = ll_create_node(entry);
    if (!node) return -1;

    LLNode *prev = list->head;

    for (size_t i = 0; i < index - 1; i++) {
        prev = prev->next;
    }

    node->next = prev->next;
    prev->next = node;

    list->size++;
    return 0;
}

/* Deletion */

int ll_delete_front(LinkedList *list)
{
    if (!list || !list->head) return -1;

    LLNode *old_head = list->head;
    list->head = old_head->next;

    log_entry_destroy(old_head->data);
    free(old_head);

    list->size--;
    return 0;
}

int ll_delete_end(LinkedList *list)
{
    if (!list || !list->head) return -1;

    if (!list->head->next) {
        log_entry_destroy(list->head->data);
        free(list->head);

        list->head = NULL;
        list->size--;

        return 0;
    }

    LLNode *prev = list->head;

    while (prev->next->next) {
        prev = prev->next;
    }

    log_entry_destroy(prev->next->data);
    free(prev->next);

    prev->next = NULL;
    list->size--;

    return 0;
}

int ll_delete_at(LinkedList *list, size_t index)
{
    if (!list || !list->head || index >= list->size) return -1;

    if (index == 0) return ll_delete_front(list);

    LLNode *prev = list->head;

    for (size_t i = 0; i < index - 1; i++) {
        prev = prev->next;
    }

    LLNode *target = prev->next;
    prev->next = target->next;

    log_entry_destroy(target->data);
    free(target);

    list->size--;
    return 0;
}

/* Access and Search */

LogEntry *ll_get(const LinkedList *list, size_t index)
{
    if (!list || index >= list->size) return NULL;

    const LLNode *current = list->head;

    for (size_t i = 0; i < index; i++) {
        current = current->next;
    }

    return current->data;
}

size_t ll_search(const LinkedList *list, int log_index)
{
    if (!list) return (size_t)-1;

    const LLNode *current = list->head;
    size_t pos = 0;

    while (current) {
        if (current->data && current->data->index == log_index) {
            return pos;
        }

        current = current->next;
        pos++;
    }

    return (size_t)-1;
}

/* Traversal and Utility */

void ll_traverse(const LinkedList *list, void (*visit)(const LogEntry *))
{
    if (!list || !visit) return;

    const LLNode *current = list->head;

    while (current) {
        visit(current->data);
        current = current->next;
    }
}

size_t ll_size(const LinkedList *list)
{
    return list ? list->size : 0;
}

int ll_is_empty(const LinkedList *list)
{
    return (!list || list->size == 0);
}