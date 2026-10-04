/*
 * queue.c
 * =======
 * FIFO Queue implementation — Member 2 contribution.
 *
 * Linked-list-based queue with O(1) enqueue and dequeue.
 * Stores generic void* data.  The queue does NOT own the data —
 * callers are responsible for freeing it.
 */

#include "queue.h"

#include <stdlib.h>  /* malloc, free */
#include <stddef.h>  /* NULL, size_t */

/* ── Creation / Destruction ───────────────────────────────────────── */

Queue *queue_create(void)
{
    Queue *queue = (Queue *)malloc(sizeof(Queue));
    if (!queue) return NULL;

    queue->front = NULL;
    queue->rear  = NULL;
    queue->size  = 0;
    return queue;
}

void queue_destroy(Queue *queue)
{
    if (!queue) return;

    /* Free all internal nodes (but NOT the data they point to) */
    queue_clear(queue);
    free(queue);
}

/* ── Core Operations ──────────────────────────────────────────────── */

int queue_enqueue(Queue *queue, void *data)
{
    if (!queue) return -1;

    QueueNode *node = (QueueNode *)malloc(sizeof(QueueNode));
    if (!node) return -1;

    node->data = data;
    node->next = NULL;

    if (queue->rear) {
        /* Non-empty queue — link after the current rear */
        queue->rear->next = node;
    } else {
        /* Empty queue — new node is also the front */
        queue->front = node;
    }
    queue->rear = node;
    queue->size++;
    return 0;
}

void *queue_dequeue(Queue *queue)
{
    if (!queue || !queue->front) return NULL;

    QueueNode *old_front = queue->front;
    void *data = old_front->data;

    queue->front = old_front->next;

    /* If the queue is now empty, reset rear as well */
    if (!queue->front) {
        queue->rear = NULL;
    }

    free(old_front);
    queue->size--;
    return data;
}

void *queue_front(const Queue *queue)
{
    if (!queue || !queue->front) return NULL;
    return queue->front->data;
}

/* ── Utility ──────────────────────────────────────────────────────── */

int queue_is_empty(const Queue *queue)
{
    return (!queue || queue->size == 0) ? 1 : 0;
}

size_t queue_size(const Queue *queue)
{
    if (!queue) return 0;
    return queue->size;
}

void queue_clear(Queue *queue)
{
    if (!queue) return;

    QueueNode *current = queue->front;
    while (current) {
        QueueNode *next = current->next;
        free(current);   /* Free the node only, NOT current->data */
        current = next;
    }

    queue->front = NULL;
    queue->rear  = NULL;
    queue->size  = 0;
}
