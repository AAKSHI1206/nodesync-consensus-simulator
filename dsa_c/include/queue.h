#ifndef QUEUE_H
#define QUEUE_H

#include <stddef.h>

typedef struct QueueNode {
    void *data;
    struct QueueNode *next;
} QueueNode;

typedef struct Queue {
    QueueNode *front;
    QueueNode *rear;
    size_t size;
} Queue;

Queue *queue_create(void);
void queue_destroy(Queue *queue);

int queue_enqueue(Queue *queue, void *data);
void *queue_dequeue(Queue *queue);
void *queue_front(const Queue *queue);

int queue_is_empty(const Queue *queue);
size_t queue_size(const Queue *queue);
void queue_clear(Queue *queue);

#endif