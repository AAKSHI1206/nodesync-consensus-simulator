
#include "log_structure.h"
#include "linked_list.h"
#include <stdlib.h>
#include <string.h>

LogEntry *log_create_entry(int index, int term, const char *command) {
  if (command == NULL)
    return NULL;

  LogEntry *entry = malloc(sizeof(LogEntry));
  if (entry == NULL)
    return NULL;

  entry->command = malloc(strlen(command) + 1);
  if (entry->command == NULL) {
    free(entry);
    return NULL;
  }

  strcpy(entry->command, command);
  entry->index = index;
  entry->term = term;

  return entry;
}

void log_entry_destroy(LogEntry *entry) {
  if (entry == NULL)
    return;

  free(entry->command);
  free(entry);
}

RaftLog *log_create(void) {
  RaftLog *log = malloc(sizeof(RaftLog));
  if (log == NULL)
    return NULL;

  log->entries = ll_create();
  if (log->entries == NULL) {
    free(log);
    return NULL;
  }

  return log;
}

void log_destroy(RaftLog *log) {
  if (log == NULL)
    return;

  ll_destroy(log->entries);
  free(log);
}

int log_append_entry(RaftLog *log, LogEntry *entry) {
  if (log == NULL || entry == NULL)
    return -1;

  return ll_insert_end(log->entries, entry);
}

LogEntry *log_get_entry(const RaftLog *log, int index) {
  if (log == NULL || index < 1)
    return NULL;

  return ll_get(log->entries, (size_t)(index - 1));
}

LogEntry *log_get_last_entry(const RaftLog *log) {
  if (log == NULL || ll_is_empty(log->entries))
    return NULL;

  return ll_get(log->entries, ll_size(log->entries) - 1);
}

size_t log_get_size(const RaftLog *log) {
  if (log == NULL)
    return 0;

  return ll_size(log->entries);
}

int log_get_last_index(const RaftLog *log) {
  LogEntry *entry = log_get_last_entry(log);
  if (entry == NULL)
    return 0;

  return entry->index;
}

int log_get_last_term(const RaftLog *log) {
  LogEntry *entry = log_get_last_entry(log);
  if (entry == NULL)
    return 0;

  return entry->term;
}

int log_truncate_from(RaftLog *log, int from_index) {
  if (log == NULL || from_index < 1)
    return -1;

  int removed = 0;

  while (!ll_is_empty(log->entries)) {
    size_t last = ll_size(log->entries) - 1;
    LogEntry *entry = ll_get(log->entries, last);

    if (entry->index < from_index)
      break;

    ll_delete_end(log->entries);
    removed++;
  }

  return removed;
}

int log_is_valid_index(const RaftLog *log, int index) {
  if (log == NULL || index < 1)
    return 0;

  return index <= log_get_last_index(log);
}