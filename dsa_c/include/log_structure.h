
#ifndef LOG_STRUCTURE_H
#define LOG_STRUCTURE_H

#include <stddef.h>

typedef struct LogEntry {
  int index;
  int term;
  char *command;
} LogEntry;

struct LinkedList;

typedef struct RaftLog {
  struct LinkedList *entries;
} RaftLog;

LogEntry *log_create_entry(int index, int term, const char *command);
void log_entry_destroy(LogEntry *entry);

RaftLog *log_create(void);
void log_destroy(RaftLog *log);

int log_append_entry(RaftLog *log, LogEntry *entry);
LogEntry *log_get_entry(const RaftLog *log, int index);
LogEntry *log_get_last_entry(const RaftLog *log);

size_t log_get_size(const RaftLog *log);
int log_get_last_index(const RaftLog *log);
int log_get_last_term(const RaftLog *log);

int log_truncate_from(RaftLog *log, int from_index);
int log_is_valid_index(const RaftLog *log, int index);

#endif