#ifndef STORAGE_H
#define STORAGE_H

#include <string>
#include <cstddef>

extern "C" {
    #include "log_structure.h"
}

class Storage {
public:
    // Constructor and destructor
    Storage();
    ~Storage();

    // Move operations
    Storage(Storage&& other) noexcept;
    Storage& operator=(Storage&& other) noexcept;

    // Copying is not allowed
    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;

    // Add a new entry
    bool appendEntry(int index, int term, const std::string& command);

    // Get an entry by index
    const LogEntry* getEntry(int index) const;

    // Get the last entry
    const LogEntry* getLastEntry() const;

    // Get total number of entries
    std::size_t getLogSize() const;

    // Get index of last entry
    int getLastLogIndex() const;

    // Get term of last entry
    int getLastLogTerm() const;

    // Remove entries from given index onwards
    int truncateFrom(int fromIndex);

    // Remove all entries
    void clear();

private:
    RaftLog* log_;
};

#endif
