#include "Storage.h"
#include <stdexcept>

// Constructor
Storage::Storage()
    : log_(log_create())
{
    if (log_ == nullptr) {
        throw std::runtime_error("Failed to create RaftLog");
    }
}

// Destructor
Storage::~Storage()
{
    log_destroy(log_);
    log_ = nullptr;
}

// Move constructor
Storage::Storage(Storage&& other) noexcept
    : log_(other.log_)
{
    other.log_ = nullptr;
}

// Move assignment
Storage& Storage::operator=(Storage&& other) noexcept
{
    if (this != &other) {
        log_destroy(log_);

        log_ = other.log_;
        other.log_ = nullptr;
    }

    return *this;
}

// Add a new log entry
bool Storage::appendEntry(
    int index,
    int term,
    const std::string& command)
{
    if (log_ == nullptr) {
        return false;
    }

    LogEntry* entry =
        log_create_entry(index, term, command.c_str());

    if (entry == nullptr) {
        return false;
    }

    if (log_append_entry(log_, entry) != 0) {
        log_entry_destroy(entry);
        return false;
    }

    return true;
}

// Get entry by index
const LogEntry* Storage::getEntry(int index) const
{
    if (log_ == nullptr) {
        return nullptr;
    }

    return log_get_entry(log_, index);
}

// Get last entry
const LogEntry* Storage::getLastEntry() const
{
    if (log_ == nullptr) {
        return nullptr;
    }

    return log_get_last_entry(log_);
}

// Get number of entries
std::size_t Storage::getLogSize() const
{
    if (log_ == nullptr) {
        return 0;
    }

    return log_get_size(log_);
}

// Get last index
int Storage::getLastLogIndex() const
{
    if (log_ == nullptr) {
        return 0;
    }

    return log_get_last_index(log_);
}

// Get last term
int Storage::getLastLogTerm() const
{
    if (log_ == nullptr) {
        return 0;
    }

    return log_get_last_term(log_);
}

// Remove entries from given index
int Storage::truncateFrom(int fromIndex)
{
    if (log_ == nullptr) {
        return -1;
    }

    return log_truncate_from(log_, fromIndex);
}

// Remove all entries
void Storage::clear()
{
    if (log_ == nullptr) {
        return;
    }

    log_destroy(log_);
    log_ = log_create();
}
