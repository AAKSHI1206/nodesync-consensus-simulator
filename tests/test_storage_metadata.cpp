#include <iostream>
#include <cassert>
#include "Storage.h"

int tests_passed = 0;
int tests_failed = 0;

void check(bool condition, const std::string& test_name)
{
    if (condition) {
        std::cout << "  PASS: " << test_name << "\n";
        tests_passed++;
    } else {
        std::cout << "  FAIL: " << test_name << "\n";
        tests_failed++;
    }
}

// ── Test: Empty log metadata ───────────────────────────────────────

void test_empty_log_metadata()
{
    std::cout << "\n--- Test: Empty Log Metadata ---\n";

    Storage storage;

    check(storage.getLogSize() == 0,
          "Empty log has size 0");
    check(storage.getLastLogIndex() == 0,
          "Empty log has last index 0");
    check(storage.getLastLogTerm() == 0,
          "Empty log has last term 0");
    check(storage.getLastEntry() == nullptr,
          "Empty log has null last entry");
}

// ── Test: Single-entry log metadata ────────────────────────────────

void test_single_entry_metadata()
{
    std::cout << "\n--- Test: Single-Entry Log Metadata ---\n";

    Storage storage;
    storage.appendEntry(1, 3, "cmd1");

    check(storage.getLogSize() == 1,
          "Log has size 1");
    check(storage.getLastLogIndex() == 1,
          "Last index is 1");
    check(storage.getLastLogTerm() == 3,
          "Last term is 3");
}

// ── Test: Multiple-entry log metadata ──────────────────────────────

void test_multiple_entry_metadata()
{
    std::cout << "\n--- Test: Multiple-Entry Log Metadata ---\n";

    Storage storage;
    storage.appendEntry(1, 1, "cmd1");
    storage.appendEntry(2, 1, "cmd2");
    storage.appendEntry(3, 2, "cmd3");
    storage.appendEntry(4, 3, "cmd4");

    check(storage.getLogSize() == 4,
          "Log has size 4");
    check(storage.getLastLogIndex() == 4,
          "Last index is 4");
    check(storage.getLastLogTerm() == 3,
          "Last term is 3 (from entry 4)");
}

// ── Test: Term/index consistency ───────────────────────────────────

void test_term_index_consistency()
{
    std::cout << "\n--- Test: Term/Index Consistency ---\n";

    Storage storage;
    storage.appendEntry(1, 1, "a");
    storage.appendEntry(2, 2, "b");
    storage.appendEntry(3, 2, "c");

    const LogEntry* e1 = storage.getEntry(1);
    const LogEntry* e2 = storage.getEntry(2);
    const LogEntry* e3 = storage.getEntry(3);

    check(e1 != nullptr && e1->index == 1 && e1->term == 1,
          "Entry 1: index=1, term=1");
    check(e2 != nullptr && e2->index == 2 && e2->term == 2,
          "Entry 2: index=2, term=2");
    check(e3 != nullptr && e3->index == 3 && e3->term == 2,
          "Entry 3: index=3, term=2");
}

// ── Test: Metadata after truncation ────────────────────────────────

void test_metadata_after_truncation()
{
    std::cout << "\n--- Test: Metadata After Truncation ---\n";

    Storage storage;
    storage.appendEntry(1, 1, "cmd1");
    storage.appendEntry(2, 2, "cmd2");
    storage.appendEntry(3, 3, "cmd3");

    check(storage.getLastLogIndex() == 3, "Before truncation: last index 3");
    check(storage.getLastLogTerm() == 3, "Before truncation: last term 3");

    storage.truncateFrom(3);

    check(storage.getLogSize() == 2,
          "After truncation: size is 2");
    check(storage.getLastLogIndex() == 2,
          "After truncation: last index is 2");
    check(storage.getLastLogTerm() == 2,
          "After truncation: last term is 2");
}

// ── Test: Metadata after full truncation ───────────────────────────

void test_metadata_after_full_truncation()
{
    std::cout << "\n--- Test: Metadata After Full Truncation ---\n";

    Storage storage;
    storage.appendEntry(1, 1, "cmd1");
    storage.appendEntry(2, 2, "cmd2");

    storage.truncateFrom(1);

    check(storage.getLogSize() == 0,
          "After full truncation: size is 0");
    check(storage.getLastLogIndex() == 0,
          "After full truncation: last index is 0");
    check(storage.getLastLogTerm() == 0,
          "After full truncation: last term is 0");
}

// ── Test: Metadata after clear ─────────────────────────────────────

void test_metadata_after_clear()
{
    std::cout << "\n--- Test: Metadata After Clear ---\n";

    Storage storage;
    storage.appendEntry(1, 5, "cmd1");
    storage.appendEntry(2, 5, "cmd2");

    storage.clear();

    check(storage.getLogSize() == 0,
          "After clear: size is 0");
    check(storage.getLastLogIndex() == 0,
          "After clear: last index is 0");
    check(storage.getLastLogTerm() == 0,
          "After clear: last term is 0");
}

// ── Test: Candidate up-to-date log comparison ──────────────────────

void test_candidate_log_uptodate()
{
    std::cout << "\n--- Test: Candidate Log Up-to-Date Comparison ---\n";

    // Voter has log: [(1,1), (2,2)]
    Storage voterLog;
    voterLog.appendEntry(1, 1, "a");
    voterLog.appendEntry(2, 2, "b");

    int voterLastIdx = voterLog.getLastLogIndex();
    int voterLastTrm = voterLog.getLastLogTerm();

    check(voterLastIdx == 2, "Voter last index is 2");
    check(voterLastTrm == 2, "Voter last term is 2");

    // Candidate A: lastLogIndex=3, lastLogTerm=2 — up-to-date (longer, same term)
    int candA_idx = 3, candA_trm = 2;
    bool candA_ok = (candA_trm > voterLastTrm) ||
                    (candA_trm == voterLastTrm && candA_idx >= voterLastIdx);
    check(candA_ok, "Candidate A (idx=3,trm=2) is up-to-date");

    // Candidate B: lastLogIndex=1, lastLogTerm=3 — up-to-date (higher term)
    int candB_idx = 1, candB_trm = 3;
    bool candB_ok = (candB_trm > voterLastTrm) ||
                    (candB_trm == voterLastTrm && candB_idx >= voterLastIdx);
    check(candB_ok, "Candidate B (idx=1,trm=3) is up-to-date (higher term)");

    // Candidate C: lastLogIndex=1, lastLogTerm=1 — outdated
    int candC_idx = 1, candC_trm = 1;
    bool candC_ok = (candC_trm > voterLastTrm) ||
                    (candC_trm == voterLastTrm && candC_idx >= voterLastIdx);
    check(!candC_ok, "Candidate C (idx=1,trm=1) is outdated");

    // Candidate D: lastLogIndex=1, lastLogTerm=2 — outdated (same term, shorter)
    int candD_idx = 1, candD_trm = 2;
    bool candD_ok = (candD_trm > voterLastTrm) ||
                    (candD_trm == voterLastTrm && candD_idx >= voterLastIdx);
    check(!candD_ok, "Candidate D (idx=1,trm=2) is outdated (shorter log)");
}

int main()
{
    std::cout << "========================================\n";
    std::cout << "  Storage Metadata Tests\n";
    std::cout << "========================================\n";

    test_empty_log_metadata();
    test_single_entry_metadata();
    test_multiple_entry_metadata();
    test_term_index_consistency();
    test_metadata_after_truncation();
    test_metadata_after_full_truncation();
    test_metadata_after_clear();
    test_candidate_log_uptodate();

    std::cout << "\n========================================\n";
    std::cout << "  Results: " << tests_passed << " passed, "
              << tests_failed << " failed\n";
    std::cout << "========================================\n";

    return tests_failed > 0 ? 1 : 0;
}