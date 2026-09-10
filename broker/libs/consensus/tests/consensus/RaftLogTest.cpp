#include <gtest/gtest.h>
#include <rune/consensus/RaftLog.h>

using namespace rune::consensus;

// ============================================================================
// Empty log tests
// ============================================================================

TEST(RaftLog, EmptyLogHasZeroIndices) {
    RaftLog log;
    EXPECT_EQ(log.lastIndex(), 0);
    EXPECT_EQ(log.lastTerm(), 0);
    EXPECT_EQ(log.commitIndex(), 0);
}

TEST(RaftLog, TermAtZeroOrBeyondReturnsZero) {
    RaftLog log;
    EXPECT_EQ(log.termAt(0), 0);
    EXPECT_EQ(log.termAt(1), 0);
    EXPECT_EQ(log.termAt(-1), 0);
}

// ============================================================================
// Append and basic accessors
// ============================================================================

TEST(RaftLog, AppendReturnsIncreasingIndices) {
    RaftLog log;
    Index idx1 = log.append(1, "cmd1");
    EXPECT_EQ(idx1, 1);
    Index idx2 = log.append(1, "cmd2");
    EXPECT_EQ(idx2, 2);
    Index idx3 = log.append(2, "cmd3");
    EXPECT_EQ(idx3, 3);
}

TEST(RaftLog, LastIndexAndTermTrackAppends) {
    RaftLog log;
    log.append(1, "cmd1");
    EXPECT_EQ(log.lastIndex(), 1);
    EXPECT_EQ(log.lastTerm(), 1);

    log.append(1, "cmd2");
    EXPECT_EQ(log.lastIndex(), 2);
    EXPECT_EQ(log.lastTerm(), 1);

    log.append(2, "cmd3");
    EXPECT_EQ(log.lastIndex(), 3);
    EXPECT_EQ(log.lastTerm(), 2);
}

TEST(RaftLog, TermAtReturnsCorrectTerms) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");

    EXPECT_EQ(log.termAt(1), 1);
    EXPECT_EQ(log.termAt(2), 1);
    EXPECT_EQ(log.termAt(3), 2);
    EXPECT_EQ(log.termAt(0), 0);
    EXPECT_EQ(log.termAt(4), 0);
}

TEST(RaftLog, AtReturnsCorrectEntry) {
    RaftLog log;
    Index idx1 = log.append(1, "cmd1");
    Index idx2 = log.append(1, "cmd2");
    Index idx3 = log.append(2, "cmd3");

    EXPECT_EQ(log.at(1).term, 1);
    EXPECT_EQ(log.at(1).command, "cmd1");
    EXPECT_EQ(log.at(2).term, 1);
    EXPECT_EQ(log.at(2).command, "cmd2");
    EXPECT_EQ(log.at(3).term, 2);
    EXPECT_EQ(log.at(3).command, "cmd3");
}

TEST(RaftLog, AtThrowsOutOfRange) {
    RaftLog log;
    log.append(1, "cmd1");

    EXPECT_THROW(log.at(0), std::out_of_range);
    EXPECT_THROW(log.at(2), std::out_of_range);
    EXPECT_THROW(log.at(-1), std::out_of_range);
}

// ============================================================================
// entriesFrom tests
// ============================================================================

TEST(RaftLog, EntriesFromReturnsRangeToEnd) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");

    auto entries = log.entriesFrom(2);
    EXPECT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].term, 1);
    EXPECT_EQ(entries[0].command, "cmd2");
    EXPECT_EQ(entries[1].term, 2);
    EXPECT_EQ(entries[1].command, "cmd3");
}

TEST(RaftLog, EntriesFromClampedToStart) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");

    // start < 1 should be treated as 1
    auto entries = log.entriesFrom(0);
    EXPECT_EQ(entries.size(), 3);
    EXPECT_EQ(entries[0].command, "cmd1");
}

TEST(RaftLog, EntriesFromBeyondLastIndexIsEmpty) {
    RaftLog log;
    log.append(1, "cmd1");

    auto entries = log.entriesFrom(2);
    EXPECT_EQ(entries.size(), 0);
}

TEST(RaftLog, EntriesFromEmptyLogIsEmpty) {
    RaftLog log;
    auto entries = log.entriesFrom(1);
    EXPECT_EQ(entries.size(), 0);
}

// ============================================================================
// truncateFrom tests
// ============================================================================

TEST(RaftLog, TruncateFromRemovesEntriesFromIndex) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");

    log.truncateFrom(2);
    EXPECT_EQ(log.lastIndex(), 1);
    EXPECT_EQ(log.lastTerm(), 1);
    EXPECT_EQ(log.at(1).command, "cmd1");
}

TEST(RaftLog, TruncateFromOneEmptiesLog) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");

    log.truncateFrom(1);
    EXPECT_EQ(log.lastIndex(), 0);
    EXPECT_EQ(log.lastTerm(), 0);
}

TEST(RaftLog, TruncateFromBeyondLastIndexIsNoOp) {
    RaftLog log;
    log.append(1, "cmd1");

    log.truncateFrom(100);
    EXPECT_EQ(log.lastIndex(), 1);
    EXPECT_EQ(log.at(1).command, "cmd1");
}

// ============================================================================
// commitIndex tests
// ============================================================================

TEST(RaftLog, CommitIndexStartsAtZero) {
    RaftLog log;
    EXPECT_EQ(log.commitIndex(), 0);
}

TEST(RaftLog, CommitIndexIsMonotonicNonDecreasing) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(1, "cmd3");

    log.setCommitIndex(2);
    EXPECT_EQ(log.commitIndex(), 2);

    // Try to set lower; should remain at 2
    log.setCommitIndex(1);
    EXPECT_EQ(log.commitIndex(), 2);
}

TEST(RaftLog, CommitIndexClampsToLastIndex) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");

    // Try to set commitIndex beyond lastIndex
    log.setCommitIndex(999);
    EXPECT_EQ(log.commitIndex(), 2);
}

TEST(RaftLog, CommitIndexAdvancesWithLog) {
    RaftLog log;
    log.append(1, "cmd1");
    log.setCommitIndex(1);
    EXPECT_EQ(log.commitIndex(), 1);

    log.append(1, "cmd2");
    log.setCommitIndex(2);
    EXPECT_EQ(log.commitIndex(), 2);
}

// ============================================================================
// Mixed scenario tests
// ============================================================================

TEST(RaftLog, MixedScenarioWithTruncate) {
    RaftLog log;

    // Append 5 entries with varying terms
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");
    log.append(2, "cmd4");
    log.append(3, "cmd5");

    EXPECT_EQ(log.lastIndex(), 5);
    EXPECT_EQ(log.termAt(3), 2);
    EXPECT_EQ(log.termAt(5), 3);

    // Truncate from index 3 (removes 3, 4, 5)
    log.truncateFrom(3);
    EXPECT_EQ(log.lastIndex(), 2);
    EXPECT_EQ(log.lastTerm(), 1);

    // Re-append with new term
    log.append(2, "cmd3_new");
    log.append(2, "cmd4_new");

    EXPECT_EQ(log.lastIndex(), 4);
    EXPECT_EQ(log.at(3).term, 2);
    EXPECT_EQ(log.at(3).command, "cmd3_new");
    EXPECT_EQ(log.at(4).term, 2);
    EXPECT_EQ(log.at(4).command, "cmd4_new");

    // Verify termAt is consistent
    EXPECT_EQ(log.termAt(1), 1);
    EXPECT_EQ(log.termAt(2), 1);
    EXPECT_EQ(log.termAt(3), 2);
    EXPECT_EQ(log.termAt(4), 2);
}

TEST(RaftLog, CommitIndexWithTruncateAndReappend) {
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(1, "cmd3");

    log.setCommitIndex(3);
    EXPECT_EQ(log.commitIndex(), 3);

    // Truncate entries 2 and 3
    log.truncateFrom(2);
    EXPECT_EQ(log.lastIndex(), 1);
    // commitIndex is still 3 (clamped to min(3, 1) = 1 by the setCommitIndex logic)
    // But wait: the existing logic clamps to lastIndex during the set.
    // Let's just verify the monotonicity: trying to set to 2 won't decrease from 3
    log.setCommitIndex(2);
    EXPECT_EQ(log.commitIndex(), 3);  // stays at max(3, min(2, 1)) = 3

    log.append(2, "cmd2_new");
    log.append(2, "cmd3_new");
    log.setCommitIndex(2);
    EXPECT_EQ(log.commitIndex(), 3);  // still monotonic non-decreasing
}
