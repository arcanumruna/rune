// TermLogTest.cpp — unit tests for rune::consensus::TermLog
// Faithful port of RecordingLog TERM-entry semantics.
// Each test uses a unique temp file and cleans up on success.

#include <gtest/gtest.h>
#include <rune/consensus/TermLog.h>

#include <cstdlib>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;
using namespace rune::consensus;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string tmpPath(const std::string& tag) {
    return (fs::temp_directory_path() / ("term_log_test_" + tag + ".bin")).string();
}

struct TempFile {
    std::string path;
    explicit TempFile(const std::string& tag) : path(tmpPath(tag)) {}
    ~TempFile() { fs::remove(path); }
};

// ---------------------------------------------------------------------------
// 1. Empty log invariants
// ---------------------------------------------------------------------------

TEST(TermLog, EmptyLog_LastTermEntryIsNullopt) {
    TempFile tf("empty_last");
    TermLog log(tf.path);
    EXPECT_FALSE(log.lastTermEntry().has_value());
}

TEST(TermLog, EmptyLog_GetTermEntryIsNullopt) {
    TempFile tf("empty_get");
    TermLog log(tf.path);
    EXPECT_FALSE(log.getTermEntry(0).has_value());
    EXPECT_FALSE(log.getTermEntry(99).has_value());
}

TEST(TermLog, EmptyLog_ValidTermEntriesIsEmpty) {
    TempFile tf("empty_valid");
    TermLog log(tf.path);
    EXPECT_TRUE(log.validTermEntries().empty());
}

TEST(TermLog, EmptyLog_NextLeadershipTermIdIsZero) {
    TempFile tf("empty_next");
    TermLog log(tf.path);
    EXPECT_EQ(log.nextLeadershipTermId(), 0);
}

// ---------------------------------------------------------------------------
// 2. appendTerm — single entry
// ---------------------------------------------------------------------------

TEST(TermLog, AppendTerm_EntryPresent) {
    TempFile tf("append_present");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);

    const auto opt = log.getTermEntry(0);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->recordingId,         10);
    EXPECT_EQ(opt->leadershipTermId,    0);
    EXPECT_EQ(opt->termBaseLogPosition, 0);
    EXPECT_EQ(opt->timestamp,           1000);
}

TEST(TermLog, AppendTerm_IsOpenLogPosition) {
    // An appended (uncommitted) term must have logPosition == kNullPosition.
    // RecordingLog.java:955: long logPosition = NULL_POSITION;
    TempFile tf("append_open");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);

    const auto opt = log.getTermEntry(0);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->logPosition, kNullPosition);
}

TEST(TermLog, AppendTerm_IsValid) {
    TempFile tf("append_valid");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);

    const auto opt = log.getTermEntry(0);
    ASSERT_TRUE(opt.has_value());
    EXPECT_TRUE(opt->isValid);
    EXPECT_EQ(opt->entryType, kEntryTypeTerm);
    EXPECT_EQ(opt->serviceId, kTermServiceId);
}

TEST(TermLog, AppendTerm_TermBaseLogPosition) {
    TempFile tf("append_base");
    TermLog log(tf.path);
    log.appendTerm(7, 3, 42000, 5000);

    const auto opt = log.getTermEntry(3);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->termBaseLogPosition, 42000);
}

// ---------------------------------------------------------------------------
// 3. commitLogPosition
// ---------------------------------------------------------------------------

TEST(TermLog, CommitLogPosition_ClosesEntry) {
    // RecordingLog.java:1085–1100: commitLogPosition sets logPosition on the entry.
    TempFile tf("commit_closes");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);
    log.commitLogPosition(0, 500);

    const auto opt = log.getTermEntry(0);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->logPosition, 500);
}

TEST(TermLog, CommitLogPosition_DoesNotChangeOtherFields) {
    TempFile tf("commit_other");
    TermLog log(tf.path);
    log.appendTerm(99, 2, 1024, 9999);
    log.commitLogPosition(2, 2048);

    const auto opt = log.getTermEntry(2);
    ASSERT_TRUE(opt.has_value());
    EXPECT_EQ(opt->recordingId,         99);
    EXPECT_EQ(opt->termBaseLogPosition, 1024);
    EXPECT_EQ(opt->timestamp,           9999);
    EXPECT_EQ(opt->logPosition,         2048);
    EXPECT_TRUE(opt->isValid);
}

// ---------------------------------------------------------------------------
// 4. Multiple terms
// ---------------------------------------------------------------------------

TEST(TermLog, MultipleTerms_LastTermEntryIsHighest) {
    TempFile tf("multi_last");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,   1000);
    log.commitLogPosition(0, 500);
    log.appendTerm(10, 1, 500, 2000);

    const auto last = log.lastTermEntry();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->leadershipTermId, 1);
}

TEST(TermLog, MultipleTerms_NextLeadershipTermId) {
    TempFile tf("multi_next");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,   1000);
    log.commitLogPosition(0, 500);
    log.appendTerm(10, 1, 500, 2000);

    EXPECT_EQ(log.nextLeadershipTermId(), 2);
}

TEST(TermLog, MultipleTerms_TermBaseLogPositionAccumulates) {
    TempFile tf("multi_base");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,    1000);
    log.commitLogPosition(0, 500);
    log.appendTerm(10, 1, 500,  2000);
    log.commitLogPosition(1, 1500);
    log.appendTerm(10, 2, 1500, 3000);

    const auto t2 = log.getTermEntry(2);
    ASSERT_TRUE(t2.has_value());
    EXPECT_EQ(t2->termBaseLogPosition, 1500);
    EXPECT_EQ(t2->logPosition, kNullPosition); // still open
}

TEST(TermLog, MultipleTerms_GetEachTerm) {
    TempFile tf("multi_get");
    TermLog log(tf.path);
    log.appendTerm(1, 0, 0,    100);
    log.appendTerm(2, 1, 1000, 200);
    log.appendTerm(3, 2, 2000, 300);

    EXPECT_EQ(log.getTermEntry(0)->recordingId, 1);
    EXPECT_EQ(log.getTermEntry(1)->recordingId, 2);
    EXPECT_EQ(log.getTermEntry(2)->recordingId, 3);
}

TEST(TermLog, MultipleTerms_ValidTermEntriesCount) {
    TempFile tf("multi_count");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,    1000);
    log.appendTerm(10, 1, 500,  2000);
    log.appendTerm(10, 2, 1500, 3000);

    EXPECT_EQ(log.validTermEntries().size(), 3u);
}

// ---------------------------------------------------------------------------
// 5. invalidateEntry
// ---------------------------------------------------------------------------

TEST(TermLog, InvalidateEntry_GetTermEntryReturnsNullopt) {
    TempFile tf("inv_get");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);
    log.invalidateEntry(0);

    EXPECT_FALSE(log.getTermEntry(0).has_value());
}

TEST(TermLog, InvalidateEntry_ExcludedFromValidTermEntries) {
    TempFile tf("inv_valid");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,    1000);
    log.appendTerm(10, 1, 500,  2000);
    log.appendTerm(10, 2, 1500, 3000);
    log.invalidateEntry(1);

    const auto valid = log.validTermEntries();
    ASSERT_EQ(valid.size(), 2u);
    EXPECT_EQ(valid[0].leadershipTermId, 0);
    EXPECT_EQ(valid[1].leadershipTermId, 2);
}

TEST(TermLog, InvalidateEntry_LastTermSkipsInvalidated) {
    TempFile tf("inv_last");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,    1000);
    log.appendTerm(10, 1, 500,  2000);
    log.appendTerm(10, 2, 1500, 3000);
    log.invalidateEntry(2);  // invalidate highest

    const auto last = log.lastTermEntry();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->leadershipTermId, 1);
}

TEST(TermLog, InvalidateEntry_NextLeadershipTermIdSkipsInvalidated) {
    TempFile tf("inv_next");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0,    1000);
    log.appendTerm(10, 1, 500,  2000);
    log.invalidateEntry(1);

    // After invalidation of term 1, last valid is term 0 → next = 1.
    EXPECT_EQ(log.nextLeadershipTermId(), 1);
}

TEST(TermLog, InvalidateNonExistentTerm_NoOp) {
    TempFile tf("inv_noop");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);
    // Should not throw
    EXPECT_NO_THROW(log.invalidateEntry(99));
    EXPECT_EQ(log.validTermEntries().size(), 1u);
}

// ---------------------------------------------------------------------------
// 6. Persistence across reopen
// ---------------------------------------------------------------------------

TEST(TermLog, PersistenceAcrossReopen_AllEntriesSurvive) {
    const std::string path = tmpPath("persist_all");

    {
        TermLog log(path);
        log.appendTerm(10, 0, 0,    1000);
        log.commitLogPosition(0, 500);
        log.appendTerm(10, 1, 500,  2000);
        log.commitLogPosition(1, 1500);
        log.appendTerm(10, 2, 1500, 3000);
    } // file closed / fd released

    {
        TermLog log(path);
        EXPECT_EQ(log.validTermEntries().size(), 3u);

        const auto t0 = log.getTermEntry(0);
        ASSERT_TRUE(t0.has_value());
        EXPECT_EQ(t0->logPosition,         500);
        EXPECT_EQ(t0->termBaseLogPosition, 0);

        const auto t1 = log.getTermEntry(1);
        ASSERT_TRUE(t1.has_value());
        EXPECT_EQ(t1->logPosition,         1500);
        EXPECT_EQ(t1->termBaseLogPosition, 500);

        const auto t2 = log.getTermEntry(2);
        ASSERT_TRUE(t2.has_value());
        EXPECT_EQ(t2->logPosition,         kNullPosition); // still open
        EXPECT_EQ(t2->termBaseLogPosition, 1500);
    }

    fs::remove(path);
}

TEST(TermLog, PersistenceAcrossReopen_LastTermCorrect) {
    const std::string path = tmpPath("persist_last");

    {
        TermLog log(path);
        log.appendTerm(5, 0, 0,   100);
        log.commitLogPosition(0, 200);
        log.appendTerm(5, 1, 200, 300);
    }

    {
        TermLog log(path);
        const auto last = log.lastTermEntry();
        ASSERT_TRUE(last.has_value());
        EXPECT_EQ(last->leadershipTermId, 1);
        EXPECT_EQ(last->logPosition, kNullPosition); // not committed
        EXPECT_EQ(log.nextLeadershipTermId(), 2);
    }

    fs::remove(path);
}

TEST(TermLog, PersistenceAcrossReopen_InvalidationSurvives) {
    const std::string path = tmpPath("persist_inv");

    {
        TermLog log(path);
        log.appendTerm(10, 0, 0,    1000);
        log.appendTerm(10, 1, 500,  2000);
        log.appendTerm(10, 2, 1500, 3000);
        log.invalidateEntry(1);
    }

    {
        TermLog log(path);
        EXPECT_FALSE(log.getTermEntry(1).has_value());
        EXPECT_EQ(log.validTermEntries().size(), 2u);
        const auto last = log.lastTermEntry();
        ASSERT_TRUE(last.has_value());
        EXPECT_EQ(last->leadershipTermId, 2);
    }

    fs::remove(path);
}

// ---------------------------------------------------------------------------
// 7. Edge cases
// ---------------------------------------------------------------------------

TEST(TermLog, GetTermEntry_NonExistentReturnsNullopt) {
    TempFile tf("edge_nonexist");
    TermLog log(tf.path);
    log.appendTerm(10, 5, 0, 1000);

    EXPECT_FALSE(log.getTermEntry(0).has_value());
    EXPECT_FALSE(log.getTermEntry(4).has_value());
    EXPECT_FALSE(log.getTermEntry(6).has_value());
    EXPECT_TRUE(log.getTermEntry(5).has_value());
}

TEST(TermLog, CommitLogPosition_IdempotentSameValue) {
    TempFile tf("edge_idempotent");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);
    log.commitLogPosition(0, 500);
    EXPECT_NO_THROW(log.commitLogPosition(0, 500)); // same value → no-op
    EXPECT_EQ(log.getTermEntry(0)->logPosition, 500);
}

TEST(TermLog, AppendTerm_DuplicateThrows) {
    TempFile tf("edge_dup");
    TermLog log(tf.path);
    log.appendTerm(10, 0, 0, 1000);
    EXPECT_THROW(log.appendTerm(10, 0, 0, 2000), std::runtime_error);
}

TEST(TermLog, SingleEntry_NextLeadershipTermId) {
    TempFile tf("single_next");
    TermLog log(tf.path);
    log.appendTerm(10, 7, 0, 1000);
    EXPECT_EQ(log.nextLeadershipTermId(), 8);
}

// ---------------------------------------------------------------------------
// 8. Out-of-order append regression tests
//    These tests would FAIL against the old sorted-insertion code because
//    commitLogPosition/invalidateEntry used idx*kRecordSize as the file offset
//    where idx was the sorted position, not the file-record position.
// ---------------------------------------------------------------------------

TEST(TermLog, OutOfOrderAppendCommitTargetsCorrectRecord) {
    // Append term=2 FIRST (file record 0), then term=0 SECOND (file record 1).
    // The old sorted-insertion code would store term=0 at entries_[0] and
    // term=2 at entries_[1], making commitLogPosition(0,…) patch file record 0
    // (which actually holds term=2) — the wrong record.
    const std::string path = tmpPath("ooo_commit");

    {
        TermLog log(path);
        log.appendTerm(1, 2, 0, 100); // term=2 appended first → file record 0
        log.appendTerm(2, 0, 0, 200); // term=0 appended second → file record 1

        log.commitLogPosition(0, 555); // must patch file record 1 (term=0)
        log.commitLogPosition(2, 999); // must patch file record 0 (term=2)

        // In-memory: each term's logPosition updated correctly.
        ASSERT_TRUE(log.getTermEntry(0).has_value());
        ASSERT_TRUE(log.getTermEntry(2).has_value());
        EXPECT_EQ(log.getTermEntry(0)->logPosition, 555);
        EXPECT_EQ(log.getTermEntry(2)->logPosition, 999);

        // lastTermEntry returns highest leadershipTermId (term=2), even though it was appended first.
        const auto last = log.lastTermEntry();
        ASSERT_TRUE(last.has_value());
        EXPECT_EQ(last->leadershipTermId, 2);
    }

    // Reopen: verify the on-disk file offsets were correct (commits hit the right records).
    {
        TermLog log(path);
        ASSERT_TRUE(log.getTermEntry(0).has_value());
        ASSERT_TRUE(log.getTermEntry(2).has_value());
        EXPECT_EQ(log.getTermEntry(0)->logPosition, 555);
        EXPECT_EQ(log.getTermEntry(2)->logPosition, 999);

        const auto last = log.lastTermEntry();
        ASSERT_TRUE(last.has_value());
        EXPECT_EQ(last->leadershipTermId, 2);
        EXPECT_EQ(log.nextLeadershipTermId(), 3);
    }

    fs::remove(path);
}

TEST(TermLog, OutOfOrderAppendInvalidateTargetsCorrectRecord) {
    // Same out-of-order setup: term=2 at file record 0, term=0 at file record 1.
    // invalidateEntry(2) must flip the invalid-flag bit on file record 0, not record 1.
    const std::string path = tmpPath("ooo_invalidate");

    {
        TermLog log(path);
        log.appendTerm(1, 2, 0, 100); // term=2 → file record 0
        log.appendTerm(2, 0, 0, 200); // term=0 → file record 1

        log.invalidateEntry(2); // must patch file record 0

        EXPECT_FALSE(log.getTermEntry(2).has_value()); // invalidated
        EXPECT_TRUE(log.getTermEntry(0).has_value());  // still valid
    }

    // Reopen: verify invalidation was persisted to the correct file record.
    {
        TermLog log(path);
        EXPECT_FALSE(log.getTermEntry(2).has_value()); // still invalid after reopen
        EXPECT_TRUE(log.getTermEntry(0).has_value());  // still valid after reopen
        EXPECT_EQ(log.validTermEntries().size(), 1u);
        EXPECT_EQ(log.validTermEntries()[0].leadershipTermId, 0);

        // lastTermEntry: only term=0 remains valid
        const auto last = log.lastTermEntry();
        ASSERT_TRUE(last.has_value());
        EXPECT_EQ(last->leadershipTermId, 0);
    }

    fs::remove(path);
}
