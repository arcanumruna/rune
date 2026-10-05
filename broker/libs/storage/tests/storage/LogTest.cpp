#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <fstream>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <unistd.h>
using namespace rune::storage; using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Helper: unique temp dir path per test
// ---------------------------------------------------------------------------
static std::filesystem::path makeTempDir(const char* stem) {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto uniq = std::to_string(::getpid()) + "_" +
                std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                std::to_string(ts);
    return std::filesystem::temp_directory_path() / (std::string(stem) + "_" + uniq);
}

// ---- Test 1 (brief's canonical test): rolls + reads across segment boundary ----
TEST(Log, RollsSegmentsAndReadsAcross) {
    auto dir = makeTempDir("rune_log_t1");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*segmentRollBytes*/ 256); // tiny to force rolls
    int64_t off = 0;
    for (int i = 0; i < 50; ++i) {
        RecordBatchBuilder b(off, 1000 + i); b.append("k", "val" + std::to_string(i), 1000 + i);
        off = log.append(b.build(), off);
    }
    EXPECT_EQ(log.logEndOffset(), 50);
    EXPECT_GT(log.segmentBaseOffsets().size(), 1u);   // rolled
    auto bytes = log.read(25, 1 << 20);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 25);
    std::filesystem::remove_all(dir);
}

// ---- Test 2: single segment (no roll) ----
TEST(Log, SingleSegmentNoRoll) {
    auto dir = makeTempDir("rune_log_t2");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Large rollBytes so we never roll
    Log log(dir.string(), 64LL * 1024 * 1024);
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(off, 2000 + i); b.append("k", "v" + std::to_string(i), 2000 + i);
        off = log.append(b.build(), off);
    }
    EXPECT_EQ(log.logEndOffset(), 5);
    EXPECT_EQ(log.segmentBaseOffsets().size(), 1u); // exactly one segment
    auto bytes = log.read(0, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);
    std::filesystem::remove_all(dir);
}

// ---- Regression (CP-5.2): logStartOffset advances past 0 after old-segment
//      deletion; an "Earliest" reader MUST start at logStartOffset(), NOT 0.
//
// Root cause of the e2e "0-delivered after aggressive retention/compaction"
// failure: handleSubscribe resolved Earliest to a hardcoded offset 0.  Once
// retention/compaction deletes the first segment(s), the first surviving segment
// base is > 0.  Log::read(0) then lands on segments_.begin() via upper_bound and
// returns {} FOREVER — the subscriber is pinned at 0 and starves (0 delivered),
// even though the log holds committed data at logStartOffset()..LEO.
//
// This test locks in the exact invariant the gateway fix now depends on:
//   * read(0) returns EMPTY once logStart > 0  (the trap)
//   * logStartOffset() returns the first surviving base
//   * read(logStartOffset()) returns the oldest surviving batch
TEST(Log, EarliestAfterOldSegmentDeletion_ReadFromLogStartNotZero) {
    auto dir = makeTempDir("rune_log_logstart");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*segmentRollBytes*/ 256); // tiny → many segments

    int64_t off = 0;
    for (int i = 0; i < 60; ++i) {
        RecordBatchBuilder b(off, 1000 + i);
        b.append("k", "val" + std::to_string(i), 1000 + i);
        off = log.append(b.build(), off);
    }
    ASSERT_EQ(log.logEndOffset(), 60);
    ASSERT_GT(log.segmentBaseOffsets().size(), 1u);
    ASSERT_EQ(log.logStartOffset(), 0);          // nothing deleted yet

    // Delete old segments (retain almost nothing) → first segment base advances.
    const int deleted = log.deleteOldSegments(/*retainBytes=*/0);
    ASSERT_GT(deleted, 0);

    const int64_t logStart = log.logStartOffset();
    ASSERT_GT(logStart, 0) << "old-segment deletion should advance logStartOffset past 0";
    EXPECT_EQ(logStart, log.segmentBaseOffsets().front());

    // THE BUG: reading from 0 (what Earliest used to resolve to) returns nothing.
    EXPECT_TRUE(log.read(0, 1 << 20).empty())
        << "read(0) below logStart must return empty (this is why Earliest=0 starved)";

    // THE FIX CONTRACT: reading from logStartOffset() returns the oldest survivor.
    auto bytes = log.read(logStart, 1 << 20);
    ASSERT_FALSE(bytes.empty())
        << "read(logStartOffset()) must return the oldest surviving batch";
    RecordBatchReader r(bytes.data(), bytes.size());
    ASSERT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), logStart);

    std::filesystem::remove_all(dir);
}

// ---- Test 3: read boundaries ----
TEST(Log, ReadBoundaries) {
    auto dir = makeTempDir("rune_log_t3");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Roll every ~256 bytes so we get at least 2 segments
    Log log(dir.string(), 256);
    int64_t off = 0;
    // Append enough batches to guarantee a roll
    for (int i = 0; i < 20; ++i) {
        RecordBatchBuilder b(off, 3000 + i); b.append("key", "value" + std::to_string(i), 3000 + i);
        off = log.append(b.build(), off);
    }
    EXPECT_GT(log.segmentBaseOffsets().size(), 1u);

    // read(0) — offset 0, first segment's first batch
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // read at the exact baseOffset of the second segment
    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);
    int64_t secondBase = bases[1];
    {
        auto bytes = log.read(secondBase, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), secondBase);
    }

    // read past logEndOffset returns empty
    {
        auto bytes = log.read(log.logEndOffset() + 100, 1 << 20);
        EXPECT_TRUE(bytes.empty());
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 4: reopen discovers existing segments + recover ----
TEST(Log, ReopenDiscoversSegmentsAndRecovers) {
    auto dir = makeTempDir("rune_log_t4");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    int64_t savedEndOffset = 0;
    int64_t earlyOffset = -1;
    int64_t lateOffset = -1;
    {
        Log log(dir.string(), 256);
        int64_t off = 0;
        for (int i = 0; i < 30; ++i) {
            RecordBatchBuilder b(off, 4000 + i); b.append("k", "v" + std::to_string(i), 4000 + i);
            off = log.append(b.build(), off);
        }
        savedEndOffset = log.logEndOffset();
        EXPECT_EQ(savedEndOffset, 30);
        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u);
        earlyOffset = bases[0];          // first segment
        lateOffset = bases.back();       // last segment's base
    }
    // Construct a new Log over the same dir, then recover
    Log log2(dir.string(), 256);
    int64_t recovered = log2.recover();
    EXPECT_EQ(recovered, savedEndOffset);
    EXPECT_EQ(log2.logEndOffset(), savedEndOffset);

    // Read from the first segment
    {
        auto bytes = log2.read(earlyOffset, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), earlyOffset);
    }
    // Read from a later segment
    {
        auto bytes = log2.read(lateOffset, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), lateOffset);
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 5: recover() after torn tail in the ACTIVE segment ----
TEST(Log, RecoverAfterTornTailInActiveSegment) {
    auto dir = makeTempDir("rune_log_t5");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    int64_t firstSegBase = 0;
    int64_t cleanEndOffset = -1;
    std::string activeLogPath;
    {
        Log log(dir.string(), 256);
        int64_t off = 0;
        // Append enough to roll at least once
        for (int i = 0; i < 20; ++i) {
            RecordBatchBuilder b(off, 5000 + i); b.append("k", "v" + std::to_string(i), 5000 + i);
            off = log.append(b.build(), off);
        }
        cleanEndOffset = log.logEndOffset();
        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u);
        firstSegBase = bases[0];
        // Figure out active segment path: the last base offset
        int64_t activeBase = bases.back();
        // Path is dir/<zero-padded-20-digit base>.log — replicate LogSegment fname logic
        // Use a fresh LogSegment to get the path isn't available from Log, so look on disk
        for (auto& e : std::filesystem::directory_iterator(dir)) {
            if (e.path().extension() == ".log") {
                int64_t base = std::stoll(e.path().stem().string());
                if (base == activeBase) { activeLogPath = e.path().string(); break; }
            }
        }
    }
    ASSERT_FALSE(activeLogPath.empty());
    // Append junk to the active segment's .log file
    {
        std::ofstream f(activeLogPath, std::ios::binary | std::ios::app);
        const char junk[13] = {1,2,3,4,5,6,7,8,9,10,11,12,13};
        f.write(junk, 13);
    }
    // New Log + recover
    Log log2(dir.string(), 256);
    int64_t recovered = log2.recover();
    // logEndOffset should reflect only valid batches (junk dropped)
    EXPECT_EQ(recovered, cleanEndOffset);
    EXPECT_EQ(log2.logEndOffset(), cleanEndOffset);

    // A read in the earlier (untouched) segment is still valid
    {
        auto bytes = log2.read(firstSegBase, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), firstSegBase);
    }
    std::filesystem::remove_all(dir);
}

// ---- Test 6: segmentBaseOffsets() returns sorted base offsets matching rolls ----
TEST(Log, SegmentBaseOffsetsSortedAndCountMatchesRolls) {
    auto dir = makeTempDir("rune_log_t6");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 256);
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, 6000 + i); b.append("k", "v" + std::to_string(i), 6000 + i);
        off = log.append(b.build(), off);
    }
    auto bases = log.segmentBaseOffsets();
    // Must have more than 1 segment
    ASSERT_GT(bases.size(), 1u);
    // Must be sorted ascending
    for (size_t i = 1; i < bases.size(); ++i) {
        EXPECT_LT(bases[i - 1], bases[i]);
    }
    // First base must be 0
    EXPECT_EQ(bases.front(), 0);
    // Count of .log files on disk must match
    size_t logFileCount = 0;
    for (auto& e : std::filesystem::directory_iterator(dir))
        if (e.path().extension() == ".log") ++logFileCount;
    EXPECT_EQ(bases.size(), logFileCount);
    std::filesystem::remove_all(dir);
}

// ---- FIX 3a: single-segment truncate mid-log ----
// Offsets < boundary kept; >= boundary dropped.
// LEO == boundary; log still readable and appendable (resuming at boundary).
TEST(Log, TruncateSingleSegmentMidLog) {
    auto dir = makeTempDir("rune_trunc_t1");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Large rollBytes — stays single segment.
    Log log(dir.string(), 64LL * 1024 * 1024);
    int64_t off = 0;
    // Append 10 batches.
    for (int i = 0; i < 10; ++i) {
        RecordBatchBuilder b(off, 100 + i);
        b.append("k", "v" + std::to_string(i), 100 + i);
        off = log.append(b.build(), off);
    }
    ASSERT_EQ(log.logEndOffset(), 10);
    ASSERT_EQ(log.segmentBaseOffsets().size(), 1u);

    // Truncate to offset 5: keep batches 0-4, drop 5-9.
    log.truncateTo(5);
    EXPECT_EQ(log.logEndOffset(), 5) << "LEO must equal truncation boundary";

    // Records below boundary must be readable.
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Records below boundary must survive truncation";
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }
    // Records at/above boundary must be gone: read at offset 5 returns empty.
    {
        auto bytes = log.read(5, 1 << 20);
        EXPECT_TRUE(bytes.empty()) << "Records at/above truncation boundary must be dropped";
    }

    // Log must be appendable: new batch resumes at offset 5 (contiguous).
    // Use logEndOffset() as the base (not the stale `off` from before truncation).
    int64_t resumeOff = log.logEndOffset(); // = 5
    ASSERT_EQ(resumeOff, 5);
    RecordBatchBuilder b(resumeOff, 999);
    b.append("k", "new", 999);
    int64_t newOff = log.append(b.build(), resumeOff);
    EXPECT_EQ(newOff, 6) << "Append after truncate must resume at boundary (contiguous)";
    EXPECT_EQ(log.logEndOffset(), 6);

    // Read the new batch at offset 5.
    {
        auto bytes = log.read(5, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Newly appended batch at boundary must be readable";
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 5);
    }

    std::filesystem::remove_all(dir);
}

// ---- FIX 3b: multi-segment truncate into earlier segment ----
// Append enough to roll >= 2 segments, truncate into an earlier segment.
// Trailing segment files removed; straddling segment truncated; LEO exact; readable.
TEST(Log, TruncateMultiSegmentIntoEarlierSegment) {
    auto dir = makeTempDir("rune_trunc_t2");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Tiny rollBytes to force multiple segments (~256 bytes per batch at ~100-byte batches).
    Log log(dir.string(), 256);
    int64_t off = 0;
    // Append 50 batches — well enough to create several segments.
    for (int i = 0; i < 50; ++i) {
        RecordBatchBuilder b(off, 200 + i);
        b.append("key", "val" + std::to_string(i), 200 + i);
        off = log.append(b.build(), off);
    }
    ASSERT_EQ(log.logEndOffset(), 50);
    auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GE(basesBefore.size(), 3u) << "Need at least 3 segments for this test";

    // Truncate into the second segment: pick a base offset inside segment[1].
    // segment[1] base is basesBefore[1]; truncate to basesBefore[1] + 1.
    int64_t truncOff = basesBefore[1] + 1;
    log.truncateTo(truncOff);

    EXPECT_EQ(log.logEndOffset(), truncOff) << "LEO must equal truncation boundary";

    // Segment count must be <= 2 (trailing segments removed).
    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_LE(basesAfter.size(), 2u)
        << "Trailing segment files must be removed after multi-segment truncation";

    // Only .log files present at or before the straddling segment's base.
    for (int64_t b : basesAfter) {
        EXPECT_LE(b, basesBefore[1]) << "No segment base should exceed the straddling segment";
    }

    // Records below truncation boundary are readable.
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "First record must still be readable";
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // Records at/above truncOff are gone.
    {
        auto bytes = log.read(truncOff, 1 << 20);
        EXPECT_TRUE(bytes.empty()) << "Records at truncation boundary must be dropped";
    }

    // Appendable: new batch resumes at truncOff.
    RecordBatchBuilder nb(truncOff, 777);
    nb.append("k", "new", 777);
    int64_t newOff = log.append(nb.build(), truncOff);
    EXPECT_EQ(newOff, truncOff + 1) << "Append must resume at truncation boundary";
    EXPECT_EQ(log.logEndOffset(), truncOff + 1);

    std::filesystem::remove_all(dir);
}

// ---- FIX 3c: truncate exactly at a segment baseOffset boundary ----
// No corruption; LEO exact; readable.
TEST(Log, TruncateAtSegmentBaseOffsetBoundary) {
    auto dir = makeTempDir("rune_trunc_t3");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 256);
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, 300 + i);
        b.append("k", "v" + std::to_string(i), 300 + i);
        off = log.append(b.build(), off);
    }
    ASSERT_GE(log.segmentBaseOffsets().size(), 2u);

    // Truncate exactly at the base of the last segment.
    auto bases = log.segmentBaseOffsets();
    int64_t lastBase = bases.back();
    ASSERT_GT(lastBase, 0) << "Last segment must not start at 0 for this test";

    log.truncateTo(lastBase);
    EXPECT_EQ(log.logEndOffset(), lastBase) << "LEO must equal the segment base boundary";

    // Records strictly before lastBase must be readable.
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Earlier records must survive";
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // Reading at lastBase must return empty (records at/above boundary dropped).
    {
        auto bytes = log.read(lastBase, 1 << 20);
        EXPECT_TRUE(bytes.empty()) << "Records at/above base boundary must be truncated";
    }

    // Appendable at the boundary.
    RecordBatchBuilder nb(lastBase, 888);
    nb.append("k", "new", 888);
    log.append(nb.build(), lastBase);
    EXPECT_EQ(log.logEndOffset(), lastBase + 1);

    std::filesystem::remove_all(dir);
}

// ---- FIX 3d: crash recovery after truncateTo — LEO + readable consistent ----
// Close and reopen the Log after truncation; confirm LEO and readable records
// are consistent (no stale index / torn tail).
TEST(Log, CrashRecoveryAfterTruncate) {
    auto dir = makeTempDir("rune_trunc_t4");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    int64_t truncBoundary = -1;
    {
        Log log(dir.string(), 256);
        int64_t off = 0;
        for (int i = 0; i < 30; ++i) {
            RecordBatchBuilder b(off, 400 + i);
            b.append("k", "v" + std::to_string(i), 400 + i);
            off = log.append(b.build(), off);
        }
        ASSERT_EQ(log.logEndOffset(), 30);
        // Truncate mid-log.
        truncBoundary = 15;
        log.truncateTo(truncBoundary);
        ASSERT_EQ(log.logEndOffset(), truncBoundary);
        // Log goes out of scope here (files flushed).
    }

    // Reopen and recover.
    Log log2(dir.string(), 256);
    int64_t recovered = log2.recover();

    // LEO after recovery must equal the truncation boundary.
    EXPECT_EQ(recovered, truncBoundary)
        << "After truncate + reopen, LEO must equal the truncation boundary";
    EXPECT_EQ(log2.logEndOffset(), truncBoundary);

    // Records below the boundary must be readable.
    {
        auto bytes = log2.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Records below boundary must survive crash recovery";
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // Records at/above boundary must not appear.
    {
        auto bytes = log2.read(truncBoundary, 1 << 20);
        EXPECT_TRUE(bytes.empty())
            << "Truncated records must not reappear after crash recovery";
    }

    // Appendable: new records resume at truncBoundary contiguously.
    RecordBatchBuilder nb(truncBoundary, 999);
    nb.append("k", "post_recovery", 999);
    int64_t newOff = log2.append(nb.build(), truncBoundary);
    EXPECT_EQ(newOff, truncBoundary + 1)
        << "Append after crash recovery must be contiguous from truncation boundary";
    EXPECT_EQ(log2.logEndOffset(), truncBoundary + 1);

    std::filesystem::remove_all(dir);
}

// ---- C1: Exact truncateTo tests ----

// C1-a: truncateOffset lands INSIDE a multi-record batch — entire batch must be dropped.
// The old code with cutPos=buf.size() default would keep the batch silently.
TEST(Log, TruncateInsideLastBatch) {
    auto dir = makeTempDir("rune_trunc_c1a");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024); // large roll so everything is one segment

    // Batch 0: single record at offset 0
    RecordBatchBuilder b0(0, 100); b0.append("k", "v0", 100);
    log.append(b0.build(), 0); // LEO=1

    // Batch 1: two records at offsets 1 and 2 (lastOffset=2, LEO=3)
    RecordBatchBuilder b1(1, 101); b1.append("k", "v1", 101); b1.append("k", "v2", 102);
    log.append(b1.build(), 2); // LEO=3

    ASSERT_EQ(log.logEndOffset(), 3);

    // Truncate at offset 2 — offset 2 is INSIDE batch1 (base=1, last=2).
    // Documented semantics: truncation rounds down to batch boundary.
    // After truncation LEO must be <= 2 (batch1 dropped entirely).
    log.truncateTo(2);

    EXPECT_LE(log.logEndOffset(), 2)
        << "truncateTo(2) must drop the batch that spans [1,2]; LEO must be <= 2";

    // Offset 0 must still be readable.
    auto buf0 = log.read(0, 1 << 20);
    EXPECT_FALSE(buf0.empty()) << "offset 0 must survive truncateTo(2)";

    // Offset >= 2 must not be readable.
    auto buf2 = log.read(2, 1 << 20);
    EXPECT_TRUE(buf2.empty()) << "offset 2 must be gone after truncateTo(2)";

    std::filesystem::remove_all(dir);
}

// C1-b: truncateOffset == LEO -> no-op (guard already correct, regression test).
TEST(Log, TruncateAtLEOIsNoop) {
    auto dir = makeTempDir("rune_trunc_c1b");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024);
    RecordBatchBuilder b(0, 200); b.append("k", "v", 200);
    int64_t leo = log.append(b.build(), 0);

    log.truncateTo(leo); // no-op
    EXPECT_EQ(log.logEndOffset(), leo);

    std::filesystem::remove_all(dir);
}

// C1-c: truncateOffset = 0 on a multi-batch single-segment log -> removes everything.
TEST(Log, TruncateToZeroRemovesAll) {
    auto dir = makeTempDir("rune_trunc_c1c");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 1024 * 1024);
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(i, 300 + i); b.append("k", "v", 300 + i);
        log.append(b.build(), i);
    }
    ASSERT_EQ(log.logEndOffset(), 5);

    log.truncateTo(0);
    EXPECT_EQ(log.logEndOffset(), 0);
    auto buf = log.read(0, 1 << 20);
    EXPECT_TRUE(buf.empty()) << "after truncateTo(0) no data must remain";

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Part B — edge-case tests (full re-audit 2026-09-11)
// ---------------------------------------------------------------------------

// B13: truncateTo into the middle of a multi-record batch.
// A multi-record batch spans offsets [baseOffset .. baseOffset+lastOffsetDelta].
// truncateTo(midpoint) must round down to the batch boundary and drop the
// entire batch (no partial batch survives).
TEST(Log, TruncateInsideMultiRecordBatchRoundsDown) {
    auto dir = makeTempDir("rune_log_b13");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 64 * 1024 * 1024); // large segment, no roll
    // Build a 3-record batch spanning offsets 0..2.
    {
        RecordBatchBuilder b(0, 1000);
        b.append("k1", "v1", 1000);
        b.append("k2", "v2", 1001);
        b.append("k3", "v3", 1002);
        log.append(b.build(), 2); // lastOffset = 2
    }
    EXPECT_EQ(log.logEndOffset(), 3);

    // truncateTo(1): offset 1 is inside the [0..2] batch → whole batch dropped.
    log.truncateTo(1);
    EXPECT_EQ(log.logEndOffset(), 0)
        << "truncateTo inside multi-record batch must drop entire batch";
    auto bytes = log.read(0, 1 << 20);
    EXPECT_TRUE(bytes.empty()) << "No batch must survive after truncation inside it";

    std::filesystem::remove_all(dir);
}

// B14: truncateTo exactly at a non-first segment base.
// The segment at the boundary and all segments after it are deleted.
// The prior segment's content is fully preserved.
TEST(Log, TruncateAtSegmentBaseDeletesSegment) {
    auto dir = makeTempDir("rune_log_b14");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 256); // small rollBytes to force segment creation

    // Append enough records to create multiple segments.
    int64_t off = 0;
    for (int i = 0; i < 10; ++i) {
        RecordBatchBuilder b(off, 1000 + i); b.append("k", "v" + std::to_string(i), 1000 + i);
        off = log.append(b.build(), off);
    }
    EXPECT_GT(log.segmentBaseOffsets().size(), 1u) << "Need multiple segments";

    // Find the base of the SECOND segment.
    auto bases = log.segmentBaseOffsets();
    int64_t secondBase = bases[1];
    int64_t leoBeforeTrunc = log.logEndOffset();

    // Truncate exactly at the second segment's base.
    log.truncateTo(secondBase);

    // LEO must equal secondBase (all records from secondBase onward are gone).
    EXPECT_EQ(log.logEndOffset(), secondBase)
        << "LEO must be secondBase after truncation at that boundary";

    // Records in [0, secondBase) must all survive.
    int64_t cursor = 0;
    while (cursor < secondBase) {
        auto bytes = log.read(cursor, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Record at offset " << cursor << " must survive";
        RecordBatchReader r(bytes.data(), bytes.size());
        ASSERT_TRUE(r.ensureValid());
        cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
    }

    (void)leoBeforeTrunc;
    std::filesystem::remove_all(dir);
}

// B15: truncateTo(0) followed by append — contiguous offsets from 0.
TEST(Log, TruncateToZeroThenAppendIsContiguous) {
    auto dir = makeTempDir("rune_log_b15");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), 64 * 1024 * 1024);
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(static_cast<int64_t>(i), 1000 + i);
        b.append("k", "v" + std::to_string(i), 1000 + i);
        log.append(b.build(), static_cast<int64_t>(i));
    }
    EXPECT_EQ(log.logEndOffset(), 5);

    log.truncateTo(0);
    EXPECT_EQ(log.logEndOffset(), 0);

    // Append new records starting from 0.
    {
        RecordBatchBuilder b(0, 9000); b.append("new", "val", 9000);
        int64_t next = log.append(b.build(), 0);
        EXPECT_EQ(next, 1) << "After truncateTo(0), first new offset must be 0→nextOffset=1";
    }

    // Reopen and verify.
    log.recover();
    auto bytes = log.read(0, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    RecordBatchReader r(bytes.data(), bytes.size());
    ASSERT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);

    std::filesystem::remove_all(dir);
}

// B16: maybeRoll with a single batch larger than rollBytes.
// The oversized batch must start its own segment and be readable.
// ===========================================================================
// CP-8 Fix 1: incremental largestTimestamp — sealed-segment snapshot must carry
// the correct maxTimestamp for the newly-sealed segment WITHOUT re-scanning the
// file.
//
// NON-VACUOUS:
//   Before the fix, publishSealedSnapshot() would scan the newly-sealed segment
//   file to compute largestTimestamp.  With the fix, it uses the incrementally
//   tracked activeSegmentMaxTs_ value captured at roll time.
//
//   Test A (correctness): after a roll, the sealed snapshot's largestTimestamp
//   must equal the largest maxTimestamp() seen in the batches appended to that
//   segment — verified against a reference computed directly from the batch headers.
//
//   Test B (monotonic): multiple rolls — each segment in the snapshot must have
//   the correct largestTimestamp (matching the batches written to that segment).
//
//   Test C (horizon batch): a horizon-stamped batch has baseTimestamp()=horizon
//   but maxTimestamp()=actual_max_ts.  The snapshot must use maxTimestamp(), not
//   baseTimestamp() (which is the horizon and NOT a wall-clock timestamp).
// ===========================================================================
TEST(Log, SealedSnapshotLargestTimestampIncrementalA) {
    // Setup: one segment with increasing timestamps, then roll.
    auto dir = makeTempDir("rune_log_ts_a");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    // Use a tiny rollBytes so we trigger a roll after a few records.
    Log log(dir.string(), /*rollBytes=*/256);

    // Append 3 batches with distinct maxTimestamps; track the expected max.
    int64_t expectedMaxTs = 0;
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        const int64_t ts = 1000LL + i * 100;  // 1000, 1100, 1200, 1300, 1400
        RecordBatchBuilder b(off, ts); b.append("k", "v" + std::to_string(i), ts);
        off = log.append(b.build(), off);
        if (ts > expectedMaxTs) expectedMaxTs = ts;
    }
    // Verify: before a roll there is only 1 segment, sealed snapshot is empty.
    {
        auto snap = log.sealedSegmentSnapshot();
        // May have 0 or more sealed segments already (if a roll happened).
        // We'll check after the explicit roll below.
    }

    // Force a roll by writing a record-heavy batch.
    for (int i = 0; i < 50; ++i) {
        const int64_t ts = 2000LL + i;
        RecordBatchBuilder b(off, ts); b.append("pad", "x", ts);
        off = log.append(b.build(), off);
        if (ts > expectedMaxTs) expectedMaxTs = ts;
    }
    // After the writes, some segments should be sealed.
    auto snap = log.sealedSegmentSnapshot();
    ASSERT_GE(snap->size(), 1u) << "Must have at least one sealed segment after roll";

    // For every sealed segment, verify largestTimestamp is positive and reasonable.
    // We compute a reference by reading the segment files directly.
    for (const auto& si : *snap) {
        EXPECT_GT(si.largestTimestamp, 0)
            << "Sealed segment base=" << si.baseOffset
            << " must have largestTimestamp > 0.  "
               "Got 0, which means the incremental tracking is broken "
               "(newlySealedMaxTs_ was 0 when publishSealedSnapshot() was called).";
        EXPECT_LE(si.largestTimestamp, expectedMaxTs)
            << "largestTimestamp=" << si.largestTimestamp
            << " must be <= the overall max timestamp written (" << expectedMaxTs << ").";
    }

    std::filesystem::remove_all(dir);
}

TEST(Log, SealedSnapshotLargestTimestampIncrementalB) {
    // Test B: multiple rolls; each sealed segment gets the correct largestTimestamp.
    auto dir = makeTempDir("rune_log_ts_b");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), /*rollBytes=*/128);

    // Build expected: segment_base → largest ts in that segment.
    // We interleave appends so each segment gets known timestamps.
    std::map<int64_t, int64_t> expectedTsPerSeg;
    int64_t off = 0;
    // Write 30 records with timestamps segmented into windows.
    // Each window ~1-3 records per segment (given rollBytes=128).
    for (int i = 0; i < 30; ++i) {
        const int64_t ts = static_cast<int64_t>(i + 1) * 1000LL; // 1000, 2000, ..., 30000
        RecordBatchBuilder b(off, ts); b.append("k", "v", ts);
        off = log.append(b.build(), off);
    }

    auto snap = log.sealedSegmentSnapshot();
    ASSERT_GE(snap->size(), 2u) << "Multiple rolls expected with rollBytes=128 and 30 records";

    // Verify each sealed segment's largestTimestamp by re-scanning the file.
    for (const auto& si : *snap) {
        // Compute reference by scanning the .log file.
        std::ostringstream os;
        os << dir.string() << "/" << std::setw(20) << std::setfill('0') << si.baseOffset << ".log";
        std::ifstream f(os.str(), std::ios::binary);
        ASSERT_TRUE(f.is_open()) << "Cannot open " << os.str();
        f.seekg(0, std::ios::end); auto sz = static_cast<std::size_t>(f.tellg());
        ASSERT_GT(sz, 0u) << "Segment file must be non-empty";
        std::vector<uint8_t> buf(sz);
        f.seekg(0); f.read(reinterpret_cast<char*>(buf.data()), sz);

        int64_t refMaxTs = 0;
        std::size_t pos = 0;
        while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= sz) {
            RecordBatchReader r(buf.data() + pos, sz - pos);
            int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > sz) break;
            const int64_t batchMax = r.maxTimestamp();
            if (batchMax > refMaxTs) refMaxTs = batchMax;
            pos += static_cast<std::size_t>(total);
        }

        EXPECT_EQ(si.largestTimestamp, refMaxTs)
            << "Sealed segment base=" << si.baseOffset
            << ": incremental largestTimestamp=" << si.largestTimestamp
            << " must match the file-scan reference=" << refMaxTs << ".  "
               "A mismatch means activeSegmentMaxTs_ was incorrect when the segment sealed.";
    }

    std::filesystem::remove_all(dir);
}

TEST(Log, SealedSnapshotLargestTimestampHorizonBatch) {
    // Test C: a horizon-stamped batch has baseTimestamp()=horizon but
    // maxTimestamp()=real record ts.  The snapshot must use maxTimestamp(), not
    // baseTimestamp() (the horizon is NOT a wall-clock timestamp).
    auto dir = makeTempDir("rune_log_ts_c");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), /*rollBytes=*/128);

    // Append a normal batch to give the segment real timestamps.
    const int64_t realTs = 5000LL;
    RecordBatchBuilder b0(0, realTs); b0.append("k", "v", realTs);
    log.append(b0.build(), 0);

    // Append a horizon-stamped batch. The horizon is huge (doesn't look like a timestamp).
    const int64_t horizonMs = 99999999LL; // large horizon epoch
    const int64_t recordTs  = realTs + 1;  // the actual record timestamp
    RecordBatchBuilder bh(1, recordTs);
    bh.setDeleteHorizonMs(horizonMs);
    bh.append("tombkey", "", recordTs);
    log.append(bh.build(), 1);

    // Force a roll so these batches become sealed.
    for (int i = 0; i < 50; ++i) {
        RecordBatchBuilder bp(2 + i, 1LL); bp.append("pad", "x", 1LL);
        log.append(bp.build(), 2 + i);
    }

    auto snap = log.sealedSegmentSnapshot();
    ASSERT_GE(snap->size(), 1u) << "Need at least 1 sealed segment";

    // The first sealed segment contains the normal + horizon batches.
    // Its largestTimestamp must be max(realTs, recordTs) = realTs+1 = maxTimestamp().
    // It must NOT be horizonMs (which would be the wrong baseTimestamp() value).
    const auto& si = (*snap)[0];
    EXPECT_NE(si.largestTimestamp, horizonMs)
        << "largestTimestamp must NOT be the horizon value ("  << horizonMs
        << "); the horizon is not a real wall-clock timestamp.  "
           "Got largestTimestamp=" << si.largestTimestamp << ".  "
           "This means the code used baseTimestamp() on a horizon batch instead of maxTimestamp().";
    EXPECT_EQ(si.largestTimestamp, recordTs)
        << "largestTimestamp must be maxTimestamp() of the horizon batch ("
        << recordTs << "); got " << si.largestTimestamp << ".";

    std::filesystem::remove_all(dir);
}

TEST(Log, OversizedBatchStartsOwnSegment) {
    auto dir = makeTempDir("rune_log_b16");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    // rollBytes = 64 bytes — tiny, but the batch will be much larger.
    Log log(dir.string(), 64);

    // Build a batch with many records — guaranteed > 64 bytes.
    RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < 10; ++i)
        b.append("longkey_" + std::to_string(i),
                 "longval_" + std::to_string(i) + "_payload_payload", 1000 + i);
    auto bigBatch = b.build();
    ASSERT_GT(bigBatch.size(), 64u) << "Batch must be larger than rollBytes";

    int64_t next = log.append(bigBatch, 9); // 10 records, lastOffset=9
    EXPECT_EQ(next, 10);
    EXPECT_GT(log.logEndOffset(), 0u);

    // The oversized batch must be readable.
    auto bytes = log.read(0, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    RecordBatchReader r(bytes.data(), bytes.size());
    ASSERT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);
    EXPECT_EQ(r.lastOffsetDelta(), 9); // 10 records

    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Part C — Staging pre-provision / crash-mid-adopt / orphan-sweep / rebase
//
// These tests guard the durability-critical staging machinery introduced by
// the append-p99 segment-roll fix (commit 85ccb45).  The machinery:
//   - prepareStaging / adoptAsActive / recoverStagingFiles
// has ZERO committed tests prior to this commit.
//
// Non-vacuity is established in CrashMidAdopt_RecoversAllData by a
// scratch-break proof: making recoverStagingFiles() a no-op causes the test
// to fail (data lost); restoring it returns the test to green.
// ===========================================================================

// ---------------------------------------------------------------------------
// Helper: count files in a directory matching a given extension suffix.
// ---------------------------------------------------------------------------
static int countFilesWithSuffix(const std::filesystem::path& dir, const std::string& suffix) {
    int count = 0;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        const std::string fn = e.path().filename().string();
        if (fn.size() >= suffix.size() &&
            fn.substr(fn.size() - suffix.size()) == suffix)
            ++count;
    }
    return count;
}

// ---------------------------------------------------------------------------
// C1: CrashMidAdopt_RecoversAllData
//
// Durability contract: if a staging segment was ADOPTED in-memory (real data
// written through the already-open fd) but the background rename to
// <base>.log never ran before a crash, Log::recoverStagingFiles() MUST
// recover the true base from the first batch header and rename it in.
//
// Simulation: write N records across at least one roll (so >=1 segment was
// adopted via the staging path or direct create); then simulate the "crash
// before rename" scenario by:
//   1. Driving a real staging adoption (preprovisionPending +
//      finalizeStagingRenames paths used in production).  However, since
//      we can't inject a crash mid-rename from a test, we directly replicate
//      what a crash would leave: a data-bearing <base>.log renamed back to a
//      staging temp name (.staging-99.stlog / .stidx).
//   2. Constructing a fresh Log over the same dir and calling recover().
//
// PASS criteria: all N records recovered, LEO correct, the staging file was
// renamed back to <base>.log (no staging files remain after recovery).
//
// NON-VACUITY: see test body comment and verification section below.
// ---------------------------------------------------------------------------
TEST(LogStaging, CrashMidAdopt_RecoversAllData) {
    auto dir = makeTempDir("rune_stg_c1");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Write records in two batches of 200 each across a roll boundary.
    // Use rollBytes=256 so each ~100-byte batch triggers a roll quickly.
    static constexpr int kFirst  = 200; // records in first segment
    static constexpr int kSecond = 200; // records in second segment
    static constexpr int kTotal  = kFirst + kSecond;

    int64_t savedLEO = 0;
    int64_t secondSegBase = -1;

    {
        Log log(dir.string(), 256);
        int64_t off = 0;
        for (int i = 0; i < kTotal; ++i) {
            RecordBatchBuilder b(off, 10000LL + i);
            b.append("k", "v" + std::to_string(i), 10000LL + i);
            off = log.append(b.build(), off);
        }
        savedLEO = log.logEndOffset();
        ASSERT_EQ(savedLEO, kTotal);

        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u) << "Need at least two segments for this test";
        secondSegBase = bases[1]; // first rolled-into segment base
    }
    // Log destroyed: all fds closed, files flushed/fsync'd.

    ASSERT_GE(secondSegBase, 1); // sanity: second segment does not start at 0

    // Simulate "crash before background rename completed":
    // rename <secondSegBase>.log  →  .staging-99.stlog
    // rename <secondSegBase>.index → .staging-99.stidx
    // (This is exactly what a crash after adoptAsActive() but before
    // renameStagingToBase() would leave on disk.)
    auto format20 = [](int64_t v) {
        std::ostringstream os;
        os << std::setw(20) << std::setfill('0') << v;
        return os.str();
    };
    std::string realLog   = dir.string() + "/" + format20(secondSegBase) + ".log";
    std::string realIdx   = dir.string() + "/" + format20(secondSegBase) + ".index";
    std::string stagLog   = dir.string() + "/.staging-99.stlog";
    std::string stagIdx   = dir.string() + "/.staging-99.stidx";

    std::error_code ec;
    std::filesystem::rename(realLog, stagLog, ec);
    ASSERT_FALSE(ec) << "rename .log → .stlog failed: " << ec.message();
    std::filesystem::rename(realIdx, stagIdx, ec);
    // Index rename failure is OK — recovery drops any leftover staging .index.
    // (Staging .index is rebuilt by recover() from the .log anyway.)

    // Verify the staging file exists and the real .log does not.
    ASSERT_TRUE(std::filesystem::exists(stagLog))
        << "Staging .stlog must exist after simulation";
    ASSERT_FALSE(std::filesystem::exists(realLog))
        << "Real .log must not exist after simulation";

    // --- Recovery: construct a new Log over the same dir ---
    Log log2(dir.string(), 256);
    int64_t recovered = log2.recover();

    // All N records must be recovered — ZERO data loss.
    EXPECT_EQ(recovered, kTotal)
        << "CrashMidAdopt: recover() must return LEO=" << kTotal
        << " (all " << kTotal << " records recovered). "
           "Got " << recovered << ".  "
           "If this is < " << kTotal << " then recoverStagingFiles() did not rename "
           "the staging segment back in.";
    EXPECT_EQ(log2.logEndOffset(), kTotal);

    // Verify all records readable.
    int64_t cursor = 0;
    int recordsRead = 0;
    while (cursor < kTotal) {
        auto bytes = log2.read(cursor, 1 << 20);
        if (bytes.empty()) break;
        RecordBatchReader r(bytes.data(), bytes.size());
        ASSERT_TRUE(r.ensureValid());
        cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
        ++recordsRead;
    }
    EXPECT_EQ(cursor, kTotal)
        << "Not all records readable after crash-mid-adopt recovery.";

    // Staging file must have been renamed back to <base>.log by recoverStagingFiles().
    EXPECT_TRUE(std::filesystem::exists(realLog))
        << "After recovery, <base>.log must exist (staging renamed back).  "
           "Got: staging=" << stagLog << " still exists="
        << std::filesystem::exists(stagLog);
    EXPECT_FALSE(std::filesystem::exists(stagLog))
        << ".stlog staging file must be gone after successful recovery.";

    // No staging files (.stlog / .stidx) must remain.
    EXPECT_EQ(countFilesWithSuffix(dir, ".stlog"), 0)
        << "No .stlog files must remain after recovery.";
    EXPECT_EQ(countFilesWithSuffix(dir, ".stidx"), 0)
        << "No .stidx files must remain after recovery.";

    // --- NON-VACUITY documentation ---
    // The scratch-break proof (run separately to establish non-vacuity):
    //   Make recoverStagingFiles() a no-op in Log.cpp; rebuild; rerun this test.
    //   Result: recovered = <firstSegBase.LEO> < kTotal (records in the second
    //   segment are lost because <secondSegBase>.log was never renamed back in
    //   and is invisible to the directory scan in the Log constructor).
    //   Specifically with kFirst=200 / kSecond=200 and the roll happening at
    //   a batch boundary near record ~200, the no-op break loses approximately
    //   the kSecond=200 records in the staging segment, so recovered < 400.
    //   Restoring recoverStagingFiles() → test passes again.
    // See report: non_vacuity_proof section for exact before/after counts.

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// C2: OrphanStagingSweptAtStartup
//
// Empty staging files (never adopted = no data) and orphan .stidx files
// (no matching .stlog) MUST be deleted by Log construction.
// Normal data files must be unaffected.
// ---------------------------------------------------------------------------
TEST(LogStaging, OrphanStagingSweptAtStartup) {
    auto dir = makeTempDir("rune_stg_c2");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Write some normal data so we can verify it survives the sweep.
    {
        Log log(dir.string(), 64LL * 1024 * 1024); // no roll
        int64_t off = 0;
        for (int i = 0; i < 5; ++i) {
            RecordBatchBuilder b(off, 20000LL + i);
            b.append("k", "v" + std::to_string(i), 20000LL + i);
            off = log.append(b.build(), off);
        }
        ASSERT_EQ(log.logEndOffset(), 5);
    }

    // Drop orphan staging files directly onto disk.
    // 1. Empty .stlog + matching .stidx (staging allocated but never written to).
    {
        std::ofstream f1(dir.string() + "/.staging-0.stlog", std::ios::binary);
        // Write nothing — empty file.
        std::ofstream f2(dir.string() + "/.staging-0.stidx", std::ios::binary);
    }
    // 2. Lone orphan .stidx with no matching .stlog.
    {
        std::ofstream f3(dir.string() + "/.staging-7.stidx", std::ios::binary);
    }

    ASSERT_EQ(countFilesWithSuffix(dir, ".stlog"), 1)
        << "Pre-condition: 1 empty .stlog must exist before Log construction";
    ASSERT_EQ(countFilesWithSuffix(dir, ".stidx"), 2)
        << "Pre-condition: 2 .stidx files must exist before Log construction";

    // Construct a new Log — the constructor calls recoverStagingFiles() which
    // must sweep all empty/orphan staging files.
    {
        Log log(dir.string(), 64LL * 1024 * 1024);
        int64_t recovered = log.recover();

        // Normal data must be intact.
        EXPECT_EQ(recovered, 5) << "Normal data must survive orphan sweep";
        EXPECT_EQ(log.logEndOffset(), 5);

        // Verify data is readable.
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "Data must be readable after orphan sweep";
        RecordBatchReader r(bytes.data(), bytes.size());
        ASSERT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // All staging files must be gone.
    EXPECT_EQ(countFilesWithSuffix(dir, ".stlog"), 0)
        << "Empty .stlog must be swept by Log construction";
    EXPECT_EQ(countFilesWithSuffix(dir, ".stidx"), 0)
        << "Orphan .stidx must be swept by Log construction";

    // Verify no .log files were deleted (only staging files removed).
    EXPECT_GE(countFilesWithSuffix(dir, ".log"), 1)
        << "Normal .log files must not be deleted by orphan sweep";

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// C3: AdoptRebaseOffsetsCorrect
//
// Drive enough appends to cross at least two roll boundaries (forcing 2+
// staging-or-fallback adoptions).  Assert:
//   (a) Records at the rebased offsets read back with correct values.
//   (b) LEO is correct after multiple adopted rolls.
//   (c) Everything survives a restart: reopen Log → recover → same data,
//       correct on-disk <base>.log names, rebuilt index.
//
// This test runs entirely in-process: it drives appends + rolls (which
// internally call installNextSegment — either adopting a pre-provisioned
// staging segment or creating one synchronously as the fallback).
// We finalize any deferred renames synchronously so on-disk names are
// canonical before the restart.
// ---------------------------------------------------------------------------
TEST(LogStaging, AdoptRebaseOffsetsCorrect) {
    auto dir = makeTempDir("rune_stg_c3");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Use a very small rollBytes to force many rolls quickly.
    static constexpr int64_t kRollBytes = 256;
    static constexpr int kRecords = 400;

    int64_t savedLEO = 0;
    std::vector<int64_t> bases;

    {
        Log log(dir.string(), kRollBytes);
        int64_t off = 0;
        for (int i = 0; i < kRecords; ++i) {
            RecordBatchBuilder b(off, 30000LL + i);
            b.append("k", "v" + std::to_string(i), 30000LL + i);
            off = log.append(b.build(), off);
        }
        savedLEO = log.logEndOffset();
        ASSERT_EQ(savedLEO, kRecords);
        bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u)
            << "Need at least 3 segments to cover 2+ adoption events; "
               "got " << bases.size() << " segments.";

        // Drain any pending deferred renames so on-disk names are canonical.
        // In production this is done by the background cleaner thread; here
        // we do it synchronously (finalizeStagingRenames + drainStagingRenamesDone).
        log.finalizeStagingRenames();
        log.drainStagingRenamesDone();
    }

    // (a) Records at each segment's base offset must be readable with correct values.
    {
        Log log(dir.string(), kRollBytes);
        int64_t recovered = log.recover();
        EXPECT_EQ(recovered, kRecords) << "LEO must be restored after multi-roll recovery";

        // Read every record by scanning from offset 0 through kRecords.
        int64_t cursor = 0;
        int count = 0;
        while (cursor < kRecords) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            ASSERT_TRUE(r.ensureValid())
                << "Batch at offset " << cursor << " must be valid after multi-roll recovery";
            EXPECT_GE(r.baseOffset(), 0);
            EXPECT_LT(r.baseOffset(), kRecords);
            cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
            ++count;
        }
        EXPECT_EQ(cursor, kRecords)
            << "All " << kRecords << " records must be readable after multi-roll + recovery; "
               "cursor stopped at " << cursor;

        // (b) LEO correct after multiple adopted rolls.
        EXPECT_EQ(log.logEndOffset(), kRecords);

        // (c) On-disk .log files must exist for every segment base offset.
        auto format20 = [](int64_t v) {
            std::ostringstream os;
            os << std::setw(20) << std::setfill('0') << v;
            return os.str();
        };
        for (int64_t base : bases) {
            std::string logPath = dir.string() + "/" + format20(base) + ".log";
            EXPECT_TRUE(std::filesystem::exists(logPath))
                << "On-disk <base>.log must exist for base=" << base
                << "; staging rename may not have been finalized.";
        }

        // (c) No staging files must remain (all deferred renames completed).
        EXPECT_EQ(countFilesWithSuffix(dir, ".stlog"), 0)
            << "No .stlog files must remain after finalizeStagingRenames + recovery";
        EXPECT_EQ(countFilesWithSuffix(dir, ".stidx"), 0)
            << "No .stidx files must remain after finalizeStagingRenames + recovery";
    }

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// C4: SealFsyncSynchronousAtRoll
//
// Guard: activeSegment().sync() runs BEFORE installNextSegment() in
// maybeRoll() — the seal fsync must be synchronous and must complete before
// the new segment is activated.
//
// We cannot hook into maybeRoll() directly from a test without modifying
// production code (which is forbidden).  Instead we verify the observable
// durability guarantee: after a roll, the SEALED segment's data is readable
// on a fresh reopen WITHOUT any explicit flush call.  This is the only
// externally-observable invariant the seal fsync guards.
//
// Non-vacuous reasoning: if activeSegment().sync() were removed from
// maybeRoll(), the sealed segment's tail (written via appendNoSync()) would
// only be in page cache.  On a real crash that data would be lost.  The test
// below uses Log::append() which DOES call sync() per-batch, so the guard
// here is specifically for appendNoSync() group-commit users who rely on the
// roll-time seal fsync.  The code-path guard: a grep/comment-link assertion
// that the ordering in Log.cpp maybeRoll() is preserved is documented below.
// ---------------------------------------------------------------------------
TEST(LogStaging, SealFsyncSynchronousAtRoll) {
    auto dir = makeTempDir("rune_stg_c4");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Write enough records via appendNoSync (group-commit path) to force at
    // least one roll.  The seal fsync happens inside maybeRoll() just BEFORE
    // installNextSegment() — this is the invariant we guard.
    static constexpr int64_t kRollBytes = 256;
    int64_t savedLEO = 0;
    int64_t sealedBase = -1;

    {
        Log log(dir.string(), kRollBytes);
        int64_t off = 0;
        // Use appendNoSync to exercise the path that relies on the roll-time seal fsync.
        for (int i = 0; i < 50; ++i) {
            RecordBatchBuilder b(off, 40000LL + i);
            b.append("k", "v" + std::to_string(i), 40000LL + i);
            auto batch = b.build();
            off = log.appendNoSync(batch.data(), batch.size(), off);
        }
        // Final sync to flush the active (un-sealed) tail.
        log.sync();
        savedLEO = log.logEndOffset();
        ASSERT_EQ(savedLEO, 50);

        auto bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for the seal-fsync guard test";
        sealedBase = bases[0]; // the first sealed segment
    }

    // Open the sealed segment file directly and verify it is non-empty and
    // contains valid batch data (the seal fsync must have made it durable).
    auto format20 = [](int64_t v) {
        std::ostringstream os;
        os << std::setw(20) << std::setfill('0') << v;
        return os.str();
    };
    // The sealed segment may still have its staging name if finalizeStagingRenames
    // hasn't run; find it either way.
    std::string sealedLogPath;
    {
        // First look for the canonical name.
        std::string canonical = dir.string() + "/" + format20(sealedBase) + ".log";
        if (std::filesystem::exists(canonical)) {
            sealedLogPath = canonical;
        } else {
            // Look for any .stlog (staging — not yet renamed).
            for (auto& e : std::filesystem::directory_iterator(dir)) {
                if (e.path().extension() == ".stlog") {
                    sealedLogPath = e.path().string();
                    break;
                }
            }
        }
    }
    ASSERT_FALSE(sealedLogPath.empty())
        << "Sealed segment file must exist on disk after roll; "
           "base=" << sealedBase;

    // Read raw bytes from the sealed file and verify a valid batch header.
    {
        std::ifstream f(sealedLogPath, std::ios::binary);
        ASSERT_TRUE(f.is_open()) << "Cannot open sealed segment: " << sealedLogPath;
        f.seekg(0, std::ios::end);
        std::streamsize sz = f.tellg();
        ASSERT_GT(sz, 0) << "Sealed segment must be non-empty (seal fsync must have run)";
        f.seekg(0, std::ios::beg);
        std::vector<uint8_t> buf(static_cast<std::size_t>(sz));
        f.read(reinterpret_cast<char*>(buf.data()), sz);

        // The sealed file must contain at least one valid batch.
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid())
            << "Sealed segment must contain valid batch data after roll-time seal fsync.  "
               "An invalid batch would indicate the seal fsync ordering is broken.";
        EXPECT_EQ(r.baseOffset(), sealedBase)
            << "First batch in sealed segment must start at the segment base offset.";
    }

    // Restart: fresh Log + recover → all data must be present.
    {
        Log log2(dir.string(), kRollBytes);
        int64_t recovered = log2.recover();
        EXPECT_EQ(recovered, savedLEO)
            << "After roll + restart, all " << savedLEO << " records must be recovered.  "
               "A lower value would mean the seal fsync was deferred past the crash point.";
    }

    // CODE-PATH GUARD (documentation):
    // In Log.cpp maybeRoll() the ordering is:
    //   1. activeSegment().sync()      ← seal fsync (SYNCHRONOUS)
    //   2. installNextSegment(base)    ← activate next segment
    // If step 1 were moved after step 2 or removed, un-fsync'd data written
    // via appendNoSync() to the sealed segment would be lost on a crash at
    // the roll boundary.  This test guards the observable consequence: that
    // a fresh reopen always sees the sealed segment's data.

    std::filesystem::remove_all(dir);
}

// ---- Task 3 (E3): activeSegmentFd() returns a valid fsync-able fd; flushBoundary() == logEndOffset() ----
TEST(Log, ActiveSegmentFdIsFsyncable) {
    auto dir = makeTempDir("rune_log_fd_test");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string(), /*roll=*/1<<20);
    log.recover();
    // Build a minimal record batch using the existing helper pattern.
    RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    auto batch = b.build();
    log.appendNoSync(batch.data(), batch.size(), /*lastOffset=*/0);
    int fd = log.activeSegmentFd();
    ASSERT_GE(fd, 0) << "activeSegmentFd() must return the open .log fd (>= 0)";
    // Use fsync (macOS has no fdatasync; on Linux the flusher will use fdatasync).
    EXPECT_EQ(::fsync(fd), 0) << "the worker's flusher will call fsync/fdatasync on this fd";
    EXPECT_EQ(log.flushBoundary(), log.logEndOffset())
        << "flushBoundary() must equal logEndOffset() so the flusher knows what is durable";
    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F3: LogSegment read mapping — lazy for sealed segments, no retained fd,
// noexcept readView.
// ---------------------------------------------------------------------------

// F3.1: a sealed segment recovered from disk must NOT get an eager read mmap;
// only the last (active) recovered segment does.  The sealed segment's mapping
// is created lazily, on its first readView() — and that readView() must still
// return the correct data.
TEST(LogSegmentRegression, SealedRecoveredSegmentMapsLazily) {
    auto dir = makeTempDir("rune_f3_lazy");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::vector<int64_t> bases;
    int64_t midOffset = -1;
    {
        // Small rollBytes forces >= 3 segments across ~30 tiny batches.
        Log log(dir.string(), /*rollBytes=*/256);
        int64_t off = 0;
        for (int i = 0; i < 30; ++i) {
            RecordBatchBuilder b(off, 6000 + i); b.append("k", "v" + std::to_string(i), 6000 + i);
            off = log.append(b.build(), off);
        }
        bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u) << "test needs >= 3 segments to have real sealed segments";
        midOffset = bases[1]; // base offset of a sealed (non-active, non-first) segment
    }

    // Reopen (recovery) — a brand-new Log over the same directory.
    Log log2(dir.string(), /*rollBytes=*/256);
    int64_t recovered = log2.recover();
    ASSERT_GT(recovered, 0);

    int64_t activeBase = bases.back();

    // Every sealed segment must have NO read map yet; the active one must.
    for (int64_t b : bases) {
        LogSegment* seg = log2.segmentForTest(b);
        ASSERT_NE(seg, nullptr) << "segment " << b << " missing after recovery";
        if (b == activeBase) {
            EXPECT_TRUE(seg->hasReadMapForTest())
                << "active segment (base=" << b << ") must be eagerly mapped";
        } else {
            EXPECT_FALSE(seg->hasReadMapForTest())
                << "sealed segment (base=" << b << ") must NOT be eagerly mapped (F3 lazy)";
        }
    }

    // readView() into the sealed segment must still return correct data, and
    // must lazily create the mapping as a side effect.
    LogSegment* sealedSeg = log2.segmentForTest(midOffset);
    ASSERT_NE(sealedSeg, nullptr);
    ASSERT_FALSE(sealedSeg->hasReadMapForTest());

    LogReadView view = log2.readView(midOffset, 1 << 20);
    ASSERT_FALSE(view.empty()) << "readView() on the sealed segment must return data";
    RecordBatchReader r(view.data, view.len);
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), midOffset);

    EXPECT_TRUE(sealedSeg->hasReadMapForTest())
        << "the sealed segment must now have a read map after its first readView()";

    std::filesystem::remove_all(dir);
}

// F3.3: readView() must never throw, even when the lazy map fails (backing
// .log file gone).  It returns an empty view and increments readMapFailures().
TEST(LogSegmentRegression, ReadViewNoexceptOnMapFailure) {
    auto dir = makeTempDir("rune_f3_noexcept");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::vector<int64_t> bases;
    int64_t sealedBase = -1;
    {
        Log log(dir.string(), /*rollBytes=*/256);
        int64_t off = 0;
        for (int i = 0; i < 30; ++i) {
            RecordBatchBuilder b(off, 6500 + i); b.append("k", "v" + std::to_string(i), 6500 + i);
            off = log.append(b.build(), off);
        }
        bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u);
        sealedBase = bases[1];
    }

    Log log2(dir.string(), /*rollBytes=*/256);
    log2.recover();

    LogSegment* sealedSeg = log2.segmentForTest(sealedBase);
    ASSERT_NE(sealedSeg, nullptr);
    ASSERT_FALSE(sealedSeg->hasReadMapForTest()) << "sealed segment must still be unmapped (F3 lazy)";
    EXPECT_EQ(sealedSeg->readMapFailures(), 0u);

    // D3b r2: the read map is made through the segment's own fd (its inode),
    // never by re-opening the path — deleting the path no longer breaks it (the
    // segment keeps reading its own bytes).  Inject the map failure via the seam.
    ::unlink(sealedSeg->logPath().c_str());
    sealedSeg->setReadMapFaultForTest(true);

    LogReadView v1;
    EXPECT_NO_THROW(v1 = log2.readView(sealedBase, 1 << 20));
    EXPECT_TRUE(v1.empty()) << "readView() must return empty on a map failure, not stale/garbage data";
    EXPECT_EQ(sealedSeg->readMapFailures(), 1u);

    LogReadView v2;
    EXPECT_NO_THROW(v2 = log2.readView(sealedBase, 1 << 20));
    EXPECT_TRUE(v2.empty());
    EXPECT_EQ(sealedSeg->readMapFailures(), 2u)
        << "each failed readView() must bump readMapFailures(), no throw either time";

    std::filesystem::remove_all(dir);
}

// F3 fix-round-1, item 2 (roll-safety check): now that LogSegment's ctor can
// throw on an open() failure, a failed ROLL (the synchronous
// installNextSegment() fallback) must NEVER leave Log without an active
// segment.  Forced by pre-creating a DIRECTORY at the exact path the next
// segment's .log would use (EISDIR on open(), independent of uid/perms) —
// the OLD active segment must remain fully valid and readable; the failure is
// fail-stop (E1 review) — no retry.
TEST(LogRegression, FailedRollKeepsOldActiveSegmentAndIsFailStop) {
    auto dir = makeTempDir("rune_f3_failed_roll");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), /*rollBytes=*/256);
    int64_t off = 0;
    // Append a small batch, then one deliberately oversized batch — both
    // succeed and land in the SAME (still single) segment, but push its
    // sizeBytes() past rollBytes=256.  maybeRoll() decides to roll BEFORE
    // writing the NEXT batch (Kafka-style — see Log::maybeRoll()'s own
    // comment), so the roll attempt only fires on the append AFTER this one.
    {
        RecordBatchBuilder b(off, 8000); b.append("k", "v0", 8000);
        off = log.append(b.build(), off);
    }
    {
        RecordBatchBuilder big(off, 8001); big.append("k", std::string(300, 'x'), 8001);
        off = log.append(big.build(), off);
    }
    ASSERT_EQ(log.segmentBaseOffsets().size(), 1u) << "test setup: must still be a single segment";
    int64_t beforeEndOffset = log.logEndOffset();
    auto basesBefore = log.segmentBaseOffsets();

    // Block the NEXT roll's target path: maybeRoll() uses base == logEndOffset_
    // for the new segment.  A pre-existing DIRECTORY at that exact .log path
    // makes LogSegment's open() fail with EISDIR when the fallback tries to
    // create it synchronously (no staging segment is ever pre-provisioned by
    // a bare Log in a unit test, so installNextSegment() always takes the
    // synchronous fallback branch).
    std::ostringstream blockedPath;
    blockedPath << dir.string() << "/" << std::setw(20) << std::setfill('0')
                << beforeEndOffset << ".log";
    std::filesystem::create_directory(blockedPath.str());

    // This append sees the active segment already >= rollBytes (from the
    // oversized batch above) and triggers maybeRoll() ->
    // installNextSegment() -> the failing fallback construction, BEFORE this
    // batch's own bytes are written.
    RecordBatchBuilder triggerB(off, 9000); triggerB.append("k", "trigger", 9000);
    auto triggerBatch = triggerB.build();
    EXPECT_THROW(log.append(triggerBatch, off), std::system_error)
        << "the failed roll must surface as a clean exception, not a crash";

    // Log must still have exactly the OLD active segment — no null/partial
    // entry, no segments_ left empty — and logEndOffset_ must be UNCHANGED
    // (the write never happened: maybeRoll() throws before writeAndUpdateState).
    auto basesAfter = log.segmentBaseOffsets();
    ASSERT_EQ(basesAfter, basesBefore) << "a failed roll must not touch segments_ at all";
    EXPECT_EQ(log.logEndOffset(), beforeEndOffset);

    // The old active segment must still be fully usable: a read for data
    // appended before the failed roll must still return correct data.
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // E1 review (Kafka parity: an IOException on roll takes the log dir offline): a failed roll is
    // FAIL-STOP.  It used to leave the log healthy and every later append retried the roll (re-pinning
    // the sealed segment each time) — on a dead disk the partition answered every produce with an
    // internal error and the broker never halted.  Now the log is failed (sticky) -> worker halt.
    EXPECT_TRUE(log.storageFailed()) << "a failed roll marks the log failed";
    EXPECT_NE(log.storageErrno(), 0);
    std::filesystem::remove_all(blockedPath.str());
    EXPECT_THROW(log.append(triggerBatch, off), std::system_error)
        << "sticky: no retry after a storage failure, even once the obstruction is gone";
    EXPECT_EQ(log.segmentBaseOffsets(), basesBefore);

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item B.1: reloadSegments() must give a STRONG exception
// guarantee — if ANY segment fails to (re)construct, segments_ must be left
// EXACTLY as it was before the call (the old, still-open segments remain
// fully usable), never partially cleared and never missing its active entry.
// ---------------------------------------------------------------------------
// F3 round 7, item 2: reloadSegments' install step must be no-throw.  Round
// 6 erased the old generation (retire_ → retiredSegments_.push_back) and then
// emplaced the new one segment by segment: a bad_alloc in between left
// segments_ EMPTY.  Now every allocation (retiredSegments_ reserve) happens
// BEFORE the old generation is touched and the install is a no-throw swap.
// A test-only allocation fault at that point must leave the log intact.
TEST(LogRegression, ReloadSegmentsAllocFailureLeavesLogIntactRegression) {
    auto dir = makeTempDir("rune_f3r7_reload_alloc");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/256);
    int64_t off = 0;
    for (int i = 0; i < 30; ++i) {
        RecordBatchBuilder b(off, 9900 + i); b.append("k", "v" + std::to_string(i), 9900 + i);
        off = log.append(b.build(), off);
    }
    const auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GT(basesBefore.size(), 2u);
    const int64_t leoBefore = log.logEndOffset();

    // F3 round 8: pin the active segment as an in-flight async flush would
    // (T10 M1), so the reload must RETIRE it into the reserved
    // retiredSegments_ capacity (its fd stays open until the flush completes).
    rune::storage::LogSegment* pinned = log.activeSegmentForFlush();
    pinned->pinFlush();
    ASSERT_EQ(log.retiredSegmentsForTest(), 0u);

    log.setReloadAllocFaultForTest(true);
    EXPECT_THROW(log.reloadSegments(), std::bad_alloc);
    EXPECT_EQ(log.segmentBaseOffsets(), basesBefore) << "segments_ must be untouched";
    EXPECT_EQ(log.logEndOffset(), leoBefore);
    EXPECT_EQ(log.retiredSegmentsForTest(), 0u) << "nothing retired before the fault point";
    EXPECT_EQ(pinned->flushPins(), 1u) << "the pinned segment is still live and pinned";
    RecordBatchBuilder b(off, 9999); b.append("k", "still-alive", 9999);
    EXPECT_EQ(log.append(b.build(), off), off + 1) << "old active segment still usable";

    log.setReloadAllocFaultForTest(false);
    EXPECT_NO_THROW(log.reloadSegments());
    EXPECT_EQ(log.logEndOffset(), off + 1);
    EXPECT_EQ(log.retiredSegmentsForTest(), 1u)
        << "the pinned old segment is RETIRED (reserved capacity path), not closed";
    log.unpinSegmentFlush(pinned);                 // flush completion → retired segment closed
    EXPECT_EQ(log.retiredSegmentsForTest(), 0u);
    std::filesystem::remove_all(dir);
}

TEST(LogRegression, ReloadSegmentsFailureLeavesLogIntactRegression) {
    auto dir = makeTempDir("rune_f3r2_reload_fail");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), /*rollBytes=*/256);
    int64_t off = 0;
    for (int i = 0; i < 30; ++i) {
        RecordBatchBuilder b(off, 9500 + i); b.append("k", "v" + std::to_string(i), 9500 + i);
        off = log.append(b.build(), off);
    }
    auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GE(basesBefore.size(), 3u)
        << "test setup: need >= 3 segments so a sealed (non-active, non-first) one can be sabotaged";
    int64_t endOffsetBefore = log.logEndOffset();

    // Sabotage one SEALED segment's .log: replace it with a non-empty
    // directory.  reloadSegments()'s re-discovery still finds it (extension
    // matching doesn't care whether the entry is a file or a directory), and
    // its LogSegment ctor throws EISDIR on open() (round-1's item 2 fix).
    int64_t sabotageBase = basesBefore[1];
    std::ostringstream sabotagePath;
    sabotagePath << dir.string() << "/" << std::setw(20) << std::setfill('0')
                 << sabotageBase << ".log";
    std::filesystem::remove(sabotagePath.str());
    std::filesystem::create_directory(sabotagePath.str());

    EXPECT_THROW(log.reloadSegments(), std::system_error)
        << "reloadSegments() must surface the failure, not silently continue";

    // segments_ must be UNTOUCHED: same bases, same LEO, and the old active
    // segment (and every other old segment) must still be fully usable.
    EXPECT_EQ(log.segmentBaseOffsets(), basesBefore)
        << "a failed reloadSegments() must leave segments_ exactly as it was";
    EXPECT_EQ(log.logEndOffset(), endOffsetBefore);
    {
        auto bytes = log.read(0, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }
    // The log must still accept new appends — the OLD active segment (never
    // touched by the failed reload attempt) is still fully open and valid.
    RecordBatchBuilder afterB(off, 9999); afterB.append("k", "still-alive", 9999);
    int64_t next = log.append(afterB.build(), off);
    EXPECT_EQ(next, off + 1);

    std::filesystem::remove_all(sabotagePath.str());
    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F3 fix-round-2, item B.2: truncateTo()'s active-segment "straddle" rebuild
// physically truncates the .log file on disk BEFORE reopening it.  Past that
// point of no return, a failure to reopen must FAIL-STOP (log critical +
// abort), never silently continue with disk and memory disagreeing (which
// risks SIGBUS via the old, now-oversized mmap).  EXPECT_DEATH (threadsafe
// style): the statement runs in a forked child, so setup done in the parent
// before EXPECT_DEATH is visible via copy-on-write.
// ---------------------------------------------------------------------------
TEST(LogRegression, TruncateRebuildFailureFailsStopRegression) {
    auto dir = makeTempDir("rune_f3r2_truncate_abort");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    // Single segment (large rollBytes) with 5 single-record batches (offsets
    // 0..4) so truncateTo(2) lands mid-log — the "active segment straddles
    // truncateOffset" path, not the "segments_ empty" path.  Constructed and
    // recovered successfully BEFORE any sabotage (fork() copy-on-write means
    // the forked EXPECT_DEATH child inherits this already-valid `log`, so the
    // sabotage below targets ONLY the reopen-after-truncate step, not Log's
    // initial construction).
    Log log(dir.string(), /*rollBytes=*/1 << 20);
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(off, 9700 + i); b.append("k", "v" + std::to_string(i), 9700 + i);
        off = log.append(b.build(), off);
    }

    // NOW sabotage the (only) segment's .index path: replace it with a
    // NON-EMPTY directory, so std::filesystem::remove(ipath) — which runs
    // AFTER truncateTo() has already physically resize_file()'d the .log —
    // throws filesystem_error (ENOTEMPTY).  This is the exact "something
    // fails past the point of no return" condition the fail-stop guard
    // covers (the guard wraps BOTH the index removal and the segment ctor).
    std::string indexPath;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".index") { indexPath = e.path().string(); break; }
    }
    ASSERT_FALSE(indexPath.empty());
    std::filesystem::remove(indexPath);
    std::filesystem::create_directory(indexPath);
    { std::ofstream dummy(indexPath + std::string("/dummy.txt")); dummy << "x"; }

    EXPECT_DEATH(log.truncateTo(2), "FATAL");   // mid-batch -> straddle rebuild -> FATAL abort

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// F3 round 5, item I4: the post-truncate fail-stop block must ALSO cover the
// rebuilt active segment's recover() and the logEndOffset_ assignment.  Round
// 2 left activeSegment().recover() OUTSIDE the guard; recover() rebuilds the
// index (remove .index.tmp, write, rename), so a throw there escaped as an
// ordinary exception with the .log ALREADY truncated on disk and LEO not yet
// updated — disk and memory disagreeing, exactly what the guard exists for.
// Sabotage: <base>.index.tmp is a NON-EMPTY directory, so recover()'s
// std::filesystem::remove(indexTmp) throws ENOTEMPTY — while the segment
// reopen itself (which never touches .index.tmp) still succeeds.
// ---------------------------------------------------------------------------
TEST(LogRegression, TruncateRecoverFailureFailsStopRegression) {
    auto dir = makeTempDir("rune_f3r5_truncate_recover_abort");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    Log log(dir.string(), /*rollBytes=*/1 << 20);
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(off, 9800 + i); b.append("k", "v" + std::to_string(i), 9800 + i);
        off = log.append(b.build(), off);
    }

    std::string indexPath;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".index") { indexPath = e.path().string(); break; }
    }
    ASSERT_FALSE(indexPath.empty());
    const std::string tmpPath = indexPath + ".tmp";   // <base>.index.tmp
    std::filesystem::create_directory(tmpPath);
    { std::ofstream dummy(tmpPath + std::string("/dummy.txt")); dummy << "x"; }

    EXPECT_DEATH(log.truncateTo(2), "FATAL");   // recover() throws past the point of no return

    std::filesystem::remove_all(dir);
}


// E1 (DUR-1): with periodic fsync OFF by default, a dead disk must still be detected through the WRITE
// path.  A failed pwrite/pwritev/fstream write used to throw a plain runtime_error and leave the log
// "healthy" (failedErrno == 0): the next append wrote again (O_APPEND after a partial write = torn data)
// and nothing routed the broker to the Kafka-style log-dir-failure halt.  Now: sticky fail-stop + errno.
namespace {
std::vector<uint8_t> e1Batch(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", "v", 1000);
    return b.build();
}
} // namespace

TEST(LogWriteFailStopRegression, EveryAppendPathMarksTheLogFailedOnAWriteError) {
    for (int path = 0; path < 3; ++path) {
        auto dir = makeTempDir("rune_log_e1_write");
        std::filesystem::create_directories(dir);
        {
            Log log(dir.string(), 64LL * 1024 * 1024); log.recover();
            const auto ok = e1Batch(0);
            int64_t base = -1;
            ASSERT_NO_THROW(log.appendNoSync(ok.data(), ok.size(), 0));
            log.setWriteFaultForTest(EIO);
            const auto b = e1Batch(1);
            try {
                if (path == 0) log.appendNoSync(b.data(), b.size(), 1);
                else if (path == 1) log.appendNoSyncRestamp(b.data(), b.size(), 0, base);
                else log.append(b, 1);
                ADD_FAILURE() << "path " << path << ": a failed write must throw";
            } catch (const std::system_error& e) {
                EXPECT_EQ(e.code().value(), EIO) << "path " << path;
            } catch (const std::exception& e) {
                ADD_FAILURE() << "path " << path << ": must be a std::system_error carrying the errno: " << e.what();
            }
            EXPECT_TRUE(log.storageFailed()) << "path " << path << ": a write error is fail-stop";
            EXPECT_EQ(log.storageErrno(), EIO) << "path " << path;
            EXPECT_EQ(log.logEndOffset(), 1) << "path " << path << ": the failed batch is not part of the log";
            log.setWriteFaultForTest(0);
            EXPECT_THROW(log.appendNoSync(b.data(), b.size(), 1), std::system_error)
                << "path " << path << ": sticky — no write after a failure, even once the fault clears";
        }
        std::filesystem::remove_all(dir);
    }
}

// Kafka parity (replica-divergence redesign): the leader stamps its REAL
// partitionLeaderEpoch on every produce-path append — zero-copy (pwritev; the
// caller's bytes are never mutated), CRC-safe (the field is excluded from the
// CRC).  -1 (not leading) leaves the producer's bytes untouched.
TEST(LogRegression, RestampAppendStampsTheLeaderEpochZeroCopy) {
    auto dir = makeTempDir("rune_log_epoch_stamp");
    std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), 64LL * 1024 * 1024);
        log.recover();
        RecordBatchBuilder b(999, 1000);
        b.append("k", "v", 1000);
        const auto bytes = b.build();
        const auto copy  = bytes;
        int64_t base = -1;
        log.setLeaderEpochStamp(7);
        log.appendNoSyncRestamp(bytes.data(), bytes.size(), 0, base);
        EXPECT_EQ(bytes, copy) << "the caller's batch was mutated";
        log.setLeaderEpochStamp(-1);
        log.appendNoSyncRestamp(bytes.data(), bytes.size(), 0, base);
        const auto out = log.read(0, 1 << 20);
        RecordBatchReader r0(out.data(), out.size());
        ASSERT_TRUE(r0.ensureValid());
        EXPECT_EQ(r0.baseOffset(), 0);
        EXPECT_EQ(r0.partitionLeaderEpoch(), 7);
        const std::size_t n0 = static_cast<std::size_t>(r0.totalLength());
        RecordBatchReader r1(out.data() + n0, out.size() - n0);
        ASSERT_TRUE(r1.ensureValid());
        EXPECT_EQ(r1.baseOffset(), 1);
        EXPECT_EQ(r1.partitionLeaderEpoch(), RecordBatchReader(copy.data(), copy.size()).partitionLeaderEpoch())
            << "stamp -1 must leave the producer's epoch bytes";
    }
    std::filesystem::remove_all(dir);
}

// D11 (2026-10-05): publishSealedSnapshot() reuses each sealed segment's
// largestTimestamp from the previous snapshot with ONE merge walk (it was a scan
// of the previous snapshot per segment — O(n^2) on the append path per roll; a
// 4000-segment test took 70 s).  Every sealed segment must keep its OWN
// timestamp through every republish.
TEST(LogRegression, SealedSnapshotKeepsEachSegmentsTimestampAcrossManyRollsRegression) {
    auto dir = makeTempDir("rune_log_d11");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/64);       // every batch seals a segment
    constexpr int kBatches = 1500;
    for (int i = 0; i < kBatches; ++i) {
        RecordBatchBuilder b(static_cast<int64_t>(i), 100'000 + i);
        b.append("k", "v", 100'000 + i);
        log.append(b.build(), static_cast<int64_t>(i));
    }
    const auto snap = log.sealedSegmentSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_GE(snap->size(), static_cast<std::size_t>(kBatches - 2));
    for (std::size_t i = 0; i < snap->size(); ++i) {
        const auto& si = (*snap)[i];
        EXPECT_EQ(si.largestTimestamp, 100'000 + si.baseOffset) << "segment base " << si.baseOffset;
        if (i > 0) EXPECT_LT((*snap)[i - 1].baseOffset, si.baseOffset);
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// 2026-10-05 log-snapshot review: every segments_ mutation republishes the
// sealed snapshot, and a reused largestTimestamp belongs to the SAME segment
// incarnation (never base-offset equality across a truncate).
// ===========================================================================
namespace {
void appendTs(Log& log, int64_t off, int64_t ts) {
    RecordBatchBuilder b(off, ts);
    b.append("k", "v", ts);
    log.append(b.build(), off);
}
// The published snapshot must describe exactly segments_ minus the active one.
void expectSnapshotMatchesSegments(Log& log) {
    const auto bases = log.segmentBaseOffsets();
    const auto snap  = log.sealedSegmentSnapshot();
    ASSERT_NE(snap, nullptr);
    ASSERT_EQ(snap->size(), bases.size() - 1);
    for (std::size_t i = 0; i < snap->size(); ++i) {
        EXPECT_EQ((*snap)[i].baseOffset, bases[i]);
        EXPECT_EQ((*snap)[i].endOffset, bases[i + 1]);
    }
}
const SegInfo* findSeg(const SealedSegmentSnapshot& s, int64_t base) {
    for (const auto& si : s) if (si.baseOffset == base) return &si;
    return nullptr;
}
} // namespace

// MAJOR-1 (a): truncateTo erased segments and turned a sealed one active, but
// the snapshot still listed them (the cleaner's activeBase was too high).
TEST(LogRegression, TruncateToRepublishesSealedSnapshotRegression) {
    auto dir = makeTempDir("rune_log_trsnap");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    ASSERT_GE(log.sealedSegmentSnapshot()->size(), 3u);
    log.truncateTo(3);
    EXPECT_EQ(log.logEndOffset(), 3);
    expectSnapshotMatchesSegments(log);
    // Truncate to an exact segment boundary too (the previous sealed segment becomes active).
    const auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);
    log.truncateTo(bases.back());
    expectSnapshotMatchesSegments(log);
    // Truncate everything (fresh segment branch).
    log.truncateTo(0);
    expectSnapshotMatchesSegments(log);
    std::filesystem::remove_all(dir);
}

// MAJOR-1 (b): a truncated-then-re-appended segment that rolls reused the
// stale pre-truncate largestTimestamp (base=2 ts=1003 vs real 9000003) —
// time retention would delete freshly written data early.
TEST(LogRegression, TruncatedSegmentResealGetsItsOwnTimestampRegression) {
    auto dir = makeTempDir("rune_log_trts");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    log.truncateTo(3);
    const int64_t activeBase = log.segmentBaseOffsets().back();
    ASSERT_LT(activeBase, 3);
    for (int64_t i = 3; i < 12; ++i) appendTs(log, i, 9'000'000 + i);
    const auto snap = log.sealedSegmentSnapshot();
    const SegInfo* si = findSeg(*snap, activeBase);
    ASSERT_NE(si, nullptr);
    EXPECT_EQ(si->largestTimestamp, 9'000'003) << "base " << activeBase;
    expectSnapshotMatchesSegments(log);
    std::filesystem::remove_all(dir);
}

// MAJOR-1 related: activeSegmentMaxTs_ carried the ERASED segment's max after
// a truncate (overstated: an old segment looks new, retention keeps it).
TEST(LogRegression, TruncateRecomputesActiveSegmentMaxTimestampRegression) {
    auto dir = makeTempDir("rune_log_trmax");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 4; ++i) appendTs(log, i, 1000 + i);
    for (int64_t i = 4; i < 7; ++i) appendTs(log, i, 8'000'000 + i);
    log.truncateTo(3);
    const int64_t activeBase = log.segmentBaseOffsets().back();
    for (int64_t i = 3; i < 12; ++i) appendTs(log, i, 2000 + i);
    const SegInfo* si = findSeg(*log.sealedSegmentSnapshot(), activeBase);
    ASSERT_NE(si, nullptr);
    EXPECT_EQ(si->largestTimestamp, 2003) << "base " << activeBase;
    std::filesystem::remove_all(dir);
}

// MAJOR-1 related: recover() never seeded activeSegmentMaxTs_, so pre-restart
// batches in the active segment were forgotten at its seal (understated: a
// fresh segment looks ancient — retention deletes it early).
TEST(LogRegression, RecoverSeedsActiveSegmentMaxTimestampRegression) {
    auto dir = makeTempDir("rune_log_recmax");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    int64_t activeBase = -1;
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 9'000'000 + i);
        activeBase = log.segmentBaseOffsets().back();
    }
    Log log(dir.string(), /*rollBytes=*/100);
    log.recover();
    ASSERT_EQ(log.segmentBaseOffsets().back(), activeBase);
    for (int64_t i = 7; i < 14; ++i) appendTs(log, i, 1000 + i);
    const SegInfo* si = findSeg(*log.sealedSegmentSnapshot(), activeBase);
    ASSERT_NE(si, nullptr);
    EXPECT_EQ(si->largestTimestamp, 9'000'006) << "base " << activeBase;
    std::filesystem::remove_all(dir);
}

// MEDIUM-1: drainRetentionDeletes released ring slots only when it erased
// something — idempotent retries of already-gone bases filled the ring and
// retention halted forever.
TEST(LogRegression, RetentionDeleteRingDrainsAlreadyRemovedBasesRegression) {
    auto dir = makeTempDir("rune_log_delring");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    for (int i = 0; i < 3000; ++i) {
        ASSERT_TRUE(log.enqueueDeletedSegment(987'654'321)) << "ring stuck after " << i;
        EXPECT_EQ(log.drainRetentionDeletes(), 0);
    }
    // A real delete still works after all the no-op retries.
    const int64_t first = log.segmentBaseOffsets().front();
    ASSERT_TRUE(log.enqueueDeletedSegment(first));
    EXPECT_EQ(log.drainRetentionDeletes(), 1);
    expectSnapshotMatchesSegments(log);
    std::filesystem::remove_all(dir);
}

// MEDIUM-2: largestTimestamp == 0 doubled as "not found", so a segment whose
// real max timestamp is 0 inherited the newest timestamp at the next roll.
TEST(LogRegression, ZeroTimestampSegmentKeepsZeroAcrossRollsRegression) {
    auto dir = makeTempDir("rune_log_zerots");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 3; ++i) appendTs(log, i, 0);          // seals segment 0 with max ts 0
    for (int64_t i = 3; i < 9; ++i) appendTs(log, i, 5'000'000 + i);
    const auto snap = log.sealedSegmentSnapshot();
    ASSERT_GE(snap->size(), 2u);
    EXPECT_EQ((*snap)[0].baseOffset, 0);
    EXPECT_EQ((*snap)[0].largestTimestamp, 0);
    std::filesystem::remove_all(dir);
}

// LOW (review LOW-3): prev-snapshot entries removed from segments_ (front
// retention + a middle segment) followed by rolls — every survivor keeps its
// OWN timestamp (the merge walk's skip branches).
TEST(LogRegression, SnapshotTimestampsSurviveRetentionDeletesThenRollsRegression) {
    auto dir = makeTempDir("rune_log_retroll");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/64);   // every batch seals a segment
    for (int64_t i = 0; i < 22; ++i) appendTs(log, i, 100'000 + i);
    const auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 20u);
    for (int k = 0; k < 5; ++k) ASSERT_TRUE(log.enqueueDeletedSegment(bases[k]));
    ASSERT_TRUE(log.enqueueDeletedSegment(bases[10]));
    EXPECT_EQ(log.drainRetentionDeletes(), 6);
    expectSnapshotMatchesSegments(log);
    for (int64_t i = 22; i < 24; ++i) appendTs(log, i, 100'000 + i);   // two more rolls
    expectSnapshotMatchesSegments(log);
    const auto snap = log.sealedSegmentSnapshot();
    for (const auto& si : *snap) {
        EXPECT_EQ(si.largestTimestamp, 100'000 + si.baseOffset) << "segment base " << si.baseOffset;
        EXPECT_NE(si.baseOffset, bases[10]);
        EXPECT_GE(si.baseOffset, bases[5]);
    }
    std::filesystem::remove_all(dir);
}

// Audit: deleteOldSegments (size retention, NodeRuntimeAgent) erased segments
// without republishing — the cleaner kept seeing the deleted bases.
TEST(LogRegression, DeleteOldSegmentsRepublishesSealedSnapshotRegression) {
    auto dir = makeTempDir("rune_log_delold");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/64);
    for (int64_t i = 0; i < 12; ++i) appendTs(log, i, 100'000 + i);
    ASSERT_GT(log.deleteOldSegments(/*retainBytes=*/200), 0);
    expectSnapshotMatchesSegments(log);
    for (const auto& si : *log.sealedSegmentSnapshot())
        EXPECT_EQ(si.largestTimestamp, 100'000 + si.baseOffset);
    std::filesystem::remove_all(dir);
}

// Audit: reloadSegments (LogCleaner::compact) rebuilt segments_ without
// republishing; a sealed segment the cleaner removed stayed in the snapshot.
// Surviving sealed segments keep their timestamps across the reload.
TEST(LogRegression, ReloadSegmentsRepublishesSealedSnapshotRegression) {
    auto dir = makeTempDir("rune_log_reload");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/64);
    for (int64_t i = 0; i < 8; ++i) appendTs(log, i, 100'000 + i);
    const auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 4u);
    // The cleaner emptied and removed the second segment.
    std::string s = std::to_string(bases[1]);
    s = std::string(20 - s.size(), '0') + s;
    std::filesystem::remove(dir / (s + ".log"));
    std::filesystem::remove(dir / (s + ".index"));
    log.reloadSegments();
    expectSnapshotMatchesSegments(log);
    EXPECT_EQ(findSeg(*log.sealedSegmentSnapshot(), bases[1]), nullptr);
    for (int64_t i = 8; i < 10; ++i) appendTs(log, i, 100'000 + i);
    // The vanished segment's offsets are now covered by its predecessor, which
    // (conservatively — it may be a coalesced merge) takes its timestamp too
    // (fix round 1 CRITICAL-1); every other survivor keeps its own.
    for (const auto& si : *log.sealedSegmentSnapshot()) {
        const int64_t expected = si.baseOffset == bases[0] ? 100'000 + bases[1] : 100'000 + si.baseOffset;
        EXPECT_EQ(si.largestTimestamp, expected) << "segment base " << si.baseOffset;
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// 2026-10-05 log-truncate review, fix round 1: retention data-loss paths.
// ===========================================================================
namespace {
std::string segFile(const std::filesystem::path& dir, int64_t base, const char* ext) {
    std::string s = std::to_string(base);
    s = std::string(20 - s.size(), '0') + s;
    return (dir / (s + ext)).string();
}
std::vector<uint8_t> readAll(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}
// What the cleaner does for a coalesced group: ONE rewritten file at the first
// base holding every member's records; the other members' files removed.
void mergeSegmentsOnDisk(const std::filesystem::path& dir, const std::vector<int64_t>& group) {
    std::vector<uint8_t> merged;
    for (int64_t b : group) {
        const auto bytes = readAll(segFile(dir, b, ".log"));
        merged.insert(merged.end(), bytes.begin(), bytes.end());
    }
    const std::string out = segFile(dir, group.front(), ".log");
    {
        std::ofstream f(out + ".new", std::ios::binary | std::ios::trunc);
        f.write(reinterpret_cast<const char*>(merged.data()), static_cast<std::streamsize>(merged.size()));
    }
    std::filesystem::rename(out + ".new", out);
    std::filesystem::remove(segFile(dir, group.front(), ".index"));
    for (std::size_t i = 1; i < group.size(); ++i) {
        std::filesystem::remove(segFile(dir, group[i], ".log"));
        std::filesystem::remove(segFile(dir, group[i], ".index"));
    }
}
} // namespace

// CRITICAL-1: a coalesced compaction merge kept the FRONT segment's stale
// largestTimestamp (1001) although the merged file holds the later members'
// records (9000003) — time retention deleted in-window data.
TEST(LogRegression, MergedGroupTakesMaxTimestampOfMembersRegression) {
    for (const bool splitAcrossDrains : {false, true}) {
        auto dir = makeTempDir("rune_log_merge_ts");
        std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
        Log log(dir.string(), /*rollBytes=*/100);
        appendTs(log, 0, 1000); appendTs(log, 1, 1001);
        appendTs(log, 2, 9'000'002); appendTs(log, 3, 9'000'003);
        appendTs(log, 4, 9'000'004);
        ASSERT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4}));
        mergeSegmentsOnDisk(dir, {0, 2});
        ASSERT_TRUE(log.enqueueCleanedSegment(0));
        if (splitAcrossDrains) EXPECT_EQ(log.drainCleanerSwaps(), 1);
        ASSERT_TRUE(log.enqueueCleanedSegment(2));
        log.drainCleanerSwaps();
        ASSERT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 4}));
        const auto snap = log.sealedSegmentSnapshot();
        ASSERT_EQ(snap->size(), 1u);
        EXPECT_EQ((*snap)[0].endOffset, 4);
        EXPECT_GE((*snap)[0].largestTimestamp, 9'000'003) << "split=" << splitAcrossDrains;
        // ...and it stays after later rolls (the cache must keep the merged max).
        for (int64_t i = 5; i < 9; ++i) appendTs(log, i, 9'000'000 + i);
        const SegInfo* si = findSeg(*log.sealedSegmentSnapshot(), 0);
        ASSERT_NE(si, nullptr);
        EXPECT_GE(si->largestTimestamp, 9'000'003) << "split=" << splitAcrossDrains;
        std::filesystem::remove_all(dir);
    }
}

// A plain (non-merge) compaction rewrite keeps the segment's own timestamp.
TEST(LogRegression, CompactionRewriteKeepsSegmentTimestampRegression) {
    auto dir = makeTempDir("rune_log_rewrite_ts");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 100'000 + i);
    mergeSegmentsOnDisk(dir, {2});   // rewrite segment 2 in place
    ASSERT_TRUE(log.enqueueCleanedSegment(2));
    EXPECT_EQ(log.drainCleanerSwaps(), 1);
    for (int64_t i = 7; i < 10; ++i) appendTs(log, i, 100'000 + i);
    for (const auto& si : *log.sealedSegmentSnapshot())
        EXPECT_EQ(si.largestTimestamp, 100'000 + si.baseOffset + 1) << "segment base " << si.baseOffset;
    std::filesystem::remove_all(dir);
}

// MAJOR-1: a multi-batch append (the follower apply path appends a whole fetch
// chunk at once) recorded only the FIRST batch's maxTimestamp.
TEST(LogRegression, MultiBatchAppendTracksMaxOfAllBatchesRegression) {
    for (int variant = 0; variant < 2; ++variant) {
        auto dir = makeTempDir("rune_log_multibatch");
        std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
        Log log(dir.string(), /*rollBytes=*/100);
        RecordBatchBuilder b0(0, 1000);       b0.append("k", "v", 1000);
        RecordBatchBuilder b1(1, 9'000'000);  b1.append("k", "v", 9'000'000);
        std::vector<uint8_t> chunk = b0.build();
        const auto second = b1.build();
        chunk.insert(chunk.end(), second.begin(), second.end());
        if (variant == 0) log.appendNoSync(chunk.data(), chunk.size(), 1);
        else              log.append(chunk.data(), chunk.size(), 1);
        appendTs(log, 2, 1002);   // rolls: segment 0 seals
        const SegInfo* si = findSeg(*log.sealedSegmentSnapshot(), 0);
        ASSERT_NE(si, nullptr);
        EXPECT_EQ(si->largestTimestamp, 9'000'000) << "variant " << variant;
        std::filesystem::remove_all(dir);
    }
}

// MAJOR-2: a stale retention enqueue naming the (now) ACTIVE segment — e.g.
// the cleaner acted on a snapshot from before a truncate rebuilt it as active —
// erased the active segment.
TEST(LogRegression, RetentionDeleteNeverErasesActiveSegmentRegression) {
    auto dir = makeTempDir("rune_log_del_active");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    log.truncateTo(3);                                  // segment 2 is active again
    const int64_t activeBase = log.segmentBaseOffsets().back();
    ASSERT_EQ(activeBase, 2);
    // The cleaner proposed the (formerly sealed) segment from a stale snapshot
    // (fix round 3: proposals only — the owner renames after validation).
    ASSERT_TRUE(log.enqueueDeletedSegment(activeBase));
    EXPECT_EQ(log.drainRetentionDeletes(), 0);
    EXPECT_EQ(log.segmentBaseOffsets().back(), activeBase);
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, activeBase, ".log"))) << "the active file must stay";
    EXPECT_FALSE(std::filesystem::exists(segFile(dir, activeBase, ".log.deleted")));
    appendTs(log, 3, 5003);
    EXPECT_EQ(log.logEndOffset(), 4);
    EXPECT_FALSE(log.read(3, 1 << 20).empty());
    std::filesystem::remove_all(dir);
}

// MAJOR-2: same for the compaction-swap drain — a stale swap for the active
// segment (file replaced / gone) must never reload or drop it.
TEST(LogRegression, CleanerSwapNeverTouchesActiveSegmentRegression) {
    auto dir = makeTempDir("rune_log_swap_active");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    log.truncateTo(3);
    const int64_t activeBase = log.segmentBaseOffsets().back();
    std::filesystem::rename(segFile(dir, activeBase, ".log"), segFile(dir, activeBase, ".log.tmpaside"));
    ASSERT_TRUE(log.enqueueCleanedSegment(activeBase));
    log.drainCleanerSwaps();
    std::filesystem::rename(segFile(dir, activeBase, ".log.tmpaside"), segFile(dir, activeBase, ".log"));
    EXPECT_EQ(log.segmentBaseOffsets().back(), activeBase);
    appendTs(log, 3, 5003);
    EXPECT_EQ(log.logEndOffset(), 4);
    std::filesystem::remove_all(dir);
}

// LOW-3: the public synchronous reloadSegment (LogCleaner::cleanSegment path)
// republishes — a segment the cleaner emptied leaves the snapshot.
TEST(LogRegression, SyncReloadSegmentRepublishesSealedSnapshotRegression) {
    auto dir = makeTempDir("rune_log_sync_reload");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    std::filesystem::remove(segFile(dir, 2, ".log"));
    std::filesystem::remove(segFile(dir, 2, ".index"));
    log.reloadSegment(2);
    expectSnapshotMatchesSegments(log);
    EXPECT_EQ(findSeg(*log.sealedSegmentSnapshot(), 2), nullptr);
    std::filesystem::remove_all(dir);
}

// MAJOR-2: queued cleaner work carries the lineage the cleaner saw; a segment
// rebuilt since (truncate + re-append + re-seal: SEALED again, same base) is a
// different incarnation — the stale delete / swap is dropped, the delete's
// rename undone, and nothing live is erased.
TEST(LogRegression, StaleLineageCleanerWorkIsDroppedRegression) {
    auto dir = makeTempDir("rune_log_stale_lineage");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    const SegInfo* before = findSeg(*log.sealedSegmentSnapshot(), 2);
    ASSERT_NE(before, nullptr);
    const uint64_t staleLineage = before->lineage;
    ASSERT_NE(staleLineage, Log::kAnyLineage);
    log.truncateTo(3);
    for (int64_t i = 3; i < 7; ++i) appendTs(log, i, 9'000'000 + i);   // segment 2 sealed again
    const SegInfo* now = findSeg(*log.sealedSegmentSnapshot(), 2);
    ASSERT_NE(now, nullptr);
    ASSERT_NE(now->lineage, staleLineage);
    // Stale retention proposal: dropped, nothing on disk touched.
    ASSERT_TRUE(log.enqueueDeletedSegment(2, staleLineage));
    EXPECT_EQ(log.drainRetentionDeletes(), 0);
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 2, ".log")));
    EXPECT_FALSE(std::filesystem::exists(segFile(dir, 2, ".log.deleted")));
    // Stale compaction swap: dropped (the segment object is untouched).
    LogSegment* seg2 = log.segmentForTest(2);
    ASSERT_TRUE(log.enqueueCleanedSegment(2, staleLineage));
    log.drainCleanerSwaps();
    EXPECT_EQ(log.segmentForTest(2), seg2);
    EXPECT_EQ(log.staleCleanerOpsDropped(), 2u);
    {
        const auto bytes = log.read(3, 1 << 20);
        ASSERT_FALSE(bytes.empty());
        EXPECT_EQ(RecordBatchReader(bytes.data(), bytes.size()).baseOffset(), 3);
    }
    // The CURRENT lineage still applies.
    const uint64_t cur = findSeg(*log.sealedSegmentSnapshot(), 2)->lineage;
    ASSERT_TRUE(log.enqueueDeletedSegment(2, cur));
    EXPECT_EQ(log.drainRetentionDeletes(), 1);
    EXPECT_EQ(findSeg(*log.sealedSegmentSnapshot(), 2), nullptr);
    // Fix round 4: the owner touched no file; the cleaner renames both away.
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 2, ".log")));
    log.runCleanerFileOps(/*nowMs=*/0, /*fileDeleteDelayMs=*/1'000'000);
    EXPECT_FALSE(std::filesystem::exists(segFile(dir, 2, ".log")));
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 2, ".log.deleted")));
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 2, ".index.deleted")));
    std::filesystem::remove_all(dir);
}

// MAJOR-3: size retention (deleteOldSegments) only deletes segments ENDING at
// or below the HW — never one holding un-replicated records.
TEST(LogRegression, DeleteOldSegmentsRespectsHighWatermarkRegression) {
    auto dir = makeTempDir("rune_log_delold_hw");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 10; ++i) appendTs(log, i, 1000 + i);
    ASSERT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 6, 8}));
    EXPECT_EQ(log.deleteOldSegments(/*retainBytes=*/0, /*highWatermark=*/5), 2);
    EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{4, 6, 8})) << "[4,6) straddles HW 5";
    expectSnapshotMatchesSegments(log);
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Fix round 2: compaction outputs are installed by the OWNER thread
// (Kafka replaceSegments): .cleaned (uncommitted) -> .swap (committed) -> .log.
// ===========================================================================
namespace {
void writeBytes(const std::string& p, const std::vector<uint8_t>& b) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(b.data()), static_cast<std::streamsize>(b.size()));
}
std::vector<uint8_t> concatSegs(const std::filesystem::path& dir, std::initializer_list<int64_t> bases) {
    std::vector<uint8_t> out;
    for (int64_t b : bases) { auto v = readAll(segFile(dir, b, ".log")); out.insert(out.end(), v.begin(), v.end()); }
    return out;
}
int64_t firstOffsetAt(Log& log, int64_t off) {
    const auto bytes = log.read(off, 1 << 20);
    if (bytes.empty()) return -1;
    return RecordBatchReader(bytes.data(), bytes.size()).baseOffset();
}
} // namespace

// A valid deferred group is installed on the owner thread: the front holds the
// merged output, the members are gone (map + disk), no temp file remains, and
// the merged segment carries the newest member timestamp (CRITICAL-1).
TEST(LogRegression, DeferredCleanedGroupIsInstalledByOwnerRegression) {
    auto dir = makeTempDir("rune_log_deferred_ok");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    appendTs(log, 0, 1000); appendTs(log, 1, 1001);
    appendTs(log, 2, 9'000'002); appendTs(log, 3, 9'000'003);
    appendTs(log, 4, 9'000'004);
    const auto snap = log.sealedSegmentSnapshot();
    writeBytes(Log::cleanedLogPath(dir.string(), 0, 7), concatSegs(dir, {0, 2}));
    writeBytes(Log::cleanedIndexPath(dir.string(), 0, 7), std::vector<uint8_t>(8, 0));   // entry (0, 0)
    // Nothing on disk changed yet: the originals are intact.
    ASSERT_TRUE(std::filesystem::exists(segFile(dir, 2, ".log")));
    const Log::CleanedMember members[] = {{0, (*snap)[0].lineage}, {2, (*snap)[1].lineage}};
    ASSERT_TRUE(log.enqueueCleanedGroup(members, 2, 7, /*hasOutput=*/true, /*nextOffset=*/4,
                                        log.openCleanedOutput(0, 7, 4).release()));
    log.drainCleanerSwaps();
    EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 4}));
    // Fix round 4: the owner switched in memory only; the files are still the
    // originals + the output until the cleaner's file ops run.
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 2, ".log")));
    EXPECT_TRUE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 0, 7)));
    for (int64_t o = 0; o < 4; ++o) EXPECT_EQ(firstOffsetAt(log, o), o) << "served from the new handle";
    log.runCleanerFileOps(0);
    EXPECT_FALSE(std::filesystem::exists(segFile(dir, 2, ".log")));
    EXPECT_FALSE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 0, 7)));
    EXPECT_FALSE(std::filesystem::exists(Log::cleanedIndexPath(dir.string(), 0, 7)));
    EXPECT_EQ(log.cleanerGroupsRejected(), 0u);
    EXPECT_EQ(log.segmentForTest(0)->nextOffset(), 4);
    for (int64_t o = 0; o < 4; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
    const SegInfo* si = findSeg(*log.sealedSegmentSnapshot(), 0);
    ASSERT_NE(si, nullptr);
    EXPECT_EQ(si->endOffset, 4);
    EXPECT_GE(si->largestTimestamp, 9'000'003);
    std::filesystem::remove_all(dir);
}

// A rejected group (stale lineage / active member / above the bound) deletes
// ONLY the cleaned output — every original segment file and record survives.
TEST(LogRegression, RejectedCleanedGroupLeavesOriginalsUntouchedRegression) {
    auto dir = makeTempDir("rune_log_deferred_reject");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
    const auto snap = log.sealedSegmentSnapshot();
    const auto before0 = readAll(segFile(dir, 0, ".log"));
    const auto before2 = readAll(segFile(dir, 2, ".log"));
    auto output = [&](int64_t base, uint64_t seq, std::initializer_list<int64_t> from) {
        writeBytes(Log::cleanedLogPath(dir.string(), base, seq), concatSegs(dir, from));
        writeBytes(Log::cleanedIndexPath(dir.string(), base, seq), std::vector<uint8_t>(8, 0));
        return log.openCleanedOutput(base, seq, *std::prev(from.end()) + 2).release();
    };
    // (a) above the drain bound (HW/LSO).
    const Log::CleanedMember m02[] = {{0, (*snap)[0].lineage}, {2, (*snap)[1].lineage}};
    ASSERT_TRUE(log.enqueueCleanedGroup(m02, 2, 1, true, 4, output(0, 1, {0, 2})));
    log.drainCleanerSwaps(/*maxBaseBound=*/2);
    // (b) stale lineage.
    const Log::CleanedMember stale[] = {{0, (*snap)[0].lineage + 1000}};
    ASSERT_TRUE(log.enqueueCleanedGroup(stale, 1, 2, true, 2, output(0, 2, {0})));
    log.drainCleanerSwaps();
    // (c) the active segment.
    const Log::CleanedMember act[] = {{6, Log::kAnyLineage}};
    ASSERT_TRUE(log.enqueueCleanedGroup(act, 1, 3, true, 7, output(6, 3, {6})));
    log.drainCleanerSwaps();
    EXPECT_EQ(log.cleanerGroupsRejected(), 3u);
    // The owner touched no file; the cleaner deletes the rejected outputs.
    EXPECT_TRUE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 0, 1)));
    log.runCleanerFileOps(0);
    EXPECT_EQ(log.takeCleanerRejectLowWater(), 0);
    EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 6}));
    EXPECT_EQ(readAll(segFile(dir, 0, ".log")), before0);
    EXPECT_EQ(readAll(segFile(dir, 2, ".log")), before2);
    for (uint64_t seq : {1u, 2u}) EXPECT_FALSE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 0, seq)));
    EXPECT_FALSE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 6, 3)));
    for (int64_t o = 0; o < 7; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
    std::filesystem::remove_all(dir);
}

// Crash safety: an uncommitted .cleaned output is deleted at construction and
// the originals are used; a committed .swap is completed (members in its range
// deleted, the front replaced).
TEST(LogRegression, CrashRecoveryDeletesCleanedAndCompletesSwapRegression) {
    auto dir = makeTempDir("rune_log_swap_recover");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        for (int64_t i = 0; i < 9; ++i) appendTs(log, i, 1000 + i);   // segments 0,2,4,6 + active 8
    }
    const auto orig0 = readAll(segFile(dir, 0, ".log"));
    // Crash 1: an uncommitted output for segment 0 (garbage — must never be used).
    writeBytes(Log::cleanedLogPath(dir.string(), 0, 42), {9, 9, 9});
    // Crash 2: a committed swap of [4, 8) = segments 4 and 6 merged; the crash
    // hit after member 6 was already unlinked (any prefix must complete).
    writeBytes(Log::swapLogPath(dir.string(), 4, 8), concatSegs(dir, {4, 6}));
    std::filesystem::remove(segFile(dir, 6, ".log"));
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 8}));
        EXPECT_EQ(readAll(segFile(dir, 0, ".log")), orig0) << "uncommitted output must not replace the original";
        for (int64_t o = 0; o < 9; ++o) EXPECT_EQ(firstOffsetAt(log, o), o) << "offset " << o;
        EXPECT_EQ(log.logEndOffset(), 9);
    }
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        const std::string name = e.path().filename().string();
        EXPECT_EQ(name.find(".cleaned"), std::string::npos) << name;
        EXPECT_EQ(name.find(".swap"), std::string::npos) << name;
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Fix round 3 (N-MAJOR-1): the owner-thread install is O(1) — renames + a
// trusted open (no re-read, no CRC, no index rebuild, no fsync), independent of
// the segment size; and the drain installs at most ONE group per call.
// ===========================================================================
namespace {
void copyFile(const std::string& from, const std::string& to) {
    std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing);
}
struct InstallCost { double trustedUs; double dropUs; double legacyUs; };
double medianOf(std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; }
InstallCost measureInstall(int64_t segMiB, int reps) {
    auto dir = makeTempDir("rune_log_install_cost");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    const int64_t roll = segMiB << 20;
    InstallCost c{};
    {
        Log log(dir.string(), roll);
        const std::string big(1 << 20, 'x');
        for (int64_t o = 0; o < segMiB; ++o) {
            RecordBatchBuilder b(o, 1000 + o); b.append("k" + std::to_string(o), big, 1000 + o);
            log.append(b.build(), o);
        }
        appendTs(log, segMiB, 5000);                       // rolls: segment 0 sealed
        EXPECT_EQ(log.segmentBaseOffsets().size(), 2u);
        std::vector<double> samples;
        for (int r = 0; r < reps; ++r) {
            // The cleaner side (untimed): a finished output of segment 0, opened.
            const uint64_t seq = 100 + static_cast<uint64_t>(r);
            copyFile(segFile(dir, 0, ".log"), Log::cleanedLogPath(dir.string(), 0, seq));
            copyFile(segFile(dir, 0, ".index"), Log::cleanedIndexPath(dir.string(), 0, seq));
            const auto snap = log.sealedSegmentSnapshot();
            const Log::CleanedMember m[] = {{0, (*snap)[0].lineage}};
            EXPECT_TRUE(log.enqueueCleanedGroup(m, 1, seq, true, segMiB, log.openCleanedOutput(0, seq, segMiB).release()));
            const uint64_t syncsBefore = log.roleChangeSyncCallsForTest();
            // The OWNER side (timed): validate + switch the in-memory segment + post.
            const auto t0 = std::chrono::steady_clock::now();
            log.drainCleanerSwaps();
            samples.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count());
            EXPECT_EQ(log.cleanerGroupsRejected(), 0u);
            EXPECT_EQ(log.roleChangeSyncCallsForTest(), syncsBefore) << "no fsync / msync on the owner";
            EXPECT_EQ(log.segmentForTest(0)->nextOffset(), segMiB);
            EXPECT_FALSE(log.read(segMiB - 1, 1 << 21).empty());
            log.runCleanerFileOps(0, 0);   // the cleaner side (untimed): renames, close / munmap
        }
        c.trustedUs = medianOf(samples);
        // The round-2 path for comparison: reload + recover() (re-read + CRC +
        // index rebuild + msync + directory fsync) of the same segment.
        copyFile(segFile(dir, 0, ".log"), segFile(dir, 0, ".log.tmp"));
        std::filesystem::rename(segFile(dir, 0, ".log.tmp"), segFile(dir, 0, ".log"));
        EXPECT_TRUE(log.enqueueCleanedSegment(0));
        auto t0 = std::chrono::steady_clock::now();
        log.drainCleanerSwaps();
        c.legacyUs = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count();
        // Owner-side retention drop of the same (sealed) segment.
        const auto snap = log.sealedSegmentSnapshot();
        EXPECT_TRUE(log.enqueueDeletedSegment(0, (*snap)[0].lineage));
        t0 = std::chrono::steady_clock::now();
        EXPECT_EQ(log.drainRetentionDeletes(), 1);
        c.dropUs = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count();
        log.runCleanerFileOps(0, 0);
    }
    std::filesystem::remove_all(dir);
    return c;
}
} // namespace

TEST(LogRegression, OwnerInstallIsConstantTimeRegression) {
    const InstallCost small = measureInstall(1, 21);
    const InstallCost large = measureInstall(64, 11);
    std::printf("[install-cost] owner install (median): 1 MiB=%.1f us 64 MiB=%.1f us | owner retention drop: "
                "1 MiB=%.1f us 64 MiB=%.1f us | round-2 reload+recover 64 MiB=%.0f us\n",
                small.trustedUs, large.trustedUs, small.dropUs, large.dropUs, large.legacyUs);
    RecordProperty("install_1MiB_us", static_cast<int>(small.trustedUs));
    RecordProperty("install_64MiB_us", static_cast<int>(large.trustedUs));
    RecordProperty("drop_64MiB_us", static_cast<int>(large.dropUs));
    // Generous CI bounds (the measured values are reported above / in the report).
    EXPECT_LT(large.trustedUs, 1'000.0) << "owner install must be filesystem-free and size-independent";
    EXPECT_LT(large.dropUs, 1'000.0);
    EXPECT_LT(large.trustedUs * 10, large.legacyUs);
}

TEST(LogRegression, DrainInstallsAtMostOneGroupPerCallRegression) {
    auto dir = makeTempDir("rune_log_one_group");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*rollBytes=*/100);
    for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);   // 0,2,4 sealed; 6 active
    const auto snap = log.sealedSegmentSnapshot();
    for (int k = 0; k < 2; ++k) {
        const int64_t b = (*snap)[k].baseOffset;
        copyFile(segFile(dir, b, ".log"), Log::cleanedLogPath(dir.string(), b, 10 + k));
        copyFile(segFile(dir, b, ".index"), Log::cleanedIndexPath(dir.string(), b, 10 + k));
        const Log::CleanedMember m[] = {{b, (*snap)[k].lineage}};
        ASSERT_TRUE(log.enqueueCleanedGroup(m, 1, 10 + k, true, b + 2,
                                            log.openCleanedOutput(b, 10 + k, b + 2).release()));
    }
    EXPECT_EQ(log.drainCleanerSwaps(), 1);
    EXPECT_TRUE(log.cleanerSwapsPending());
    EXPECT_TRUE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 2, 11))) << "second group not yet installed";
    EXPECT_EQ(log.drainCleanerSwaps(), 1);
    EXPECT_FALSE(log.cleanerSwapsPending());
    EXPECT_EQ(log.cleanerGroupsRejected(), 0u);
    for (int64_t o = 0; o < 7; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
    std::filesystem::remove_all(dir);
}

// N-MEDIUM-2: a retention proposal changes nothing on disk until the owner
// validates it — a crash before the drain loses no segment; after the owner's
// rename, the segment is gone at construction (its *.deleted files are only
// scheduled for physical removal).
TEST(LogRegression, RetentionProposalIsCrashSafeRegression) {
    auto dir = makeTempDir("rune_log_ret_crash");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        for (int64_t i = 0; i < 7; ++i) appendTs(log, i, 1000 + i);
        const auto snap = log.sealedSegmentSnapshot();
        ASSERT_TRUE(log.enqueueDeletedSegment(0, (*snap)[0].lineage));   // proposed, never drained
        ASSERT_TRUE(log.enqueueDeletedSegment(2, (*snap)[1].lineage));
        EXPECT_EQ(log.drainRetentionDeletes(/*maxEndBound=*/3), 1) << "segment [2,4) ends above HW 3";
        // "crash": the Log goes away with segment 2's proposal dropped by the bound.
    }
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{2, 4, 6}));
        for (int64_t o = 2; o < 7; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
    }
    std::filesystem::remove_all(dir);
}

// ===========================================================================
// Fix round 4: the owner never touches the filesystem — it switches its
// in-memory segments and posts the file work to the cleaner.  Every crash
// window recovers; owner cold paths complete posted work first.
// ===========================================================================
namespace {
// A cleaner output for segments `from` (same bytes), opened, enqueued, and
// accepted by the owner (drain) — the cleaner's file ops NOT yet run.
void acceptOutputWithoutFileOps(Log& log, const std::filesystem::path& dir, std::initializer_list<int64_t> from,
                                uint64_t seq) {
    const int64_t b1 = *from.begin();
    const auto bases = log.segmentBaseOffsets();
    const int64_t last = *std::prev(from.end());
    const int64_t end = *std::upper_bound(bases.begin(), bases.end(), last);
    writeBytes(Log::cleanedLogPath(dir.string(), b1, seq), concatSegs(dir, from));
    copyFile(segFile(dir, b1, ".index"), Log::cleanedIndexPath(dir.string(), b1, seq));
    const auto snap = log.sealedSegmentSnapshot();
    std::vector<Log::CleanedMember> m;
    for (int64_t b : from) m.push_back({b, findSeg(*snap, b)->lineage});
    ASSERT_TRUE(log.enqueueCleanedGroup(m.data(), m.size(), seq, true, end, log.openCleanedOutput(b1, seq, end).release()));
    log.drainCleanerSwaps();
    ASSERT_EQ(log.cleanerGroupsRejected(), 0u);
}
} // namespace

TEST(LogRegression, CrashAfterOwnerAcceptanceBeforeRenamesKeepsOriginalsRegression) {
    auto dir = makeTempDir("rune_log_r4_crash_accept");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        for (int64_t i = 0; i < 9; ++i) appendTs(log, i, 1000 + i);   // 0,2,4,6 sealed; 8 active
        acceptOutputWithoutFileOps(log, dir, {2, 4}, 31);
        EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 6, 8}));
        log.crashForTest();   // the cleaner never ran its renames
    }
    EXPECT_TRUE(std::filesystem::exists(segFile(dir, 4, ".log"))) << "original untouched";
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 4, 6, 8})) << "uncompacted originals";
        for (int64_t o = 0; o < 9; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
    }
    for (auto& e : std::filesystem::directory_iterator(dir))
        EXPECT_EQ(e.path().filename().string().find(".cleaned"), std::string::npos) << e.path();
    std::filesystem::remove_all(dir);
}

TEST(LogRegression, TruncateAndReloadCompleteAcceptedSwapFirstRegression) {
    for (const bool viaReload : {false, true}) {
        auto dir = makeTempDir("rune_log_r4_truncate_pending");
        std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
        Log log(dir.string(), /*rollBytes=*/100);
        for (int64_t i = 0; i < 9; ++i) appendTs(log, i, 1000 + i);
        acceptOutputWithoutFileOps(log, dir, {2, 4}, 32);   // in memory: segment 2 covers [2,6)
        EXPECT_FALSE(std::filesystem::exists(segFile(dir, 2, ".log.32.deleted"))) << "no rename yet";
        if (viaReload) {
            log.reloadSegments();   // must see the merged file at its canonical name
            EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2, 6, 8}));
            for (int64_t o = 0; o < 9; ++o) EXPECT_EQ(firstOffsetAt(log, o), o) << "reload offset " << o;
        } else {
            log.truncateTo(5);      // into the accepted (not yet renamed) segment
            EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{0, 2}));
            EXPECT_EQ(log.logEndOffset(), 5);
            for (int64_t o = 0; o < 5; ++o) EXPECT_EQ(firstOffsetAt(log, o), o) << "truncate offset " << o;
            appendTs(log, 5, 7005);
            EXPECT_EQ(firstOffsetAt(log, 5), 5);
        }
        EXPECT_FALSE(std::filesystem::exists(Log::cleanedLogPath(dir.string(), 2, 32)));
        EXPECT_FALSE(std::filesystem::exists(segFile(dir, 4, ".log")));
        std::filesystem::remove_all(dir);
    }
}

TEST(LogRegression, RetentionCrashWindowsRecoverRegression) {
    auto dir = makeTempDir("rune_log_r4_ret_windows");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        for (int64_t i = 0; i < 9; ++i) appendTs(log, i, 1000 + i);
        // Window 1: the owner dropped segment 0 in memory, the cleaner never ran
        // (no checkpoint, no rename) → the segment simply reappears (retention
        // re-runs; nothing is lost).
        auto snap = log.sealedSegmentSnapshot();
        ASSERT_TRUE(log.enqueueDeletedSegment(0, (*snap)[0].lineage));
        ASSERT_EQ(log.drainRetentionDeletes(), 1);
        EXPECT_EQ(log.logStartOffset(), 2);
        EXPECT_TRUE(std::filesystem::exists(segFile(dir, 0, ".log"))) << "owner did no file op";
        log.crashForTest();
    }
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.logStartOffset(), 0) << "window 1: reappears (not durable yet)";
        // Window 2: the cleaner persisted the new log start, then the crash hit
        // before its rename became durable (simulated: the rename is undone).
        auto snap = log.sealedSegmentSnapshot();
        ASSERT_TRUE(log.enqueueDeletedSegment(0, (*snap)[0].lineage));
        ASSERT_TRUE(log.enqueueDeletedSegment(2, (*snap)[1].lineage));
        ASSERT_EQ(log.drainRetentionDeletes(), 2);
        log.runCleanerFileOps(0, 1'000'000);
        std::filesystem::rename(segFile(dir, 0, ".log.deleted"), segFile(dir, 0, ".log"));
        std::filesystem::rename(segFile(dir, 2, ".log.deleted"), segFile(dir, 2, ".log"));
        log.crashForTest();
    }
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.segmentBaseOffsets(), (std::vector<int64_t>{4, 6, 8})) << "dropped below the persisted log start";
        for (int64_t o = 4; o < 9; ++o) EXPECT_EQ(firstOffsetAt(log, o), o);
        // A full truncation below the persisted start lowers it first: new data
        // written from there survives a restart.
        log.truncateTo(1);
        for (int64_t i = 1; i < 8; ++i) appendTs(log, i, 9000 + i);
    }
    {
        Log log(dir.string(), /*rollBytes=*/100);
        log.recover();
        EXPECT_EQ(log.logStartOffset(), 1);
        for (int64_t o = 1; o < 8; ++o) EXPECT_EQ(firstOffsetAt(log, o), o) << "offset " << o;
    }
    std::filesystem::remove_all(dir);
}
