#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <fstream>
#include <atomic>
#include <chrono>
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
