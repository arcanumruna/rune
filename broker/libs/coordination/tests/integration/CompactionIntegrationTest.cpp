// ---------------------------------------------------------------------------
// CompactionIntegrationTest.cpp
//
// P4-5: Log compaction end-to-end integration tests.
// Namespace: rune::coordination (tests live here, use rune::storage components).
//
// All tests:
//  - Are SINGLE-THREADED.
//  - Use the REAL Log + LogCleaner + OffsetMap from libs/storage.
//  - Assert compaction semantics: latest-per-key, tombstone, boundary safety,
//    offset preservation, consumer offset validity post-compaction.
//
// Test index (16 compaction integration tests):
//
//   Consumer-reads-compacted-topic:
//    1.  Compaction_ConsumerSeesOnlyLatestPerKey
//    2.  Compaction_OffsetsPreservedWithGaps
//    3.  Compaction_TombstoneDeletesKey
//    4.  Compaction_TombstoneRetainedBeforeRetentionExpires
//    5.  Compaction_TombstoneDroppedAfterRetentionExpires
//    6.  Compaction_CleanBoundaryUntouched
//    7.  Compaction_ActiveSegmentNeverCompacted
//    8.  Compaction_IdempotentTwoRounds
//    9.  Compaction_WhileProducing_BelowHWCompacted_AboveHWIntact
//   10.  Compaction_MultiKey_AllLatestSurvive
//   11.  Compaction_SingleKey_MultipleUpdates_KeepLast
//   12.  Compaction_OffsetMap_BuildAndQuery
//   Consumer offset validity post-compaction:
//   13.  Compaction_ConsumerOffsetStillValidAfterCompaction
//   14.  Compaction_ResumeAfterCompaction_NoDataLoss
//   Coordinator + compaction combined:
//   15.  CompactionAndGroup_CommittedOffsetRemainsValidPostCompact
//   16.  CompactionAndGroup_TombstoneKey_ConsumerNoLongerSeesKey
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/Types.h>

#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <filesystem>
#include <functional>
#include <limits>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::storage;
using namespace rune::protocol;
using namespace rune::coordination;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
static fs::path makeTempDir(const char* stem) {
    static std::atomic<uint64_t> g_seq{0};
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

static std::string makeOffsetPath(const char* stem) {
    static std::atomic<uint64_t> g_seq{0};
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    return (fs::temp_directory_path() / (std::string(stem) + "_" + n + ".log")).string();
}

// Build a single-record batch (key/value/offset/timestamp).
static std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                       std::string_view value, int64_t timestamp) {
    RecordBatchBuilder b(baseOffset, timestamp);
    b.append(key, value, timestamp);
    return b.build();
}

// Build a tombstone batch (empty value = delete marker).
static std::vector<uint8_t> makeTombstone(int64_t baseOffset, std::string_view key,
                                           int64_t timestamp) {
    return makeBatch(baseOffset, key, "", timestamp);
}

// Collect all records from log starting at startOffset, iterating segments.
struct Record { std::string key, value; int64_t offset, timestamp; };
static std::vector<Record> readAllRecords(Log& log, int64_t startOffset = 0) {
    std::vector<Record> out;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return out;

    int64_t cursor = startOffset;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];

        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
                out.push_back({std::string(k), std::string(v), off, ts});
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return out;
}

static int64_t latestOffsetForKey(const std::vector<Record>& recs, std::string_view key) {
    int64_t latest = -1;
    for (auto& r : recs) if (r.key == key) latest = r.offset;
    return latest;
}

static int countKey(const std::vector<Record>& recs, std::string_view key) {
    int n = 0; for (auto& r : recs) if (r.key == key) ++n; return n;
}

// ===========================================================================
// Group 1: Consumer reads compacted topic
// ===========================================================================

// Test 1: After compaction, consumer sees ONLY the latest value per key.
TEST(CompactionIntegration, ConsumerSeesOnlyLatestPerKey) {
    auto dir = makeTempDir("ci_t1");
    Log log(dir.string(), 512);

    // k1@0=v1, k1@1=v2, k2@2=v3, k1@3=v4 — expect k1@3, k2@2 survive.
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    log.append(makeBatch(2, "k2", "v3", 1002), 2);
    log.append(makeBatch(3, "k1", "v4", 1003), 3);
    // Force into non-active segment.
    for (int i = 4; i < 30; ++i)
        log.append(makeBatch(i, "pad", "x", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);

    // k1 appears exactly once (offset 3).
    EXPECT_EQ(countKey(recs, "k1"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 3);
    // k2 appears exactly once.
    EXPECT_EQ(countKey(recs, "k2"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "k2"), 2);

    fs::remove_all(dir);
}

// Test 2: Offsets are preserved with gaps after compaction.
TEST(CompactionIntegration, OffsetsPreservedWithGaps) {
    auto dir = makeTempDir("ci_t2");
    Log log(dir.string(), 512);

    log.append(makeBatch(0, "k1", "a", 1000), 0);
    log.append(makeBatch(1, "k1", "b", 1001), 1); // superseded
    log.append(makeBatch(2, "k2", "c", 1002), 2);
    log.append(makeBatch(3, "k1", "d", 1003), 3); // latest k1
    for (int i = 4; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    // k1@3 survives, k2@2 survives. Gap at offset 0 and 1.
    bool foundK2at2 = false, foundK1at3 = false;
    for (auto& r : recs) {
        if (r.key == "k2") { EXPECT_EQ(r.offset, 2); foundK2at2 = true; }
        if (r.key == "k1") { EXPECT_EQ(r.offset, 3); foundK1at3 = true; }
    }
    EXPECT_TRUE(foundK2at2) << "k2@2 must survive";
    EXPECT_TRUE(foundK1at3) << "k1@3 must survive";

    fs::remove_all(dir);
}

// Test 3: Tombstone deletes the key (consumer no longer sees it after retention).
TEST(CompactionIntegration, TombstoneDeletesKey) {
    auto dir = makeTempDir("ci_t3");
    Log log(dir.string(), 512);

    // Tombstone timestamp = 1001 (same unit as the LogCleaner comparison field).
    // The LogCleaner expiry condition: (nowNs - tombstoneTs) > deleteRetentionNs.
    // With deleteRetentionMs=0 (deleteRetentionNs=0), we need nowNs > 1001.
    int64_t tombstoneTs = 1001;
    log.append(makeBatch(0, "k1", "value", 1000), 0);
    log.append(makeTombstone(1, "k1", tombstoneTs), 1);
    for (int i = 2; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    // deleteRetentionMs=0, nowNs well past tombstoneTs → tombstone expired.
    cleaner.clean(log, 0, 2, /*nowNs=*/tombstoneTs + 1'000'000LL, /*deleteRetentionMs=*/0);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 0) << "k1 must be gone after tombstone + retention";

    fs::remove_all(dir);
}

// Test 4: Tombstone retained if deletion retention has NOT expired.
TEST(CompactionIntegration, TombstoneRetainedBeforeRetentionExpires) {
    auto dir = makeTempDir("ci_t4");
    Log log(dir.string(), 512);

    int64_t tombstoneTs = 5'000; // ms as timestamp
    log.append(makeBatch(0, "k1", "val", 1000), 0);
    log.append(makeTombstone(1, "k1", tombstoneTs), 1);
    for (int i = 2; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    // deleteRetentionMs=1_000_000 (very large) → tombstone must be retained.
    cleaner.clean(log, 0, 2, /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    // Tombstone for k1 should be present (empty value = tombstone retained).
    bool foundTombstone = false;
    for (auto& r : recs) {
        if (r.key == "k1" && r.value.empty()) { foundTombstone = true; }
    }
    EXPECT_TRUE(foundTombstone) << "Tombstone for k1 must be retained before retention window";

    fs::remove_all(dir);
}

// Test 5: Tombstone dropped after deleteRetentionMs expires.
TEST(CompactionIntegration, TombstoneDroppedAfterRetentionExpires) {
    auto dir = makeTempDir("ci_t5");
    Log log(dir.string(), 512);

    // Tombstone timestamp = 1000ms = 1e9 ns.
    int64_t tsNs = 1'000'000'000LL;
    log.append(makeBatch(0, "k1", "val", 1000), 0);
    log.append(makeTombstone(1, "k1", tsNs / 1'000'000), 1); // ts in ms for batch builder
    for (int i = 2; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    // nowNs well past tombstone ts + 0ms retention → tombstone expired.
    cleaner.clean(log, 0, 2, /*nowNs=*/tsNs + 10'000'000LL, /*deleteRetentionMs=*/0);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 0) << "Tombstone must be dropped after retention expires";

    fs::remove_all(dir);
}

// Test 6: Records at or past cleanBoundaryOffset are untouched.
TEST(CompactionIntegration, CleanBoundaryUntouched) {
    auto dir = makeTempDir("ci_t6");
    Log log(dir.string(), 512);

    // k1 updated many times; cleanBoundary = 3 → offsets 0-2 compacted, 3+ intact.
    log.append(makeBatch(0, "k1", "old0", 1000), 0);
    log.append(makeBatch(1, "k1", "old1", 1001), 1);
    log.append(makeBatch(2, "k1", "old2", 1002), 2);
    // These are beyond the boundary.
    log.append(makeBatch(3, "k1", "new0", 2000), 3);
    log.append(makeBatch(4, "k1", "new1", 2001), 4);
    for (int i = 5; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 3000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    // Records at offset 3 and 4 must still be present (past boundary).
    int countAboveBoundary = 0;
    for (auto& r : recs) {
        if (r.key == "k1" && r.offset >= 3) ++countAboveBoundary;
    }
    EXPECT_GE(countAboveBoundary, 2) << "Records past cleanBoundary must be untouched";

    fs::remove_all(dir);
}

// Test 7: Active segment (highest base offset) is NEVER compacted.
TEST(CompactionIntegration, ActiveSegmentNeverCompacted) {
    auto dir = makeTempDir("ci_t7");
    Log log(dir.string(), 64); // tiny roll to force many segments

    // Fill many segments. Active = last one.
    int64_t off = 0;
    for (int i = 0; i < 50; ++i) {
        log.append(makeBatch(off, "k", "v" + std::to_string(i), 1000 + i), off);
        off++;
    }
    int64_t leo = log.logEndOffset();
    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for this test";
    int64_t activeBase = bases.back();

    LogCleaner cleaner;
    // compact up to the active segment base offset — active segment must be untouched.
    cleaner.clean(log, 0, activeBase, 0, 1'000'000);
    log.reloadSegments();

    // logEndOffset must be unchanged (active segment not truncated).
    EXPECT_EQ(log.logEndOffset(), leo)
        << "Active segment must not be modified by compaction";

    // Records in the active segment must be readable.
    auto recs = readAllRecords(log, activeBase);
    EXPECT_FALSE(recs.empty()) << "Active segment records must still be readable post-compaction";

    fs::remove_all(dir);
}

// Test 8: Compaction is idempotent — running it twice yields same result.
TEST(CompactionIntegration, IdempotentTwoRounds) {
    auto dir = makeTempDir("ci_t8");
    Log log(dir.string(), 512);

    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    log.append(makeBatch(2, "k2", "v3", 1002), 2);
    log.append(makeBatch(3, "k1", "v4", 1003), 3);
    for (int i = 4; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);
    log.reloadSegments();
    auto recs1 = readAllRecords(log, 0);

    // Second compaction run.
    cleaner.clean(log, 0, 4, 0, 1'000'000);
    log.reloadSegments();
    auto recs2 = readAllRecords(log, 0);

    // Same keys/offsets — idempotent.
    EXPECT_EQ(recs1.size(), recs2.size()) << "Compaction must be idempotent";
    for (size_t i = 0; i < std::min(recs1.size(), recs2.size()); ++i) {
        EXPECT_EQ(recs1[i].key, recs2[i].key);
        EXPECT_EQ(recs1[i].offset, recs2[i].offset);
    }

    fs::remove_all(dir);
}

// Test 9: Compaction while producing — records at/above HW untouched, below HW compacted.
TEST(CompactionIntegration, WhileProducing_BelowHWCompacted_AboveHWIntact) {
    auto dir = makeTempDir("ci_t9");
    Log log(dir.string(), 512);

    // Produce keys 0..5 (committed range).
    int64_t off = 0;
    for (int i = 0; i < 6; ++i) {
        log.append(makeBatch(off, "k" + std::to_string(i % 2), "v" + std::to_string(i), 1000 + i), off);
        off++;
    }
    // Simulate HW = 4 (records 0-3 are committed).
    int64_t hw = 4;

    // Records 4 and 5 are "above HW" (not yet committed, or active zone).
    // Force into a non-active segment for 0-3.
    for (int i = 6; i < 30; ++i) {
        log.append(makeBatch(off, "pad", "p", 2000 + i), off);
        off++;
    }

    LogCleaner cleaner;
    // Only compact [0, hw=4).
    cleaner.clean(log, 0, hw, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);

    // Records at or above hw must exist (offsets 4, 5 must still be present).
    int countAboveHw = 0;
    for (auto& r : recs) {
        if (r.offset >= hw) ++countAboveHw;
    }
    EXPECT_GE(countAboveHw, 2) << "Records at/above HW must not be compacted";

    // Compacted range: k0 and k1 should each appear at most once (latest only).
    int countK0 = countKey(recs, "k0");
    int countK1 = countKey(recs, "k1");
    // Within [0,4): k0 at offsets 0,2 → latest=2; k1 at offsets 1,3 → latest=3.
    EXPECT_LE(countK0, 2) << "k0 at most appears once in compacted + once in uncompacted zones";
    EXPECT_LE(countK1, 2) << "k1 at most appears once in compacted + once in uncompacted zones";

    fs::remove_all(dir);
}

// Test 10: Multiple distinct keys — ALL latest values survive.
TEST(CompactionIntegration, MultiKey_AllLatestSurvive) {
    auto dir = makeTempDir("ci_t10");
    Log log(dir.string(), 512);

    // 5 keys, 3 updates each.
    int64_t off = 0;
    for (int upd = 0; upd < 3; ++upd) {
        for (int k = 0; k < 5; ++k) {
            log.append(makeBatch(off, "k" + std::to_string(k),
                                  "v" + std::to_string(k) + "_" + std::to_string(upd),
                                  1000 + off), off);
            off++;
        }
    }
    int64_t boundary = off;
    for (int i = 0; i < 20; ++i) {
        log.append(makeBatch(off, "pad", "p", 3000 + i), off);
        off++;
    }

    LogCleaner cleaner;
    cleaner.clean(log, 0, boundary, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    for (int k = 0; k < 5; ++k) {
        std::string key = "k" + std::to_string(k);
        EXPECT_EQ(countKey(recs, key), 1)
            << key << " must appear exactly once (latest only)";
    }

    fs::remove_all(dir);
}

// Test 11: Single key updated many times — only the very last survives.
TEST(CompactionIntegration, SingleKey_MultipleUpdates_KeepLast) {
    auto dir = makeTempDir("ci_t11");
    Log log(dir.string(), 512);

    int64_t off = 0;
    for (int i = 0; i < 10; ++i) {
        log.append(makeBatch(off, "k1", "v" + std::to_string(i), 1000 + i), off);
        off++;
    }
    int64_t boundary = off;
    for (int i = 0; i < 20; ++i) {
        log.append(makeBatch(off, "pad", "p", 3000 + i), off);
        off++;
    }

    LogCleaner cleaner;
    cleaner.clean(log, 0, boundary, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1) << "Only 1 record for k1 after 10 updates";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 9) << "Surviving k1 must be at offset 9 (last)";

    fs::remove_all(dir);
}

// Test 12: OffsetMap builds correctly and get() returns latest.
TEST(CompactionIntegration, OffsetMap_BuildAndQuery) {
    auto dir = makeTempDir("ci_t12");
    Log log(dir.string(), 512);

    log.append(makeBatch(0, "a", "v0", 1000), 0);
    log.append(makeBatch(1, "b", "v1", 1001), 1);
    log.append(makeBatch(2, "a", "v2", 1002), 2);
    log.append(makeBatch(3, "c", "v3", 1003), 3);
    log.append(makeBatch(4, "b", "v4", 1004), 4);

    OffsetMap map;
    LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 0, 5, map);

    ASSERT_TRUE(map.get("a").has_value());
    ASSERT_TRUE(map.get("b").has_value());
    ASSERT_TRUE(map.get("c").has_value());
    EXPECT_EQ(*map.get("a"), 2) << "Latest 'a' is at offset 2";
    EXPECT_EQ(*map.get("b"), 4) << "Latest 'b' is at offset 4";
    EXPECT_EQ(*map.get("c"), 3) << "Latest 'c' is at offset 3";

    fs::remove_all(dir);
}

// ===========================================================================
// Group 2: Consumer offset validity post-compaction
// ===========================================================================

// Test 13: A consumer's committed offset is still valid (readable) after compaction.
TEST(CompactionIntegration, ConsumerOffsetStillValidAfterCompaction) {
    auto dir = makeTempDir("ci_t13");
    Log log(dir.string(), 512);

    // Produce k1 three times.
    log.append(makeBatch(0, "k1", "v0", 1000), 0);
    log.append(makeBatch(1, "k1", "v1", 1001), 1);
    log.append(makeBatch(2, "k2", "v2", 1002), 2);
    log.append(makeBatch(3, "k1", "v3", 1003), 3); // latest k1
    for (int i = 4; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // Consumer committed at offset 3 (just saw latest k1).
    std::string offsetPath = makeOffsetPath("ci_os_t13");
    {
        OffsetStore store(offsetPath);
        store.commit("grp", {"t", 0}, 3, "ck13", 0);
    }

    // Compact [0,4).
    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);
    log.reloadSegments();

    // Consumer's committed offset (3) must still be readable.
    auto bytes = log.read(3, 1 << 20);
    EXPECT_FALSE(bytes.empty()) << "Committed offset 3 must still be readable after compaction";

    // OffsetStore must still have the committed value.
    OffsetStore store2(offsetPath);
    auto co = store2.fetch("grp", {"t", 0});
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 3);

    fs::remove(offsetPath);
    fs::remove_all(dir);
}

// Test 14: Consumer resumes from committed offset after compaction — no data loss.
TEST(CompactionIntegration, ResumeAfterCompaction_NoDataLoss) {
    auto dir = makeTempDir("ci_t14");
    Log log(dir.string(), 512);

    // Three keys, multiple updates.
    int64_t off = 0;
    { auto b = makeBatch(off, "a", "a0", 1000); log.append(b, off); off++; }
    { auto b = makeBatch(off, "b", "b0", 1001); log.append(b, off); off++; }
    { auto b = makeBatch(off, "a", "a1", 1002); log.append(b, off); off++; }
    { auto b = makeBatch(off, "c", "c0", 1003); log.append(b, off); off++; }
    { auto b = makeBatch(off, "b", "b1", 1004); log.append(b, off); off++; }
    int64_t boundary = off;
    for (int i = 0; i < 20; ++i) {
        log.append(makeBatch(off, "pad", "p", 3000 + i), off);
        off++;
    }

    // Compact [0, boundary).
    LogCleaner cleaner;
    cleaner.clean(log, 0, boundary, 0, 1'000'000);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);

    // All three latest values must be present.
    EXPECT_EQ(countKey(recs, "a"), 1) << "latest 'a' must survive";
    EXPECT_EQ(countKey(recs, "b"), 1) << "latest 'b' must survive";
    EXPECT_EQ(countKey(recs, "c"), 1) << "latest 'c' must survive";

    // Resume from compacted offset — should still see the surviving records.
    int64_t survivingA = latestOffsetForKey(recs, "a");
    EXPECT_GE(survivingA, 0) << "'a' must have a valid offset";
    auto resumeBytes = log.read(survivingA, 1 << 20);
    EXPECT_FALSE(resumeBytes.empty())
        << "Consumer resuming from compacted offset must still read data";

    fs::remove_all(dir);
}

// ===========================================================================
// Group 3: Coordinator + compaction combined
// ===========================================================================

// Test 15: Committed consumer offset remains valid after log compaction.
TEST(CompactionIntegration, CommittedOffsetRemainsValidPostCompact) {
    auto dir = makeTempDir("ci_t15");
    Log log(dir.string(), 512);

    // Produce records for key "k1" at offsets 0-4, "k2" at offsets 5-9.
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        log.append(makeBatch(off, "k1", "v1_" + std::to_string(i), 1000 + i), off);
        off++;
    }
    for (int i = 0; i < 5; ++i) {
        log.append(makeBatch(off, "k2", "v2_" + std::to_string(i), 2000 + i), off);
        off++;
    }
    int64_t boundary = off;
    for (int i = 0; i < 20; ++i) {
        log.append(makeBatch(off, "pad", "p", 3000 + i), off);
        off++;
    }

    // Coordinator + offset store: consumer commits at offset 4 (latest k1)
    //                             and at offset 9 (latest k2).
    std::string osPath = makeOffsetPath("ci_os_t15");
    {
        OffsetStore store(osPath);
        store.commit("grp", {"t", 0}, 4, "", 0);  // committed after seeing k1@4
        store.commit("grp", {"t", 1}, 9, "", 0);  // committed after seeing k2@9
    }

    // Compact [0, boundary).
    LogCleaner cleaner;
    cleaner.clean(log, 0, boundary, 0, 1'000'000);
    log.reloadSegments();

    // Committed offsets must still be valid reads.
    auto bytes4 = log.read(4, 1 << 20);
    EXPECT_FALSE(bytes4.empty()) << "Offset 4 (committed by consumer) must be readable post-compaction";
    auto bytes9 = log.read(9, 1 << 20);
    EXPECT_FALSE(bytes9.empty()) << "Offset 9 (committed by consumer) must be readable post-compaction";

    // OffsetStore must still hold the committed values.
    OffsetStore store2(osPath);
    auto co0 = store2.fetch("grp", {"t", 0});
    auto co1 = store2.fetch("grp", {"t", 1});
    ASSERT_TRUE(co0.has_value());
    ASSERT_TRUE(co1.has_value());
    EXPECT_EQ(co0->offset, 4);
    EXPECT_EQ(co1->offset, 9);

    fs::remove(osPath);
    fs::remove_all(dir);
}

// Test 16: Tombstone key — after compaction consumer no longer sees the key.
TEST(CompactionIntegration, TombstoneKey_ConsumerNoLongerSeesKey) {
    auto dir = makeTempDir("ci_t16");
    Log log(dir.string(), 512);

    // Produce "secret_key" and then tombstone it.
    log.append(makeBatch(0, "secret", "sensitive_data", 1000), 0);
    log.append(makeBatch(1, "other",  "keep_me", 1001), 1);
    log.append(makeTombstone(2, "secret", 1002), 2);  // delete intent
    for (int i = 3; i < 30; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // Simulate coordinator tracking that a consumer committed at offset 2.
    std::string osPath = makeOffsetPath("ci_os_t16");
    {
        OffsetStore store(osPath);
        store.commit("grp", {"t", 0}, 2, "", 0);
    }

    // Compact with deleteRetentionMs=0 (tombstone past retention immediately).
    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, /*nowNs=*/10'000'000'000LL, /*deleteRetentionMs=*/0);
    log.reloadSegments();

    auto recs = readAllRecords(log, 0);

    // "secret" must not appear in any form (tombstone was dropped after retention).
    EXPECT_EQ(countKey(recs, "secret"), 0)
        << "Tombstoned key must not appear after retention expires";

    // "other" must still be present.
    EXPECT_EQ(countKey(recs, "other"), 1);

    // Committed offset store is unchanged by compaction.
    OffsetStore store2(osPath);
    auto co = store2.fetch("grp", {"t", 0});
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 2);

    fs::remove(osPath);
    fs::remove_all(dir);
}
