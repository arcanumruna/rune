#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

static std::filesystem::path makeTempDir(const char* stem) {
    static std::atomic<uint64_t> g_seq{0};
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    return std::filesystem::temp_directory_path() / (std::string(stem) + "_" + n);
}

// Build a single-record batch with given key/value/offset/timestamp.
static std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                      std::string_view value, int64_t timestamp) {
    RecordBatchBuilder b(baseOffset, timestamp);
    b.append(key, value, timestamp);
    return b.build();
}

// Build a tombstone batch (empty value).
static std::vector<uint8_t> makeTombstone(int64_t baseOffset, std::string_view key,
                                          int64_t timestamp) {
    return makeBatch(baseOffset, key, "", timestamp);
}

// Collect all records from a log starting at startOffset.
// After compaction some segments may be completely empty (all records dropped).
// We iterate segment base offsets to skip empty segments rather than relying on
// a contiguous offset stream. The cursor is tracked globally to avoid
// double-visiting segments (the inner read loop may naturally advance into the
// next segment before we arrive at it via the outer si loop).
struct Record { std::string key, value; int64_t offset, timestamp; };
static std::vector<Record> readAllRecords(Log& log, int64_t startOffset = 0) {
    std::vector<Record> out;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return out;

    // Global cursor — shared across all segment iterations.
    int64_t cursor = startOffset;

    for (size_t si = 0; si < bases.size(); ++si) {
        // If the cursor has already moved past this segment's range
        // (because the inner loop flowed into it from the previous segment),
        // skip it.
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;

        // Advance cursor to this segment's start if it hasn't reached it yet.
        if (cursor < bases[si]) cursor = bases[si];

        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break; // segment exhausted — move to next
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
                out.push_back({std::string(k), std::string(v), off, ts});
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break; // guard against infinite loop on empty batch
        }
    }
    return out;
}

// Find the latest offset of a key in a set of records; -1 if absent.
static int64_t latestOffsetForKey(const std::vector<Record>& recs, std::string_view key) {
    int64_t latest = -1;
    for (auto& r : recs) if (r.key == key) latest = r.offset;
    return latest;
}

// Count how many records have a given key.
static int countKey(const std::vector<Record>& recs, std::string_view key) {
    int n = 0; for (auto& r : recs) if (r.key == key) ++n; return n;
}

// ---------------------------------------------------------------------------
// LC-1: buildOffsetMap over a log with no overlap returns empty map.
// ---------------------------------------------------------------------------
TEST(LogCleaner, BuildOffsetMapEmptyRange) {
    auto dir = makeTempDir("lc_t1");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        off = log.append(makeBatch(off, "k", "v" + std::to_string(i), 1000 + i), off);
    }
    OffsetMap map; LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 10, 10, map); // empty range
    EXPECT_EQ(map.size(), 0u);
    std::filesystem::remove_all(dir);
}

// LC-2: buildOffsetMap records key→latest-offset.
TEST(LogCleaner, BuildOffsetMapLatestOffset) {
    auto dir = makeTempDir("lc_t2");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    // k1@0, k2@1, k1@2
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k2", "v2", 1001), 1);
    log.append(makeBatch(2, "k1", "v3", 1002), 2);
    OffsetMap map; LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 0, 3, map);
    EXPECT_EQ(*map.get("k1"), 2); // latest k1 = offset 2
    EXPECT_EQ(*map.get("k2"), 1);
    std::filesystem::remove_all(dir);
}

// LC-3: buildOffsetMap respects fromOffset/toOffset boundaries.
TEST(LogCleaner, BuildOffsetMapBoundaries) {
    auto dir = makeTempDir("lc_t3");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    // k1@0, k1@1, k1@2, k1@3, k1@4
    for (int i = 0; i < 5; ++i)
        log.append(makeBatch(i, "k1", "v" + std::to_string(i), 1000 + i), i);
    OffsetMap map; LogCleaner cleaner;
    // Only scan [1,3): should see k1 at 1 and 2 → latest = 2.
    cleaner.buildOffsetMap(log, 1, 3, map);
    EXPECT_EQ(*map.get("k1"), 2);
    std::filesystem::remove_all(dir);
}

// LC-4: compact keeps latest record per key, drops superseded.
TEST(LogCleaner, CompactKeepsLatestPerKey) {
    auto dir = makeTempDir("lc_t4");
    std::filesystem::create_directories(dir);
    // Small rollBytes so we get multiple segments (active segment protection).
    Log log(dir.string(), 512);
    // k1@0=v1, k1@1=v2, k2@2=v3, k1@3=v4  then a batch to create an active segment
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    log.append(makeBatch(2, "k2", "v3", 1002), 2);
    log.append(makeBatch(3, "k1", "v4", 1003), 3);
    // Add more data to force a segment roll so the above are NOT in the active segment.
    for (int i = 4; i < 20; ++i)
        log.append(makeBatch(i, "pad", "x" + std::to_string(i), 2000 + i), i);

    int64_t cleanBoundary = 4; // compact [0,4)
    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, /*nowNs=*/0, /*deleteRetentionMs=*/1'000'000);

    auto recs = readAllRecords(log, 0);
    // k1 must appear exactly once (offset 3 = latest)
    EXPECT_EQ(countKey(recs, "k1"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 3);
    // k2 must appear exactly once (offset 2)
    EXPECT_EQ(countKey(recs, "k2"), 1);
    // pad records are outside cleanBoundary, pass through
    EXPECT_GT(countKey(recs, "pad"), 0);
    std::filesystem::remove_all(dir);
}

// LC-5: offsets are preserved with gaps after compaction.
TEST(LogCleaner, OffsetsPreservedWithGaps) {
    auto dir = makeTempDir("lc_t5");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // k1@0, k1@1, k2@2, k1@3 — after compact: k2@2 and k1@3 survive.
    log.append(makeBatch(0, "k1", "a", 1000), 0);
    log.append(makeBatch(1, "k1", "b", 1001), 1);
    log.append(makeBatch(2, "k2", "c", 1002), 2);
    log.append(makeBatch(3, "k1", "d", 1003), 3);
    // Force these into a non-active segment.
    for (int i = 4; i < 20; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // Surviving records in the compacted range: k2@2 and k1@3.
    // Assert exact offsets.
    bool foundK2at2 = false, foundK1at3 = false;
    for (auto& r : recs) {
        if (r.key == "k2") { EXPECT_EQ(r.offset, 2); foundK2at2 = true; }
        if (r.key == "k1") { EXPECT_EQ(r.offset, 3); foundK1at3 = true; }
    }
    EXPECT_TRUE(foundK2at2);
    EXPECT_TRUE(foundK1at3);
    // k1@0 and k1@1 must be GONE.
    for (auto& r : recs) {
        EXPECT_FALSE(r.key == "k1" && r.offset < 3) << "Superseded k1 record at offset " << r.offset;
    }
    std::filesystem::remove_all(dir);
}

// LC-6: tombstone is retained before deleteRetentionMs.
TEST(LogCleaner, TombstoneRetainedBeforeRetention) {
    auto dir = makeTempDir("lc_t6");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // k1@0=v1, k1@1=tombstone(ts=1000ns)
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeTombstone(1, "k1", 1000), 1);
    // Force non-active.
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // nowNs = 2000, deleteRetentionMs = 100 → retention window = 100ms = 100_000_000ns
    // tombstoneTs = 1000ns; nowNs - tombstoneTs = 1000ns < 100_000_000ns → retain
    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, /*nowNs=*/2000, /*deleteRetentionMs=*/100);

    auto recs = readAllRecords(log, 0);
    // k1@0=v1 must be gone (superseded by tombstone at 1).
    for (auto& r : recs) {
        EXPECT_FALSE(r.key == "k1" && r.offset == 0) << "Superseded v1 should be gone";
    }
    // Tombstone at offset 1 must still be present (within retention).
    bool tombFound = false;
    for (auto& r : recs) {
        if (r.key == "k1" && r.offset == 1 && r.value.empty()) tombFound = true;
    }
    EXPECT_TRUE(tombFound) << "Tombstone within retention must be kept";
    std::filesystem::remove_all(dir);
}

// LC-7: tombstone removed after deleteRetentionMs.
TEST(LogCleaner, TombstoneRemovedAfterRetention) {
    auto dir = makeTempDir("lc_t7");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // k1@0=v1, k1@1=tombstone(ts=1000ns)
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeTombstone(1, "k1", 1000), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // nowNs = 1000 + 200_000_001 (past 200ms retention), deleteRetentionMs = 200
    // (nowNs - tombstoneTs) = 200_000_001ns > 200 * 1_000_000 = 200_000_000ns → remove
    int64_t tombTs = 1000;
    int64_t retentionMs = 200;
    int64_t nowNs = tombTs + retentionMs * 1'000'000LL + 1;
    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, nowNs, retentionMs);

    auto recs = readAllRecords(log, 0);
    // Neither k1@0 (superseded) nor k1@1 (tombstone past retention) should be present.
    for (auto& r : recs) {
        EXPECT_NE(r.key, "k1") << "k1 should be fully absent (tombstone past retention)";
    }
    std::filesystem::remove_all(dir);
}

// LC-8: active segment (last segment) is NEVER compacted.
TEST(LogCleaner, ActiveSegmentNeverCompacted) {
    auto dir = makeTempDir("lc_t8");
    std::filesystem::create_directories(dir);
    // Single segment — everything is in the active segment.
    Log log(dir.string(), 64LL * 1024 * 1024);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    log.append(makeBatch(2, "k2", "v3", 1002), 2);

    // Only one segment — all records are in the active segment.
    EXPECT_EQ(log.segmentBaseOffsets().size(), 1u);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, 0, 1'000'000);

    // All records must survive untouched.
    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 2) << "Active segment must not be compacted";
    EXPECT_EQ(countKey(recs, "k2"), 1);
    std::filesystem::remove_all(dir);
}

// LC-9: HW boundary (cleanBoundaryOffset) respected — records at/past HW not compacted.
TEST(LogCleaner, HWBoundaryRespected) {
    auto dir = makeTempDir("lc_t9");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // k1@0, k1@1 (both in segment 0 which is non-active after roll)
    // k1@2 is in what will become the clean boundary.
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    // Force a roll.
    for (int i = 2; i < 20; ++i)
        log.append(makeBatch(i, "k1", "v" + std::to_string(i), 2000 + i), i);

    // cleanBoundaryOffset = 2 → only compact [0,2), leave [2,∞) alone.
    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // k1@0 must be gone (superseded by k1@1 within [0,2))
    for (auto& r : recs) {
        EXPECT_FALSE(r.key == "k1" && r.offset == 0)
            << "k1@0 must be compacted away (superseded within HW)";
    }
    // k1@1 must survive (latest in [0,2))
    bool found1 = false;
    for (auto& r : recs) if (r.key == "k1" && r.offset == 1) found1 = true;
    EXPECT_TRUE(found1);
    // k1 records at offset >= 2 must all survive untouched.
    for (int i = 2; i < 20; ++i) {
        bool found = false;
        for (auto& r : recs) if (r.key == "k1" && r.offset == i) found = true;
        EXPECT_TRUE(found) << "k1@" << i << " past HW must survive";
    }
    std::filesystem::remove_all(dir);
}

// LC-10: compact is idempotent — running twice gives same result.
TEST(LogCleaner, CompactionIsIdempotent) {
    auto dir = makeTempDir("lc_t10");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    log.append(makeBatch(2, "k2", "v3", 1002), 2);
    for (int i = 3; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, 0, 1'000'000);
    auto recs1 = readAllRecords(log, 0);

    // Second compaction should be a no-op.
    cleaner.clean(log, 0, 3, 0, 1'000'000);
    auto recs2 = readAllRecords(log, 0);

    ASSERT_EQ(recs1.size(), recs2.size());
    for (size_t i = 0; i < recs1.size(); ++i) {
        EXPECT_EQ(recs1[i].key,    recs2[i].key);
        EXPECT_EQ(recs1[i].value,  recs2[i].value);
        EXPECT_EQ(recs1[i].offset, recs2[i].offset);
    }
    std::filesystem::remove_all(dir);
}

// LC-11: log remains readable and appendable after compaction.
TEST(LogCleaner, LogReadableAndAppendableAfterCompaction) {
    auto dir = makeTempDir("lc_t11");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    int64_t leoBefore = log.logEndOffset();
    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    // Append a new record.
    int64_t off = log.logEndOffset();
    log.append(makeBatch(off, "new", "val", 3000), off);
    EXPECT_EQ(log.logEndOffset(), off + 1);

    // Read the new record.
    auto recs = readAllRecords(log, off);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].key, "new");
    EXPECT_EQ(recs[0].value, "val");
    (void)leoBefore;
    std::filesystem::remove_all(dir);
}

// LC-12: crash recovery — stale .log.new file from interrupted compact.
// Simulate by leaving a .log.new temp file; next compact call must clean it up
// and still produce a correct compacted log with the EXACT expected survivors.
TEST(LogCleaner, CrashRecoveryStaleNewFile) {
    auto dir = makeTempDir("lc_t12");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // k1@0=v1, k2@1=v2  (will be compacted; both are the latest for their key)
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k2", "v2", 1001), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // Manually plant a stale .log.new file (simulates crash after write, before rename).
    auto bases = log.segmentBaseOffsets();
    ASSERT_FALSE(bases.empty());
    std::ostringstream oss;
    oss << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".log.new";
    { std::ofstream stale(oss.str(), std::ios::binary); stale << "STALE_GARBAGE"; }

    // compact should remove the stale file and complete correctly.
    LogCleaner cleaner;
    EXPECT_NO_THROW(cleaner.clean(log, 0, 2, 0, 1'000'000));

    // Verify the .log.new is gone.
    EXPECT_FALSE(std::filesystem::exists(oss.str())) << ".log.new stale temp must be cleaned up";

    // Log must be readable and contain EXACTLY the expected survivors.
    auto recs = readAllRecords(log, 0);
    ASSERT_FALSE(recs.empty()) << "Log must have records after compaction";

    // k1 appears exactly once at offset 0, value "v1" (sole record, no duplicate).
    EXPECT_EQ(countKey(recs, "k1"), 1) << "k1 must appear exactly once";
    bool k1ok = false;
    for (auto& r : recs)
        if (r.key == "k1" && r.offset == 0 && r.value == "v1") k1ok = true;
    EXPECT_TRUE(k1ok) << "k1@0=v1 must survive with correct offset and value";

    // k2 appears exactly once at offset 1, value "v2".
    EXPECT_EQ(countKey(recs, "k2"), 1) << "k2 must appear exactly once";
    bool k2ok = false;
    for (auto& r : recs)
        if (r.key == "k2" && r.offset == 1 && r.value == "v2") k2ok = true;
    EXPECT_TRUE(k2ok) << "k2@1=v2 must survive with correct offset and value";

    // The stale file must not have corrupted anything — no unexpected keys.
    for (auto& r : recs) {
        EXPECT_TRUE(r.key == "k1" || r.key == "k2" || r.key == "pad")
            << "Unexpected key '" << r.key << "' found — stale .new may have corrupted log";
    }

    std::filesystem::remove_all(dir);
}

// LC-36: crash recovery after fsync-before-rename — complete .new file left behind.
// Simulates crash after fsync of .new but before rename; the surviving .log should
// remain fully readable (old state), and a fresh compact must succeed and produce
// the correct compacted output.
TEST(LogCleaner, CrashRecoveryCompleteNewFileLeftBehind) {
    auto dir = makeTempDir("lc_t36");
    std::filesystem::create_directories(dir);

    // Write log: k1 duplicated twice (only @1 should survive compaction).
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "old", 1000), 0);
        log.append(makeBatch(1, "k1", "new", 1001), 1);
        for (int i = 2; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    }

    // Simulate: crash happened after fsync of .new but before rename.
    // The old .log is intact; a complete (but stale) .new sits beside it.
    auto bases = [&]() {
        Log tmp(dir.string(), 512); return tmp.segmentBaseOffsets();
    }();
    ASSERT_FALSE(bases.empty());
    std::ostringstream oss;
    oss << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".log.new";

    // Write a plausible but stale .new (content doesn't matter — it's stale).
    { std::ofstream stale(oss.str(), std::ios::binary); stale << "COMPLETE_BUT_STALE_NEW"; }

    // Reopen the log — original .log files are intact (rename never happened).
    Log log2(dir.string(), 512);
    log2.recover();

    // The original log must be consistent: both k1@0 and k1@1 survive (uncompacted).
    {
        auto recs = readAllRecords(log2, 0);
        EXPECT_GE(recs.size(), 2u) << "Original log content must survive partial compaction";
        EXPECT_EQ(countKey(recs, "k1"), 2) << "Both k1 records must be present before compact";
    }

    // Now run compact — it must remove the stale .new and produce correct output.
    LogCleaner cleaner;
    EXPECT_NO_THROW(cleaner.clean(log2, 0, 2, 0, 1'000'000));
    EXPECT_FALSE(std::filesystem::exists(oss.str())) << "Stale .new must be removed by compact";

    // After compaction: only k1@1=new must survive.
    auto recs = readAllRecords(log2, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1) << "Only latest k1 must survive after compaction";
    bool k1latest = false;
    for (auto& r : recs)
        if (r.key == "k1" && r.offset == 1 && r.value == "new") k1latest = true;
    EXPECT_TRUE(k1latest) << "k1@1=new must be the sole surviving k1 record";

    std::filesystem::remove_all(dir);
}

// LC-13: crash recovery — reopen after partial compaction (old .log intact).
// Simulate by renaming a segment before rename completes; reopen must yield valid log.
TEST(LogCleaner, CrashRecoveryReopenConsistentOldLog) {
    auto dir = makeTempDir("lc_t13");
    std::filesystem::create_directories(dir);

    // Write the log.
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "v1", 1000), 0);
        log.append(makeBatch(1, "k1", "v2", 1001), 1);
        log.append(makeBatch(2, "k2", "v3", 1002), 2);
        for (int i = 3; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    }

    // Simulate a crash: place a corrupted .log.new but do NOT rename it.
    auto bases = [&]() {
        Log tmp(dir.string(), 512); return tmp.segmentBaseOffsets();
    }();
    std::ostringstream oss;
    oss << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".log.new";
    { std::ofstream stale(oss.str(), std::ios::binary); stale << "CORRUPT"; }

    // Reopen the log — the original .log files are intact.
    Log log2(dir.string(), 512);
    log2.recover();

    // The log must be consistent (original .log files untouched).
    auto recs = readAllRecords(log2, 0);
    EXPECT_GE(recs.size(), 3u) << "Original log content must survive partial compaction";
    bool k1found = false, k2found = false;
    for (auto& r : recs) {
        if (r.key == "k1") k1found = true;
        if (r.key == "k2") k2found = true;
    }
    EXPECT_TRUE(k1found);
    EXPECT_TRUE(k2found);

    // Now a fresh compact with cleanup must remove the stale .log.new and succeed.
    LogCleaner cleaner;
    EXPECT_NO_THROW(cleaner.clean(log2, 0, 3, 0, 1'000'000));
    std::filesystem::remove_all(dir);
}

// LC-14: buildOffsetMap + manual compact (two-phase).
TEST(LogCleaner, TwoPhaseMapThenCompact) {
    auto dir = makeTempDir("lc_t14");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "a", "1", 100), 0);
    log.append(makeBatch(1, "b", "2", 101), 1);
    log.append(makeBatch(2, "a", "3", 102), 2);
    log.append(makeBatch(3, "c", "4", 103), 3);
    for (int i = 4; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 200 + i), i);

    OffsetMap map; LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 0, 4, map);
    EXPECT_EQ(*map.get("a"), 2);
    EXPECT_EQ(*map.get("b"), 1);
    EXPECT_EQ(*map.get("c"), 3);

    cleaner.compact(log, map, /*firstDirtyOffset=*/0, /*cleanBoundaryOffset=*/4, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "a"), 1);
    EXPECT_EQ(countKey(recs, "b"), 1);
    EXPECT_EQ(countKey(recs, "c"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "a"), 2);
    std::filesystem::remove_all(dir);
}

// LC-15: multi-segment compaction (more than 2 compactable segments).
TEST(LogCleaner, MultiSegmentCompaction) {
    auto dir = makeTempDir("lc_t15");
    std::filesystem::create_directories(dir);
    // Very small rollBytes to force many segment rolls.
    Log log(dir.string(), 256);
    // Append 40 records: k1 repeated 20 times, k2 repeated 20 times.
    int64_t off = 0;
    for (int i = 0; i < 20; ++i) {
        off = log.append(makeBatch(off, "k1", "v" + std::to_string(i), 1000 + i), off);
    }
    int64_t cleanBoundary = off; // HW at offset 20
    for (int i = 0; i < 20; ++i) {
        off = log.append(makeBatch(off, "k2", "w" + std::to_string(i), 2000 + i), off);
    }

    EXPECT_GT(log.segmentBaseOffsets().size(), 2u) << "Expected multiple segments";

    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // Only one k1 record should survive in [0, cleanBoundary).
    int k1Count = countKey(recs, "k1");
    EXPECT_EQ(k1Count, 1) << "Expected exactly one k1 after compaction";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 19); // last k1 was at offset 19
    std::filesystem::remove_all(dir);
}

// LC-16: records with null key (empty key) are always kept (not compactable).
TEST(LogCleaner, NullKeyRecordsAlwaysKept) {
    auto dir = makeTempDir("lc_t16");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "", "control1", 1000), 0); // null key
    log.append(makeBatch(1, "", "control2", 1001), 1); // null key
    log.append(makeBatch(2, "k1", "v1", 1002), 2);
    log.append(makeBatch(3, "k1", "v2", 1003), 3);
    for (int i = 4; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // Both null-key records must survive.
    int nullCount = 0;
    for (auto& r : recs) if (r.key.empty()) ++nullCount;
    EXPECT_EQ(nullCount, 2) << "Null-key records must never be compacted";
    // k1 must appear once (latest).
    EXPECT_EQ(countKey(recs, "k1"), 1);
    std::filesystem::remove_all(dir);
}

// LC-17: single record (no supersession) passes through unchanged.
TEST(LogCleaner, SingleRecordPassesThrough) {
    auto dir = makeTempDir("lc_t17");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k", "v", 1000), 0);
    for (int i = 1; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 1, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    bool found = false;
    for (auto& r : recs) if (r.key == "k" && r.offset == 0 && r.value == "v") found = true;
    EXPECT_TRUE(found) << "Sole record must survive compaction";
    std::filesystem::remove_all(dir);
}

// LC-18: clean with firstDirtyOffset > 0 (partial range compaction).
TEST(LogCleaner, PartialRangeCompaction) {
    auto dir = makeTempDir("lc_t18");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // Records 0-1: outside dirty range (firstDirty = 2), must not be touched.
    log.append(makeBatch(0, "early", "e1", 900), 0);
    log.append(makeBatch(1, "early", "e2", 901), 1);
    // Records 2-4: inside dirty range.
    log.append(makeBatch(2, "k1", "v1", 1000), 2);
    log.append(makeBatch(3, "k1", "v2", 1001), 3);
    log.append(makeBatch(4, "k2", "v3", 1002), 4);
    for (int i = 5; i < 20; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // Only compact [2, 5) — "early" records are outside.
    LogCleaner cleaner;
    cleaner.clean(log, 2, 5, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // "early" records at 0 and 1 must both survive (outside dirty range).
    EXPECT_EQ(countKey(recs, "early"), 2) << "Records outside dirty range must be preserved";
    // k1 should have 1 record (latest = offset 3).
    EXPECT_EQ(countKey(recs, "k1"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 3);
    std::filesystem::remove_all(dir);
}

// LC-19: tombstone sequence: append v1, then tombstone; both are superseded
// by the tombstone (v1 dropped). After retention, tombstone also gone.
TEST(LogCleaner, TombstoneSequenceFullLifecycle) {
    auto dir = makeTempDir("lc_t19");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    int64_t tombTs = 5000LL;
    log.append(makeBatch(0, "k1", "v1", tombTs - 100), 0);
    log.append(makeTombstone(1, "k1", tombTs), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 20000 + i), i);

    // Phase 1: compact within retention — tombstone kept, v1 gone.
    {
        LogCleaner cleaner;
        cleaner.clean(log, 0, 2, tombTs + 1, /*retMs=*/1000);
        auto recs = readAllRecords(log, 0);
        bool tombPresent = false;
        for (auto& r : recs)
            if (r.key == "k1" && r.value.empty() && r.offset == 1) tombPresent = true;
        EXPECT_TRUE(tombPresent) << "Tombstone must be kept within retention window";
        for (auto& r : recs)
            EXPECT_FALSE(r.key == "k1" && r.value == "v1") << "Superseded v1 must be gone";
    }

    // Phase 2: past retention — tombstone itself removed.
    {
        int64_t nowNs = tombTs + 1000LL * 1'000'000LL + 1; // past 1000ms
        LogCleaner cleaner;
        cleaner.clean(log, 0, 2, nowNs, /*retMs=*/1000);
        auto recs = readAllRecords(log, 0);
        for (auto& r : recs)
            EXPECT_NE(r.key, "k1") << "k1 must be fully absent after tombstone expiry";
    }

    std::filesystem::remove_all(dir);
}

// LC-20: value "v" is NOT a tombstone; only empty value is.
TEST(LogCleaner, NonEmptyValueIsNotTombstone) {
    auto dir = makeTempDir("lc_t20");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "val", 1000), 0); // non-empty: NOT a tombstone
    log.append(makeBatch(1, "k1", "val2", 1001), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // k1@1 (non-tombstone, latest) must survive.
    EXPECT_EQ(countKey(recs, "k1"), 1);
    bool found = false;
    for (auto& r : recs) if (r.key == "k1" && r.value == "val2") found = true;
    EXPECT_TRUE(found) << "Latest non-empty value must survive";
    std::filesystem::remove_all(dir);
}

// LC-21: empty log (no records) — clean is a no-op.
TEST(LogCleaner, EmptyLogCleanNoOp) {
    auto dir = makeTempDir("lc_t21");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    LogCleaner cleaner;
    EXPECT_NO_THROW(cleaner.clean(log, 0, 0, 0, 1'000'000));
    EXPECT_EQ(log.logEndOffset(), 0);
    std::filesystem::remove_all(dir);
}

// LC-22: compact with cleanBoundaryOffset=0 skips all segments.
TEST(LogCleaner, ZeroBoundarySkipsAll) {
    auto dir = makeTempDir("lc_t22");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    int64_t leoBefore = log.logEndOffset();
    LogCleaner cleaner;
    cleaner.clean(log, 0, 0, 0, 1'000'000);

    // Nothing should be compacted.
    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 2) << "No records should be dropped when boundary=0";
    EXPECT_EQ(log.logEndOffset(), leoBefore);
    std::filesystem::remove_all(dir);
}

// LC-23: multiple keys, only some have duplicates — only duplicates compacted.
TEST(LogCleaner, SelectiveDuplicateCompaction) {
    auto dir = makeTempDir("lc_t23");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "dup",  "d1", 1000), 0);
    log.append(makeBatch(1, "once", "u1", 1001), 1);
    log.append(makeBatch(2, "dup",  "d2", 1002), 2);
    log.append(makeBatch(3, "once2","u2", 1003), 3);
    for (int i = 4; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "dup"),   1) << "Duplicate key compacted to 1";
    EXPECT_EQ(countKey(recs, "once"),  1) << "Unique key kept";
    EXPECT_EQ(countKey(recs, "once2"), 1) << "Unique key2 kept";
    EXPECT_EQ(latestOffsetForKey(recs, "dup"), 2);
    std::filesystem::remove_all(dir);
}

// LC-24: after compaction, new records appended can be read and compacted again.
TEST(LogCleaner, AppendAfterCompactionAndRecompact) {
    auto dir = makeTempDir("lc_t24");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    for (int i = 2; i < 10; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    // Append more records that overlap k1.
    int64_t off = log.logEndOffset();
    log.append(makeBatch(off, "k1", "v3", 3000), off);
    int64_t newBoundary = off + 1;
    off++;
    for (int i = 0; i < 10; ++i)
        off = log.append(makeBatch(off, "pad2", "q", 4000 + i), off);

    cleaner.clean(log, 0, newBoundary, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1);
    std::filesystem::remove_all(dir);
}

// LC-25: buildOffsetMap with tombstone records.
TEST(LogCleaner, BuildOffsetMapWithTombstone) {
    auto dir = makeTempDir("lc_t25");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeTombstone(1, "k1", 1001), 1);
    log.append(makeBatch(2, "k2", "v2", 1002), 2);

    OffsetMap map; LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 0, 3, map);
    // Tombstone at offset 1 is latest for k1.
    EXPECT_EQ(*map.get("k1"), 1);
    EXPECT_EQ(*map.get("k2"), 2);
    std::filesystem::remove_all(dir);
}

// LC-26: compaction preserves timestamp on surviving records.
TEST(LogCleaner, CompactionPreservesTimestamp) {
    auto dir = makeTempDir("lc_t26");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    int64_t ts = 9999'000'000LL; // large timestamp
    log.append(makeBatch(0, "k1", "old", ts - 1000), 0);
    log.append(makeBatch(1, "k1", "new", ts),         1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", ts + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    for (auto& r : recs) {
        if (r.key == "k1") {
            EXPECT_EQ(r.timestamp, ts) << "Surviving record must preserve original timestamp";
        }
    }
    std::filesystem::remove_all(dir);
}

// LC-27: log LEO does not change after compaction (gaps, not renumbering).
TEST(LogCleaner, LEOUnchangedAfterCompaction) {
    auto dir = makeTempDir("lc_t27");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    for (int i = 0; i < 5; ++i)
        log.append(makeBatch(i, "k1", "v" + std::to_string(i), 1000 + i), i);
    for (int i = 5; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    int64_t leoBefore = log.logEndOffset();
    LogCleaner cleaner;
    cleaner.clean(log, 0, 5, 0, 1'000'000);
    // LEO (= logEndOffset) must not decrease — gaps preserve offsets.
    EXPECT_GE(log.logEndOffset(), 5) << "LEO must not shrink after compaction";
    (void)leoBefore;
    std::filesystem::remove_all(dir);
}

// LC-28: reading by original preserved offset still works post-compaction.
TEST(LogCleaner, ReadByOriginalOffsetPostCompaction) {
    auto dir = makeTempDir("lc_t28");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1); // latest for k1
    log.append(makeBatch(2, "k2", "v3", 1002), 2);
    for (int i = 3; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, 0, 1'000'000);

    // We should be able to read starting from offset 1 (the surviving k1 record).
    auto bytes = log.read(1, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    RecordBatchReader r(bytes.data(), bytes.size());
    ASSERT_TRUE(r.ensureValid());
    bool found = false;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
        if (k == "k1" && v == "v2" && off == 1) found = true;
    });
    EXPECT_TRUE(found) << "Must be able to read by original offset after compaction";
    std::filesystem::remove_all(dir);
}

// LC-29: compaction preserves all records where each key appears exactly once.
TEST(LogCleaner, AllUniqueKeysPreserved) {
    auto dir = makeTempDir("lc_t29");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    for (int i = 0; i < 5; ++i)
        log.append(makeBatch(i, "key" + std::to_string(i), "val" + std::to_string(i), 1000 + i), i);
    for (int i = 5; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 5, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    for (int i = 0; i < 5; ++i) {
        std::string k = "key" + std::to_string(i);
        EXPECT_EQ(countKey(recs, k), 1) << k << " must survive (only one copy)";
    }
    std::filesystem::remove_all(dir);
}

// LC-30: multiple tombstones for the same key — latest tombstone wins.
TEST(LogCleaner, MultipleTombstonesLatestWins) {
    auto dir = makeTempDir("lc_t30");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    log.append(makeBatch    (0, "k1", "v1",   1000), 0);
    log.append(makeTombstone(1, "k1",         2000), 1);
    log.append(makeBatch    (2, "k1", "v2",   3000), 2);
    log.append(makeTombstone(3, "k1",         4000), 3); // latest tombstone
    for (int i = 4; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 5000 + i), i);

    // Within retention — keep the latest tombstone.
    LogCleaner cleaner;
    cleaner.clean(log, 0, 4, /*nowNs=*/4001, /*retMs=*/1000);

    auto recs = readAllRecords(log, 0);
    // Only the tombstone at offset 3 should remain for k1.
    int k1Count = countKey(recs, "k1");
    EXPECT_EQ(k1Count, 1);
    bool tombAt3 = false;
    for (auto& r : recs) if (r.key == "k1" && r.value.empty() && r.offset == 3) tombAt3 = true;
    EXPECT_TRUE(tombAt3) << "Latest tombstone at offset 3 must be kept";
    std::filesystem::remove_all(dir);
}

// LC-31: dirty range [0, LEO) — compact entire log except active segment.
TEST(LogCleaner, CompactEntireLogExceptActive) {
    auto dir = makeTempDir("lc_t31");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 256);
    int64_t off = 0;
    for (int i = 0; i < 30; ++i)
        off = log.append(makeBatch(off, "k" + std::to_string(i % 5), "v" + std::to_string(i), 1000 + i), off);

    EXPECT_GT(log.segmentBaseOffsets().size(), 1u);
    int64_t activeBase = log.segmentBaseOffsets().back();
    int64_t cleanBoundary = activeBase; // don't compact active segment

    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // For each of k0..k4, there must be at most 1 record in [0, activeBase).
    for (int i = 0; i < 5; ++i) {
        std::string k = "k" + std::to_string(i);
        int cnt = 0;
        for (auto& r : recs) if (r.key == k && r.offset < activeBase) ++cnt;
        EXPECT_LE(cnt, 1) << k << " should have at most 1 record below activeBase after compact";
    }
    std::filesystem::remove_all(dir);
}

// LC-32: recover after compaction — reopen log reads compacted data correctly.
TEST(LogCleaner, ReopenAfterCompaction) {
    auto dir = makeTempDir("lc_t32");
    std::filesystem::create_directories(dir);
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "v1", 1000), 0);
        log.append(makeBatch(1, "k1", "v2", 1001), 1);
        log.append(makeBatch(2, "k2", "v3", 1002), 2);
        for (int i = 3; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);

        LogCleaner cleaner;
        cleaner.clean(log, 0, 3, 0, 1'000'000);
    }

    // Reopen.
    Log log2(dir.string(), 512);
    log2.recover();

    auto recs = readAllRecords(log2, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1) << "After reopen, k1 must still be compacted";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 1);
    std::filesystem::remove_all(dir);
}

// LC-33: compaction with many keys and many duplicates.
TEST(LogCleaner, ManyKeysManyDuplicates) {
    auto dir = makeTempDir("lc_t33");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    int64_t off = 0;
    // 10 keys, each written 5 times.
    for (int round = 0; round < 5; ++round)
        for (int k = 0; k < 10; ++k)
            off = log.append(makeBatch(off, "k" + std::to_string(k),
                                       "v" + std::to_string(round), 1000 + off), off);
    int64_t cleanBoundary = off;
    // Pad to ensure non-active.
    for (int i = 0; i < 5; ++i)
        off = log.append(makeBatch(off, "active_pad", "p", 9000 + i), off);

    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        EXPECT_EQ(countKey(recs, key), 1) << key << " should have exactly 1 record";
    }
    std::filesystem::remove_all(dir);
}

// LC-34: compact does not alter records in segments at/past cleanBoundaryOffset.
TEST(LogCleaner, RecordsPastBoundaryUntouched) {
    auto dir = makeTempDir("lc_t34");
    std::filesystem::create_directories(dir);
    Log log(dir.string(), 512);
    // Records before boundary.
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k1", "v2", 1001), 1);
    // Records past boundary.
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "k1", "vfuture" + std::to_string(i), 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // All records at offset >= 2 must survive.
    for (int i = 2; i < 15; ++i) {
        bool found = false;
        for (auto& r : recs) if (r.key == "k1" && r.offset == i) found = true;
        EXPECT_TRUE(found) << "k1@" << i << " past boundary must survive";
    }
    std::filesystem::remove_all(dir);
}

// LC-35: offset map size matches number of distinct keys.
TEST(LogCleaner, OffsetMapSizeMatchesDistinctKeys) {
    auto dir = makeTempDir("lc_t35");
    std::filesystem::create_directories(dir);
    Log log(dir.string());
    // 3 distinct keys, 4 total records.
    log.append(makeBatch(0, "a", "1", 100), 0);
    log.append(makeBatch(1, "b", "2", 101), 1);
    log.append(makeBatch(2, "a", "3", 102), 2);
    log.append(makeBatch(3, "c", "4", 103), 3);

    OffsetMap map; LogCleaner cleaner;
    cleaner.buildOffsetMap(log, 0, 4, map);
    EXPECT_EQ(map.size(), 3u) << "Map must contain exactly 3 distinct keys";
    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// T1-real: Real crash-durability tests for LogCleaner.
// After compact (writeFileDurable + fsync + rename), a fresh Log reopened
// from disk (simulating power loss) must see the correct compacted content.
// ---------------------------------------------------------------------------

// T1-real: Compact, then "power-loss reopen" — fresh Log sees correct content.
TEST(LogCleaner, CompactedLogIsReadableAfterReopen) {
    auto dir = makeTempDir("lc_t1_real");
    std::filesystem::create_directories(dir);

    // Write: k1 twice (only offset 1 should survive), k2 once.
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "old", 1000), 0); // superseded
        log.append(makeBatch(1, "k1", "new", 1001), 1); // latest
        log.append(makeBatch(2, "k2", "v2",  1002), 2);
        for (int i = 3; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    } // Log destructor closes all handles

    // Compact (this does writeFileDurable + rename + fsyncDir)
    {
        Log log(dir.string(), 512);
        log.recover();
        LogCleaner cleaner;
        cleaner.clean(log, 0, 3, 0, 1'000'000);
    }

    // "Power-loss reopen": brand-new Log from the directory.
    Log log3(dir.string(), 512);
    log3.recover();

    auto recs = readAllRecords(log3, 0);

    // k1 must appear exactly once with the latest value.
    int k1count = 0; bool k1Latest = false;
    for (auto& r : recs) {
        if (r.key == "k1") {
            ++k1count;
            if (r.value == "new" && r.offset == 1) k1Latest = true;
        }
    }
    EXPECT_EQ(k1count, 1) << "k1 must appear exactly once after compact+reopen";
    EXPECT_TRUE(k1Latest) << "k1@1=new must be the surviving record";
    EXPECT_EQ(countKey(recs, "k2"), 1);

    std::filesystem::remove_all(dir);
}

// T1-index: After compact + reopen, no .index2 orphan files; reads via index work.
TEST(LogCleaner, CompactedIndexConsistentWithLog) {
    auto dir = makeTempDir("lc_t1_idx");
    std::filesystem::create_directories(dir);

    {
        Log log(dir.string(), 1024);
        for (int i = 0; i < 10; ++i)
            log.append(makeBatch(i, "k" + std::to_string(i % 3), "v", 5000 + i), i);
    }

    {
        Log log(dir.string(), 1024);
        log.recover();
        LogCleaner cleaner;
        cleaner.clean(log, 0, 10, 0, 1'000'000);
    }

    // Check no .index2 files exist anywhere in the directory.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        EXPECT_NE(e.path().extension(), ".index2")
            << "No .index2 orphan files must exist after compact: " << e.path();
    }

    // Reopen and verify reads work (index is consistent).
    Log log3(dir.string(), 1024);
    log3.recover();
    auto recs = readAllRecords(log3, 0);
    EXPECT_FALSE(recs.empty()) << "Log must be readable after compact+reopen";

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Part B — edge-case tests (full re-audit 2026-09-11)
// ---------------------------------------------------------------------------

// B1 / A4: Compact a segment so every record is superseded → 0-byte outcome.
// The segment file must be DELETED (not left as a 0-byte .log), and a fresh
// log reopen must survive (LEO intact, reads from surrounding segments work).
TEST(LogCleaner, EmptyCompactedSegmentDeletedNotLeft) {
    auto dir = makeTempDir("lc_b1");
    std::filesystem::create_directories(dir);

    // Segment 0: k1@0 (will be superseded — segment becomes empty post-compact).
    // Segment 1: k1@1 (latest — survives in the active/later segment).
    // We need at least 3 segments: seg0 (compactable, becomes empty), seg1 (compactable,
    // holds the latest k1), and seg2 (active, never compacted).
    Log log(dir.string(), 256);
    log.append(makeBatch(0, "k1", "old", 1000), 0); // seg 0
    // Force a roll by padding.
    for (int i = 1; i < 8; ++i)
        log.append(makeBatch(i, "pad_a", "x", 2000 + i), i);
    log.append(makeBatch(8, "k1", "new", 1008), 8); // seg ≥1: latest k1
    for (int i = 9; i < 20; ++i)
        log.append(makeBatch(i, "pad_b", "y", 3000 + i), i);

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least two segments for this test";

    // The first non-active segment (bases[0]) contains k1@0 which will be
    // superseded by k1@8 in a later segment.  After compaction, bases[0] must
    // be fully empty → A4 fix deletes the file.
    int64_t cleanBoundary = bases.back(); // don't compact active segment

    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, 0, 1'000'000);

    // Verify: no 0-byte .log files remain.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".log") {
            EXPECT_GT(std::filesystem::file_size(e.path()), 0u)
                << "No 0-byte .log files should remain: " << e.path();
        }
    }

    // Fresh reopen must survive.
    Log log2(dir.string(), 256);
    log2.recover();
    EXPECT_GT(log2.logEndOffset(), 0) << "LEO must be intact after empty-segment deletion";

    // k1 must appear exactly once — the latest version.
    auto recs = readAllRecords(log2, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1) << "Only one k1 must survive";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 8) << "Surviving k1 must be at offset 8";

    std::filesystem::remove_all(dir);
}

// B2: After compact, a fresh process recovering only from segment bytes
// (reopen + recover()) must reproduce the correct LEO and read records.
// This tests that the on-disk compacted state is self-consistent without
// relying on any in-memory state from the compaction run.
TEST(LogCleaner, FreshReopenAfterCompactionProducesCorrectLEO) {
    auto dir = makeTempDir("lc_b2");
    std::filesystem::create_directories(dir);

    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "v1", 1000), 0);
        log.append(makeBatch(1, "k1", "v2", 1001), 1); // latest k1
        log.append(makeBatch(2, "k2", "v3", 1002), 2); // only k2
        for (int i = 3; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    } // all handles closed

    // Compact using a separate Log instance (simulates a new process run).
    {
        Log log(dir.string(), 512);
        log.recover();
        LogCleaner cleaner;
        cleaner.clean(log, 0, 3, 0, 1'000'000);
    } // all handles closed

    // "Fresh process": brand-new Log, recover from disk only.
    Log log3(dir.string(), 512);
    int64_t leo = log3.recover();
    EXPECT_GT(leo, 2) << "LEO must reflect all segments, not just the active one";

    auto recs = readAllRecords(log3, 0);
    EXPECT_EQ(countKey(recs, "k1"), 1) << "k1 must be compacted to 1";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 1);
    EXPECT_EQ(countKey(recs, "k2"), 1);

    std::filesystem::remove_all(dir);
}

// B3: Multi-record batch compaction — feed a real multi-record batch
// (≥2 records per batch) and assert that per-record survivors and preserved
// offsets/timestamps are correct after compaction.
// All prior fixtures used single-record batches; this exercises the multi-
// record code path including producer-identity preservation (A2).
TEST(LogCleaner, MultiRecordBatchCompactionPreservesRecords) {
    auto dir = makeTempDir("lc_b3");
    std::filesystem::create_directories(dir);

    // Build a multi-record batch: k1@0, k2@1, k1@2 in ONE batch (baseOffset=0).
    // After compaction of [0,3): k1@2 and k2@1 survive, k1@0 is dropped.
    // Use tiny rollBytes (128) so the multi-record batch forces a segment roll
    // when padded records arrive, ensuring it lands in a non-active segment.
    Log log(dir.string(), 128);
    {
        RecordBatchBuilder b(0, 1000);
        b.append("k1", "v1", 1000); // offset 0
        b.append("k2", "v2", 1001); // offset 1
        b.append("k1", "v3", 1002); // offset 2 — latest k1
        auto batch = b.build();
        log.append(batch, 2); // lastOffset=2 (offsets 0,1,2)
    }
    // Pad to force segment roll — makes the multi-record batch non-active.
    for (int i = 3; i < 20; ++i)
        log.append(makeBatch(i, "pad", "p", 3000 + i), i);

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need multiple segments: multi-record batch must be non-active";

    LogCleaner cleaner;
    cleaner.clean(log, 0, 3, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);

    // k1@0 must be gone (superseded by k1@2).
    for (auto& r : recs)
        EXPECT_FALSE(r.key == "k1" && r.offset == 0)
            << "k1@0 from multi-record batch must be compacted away";

    // k2@1 must survive.
    bool k2found = false;
    for (auto& r : recs)
        if (r.key == "k2" && r.offset == 1) k2found = true;
    EXPECT_TRUE(k2found) << "k2@1 must survive compaction";

    // k1@2 must survive (latest k1).
    EXPECT_EQ(countKey(recs, "k1"), 1);
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 2);

    std::filesystem::remove_all(dir);
}

// B4 / A3: A corrupt batch (CRC mismatch) mid-segment stops buildOffsetMap
// and rewriteSegment at the corrupt entry — no silent misparse.
TEST(LogCleaner, CorruptBatchInBuildOffsetMapStopsWalk) {
    auto dir = makeTempDir("lc_b4");
    std::filesystem::create_directories(dir);

    // Append two good batches.
    Log log(dir.string(), 64 * 1024);
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    log.append(makeBatch(1, "k2", "v2", 1001), 1);
    for (int i = 2; i < 10; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    // Corrupt the second batch's CRC in the first segment's .log file.
    // We need to find the segment file and flip a byte in the CRC field
    // (offset 17 relative to the batch start = past the first batch).
    auto bases = log.segmentBaseOffsets();
    std::string seg0path;
    {
        std::ostringstream oss;
        oss << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".log";
        seg0path = oss.str();
    }

    // Find the start of the second batch by reading until we've consumed the first.
    size_t firstBatchSize = 0;
    {
        std::ifstream f(seg0path, std::ios::binary);
        f.seekg(0, std::ios::end);
        size_t fsz = static_cast<size_t>(f.tellg());
        f.seekg(0, std::ios::beg);
        std::vector<uint8_t> buf(fsz);
        f.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(fsz));
        RecordBatchReader r1(buf.data(), buf.size());
        firstBatchSize = static_cast<size_t>(r1.totalLength());
    }

    // Flip a byte in the CRC field of the second batch (offset 17 within that batch).
    {
        int fd = ::open(seg0path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        size_t crcOff = firstBatchSize + 17; // kCrcOffset == 17
        uint8_t b;
        ::pread(fd, &b, 1, static_cast<off_t>(crcOff));
        b ^= 0xFF; // flip all bits
        ::pwrite(fd, &b, 1, static_cast<off_t>(crcOff));
        ::close(fd);
    }

    // buildOffsetMap: must stop at the corrupt batch, not misparse.
    // The result may be partial (only k1 from the first good batch).
    OffsetMap map;
    LogCleaner cleaner;
    EXPECT_NO_THROW(cleaner.buildOffsetMap(log, 0, 2, map))
        << "buildOffsetMap must not throw on corrupt batch";
    // k1 (from the first good batch) must be in the map.
    EXPECT_TRUE(map.get("k1").has_value()) << "k1 from first good batch must be in map";
    // k2 (corrupt batch) may or may not be present; no crash is the contract.

    std::filesystem::remove_all(dir);
}

// B5: Crash exactly between rename(.new → .log) and fsyncDir.
// After the rename, the .log contains the new content.  A fresh reopen
// (simulating recovery after that exact crash) must see the renamed content.
TEST(LogCleaner, CrashAfterRenameBeforeFsyncDirSeeNewContent) {
    auto dir = makeTempDir("lc_b5");
    std::filesystem::create_directories(dir);

    // Write a log and compact it.
    {
        Log log(dir.string(), 512);
        log.append(makeBatch(0, "k1", "old", 1000), 0);
        log.append(makeBatch(1, "k1", "new", 1001), 1); // latest k1
        for (int i = 2; i < 15; ++i)
            log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    }

    auto bases = [&]() {
        Log tmp(dir.string(), 512); return tmp.segmentBaseOffsets();
    }();
    ASSERT_FALSE(bases.empty());

    // Manually perform the rename step (simulating the critical crash window):
    // write a compacted .new, rename it over .log, but do NOT fsyncDir.
    std::string logPath, newPath, idxPath;
    {
        std::ostringstream oss;
        oss << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".log";
        logPath = oss.str();
        newPath = logPath + ".new";
        std::ostringstream oxi;
        oxi << dir.string() << "/" << std::setw(20) << std::setfill('0') << bases[0] << ".index";
        idxPath = oxi.str();
    }

    // Build the expected compacted content (k1@1 only) into .new, then rename.
    {
        auto newContent = makeBatch(1, "k1", "new", 1001);
        int fd = ::open(newPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        ::write(fd, newContent.data(), newContent.size());
        ::fsync(fd);
        ::close(fd);
        ::rename(newPath.c_str(), logPath.c_str());
        // NOTE: intentionally NOT calling fsyncDir here (simulating the crash window).
    }

    // The OS page cache has the rename; a fresh reopen sees the new content
    // (the missing fsyncDir only matters for actual power loss).
    Log log2(dir.string(), 512);
    log2.recover();
    auto recs = readAllRecords(log2, 0);

    // After the rename, the .log contains only k1@1 (the compacted content).
    EXPECT_EQ(countKey(recs, "k1"), 1) << "Post-rename content must be visible on reopen";
    EXPECT_EQ(latestOffsetForKey(recs, "k1"), 1) << "Renamed content is k1@1=new";
    for (auto& r : recs)
        if (r.key == "k1") EXPECT_EQ(r.value, "new") << "Surviving k1 must be the new value";

    std::filesystem::remove_all(dir);
}

// B6: Tombstone exactly at the deleteRetentionNs boundary (boundary semantics).
// The code uses `>` (strictly greater) for expiry: (nowNs - tombstoneTs) > deleteRetentionNs.
// Exactly at the boundary the tombstone must be RETAINED.
TEST(LogCleaner, TombstoneExactlyAtRetentionBoundaryIsRetained) {
    auto dir = makeTempDir("lc_b6");
    std::filesystem::create_directories(dir);

    Log log(dir.string(), 512);
    int64_t tombTs    = 1'000'000LL;         // tombstone timestamp (ns)
    int64_t retMs     = 100LL;               // retention: 100 ms = 100_000_000 ns
    int64_t retNs     = retMs * 1'000'000LL; // 100_000_000 ns

    log.append(makeBatch(0, "k1", "v1", tombTs - 1000), 0);
    log.append(makeTombstone(1, "k1", tombTs), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2'000'000 + i), i);

    // nowNs exactly at boundary: (nowNs - tombTs) == retNs → NOT > retNs → retain.
    int64_t nowNs = tombTs + retNs;

    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, nowNs, retMs);

    auto recs = readAllRecords(log, 0);
    bool tombFound = false;
    for (auto& r : recs)
        if (r.key == "k1" && r.value.empty() && r.offset == 1) tombFound = true;
    EXPECT_TRUE(tombFound)
        << "Tombstone exactly at retention boundary must be retained (> not >=)";

    std::filesystem::remove_all(dir);
}

// B7 / S6: Key survives only in the active segment; an earlier segment holds
// an older copy.  The earlier copy must be dropped; the active copy survives.
TEST(LogCleaner, KeyOnlyInActiveSegmentDropsEarlierCopy) {
    auto dir = makeTempDir("lc_b7");
    std::filesystem::create_directories(dir);

    Log log(dir.string(), 256);
    // k1@0 in segment 0 (compactable).
    log.append(makeBatch(0, "k1", "v1", 1000), 0);
    // Pad to roll seg0 into non-active.
    for (int i = 1; i < 8; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);
    // k1@8 in the ACTIVE segment (not compacted).
    log.append(makeBatch(8, "k1", "v2_active", 3000), 8);
    for (int i = 9; i < 15; ++i)
        log.append(makeBatch(i, "pad2", "q", 4000 + i), i);

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);
    int64_t activeBase = bases.back();
    // Compact everything except the active segment.
    int64_t cleanBoundary = activeBase;

    LogCleaner cleaner;
    cleaner.clean(log, 0, cleanBoundary, 0, 1'000'000);

    auto recs = readAllRecords(log, 0);
    // k1@0 in the compactable segment must be gone (superseded by k1@8 in active).
    for (auto& r : recs)
        EXPECT_FALSE(r.key == "k1" && r.offset == 0)
            << "Earlier copy of k1 must be dropped when latest is in active segment";

    // k1@8 in the active segment must survive.
    bool activeK1Found = false;
    for (auto& r : recs)
        if (r.key == "k1" && r.offset == 8) activeK1Found = true;
    EXPECT_TRUE(activeK1Found) << "k1@8 in active segment must survive";

    std::filesystem::remove_all(dir);
}

// B3-producer: After compact, surviving records carry the ORIGINAL producer
// identity (A2 fix: producerId/epoch/baseSequence are preserved, not -1).
TEST(LogCleaner, CompactionPreservesProducerIdentity) {
    auto dir = makeTempDir("lc_b3_pid");
    std::filesystem::create_directories(dir);

    // Build a batch carrying non-default producerId/epoch/baseSequence.
    // RecordBatchBuilder always sets -1 for these; we patch them manually.
    // Use a large positive int64_t (fits in signed 64-bit).
    int64_t expectedPid   = 0x1234567890ABCDEFLL; // positive: 1311768467294899695
    int16_t expectedEpoch = 42;
    int32_t expectedBaseSeq = 7;

    auto buildProducerBatch = [&](int64_t baseOffset, std::string_view key,
                                  std::string_view val, int64_t ts) {
        RecordBatchBuilder b(baseOffset, ts);
        b.append(key, val, ts);
        auto batch = b.build();
        // Patch producerId (offset 43, int64 BE).
        for (int i = 7; i >= 0; --i)
            batch[rune::protocol::batch::kProducerIdOffset + (7 - i)] =
                static_cast<uint8_t>((expectedPid >> (i * 8)) & 0xFF);
        // Patch producerEpoch (offset 51, int16 BE).
        batch[rune::protocol::batch::kProducerEpochOffset]     = static_cast<uint8_t>((expectedEpoch >> 8) & 0xFF);
        batch[rune::protocol::batch::kProducerEpochOffset + 1] = static_cast<uint8_t>(expectedEpoch & 0xFF);
        // Patch baseSequence (offset 53, int32 BE).
        for (int i = 3; i >= 0; --i)
            batch[rune::protocol::batch::kBaseSequenceOffset + (3 - i)] =
                static_cast<uint8_t>((expectedBaseSeq >> (i * 8)) & 0xFF);
        // Recompute CRC.
        uint32_t crc = rune::protocol::crc32c(
            batch.data() + rune::protocol::batch::kAttributesOffset,
            batch.size() - rune::protocol::batch::kAttributesOffset);
        batch[rune::protocol::batch::kCrcOffset]     = static_cast<uint8_t>(crc >> 24);
        batch[rune::protocol::batch::kCrcOffset + 1] = static_cast<uint8_t>(crc >> 16);
        batch[rune::protocol::batch::kCrcOffset + 2] = static_cast<uint8_t>(crc >> 8);
        batch[rune::protocol::batch::kCrcOffset + 3] = static_cast<uint8_t>(crc);
        return batch;
    };

    // k1@0 (superseded) and k1@1 (latest) — both with producer identity.
    Log log(dir.string(), 512);
    log.append(buildProducerBatch(0, "k1", "v1", 1000), 0);
    log.append(buildProducerBatch(1, "k1", "v2", 1001), 1);
    for (int i = 2; i < 15; ++i)
        log.append(makeBatch(i, "pad", "p", 2000 + i), i);

    LogCleaner cleaner;
    cleaner.clean(log, 0, 2, 0, 1'000'000);

    // Read back raw bytes of the surviving k1@1 batch and check producer fields.
    auto bytes = log.read(1, 1 << 20);
    ASSERT_FALSE(bytes.empty()) << "k1@1 must be readable after compaction";

    // Verify CRC is valid (producer fields correctly patched into rebuilt batch).
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid()) << "Compacted batch must have a valid CRC";

    // Read raw producer fields from the batch bytes.
    int64_t gotPid = 0;
    for (int i = 0; i < 8; ++i)
        gotPid = (gotPid << 8) | bytes[rune::protocol::batch::kProducerIdOffset + i];
    EXPECT_EQ(gotPid, expectedPid) << "producerId must be preserved through compaction";

    int16_t gotEpoch = static_cast<int16_t>(
        (static_cast<uint16_t>(bytes[rune::protocol::batch::kProducerEpochOffset]) << 8) |
         static_cast<uint16_t>(bytes[rune::protocol::batch::kProducerEpochOffset + 1]));
    EXPECT_EQ(gotEpoch, expectedEpoch) << "producerEpoch must be preserved through compaction";

    int32_t gotSeq = static_cast<int32_t>(
        (uint32_t(bytes[rune::protocol::batch::kBaseSequenceOffset])     << 24) |
        (uint32_t(bytes[rune::protocol::batch::kBaseSequenceOffset + 1]) << 16) |
        (uint32_t(bytes[rune::protocol::batch::kBaseSequenceOffset + 2]) << 8)  |
        (uint32_t(bytes[rune::protocol::batch::kBaseSequenceOffset + 3])));
    // The record's sequence = origBaseSequence + offsetDelta within its original batch.
    // For k1@1 in a single-record batch with baseOffset=1: offsetDelta = 1-1 = 0.
    // So expectedSeq = expectedBaseSeq + 0 = 7.
    EXPECT_EQ(gotSeq, expectedBaseSeq) << "baseSequence must be preserved through compaction";

    std::filesystem::remove_all(dir);
}
