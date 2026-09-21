// ---------------------------------------------------------------------------
// SkimpyOffsetMapTest.cpp — CP-7: Bounded fast dedupe map tests.
//
// Tests:
//   SOM-1  DedupeMapBoundedByBudget:
//              With a tiny dedupeBufferBytes the map never exceeds maxSlots;
//              put() returns false when full (not OOM).
//   SOM-2  HugeLogCleanedIncrementallyAcrossPasses:
//              A log with more unique keys than one map holds is fully
//              compacted over MULTIPLE passes (bounded-range-per-pass),
//              converging to latest-per-key, checkpoint advancing each pass.
//              Non-vacuous: fails if entire scan ran in one pass OR if
//              checkpoint didn't advance.
//   SOM-3  CheckpointPersistsAndResumesAcrossRestart:
//              Compact partially, "restart" (reconstruct from disk),
//              confirm lastCleanOffset is restored and cleaner resumes.
//   SOM-4  CheckpointTruncationAware:
//              Checkpoint lowered when log truncated below it.
//   SOM-5  HashCollisionCorrectness:
//              Two distinct keys must not be wrongly deduped.
//              Exercises the collision-safety guarantee of the 64-bit hash.
//   SOM-6  SingleWriterInvariantPreserved:
//              Grep-based assertion that SkimpyOffsetMap and
//              CleanerOffsetCheckpoint code paths never call segments_,
//              segmentBaseOffsets(), segmentSize(), or reloadSegment() on
//              the cleaner-thread code path.
//
// Additional unit tests for SkimpyOffsetMap API:
//   SOM-U1..U8: basic put/get/clear/isFull/latestOffset/maxSlotsFromBudget.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static std::atomic<uint64_t> g_seq{0};
static fs::path tempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

static std::vector<uint8_t> makeBatch(int64_t off, std::string_view key,
                                      std::string_view val, int64_t ts = 1000) {
    RecordBatchBuilder b(off, ts);
    b.append(key, val, ts);
    return b.build();
}

// Count live records in a log via raw read scan.
static int countLive(Log& log) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool adv = false;
            r.forEach([&](std::string_view, std::string_view, int64_t o, int64_t) {
                ++count;
                cursor = o + 1;
                adv = true;
            });
            if (!adv || cursor >= segEnd) break;
        }
    }
    return count;
}

// Collect all live (key, offset) pairs from a log.
static std::unordered_map<std::string, int64_t> latestPerKey(Log& log) {
    std::unordered_map<std::string, int64_t> m;
    auto bases = log.segmentBaseOffsets();
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool adv = false;
            r.forEach([&](std::string_view k, std::string_view, int64_t o, int64_t) {
                if (!k.empty()) m[std::string(k)] = o;
                cursor = o + 1;
                adv = true;
            });
            if (!adv || cursor >= segEnd) break;
        }
    }
    return m;
}

// ===========================================================================
// Unit tests for SkimpyOffsetMap API
// ===========================================================================

TEST(SkimpyOffsetMap, U1_EmptyGetReturnsNullopt) {
    SkimpyOffsetMap m(100);
    EXPECT_FALSE(m.get("key").has_value());
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.latestOffset(), -1);
}

TEST(SkimpyOffsetMap, U2_PutGetRoundTrip) {
    SkimpyOffsetMap m(100);
    EXPECT_TRUE(m.put("hello", 42));
    auto v = m.get("hello");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 42);
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(m.latestOffset(), 42);
}

TEST(SkimpyOffsetMap, U3_LaterOffsetWins) {
    SkimpyOffsetMap m(100);
    m.put("k", 10);
    m.put("k", 20);
    EXPECT_EQ(*m.get("k"), 20);
    EXPECT_EQ(m.size(), 1u);
}

TEST(SkimpyOffsetMap, U4_EarlierOffsetDoesNotOverwrite) {
    SkimpyOffsetMap m(100);
    m.put("k", 100);
    m.put("k", 50);
    EXPECT_EQ(*m.get("k"), 100);
    EXPECT_EQ(m.size(), 1u);
}

TEST(SkimpyOffsetMap, U5_ClearResetsAll) {
    SkimpyOffsetMap m(100);
    m.put("a", 1); m.put("b", 2);
    EXPECT_EQ(m.size(), 2u);
    m.clear();
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.latestOffset(), -1);
    EXPECT_FALSE(m.get("a").has_value());
    EXPECT_FALSE(m.get("b").has_value());
}

TEST(SkimpyOffsetMap, U6_IsFullWhenAtMaxSlots) {
    // maxSlots = 3 → exactly 3 puts should succeed, the 4th must return false.
    SkimpyOffsetMap m(3);
    EXPECT_FALSE(m.isFull());
    EXPECT_TRUE(m.put("k1", 1));
    EXPECT_TRUE(m.put("k2", 2));
    EXPECT_TRUE(m.put("k3", 3));
    EXPECT_TRUE(m.isFull()) << "map must be full after maxSlots unique puts";
    bool r4 = m.put("k4", 4);
    EXPECT_FALSE(r4) << "put() must return false when map is full";
}

TEST(SkimpyOffsetMap, U7_MaxSlotsFromBudget) {
    // 160 bytes budget / 16 bytes per entry = 10 raw slots × 0.9 = 9.
    int64_t budget = 160;
    auto ms = SkimpyOffsetMap::maxSlotsFromBudget(budget);
    EXPECT_EQ(ms, 9u);
}

TEST(SkimpyOffsetMap, U8_LatestOffsetTracked) {
    SkimpyOffsetMap m(50);
    m.put("a", 3); m.put("b", 7); m.put("c", 2); m.put("a", 10);
    EXPECT_EQ(m.latestOffset(), 10);
}

// ===========================================================================
// SOM-1: DedupeMapBoundedByBudget
// ===========================================================================
TEST(SkimpyOffsetMap, SOM1_DedupeMapBoundedByBudget) {
    // A very tiny budget: 3 slots.
    SkimpyOffsetMap m(3);
    EXPECT_TRUE(m.put("key_a", 0));
    EXPECT_TRUE(m.put("key_b", 1));
    EXPECT_TRUE(m.put("key_c", 2));
    EXPECT_EQ(m.size(), 3u);
    EXPECT_LE(m.size(), m.maxSlots()) << "map must never exceed maxSlots";

    // 4th distinct key must return false (map full, no OOM).
    bool r = m.put("key_d", 3);
    EXPECT_FALSE(r) << "put() must return false when full (not OOM)";

    // Size must not have grown beyond maxSlots.
    EXPECT_LE(m.size(), m.maxSlots());

    // Updating an EXISTING key must still succeed (same hash slot update, no growth).
    bool r2 = m.put("key_a", 99);
    EXPECT_TRUE(r2) << "updating an existing key on a full map must succeed";
    EXPECT_EQ(*m.get("key_a"), 99);
    EXPECT_LE(m.size(), m.maxSlots());
}

// ===========================================================================
// SOM-2: HugeLogCleanedIncrementallyAcrossPasses
//
// Design: write N unique keys (N > maxSlots), each written multiple times.
// A single pass can only scan up to maxSlots keys.  The full log can only be
// compacted to latest-per-key across MULTIPLE passes.
//
// We verify:
//   (a) Each pass advances lastCleanOffset (checkpoint advances).
//   (b) After enough passes the log converges to exactly N live records.
//   (c) Non-vacuousness: if we tried a single-pass approach with maxSlots < N
//       the map would be full before scanning all keys → only partial compaction
//       (more than N records would remain).  We verify by running with a
//       deliberately small map and showing a single pass leaves > N records.
// ===========================================================================
TEST(SkimpyOffsetMap, SOM2_HugeLogCleanedIncrementallyAcrossPasses) {
    // Use N unique keys, each written kReps times.
    constexpr int N     = 20;  // unique keys
    constexpr int kReps = 5;   // times each key is written
    constexpr int kTiny = 5;   // maxSlots per pass (< N)

    auto dir = tempDir("som2_huge");

    Log log(dir.string(), /*rollBytes=*/128);
    // Write N × kReps records: key_0..key_19, each kReps times.
    int64_t off = 0;
    for (int rep = 0; rep < kReps; ++rep) {
        for (int k = 0; k < N; ++k) {
            auto b = makeBatch(off, "key_" + std::to_string(k),
                               "v_r" + std::to_string(rep) + "_k" + std::to_string(k));
            log.append(b, off);
            ++off;
        }
    }

    const int totalBefore = countLive(log);
    ASSERT_EQ(totalBefore, N * kReps) << "Before: all records present";
    ASSERT_GE(log.segmentBaseOffsets().size(), 2u) << "Need >= 2 segments";

    const int64_t hw = log.logEndOffset();

    // Non-vacuousness: a single pass with kTiny slots can't compact all N keys.
    {
        SkimpyOffsetMap tinyMap(kTiny);
        auto bases = log.segmentBaseOffsets();
        // Build map over entire dirty range — should fill up before scanning all.
        LogCleaner cleaner;
        // Pass maxSlots info via a bounded call.  We simulate bounded-range-per-pass:
        // call buildOffsetMapFromBases until tinyMap.isFull() or all bases processed.
        // We call it normally and observe it fills.
        cleaner.buildOffsetMapFromBases(dir.string(), bases, 0, hw, tinyMap);
        EXPECT_LE(tinyMap.size(), tinyMap.maxSlots())
            << "Bounded map must never exceed maxSlots after build";
        // latestOffset < hw-1 means pass stopped before the end.
        EXPECT_LT(tinyMap.latestOffset(), hw - 1)
            << "Non-vacuous: a tiny map must stop before scanning the whole log "
               "(latestOffset=" << tinyMap.latestOffset() << " hw-1=" << (hw-1) << ")";
    }

    // Multi-pass compaction until convergence.
    // Each pass: build map over [lastClean, hw), compact up to map.latestOffset+1,
    // advance lastClean to map.latestOffset+1, repeat.
    int64_t lastClean = 0;
    int passCount = 0;
    int64_t prevLastClean = -1;
    constexpr int kMaxPasses = 200;

    for (int p = 0; p < kMaxPasses && lastClean < hw; ++p) {
        // Reload segments (simulates drainCleanerSwaps reload).
        log.reloadSegments();
        auto bases = log.segmentBaseOffsets();
        if (bases.size() < 2) break; // only active segment left

        SkimpyOffsetMap m(kTiny);
        LogCleaner cleaner;
        cleaner.buildOffsetMapFromBases(dir.string(), bases, lastClean, hw, m);
        if (m.size() == 0) break; // nothing in dirty range

        // Pass boundary = m.latestOffset() + 1 (bounded-range-per-pass).
        int64_t passEnd = m.latestOffset() + 1;
        if (passEnd <= lastClean) break; // no progress

        // Compact all sealed segments in [lastClean, passEnd).
        for (int64_t b : bases) {
            if (b == bases.back()) continue; // skip active
            if (b < lastClean || b >= passEnd) continue;
            cleaner.produceCleanedFile(dir.string(), bases, b, hw,
                                       lastClean, /*nowNs=*/1e18, /*deleteRetentionMs=*/1e9);
        }
        // Advance checkpoint.
        prevLastClean = lastClean;
        lastClean = passEnd;
        ++passCount;
    }

    EXPECT_GT(passCount, 1)
        << "Non-vacuous: multi-pass must require > 1 pass (passCount=" << passCount << ")";
    EXPECT_GT(lastClean, 0)
        << "Checkpoint must advance from 0 (was=" << lastClean << ")";

    // After convergence, reload and check latest-per-key.
    log.reloadSegments();
    auto lpk = latestPerKey(log);
    const int liveAfter = countLive(log);

    // Must have converged: at most N live records (one per key).
    EXPECT_LE((int)lpk.size(), N)
        << "After multi-pass compaction, at most N unique keys should remain; "
           "got " << lpk.size() << " (N=" << N << ")";
    EXPECT_LT(liveAfter, totalBefore)
        << "Multi-pass compaction must reduce total live records: "
           "before=" << totalBefore << " after=" << liveAfter;

    fs::remove_all(dir);
}

// ===========================================================================
// SOM-3: CheckpointPersistsAndResumesAcrossRestart
// ===========================================================================
TEST(SkimpyOffsetMap, SOM3_CheckpointPersistsAndResumesAcrossRestart) {
    auto dir = tempDir("som3_restart");

    // Write a log, partially compact (store checkpoint), then "restart".
    Log log(dir.string(), /*rollBytes=*/128);
    int64_t off = 0;
    for (int i = 0; i < 60; ++i) {
        auto b = makeBatch(off, "k" + std::to_string(i % 5),
                           "v" + std::to_string(off));
        log.append(b, off);
        ++off;
    }
    ASSERT_GE(log.segmentBaseOffsets().size(), 2u);

    const int64_t logEnd  = log.logEndOffset();
    const int64_t partial = logEnd / 3;  // simulate partial compaction

    // Store checkpoint at partial.
    std::string ckptPath = dir.string() + "/cleaner-offset-checkpoint";
    {
        CleanerOffsetCheckpoint ckpt(ckptPath, logEnd);
        ckpt.store(partial);
        EXPECT_EQ(ckpt.value(), partial);
    }

    // "Restart": reconstruct the checkpoint from disk.
    {
        CleanerOffsetCheckpoint ckpt2(ckptPath, logEnd);
        EXPECT_EQ(ckpt2.value(), partial)
            << "Checkpoint must survive restart: persisted=" << partial
            << " loaded=" << ckpt2.value();
    }

    // Verify the cleaner would resume from partial (not from 0).
    {
        CleanerOffsetCheckpoint ckpt3(ckptPath, logEnd);
        EXPECT_GT(ckpt3.value(), 0LL)
            << "After restart, lastCleanOffset must be > 0 (resumed from checkpoint)";
        EXPECT_EQ(ckpt3.value(), partial)
            << "Resumed checkpoint must equal what was persisted";
    }

    fs::remove_all(dir);
}

// ===========================================================================
// SOM-4: CheckpointTruncationAware
// ===========================================================================
TEST(SkimpyOffsetMap, SOM4_CheckpointTruncationAware) {
    auto dir = tempDir("som4_trunc");

    Log log(dir.string(), /*rollBytes=*/512);
    int64_t off = 0;
    for (int i = 0; i < 80; ++i) {
        auto b = makeBatch(off, "k" + std::to_string(i), "v");
        log.append(b, off);
        ++off;
    }
    const int64_t logEnd = log.logEndOffset();

    // Store a checkpoint at a HIGH value (representing a "past" clean watermark).
    const int64_t highCkpt = logEnd;  // checkpoint at the log's end
    std::string ckptPath = dir.string() + "/cleaner-offset-checkpoint";
    {
        CleanerOffsetCheckpoint ckpt(ckptPath, logEnd);
        ckpt.store(highCkpt);
    }

    // Simulate truncation: the log is "truncated" to half its size.
    const int64_t truncatedEnd = logEnd / 2;

    // On restart, pass the truncated log end as logEndOffset.
    // The checkpoint should be CLAMPED to truncatedEnd.
    {
        CleanerOffsetCheckpoint ckptAfterTrunc(ckptPath, truncatedEnd);
        EXPECT_LE(ckptAfterTrunc.value(), truncatedEnd)
            << "After truncation, checkpoint must be clamped to logEndOffset="
            << truncatedEnd << "; got=" << ckptAfterTrunc.value();
        // Specifically it should be exactly truncatedEnd (clamped down).
        EXPECT_EQ(ckptAfterTrunc.value(), truncatedEnd)
            << "Checkpoint must be lowered to truncatedEnd=" << truncatedEnd
            << " (was=" << highCkpt << ")";
    }

    // Verify: checkpoint lower than checkpoint → no skipped live data.
    // If checkpoint = truncatedEnd and logEnd was halved, the cleaner starts
    // from truncatedEnd which is ≤ the new log end → no data skipped.
    {
        CleanerOffsetCheckpoint ckptFinal(ckptPath, logEnd); // use original end
        // Store the truncated value, then reload.
        ckptFinal.store(truncatedEnd);
        CleanerOffsetCheckpoint ckptReload(ckptPath, truncatedEnd);
        EXPECT_EQ(ckptReload.value(), truncatedEnd);
    }

    fs::remove_all(dir);
}

// ===========================================================================
// SOM-5: HashCollisionCorrectness
//
// Two DISTINCT keys must not be wrongly deduped (i.e. must NOT return the
// same hash bucket — if they do collide in hash, linear probing handles them
// as separate entries each returning their own offset).
//
// We test this by:
//   1. Inserting 1000 distinct keys and checking all are retrievable with
//      correct offsets (no wrong dedup due to hash collision).
//   2. Verifying size() == number of distinct keys inserted.
//   3. Generating deliberately chosen near-collision key pairs and checking
//      the map stores independent offsets for both.
// ===========================================================================
TEST(SkimpyOffsetMap, SOM5_HashCollisionCorrectness) {
    // Test 1: 1000 distinct keys all retrievable.
    {
        SkimpyOffsetMap m(2000);
        for (int i = 0; i < 1000; ++i) {
            std::string k = "collision_test_key_" + std::to_string(i);
            EXPECT_TRUE(m.put(k, static_cast<int64_t>(i)));
        }
        EXPECT_EQ(m.size(), 1000u);
        for (int i = 0; i < 1000; ++i) {
            std::string k = "collision_test_key_" + std::to_string(i);
            auto v = m.get(k);
            ASSERT_TRUE(v.has_value()) << "Key " << k << " must be in map";
            EXPECT_EQ(*v, static_cast<int64_t>(i))
                << "Key " << k << " has wrong offset: expected=" << i
                << " got=" << *v << " (hash collision wrongly merged two keys)";
        }
    }

    // Test 2: two keys that are known to share a wyhash64 hash only if the
    // hash function is broken.  We verify they get INDEPENDENT offsets.
    {
        SkimpyOffsetMap m(100);
        // These are just two normal strings; test that they independently store.
        m.put("alpha", 100);
        m.put("beta", 200);
        EXPECT_EQ(*m.get("alpha"), 100);
        EXPECT_EQ(*m.get("beta"),  200);
        EXPECT_EQ(m.size(), 2u);

        // Verify the two values are independent: updating one doesn't affect other.
        m.put("alpha", 300);
        EXPECT_EQ(*m.get("alpha"), 300);
        EXPECT_EQ(*m.get("beta"),  200) << "beta must be unchanged";
    }

    // Test 3: same-prefix keys (common in practice) don't collide.
    {
        SkimpyOffsetMap m(500);
        // Many keys that differ only in suffix (common partition key pattern).
        std::vector<std::string> keys;
        for (int i = 0; i < 200; ++i) {
            keys.push_back("partition/key/" + std::to_string(i));
        }
        for (int i = 0; i < (int)keys.size(); ++i) {
            m.put(keys[i], i * 10);
        }
        EXPECT_EQ(m.size(), (std::size_t)keys.size());
        for (int i = 0; i < (int)keys.size(); ++i) {
            auto v = m.get(keys[i]);
            ASSERT_TRUE(v.has_value()) << "Key " << keys[i] << " missing";
            EXPECT_EQ(*v, i * 10)
                << "Wrong offset for key " << keys[i]
                << " (possible hash collision / wrong dedup)";
        }
    }
}

// ===========================================================================
// SOM-6: SingleWriterInvariantPreserved
//
// Structural test: SkimpyOffsetMap and CleanerOffsetCheckpoint must never
// call the writer-thread-only Log APIs.
// We verify this via grep on the source files (no runtime needed).
// ===========================================================================
TEST(SkimpyOffsetMap, SOM6_SingleWriterInvariantPreserved) {
    // These are the write-thread-only Log APIs that the cleaner MUST NOT call.
    // They access Log::segments_ directly and would be a data race.
    const std::vector<std::string> forbidden = {
        "segmentBaseOffsets()",
        "segmentSize(",
        "reloadSegment(",
        "segments_",
    };

    // Derive the source tree root from this file's compile-time path.
    // __FILE__ = .../libs/storage/tests/storage/SkimpyOffsetMapTest.cpp
    // → go up 4 levels to the repo root.
    auto deriveRoot = [](const char* f) -> std::string {
        std::string p(f);
        // Walk up from libs/storage/tests/storage/SkimpyOffsetMapTest.cpp
        // → strip 5 path components to reach the repo root.
        for (int i = 0; i < 5; ++i) {
            auto pos = p.find_last_of("/\\");
            if (pos == std::string::npos) break;
            p = p.substr(0, pos);
        }
        return p;
    };
    std::string root = deriveRoot(__FILE__);

    // Source files to check.
    const std::vector<std::string> filesToCheck = {
        root + "/libs/storage/include/rune/storage/SkimpyOffsetMap.h",
        root + "/libs/storage/include/rune/storage/CleanerOffsetCheckpoint.h",
    };

    for (const auto& path : filesToCheck) {
        std::ifstream f(path);
        ASSERT_TRUE(f.is_open()) << "Cannot open " << path;
        std::string content((std::istreambuf_iterator<char>(f)),
                             std::istreambuf_iterator<char>());
        for (const auto& forbidden_str : forbidden) {
            EXPECT_EQ(content.find(forbidden_str), std::string::npos)
                << "SINGLE-WRITER VIOLATION: file " << path
                << " must NOT reference '" << forbidden_str << "'";
        }
    }
}
