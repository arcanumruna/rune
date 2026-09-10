#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <fstream>
using namespace rune::storage; using namespace rune::protocol;

// ---- Test 1 (brief's canonical test): rolls + reads across segment boundary ----
TEST(Log, RollsSegmentsAndReadsAcross) {
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t1";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t2";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t3";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t4";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t5";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_log_t6";
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
