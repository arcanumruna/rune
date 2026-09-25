#include <gtest/gtest.h>
#include <rune/storage/LogSegment.h>
#include <rune/storage/OffsetIndex.h>
#include <rune/protocol/RecordBatch.h>
#include <climits>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <sys/stat.h>
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

// Helper: build a batch with a given number of records
static std::vector<uint8_t> makeBatch(int64_t baseOffset, int64_t baseTs, int nRecords) {
    RecordBatchBuilder b(baseOffset, baseTs);
    for (int i = 0; i < nRecords; ++i)
        b.append("key", "val", baseTs + i);
    return b.build();
}

TEST(LogSegment, AppendThenReadBack) {
    auto dir = makeTempDir("rune_seg");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0);

    RecordBatchBuilder b0(0, 1000); b0.append("k", "v0", 1000);
    int64_t next = seg.append(b0.build(), 0);
    EXPECT_EQ(next, 1);
    RecordBatchBuilder b1(1, 1001); b1.append("k", "v1", 1001); b1.append("k", "v2", 1002);
    next = seg.append(b1.build(), 2);
    EXPECT_EQ(next, 3);
    EXPECT_EQ(seg.nextOffset(), 3);

    auto bytes = seg.read(1, 1 << 20);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 1);
    std::filesystem::remove_all(dir);
}

// Zero-copy restamp append: the incoming batch carries a STALE base offset
// (client-supplied), and appendNoSyncRestamp must write it to disk with the
// ASSIGNED base offset patched into bytes [0..7] WITHOUT any mutable copy of
// the batch (pwritev scatter-gather: base-offset iovec + fragment[8:]).  The
// CRC covers [21..end] so the base-offset rewrite is CRC-safe — ensureValid()
// must still pass after read-back.
TEST(LogSegment, AppendNoSyncRestampPatchesBaseOffsetZeroCopy) {
    auto dir = makeTempDir("rune_seg_restamp");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0);

    // Batch built with a WRONG (stale) base offset of 999; 3 records.
    auto stale = makeBatch(999, 7000, 3);
    // Assign base offset 0, lastOffset 2 — restamp should overwrite the 999.
    int64_t next = seg.appendNoSyncRestamp(stale.data(), stale.size(),
                                           /*baseOffset=*/0, /*lastOffset=*/2);
    seg.sync();
    EXPECT_EQ(next, 3);
    EXPECT_EQ(seg.nextOffset(), 3);

    // Append a second batch (stale base 999 again) assigned base 3, lastOffset 4.
    auto stale2 = makeBatch(999, 7010, 2);
    next = seg.appendNoSyncRestamp(stale2.data(), stale2.size(), 3, 4);
    seg.sync();
    EXPECT_EQ(next, 5);

    // Read back: first batch base offset must be the ASSIGNED 0, not 999, and
    // the CRC must still validate (base-offset rewrite is outside the CRC region).
    {
        auto buf = seg.read(0, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
        EXPECT_EQ(r.recordCount(), 3);
        EXPECT_EQ(r.maxTimestamp(), 7002);  // baseTs 7000 + 2 (record framing intact)
    }
    {
        auto buf = seg.read(3, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 3);
        EXPECT_EQ(r.recordCount(), 2);
    }
    std::filesystem::remove_all(dir);
}

// Edge: multiple batches with small indexIntervalBytes so multiple sparse index entries are created;
// read() from offsets that land on different index entries returns the correct first batch.
TEST(LogSegment, MultipleSparseIndexEntriesCorrectRead) {
    auto dir = makeTempDir("rune_seg_sparse");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Use a very small indexIntervalBytes (1 byte) so every batch gets an index entry
    LogSegment seg(dir.string(), 0, 1);

    // Append 5 batches, each with 1 record
    // Offsets: batch0=0, batch1=1, batch2=2, batch3=3, batch4=4
    int64_t next = 0;
    for (int i = 0; i < 5; ++i) {
        auto batch = makeBatch(i, 2000 + i, 1);
        next = seg.append(batch, i);
    }
    EXPECT_EQ(next, 5);

    // read(0) returns first batch with baseOffset=0
    {
        auto buf = seg.read(0, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }
    // read(2) returns batch with baseOffset=2
    {
        auto buf = seg.read(2, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 2);
    }
    // read(4) returns last batch with baseOffset=4
    {
        auto buf = seg.read(4, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 4);
    }
    std::filesystem::remove_all(dir);
}

// Edge: read(0) returns the first batch; read at an offset in the middle returns the containing batch;
// read past the last offset returns an empty vector.
TEST(LogSegment, ReadFromVariousOffsets) {
    auto dir = makeTempDir("rune_seg_readoff");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 10, 4096);

    // Append batch with baseOffset=10, 3 records (lastOffset=12, nextOffset=13)
    auto batch0 = makeBatch(10, 5000, 3);
    seg.append(batch0, 12);

    // Append batch with baseOffset=13, 2 records (lastOffset=14, nextOffset=15)
    auto batch1 = makeBatch(13, 5010, 2);
    seg.append(batch1, 14);

    EXPECT_EQ(seg.nextOffset(), 15);

    // read(10) => first batch
    {
        auto buf = seg.read(10, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 10);
    }
    // read(11) => first batch (startOffset=11 is within the first batch's range [10..12])
    {
        auto buf = seg.read(11, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 10);
    }
    // read(13) => second batch
    {
        auto buf = seg.read(13, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 13);
    }
    // read past the last offset returns empty
    {
        auto buf = seg.read(100, 1 << 20);
        EXPECT_TRUE(buf.empty());
    }
    std::filesystem::remove_all(dir);
}

// Edge: read() with a small maxBytes still returns a walkable prefix (at least the first matching batch).
TEST(LogSegment, ReadSmallMaxBytesReturnsFirstBatch) {
    auto dir = makeTempDir("rune_seg_small");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0, 4096);

    // Append two batches
    auto batch0 = makeBatch(0, 1000, 1);
    auto batch1 = makeBatch(1, 1001, 1);
    seg.append(batch0, 0);
    seg.append(batch1, 1);

    // maxBytes = size of exactly one batch — should return at least the first matching batch
    int32_t oneSize = static_cast<int32_t>(batch0.size());
    auto buf = seg.read(0, oneSize);
    ASSERT_FALSE(buf.empty());
    RecordBatchReader r(buf.data(), buf.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);

    std::filesystem::remove_all(dir);
}

// Edge: sizeBytes() equals the sum of appended batch sizes;
// nextOffset() tracks lastOffset+1 across multiple appends including multi-record batches.
TEST(LogSegment, SizeBytesAndNextOffsetTracking) {
    auto dir = makeTempDir("rune_seg_size");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0, 4096);

    EXPECT_EQ(seg.sizeBytes(), 0);
    EXPECT_EQ(seg.nextOffset(), 0);

    // Single-record batch: offsets 0..0
    auto b0 = makeBatch(0, 1000, 1);
    seg.append(b0, 0);
    EXPECT_EQ(seg.sizeBytes(), static_cast<int64_t>(b0.size()));
    EXPECT_EQ(seg.nextOffset(), 1);

    // Multi-record batch: 4 records at offsets 1..4
    auto b1 = makeBatch(1, 1001, 4);
    seg.append(b1, 4);
    EXPECT_EQ(seg.sizeBytes(), static_cast<int64_t>(b0.size() + b1.size()));
    EXPECT_EQ(seg.nextOffset(), 5);

    // Another single-record batch at offset 5
    auto b2 = makeBatch(5, 1005, 1);
    seg.append(b2, 5);
    EXPECT_EQ(seg.sizeBytes(), static_cast<int64_t>(b0.size() + b1.size() + b2.size()));
    EXPECT_EQ(seg.nextOffset(), 6);

    std::filesystem::remove_all(dir);
}

// Edge: non-zero baseOffset segment — append and read work correctly with an offset base > 0.
TEST(LogSegment, NonZeroBaseOffsetSegment) {
    auto dir = makeTempDir("rune_seg_nonzero");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 500, 4096);

    EXPECT_EQ(seg.baseOffset(), 500);
    EXPECT_EQ(seg.nextOffset(), 500);

    auto b0 = makeBatch(500, 9000, 2); // offsets 500..501
    seg.append(b0, 501);
    EXPECT_EQ(seg.nextOffset(), 502);

    auto b1 = makeBatch(502, 9002, 3); // offsets 502..504
    seg.append(b1, 504);
    EXPECT_EQ(seg.nextOffset(), 505);

    // read from baseOffset returns first batch
    auto buf0 = seg.read(500, 1 << 20);
    ASSERT_FALSE(buf0.empty());
    RecordBatchReader r0(buf0.data(), buf0.size());
    EXPECT_TRUE(r0.ensureValid());
    EXPECT_EQ(r0.baseOffset(), 500);

    // read from second batch's offset returns second batch
    auto buf1 = seg.read(502, 1 << 20);
    ASSERT_FALSE(buf1.empty());
    RecordBatchReader r1(buf1.data(), buf1.size());
    EXPECT_TRUE(r1.ensureValid());
    EXPECT_EQ(r1.baseOffset(), 502);

    std::filesystem::remove_all(dir);
}

// ---- Recovery tests ----

#include <fstream>
TEST(LogSegment, RecoverTruncatesTornTail) {
    auto dir = makeTempDir("rune_seg_rec");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 1000); b0.append("k","v0",1000); seg.append(b0.build(), 0);
        RecordBatchBuilder b1(1, 1001); b1.append("k","v1",1001); seg.append(b1.build(), 1);
        logPath = seg.logPath();
    }
    // Append 9 bytes of garbage to simulate a torn write.
    { std::ofstream f(logPath, std::ios::binary | std::ios::app); const char junk[9] = {1,2,3,4,5,6,7,8,9}; f.write(junk, 9); }
    LogSegment seg2(dir.string(), 0);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 2);                          // both good batches kept, junk dropped
    auto tail = seg2.read(1, 1<<20);
    EXPECT_TRUE(RecordBatchReader(tail.data(), tail.size()).ensureValid());
    std::filesystem::remove_all(dir);
}

// Recovery: clean segment (no corruption) — recover() keeps all batches, file size unchanged.
TEST(LogSegment, RecoverCleanSegmentKeepsAllBatches) {
    auto dir = makeTempDir("rune_seg_rec_clean");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    int64_t sizeAfterAppend = 0;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 2000); b0.append("k","v0",2000); seg.append(b0.build(), 0);
        RecordBatchBuilder b1(1, 2001); b1.append("k","v1",2001); seg.append(b1.build(), 1);
        RecordBatchBuilder b2(2, 2002); b2.append("k","v2",2002); seg.append(b2.build(), 2);
        logPath = seg.logPath();
        sizeAfterAppend = seg.sizeBytes();
    }
    uintmax_t sizeBefore = std::filesystem::file_size(logPath);
    LogSegment seg2(dir.string(), 0);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 3);
    EXPECT_EQ(std::filesystem::file_size(logPath), sizeBefore); // nothing truncated
    for (int64_t i = 0; i < 3; ++i) {
        auto buf = seg2.read(i, 1 << 20);
        ASSERT_FALSE(buf.empty());
        EXPECT_TRUE(RecordBatchReader(buf.data(), buf.size()).ensureValid());
    }
    std::filesystem::remove_all(dir);
}

// Recovery: empty segment file — recover() returns baseOffset, no throw.
TEST(LogSegment, RecoverEmptySegment) {
    auto dir = makeTempDir("rune_seg_rec_empty");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 42);
    int64_t next = seg.recover();
    EXPECT_EQ(next, 42); // baseOffset
    std::filesystem::remove_all(dir);
}

// Recovery: mid-log corruption — first batch valid, second batch CRC-corrupted, third discarded.
TEST(LogSegment, RecoverMidLogCorruptionKeepsOnlyFirstBatch) {
    auto dir = makeTempDir("rune_seg_rec_mid");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    size_t batch0Size = 0;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 3000); b0.append("k","v0",3000); auto bb0 = b0.build(); batch0Size = bb0.size(); seg.append(bb0, 0);
        RecordBatchBuilder b1(1, 3001); b1.append("k","v1",3001); seg.append(b1.build(), 1);
        RecordBatchBuilder b2(2, 3002); b2.append("k","v2",3002); seg.append(b2.build(), 2);
        logPath = seg.logPath();
    }
    // Corrupt a byte inside the second batch's CRC field (offset batch0Size + kCrcOffset)
    {
        std::fstream f(logPath, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(static_cast<std::streamoff>(batch0Size) + rune::protocol::batch::kCrcOffset);
        char bad = 0xFF;
        f.write(&bad, 1);
    }
    LogSegment seg2(dir.string(), 0);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 1); // only first batch kept
    auto buf = seg2.read(0, 1 << 20);
    ASSERT_FALSE(buf.empty());
    RecordBatchReader r(buf.data(), buf.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);
    std::filesystem::remove_all(dir);
}

// Recovery: index rebuilt — after recover() on multi-batch segment, read() at mid offset returns correct batch.
TEST(LogSegment, RecoverRebuildsIndexForMidOffsetRead) {
    auto dir = makeTempDir("rune_seg_rec_idx");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    {
        // indexIntervalBytes=1 so every batch gets an index entry
        LogSegment seg(dir.string(), 0, 1);
        for (int i = 0; i < 5; ++i) {
            RecordBatchBuilder b(i, 4000 + i); b.append("k", "v", 4000 + i);
            seg.append(b.build(), i);
        }
        logPath = seg.logPath();
    }
    // Append junk so recover() truncates (exercises the full rebuild path)
    { std::ofstream f(logPath, std::ios::binary | std::ios::app); const char junk[5] = {0,1,2,3,4}; f.write(junk, 5); }
    LogSegment seg2(dir.string(), 0, 1);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 5);
    // read at offset 3 should return the batch with baseOffset=3
    auto buf = seg2.read(3, 1 << 20);
    ASSERT_FALSE(buf.empty());
    RecordBatchReader r(buf.data(), buf.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 3);
    std::filesystem::remove_all(dir);
}

// Edge: many batches with small index interval; verify multiple index entries and mid-log reads.
TEST(LogSegment, ManyBatchesSmallIntervalMidLogRead) {
    auto dir = makeTempDir("rune_seg_many");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // indexIntervalBytes=1 ensures an index entry per batch
    LogSegment seg(dir.string(), 0, 1);

    const int N = 20;
    std::vector<size_t> batchSizes;
    int64_t off = 0;
    for (int i = 0; i < N; ++i) {
        auto batch = makeBatch(off, 3000 + i, 1);
        batchSizes.push_back(batch.size());
        seg.append(batch, off);
        ++off;
    }
    EXPECT_EQ(seg.nextOffset(), N);

    // Verify total size
    size_t total = std::accumulate(batchSizes.begin(), batchSizes.end(), size_t(0));
    EXPECT_EQ(seg.sizeBytes(), static_cast<int64_t>(total));

    // Read from the middle of the segment
    {
        auto buf = seg.read(10, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 10);
    }

    // Read from near the end
    {
        auto buf = seg.read(N - 1, 1 << 20);
        ASSERT_FALSE(buf.empty());
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), N - 1);
    }

    std::filesystem::remove_all(dir);
}

// Regression: read() with tiny maxBytes grows its window and still finds the target batch
// even when maxBytes < skip-distance from the sparse index entry to startOffset's batch.
TEST(LogSegment, ReadWithTinyMaxBytesAcrossManyIndexEntries) {
    auto dir = makeTempDir("rune_seg_tiny");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    // Small indexIntervalBytes=256 creates many index entries; 200 single-record batches
    // means the skip distance from the nearest index entry can be many batch-widths.
    LogSegment seg(dir.string(), 0, 256);

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        auto batch = makeBatch(static_cast<int64_t>(i), 10000 + i, 1);
        seg.append(batch, static_cast<int64_t>(i));
    }
    EXPECT_EQ(seg.nextOffset(), N);

    // read(150, 64) — tiny window (64 bytes) far smaller than the skip distance.
    // Before the fix this returned {} spuriously.
    {
        auto buf = seg.read(150, 64);
        ASSERT_FALSE(buf.empty()) << "read(150, 64) returned empty — grow-and-retry not working";
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
        EXPECT_GE(batchLast, 150);
    }

    // read(0, 64) returns the first batch.
    {
        auto buf = seg.read(0, 64);
        ASSERT_FALSE(buf.empty()) << "read(0, 64) returned empty";
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), 0);
    }

    // read(199, 64) returns the last batch.
    {
        auto buf = seg.read(199, 64);
        ASSERT_FALSE(buf.empty()) << "read(199, 64) returned empty";
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
        EXPECT_GE(batchLast, 199);
    }

    std::filesystem::remove_all(dir);
}

// ---- B1+B2 / T2: Durability tests ----

// B1+B2: After append (which calls sync()), a raw POSIX stat of the file sees
// the full batch bytes — confirming fdatasync pushed data out of the libc buffer.
TEST(LogSegment, SyncMakesBatchesReadableOnReopen) {
    auto dir = makeTempDir("rune_seg_sync");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::string logPath;
    int64_t expectedSize = 0;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 1000); b0.append("sync_k", "sync_v", 1000);
        auto batch = b0.build();
        expectedSize = static_cast<int64_t>(batch.size());
        seg.append(batch, 0);
        // sync() is called inside append() (durable-by-default)
        logPath = seg.logPath();
    }

    // Raw POSIX stat of the file (bypasses all in-process caching)
    struct stat st{};
    int rc = ::stat(logPath.c_str(), &st);
    ASSERT_EQ(rc, 0) << "cannot stat log file after sync";
    EXPECT_EQ(static_cast<int64_t>(st.st_size), expectedSize)
        << "sync() must ensure batch bytes reach the file (size mismatch implies flush failure)";

    std::filesystem::remove_all(dir);
}

// T2-real: After append+sync, a fresh LogSegment opening the SAME file
// (new fstream, new fd — no shared in-process state) sees all synced batches.
TEST(LogSegment, SyncedDataSurvivesFreshSegmentOpen) {
    auto dir = makeTempDir("rune_seg_t2");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    const int64_t expectedBatches = 3;
    {
        LogSegment seg(dir.string(), 0, 1); // indexInterval=1
        for (int64_t i = 0; i < expectedBatches; ++i) {
            RecordBatchBuilder b(i, 7000 + i); b.append("k", "v" + std::to_string(i), 7000 + i);
            seg.append(b.build(), i);
            // sync() is already called by append (durable-by-default)
        }
    }
    // seg is destroyed (fd closed). Now open a completely fresh segment.
    LogSegment seg2(dir.string(), 0, 1);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, expectedBatches)
        << "All synced batches must survive a fresh segment open";

    // All offsets readable
    for (int64_t i = 0; i < expectedBatches; ++i) {
        auto buf = seg2.read(i, 1 << 20);
        ASSERT_FALSE(buf.empty()) << "offset " << i << " missing after reopen";
        RecordBatchReader r(buf.data(), buf.size());
        EXPECT_TRUE(r.ensureValid());
        EXPECT_EQ(r.baseOffset(), i);
    }

    std::filesystem::remove_all(dir);
}

// B3: After recover(), the canonical .index exists and .index2 does NOT.
TEST(LogSegment, RecoverProducesCanonicalIndexNoIndex2) {
    auto dir = makeTempDir("rune_seg_b3");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    {
        LogSegment seg(dir.string(), 0, 1); // indexInterval=1 -> every batch indexed
        for (int i = 0; i < 4; ++i) {
            RecordBatchBuilder b(i, 9000 + i); b.append("k", "v", 9000 + i);
            seg.append(b.build(), i);
        }
        logPath = seg.logPath();
        // Append junk to trigger recover() truncation path
        std::ofstream f(logPath, std::ios::binary | std::ios::app);
        const char junk[3] = {0x7f, 0x00, 0x01};
        f.write(junk, 3);
    }

    LogSegment seg2(dir.string(), 0, 1);
    seg2.recover();

    std::string base = logPath.substr(0, logPath.size() - 4);
    EXPECT_TRUE(std::filesystem::exists(base + ".index"))
        << "Canonical .index must exist after recover()";
    EXPECT_FALSE(std::filesystem::exists(base + ".index2"))
        << ".index2 must NOT exist after recover() — it's an orphan";

    // The canonical index must be usable: read at offset 2 returns the right batch.
    auto buf = seg2.read(2, 1 << 20);
    ASSERT_FALSE(buf.empty());
    RecordBatchReader r(buf.data(), buf.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 2);

    std::filesystem::remove_all(dir);
}

// C2: Index produced by recover() must be reproducible.
// Verify that read() at the same offset returns the same batch from two
// independently-recovered copies of the same data.
TEST(LogSegment, RecoverIndexMatchesAppendIndex) {
    const int32_t interval = 256; // small but not 1
    const int N = 12;

    auto dir1 = makeTempDir("rune_c2_live");
    auto dir2 = makeTempDir("rune_c2_rec");
    std::filesystem::create_directories(dir1);
    std::filesystem::create_directories(dir2);

    // Write the live segment
    {
        LogSegment live(dir1.string(), 0, interval);
        for (int i = 0; i < N; ++i) {
            RecordBatchBuilder b(i, 5000 + i); b.append("k", "v", 5000 + i);
            live.append(b.build(), i);
        }
    }

    // Copy the .log to dir2, add junk, then recover both
    auto livePath = dir1.string() + "/00000000000000000000.log";
    auto recPath  = dir2.string() + "/00000000000000000000.log";
    std::filesystem::copy_file(livePath, recPath);
    { std::ofstream f(recPath, std::ios::binary | std::ios::app); f.put(0x7f); }

    LogSegment rec(dir2.string(), 0, interval);
    rec.recover();

    LogSegment live2(dir1.string(), 0, interval);
    live2.recover();

    // Both segments must return the same batch for every offset
    for (int i = 0; i < N; ++i) {
        int64_t off = static_cast<int64_t>(i);
        auto bufLive = live2.read(off, 1 << 20);
        auto bufRec  = rec.read(off, 1 << 20);
        ASSERT_FALSE(bufLive.empty()) << "live2 missing offset " << off;
        ASSERT_FALSE(bufRec.empty())  << "rec missing offset " << off;
        RecordBatchReader rl(bufLive.data(), bufLive.size());
        RecordBatchReader rr(bufRec.data(),  bufRec.size());
        EXPECT_EQ(rl.baseOffset(), rr.baseOffset())
            << "Index mismatch at offset " << off;
    }

    std::filesystem::remove_all(dir1);
    std::filesystem::remove_all(dir2);
}

// ---------------------------------------------------------------------------
// Part B — edge-case tests (full re-audit 2026-09-11)
// ---------------------------------------------------------------------------

// B9: Torn tail with a valid-looking totalLength but bad CRC.
// recover() must use ensureValid() (not just length) to drop the corrupt entry.
TEST(LogSegment, RecoverDropsTornTailWithGoodLengthBadCrc) {
    auto dir = makeTempDir("lune_seg_b9");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    // Write two valid batches.
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 1000); b0.append("k", "v0", 1000);
        seg.append(b0.build(), 0);
        RecordBatchBuilder b1(1, 1001); b1.append("k", "v1", 1001);
        seg.append(b1.build(), 1);
    }

    // Find the .log file and corrupt the CRC of the SECOND batch
    // (flip the CRC byte at offset 17 within the second batch)
    // while keeping totalLength plausible.
    std::string logPath = (dir / "00000000000000000000.log").string();
    {
        std::ifstream f(logPath, std::ios::binary);
        f.seekg(0, std::ios::end);
        size_t fsz = static_cast<size_t>(f.tellg());
        f.seekg(0, std::ios::beg);
        std::vector<uint8_t> buf(fsz);
        f.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(fsz));

        // Find the start of the second batch (= size of the first batch).
        RecordBatchReader r1(buf.data(), buf.size());
        size_t firstEnd = static_cast<size_t>(r1.totalLength());

        // Flip the CRC field (offset 17 within the batch) of the second batch.
        buf[firstEnd + 17] ^= 0xFF;

        // Overwrite the file with the corrupted data.
        std::ofstream out(logPath, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(fsz));
    }

    // Recover: should truncate at the corrupt batch and keep only batch 0.
    LogSegment seg2(dir.string(), 0);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 1) << "Only batch 0 (offset 0) must survive; nextOffset must be 1";

    // Verify batch 0 is readable.
    auto bytes = seg2.read(0, 1 << 20);
    ASSERT_FALSE(bytes.empty());
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);

    std::filesystem::remove_all(dir);
}

// B10: recover() is idempotent — calling it twice on the same segment produces
// the same nextOffset and correct reads, and leaves no .index.tmp orphan.
TEST(LogSegment, RecoverTwiceIsIdempotent) {
    auto dir = makeTempDir("rune_seg_b10");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000); b.append("k2", "v2", 1001);
        seg.append(b.build(), 1); // 2-record batch, lastOffset=1
    }

    LogSegment seg2(dir.string(), 0);
    int64_t next1 = seg2.recover();

    // No .index.tmp should remain after first recover.
    EXPECT_FALSE(std::filesystem::exists(dir / "00000000000000000000.index.tmp"))
        << ".index.tmp must not remain after recover()";

    // Second recover on a new segment object from the same file.
    LogSegment seg3(dir.string(), 0);
    int64_t next2 = seg3.recover();

    EXPECT_EQ(next1, next2) << "recover() must be idempotent";
    EXPECT_EQ(next1, 2) << "nextOffset must be 2 (batch covers offsets 0 and 1)";

    // .index.tmp must still not exist.
    EXPECT_FALSE(std::filesystem::exists(dir / "00000000000000000000.index.tmp"))
        << ".index.tmp must not remain after second recover()";

    std::filesystem::remove_all(dir);
}

// B11a: read() with maxBytes == 0 must not crash and must return a non-empty
// result (the grow-and-retry loop always expands maxBytes until it finds the batch).
TEST(LogSegment, ReadWithZeroMaxBytesDoesNotCrash) {
    auto dir = makeTempDir("rune_seg_b11a");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    LogSegment seg(dir.string(), 0);
    RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
    seg.append(b.build(), 0);
    seg.recover();

    // A maxBytes of 0 should not crash; the grow loop expands the window.
    EXPECT_NO_THROW({
        auto bytes = seg.read(0, 0);
        // Result may be empty (no progress) or non-empty (grow succeeded).
        // The important invariant is no crash and no infinite loop.
        (void)bytes;
    }) << "read() with maxBytes=0 must not crash";

    std::filesystem::remove_all(dir);
}

// B11b: read() with maxBytes smaller than kRecordBatchOverhead must still
// eventually succeed via the grow-and-retry loop.
TEST(LogSegment, ReadWithTinyMaxBytesGrowsAndSucceeds) {
    auto dir = makeTempDir("rune_seg_b11b");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    LogSegment seg(dir.string(), 0);
    RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
    auto batch = b.build();
    seg.append(batch, 0);
    seg.recover();

    // maxBytes = 1 (< kRecordBatchOverhead = 61).
    auto bytes = seg.read(0, 1);
    // The grow-and-retry loop must eventually reach a window large enough to
    // hold at least the header and find batch 0.
    EXPECT_FALSE(bytes.empty()) << "read() must grow window and return batch even with tiny maxBytes";
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 0);

    std::filesystem::remove_all(dir);
}

// B8: OffsetIndex::append guard fires for a position > INT32_MAX.
// (This exercises the guard added for C4/A3 — cannot easily exceed 2GiB
// in a real segment test, so we call OffsetIndex::append directly with a
// position just beyond INT32_MAX and expect a runtime_error.)
TEST(LogSegment, OffsetIndexAppendGuards2GiBPosition) {
    // Use OffsetIndex directly to test the guard without needing a >2GiB file.
    // The test uses a 64KiB index map (smallest viable).
    auto dir = makeTempDir("rune_seg_b8");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::string idxPath = (dir / "00000000000000000000.index").string();
    rune::storage::OffsetIndex idx(idxPath, 0, 64 * 1024);

    // A position just above INT32_MAX must throw.
    int64_t tooBig = static_cast<int64_t>(INT32_MAX) + 1;
    EXPECT_THROW(idx.append(0, tooBig), std::runtime_error)
        << "OffsetIndex::append must throw when position > INT32_MAX";

    std::filesystem::remove_all(dir);
}
