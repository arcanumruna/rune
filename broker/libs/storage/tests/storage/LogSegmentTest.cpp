#include <gtest/gtest.h>
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <numeric>
using namespace rune::storage; using namespace rune::protocol;

// Helper: build a batch with a given number of records
static std::vector<uint8_t> makeBatch(int64_t baseOffset, int64_t baseTs, int nRecords) {
    RecordBatchBuilder b(baseOffset, baseTs);
    for (int i = 0; i < nRecords; ++i)
        b.append("key", "val", baseTs + i);
    return b.build();
}

TEST(LogSegment, AppendThenReadBack) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg";
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

// Edge: multiple batches with small indexIntervalBytes so multiple sparse index entries are created;
// read() from offsets that land on different index entries returns the correct first batch.
TEST(LogSegment, MultipleSparseIndexEntriesCorrectRead) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_sparse";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_readoff";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_small";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_size";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_nonzero";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec_clean";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec_empty";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 42);
    int64_t next = seg.recover();
    EXPECT_EQ(next, 42); // baseOffset
    std::filesystem::remove_all(dir);
}

// Recovery: mid-log corruption — first batch valid, second batch CRC-corrupted, third discarded.
TEST(LogSegment, RecoverMidLogCorruptionKeepsOnlyFirstBatch) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec_mid";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec_idx";
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
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_many";
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
