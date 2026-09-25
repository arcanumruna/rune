#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <atomic>
#include <chrono>
#include <cstring>
#include <unistd.h>
using namespace rune::storage; using namespace rune::protocol;

// ---------------------------------------------------------------------------
// T1 — zero-copy Log::readView (mmap segment).
//
// readView returns a span into a read-only mmap of the .log covering WHOLE
// record batches, starting at the first batch whose lastOffset >= startOffset,
// up to maxBytes (always >= 1 batch).  These tests lock the contract that
// callers (gateway push, per-worker read pool) will depend on in T2/T3, and
// prove byte-equivalence with the existing vector read() on the first batch.
// ---------------------------------------------------------------------------

static std::filesystem::path makeTempDir(const char* stem) {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto uniq = std::to_string(::getpid()) + "_" +
                std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                std::to_string(ts);
    return std::filesystem::temp_directory_path() / (std::string(stem) + "_" + uniq);
}

// readView's first batch must be byte-identical to read()'s first batch, and the
// view must be a whole number of parseable batches ending exactly on a boundary.
TEST(LogReadView, MatchesReadFirstBatchAndIsWholeBatches) {
    auto dir = makeTempDir("rune_rv_match");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 64LL * 1024 * 1024); // single segment
    int64_t off = 0;
    for (int i = 0; i < 20; ++i) {
        RecordBatchBuilder b(off, 1000 + i);
        b.append("key", "value" + std::to_string(i), 1000 + i);
        off = log.append(b.build(), off);
    }
    ASSERT_EQ(log.logEndOffset(), 20);

    for (int64_t start : {int64_t{0}, int64_t{1}, int64_t{7}, int64_t{19}}) {
        auto vec = log.read(start, 1 << 20);
        auto view = log.readView(start, 1 << 20);
        ASSERT_FALSE(vec.empty()) << "read() empty at start=" << start;
        ASSERT_FALSE(view.empty()) << "readView() empty at start=" << start;

        // First batch of read() and readView() must be byte-identical.
        RecordBatchReader rv(vec.data(), vec.size());
        ASSERT_TRUE(rv.ensureValid());
        RecordBatchReader rw(view.data, view.len);
        ASSERT_TRUE(rw.ensureValid());
        EXPECT_EQ(rw.baseOffset(), rv.baseOffset());
        EXPECT_EQ(view.baseOffset, rv.baseOffset());
        EXPECT_LE(view.baseOffset, start);
        EXPECT_GE(rv.baseOffset() + rv.lastOffsetDelta(), start);
        ASSERT_EQ(rw.totalLength(), rv.totalLength());
        EXPECT_EQ(0, std::memcmp(view.data, vec.data(),
                                 static_cast<size_t>(rv.totalLength())));

        // The whole view must parse into complete batches ending exactly at len,
        // and nextOffset must equal lastOffset(last batch)+1.
        size_t pos = 0; int64_t lastNext = view.baseOffset;
        while (pos < view.len) {
            RecordBatchReader r(view.data + pos, view.len - pos);
            ASSERT_TRUE(r.ensureValid()) << "batch at pos=" << pos << " invalid";
            int32_t total = r.totalLength();
            ASSERT_GT(total, 0);
            ASSERT_LE(pos + static_cast<size_t>(total), view.len)
                << "view ended mid-batch (not whole batches)";
            lastNext = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += static_cast<size_t>(total);
        }
        EXPECT_EQ(pos, view.len) << "view length must be a whole number of batches";
        EXPECT_EQ(view.nextOffset, lastNext);
        EXPECT_EQ(view.nextOffset, log.logEndOffset()); // read to end with huge maxBytes
    }
    std::filesystem::remove_all(dir);
}

// maxBytes must be honored (whole batches only) but must ALWAYS return >= 1 batch
// even when the single batch exceeds maxBytes (progress guarantee).
TEST(LogReadView, MaxBytesBoundedButAtLeastOneBatch) {
    auto dir = makeTempDir("rune_rv_max");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 64LL * 1024 * 1024);
    int64_t off = 0;
    for (int i = 0; i < 10; ++i) {
        RecordBatchBuilder b(off, 2000 + i);
        b.append("k", "v" + std::to_string(i), 2000 + i);
        off = log.append(b.build(), off);
    }

    // Size of a single batch (batch 0).
    auto one = log.readView(0, 1 << 20);
    ASSERT_FALSE(one.empty());
    RecordBatchReader r0(one.data, one.len);
    ASSERT_TRUE(r0.ensureValid());
    const int32_t oneBatch = r0.totalLength();

    // maxBytes smaller than one batch → still returns exactly one batch.
    auto tiny = log.readView(0, oneBatch - 1);
    ASSERT_FALSE(tiny.empty());
    EXPECT_EQ(tiny.len, static_cast<size_t>(oneBatch))
        << "must return at least one whole batch even below maxBytes";
    EXPECT_EQ(tiny.nextOffset, 1);

    // maxBytes just under two batches → exactly one batch (no partial second).
    auto capd = log.readView(0, oneBatch + (oneBatch / 2));
    EXPECT_EQ(capd.len, static_cast<size_t>(oneBatch));
    EXPECT_EQ(capd.nextOffset, 1);

    // maxBytes for exactly two batches → two batches.
    auto two = log.readView(0, oneBatch * 2);
    EXPECT_EQ(two.len, static_cast<size_t>(oneBatch) * 2);
    EXPECT_EQ(two.nextOffset, 2);

    std::filesystem::remove_all(dir);
}

// Multi-segment: readView at a later segment's base returns that segment's data;
// readView past the LEO returns an empty view.
TEST(LogReadView, MultiSegmentAndPastEndEmpty) {
    auto dir = makeTempDir("rune_rv_multi");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 256); // tiny → force rolls
    int64_t off = 0;
    for (int i = 0; i < 50; ++i) {
        RecordBatchBuilder b(off, 3000 + i);
        b.append("key", "val" + std::to_string(i), 3000 + i);
        off = log.append(b.build(), off);
    }
    auto bases = log.segmentBaseOffsets();
    ASSERT_GT(bases.size(), 1u);

    // read at the exact base of the second segment.
    int64_t secondBase = bases[1];
    auto v = log.readView(secondBase, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.baseOffset, secondBase);
    RecordBatchReader r(v.data, v.len);
    ASSERT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), secondBase);

    // read in the middle of a segment.
    int64_t mid = secondBase + 1;
    if (mid < log.logEndOffset()) {
        auto vm = log.readView(mid, 1 << 20);
        ASSERT_FALSE(vm.empty());
        EXPECT_LE(vm.baseOffset, mid);
        RecordBatchReader rm(vm.data, vm.len);
        ASSERT_TRUE(rm.ensureValid());
        EXPECT_GE(rm.baseOffset() + rm.lastOffsetDelta(), mid);
    }

    // past the end → empty.
    EXPECT_TRUE(log.readView(log.logEndOffset(), 1 << 20).empty());
    EXPECT_TRUE(log.readView(log.logEndOffset() + 100, 1 << 20).empty());

    std::filesystem::remove_all(dir);
}

// The active segment mmap must grow-remap so a readView issued after more appends
// observes the newly-committed bytes (no stale short mapping).
TEST(LogReadView, ActiveSegmentGrowRemapSeesNewData) {
    auto dir = makeTempDir("rune_rv_grow");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 64LL * 1024 * 1024); // single segment, no roll
    int64_t off = 0;
    for (int i = 0; i < 5; ++i) {
        RecordBatchBuilder b(off, 4000 + i);
        b.append("k", "v" + std::to_string(i), 4000 + i);
        off = log.append(b.build(), off);
    }
    auto v1 = log.readView(0, 1 << 20);   // creates the read mmap at current size
    ASSERT_FALSE(v1.empty());
    EXPECT_EQ(v1.nextOffset, 5);

    // Append more, then read from where we left off — remap must expose it.
    for (int i = 5; i < 12; ++i) {
        RecordBatchBuilder b(off, 4000 + i);
        b.append("k", "v" + std::to_string(i), 4000 + i);
        off = log.append(b.build(), off);
    }
    auto v2 = log.readView(v1.nextOffset, 1 << 20);
    ASSERT_FALSE(v2.empty()) << "grow-remap must expose newly-appended batches";
    EXPECT_EQ(v2.baseOffset, 5);
    EXPECT_EQ(v2.nextOffset, 12);

    std::filesystem::remove_all(dir);
}

// Zero-copy: for a SEALED segment (fixed size) two readView calls at the same
// offset must return the SAME pointer — the span aliases the persistent mmap,
// not a freshly-allocated buffer.
TEST(LogReadView, SealedSegmentSpanIsStableAlias) {
    auto dir = makeTempDir("rune_rv_alias");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), 256); // roll so early segments seal
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) {
        RecordBatchBuilder b(off, 5000 + i);
        b.append("key", "val" + std::to_string(i), 5000 + i);
        off = log.append(b.build(), off);
    }
    auto bases = log.segmentBaseOffsets();
    ASSERT_GT(bases.size(), 1u);
    int64_t firstBase = bases.front(); // sealed once a roll happened

    auto a = log.readView(firstBase, 1 << 20);
    auto b = log.readView(firstBase, 1 << 20);
    ASSERT_FALSE(a.empty());
    ASSERT_FALSE(b.empty());
    EXPECT_EQ(a.data, b.data) << "sealed-segment readView must alias a stable mmap";
    EXPECT_EQ(a.len, b.len);

    std::filesystem::remove_all(dir);
}
