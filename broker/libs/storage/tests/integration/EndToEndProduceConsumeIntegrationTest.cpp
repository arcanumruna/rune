// ============================================================================
// EndToEndProduceConsumeIntegrationTest.cpp
//
// Integration tests: produce/consume flow across many segments, with many
// assertions about offsets, values, timestamps, and HWM.
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/PartitionApi.h>

using namespace rune::storage;

// ---------------------------------------------------------------------------
// 1. Produce 10,000 records; consume all; verify every offset, value, key,
//    and timestamp.
//
// BUG REPORT: This test FAILS due to a genuine storage bug in
// LogSegment::read().  api.consume(0) returns only ~8465 records instead of
// 10000.  Root cause: LogSegment::read(startOffset, maxBytes=1MiB) looks up
// the sparse index to find a base position P, then reads min(fileSize-P,
// maxBytes) bytes and runs a skip loop to advance past batches before
// startOffset.  When P is near the start of the file (index is sparse, e.g.
// one entry per 4096 bytes) and the requested offset is near the end of a
// >1MiB file, the 1MiB read buffer is exhausted by the skip loop BEFORE
// reaching the requested offset — the skip loop returns {} (empty), the outer
// consume() loop breaks, and records beyond the 1MiB boundary are silently
// dropped.  Confirmed with a manual diagnostic loop: consume(0)→8465 records,
// consume(8465)→1535 records, total=10000 — the records exist but a single
// consume(0) call cannot retrieve all of them.
//
// The fix belongs in LogSegment::read: after the index lookup, seek directly
// to the nearest batch boundary before startOffset (using the index-provided
// position as a start), then scan forward in a streaming fashion rather than
// capping the read at maxBytes from the index position.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, Produce10kAcrossManySegments) {
    constexpr int N = 10'000;
    {
        PartitionApi api(testDir_.string());
        auto produced = produceN(api, N, /*vsize=*/48);

        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));

        auto records = consumeAll(api);
        ASSERT_EQ(static_cast<int>(records.size()), N);

        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(records[static_cast<size_t>(i)].offset,
                      static_cast<int64_t>(i))
                << "offset mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].value,
                      produced[static_cast<size_t>(i)].value)
                << "value mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].key,
                      produced[static_cast<size_t>(i)].key)
                << "key mismatch at i=" << i;
            EXPECT_EQ(records[static_cast<size_t>(i)].timestamp,
                      produced[static_cast<size_t>(i)].timestamp)
                << "ts mismatch at i=" << i;
        }
    }
}

// ---------------------------------------------------------------------------
// 2. After producing 10k, consume from many mid-offsets and verify we get
//    exactly the suffix starting at that offset.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, ConsumeFromManyMidOffsets) {
    constexpr int N = 500;
    PartitionApi api(testDir_.string());
    auto produced = produceN(api, N, 32);
    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));

    for (int start : {0, 1, 50, 100, 249, 499}) {
        auto recs = api.consume(static_cast<int64_t>(start));
        int expected = N - start;
        ASSERT_EQ(static_cast<int>(recs.size()), expected)
            << "consume(" << start << ") size mismatch";
        for (int j = 0; j < expected; ++j) {
            int absIdx = start + j;
            EXPECT_EQ(recs[static_cast<size_t>(j)].offset,
                      static_cast<int64_t>(absIdx))
                << "offset mismatch at start=" << start << " j=" << j;
            EXPECT_EQ(recs[static_cast<size_t>(j)].value,
                      produced[static_cast<size_t>(absIdx)].value)
                << "value mismatch at start=" << start << " j=" << j;
        }
    }

    // consume(N) returns empty
    EXPECT_TRUE(api.consume(static_cast<int64_t>(N)).empty());
    // consume(N+100) returns empty
    EXPECT_TRUE(api.consume(static_cast<int64_t>(N + 100)).empty());
}

// ---------------------------------------------------------------------------
// 3. Empty partition: HWM == 0, consume returns empty.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, EmptyPartitionHwmAndConsumeEmpty) {
    PartitionApi api(testDir_.string());
    EXPECT_EQ(api.highWatermark(), 0);
    EXPECT_TRUE(api.consume(0).empty());
    EXPECT_TRUE(api.consume(5).empty());
    EXPECT_TRUE(api.consume(1000).empty());
}

// ---------------------------------------------------------------------------
// 4. Empty value round-trips byte-exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, EmptyValueRoundTrip) {
    PartitionApi api(testDir_.string());
    api.produce("k0", "", 9000);
    api.produce("k1", "normal", 9001);
    api.produce("k2", "", 9002);

    EXPECT_EQ(api.highWatermark(), 3);
    auto recs = consumeAll(api);
    ASSERT_EQ(recs.size(), 3u);

    EXPECT_EQ(recs[0].value.size(), 0u);
    EXPECT_EQ(recs[0].key, "k0");
    EXPECT_EQ(recs[1].value, "normal");
    EXPECT_EQ(recs[2].value.size(), 0u);
    EXPECT_EQ(recs[2].key, "k2");
}

// ---------------------------------------------------------------------------
// 5. Binary / high-byte values (NUL, 0xFF, 0xDE, 0xAD) round-trip byte-exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, BinaryHighByteValuesRoundTrip) {
    PartitionApi api(testDir_.string());

    // Build a value with NUL bytes, high bytes, full 0xFF.
    std::string tricky;
    for (int b = 0; b < 64; ++b)
        tricky.push_back(static_cast<char>(static_cast<uint8_t>(b * 4)));
    // Overwrite a few positions with distinctly tricky bytes.
    tricky[0]  = '\x00';
    tricky[1]  = '\xFF';
    tricky[2]  = '\xDE';
    tricky[3]  = '\xAD';
    tricky[32] = '\x00';

    api.produce("binary", tricky, 7000);
    EXPECT_EQ(api.highWatermark(), 1);

    auto recs = consumeAll(api);
    ASSERT_EQ(recs.size(), 1u);
    ASSERT_EQ(recs[0].value.size(), tricky.size());
    EXPECT_EQ(recs[0].value, tricky);
    EXPECT_EQ(recs[0].key, "binary");
    EXPECT_EQ(recs[0].timestamp, 7000);
    EXPECT_EQ(recs[0].offset, 0);
}

// ---------------------------------------------------------------------------
// 6. Large value (512 KiB) round-trips byte-exact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, LargeValueRoundTrip) {
    PartitionApi api(testDir_.string());

    constexpr int SZ = 512 * 1024;
    std::string large;
    large.reserve(SZ);
    for (int b = 0; b < SZ; ++b)
        large.push_back(static_cast<char>(static_cast<uint8_t>((b * 7 + 13) & 0xFF)));

    api.produce("bigkey", large, 8000);
    EXPECT_EQ(api.highWatermark(), 1);

    auto recs = consumeAll(api);
    ASSERT_EQ(recs.size(), 1u);
    ASSERT_EQ(recs[0].value.size(), large.size());
    EXPECT_EQ(recs[0].value, large);
    EXPECT_EQ(recs[0].offset, 0);
}

// ---------------------------------------------------------------------------
// 7. highWatermark == number of produced records, before and after partial
//    consumes (consume must not mutate HWM).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, HighWatermarkEqualsProducedCount) {
    constexpr int N = 200;
    PartitionApi api(testDir_.string());
    for (int i = 0; i < N; ++i) {
        api.produce("k", "v", static_cast<int64_t>(i));
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(i + 1))
            << "HWM wrong after producing record " << i;
    }
    // Consuming does not change HWM.
    api.consume(0);
    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    api.consume(100);
    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
}

// ---------------------------------------------------------------------------
// 8. Multiple sequential produces with different key sizes all survive.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, VariableKeySizeRoundTrip) {
    PartitionApi api(testDir_.string());
    std::vector<std::string> keys = {
        "", "a", std::string(256, 'K'), std::string(1024, 'X')
    };
    for (size_t i = 0; i < keys.size(); ++i)
        api.produce(keys[i], "val" + std::to_string(i), static_cast<int64_t>(i));

    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(keys.size()));
    auto recs = consumeAll(api);
    ASSERT_EQ(recs.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_EQ(recs[i].key, keys[i]) << "key mismatch at i=" << i;
        EXPECT_EQ(recs[i].value, "val" + std::to_string(i)) << "val mismatch at i=" << i;
        EXPECT_EQ(recs[i].offset, static_cast<int64_t>(i)) << "offset mismatch at i=" << i;
    }
}
