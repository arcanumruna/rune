#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Crc32c.h>
#include <functional>
#include <string>
#include <vector>
#include <cstdint>
#include <climits>
namespace b = rune::protocol::batch;

static uint32_t beU32(const std::vector<uint8_t>& b, int off) {
    return (b[off]<<24)|(b[off+1]<<16)|(b[off+2]<<8)|b[off+3];
}

static int64_t beS64(const std::vector<uint8_t>& buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | buf[off + i];
    return static_cast<int64_t>(v);
}

static int32_t beS32(const std::vector<uint8_t>& buf, int off) {
    return static_cast<int32_t>(beU32(buf, off));
}

// Helper: read the partitionLeaderEpoch field from raw bytes.
static int32_t readEpoch(const std::vector<uint8_t>& buf) {
    return beS32(buf, b::kPartitionLeaderEpochOffset);
}

// Helper: read maxTimestamp field.
static int64_t readMaxTimestamp(const std::vector<uint8_t>& buf) {
    return beS64(buf, b::kMaxTimestampOffset);
}

TEST(RecordBatchLayout, FieldOffsetsMatchKafkaV2) {
    EXPECT_EQ(b::kBaseOffsetOffset, 0);
    EXPECT_EQ(b::kLengthOffset, 8);
    EXPECT_EQ(b::kPartitionLeaderEpochOffset, 12);
    EXPECT_EQ(b::kMagicOffset, 16);
    EXPECT_EQ(b::kCrcOffset, 17);
    EXPECT_EQ(b::kAttributesOffset, 21);
    EXPECT_EQ(b::kLastOffsetDeltaOffset, 23);
    EXPECT_EQ(b::kBaseTimestampOffset, 27);
    EXPECT_EQ(b::kMaxTimestampOffset, 35);
    EXPECT_EQ(b::kProducerIdOffset, 43);
    EXPECT_EQ(b::kProducerEpochOffset, 51);
    EXPECT_EQ(b::kBaseSequenceOffset, 53);
    EXPECT_EQ(b::kRecordCountOffset, 57);
    EXPECT_EQ(b::kRecordBatchOverhead, 61);
    EXPECT_EQ(b::kMagicValue, 2);
}

TEST(RecordBatchBuilder, EncodesHeaderAndCrc) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(100 /*baseOffset*/, 5000 /*baseTs*/);
    rb.append("k1", "v1", 5000);
    rb.append("k2", "v2", 5001);
    auto bytes = rb.build();

    EXPECT_EQ(bytes[batch::kMagicOffset], batch::kMagicValue);
    EXPECT_EQ(beU32(bytes, batch::kRecordCountOffset), 2u);
    EXPECT_EQ(beU32(bytes, batch::kLastOffsetDeltaOffset), 1u); // 2 records -> delta 1
    // LENGTH = total - (baseOffset(8) + lengthField(4))
    EXPECT_EQ(beU32(bytes, batch::kLengthOffset), bytes.size() - 12);
    // CRC covers [attributes .. end]
    uint32_t expect = crc32c(bytes.data() + batch::kAttributesOffset,
                             bytes.size() - batch::kAttributesOffset);
    EXPECT_EQ(beU32(bytes, batch::kCrcOffset), expect);
}

TEST(RecordBatchReader, RoundTripAndValidation) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(100, 5000);
    rb.append("k1", "v1", 5000);
    rb.append("k2", "v2", 5001);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 100);
    EXPECT_EQ(r.recordCount(), 2);
    EXPECT_EQ(r.totalLength(), (int)bytes.size());

    std::vector<std::pair<std::string,int64_t>> seen;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t){
        seen.emplace_back(std::string(v), off);
    });
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0].first, "v1"); EXPECT_EQ(seen[0].second, 100);
    EXPECT_EQ(seen[1].first, "v2"); EXPECT_EQ(seen[1].second, 101);

    // Stamping the (CRC-excluded) leader epoch must NOT invalidate the batch.
    RecordBatchReader::stampLeaderEpoch(bytes.data(), 7);
    RecordBatchReader r2(bytes.data(), bytes.size());
    EXPECT_TRUE(r2.ensureValid());

    // Corrupt a value byte -> ensureValid() fails.
    bytes.back() ^= 0xFF;
    EXPECT_FALSE(RecordBatchReader(bytes.data(), bytes.size()).ensureValid());
}

TEST(RecordBatchReader, TruncatedBatchIsInvalid) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("k", "value", 1000);
    auto bytes = rb.build();
    // Claim the full length but only expose fewer bytes than totalLength():
    RecordBatchReader r(bytes.data(), bytes.size() - 3);
    EXPECT_FALSE(r.ensureValid());
}

// ===========================================================================
// Additional edge-case tests
// ===========================================================================

// --- Single record ---

TEST(RecordBatchEdge, SingleRecord) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("key", "val", 1000);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.recordCount(), 1);
    EXPECT_EQ(r.lastOffsetDelta(), 0);  // count-1 = 0
    EXPECT_EQ(r.baseOffset(), 0);

    int called = 0;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
        EXPECT_EQ(std::string(k), "key");
        EXPECT_EQ(std::string(v), "val");
        EXPECT_EQ(off, 0);
        EXPECT_EQ(ts, 1000);
        ++called;
    });
    EXPECT_EQ(called, 1);
}

// --- Many records (1000) ---

TEST(RecordBatchEdge, ThousandRecords) {
    using namespace rune::protocol;
    constexpr int N = 1000;
    constexpr int64_t kBase = 500;
    constexpr int64_t kBaseTs = 2000000LL;

    RecordBatchBuilder rb(kBase, kBaseTs);
    for (int i = 0; i < N; ++i)
        rb.append("k" + std::to_string(i), "v" + std::to_string(i), kBaseTs + i);

    auto bytes = rb.build();
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.recordCount(), N);
    EXPECT_EQ(r.lastOffsetDelta(), N - 1);
    EXPECT_EQ(r.baseOffset(), kBase);

    int idx = 0;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
        EXPECT_EQ(off, kBase + idx) << "idx=" << idx;
        EXPECT_EQ(ts, kBaseTs + idx) << "idx=" << idx;
        EXPECT_EQ(std::string(k), "k" + std::to_string(idx));
        EXPECT_EQ(std::string(v), "v" + std::to_string(idx));
        ++idx;
    });
    EXPECT_EQ(idx, N);
}

// --- Empty key, empty value, both empty ---

TEST(RecordBatchEdge, EmptyKey) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 100);
    rb.append("", "hello", 100);
    auto bytes = rb.build();
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) {
        EXPECT_EQ(k.size(), 0u);
        EXPECT_EQ(std::string(v), "hello");
    });
}

TEST(RecordBatchEdge, EmptyValue) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 100);
    rb.append("mykey", "", 100);
    auto bytes = rb.build();
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) {
        EXPECT_EQ(std::string(k), "mykey");
        EXPECT_EQ(v.size(), 0u);
    });
}

TEST(RecordBatchEdge, BothKeyAndValueEmpty) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 100);
    rb.append("", "", 100);
    auto bytes = rb.build();
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    int called = 0;
    r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) {
        EXPECT_EQ(k.size(), 0u);
        EXPECT_EQ(v.size(), 0u);
        ++called;
    });
    EXPECT_EQ(called, 1);
}

// --- Binary key/value with embedded NUL bytes and 0xFF bytes ---

TEST(RecordBatchEdge, BinaryKeyValueWithNulAndHighBytes) {
    using namespace rune::protocol;
    // Build a key/value that contains embedded NUL bytes and 0xFF bytes.
    std::string binKey(8, '\0');
    binKey[0] = '\xDE'; binKey[2] = '\xFF'; binKey[4] = '\x00'; binKey[7] = '\xAB';

    std::string binVal(12, '\xFF');
    binVal[3] = '\x00'; binVal[6] = '\x01'; binVal[11] = '\x00';

    RecordBatchBuilder rb(0, 999);
    rb.append(binKey, binVal, 999);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());

    r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) {
        ASSERT_EQ(k.size(), binKey.size());
        ASSERT_EQ(v.size(), binVal.size());
        EXPECT_EQ(std::string(k), binKey);
        EXPECT_EQ(std::string(v), binVal);
    });
}

// --- Large value (100 KB) round-trips ---

TEST(RecordBatchEdge, LargeValueRoundTrip) {
    using namespace rune::protocol;
    constexpr size_t kValSz = 100 * 1024; // 100 KB
    std::string largeVal(kValSz, 'X');
    // Fill with a deterministic pattern.
    for (size_t i = 0; i < kValSz; ++i)
        largeVal[i] = static_cast<char>((i * 0x37 + 0xA5) & 0xFF);

    RecordBatchBuilder rb(0, 1);
    rb.append("bigkey", largeVal, 1);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());

    r.forEach([&](std::string_view k, std::string_view v, int64_t, int64_t) {
        EXPECT_EQ(std::string(k), "bigkey");
        ASSERT_EQ(v.size(), kValSz);
        EXPECT_EQ(std::string(v), largeVal);
    });
}

// --- Timestamps: increasing ---

TEST(RecordBatchEdge, IncreasingTimestamps) {
    using namespace rune::protocol;
    constexpr int64_t kBase = 10000LL;
    RecordBatchBuilder rb(0, kBase);
    rb.append("a", "1", kBase + 0);
    rb.append("b", "2", kBase + 5);
    rb.append("c", "3", kBase + 99);
    auto bytes = rb.build();

    // maxTimestamp should be kBase+99.
    EXPECT_EQ(readMaxTimestamp(bytes), kBase + 99);

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseTimestamp(), kBase);

    std::vector<int64_t> ts_seen;
    r.forEach([&](std::string_view, std::string_view, int64_t, int64_t ts) {
        ts_seen.push_back(ts);
    });
    ASSERT_EQ(ts_seen.size(), 3u);
    EXPECT_EQ(ts_seen[0], kBase + 0);
    EXPECT_EQ(ts_seen[1], kBase + 5);
    EXPECT_EQ(ts_seen[2], kBase + 99);
}

TEST(RecordBatchEdge, AllEqualTimestamps) {
    using namespace rune::protocol;
    constexpr int64_t kTs = 777777LL;
    RecordBatchBuilder rb(0, kTs);
    rb.append("a", "1", kTs);
    rb.append("b", "2", kTs);
    rb.append("c", "3", kTs);
    auto bytes = rb.build();

    EXPECT_EQ(readMaxTimestamp(bytes), kTs);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseTimestamp(), kTs);

    int idx = 0;
    r.forEach([&](std::string_view, std::string_view, int64_t, int64_t ts) {
        EXPECT_EQ(ts, kTs) << "record " << idx;
        ++idx;
    });
    EXPECT_EQ(idx, 3);
}

// --- Non-zero baseOffset ---

TEST(RecordBatchEdge, NonZeroBaseOffset) {
    using namespace rune::protocol;
    constexpr int64_t kBaseOff = 1000000LL;
    RecordBatchBuilder rb(kBaseOff, 5000);
    rb.append("a", "1", 5000);
    rb.append("b", "2", 5001);
    rb.append("c", "3", 5002);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), kBaseOff);

    int idx = 0;
    r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
        EXPECT_EQ(off, kBaseOff + idx) << "idx=" << idx;
        ++idx;
    });
    EXPECT_EQ(idx, 3);
}

// --- ensureValid: well-formed is true ---

TEST(RecordBatchEdge, EnsureValidTrueForWellFormed) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    auto bytes = rb.build();
    EXPECT_TRUE(RecordBatchReader(bytes.data(), bytes.size()).ensureValid());
}

// --- ensureValid: CRC corruption at several positions ---

TEST(RecordBatchEdge, EnsureValidFalseWhenCrcRegionCorrupted) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("key0", "value0", 1000);
    rb.append("key1", "value1", 1001);
    auto bytes = rb.build();

    // Positions in the CRC-covered region [kAttributesOffset .. end).
    // Corrupt each one independently.
    std::vector<size_t> positions = {
        (size_t)b::kAttributesOffset,         // first byte of CRC region
        (size_t)b::kAttributesOffset + 1,
        (size_t)b::kLastOffsetDeltaOffset,
        (size_t)b::kBaseTimestampOffset,
        (size_t)b::kRecordCountOffset,
        bytes.size() - 1,                     // last byte
        bytes.size() - 2,
    };

    for (size_t pos : positions) {
        auto corrupt = bytes;
        corrupt[pos] ^= 0xFF;
        EXPECT_FALSE(RecordBatchReader(corrupt.data(), corrupt.size()).ensureValid())
            << "corrupted at pos=" << pos;
    }
}

TEST(RecordBatchEdge, EnsureValidFalseForFirstRecordByteCorrpuption) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 100);
    rb.append("k", "v", 100);
    auto bytes = rb.build();
    // The first record byte is at kRecordBatchOverhead.
    auto corrupt = bytes;
    corrupt[b::kRecordBatchOverhead] ^= 0x01;
    EXPECT_FALSE(RecordBatchReader(corrupt.data(), corrupt.size()).ensureValid());
}

// --- ensureValid: buffer shorter than kRecordBatchOverhead ---

TEST(RecordBatchEdge, EnsureValidFalseForTooShortBuffer) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    auto bytes = rb.build();

    // Expose exactly kRecordBatchOverhead-1 bytes.
    RecordBatchReader r(bytes.data(), b::kRecordBatchOverhead - 1);
    EXPECT_FALSE(r.ensureValid());

    // Also test zero bytes.
    RecordBatchReader r0(bytes.data(), 0);
    EXPECT_FALSE(r0.ensureValid());
}

// --- ensureValid: declared totalLength exceeds provided bytes (truncation) ---

TEST(RecordBatchEdge, EnsureValidFalseForTruncationAtMultipleLengths) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("keyA", "valueA", 1000);
    rb.append("keyB", "valueB", 1001);
    auto bytes = rb.build();

    size_t full = bytes.size();
    // Truncate at several positions (not the last byte which is already tested).
    for (size_t exposed : {full - 1, full - 2, full - 5, full - 10, (size_t)b::kRecordBatchOverhead}) {
        if (exposed == 0) continue;
        RecordBatchReader r(bytes.data(), exposed);
        EXPECT_FALSE(r.ensureValid()) << "exposed=" << exposed;
    }
}

// --- stampLeaderEpoch: various epoch values stay valid ---

TEST(RecordBatchEdge, StampLeaderEpochKeepsValid) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 1000);
    rb.append("k", "v", 1000);
    auto bytes = rb.build();

    struct EpochCase { int32_t epoch; const char* label; };
    EpochCase cases[] = {
        {0,          "zero"},
        {1,          "one"},
        {7,          "seven"},
        {INT32_MAX,  "INT32_MAX"},
        {-1,         "minus one (0xFFFFFFFF)"},
    };
    for (auto& c : cases) {
        auto copy = bytes;
        RecordBatchReader::stampLeaderEpoch(copy.data(), c.epoch);
        EXPECT_TRUE(RecordBatchReader(copy.data(), copy.size()).ensureValid())
            << "epoch=" << c.label;
        EXPECT_EQ(readEpoch(copy), c.epoch) << "epoch=" << c.label;
    }
}

// --- Back-to-back batches in one buffer ---

TEST(RecordBatchEdge, BackToBackBatchesInOneBuffer) {
    using namespace rune::protocol;

    RecordBatchBuilder rb1(0, 1000);
    rb1.append("k0", "v0", 1000);
    rb1.append("k1", "v1", 1001);
    auto b1 = rb1.build();

    RecordBatchBuilder rb2(2, 2000);
    rb2.append("k2", "v2", 2000);
    rb2.append("k3", "v3", 2001);
    rb2.append("k4", "v4", 2002);
    auto b2 = rb2.build();

    // Concatenate into one contiguous buffer.
    std::vector<uint8_t> combined;
    combined.insert(combined.end(), b1.begin(), b1.end());
    combined.insert(combined.end(), b2.begin(), b2.end());

    const uint8_t* ptr = combined.data();
    size_t remaining = combined.size();

    // Walk batch 1.
    {
        RecordBatchReader r1(ptr, remaining);
        EXPECT_TRUE(r1.ensureValid());
        EXPECT_EQ(r1.baseOffset(), 0);
        EXPECT_EQ(r1.recordCount(), 2);
        size_t step = (size_t)r1.totalLength();
        ptr += step; remaining -= step;
    }

    // Walk batch 2.
    {
        RecordBatchReader r2(ptr, remaining);
        EXPECT_TRUE(r2.ensureValid());
        EXPECT_EQ(r2.baseOffset(), 2);
        EXPECT_EQ(r2.recordCount(), 3);
        EXPECT_EQ((size_t)r2.totalLength(), b2.size());

        int idx = 0;
        r2.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
            EXPECT_EQ(off, 2 + idx);
            EXPECT_EQ(ts, 2000 + idx);
            // Check key and value match what was written to batch 2
            EXPECT_EQ(std::string(k), "k" + std::to_string(idx + 2));
            EXPECT_EQ(std::string(v), "v" + std::to_string(idx + 2));
            ++idx;
        });
        EXPECT_EQ(idx, 3);
    }
}

// --- totalLength accuracy ---

TEST(RecordBatchEdge, TotalLengthMatchesBuildSize) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(42, 9999);
    rb.append("hello", "world", 9999);
    rb.append("foo", "bar", 10000);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_EQ((size_t)r.totalLength(), bytes.size());
}

// --- magic byte is set correctly ---

TEST(RecordBatchEdge, MagicByteIsTwo) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(0, 0);
    rb.append("x", "y", 0);
    auto bytes = rb.build();
    EXPECT_EQ(bytes[b::kMagicOffset], static_cast<uint8_t>(b::kMagicValue));
    EXPECT_EQ(b::kMagicValue, 2);
}

// --- multiple records: lastOffsetDelta == recordCount - 1 ---

TEST(RecordBatchEdge, LastOffsetDeltaIsCountMinusOne) {
    using namespace rune::protocol;
    for (int n : {1, 2, 3, 5, 10, 50}) {
        RecordBatchBuilder rb(0, 0);
        for (int i = 0; i < n; ++i)
            rb.append("k", "v", i);
        auto bytes = rb.build();
        RecordBatchReader r(bytes.data(), bytes.size());
        EXPECT_EQ(r.lastOffsetDelta(), n - 1) << "n=" << n;
        EXPECT_EQ(r.recordCount(), n) << "n=" << n;
    }
}
