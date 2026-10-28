#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Crc32c.h>
#include <cstdlib>
#include <memory>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <climits>
#include <cstring>
namespace b = rune::protocol::batch;

// Little-endian field readers (all fixed header fields are LE after the BE→LE unification).
static uint32_t leU32(const std::vector<uint8_t>& buf, int off) {
    return uint32_t(buf[off]) | (uint32_t(buf[off+1]) << 8)
         | (uint32_t(buf[off+2]) << 16) | (uint32_t(buf[off+3]) << 24);
}

static int64_t leS64(const std::vector<uint8_t>& buf, int off) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(buf[off + i]) << (i * 8);
    return static_cast<int64_t>(v);
}

static int32_t leS32(const std::vector<uint8_t>& buf, int off) {
    return static_cast<int32_t>(leU32(buf, off));
}

// Helper: read the partitionLeaderEpoch field from raw bytes (LE int32).
static int32_t readEpoch(const std::vector<uint8_t>& buf) {
    return leS32(buf, b::kPartitionLeaderEpochOffset);
}

// Helper: read maxTimestamp field (LE int64).
static int64_t readMaxTimestamp(const std::vector<uint8_t>& buf) {
    return leS64(buf, b::kMaxTimestampOffset);
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
    EXPECT_EQ(leU32(bytes, batch::kRecordCountOffset), 2u);
    EXPECT_EQ(leU32(bytes, batch::kLastOffsetDeltaOffset), 1u); // 2 records -> delta 1
    // LENGTH = total - (baseOffset(8) + lengthField(4))
    EXPECT_EQ(leU32(bytes, batch::kLengthOffset), bytes.size() - 12);
    // CRC covers [attributes .. end]
    uint32_t expect = crc32c(bytes.data() + batch::kAttributesOffset,
                             bytes.size() - batch::kAttributesOffset);
    EXPECT_EQ(leU32(bytes, batch::kCrcOffset), expect);
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

// ===========================================================================
// CP-8P: delete-horizon (KIP-534) tests
// ===========================================================================

// --- Attribute bit constant value ---

TEST(DeleteHorizon, AttributeBitValue) {
    // kAttrDeleteHorizon must be 0x0040 (bit 6) — next free after Control(0x0020).
    EXPECT_EQ(b::kAttrDeleteHorizon, static_cast<int16_t>(0x0040));
    // Must not collide with existing bits.
    EXPECT_EQ(b::kAttrDeleteHorizon & b::kAttrControl,       0);
    EXPECT_EQ(b::kAttrDeleteHorizon & b::kAttrTransactional, 0);
}

// --- Round-trip: set deleteHorizonMs → encode → decode → same value + bit ---

TEST(DeleteHorizon, RoundTrip) {
    using namespace rune::protocol;
    constexpr int64_t kHorizon = 1000000000LL + 12345LL; // 1000012345 ms

    RecordBatchBuilder rb(10, 5000);
    rb.setDeleteHorizonMs(kHorizon);
    rb.append("k", "v", 5000);
    auto bytes = rb.build();

    // Attribute bit must be set in the encoded batch.
    const int16_t attrs = static_cast<int16_t>(
        uint16_t(bytes[b::kAttributesOffset]) |
        (uint16_t(bytes[b::kAttributesOffset + 1]) << 8));
    EXPECT_NE(attrs & b::kAttrDeleteHorizon, 0) << "kAttrDeleteHorizon bit not set";

    // CRC must still be valid.
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());

    // Reader must report the bit and return the stored horizon value.
    EXPECT_TRUE(r.hasDeleteHorizon());
    EXPECT_EQ(r.deleteHorizonMs(), kHorizon);
}

// --- When the bit is clear, deleteHorizonMs() returns -1 (not set) ---

TEST(DeleteHorizon, NotSetWhenBitClear) {
    using namespace rune::protocol;

    RecordBatchBuilder rb(0, 8000);
    rb.append("key", "val", 8000);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_FALSE(r.hasDeleteHorizon());
    EXPECT_EQ(r.deleteHorizonMs(), -1LL);

    // The baseTimestamp field is unchanged (not overwritten with a horizon).
    EXPECT_EQ(r.baseTimestamp(), 8000LL);
}

// --- When the bit is set, baseTimestamp() returns the horizon value ---
//     (same field, horizon IS the baseTimestamp when bit is set)

TEST(DeleteHorizon, BaseTimestampCarriesHorizonWhenBitSet) {
    using namespace rune::protocol;
    constexpr int64_t kHorizon = 999888777LL;

    RecordBatchBuilder rb(0, 1000);
    rb.setDeleteHorizonMs(kHorizon);
    rb.append("x", "y", 1000);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    // baseTimestamp field holds the horizon when bit is set.
    EXPECT_EQ(r.baseTimestamp(), kHorizon);
    EXPECT_EQ(r.deleteHorizonMs(), kHorizon);
}

// --- Backward-compat: plain batch (no horizon) decodes as before ---

TEST(DeleteHorizon, BackwardCompatPlainBatch) {
    using namespace rune::protocol;
    constexpr int64_t kBaseTs = 77777777LL; // no single-quote separators

    RecordBatchBuilder rb(42, kBaseTs);
    rb.append("a", "1", kBaseTs);
    rb.append("b", "2", kBaseTs + 10);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(),   42LL);
    EXPECT_EQ(r.recordCount(),  2);
    EXPECT_EQ(r.baseTimestamp(), kBaseTs);
    EXPECT_FALSE(r.hasDeleteHorizon());
    EXPECT_EQ(r.deleteHorizonMs(), -1LL);
    // All existing fields are unaffected.
    EXPECT_EQ(r.producerId(),     -1LL);
    EXPECT_EQ(r.producerEpoch(),  -1);
    EXPECT_EQ(r.baseSequence(),   -1);
}

// --- buildInto path also sets the bit and horizon correctly ---

TEST(DeleteHorizon, BuildIntoPath) {
    using namespace rune::protocol;
    constexpr int64_t kHorizon = 500500500LL;

    RecordBatchBuilder rb(0, 3000);
    rb.setDeleteHorizonMs(kHorizon);
    rb.append("p", "q", 3000);

    std::vector<uint8_t> out;
    rb.buildInto(out);

    RecordBatchReader r(out.data(), out.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_TRUE(r.hasDeleteHorizon());
    EXPECT_EQ(r.deleteHorizonMs(), kHorizon);
}

// --- Conformance golden vector: byte-exact golden for delete-horizon batch ---
//
// Batch parameters:
//   baseOffset=100, baseTimestamp=1_000_000_000 (but OVERWRITTEN by horizon)
//   deleteHorizonMs=1_700_000_000  (written into baseTimestamp field)
//   One record: key="dh", value="test", timestamp=1_700_000_001
//   producerId=-1, producerEpoch=-1, baseSequence=-1
//
// The golden vector file is written alongside this test; the bytes are
// computed deterministically by the encoder, so the test is self-consistent.

TEST(DeleteHorizon, ConformanceGoldenVector) {
    using namespace rune::protocol;
    constexpr int64_t kHorizon   = 1700000000LL;
    constexpr int64_t kRecordTs  = 1700000001LL;
    constexpr int64_t kBaseOff   = 100LL;

    // Build using the new accessor.
    RecordBatchBuilder rb(kBaseOff, 1000000000LL);
    rb.setDeleteHorizonMs(kHorizon);
    rb.append("dh", "test", kRecordTs);
    auto encoded = rb.build();

    // Validate the batch.
    RecordBatchReader r(encoded.data(), encoded.size());
    ASSERT_TRUE(r.ensureValid());
    ASSERT_TRUE(r.hasDeleteHorizon());
    ASSERT_EQ(r.deleteHorizonMs(), kHorizon);
    ASSERT_EQ(r.baseOffset(), kBaseOff);
    ASSERT_EQ(r.recordCount(), 1);

    // Verify the attribute bit is set in the raw bytes.
    const int16_t attrs = static_cast<int16_t>(
        uint16_t(encoded[b::kAttributesOffset]) |
        (uint16_t(encoded[b::kAttributesOffset + 1]) << 8));
    EXPECT_NE(attrs & b::kAttrDeleteHorizon, 0);

    // Verify the baseTimestamp field holds the horizon (LE int64 at offset 27).
    uint64_t rawTs = 0;
    for (int i = 0; i < 8; ++i)
        rawTs |= static_cast<uint64_t>(encoded[b::kBaseTimestampOffset + i]) << (i * 8);
    EXPECT_EQ(static_cast<int64_t>(rawTs), kHorizon);

    // Verify records decode correctly with the horizon batch.
    int recordsCalled = 0;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t /*ts*/) {
        EXPECT_EQ(std::string(k), "dh");
        EXPECT_EQ(std::string(v), "test");
        EXPECT_EQ(off, kBaseOff);
        ++recordsCalled;
    });
    EXPECT_EQ(recordsCalled, 1);

    // CP-10 Fix 4: read the committed golden vector and assert byte-exact equality.
    // The test must NOT regenerate the file on every run — that would silently accept
    // wire-format regressions.  Regeneration is only permitted when the env var
    // RUNE_REGEN_GOLDEN_VECTORS is set to "1" (explicit developer opt-in).
    const std::string goldenPath =
        std::string(RUNE_CONFORMANCE_DIR) + "/vectors/record_batch_delete_horizon.bin";

    const char* regenEnv = std::getenv("RUNE_REGEN_GOLDEN_VECTORS");
    const bool  doRegen  = regenEnv && std::string_view(regenEnv) == "1";

    if (doRegen) {
        // Developer opt-in: regenerate the committed file (use only after a
        // deliberate wire-format change with careful review).
        FILE* f = std::fopen(goldenPath.c_str(), "wb");
        ASSERT_NE(f, nullptr) << "Cannot create golden vector: " << goldenPath;
        const std::size_t written = std::fwrite(encoded.data(), 1, encoded.size(), f);
        std::fclose(f);
        ASSERT_EQ(written, encoded.size()) << "Short write to golden vector file";
        SUCCEED() << "RUNE_REGEN_GOLDEN_VECTORS=1: regenerated " << goldenPath;
    } else {
        // Normal CI path: read the committed file and assert byte-exact equality.
        FILE* fr = std::fopen(goldenPath.c_str(), "rb");
        ASSERT_NE(fr, nullptr)
            << "Cannot open committed golden vector: " << goldenPath
            << "  (To regenerate, set RUNE_REGEN_GOLDEN_VECTORS=1 and re-run.)";
        std::fseek(fr, 0, SEEK_END);
        const long sz = std::ftell(fr);
        std::fseek(fr, 0, SEEK_SET);
        ASSERT_GT(sz, 0) << "Golden vector file is empty: " << goldenPath;
        std::vector<uint8_t> committedBytes(static_cast<std::size_t>(sz));
        const std::size_t got = std::fread(committedBytes.data(), 1, committedBytes.size(), fr);
        std::fclose(fr);
        ASSERT_EQ(got, committedBytes.size()) << "Short read from golden vector file";
        EXPECT_EQ(encoded, committedBytes)
            << "Encoded bytes differ from committed golden vector " << goldenPath
            << ".  Wire-format regression detected!  "
            << "If this is intentional, set RUNE_REGEN_GOLDEN_VECTORS=1 and re-run to regenerate.";
    }
}

// ===========================================================================
// CP-8 Fix 2: forEach horizon-timestamp gating
//
// When hasDeleteHorizon() is true, baseTimestamp() holds the delete HORIZON
// (epoch-ms), not the real batch base timestamp.  forEach must NOT derive
// per-record timestamps from baseTimestamp() on a horizon-stamped batch —
// that would yield horizon-epoch values instead of real record timestamps.
//
// KIP-534 faithful behavior: the real base timestamp was overwritten by the
// horizon so per-record wall-clock timestamps are not recoverable via the
// tsDelta field.  forEach must expose maxTimestamp() as the record timestamp
// for every record in a horizon-stamped batch.
//
// NON-VACUOUS: with the OLD code (baseTs + tsDelta always), the ts returned
// by forEach on a horizon batch is:
//   horizonMs + tsDelta  (e.g. 99999 + 0 = 99999 for the first record)
// which equals horizonMs — NOT recordTs.  The test below asserts ts == recordTs
// and ts != horizonMs, which FAILS against the unpatched code.
// ===========================================================================

// CP-8: forEach on a horizon-stamped batch must NOT yield the horizon epoch
// as a record timestamp.  It must yield maxTimestamp() instead.
// Non-vacuous: removing the horizon gate in forEach makes the ts equal to
// horizonMs (the old code path), causing all three EXPECT_* below to fail.
TEST(DeleteHorizon, ForEachDoesNotYieldHorizonAsTimestamp) {
    using namespace rune::protocol;

    // Construct a horizon-stamped batch with a CLEARLY DISTINCT horizon vs record ts.
    constexpr int64_t kHorizonMs  = 99999LL;   // delete horizon (far future epoch-ms)
    constexpr int64_t kRecordTs   = 12345LL;   // actual record wall-clock timestamp
    constexpr int64_t kBaseOffset = 42LL;

    RecordBatchBuilder b(kBaseOffset, kRecordTs);
    b.setDeleteHorizonMs(kHorizonMs);
    b.append("key", "val", kRecordTs);
    auto batch = b.build();

    RecordBatchReader r(batch.data(), batch.size());
    ASSERT_TRUE(r.ensureValid());
    ASSERT_TRUE(r.hasDeleteHorizon())
        << "Precondition: batch must have the delete-horizon bit set.";

    // Verify the fields are what we expect.
    EXPECT_EQ(r.baseTimestamp(),  kHorizonMs)
        << "baseTimestamp() must carry the horizon (CONTRACT).";
    EXPECT_EQ(r.maxTimestamp(),   kRecordTs)
        << "maxTimestamp() must carry the actual record timestamp.";
    EXPECT_EQ(r.deleteHorizonMs(), kHorizonMs);

    // Now invoke forEach and check the timestamp passed to the callback.
    int recordsVisited = 0;
    int64_t observedTs = -1;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
        EXPECT_EQ(std::string(k), "key")   << "key mismatch";
        EXPECT_EQ(std::string(v), "val")   << "value mismatch";
        EXPECT_EQ(off, kBaseOffset)        << "offset mismatch";
        observedTs = ts;
        ++recordsVisited;
    });
    ASSERT_EQ(recordsVisited, 1) << "Expected exactly one record in the batch.";

    // The timestamp yielded to the callback MUST be maxTimestamp (kRecordTs),
    // NOT the horizon (kHorizonMs).
    //
    // With the OLD unpatched forEach (baseTs + tsDelta always):
    //   ts = baseTimestamp() + tsDelta = kHorizonMs + 0 = kHorizonMs = 99999
    //   => EXPECT_NE(observedTs, kHorizonMs) FAILS  [non-vacuous]
    //   => EXPECT_EQ(observedTs, kRecordTs)  FAILS  [non-vacuous]
    //
    // With the FIXED forEach (use maxTimestamp() for horizon batches):
    //   ts = maxTimestamp() = kRecordTs = 12345
    //   => both assertions PASS.
    EXPECT_NE(observedTs, kHorizonMs)
        << "forEach on a horizon-stamped batch must NOT yield the horizon epoch ("
        << kHorizonMs << ") as a record timestamp; got ts=" << observedTs
        << ".  The OLD code path (baseTs+tsDelta) would yield horizonMs here.";
    EXPECT_EQ(observedTs, kRecordTs)
        << "forEach on a horizon-stamped batch must yield maxTimestamp() ("
        << kRecordTs << ") as the record timestamp; got ts=" << observedTs << ".";
}

// CP-8: multi-record horizon batch — every record gets maxTimestamp(), not
// horizon+tsDelta.  Also verifies offsets and keys are still correct.
TEST(DeleteHorizon, ForEachMultiRecordHorizonBatchUsesMaxTimestamp) {
    using namespace rune::protocol;

    constexpr int64_t kHorizonMs  = 1000000LL;
    constexpr int64_t kBaseTs     = 500LL;
    constexpr int64_t kBaseOffset = 10LL;
    // Three records at timestamps 500, 600, 700. maxTimestamp = 700.
    constexpr int64_t kMaxTs = 700LL;

    RecordBatchBuilder b(kBaseOffset, kBaseTs);
    b.setDeleteHorizonMs(kHorizonMs);
    b.append("k0", "v0", 500LL);
    b.append("k1", "v1", 600LL);
    b.append("k2", "v2", 700LL);
    auto batch = b.build();

    RecordBatchReader r(batch.data(), batch.size());
    ASSERT_TRUE(r.ensureValid());
    ASSERT_TRUE(r.hasDeleteHorizon());
    EXPECT_EQ(r.maxTimestamp(), kMaxTs);
    EXPECT_EQ(r.baseTimestamp(), kHorizonMs);

    // Every record's ts must be maxTimestamp() (kMaxTs), not horizon+tsDelta.
    int idx = 0;
    r.forEach([&](std::string_view k, std::string_view /*v*/, int64_t off, int64_t ts) {
        EXPECT_EQ(off, kBaseOffset + idx) << "record " << idx;
        EXPECT_EQ(std::string(k), "k" + std::to_string(idx)) << "record " << idx;
        EXPECT_EQ(ts, kMaxTs)
            << "forEach on horizon batch must yield maxTimestamp()=" << kMaxTs
            << " for every record; got ts=" << ts << " for record " << idx
            << ".  Old code would yield kHorizonMs+tsDelta=" << (kHorizonMs + idx*100);
        ++idx;
    });
    EXPECT_EQ(idx, 3);
}

// CP-8: Non-horizon batch forEach is UNCHANGED — still uses baseTimestamp()+tsDelta.
// Regression guard: the fix must NOT break the non-horizon forEach path.
TEST(DeleteHorizon, ForEachNonHorizonBatchUnchanged) {
    using namespace rune::protocol;

    constexpr int64_t kBaseTs    = 2000LL;
    constexpr int64_t kBaseOffset = 5LL;

    RecordBatchBuilder b(kBaseOffset, kBaseTs);
    // No setDeleteHorizonMs call — plain batch.
    b.append("a", "1", kBaseTs + 0);
    b.append("b", "2", kBaseTs + 10);
    b.append("c", "3", kBaseTs + 25);
    auto batch = b.build();

    RecordBatchReader r(batch.data(), batch.size());
    ASSERT_TRUE(r.ensureValid());
    ASSERT_FALSE(r.hasDeleteHorizon()) << "Non-horizon batch must have bit clear.";

    std::vector<int64_t> tsSeen;
    r.forEach([&](std::string_view, std::string_view, int64_t, int64_t ts) {
        tsSeen.push_back(ts);
    });
    ASSERT_EQ(tsSeen.size(), 3u);
    EXPECT_EQ(tsSeen[0], kBaseTs + 0)  << "Non-horizon: tsDelta path must still work.";
    EXPECT_EQ(tsSeen[1], kBaseTs + 10) << "Non-horizon: tsDelta path must still work.";
    EXPECT_EQ(tsSeen[2], kBaseTs + 25) << "Non-horizon: tsDelta path must still work.";
}

// forEachRecord is templated: a MOVE-ONLY visitor (not convertible to
// std::function) compiles and visits every record — no type erasure per record.
TEST(RecordBatchReaderRegression, ForEachRecordTakesAMoveOnlyVisitor) {
    rune::protocol::RecordBatchBuilder b(100, 1000);
    b.append("k1", "v1", 1000);
    b.append("k2", "v2", 1001);
    const auto bytes = b.build();
    rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
    auto n = std::make_unique<int>(0);
    int64_t lastOff = -1;
    r.forEachRecord([c = std::move(n), &lastOff](std::string_view, std::string_view, int64_t off, int64_t) mutable {
        ++*c;
        lastOff = off;
    });
    EXPECT_EQ(lastOff, 101);
}

// F1 regression: a stored length field of 0..8 (torn / zero-filled tail) or a
// huge/negative one must be refused, never read out of bounds (the CRC length
// used to underflow as tl - 21).
TEST(RecordBatchReaderRegression, EnsureValidRefusesShortAndHugeLengths) {
    const uint32_t lens[] = {0u, 1u, 8u, 9u, 48u, 0xFFFFFFFFu, 0xFFFFFFF0u,
                             0x80000000u, 0x7FFFFFFFu, 0x7FFFFFF4u, 0x7FFFFFF3u};
    for (uint32_t l : lens) {
        // Exactly-sized heap buffer so ASan/UBSan flags any OOB read.
        std::vector<uint8_t> buf(b::kRecordBatchOverhead, 0);
        for (int i = 0; i < 4; ++i) buf[b::kLengthOffset + i] = uint8_t(l >> (8 * i));
        rune::protocol::RecordBatchReader r(buf.data(), buf.size());
        EXPECT_FALSE(r.ensureValid()) << "len field " << l;
    }
    // Total lengths 0,1,12,20,60 on a larger buffer (length field = total-12).
    for (int total : {0, 1, 12, 20, 60}) {
        std::vector<uint8_t> buf(200, 0);
        uint32_t l = uint32_t(total - 12);   // wraps for total < 12: also garbage
        for (int i = 0; i < 4; ++i) buf[b::kLengthOffset + i] = uint8_t(l >> (8 * i));
        EXPECT_FALSE(rune::protocol::RecordBatchReader(buf.data(), buf.size()).ensureValid())
            << "total " << total;
    }
    // totalLength() near INT32_MAX is overflow-safe (-1), not UB.
    std::vector<uint8_t> buf(b::kRecordBatchOverhead, 0);
    const uint32_t l = 0x7FFFFFFFu;
    for (int i = 0; i < 4; ++i) buf[b::kLengthOffset + i] = uint8_t(l >> (8 * i));
    EXPECT_EQ(rune::protocol::RecordBatchReader(buf.data(), buf.size()).totalLength(), -1);
}

// ---------------------------------------------------------------------------
// C17 adversarial review (#180): ensureValid refuses a batch whose header
// recordCount disagrees with the record bodies — with the CRC RE-COMPUTED after
// each mutation, so only the count walk can catch it.  Input bounds on the count
// (0, ±1, max, min), truncation of the record area at every offset, trailing
// bytes, varint overflow, negative record length; and the legitimate shapes
// (empty, horizon-stamped, transactional, header-carrying records) stay valid.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> adv180Batch(int n) {
    rune::protocol::RecordBatchBuilder bb(500, 1'000);
    for (int i = 0; i < n; ++i)
        bb.append("key" + std::to_string(i), std::string(static_cast<std::size_t>(i * 37 % 200), 'v'), 1'000 + i);
    return bb.build();
}
void adv180Put32(std::vector<uint8_t>& v, int off, uint32_t x) {
    for (int i = 0; i < 4; ++i) v[static_cast<std::size_t>(off + i)] = uint8_t(x >> (8 * i));
}
void adv180Reseal(std::vector<uint8_t>& v) {   // length + CRC consistent with the current bytes
    adv180Put32(v, b::kLengthOffset, static_cast<uint32_t>(v.size() - 12));
    adv180Put32(v, b::kCrcOffset, rune::protocol::crc32c(v.data() + b::kAttributesOffset, v.size() - b::kAttributesOffset));
}
bool adv180Valid(const std::vector<uint8_t>& v) {
    // Exactly-sized copy so ASan flags any read past the batch.
    auto heap = std::make_unique<uint8_t[]>(v.size());
    std::memcpy(heap.get(), v.data(), v.size());
    return rune::protocol::RecordBatchReader(heap.get(), v.size()).ensureValid();
}
}   // namespace

TEST(RecordBatchAdversarialRegression, CountMismatchWithAValidCrcIsRefusedRegression) {
    for (int n : {1, 2, 7}) {
        const auto good = adv180Batch(n);
        ASSERT_TRUE(adv180Valid(good)) << n;
        for (int64_t c : {int64_t{0}, int64_t{n} - 1, int64_t{n} + 1, int64_t{n} + 1000, int64_t{-1},
                          int64_t{INT32_MAX}, int64_t{INT32_MIN}}) {
            if (c == n) continue;
            auto bad = good;
            adv180Put32(bad, b::kRecordCountOffset, static_cast<uint32_t>(static_cast<int32_t>(c)));
            adv180Reseal(bad);
            EXPECT_FALSE(adv180Valid(bad)) << "records " << n << " header count " << c;
        }
    }
}

TEST(RecordBatchAdversarialRegression, RecordAreaTruncatedAtEveryOffsetIsRefusedRegression) {
    const auto good = adv180Batch(5);
    for (std::size_t cut = b::kRecordBatchOverhead; cut < good.size(); ++cut) {
        std::vector<uint8_t> bad(good.begin(), good.begin() + static_cast<std::ptrdiff_t>(cut));
        adv180Reseal(bad);   // the length field and CRC both agree with the short body
        EXPECT_FALSE(adv180Valid(bad)) << "cut at " << cut << " of " << good.size();
    }
}

TEST(RecordBatchAdversarialRegression, TrailingBytesInsideTheBatchAreRefusedRegression) {
    for (int extra : {1, 2, 5, 64}) {
        auto bad = adv180Batch(3);
        bad.insert(bad.end(), static_cast<std::size_t>(extra), uint8_t{0});
        adv180Reseal(bad);
        EXPECT_FALSE(adv180Valid(bad)) << "trailing " << extra;
    }
}

TEST(RecordBatchAdversarialRegression, OverlongNegativeAndOversizedRecordLengthsAreRefusedRegression) {
    const auto good = adv180Batch(1);
    std::vector<uint8_t> hdr(good.begin(), good.begin() + b::kRecordBatchOverhead);
    // (a) a six-byte varint (continuation bit on five bytes): overflow, never UB.
    // (b) zigzag -1 (raw 1).  (c) a length far past the end.  (d) an unterminated varint.
    const std::vector<std::vector<uint8_t>> bodies = {
        {0x80, 0x80, 0x80, 0x80, 0x80, 0x00},
        {0x01, 0x00, 0x00},
        {0xFE, 0xFF, 0xFF, 0xFF, 0x0F},
        {0x80},
    };
    for (std::size_t i = 0; i < bodies.size(); ++i) {
        auto bad = hdr;
        bad.insert(bad.end(), bodies[i].begin(), bodies[i].end());
        adv180Put32(bad, b::kRecordCountOffset, 1u);
        adv180Reseal(bad);
        EXPECT_FALSE(adv180Valid(bad)) << "body " << i;
    }
}

TEST(RecordBatchAdversarialRegression, LegitimateShapesStayValidRegression) {
    // Empty (count 0, no record bytes).
    {
        rune::protocol::RecordBatchBuilder bb(0, 1);
        EXPECT_TRUE(adv180Valid(bb.build())) << "empty batch";
    }
    // Horizon-stamped, transactional, protocol-built control, idempotent.
    {
        rune::protocol::RecordBatchBuilder bb(10, 1'000);
        bb.append("a", "", 1'000);
        bb.append("", "tombstone-ish", 1'005);
        bb.setDeleteHorizonMs(9'999'999);
        bb.setTransactional(true);
        bb.setProducerId(77); bb.setProducerEpoch(3); bb.setBaseSequence(41);
        EXPECT_TRUE(adv180Valid(bb.build())) << "horizon + transactional";
        bb.setControl(true);
        EXPECT_TRUE(adv180Valid(bb.build())) << "protocol-built control batch";
    }
    // A record carrying two headers, hand-encoded: the walk only follows the
    // length prefix, so the headers are covered by it.
    {
        auto v = adv180Batch(1);
        std::vector<uint8_t> hdr(v.begin(), v.begin() + b::kRecordBatchOverhead);
        // attrs 0, tsDelta 0, offDelta 0, keyLen 1 'k', valLen 1 'v', 2 headers: ("h",'x'), ("i",'y')
        const std::vector<uint8_t> body = {0x00, 0x00, 0x00, 0x02, 'k', 0x02, 'v',
                                           0x04, 0x02, 'h', 0x02, 'x', 0x02, 'i', 0x02, 'y'};
        hdr.push_back(static_cast<uint8_t>(body.size() * 2));   // zigzag length
        hdr.insert(hdr.end(), body.begin(), body.end());
        adv180Put32(hdr, b::kRecordCountOffset, 1u);
        adv180Reseal(hdr);
        EXPECT_TRUE(adv180Valid(hdr)) << "header-carrying record";
        int seen = 0;
        rune::protocol::RecordBatchReader(hdr.data(), hdr.size()).forEachRecord(
            [&](std::string_view k, std::string_view val, int64_t, int64_t) { ++seen; EXPECT_EQ(k, "k"); EXPECT_EQ(val, "v"); });
        EXPECT_EQ(seen, 1);
    }
    // Two batches back to back: the first one's walk stops at ITS end.
    {
        auto two = adv180Batch(3);
        const auto second = adv180Batch(2);
        two.insert(two.end(), second.begin(), second.end());
        EXPECT_TRUE(rune::protocol::RecordBatchReader(two.data(), two.size()).ensureValid());
    }
}
