// BatchRebuildTest.cpp — Phase 2c Task 1: a filtered subscription's rebuilt batch keeps the source's offsets.
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

using namespace rune::protocol;

namespace {
std::vector<uint8_t> batch5(int64_t base) {
    RecordBatchBuilder b(base, 1'000'000);
    b.setProducerId(77); b.setProducerEpoch(3); b.setBaseSequence(40);
    for (int i = 0; i < 5; ++i) b.append("k" + std::to_string(i), "v" + std::to_string(i), 1'000'000 + i);
    return b.build();
}
std::vector<RecordSpan> spans(const std::vector<uint8_t>& b) {
    std::vector<RecordSpan> v;
    EXPECT_TRUE(forEachRecordSpan(b.data(), b.size(), [&](const RecordSpan& r) { v.push_back(r); }));
    return v;
}
}  // namespace

TEST(BatchRebuild, SpansCoverEveryRecordInOrder) {
    const auto b = batch5(100);
    const auto s = spans(b);
    ASSERT_EQ(s.size(), 5u);
    for (std::size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(s[i].offset, 100 + static_cast<int64_t>(i));
        EXPECT_EQ(s[i].key, "k" + std::to_string(i));
        EXPECT_EQ(s[i].value, "v" + std::to_string(i));
        EXPECT_FALSE(s[i].nullValue);
        if (i > 0) EXPECT_EQ(s[i].pos, s[i - 1].pos + s[i - 1].len);
    }
    EXPECT_EQ(s.back().pos + s.back().len, b.size());
}

TEST(BatchRebuildRegression, RebuiltBatchKeepsOffsetsBaseAndLastOffsetDeltaRegression) {
    const auto src = batch5(100);
    const auto s = spans(src);
    const std::array<uint32_t, 2> keep{1, 3};
    std::vector<uint8_t> out(src.size());
    const std::size_t n = rebuildBatch(src, s, keep, out);
    ASSERT_GT(n, 0u);
    RecordBatchReader r(out.data(), n);
    ASSERT_TRUE(r.ensureValid()) << "CRC recomputed";
    EXPECT_EQ(r.baseOffset(), 100);
    EXPECT_EQ(r.lastOffsetDelta(), 4) << "the rebuilt batch still spans the whole source range";
    EXPECT_EQ(r.recordCount(), 2);
    EXPECT_EQ(r.totalLength(), static_cast<int32_t>(n));
    std::vector<int64_t> offs;
    std::vector<std::string> vals;
    r.forEachRecord([&](std::string_view, std::string_view v, int64_t o, int64_t) { offs.push_back(o); vals.emplace_back(v); });
    EXPECT_EQ(offs, (std::vector<int64_t>{101, 103}));
    EXPECT_EQ(vals, (std::vector<std::string>{"v1", "v3"}));
    // The producer identity and timestamps are the source's.
    RecordBatchReader sr(src.data(), src.size());
    EXPECT_EQ(r.producerId(), sr.producerId());
    EXPECT_EQ(r.baseTimestamp(), sr.baseTimestamp());
}

TEST(BatchRebuildRegression, RefusesEmptyKeepOutOfRangeAndShortOutputRegression) {
    const auto src = batch5(0);
    const auto s = spans(src);
    std::vector<uint8_t> out(src.size());
    EXPECT_EQ(rebuildBatch(src, s, std::span<const uint32_t>{}, out), 0u);
    const std::array<uint32_t, 1> bad{5};
    EXPECT_EQ(rebuildBatch(src, s, bad, out), 0u);
    const std::array<uint32_t, 1> one{0};
    std::vector<uint8_t> tiny(batch::kRecordBatchOverhead);
    EXPECT_EQ(rebuildBatch(src, s, one, tiny), 0u);
    const std::array<uint32_t, 2> dup{1, 1};
    EXPECT_EQ(rebuildBatch(src, s, dup, out), 0u);
    const std::array<uint32_t, 2> desc{3, 1};
    EXPECT_EQ(rebuildBatch(src, s, desc, out), 0u);
}

TEST(BatchRebuildRegression, ARecordThatDoesNotFitTheBatchStopsTheWalkRegression) {
    auto b = batch5(0);
    // A buffer shorter than the header's totalLength: refused before any record is visited.
    auto shortBuf = b;
    shortBuf.resize(shortBuf.size() - 3);
    int seen = 0;
    EXPECT_FALSE(forEachRecordSpan(shortBuf.data(), shortBuf.size(), [&](const RecordSpan&) { ++seen; }));
    EXPECT_EQ(seen, 0);
    // A header whose length is consistent with a torn last record: the walk stops AT that record.
    b.resize(b.size() - 3);
    const auto l = static_cast<uint32_t>(b.size() - 12);
    for (int i = 0; i < 4; ++i) b[static_cast<std::size_t>(batch::kLengthOffset + i)] = static_cast<uint8_t>(l >> (8 * i));
    EXPECT_FALSE(forEachRecordSpan(b.data(), b.size(), [&](const RecordSpan&) { ++seen; }));
    EXPECT_EQ(seen, 4);
}

TEST(BatchRebuild, AnOffsetDeltaPastLastOffsetDeltaStopsTheWalkRegression) {
    auto b = batch5(0);
    const auto s = spans(b);
    // Record layout: bodyLen varint (1 byte), attributes (1), timestampDelta varint (1), offsetDelta varint.
    const std::size_t at = s[2].pos + 3;
    ASSERT_EQ(b[at], 4u) << "zigzag(2)";
    b[at] = 18;   // zigzag(9) > lastOffsetDelta (4)
    int seen = 0;
    EXPECT_FALSE(forEachRecordSpan(b.data(), b.size(), [&](const RecordSpan&) { ++seen; }));
    EXPECT_EQ(seen, 2);
}

namespace {
std::vector<uint8_t> intentBatch() {
    RecordBatchBuilder b(0, 1'000'000);
    b.append("a", "1", 1'000'000, 0);
    b.append("bb", "", 1'000'000, 1);
    b.append("ccc", "3", 1'000'000, 2);
    return b.build();
}
}  // namespace

TEST(CheckedWritesRecordSpan, CarriesAttributesKeyAndIndex) {
    const auto b = intentBatch();
    const auto s = spans(b);
    ASSERT_EQ(s.size(), 3u);
    const uint8_t attrs[] = {0, 1, 2};
    const char* keys[] = {"a", "bb", "ccc"};
    for (uint32_t i = 0; i < 3; ++i) {
        EXPECT_EQ(s[i].attributes, attrs[i]);
        EXPECT_EQ(s[i].index, i);
        EXPECT_EQ(s[i].key, keys[i]);
        EXPECT_GE(reinterpret_cast<const uint8_t*>(s[i].key.data()), b.data());
        EXPECT_LE(reinterpret_cast<const uint8_t*>(s[i].key.data()) + s[i].key.size(), b.data() + b.size());
    }
    EXPECT_TRUE(s[1].value.empty());
    EXPECT_FALSE(s[1].nullValue);
    RecordBatchReader r(b.data(), b.size());
    EXPECT_TRUE(r.ensureValid());
}

TEST(CheckedWritesRecordSpanRegression, RebuildKeepsAttributesVerbatimRegression) {
    const auto b = intentBatch();
    const auto s = spans(b);
    const std::array<uint32_t, 2> keep{0, 2};
    std::vector<uint8_t> out(b.size());
    const std::size_t n = rebuildBatch(b, s, keep, out);
    ASSERT_GT(n, 0u);
    out.resize(n);
    const auto rs = spans(out);
    ASSERT_EQ(rs.size(), 2u);
    EXPECT_EQ(rs[0].attributes, 0);
    EXPECT_EQ(rs[1].attributes, 2);
    EXPECT_EQ(rs[1].index, 1u);
    RecordBatchReader r(out.data(), out.size());
    EXPECT_TRUE(r.ensureValid());
}
