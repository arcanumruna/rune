// Part 04 Task 4: RejectedOffsetIndex (C11).
#include <gtest/gtest.h>

#include <vector>

#include <rune/storage/RejectedOffsetIndex.h>

using rune::storage::RejectedOffsetIndex;
using Entry = RejectedOffsetIndex::Entry;

namespace {
RejectedOffsetIndex indexWith(std::initializer_list<int64_t> offsets, std::size_t capacity = 64) {
    RejectedOffsetIndex idx;
    idx.reserve(capacity);
    for (const int64_t o : offsets) EXPECT_TRUE(idx.add(o, 55, 1));
    return idx;
}
}  // namespace

TEST(CheckedWritesRejectedIndex, AddIsAscendingOnly) {
    RejectedOffsetIndex idx;
    idx.reserve(8);
    EXPECT_TRUE(idx.add(5, 55, 1));
    EXPECT_TRUE(idx.add(9, 55, 1));
    EXPECT_FALSE(idx.add(7, 55, 1));
    EXPECT_EQ(idx.size(), 2u);
    EXPECT_EQ(idx.outOfOrder(), 1u);
}

TEST(CheckedWritesRejectedIndex, LookupsAndRanges) {
    const auto idx = indexWith({5, 9, 20});
    EXPECT_TRUE(idx.isRejected(9));
    EXPECT_FALSE(idx.isRejected(10));
    EXPECT_EQ(idx.nextRejectedAtOrAfter(10), 20);
    EXPECT_EQ(idx.nextRejectedAtOrAfter(21), INT64_MAX);
    std::vector<int64_t> seen;
    idx.forEachIn(6, 20, [&](const Entry& e) { seen.push_back(e.offset); });
    EXPECT_EQ(seen, std::vector<int64_t>{9});
    ASSERT_NE(idx.find(20), nullptr);
    EXPECT_EQ(idx.find(20)->code, 55);
}

TEST(CheckedWritesRejectedIndex, MarkerMergeIsIdempotent) {
    RejectedOffsetIndex idx;
    idx.reserve(8);
    const std::vector<Entry> e{{5, 55, 1}, {9, 55, 1}};
    idx.onMarker(0, 30, e);
    idx.onMarker(0, 30, e);
    EXPECT_EQ(idx.size(), 2u);
    EXPECT_EQ(idx.mismatches(), 0u);
    EXPECT_EQ(idx.coveredThrough(), 30);
}

TEST(CheckedWritesRejectedIndexRegression, MarkerDisagreementCountsAMismatchRegression) {
    RejectedOffsetIndex idx;
    idx.reserve(8);
    ASSERT_TRUE(idx.add(9, 55, 1));
    idx.onMarker(0, 30, std::vector<Entry>{{9, 56, 0}});
    EXPECT_EQ(idx.mismatches(), 1u);
    EXPECT_EQ(idx.find(9)->code, 55);   // unchanged
    idx.onMarker(0, 30, {});
    EXPECT_EQ(idx.mismatches(), 2u);
}

TEST(CheckedWritesRejectedIndex, FullIsFalseNeverOverwrites) {
    RejectedOffsetIndex idx;
    idx.reserve(2);
    EXPECT_TRUE(idx.add(1, 55, 1));
    EXPECT_TRUE(idx.add(2, 55, 1));
    EXPECT_FALSE(idx.add(3, 55, 1));
    EXPECT_EQ(idx.size(), 2u);
    EXPECT_TRUE(idx.isRejected(2));
}

TEST(CheckedWritesRejectedIndex, GrowKeepsEntries) {
    RejectedOffsetIndex idx;
    idx.reserve(2);
    EXPECT_TRUE(idx.add(1, 55, 1));
    EXPECT_TRUE(idx.add(2, 55, 1));
    idx.grow(8);
    EXPECT_EQ(idx.capacity(), 8u);
    EXPECT_TRUE(idx.isRejected(1));
    EXPECT_TRUE(idx.isRejected(2));
    EXPECT_TRUE(idx.add(3, 55, 1));
}

TEST(CheckedWritesRejectedIndex, TruncateAndDropBelow) {
    auto idx = indexWith({5, 9, 20});
    idx.truncateFrom(9);
    EXPECT_EQ(idx.size(), 1u);
    EXPECT_TRUE(idx.isRejected(5));
    idx.dropBelow(6);
    EXPECT_EQ(idx.size(), 0u);
}

TEST(CheckedWritesRejectedIndex, CountAtOrAfter) {
    const auto idx = indexWith({5, 9, 20});
    EXPECT_EQ(idx.countAtOrAfter(9), 2u);
    EXPECT_EQ(idx.countAtOrAfter(21), 0u);
}
