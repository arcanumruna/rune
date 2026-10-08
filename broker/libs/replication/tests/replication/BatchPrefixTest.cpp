#include <gtest/gtest.h>
#include <rune/replication/BatchPrefix.h>
#include <rune/protocol/RecordBatch.h>
#include <vector>

using namespace rune::replication;

namespace {
std::vector<uint8_t> threeBatches(std::size_t& one) {
    std::vector<uint8_t> all;
    for (int64_t base = 0; base < 3; ++base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", "v", 1000);
        const auto x = b.build();
        one = x.size();
        all.insert(all.end(), x.begin(), x.end());
    }
    return all;
}
}

TEST(BatchPrefix, WholeBatchesUnderCap) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), 2 * one + 1, 1LL << 62);
    EXPECT_EQ(p.bytes, 2 * one);
    EXPECT_EQ(p.endOffset, 2);
}

TEST(BatchPrefix, CeilingStopsBeforeBatchAtOrAboveIt) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), all.size(), /*ceiling=*/1);
    EXPECT_EQ(p.bytes, one);
    EXPECT_EQ(p.endOffset, 1);
}

// F18 (KIP-74): a first batch larger than the cap is still served WHOLE — never
// "nothing" (that stalled the follower at that offset forever).  Later batches
// stay bounded by the cap.
TEST(BatchPrefixRegression, FirstBatchLargerThanCapIsStillServedWhole) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), one - 1, 1LL << 62);
    EXPECT_EQ(p.bytes, one);
    EXPECT_EQ(p.endOffset, 1);
    // cap 0 → still exactly the first batch.
    const auto z = wholeBatchPrefix(all.data(), all.size(), 0, 1LL << 62);
    EXPECT_EQ(z.bytes, one);
    EXPECT_EQ(z.endOffset, 1);
}

// The flow-control ceiling still applies to the first batch (push window).
TEST(BatchPrefixRegression, CeilingStillGatesAnOversizedFirstBatch) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), one - 1, /*ceiling=*/0);
    EXPECT_EQ(p.bytes, 0u);
    EXPECT_EQ(p.endOffset, -1);
}

// A torn (incomplete) first batch is never served, whatever the cap.
TEST(BatchPrefixRegression, TornOversizedFirstBatchNotServed) {
    std::size_t one = 0;
    auto all = threeBatches(one);
    all.resize(one - 1);
    EXPECT_EQ(wholeBatchPrefix(all.data(), all.size(), 1, 1LL << 62).bytes, 0u);
}

TEST(BatchPrefix, TornTailIgnoredAndNullSafe) {
    std::size_t one = 0;
    auto all = threeBatches(one);
    all.resize(all.size() - 3);
    EXPECT_EQ(wholeBatchPrefix(all.data(), all.size(), all.size(), 1LL << 62).bytes, 2 * one);
    EXPECT_EQ(wholeBatchPrefix(nullptr, 0, 100, 1LL << 62).bytes, 0u);
}
