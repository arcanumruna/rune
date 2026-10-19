#include <gtest/gtest.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>
#include <cstring>
#include <vector>

using rune::storage::InMemoryLog;
using rune::storage::FullPolicy;

namespace {
std::vector<uint8_t> batchAt(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", "v", 1000);
    return b.build();
}
}

TEST(InMemoryLogReadView, ContiguousBatchesAreOneZeroCopyView) {
    InMemoryLog log(1 << 20, 1024, FullPolicy::DropOldest);
    const auto b0 = batchAt(0), b1 = batchAt(1), b2 = batchAt(2);
    log.appendNoSync(b0.data(), b0.size(), 0);
    log.appendNoSync(b1.data(), b1.size(), 1);
    log.appendNoSync(b2.data(), b2.size(), 2);
    const auto v = log.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.len, b0.size() + b1.size() + b2.size());
    EXPECT_EQ(v.baseOffset, 0);
    EXPECT_EQ(v.nextOffset, 3);
    EXPECT_EQ(0, std::memcmp(v.data, b0.data(), b0.size()));
}

TEST(InMemoryLogReadView, CapLimitsToWholeBatchesButFirstAlwaysIncluded) {
    InMemoryLog log(1 << 20, 1024, FullPolicy::DropOldest);
    const auto b0 = batchAt(0), b1 = batchAt(1);
    log.appendNoSync(b0.data(), b0.size(), 0);
    log.appendNoSync(b1.data(), b1.size(), 1);
    const auto v = log.readView(0, static_cast<int32_t>(b0.size() + 1));
    EXPECT_EQ(v.len, b0.size());
    EXPECT_EQ(v.nextOffset, 1);
    const auto tiny = log.readView(0, 1);
    EXPECT_EQ(tiny.len, b0.size()) << "first batch always included (progress guarantee)";
}

TEST(InMemoryLogReadViewRegression, ViewStopsAtRingWrap) {
    const auto b = batchAt(0);
    InMemoryLog log(3 * b.size() + b.size() / 2, 1024, FullPolicy::DropOldest);
    for (int64_t i = 0; i < 4; ++i) {           // the 4th batch wraps to ring offset 0
        const auto x = batchAt(i);
        log.appendNoSync(x.data(), x.size(), i);
    }
    const auto v2 = log.readView(2, 1 << 20);
    EXPECT_EQ(v2.len, b.size()) << "batch 3 is not adjacent to batch 2 after the wrap";
    EXPECT_EQ(v2.nextOffset, 3);
    const auto v3 = log.readView(3, 1 << 20);
    EXPECT_EQ(v3.baseOffset, 3);
    EXPECT_EQ(v3.len, b.size());
}
