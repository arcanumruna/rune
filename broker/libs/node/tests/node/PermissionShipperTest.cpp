// PermissionShipperTest.cpp - authentication Task 13: the control thread's resumable per-worker chunk sender.
#include <rune/metadata/ShardRouter.h>
#include <rune/node/PermissionShipper.h>
#include <rune/node/WorkerPool.h>

#include <gtest/gtest.h>

#include <vector>

using namespace rune::node;

namespace {
std::unique_ptr<WorkerPool> pool(int32_t n, std::size_t ringBytes = 1 << 16) {
    return std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
}
void fillLifecycleRing(Worker& w) {
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (w.tryEnqueueOp(filler)) {}
}
void drain(Worker& w) { for (int i = 0; i < 3; ++i) w.doWork(1); }   // a ring read stops at the wrap point
std::vector<uint64_t> words(std::size_t n) { std::vector<uint64_t> v(n); for (std::size_t i = 0; i < n; ++i) v[i] = i + 1; return v; }
}  // namespace

TEST(PermissionShipperRegression, AFullRingResumesWithoutDuplicatingAChunk) {
    auto p = pool(2);
    PermissionShipper s;
    fillLifecycleRing(p->worker(1));                    // worker 1 cannot take anything
    const auto bits = words(20'000);                    // 40 chunks of 512 words (more than the 64 KiB ring holds)
    EXPECT_FALSE(s.ship(*p, 5, bits, false, 1, /*owner=*/7));
    EXPECT_EQ(s.fullyShippedEpoch(5), 0u);
    for (int round = 0; round < 200 && s.fullyShippedEpoch(5) < 1; ++round) {
        drain(p->worker(0));
        drain(p->worker(1));
        (void)s.retry(*p);
    }
    EXPECT_EQ(s.fullyShippedEpoch(5), 1u);
    drain(p->worker(0));
    drain(p->worker(1));
    const uint64_t chunks = (bits.size() + PermissionShipper::kChunkWords - 1) / PermissionShipper::kChunkWords;
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), chunks);
    EXPECT_EQ(p->worker(1).sessionPermissionChunksSeenForTest(), chunks) << "resumed, never re-sent";
    EXPECT_EQ(s.chunksSentForTest(), 2 * chunks);
}

TEST(PermissionShipperRegression, LatestUpdateReplacesAQueuedOne) {
    auto p = pool(2);
    PermissionShipper s;
    fillLifecycleRing(p->worker(1));
    EXPECT_FALSE(s.ship(*p, 5, words(10), false, 1, /*owner=*/7));   // worker 0 takes it, worker 1 is queued
    EXPECT_FALSE(s.ship(*p, 5, words(10), false, 2, /*owner=*/7));   // the queued older update is replaced
    EXPECT_EQ(s.pendingUpdates(), 1u);
    drain(p->worker(1));
    (void)s.retry(*p);
    EXPECT_EQ(s.fullyShippedEpoch(5), 2u);
    drain(p->worker(0));
    drain(p->worker(1));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 2u) << "worker 0 took both updates";
    EXPECT_EQ(p->worker(1).sessionPermissionChunksSeenForTest(), 1u) << "worker 1 only ever saw the newer one";
}

TEST(PermissionShipperRegression, FullyShippedEpochAdvancesOnlyWhenEveryWorkerHasIt) {
    auto p = pool(3);
    PermissionShipper s;
    fillLifecycleRing(p->worker(2));
    EXPECT_FALSE(s.ship(*p, 9, words(3), true, 7, /*owner=*/7));
    EXPECT_EQ(s.fullyShippedEpoch(9), 0u) << "two of three workers is not shipped";
    drain(p->worker(2));
    EXPECT_GE(s.retry(*p), 1);
    EXPECT_EQ(s.fullyShippedEpoch(9), 7u);
    EXPECT_EQ(s.pendingUpdates(), 0u);
    s.forget(9);
    EXPECT_EQ(s.fullyShippedEpoch(9), 0u);
}

TEST(PermissionShipperRegression, AnEmptyBitmapIsShippedAsOneLastChunk) {
    auto p = pool(1);
    PermissionShipper s;
    EXPECT_TRUE(s.ship(*p, 4, {}, false, 3, /*owner=*/7)) << "a user with no WRITE grant still replaces the table";
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 1u);
}
