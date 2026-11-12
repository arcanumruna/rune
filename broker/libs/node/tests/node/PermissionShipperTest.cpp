// PermissionShipperTest.cpp - authentication Task 13 / issue #259: the control thread's resumable, strictly FIFO sender of the WRITE
// tables (once per grant set), their patches and the sessions' binds.
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
    const uint64_t seq = s.shipTable(*p, 5, bits, false);
    EXPECT_LT(s.shippedSeq(), seq);
    for (int round = 0; round < 200 && s.shippedSeq() < seq; ++round) {
        drain(p->worker(0));
        drain(p->worker(1));
        (void)s.retry(*p);
    }
    EXPECT_GE(s.shippedSeq(), seq);
    drain(p->worker(0));
    drain(p->worker(1));
    const uint64_t chunks = (bits.size() + PermissionShipper::kChunkWords - 1) / PermissionShipper::kChunkWords;
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), chunks);
    EXPECT_EQ(p->worker(1).sessionPermissionChunksSeenForTest(), chunks) << "resumed, never re-sent";
    EXPECT_EQ(s.chunksSentForTest(), 2 * chunks);
}

// Issue #259: nothing is coalesced or reordered - a bind always reaches a worker after the table it names, and a
// table's drop after the rebinds away from it (a worker that saw them out of order would grant a stale set).
TEST(PermissionShipperRegression, ItemsReachEveryWorkerInQueueOrderRegression) {
    auto p = pool(2);
    PermissionShipper s;
    fillLifecycleRing(p->worker(1));
    const uint64_t t  = s.shipTable(*p, 3, words(10), false);
    const uint64_t b  = s.bind(*p, 5, 3, 1, /*owner=*/7);
    const uint64_t d  = s.dropTable(*p, 2);
    EXPECT_LT(t, b);
    EXPECT_LT(b, d);
    EXPECT_EQ(s.pendingUpdates(), 3u) << "worker 1 has none of them";
    drain(p->worker(1));
    (void)s.retry(*p);
    EXPECT_EQ(s.shippedSeq(), d);
    EXPECT_EQ(s.pendingUpdates(), 0u);
    drain(p->worker(0));
    drain(p->worker(1));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 3u);
    EXPECT_EQ(p->worker(1).sessionPermissionChunksSeenForTest(), 3u);
}

TEST(PermissionShipperRegression, ShippedSeqAdvancesOnlyWhenEveryWorkerHasIt) {
    auto p = pool(3);
    PermissionShipper s;
    fillLifecycleRing(p->worker(2));
    const uint64_t seq = s.bind(*p, 9, PermissionShipper::kDenyTable, 7, /*owner=*/7);
    EXPECT_LT(s.shippedSeq(), seq) << "two of three workers is not shipped";
    drain(p->worker(2));
    EXPECT_GE(s.retry(*p), 1);
    EXPECT_EQ(s.shippedSeq(), seq);
    EXPECT_EQ(s.pendingUpdates(), 0u);
}

TEST(PermissionShipperRegression, AnEmptyBitmapIsShippedAsOneLastChunk) {
    auto p = pool(1);
    PermissionShipper s;
    const uint64_t seq = s.shipTable(*p, 4, {}, false);
    EXPECT_EQ(s.shippedSeq(), seq) << "a set with no WRITE grant still ships its (empty) table";
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 1u);
}

TEST(PermissionShipperRegression, APatchIsOneOpOfItsWordsOnlyRegression) {
    auto p = pool(2);
    PermissionShipper s;
    const auto w = words(1);
    (void)s.patchTable(*p, 4, /*first=*/1500, w, /*total=*/1600);
    EXPECT_EQ(s.chunksSentForTest(), 2u) << "one op per worker";
    EXPECT_EQ(s.wordsSentForTest(), 2u) << "one word per worker, never the 1600-word table";
}

TEST(CheckedWritesImportGrantRegression, WriteTablesShipUnchangedRegression) {
    auto p = pool(1);
    PermissionShipper s;
    const uint64_t t = s.shipTable(*p, 3, words(10), false);
    (void)s.bind(*p, 5, 3, 1, /*owner=*/7);
    EXPECT_EQ(s.queuedSeq(), t + 1) << "a table without IMPORT bits queues no import item";
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 2u);
    EXPECT_EQ(s.chunksSentForTest(), 2u);
}

TEST(CheckedWritesImportGrant, AnImportTableAndPatchShipAsOneChunkEachBehindTheirTable) {
    auto p = pool(1);
    PermissionShipper s;
    (void)s.shipTable(*p, 3, words(10), false);
    (void)s.shipImportTable(*p, 3, words(10));
    (void)s.patchImportTable(*p, 3, 0, words(1), 10);
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 3u);
    EXPECT_EQ(s.chunksSentForTest(), 3u);
}

// #500: the applied topic frontier ships to every worker on the same FIFO as the permission items, as the
// kPermFlagSecurityBehind wire flag with its own `topicId` (kPermLagTopicFrontier), so a worker never reads it as a lag
// level - and a lag level shipped later does not disturb it.
TEST(TopicFrontierShipperRegression, TheFrontierReachesEveryWorkerAndIsNeverALagLevelRegression) {
    auto p = pool(2);
    PermissionShipper s;
    EXPECT_EQ(p->worker(0).appliedTopicFrontierForTest(), INT32_MAX) << "none shipped yet";
    const uint64_t seq = s.topicFrontier(*p, 7);
    drain(p->worker(0));
    drain(p->worker(1));
    EXPECT_EQ(s.shippedSeq(), seq);
    for (int w = 0; w < 2; ++w) {
        EXPECT_EQ(p->worker(w).appliedTopicFrontierForTest(), 7) << "worker " << w;
        EXPECT_EQ(p->worker(w).securityLagForTest(), 0u) << "worker " << w << ": the frontier is not a lag level";
        EXPECT_EQ(p->worker(w).topicFrontierRejectedForTest(), 0u);
    }
    (void)s.securityLag(*p, 2);
    drain(p->worker(0));
    drain(p->worker(1));
    for (int w = 0; w < 2; ++w) {
        EXPECT_EQ(p->worker(w).securityLagForTest(), 2u) << "worker " << w;
        EXPECT_EQ(p->worker(w).appliedTopicFrontierForTest(), 7) << "worker " << w << ": a lag level leaves it alone";
    }
}

// #500: the worker's copy of the frontier only grows; a lower value, a negative one and one past int32 are ignored and
// counted.  Mutation: accept a lower value -> the frontier moves back (red).
TEST(TopicFrontierShipperRegression, TheWorkersFrontierNeverMovesBackRegression) {
    auto p = pool(1);
    PermissionShipper s;
    (void)s.topicFrontier(*p, 10);
    (void)s.topicFrontier(*p, 4);
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).appliedTopicFrontierForTest(), 10);
    EXPECT_EQ(p->worker(0).topicFrontierRejectedForTest(), 1u);
    (void)s.topicFrontier(*p, 10);
    (void)s.topicFrontier(*p, 12);
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).appliedTopicFrontierForTest(), 12);
    EXPECT_EQ(p->worker(0).topicFrontierRejectedForTest(), 1u);
    (void)s.topicFrontier(*p, -1);   // 0xFFFFFFFF on the wire: past int32, out of range
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).appliedTopicFrontierForTest(), 12);
    EXPECT_EQ(p->worker(0).topicFrontierRejectedForTest(), 2u);
}

// #500: ORDER - the frontier of an event is queued behind the permission patches of that same event, so a worker that
// sees the new frontier has already installed the bitmap bit of the topic the event created (otherwise a produce could
// be refused NotLeader after the apply, or 45 before it).  Held queue: the patch precedes the frontier.
// Mutation: queue the frontier at the front -> the patch no longer precedes it (red).
TEST(TopicFrontierShipperRegression, TheFrontierIsQueuedBehindThePatchesOfItsEventRegression) {
    auto p = pool(1);
    PermissionShipper s;
    s.setHoldForTest(true);
    const std::vector<uint64_t> w1{1};
    (void)s.patchTable(*p, 0, 0, w1, 1);
    (void)s.patchTable(*p, 1, 0, w1, 1);
    (void)s.topicFrontier(*p, 9);
    const auto q = s.queuedFrontiersForTest();
    ASSERT_EQ(q.size(), 1u);
    EXPECT_EQ(q[0].first, 9);
    EXPECT_EQ(q[0].second, 2u) << "both patches are queued before the frontier";
    s.setHoldForTest(false);
    (void)s.retry(*p);
    drain(p->worker(0));
    EXPECT_EQ(p->worker(0).sessionPermissionChunksSeenForTest(), 3u);
    EXPECT_EQ(p->worker(0).appliedTopicFrontierForTest(), 9);
}
