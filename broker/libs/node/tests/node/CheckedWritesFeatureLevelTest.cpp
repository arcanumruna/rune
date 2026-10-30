// CheckedWritesFeatureLevelTest.cpp — checked writes Part 02 Task 2: MetadataFeature::kCheckedWrites and its
// propagation (control thread -> every worker and the admin thread), resumable and latest-wins.
#include <rune/node/ClusterFeatureLevelFanout.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/ReplicationMessages.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
namespace mf = rune::replication::MetadataFeature;

namespace {
std::unique_ptr<WorkerPool> poolOf(int32_t n, std::size_t ringBytes = 1 << 16) {
    auto p = std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
    p->setRunnersStartedForTest(true);
    return p;
}
void drainAll(WorkerPool& p) {
    for (int pass = 0; pass < 2; ++pass)
        for (int32_t w = 0; w < p.workerCount(); ++w) p.worker(w).doWork(1);
}
void sendLevel(Worker& w, int16_t level) {
    WorkerOp op;
    op.type       = WorkerOp::Type::ClusterFeatureLevel;
    op.nextOffset = level;
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    w.doWork(1);
}
} // namespace

TEST(CheckedWritesFeatureLevel, LatestIsCheckedWrites) {
    static_assert(mf::kCheckedWrites == 5 && mf::kLatest == mf::kCheckedWrites && mf::kTxnPartitionVerify == 4);
    SUCCEED();
}

TEST(CheckedWritesFeatureLevel, WorkerStartsAtBaseAndAppliesTheOp) {
    Worker w(0);
    EXPECT_EQ(w.clusterFeatureLevelForTest(), 0);
    sendLevel(w, 5);
    EXPECT_EQ(w.clusterFeatureLevelForTest(), 5);
}

TEST(CheckedWritesFeatureLevel, FeatureLevelNeverLowers) {
    Worker w(0);
    sendLevel(w, 5);
    sendLevel(w, 4);
    EXPECT_EQ(w.clusterFeatureLevelForTest(), 5);
}

TEST(CheckedWritesFeatureLevel, FanoutReachesEveryWorkerAndTheAdmin) {
    auto p = poolOf(3);
    OneToOneByteRing admin(kControlToAdminRingBytes);
    ClusterFeatureLevelFanout f;
    f.post(5);
    EXPECT_GT(f.pump(p.get(), &admin), 0);
    EXPECT_FALSE(f.pendingForTest());
    drainAll(*p);
    for (int32_t w = 0; w < 3; ++w) EXPECT_EQ(p->worker(w).clusterFeatureLevelForTest(), 5) << "worker " << w;
    int seen = 0;
    admin.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        ++seen;
        EXPECT_EQ(tid, kMsgClusterFeatureLevel);
        int16_t level = 0;
        ASSERT_TRUE(decodeClusterFeatureLevel(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len), level));
        EXPECT_EQ(level, 5);
    }, 8);
    EXPECT_EQ(seen, 1);
}

TEST(CheckedWritesFeatureLevelRegression, FullRingIsRetriedNotDroppedRegression) {
    auto p = poolOf(2, /*ringBytes=*/4096);
    OneToOneByteRing admin(kControlToAdminRingBytes);
    WorkerOp filler;
    filler.type       = WorkerOp::Type::ReadLease;
    filler.nextOffset = 1;
    while (p->worker(1).tryEnqueueOp(filler)) {}
    ClusterFeatureLevelFanout f;
    f.post(5);
    (void)f.pump(p.get(), &admin);
    EXPECT_TRUE(f.pendingForTest()) << "worker 1 has not got it yet";
    p->worker(1).doWork(1);
    drainAll(*p);
    (void)f.pump(p.get(), &admin);
    EXPECT_FALSE(f.pendingForTest());
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).clusterFeatureLevelForTest(), 5) << "worker " << w;
    int seen = 0;
    admin.drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) { ++seen; }, 8);
    EXPECT_EQ(seen, 1) << "the admin ring got exactly one frame (a reached destination is never re-sent)";
}

TEST(CheckedWritesFeatureLevel, NewerLevelSupersedesUndelivered) {
    auto p = poolOf(2);
    OneToOneByteRing admin(kControlToAdminRingBytes);
    ClusterFeatureLevelFanout f;
    f.post(4);
    f.post(5);
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).clusterFeatureLevelForTest(), 5);
    EXPECT_EQ(p->worker(0).readLeasesSeenForTest(), 0u);
    int seen = 0;
    admin.drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t) { ++seen; }, 8);
    EXPECT_EQ(seen, 1);
}

TEST(CheckedWritesFeatureLevel, AdminKeepsTheMaximum) {
    rune::node::test::CatalogDdlRig rig("cwlevel");
    EXPECT_EQ(rig.admin().clusterFeatureLevel(), mf::kBase);
    rig.admin().setClusterFeatureLevel(5);
    rig.admin().setClusterFeatureLevel(4);
    EXPECT_EQ(rig.admin().clusterFeatureLevel(), 5);
}
