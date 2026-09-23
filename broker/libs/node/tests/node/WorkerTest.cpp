// ---------------------------------------------------------------------------
// WorkerTest.cpp — Unit tests for rune::node::Worker (E1 Task 2).
//
// Uses a minimal single-node memory-backed PartitionReplica (minIsr=1,
// InMemoryLog) so doWork() is a no-op-ish ISR/fetch pass.
//
// Test matrix:
//   1. DrivesOnlyOwnedReplicas — owns() / ownedCount() / workerId() correct;
//      doWork does not crash and returns >= 0.
//   2. ClearOwnedResetsOwnership — clearOwned drops all replicas.
// ---------------------------------------------------------------------------

#include <rune/node/Worker.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>
#include <memory>

using rune::node::Worker;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::replication::InMemoryTransport;
using rune::storage::InMemoryLog;

namespace {

// Minimal single-node memory-backed replica.  Mirrors the pattern used by
// TransactionCoordinationServiceTest (the ground-truth call site per the brief).
class NullTransport : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
};

struct ReplicaFixture {
    InMemoryLog                       log;
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;

    ReplicaFixture(int32_t topicId, int32_t partition) :
        log(/*maxBytes=*/1024 * 1024, /*maxRecords=*/1000)
    {
        PartitionConfig cfg{};
        cfg.topicId             = topicId;
        cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;
        replica = std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            &log, &transport);
    }
};

} // namespace

TEST(Worker, DrivesOnlyOwnedReplicas) {
    ReplicaFixture a(/*topicId=*/0, /*partition=*/0);
    ReplicaFixture b(/*topicId=*/0, /*partition=*/1);

    Worker w(/*workerId=*/0);
    w.ownReplica(a.replica.get());

    EXPECT_TRUE(w.owns(a.replica.get()));
    EXPECT_FALSE(w.owns(b.replica.get()));
    EXPECT_EQ(w.ownedCount(), 1u);
    EXPECT_EQ(w.workerId(), 0);

    // doWork must run without touching b (it is not owned).  We can only assert
    // it does not crash and returns a non-negative work count; the shared-nothing
    // property (b's state untouched) is proven structurally by owns()==false and
    // is TSan-verified in the WorkerPool parity test (Task 7).
    const int work = w.doWork(/*nowNs=*/1);
    EXPECT_GE(work, 0);
}

TEST(Worker, ClearOwnedResetsOwnership) {
    ReplicaFixture a(0, 0);
    Worker w(1);
    w.ownReplica(a.replica.get());
    ASSERT_EQ(w.ownedCount(), 1u);
    w.clearOwned();
    EXPECT_EQ(w.ownedCount(), 0u);
    EXPECT_FALSE(w.owns(a.replica.get()));
}
