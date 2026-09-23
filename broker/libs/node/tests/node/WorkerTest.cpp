// ---------------------------------------------------------------------------
// WorkerTest.cpp — Unit tests for rune::node::Worker (E1 Task 2).
//
// Uses a minimal single-node memory-backed PartitionReplica (minIsr=1,
// InMemoryLog) so doWork() is a no-op-ish ISR/fetch pass.
//
// Test matrix:
//   1. DrivesOnlyOwnedReplicas — owns() / ownedCount() / workerId() correct;
//      doWork does not crash and returns >= 0.
//   2. PublishOwnedResetsOwnership — publishOwned with empty set drops all replicas.
//   3. SetCoreIdAndDoWorkNocrash (E4) — setCoreId + doWork does not crash;
//      on macOS the pin is a no-op and doWork still returns >= 0.
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
    w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});

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

TEST(Worker, PublishOwnedResetsOwnership) {
    ReplicaFixture a(0, 0);
    Worker w(1);
    w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});
    ASSERT_EQ(w.ownedCount(), 1u);
    // Publishing an empty set clears ownership (empty, not null).
    w.setOwnedDirect(Worker::OwnedSet{});
    EXPECT_EQ(w.ownedCount(), 0u);
    EXPECT_FALSE(w.owns(a.replica.get()));
}

// ---------------------------------------------------------------------------
// E4: CPU affinity — setCoreId + doWork does not crash.
//
// On macOS the pin is a deliberate no-op (pinCurrentThreadToCore_ returns false).
// On Linux the pin fires; the actual core-migration assertion is validated on
// Hetzner separately.  This test only asserts the function returns >= 0 and does
// not throw or crash on either platform.
//
// We call setCoreId with a real core id (0) to exercise the code path that
// enters the #if defined(__linux__) branch on Linux and the no-op on macOS.
// We also verify the default (coreId_=-1) leaves doWork unaffected.
// ---------------------------------------------------------------------------
TEST(Worker, SetCoreIdAndDoWorkNoCrash) {
    ReplicaFixture a(/*topicId=*/0, /*partition=*/0);

    // Case 1: default (no affinity) — doWork returns >= 0 unaffected.
    {
        Worker w(/*workerId=*/2);
        w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});
        const int work = w.doWork(/*nowNs=*/1);
        EXPECT_GE(work, 0);
    }

    // Case 2: setCoreId(0) — self-pin attempt on first doWork.
    // On macOS: no-op (returns false), spdlog warn; doWork still returns >= 0.
    // On Linux: actually pins; doWork still returns >= 0.
    // Either way: no crash, no exception.
    {
        Worker w(/*workerId=*/3);
        w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});
        w.setCoreId(0);  // Set before "runner starts" (single-threaded test)
        const int work = w.doWork(/*nowNs=*/2);
        EXPECT_GE(work, 0);

        // Second doWork call must not re-attempt the pin (pinned_ == true).
        const int work2 = w.doWork(/*nowNs=*/3);
        EXPECT_GE(work2, 0);
    }
}
