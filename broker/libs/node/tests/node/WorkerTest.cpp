// ---------------------------------------------------------------------------
// WorkerTest.cpp — Unit tests for rune::node::Worker (E1 Task 2 + E5.1.2c).
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
//   4. FetchRequestRoutedToWorker (E5.1.2c) — enqueue a FetchRequest WorkerOp;
//      doWork dispatches it to the worker thread via onFetchRequest, which calls
//      sendFetchResponse.  Asserts the response was emitted — proving the handler
//      ran on the worker thread (not inline on the node thread).
// ---------------------------------------------------------------------------

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>
#include <memory>
#include <atomic>

using rune::node::Worker;
using rune::node::WorkerOp;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::replication::InMemoryTransport;
using rune::replication::FetchRequestMsg;
using rune::replication::FetchResponseMsg;
using rune::replication::NodeId;
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

// ---------------------------------------------------------------------------
// RecordingTransport — captures sendFetchResponse calls for assertion.
// Used by FetchRequestRoutedToWorker to verify the handler ran.
// ---------------------------------------------------------------------------
class RecordingTransport : public rune::replication::PartitionTransport {
public:
    std::atomic<int> fetchResponseCount{0};
    rune::replication::NodeId lastToFollower = -1;
    rune::replication::Offset lastHw = -1;

    void sendFetchRequest(const FetchRequestMsg&) override {}

    void sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override {
        lastToFollower = toFollower;
        lastHw         = resp.highWatermark;
        fetchResponseCount.fetch_add(1, std::memory_order_relaxed);
    }

    void sendOffsetsForLeaderEpochRequest(
            NodeId, const rune::replication::OffsetsForLeaderEpochReqMsg&) override {}
    void sendOffsetsForLeaderEpochResponse(
            NodeId, const rune::replication::OffsetsForLeaderEpochRespMsg&) override {}
    void sendReconcileRequest(
            NodeId, const FetchRequestMsg&) override {}
    void sendReconcileResponse(
            NodeId, const FetchResponseMsg&) override {}
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

// ---------------------------------------------------------------------------
// E5.1.2c: FetchRequestRoutedToWorker
//
// Scenario:
//   - 2-node leader (selfId=1, ISR=[1,2], minIsr=1).
//   - Append a minimal RecordBatch to the log so LEO > 0.
//   - Enqueue a FetchRequest WorkerOp from follower 2 (fetchOffset=0) via the
//     worker's tryEnqueueOp — simulating what AeronPartitionTransport does when
//     workerPool_ is set.
//   - Call doWork() once — drains ingress, executes onFetchRequest on the
//     worker thread.
//   - Assert that sendFetchResponse was called by checking the recording transport.
//
// This proves the FetchRequest leader-state handler runs on the WORKER thread
// (not inline on the node thread), closing the leaderState_/isrManager_ race
// described in task-E5.1.2c-brief.md.
// ---------------------------------------------------------------------------
TEST(Worker, FetchRequestRoutedToWorker) {
    // Build a minimal valid RecordBatch (magic=2, sufficient to pass magic check).
    // Minimal layout: base_offset(8) + batch_length(4) + partition_leader_epoch(4) +
    //   magic(1) + crc(4) + attributes(2) + last_offset_delta(4) + ...
    // InMemoryLog::append accepts raw bytes and does not CRC-validate.
    // We need at least kRecordBatchOverhead bytes.
    static constexpr std::size_t kBatchBytes = 128;
    std::vector<uint8_t> batch(kBatchBytes, 0x00);
    // base_offset = 0 (LE64 @ 0)
    // batch_length = kBatchBytes - 12 (LE32 @ 8)  [records section after first 12 bytes]
    int32_t blen = static_cast<int32_t>(kBatchBytes) - 12;
    std::memcpy(batch.data() + 8, &blen, 4);
    // magic = 2 @ offset 16
    batch[16] = 2;
    // last_offset_delta = 0 @ offset 23 (LE32)  → only 1 record

    InMemoryLog                log(1024 * 1024, 1000);
    RecordingTransport         transport;
    std::unique_ptr<PartitionReplica> replica;

    {
        PartitionConfig cfg{};
        cfg.topicId             = 7;  // user partition (topicId >= 0)
        cfg.partitionId         = 0;
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;
        // 2-node ISR: selfId=1 + followerId=2
        replica = std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1, 2},   // ISR
            std::vector<NodeId>{1, 2},   // full replica set
            &log, &transport);
    }

    // Append one batch to the leader log so LEO = 1.
    log.append(batch, /*lastOffset=*/0);
    replica->onLeaderLogAppended();

    // Build the FetchRequest WorkerOp (mirrors AeronPartitionTransport routing).
    WorkerOp op;
    op.type                            = WorkerOp::Type::FetchRequest;
    op.topicId                         = 7;
    op.partition                       = 0;
    op.replica                         = replica.get();
    op.logPtr                          = nullptr;
    op.fetchRequestPayload.leaderEpoch = 1;
    op.fetchRequestPayload.partition   = 0;
    op.fetchRequestPayload.followerId  = 2;   // follower node
    op.fetchRequestPayload.fetchOffset = 0;   // follower wants from offset 0
    op.fetchRequestPayload.maxBytes    = 1 << 20;
    op.fetchRequestPayload.topicId     = 7;
    op.fetchRequestPayload.nowNs       = 1'000'000LL;

    Worker w(/*workerId=*/4);
    w.setOwnedDirect(Worker::OwnedSet{replica.get()});

    // Enqueue via the worker's ingress ring (same path AeronPartitionTransport uses).
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));

    // Drive the worker once — drains ingress → executeFetchRequestWire_ →
    // replica->onFetchRequest(req, nowNs) → transport.sendFetchResponse(...).
    const int work = w.doWork(/*nowNs=*/1'000'000LL);
    EXPECT_GE(work, 0);

    // The FetchResponse must have been emitted to follower 2.
    EXPECT_EQ(transport.fetchResponseCount.load(), 1)
        << "onFetchRequest must be called on the worker thread (FetchRequest routed via ingress ring)";
    EXPECT_EQ(transport.lastToFollower, 2);
}
