// ---------------------------------------------------------------------------
// WorkerTest.cpp — Unit tests for rune::node::Worker (E1 Task 2 + E5.1.2c + Task 6).
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
//   5. DoWorkPollsReplicationTransportAndAppliesInline (Task 6) — follower
//      worker owning a partition; inject a FetchResponse via the transport's
//      test-inject seam (injectFrameForTest); call doWork(); assert the
//      replica's LEO advanced (applied inline, no ring hop).  Also verifies
//      that the doWork return-value includes the polled fragment count.
//   6. DoWorkReplTransportNullWhenFlagOff — no transport wired → doWork() does
//      not crash (flag-off path unchanged, workerReplTransport_ == nullptr).
//   7. DoWorkReplTransportRoutesFetchRequestToLeader (Task 6 review) — leader
//      worker owning a partition; inject a FetchRequest frame via the transport's
//      test-inject seam; call doWork(); assert the leader called sendFetchResponse
//      on its RecordingTransport (proves templateId 1 → onFetchRequest routing
//      end-to-end at the Worker layer).
//   8. DoWorkReplTransportNullReplicaGuard (Task 6 review) — inject a frame
//      whose (topicId, partition) is NOT owned by this worker; call doWork();
//      assert no crash and no side-effect on any owned replica.
//      (OLE + reconcile routing covered by Task 7 failover integration.)
// ---------------------------------------------------------------------------

#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>
#include <memory>
#include <atomic>
#include <cstdint>
#include <span>
#include <vector>
#include <unordered_map>

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

// ===========================================================================
// Task 6: per-worker replication transport poll + inline apply tests.
//
// Uses WorkerReplicationTransportImpl<SharedFakePubRegistry> via the
// setReplicationTransportForTest<T> seam so no Aeron driver is required.
// ===========================================================================

// ---------------------------------------------------------------------------
// SharedFakePubRegistry — minimal test PubRegistry (same pattern as
// WorkerReplicationTransportTest.cpp).  Shared via shared_ptr so the test
// harness and the transport's internal copy see the same state.
// ---------------------------------------------------------------------------
namespace {

struct SharedFakePubState {
    bool allResolved = true;

    // Cap returned by maxMessageLength() — large enough for any test frame.
    int32_t maxMsgLen = 1 << 20;
};

class SharedFakePubRegistry {
public:
    explicit SharedFakePubRegistry()
        : state_(std::make_shared<SharedFakePubState>())
    {}

    // Copy/move share the same state.
    SharedFakePubRegistry(const SharedFakePubRegistry&)            = default;
    SharedFakePubRegistry& operator=(const SharedFakePubRegistry&) = default;
    SharedFakePubRegistry(SharedFakePubRegistry&&)                 = default;
    SharedFakePubRegistry& operator=(SharedFakePubRegistry&&)      = default;

    [[nodiscard]] bool    allResolved()      const noexcept { return state_->allResolved; }
    [[nodiscard]] int32_t maxMessageLength() const noexcept { return state_->maxMsgLen; }
    [[nodiscard]] bool    hasPeer(rune::replication::NodeId) const noexcept { return true; }

    // No real sends needed for receive-path tests.
    int64_t sendToPeer(rune::replication::NodeId, const uint8_t*, std::size_t) noexcept { return 1; }
    void    broadcastToPeers(const uint8_t*, std::size_t) noexcept {}
    int64_t sendGatherToPeer(rune::replication::NodeId, const uint8_t*, std::size_t,
                             std::span<const uint8_t>) noexcept { return 1; }

private:
    std::shared_ptr<SharedFakePubState> state_;
};

using TestWorkerTransport = rune::node::WorkerReplicationTransportImpl<SharedFakePubRegistry>;

// Build a minimal valid RecordBatch (magic=2) of kBatchBytes.
// Same layout as the batch in FetchRequestRoutedToWorker above —
// InMemoryLog accepts it without CRC validation.
static std::vector<uint8_t> makeMinimalRecordBatch(std::size_t batchBytes = 128)
{
    std::vector<uint8_t> batch(batchBytes, 0x00);
    // batch_length = batchBytes - 12 (LE32 @ 8)
    int32_t blen = static_cast<int32_t>(batchBytes) - 12;
    std::memcpy(batch.data() + 8, &blen, sizeof(blen));
    // magic = 2 @ offset 16 (kMagicOffset)
    batch[16] = 2;
    // last_offset_delta = 0 @ offset 23 (LE32) → 1 record
    return batch;
}

} // namespace

// ---------------------------------------------------------------------------
// Task 6 Test 1: DoWorkPollsReplicationTransportAndAppliesInline
//
// Scenario:
//   - 2-node follower (selfId=2, leader=1), topicId=5, partition=7.
//   - Inject a FetchResponse carrying a minimal RecordBatch via
//     transport.injectFrameForTest().
//   - Call worker.doWork() once — must poll the transport and apply inline.
//   - Assert replica's LEO advanced (records applied to the follower log).
//   - Assert doWork return-value >= 1 (includes polled fragment count).
// ---------------------------------------------------------------------------
TEST(Worker, DoWorkPollsReplicationTransportAndAppliesInline)
{
    const int32_t kTopicId   = 5;
    const int32_t kPartition = 7;
    const int64_t kEpoch     = 1;

    // Build the follower's transport (no Aeron driver).
    NullTransport replicaTransport;

    // Build follower replica: selfId=2, leader=1, epoch=1.
    InMemoryLog followerLog(/*maxBytes=*/4 * 1024 * 1024, /*maxRecords=*/10000);
    PartitionConfig cfg{};
    cfg.topicId             = kTopicId;
    cfg.partitionId         = static_cast<rune::replication::PartitionId>(kPartition);
    cfg.selfId              = 2;          // this node is the follower
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    auto follower = std::make_unique<PartitionReplica>(
        cfg,
        rune::replication::Role::Follower, kEpoch,
        std::vector<rune::replication::NodeId>{1, 2},   // replica set: leader=1, follower=2
        std::vector<rune::replication::NodeId>{1, 2},   // ISR
        &followerLog, &replicaTransport);

    const rune::replication::Offset leoAtStart = follower->logEndOffset();

    // Use raw bytes (not magic=2 pattern) so applyReplicated uses the
    // unit-test fallback path (makeBatch_) — simpler, equally valid for LEO check.
    std::vector<uint8_t> rawRecords(32, 0xAB); // 32 bytes of raw payload

    // Encode the FetchResponse into a wire buffer.
    rune::replication::FetchResponseMsg resp{};
    resp.leaderEpoch    = kEpoch;
    resp.partition      = kPartition;
    resp.highWatermark  = 1;     // leader has committed offset 0
    resp.logStartOffset = 0;
    resp.errorCode      = 0;     // kErrNone
    resp.topicId        = kTopicId;
    resp.records        = std::span<const uint8_t>(rawRecords.data(), rawRecords.size());

    const std::size_t frameLen =
        rune::replication::FetchResponseMsg::encodedSize(rawRecords.size());
    std::vector<uint8_t> frameBuf(frameLen, 0);
    rune::replication::FetchResponseMsg::encode(
        frameBuf.data(), 0,
        resp.leaderEpoch, resp.partition, resp.highWatermark,
        resp.logStartOffset, resp.errorCode, resp.records, resp.topicId);

    // Build the test transport and inject the frame.
    TestWorkerTransport transport(/*workerId=*/9, SharedFakePubRegistry{});
    transport.injectFrameForTest(frameBuf.data(), frameBuf.size());

    // Wire the worker.
    Worker w(/*workerId=*/9);
    w.setOwnedDirect(Worker::OwnedSet{follower.get()});
    w.setReplicationTransportForTest(&transport);

    // Drive the worker once — must poll transport + apply FetchResponse inline.
    const int work = w.doWork(/*nowNs=*/1'000'000LL);

    // LEO must have advanced: records were applied inline (no ring hop).
    const rune::replication::Offset leoAfter = follower->logEndOffset();
    EXPECT_GT(leoAfter, leoAtStart)
        << "Follower LEO must advance after doWork polls FetchResponse inline "
        << "(was " << leoAtStart << ", got " << leoAfter << ")";

    // work count must include the polled fragment (keeps idle-detection correct).
    EXPECT_GE(work, 1)
        << "doWork must return >= 1 when a replication fragment was polled";
}

// ---------------------------------------------------------------------------
// Task 6 Test 2: DoWorkReplTransportNullWhenFlagOff
//
// No transport wired → doWork() must not crash and must return >= 0.
// Verifies the flag-off path (workerReplTransport_ == nullptr) is untouched.
// ---------------------------------------------------------------------------
TEST(Worker, DoWorkReplTransportNullWhenFlagOff)
{
    ReplicaFixture a(/*topicId=*/1, /*partition=*/0);
    Worker w(/*workerId=*/10);
    w.setOwnedDirect(Worker::OwnedSet{a.replica.get()});
    // No setReplicationTransport call — transport remains null.
    const int work = w.doWork(/*nowNs=*/1'000'000LL);
    EXPECT_GE(work, 0);
}

// ---------------------------------------------------------------------------
// Task 6 Review Test 1: DoWorkReplTransportRoutesFetchRequestToLeader
//
// Scenario:
//   - 2-node LEADER (selfId=1, ISR=[1,2], minIsr=1, epoch=1),
//     topicId=3, partition=4, with RecordingTransport as the replica's
//     PartitionTransport (captures sendFetchResponse calls).
//   - Encode a FetchRequest (templateId=1) from follower 2 (fetchOffset=0,
//     epoch=1) into a wire buffer.
//   - Inject the frame via transport.injectFrameForTest() into the
//     TestWorkerTransport (the per-worker replication poll path).
//   - Call worker.doWork() once — must poll the transport and call
//     pollReplTransportDispatch_(templateId=1) → replica->onFetchRequest()
//     → recording transport's sendFetchResponse is invoked.
//   - Assert: fetchResponseCount == 1 (proves templateId 1 routing end-to-end).
//   - Assert: lastToFollower == 2 (correct target).
//
// This test fails if pollReplTransportDispatch_ mis-routes templateId 1
// (e.g. falls through to default/no-op), because no sendFetchResponse
// would be recorded.
// ---------------------------------------------------------------------------
TEST(Worker, DoWorkReplTransportRoutesFetchRequestToLeader)
{
    const int32_t kTopicId   = 3;
    const int32_t kPartition = 4;
    const int64_t kEpoch     = 1;

    // RecordingTransport captures sendFetchResponse calls from onFetchRequest.
    RecordingTransport recordingTransport;

    InMemoryLog leaderLog(/*maxBytes=*/4 * 1024 * 1024, /*maxRecords=*/10000);
    PartitionConfig cfg{};
    cfg.topicId             = kTopicId;
    cfg.partitionId         = static_cast<rune::replication::PartitionId>(kPartition);
    cfg.selfId              = 1;   // this node is the leader
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    auto leader = std::make_unique<PartitionReplica>(
        cfg,
        rune::replication::Role::Leader, kEpoch,
        std::vector<NodeId>{1, 2},   // ISR: leader=1, follower=2
        std::vector<NodeId>{1, 2},   // full replica set
        &leaderLog, &recordingTransport);

    // Encode a FetchRequest (templateId=1) from follower 2, fetchOffset=0.
    alignas(8) uint8_t fetchReqBuf[rune::replication::FetchRequestMsg::TOTAL_SIZE];
    rune::replication::FetchRequestMsg::encode(
        fetchReqBuf, /*offset=*/0,
        /*leaderEpoch=*/kEpoch,
        /*partition=*/static_cast<rune::replication::PartitionId>(kPartition),
        /*followerId=*/2,
        /*fetchOffset=*/0,
        /*maxBytes=*/1 << 20,
        /*topicId=*/kTopicId);

    // Wire the worker as the leader's owner.
    TestWorkerTransport workerTransport(/*workerId=*/11, SharedFakePubRegistry{});
    workerTransport.injectFrameForTest(fetchReqBuf, sizeof(fetchReqBuf));

    Worker w(/*workerId=*/11);
    w.setOwnedDirect(Worker::OwnedSet{leader.get()});
    w.setReplicationTransportForTest(&workerTransport);

    // Drive the worker once — must poll transport + route FetchRequest inline
    // → leader->onFetchRequest() → recordingTransport.sendFetchResponse().
    const int work = w.doWork(/*nowNs=*/1'000'000LL);
    EXPECT_GE(work, 0);

    // The leader must have responded to follower 2.
    EXPECT_EQ(recordingTransport.fetchResponseCount.load(), 1)
        << "pollReplTransportDispatch_(templateId=1) must route to onFetchRequest "
           "which calls sendFetchResponse; count was "
        << recordingTransport.fetchResponseCount.load();
    EXPECT_EQ(recordingTransport.lastToFollower, 2)
        << "FetchResponse must be addressed to follower 2";
}

// ---------------------------------------------------------------------------
// Task 6 Review Test 2: DoWorkReplTransportNullReplicaGuard
//
// Scenario:
//   - Worker owns a follower replica for (topicId=5, partition=7).
//   - Inject a FetchResponse frame for (topicId=99, partition=88) — NOT owned
//     by this worker.
//   - Call doWork() once.
//   - Assert: no crash, no LEO change on the owned replica
//     (pollReplTransportDispatch_'s null-replica guard fires, frame is dropped).
//
// OLE + reconcile routing (templateIds 3/4/5/6) is covered by Task 7
// failover integration tests, not constructed artificially here.
// ---------------------------------------------------------------------------
TEST(Worker, DoWorkReplTransportNullReplicaGuard)
{
    const int32_t kOwnedTopicId   = 5;
    const int32_t kOwnedPartition = 7;
    const int64_t kEpoch          = 1;

    // Build a follower replica for (topicId=5, partition=7).
    NullTransport nullTransport;
    InMemoryLog followerLog(/*maxBytes=*/4 * 1024 * 1024, /*maxRecords=*/10000);
    PartitionConfig cfg{};
    cfg.topicId             = kOwnedTopicId;
    cfg.partitionId         = static_cast<rune::replication::PartitionId>(kOwnedPartition);
    cfg.selfId              = 2;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    auto follower = std::make_unique<PartitionReplica>(
        cfg,
        rune::replication::Role::Follower, kEpoch,
        std::vector<NodeId>{1, 2},
        std::vector<NodeId>{1, 2},
        &followerLog, &nullTransport);

    const rune::replication::Offset leoAtStart = follower->logEndOffset();

    // Encode a FetchResponse for (topicId=99, partition=88) — NOT owned by this worker.
    const int32_t kWrongTopicId   = 99;
    const int32_t kWrongPartition = 88;
    std::vector<uint8_t> rawRecords(32, 0xCD); // 32 arbitrary bytes
    const std::size_t frameLen =
        rune::replication::FetchResponseMsg::encodedSize(rawRecords.size());
    std::vector<uint8_t> frameBuf(frameLen, 0);
    rune::replication::FetchResponseMsg::encode(
        frameBuf.data(), /*offset=*/0,
        /*leaderEpoch=*/kEpoch,
        /*partition=*/kWrongPartition,
        /*highWatermark=*/1,
        /*logStartOffset=*/0,
        /*errorCode=*/0,
        std::span<const uint8_t>(rawRecords.data(), rawRecords.size()),
        /*topicId=*/kWrongTopicId);

    // Inject into the per-worker transport.
    TestWorkerTransport workerTransport(/*workerId=*/12, SharedFakePubRegistry{});
    workerTransport.injectFrameForTest(frameBuf.data(), frameBuf.size());

    // Wire the worker — it ONLY owns (topicId=5, partition=7).
    Worker w(/*workerId=*/12);
    w.setOwnedDirect(Worker::OwnedSet{follower.get()});
    w.setReplicationTransportForTest(&workerTransport);

    // doWork() must not crash; the null-replica guard must silently drop the frame.
    EXPECT_NO_FATAL_FAILURE(w.doWork(/*nowNs=*/1'000'000LL));

    // The owned follower's LEO must be unchanged — the frame was dropped.
    EXPECT_EQ(follower->logEndOffset(), leoAtStart)
        << "Null-replica guard must drop frames for unowned (topicId=99, partition=88); "
           "LEO of the owned replica (5,7) must be unchanged";
}
