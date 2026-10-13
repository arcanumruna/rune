// TransientDataPathTest.cpp — Phase 2b E2: the worker data path of a TRANSIENT
// (InMemoryLog) partition — produce over shard ingress and the op ring, push
// reads served from the ring, read_committed, the full-ring rule, the evicted
// cursor, ownership moves.  Deterministic: one worker driven by hand, no Aeron.
#include "TransientRig.h"

using namespace transient_rig;

namespace {
constexpr int16_t kOk               = 0;
constexpr int16_t kNotLeader        = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int16_t kTransientLogFull = static_cast<int16_t>(pc::ErrorCode::TransientLogFull);   // 42 (Task 1)
} // namespace

TEST(TransientDataPathRegression, ShardIngressProduceAppendsToTheRingRegression) {
    Rig rig;
    const auto b = batchOf(3, 100);
    int64_t base = -1;
    ASSERT_EQ(rig.produce(b, /*acks=*/1, &base), kOk) << "a transient produce over shard ingress was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    ASSERT_EQ(rig.produce(b, 1, &base), kOk);
    EXPECT_EQ(base, 3);
    EXPECT_EQ(rig.mem.logEndOffset(), 6);
    EXPECT_EQ(rig.rep->highWatermark(), 6) << "single node: acks=all commits at once";
}

TEST(TransientDataPathRegression, ProducedBatchCarriesTheLeaderEpochRegression) {
    Rig rig;
    ASSERT_EQ(rig.produce(batchOf(2, 50), /*acks=*/0), kOk);
    const auto v = rig.mem.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 0);
    EXPECT_EQ(r.partitionLeaderEpoch(), 1) << "followers build their epoch cache from the stamped epoch (KIP-320)";
    EXPECT_TRUE(r.ensureValid());
}

TEST(TransientDataPathRegression, GatewayRoutedProduceOpAppendsToTheRingRegression) {
    Rig rig;
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = kTopic; op.partition = 0;
    op.logPtr = nullptr;   // the gateway has no disk Log for a transient partition (legacy mode)
    op.sessionId = 1; op.correlationId = 77;
    op.lastOffsetDelta = 2; op.recordCount = 3; op.durabilityTier = 1;
    op.recordBatchBytes = batchOf(3, 100);
    ASSERT_TRUE(rig.pool.tryEnqueueOp(kTopic, 0, std::move(op)));
    int16_t code = -1;
    int64_t base = -1;
    for (int i = 0; i < 64 && code == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == 77) { code = r.errorCode; base = r.baseOffset; }
        });
    }
    ASSERT_EQ(code, kOk) << "a gateway-routed (legacy mode) transient produce was refused (was: Internal)";
    EXPECT_EQ(base, 0);
    EXPECT_EQ(rig.mem.logEndOffset(), 3);
}

TEST(TransientDataPathRegression, FullRingRefusesRetriablyAndNeverEvictsUncommittedRegression) {
    Rig rig(/*ringBytes=*/16 * 1024, /*maxBatches=*/1000, /*withFollower=*/true);   // ISR {1,2}: HW waits for 2
    const auto b = batchOf(1, 1000);
    int ok = 0;
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) {
        code = rig.produce(b, /*acks=*/0);
        if (code == kOk) ++ok;
    }
    ASSERT_EQ(code, kTransientLogFull) << "a full ring must refuse (42, retriable), not evict unreplicated data";
    EXPECT_GT(ok, 0);
    EXPECT_EQ(rig.mem.logStartOffset(), 0) << "an unreplicated record was evicted";
    EXPECT_GE(rig.mem.fullRefusals(), 1u);
    EXPECT_GE(rig.worker().transientFullRefusalsForTest(), 1u);
    const int64_t leo = rig.mem.logEndOffset();
    EXPECT_EQ(leo, ok) << "a refused produce appended nothing";

    rig.fetch(leo);   // follower 2 now holds everything: the HW reaches the LEO
    for (int i = 0; i < 8; ++i) rig.pass();
    ASSERT_EQ(rig.rep->highWatermark(), leo);
    EXPECT_EQ(rig.produce(b, 0), kOk) << "committed records make room";
    EXPECT_GT(rig.mem.logStartOffset(), 0);
}

TEST(TransientDataPathRegression, FullRingMarkerIsRefusedRetriablyRegression) {
    // Full by the batch-count cap (8 slots), not by bytes: a byte-full ring can
    // still hold a ~80-byte marker in its slack (the marker then lands and parks
    // on the HW — correct, but not the refusal this test pins).
    Rig rig(/*ringBytes=*/1u << 20, /*maxBatches=*/8, /*withFollower=*/true);
    int16_t code = kOk;
    for (int i = 0; i < 64 && code == kOk; ++i) code = rig.produce(batchOf(1, 100), 0);
    ASSERT_EQ(code, kTransientLogFull) << "precondition: the ring is full of uncommitted data";
    const int64_t leo = rig.mem.logEndOffset();
    const uint64_t refusedBefore = rig.worker().transientFullRefusalsForTest();
    rig.marker(/*token=*/5, /*pid=*/9, /*commit=*/true);
    int16_t mcode = -1;
    for (int i = 0; i < 64 && mcode == -1; ++i) {
        rig.pass();
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::ControlRecordAppended && r.correlationId == 5) mcode = r.errorCode;
        });
    }
    EXPECT_EQ(mcode, kTransientLogFull) << "the coordinator must retry a refused marker (was: Internal — dropped)";
    EXPECT_EQ(rig.mem.logEndOffset(), leo) << "a refused marker appended nothing";
    EXPECT_EQ(rig.worker().transientFullRefusalsForTest(), refusedBefore + 1);
}

// C1 at the worker: a transient topic whose catalog record has not committed has
// NO replica on any worker (BrokerNode builds it on kA2cTopicCommitted), so a
// shard-ingress produce for it is "not owned" — retriable NotLeader, nothing
// appended anywhere.  Pins that the transient branch (replica->memoryLog())
// never turns "no replica" into an append.
TEST(TransientDataPathRegression, UnregisteredTransientPartitionIsNotLeaderRegression) {
    Rig rig;
    rig.worker().setOwnedDirect({});   // the partition is not (yet) a replica of this worker
    const auto b = batchOf(1, 50);
    EXPECT_EQ(rig.produce(b, /*acks=*/1), kNotLeader);
    EXPECT_EQ(rig.mem.logEndOffset(), 0) << "an uncommitted / unowned transient partition was appended to";
}
