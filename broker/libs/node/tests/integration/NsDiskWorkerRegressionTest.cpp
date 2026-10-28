// NsDiskWorkerRegressionTest.cpp - namespaces Phase 2 Task 11 (D25): a namespace's disk limit at the owning WORKER, driven
// deterministically (the test thread is the worker; HotPathBRig, no Aeron, no sleeps).
//
// Interaction matrix (limit x {below, at, above} x role / path):
//   | limit            | operation                 | state                      | expected                                         | test
//   |------------------|---------------------------|----------------------------|--------------------------------------------------|-----
//   | max-disk-bytes L | leader produce            | usage < L, crossing, >= L  | ok, ok (usage >= L after), refused 52, no append | LimitEnforcementTableRegression.DiskRegression
//   | max-disk-bytes   | leader produce            | usage >= L, then limit up  | refused, then served after the next scan         | OverDiskRegression.RaisedLimitResumesProduceRegression
//   | max-disk-bytes   | follower ApplyReplicated  | the slot is over           | every batch applied, never refused               | OverDiskRegression.FollowerApplyNeverRefusedRegression
//   | max-disk-bytes   | the per-slot sum          | after appends / scan       | == the logs' bytesOnDisk()                       | OverDiskRegression.ScanEqualsLogBytesRegression
#include "HotPathBRig.h"

#include <rune/node/NsLimitAggregator.h>
#include <rune/protocol/NamespaceLimits.h>

namespace {

// Task 14 (D25, D28): the over-disk byte is no longer decided on the worker - the control thread's NsLimitAggregator sums the
// workers' per-slot bytes and broadcasts Throttle{Disk}.  This rig closes the same loop deterministically with the REAL
// aggregator: the worker's usage report (a SessionSignal on its return ring) -> report() -> one tick -> the Throttle applied
// to the worker exactly as the ring would.  settle() runs after every pass whose answer is read (see answerOf).
struct AggLoop {
    rune::node::NsLimitAggregator agg;
    int64_t now = 1;
    bool configured = false;
    static constexpr int64_t kTick = 10'000'000;
    void ensure() {
        if (configured) return;
        agg.configure(8, 1, kTick);
        (void)agg.doWork(now);   // baseline
        configured = true;
    }
};
AggLoop g_loop;


constexpr uint32_t kNs   = 1;
constexpr uint16_t kSlot = 1;
constexpr uint64_t kLimit = 8192;

void assignLimits(Rig& rig, const rune::protocol::NamespaceLimits& l) {
    g_loop = AggLoop{};
    g_loop.ensure();
    g_loop.agg.onSlotAssign(kSlot, kNs, l);
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
}
void relimit(Rig& rig, const rune::protocol::NamespaceLimits& l) {   // SetLimits: same owner, new limits
    g_loop.agg.onSlotAssign(kSlot, kNs, l);
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
}

/// Assign the namespace to the worker's slot table with `limit`, tag the log, and let the worker take a first scan.
void tagNamespace(Rig& rig, uint64_t limit) {
    rig.log->setNsTag(kNs, kSlot);
    rune::protocol::NamespaceLimits l;
    l.maxDiskBytes = limit;
    assignLimits(rig, l);
}

/// The ErrorCode of the single produce answer the pass produced (-1: none).  Also closes the aggregator loop (above).
int answerOf(Rig& rig) {
    int code = -1;
    (void)rig.worker().reportNsUsageForTest();
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) code = r.errorCode;
        else if (r.type == WorkerReturn::Type::SessionSignal &&
                 r.lastOffset == static_cast<int64_t>(rune::node::SessionSignalKind::NsUsage))
            (void)g_loop.agg.report(static_cast<uint32_t>(r.partition), static_cast<uint16_t>(r.sessionId >> 8),
                                    static_cast<uint32_t>(r.correlationId), static_cast<rune::node::NsMetric>(r.sessionId & 0xFF),
                                    static_cast<uint64_t>(r.baseOffset));
    });
    g_loop.now += AggLoop::kTick;
    if (g_loop.agg.doWork(g_loop.now) > 0)
        g_loop.agg.drain([&](const rune::node::Throttle& t) { rig.worker().applyNsThrottleDirect(t.slot, t.nsId, t.kind, t.untilNs); },
                         [&](const rune::node::ViewMemOver&) {});
    return code;
}
/// Settle without a produce answer (after a scan or a limit change).
void settle(Rig& rig) { (void)answerOf(rig); }

void applyOne(Rig& rig, int64_t leaderEpoch) {
    const int64_t leo = rig.log->logEndOffset();
    rune::protocol::RecordBatchBuilder b(leo, 1000);
    for (int k = 0; k < 2; ++k) b.append("k", std::string(300, 'x'), 1000 + k);
    auto v = b.build();
    rune::protocol::RecordBatchReader::stampLeaderEpoch(v.data(), static_cast<int32_t>(leaderEpoch));
    WorkerOp op;
    op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 1; op.partition = 0;
    op.replicatedPayload.leaderEpoch = leaderEpoch;
    op.replicatedPayload.partition = 0;
    op.replicatedPayload.topicId = 1;
    op.replicatedPayload.highWatermark = leo + 2;
    op.replicatedPayload.records.assign(v.begin(), v.end());
    ASSERT_TRUE(rig.pool.tryEnqueueOp(1, 0, std::move(op)));
    rig.pass();
}

}  // namespace

TEST(LimitEnforcementTableRegression, DiskRegression) {
    Rig rig;
    tagNamespace(rig, kLimit);
    const auto frame = produceFrame(1, 1, 0, 1000, 1);   // ~1.1 KB per batch
    int below = 0, at = 0, above = 0;
    int64_t endBeforeRefusal = -1;
    for (int i = 0; i < 40; ++i) {
        const int64_t usageBefore = rig.worker().nsDiskBytesForTest(kSlot);
        const int64_t endBefore = rig.log->logEndOffset();
        rig.pass(&frame);
        const int code = answerOf(rig);
        if (static_cast<uint64_t>(usageBefore) < kLimit) {
            ASSERT_EQ(code, 0) << "served while usage (" << usageBefore << ") is under the limit";
            if (static_cast<uint64_t>(rig.worker().nsDiskBytesForTest(kSlot)) >= kLimit) ++at; else ++below;
        } else {
            EXPECT_EQ(code, static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded)) << "refused once usage >= the limit";
            EXPECT_EQ(rig.log->logEndOffset(), endBefore) << "a refused produce appends nothing";
            endBeforeRefusal = endBefore;
            ++above;
        }
    }
    EXPECT_GT(below, 0) << "Below";
    EXPECT_EQ(at, 1) << "At: exactly the produce that brings usage to the limit is served";
    EXPECT_GT(above, 5) << "Above: every later produce is refused";
    EXPECT_EQ(rig.log->logEndOffset(), endBeforeRefusal);
    EXPECT_EQ(rig.worker().overDiskRefusalsForTest(), static_cast<uint64_t>(above));
    EXPECT_TRUE(rig.worker().nsOverDiskForTest(kSlot));
}

TEST(OverDiskRegression, RaisedLimitResumesProduceRegression) {
    Rig rig;
    tagNamespace(rig, kLimit);
    const auto frame = produceFrame(1, 1, 0, 1000, 1);
    for (int i = 0; i < 20; ++i) { rig.pass(&frame); (void)answerOf(rig); }
    ASSERT_TRUE(rig.worker().nsOverDiskForTest(kSlot));
    rig.pass(&frame);
    ASSERT_EQ(answerOf(rig), static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded));
    // The limit is raised (a SetLimits reaches the aggregator and the worker): the next tick clears the byte.
    rune::protocol::NamespaceLimits l;
    l.maxDiskBytes = 1 << 20;
    relimit(rig, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    settle(rig);
    EXPECT_FALSE(rig.worker().nsOverDiskForTest(kSlot));
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), 0) << "served again";
    // Lowering below usage refuses again but keeps every byte.
    const int64_t end = rig.log->logEndOffset();
    l.maxDiskBytes = kLimit;
    relimit(rig, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    settle(rig);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded));
    EXPECT_EQ(rig.log->logEndOffset(), end) << "lowering deleted nothing";
    // Limit 0 = unlimited.
    l.maxDiskBytes = 0;
    relimit(rig, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    settle(rig);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), 0);
}

TEST(OverDiskRegression, FollowerApplyNeverRefusedRegression) {
    Rig rig;
    tagNamespace(rig, kLimit);
    const auto frame = produceFrame(1, 1, 0, 1000, 1);
    for (int i = 0; i < 20; ++i) { rig.pass(&frame); (void)answerOf(rig); }
    ASSERT_TRUE(rig.worker().nsOverDiskForTest(kSlot)) << "precondition: the namespace is over its disk limit";
    rig.roleOp(/*leader=*/false, 2);
    rig.pass();
    ASSERT_EQ(rig.rep->role(), Role::Follower);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    settle(rig);
    ASSERT_TRUE(rig.worker().nsOverDiskForTest(kSlot)) << "still over on the follower";
    const uint64_t refusals = rig.worker().overDiskRefusalsForTest();
    for (int i = 0; i < 12; ++i) {
        const int64_t before = rig.log->logEndOffset();
        applyOne(rig, 2);
        EXPECT_EQ(rig.log->logEndOffset(), before + 2) << "replicated batch " << i << " was refused while over the disk limit";
    }
    EXPECT_EQ(rig.worker().overDiskRefusalsForTest(), refusals) << "a follower apply never counts as a refusal";
    EXPECT_TRUE(rig.worker().nsOverDiskForTest(kSlot));
}

TEST(OverDiskRegression, ScanEqualsLogBytesRegression) {
    Rig rig;
    tagNamespace(rig, 0);   // unlimited: only the accounting is under test
    const auto frame = produceFrame(1, 1, 0, 700, 2);
    for (int i = 0; i < 30; ++i) { rig.pass(&frame); (void)answerOf(rig); }
    rig.worker().scanNamespaceDiskForTest(rig.now);
    EXPECT_EQ(rig.worker().nsDiskBytesForTest(kSlot), rig.log->bytesOnDisk()) << "the scan sums the log's O(1) counter";
    EXPECT_GT(rig.worker().nsDiskBytesForTest(kSlot), 0);
    EXPECT_EQ(rig.worker().nsDiskBytesForTest(0), 0) << "another slot is not charged";
    EXPECT_FALSE(rig.worker().nsOverDiskForTest(kSlot)) << "0 = unlimited";
}
