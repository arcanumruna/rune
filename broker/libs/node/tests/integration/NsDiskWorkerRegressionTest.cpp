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

#include <rune/protocol/NamespaceLimits.h>

namespace {

constexpr uint32_t kNs   = 1;
constexpr uint16_t kSlot = 1;
constexpr uint64_t kLimit = 8192;

/// Assign the namespace to the worker's slot table with `limit`, tag the log, and let the worker take a first scan.
void tagNamespace(Rig& rig, uint64_t limit) {
    rig.log->setNsTag(kNs, kSlot);
    rune::protocol::NamespaceLimits l;
    l.maxDiskBytes = limit;
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
}

/// The ErrorCode of the single produce answer the pass produced (-1: none).
int answerOf(Rig& rig) {
    int code = -1;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) code = r.errorCode;
    });
    return code;
}

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
    // The limit is raised (a SetLimits reaches the worker as NsSlotAssign): the next scan clears the byte.
    rune::protocol::NamespaceLimits l;
    l.maxDiskBytes = 1 << 20;
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    EXPECT_FALSE(rig.worker().nsOverDiskForTest(kSlot));
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), 0) << "served again";
    // Lowering below usage refuses again but keeps every byte.
    const int64_t end = rig.log->logEndOffset();
    l.maxDiskBytes = kLimit;
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded));
    EXPECT_EQ(rig.log->logEndOffset(), end) << "lowering deleted nothing";
    // Limit 0 = unlimited.
    l.maxDiskBytes = 0;
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
    rig.worker().scanNamespaceDiskForTest(rig.now);
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
