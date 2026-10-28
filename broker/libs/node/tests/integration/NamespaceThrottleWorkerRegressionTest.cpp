// NamespaceThrottleWorkerRegressionTest.cpp - namespaces Phase 2 Task 14 (D22, D28): the Throttle message at the owning WORKER,
// driven deterministically (the test thread is the worker; HotPathBRig, no Aeron, no sleeps).  The aggregator's decision
// logic is in rune_ns_limit_aggregator_tests; the real client against a real broker is in rune_ns_throttle_tests.
//
// Interaction matrix (Throttle x slot owner x timing):
//   | message                         | slot state                      | timing                          | expected                                     | test
//   |---------------------------------|---------------------------------|---------------------------------|----------------------------------------------|-----
//   | Throttle{ProduceBytes, until}   | the owner                       | now < until, then now >= until  | refused 53 + retry-after, nothing appended; then served | NamespaceThrottleRegression.ProduceThrottledReturnsRetryAfterWorkerRegression
//   | Throttle for slot B             | slot A produces                 | B throttled                     | A is served                                  | NamespaceThrottleRegression.OtherNamespaceNotThrottledWorkerRegression
//   | Throttle{old nsId}              | slot deleted + recreated        | stale message after the recreate | ignored + counted; the new owner is served   | NamespaceThrottleRegression.RecreateUnderThrottleStartsClearRegression
//   | a live throttle                 | slot deleted + recreated        | before any message              | the new owner starts clear                   | NamespaceThrottleRegression.RecreateUnderThrottleStartsClearRegression
//   | Throttle{Disk, INT64_MAX}/{0}   | the owner                       | set, then cleared               | refused 52, then served                      | NamespaceThrottleRegression.DiskThrottleIsTheAggregatorsRegression
//   | Throttle{RequestOps}            | the owner                       | produce                         | refused 53 (the shared kind gates produce)   | NamespaceThrottleRegression.RequestOpsGatesProduceRegression
//   | the worker's counters           | produces                        | usage report                    | NsUsage signals carry the cumulative bytes   | NamespaceThrottleRegression.WorkerReportsCountersRegression
#include "HotPathBRig.h"

#include <rune/node/NsLimitAggregator.h>
#include <rune/protocol/NamespaceLimits.h>

namespace {

using rune::node::ThrottleKind;
constexpr uint32_t kNsA = 1, kNsB = 2;
constexpr uint16_t kSlotA = 1, kSlotB = 2;

void setup(Rig& rig) {
    rig.log->setNsTag(kNsA, kSlotA);
    rune::protocol::NamespaceLimits l;
    rig.worker().applyNsSlotAssignDirect(kSlotA, kNsA, l);
    rig.worker().applyNsSlotAssignDirect(kSlotB, kNsB, l);
}

struct Answer { int code = -1; int64_t lastOffset = 0; };
Answer answerOf(Rig& rig) {
    Answer a;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) { a.code = r.errorCode; a.lastOffset = r.lastOffset; }
    });
    return a;
}

constexpr int kThrottled = static_cast<int>(pc::ErrorCode::Throttled);

}  // namespace

TEST(NamespaceThrottleRegression, ProduceThrottledReturnsRetryAfterWorkerRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 1);
    rig.pass(&frame);
    ASSERT_EQ(answerOf(rig).code, 0) << "served before the throttle";
    const int64_t until = rig.now + 50'000'000;
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::ProduceBytes, until);
    const int64_t end = rig.log->logEndOffset();
    rig.pass(&frame);
    const Answer a = answerOf(rig);
    EXPECT_EQ(a.code, kThrottled);
    EXPECT_EQ(a.code, 53);
    EXPECT_EQ(a.lastOffset, until - rig.now) << "the retry-after is the time to the deadline";
    EXPECT_GT(a.lastOffset, 0);
    EXPECT_EQ(rig.log->logEndOffset(), end) << "a throttled produce appends nothing";
    EXPECT_EQ(rig.worker().throttledRefusalsForTest(), 1u);
    // The deadline passes (injected clock): served, no message needed.
    rig.now = until;
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0) << "now >= untilNs: served again without a clear message";
    EXPECT_GT(rig.log->logEndOffset(), end);
    // An explicit clear (until 0) lifts an unexpired deadline.
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::ProduceBytes, rig.now + 10'000'000'000LL);
    rig.pass(&frame);
    ASSERT_EQ(answerOf(rig).code, kThrottled);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::ProduceBytes, 0);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0);
}

TEST(NamespaceThrottleRegression, OtherNamespaceNotThrottledWorkerRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 1);
    for (const ThrottleKind k : {ThrottleKind::ProduceBytes, ThrottleKind::RequestOps, ThrottleKind::Disk,
                                 ThrottleKind::FetchBytes, ThrottleKind::QueryCycles})
        rig.worker().applyNsThrottleDirect(kSlotB, kNsB, k, INT64_MAX);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0) << "namespace A's log is not throttled by B's throttle";
    EXPECT_EQ(rig.worker().throttledRefusalsForTest(), 0u);
    EXPECT_EQ(rig.worker().nsThrottleUntilForTest(kSlotA, ThrottleKind::ProduceBytes), 0);
}

// Mutation: ignore the nsId on a Throttle (apply by slot only) -> the stale message throttles the new owner.
TEST(NamespaceThrottleRegression, RecreateUnderThrottleStartsClearRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 1);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::ProduceBytes, INT64_MAX);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::Disk, INT64_MAX);
    rig.pass(&frame);
    ASSERT_NE(answerOf(rig).code, 0) << "precondition: throttled";
    // `a` is deleted (the slot is released) and recreated as nsId 5 in the same slot.
    ASSERT_TRUE(rig.worker().applyNsSlotReleaseDirect(kSlotA, kNsA));
    constexpr uint32_t kNew = 5;
    rune::protocol::NamespaceLimits l;
    rig.worker().applyNsSlotAssignDirect(kSlotA, kNew, l);
    rig.log->setNsTag(kNew, kSlotA);
    EXPECT_EQ(rig.worker().nsThrottleUntilForTest(kSlotA, ThrottleKind::ProduceBytes), 0) << "the new owner starts clear";
    EXPECT_FALSE(rig.worker().nsOverDiskForTest(kSlotA));
    // The aggregator's in-flight message for the OLD owner arrives after the recreate: dropped, counted.
    const uint64_t mism = rig.worker().nsSlotMismatchesForTest();
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::ProduceBytes, INT64_MAX);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::Disk, INT64_MAX);
    EXPECT_EQ(rig.worker().nsSlotMismatchesForTest(), mism + 2) << "stale messages are counted";
    EXPECT_EQ(rig.worker().nsThrottleUntilForTest(kSlotA, ThrottleKind::ProduceBytes), 0);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0) << "the new owner is served";
    // And a message for the new owner applies.
    rig.worker().applyNsThrottleDirect(kSlotA, kNew, ThrottleKind::ProduceBytes, INT64_MAX);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, kThrottled);
}

TEST(NamespaceThrottleRegression, DiskThrottleIsTheAggregatorsRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 1);
    // The worker no longer decides over-disk itself, whatever the limit and its own bytes are.
    rune::protocol::NamespaceLimits l;
    l.maxDiskBytes = 1;
    rig.worker().applyNsSlotAssignDirect(kSlotA, kNsA, l);
    for (int i = 0; i < 5; ++i) { rig.pass(&frame); EXPECT_EQ(answerOf(rig).code, 0) << i; }
    rig.worker().scanNamespaceDiskForTest(rig.now);
    EXPECT_FALSE(rig.worker().nsOverDiskForTest(kSlotA)) << "the byte is set only by Throttle{Disk}";
    EXPECT_GT(rig.worker().nsDiskBytesForTest(kSlotA), 1) << "the byte accounting stays on the worker";
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::Disk, INT64_MAX);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded));
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::Disk, 0);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0);
}

TEST(NamespaceThrottleRegression, RequestOpsGatesProduceRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 1);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::RequestOps, INT64_MAX);
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, kThrottled);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::RequestOps, 0);
    rig.worker().applyNsThrottleDirect(kSlotA, kNsA, ThrottleKind::FetchBytes, INT64_MAX);   // a fetch throttle never gates produce
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig).code, 0);
}

TEST(NamespaceThrottleRegression, WorkerReportsCountersRegression) {
    Rig rig;
    setup(rig);
    const auto frame = produceFrame(1, 1, 0, 200, 3);
    for (int i = 0; i < 4; ++i) { rig.pass(&frame); ASSERT_EQ(answerOf(rig).code, 0); }
    EXPECT_EQ(rig.worker().nsCounterForTest(kSlotA, rune::node::NsMetric::ProduceRecords), 12u);
    EXPECT_GT(rig.worker().nsCounterForTest(kSlotA, rune::node::NsMetric::ProduceBytes), 4u * 600u);
    EXPECT_EQ(rig.worker().nsCounterForTest(kSlotA, rune::node::NsMetric::RequestOps), 4u);
    EXPECT_EQ(rig.worker().nsCounterForTest(kSlotB, rune::node::NsMetric::ProduceBytes), 0u);
    // The usage report: one NsUsage signal per changed metric of the owning slot, tagged with the nsId; nothing when unchanged.
    std::map<int, uint64_t> got;
    auto drain = [&] {
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::SessionSignal &&
                r.lastOffset == static_cast<int64_t>(rune::node::SessionSignalKind::NsUsage) && (r.sessionId >> 8) == kSlotA) {
                EXPECT_EQ(r.correlationId, static_cast<int64_t>(kNsA));
                got[static_cast<int>(r.sessionId & 0xFF)] = static_cast<uint64_t>(r.baseOffset);
            }
        });
    };
    (void)rig.worker().reportNsUsageForTest();
    drain();
    EXPECT_EQ(got[static_cast<int>(rune::node::NsMetric::ProduceRecords)], 12u);
    EXPECT_EQ(got[static_cast<int>(rune::node::NsMetric::RequestOps)], 4u);
    EXPECT_EQ(rig.worker().reportNsUsageForTest(), 0) << "nothing changed: nothing is sent";
}
