#include "support/CheckedWritesRig.h"
#include <rune/protocol/NamespaceLimits.h>
using namespace rune::node;
using namespace rune::node::cw;
namespace {
constexpr uint32_t kNs = 7;
constexpr uint16_t kSlot = 2;
void tagAndAssign(VerdictRig& rig) {
    rig.log->setNsTag(kNs, kSlot);
    rig.worker->applyNsSlotAssignDirect(kSlot, kNs, rune::protocol::NamespaceLimits{});
}
}  // namespace

TEST(NsFetchGateAdversarial, LegacyPushReadPacedAdversarialRegression) {
    VerdictRig rig;
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);
    const int64_t id = rig.subscribe(SubKind::PushRead, 0);
    EXPECT_TRUE(rig.collect(id).records.empty());
    EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u);
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, 0);
    EXPECT_EQ(rig.collect(id).records.size(), 3u);
    EXPECT_GT(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u);
}
TEST(NsFetchGateAdversarial, WorkerOwnedSubscriptionFetchThrottledAdversarialRegression) {
    VerdictRig rig;
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);
    const int64_t id = rig.subscribe(SubKind::Raw, 0);
    rig.passes(50);
    EXPECT_EQ(rig.worker->subscriptionPositionForTest(id), 0) << "a fetch-throttled namespace was pushed to";
}
TEST(NsFetchGateAdversarial, WorkerOwnedSubscriptionFetchBytesCountedAdversarialRegression) {
    VerdictRig rig;
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
    const int64_t id = rig.subscribe(SubKind::Raw, 0);
    ASSERT_EQ(rig.collect(id).records.size(), 3u);
    EXPECT_GT(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u);
}
TEST(NsFetchGateAdversarial, FilteredSubscriptionFetchThrottledAdversarialRegression) {
    VerdictRig rig;
    rig.typed();
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10, 1}, {2, 20, 1}, {3, 30, 1}}), 0);
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);
    const int64_t id = rig.subscribe(SubKind::Filtered, 0);
    rig.passes(50);
    EXPECT_EQ(rig.worker->subscriptionPositionForTest(id), 0);
}

TEST(NsFetchGateAdversarial, GroupSubscriptionFetchThrottledAdversarialRegression) {
    VerdictRig rig;
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);
    const int64_t id = rig.subscribe(SubKind::Group, 0);
    rig.passes(50);
    EXPECT_EQ(rig.worker->subscriptionPositionForTest(id), 0) << "a fetch-throttled namespace was pushed to a group member";
    rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, 0);
    EXPECT_EQ(rig.collect(id).records.size(), 3u);
}

TEST(NsFetchGateAdversarial, RequestOpsCountedOncePerSubscribeAdversarialRegression) {
    VerdictRig rig;
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
    const uint64_t before = rig.worker->nsCounterForTest(kSlot, NsMetric::RequestOps);   // the produce is one op too
    (void)rig.subscribe(SubKind::Raw, 0);
    rig.passes(50);
    EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::RequestOps) - before, 1u) << "once per Subscribe, never per push pass";
}

TEST(NsFetchGateAdversarial, StaleTagNotChargedNorPacedAdversarialRegression) {
    for (const SubKind kind : {SubKind::Raw, SubKind::PushRead}) {
        SCOPED_TRACE(kind == SubKind::Raw ? "Raw" : "PushRead");
        VerdictRig rig;
        tagAndAssign(rig);                                            // (slot 2, ns 7) holds the log
        ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
        ASSERT_TRUE(rig.worker->applyNsSlotReleaseDirect(kSlot, kNs));
        rig.worker->applyNsSlotAssignDirect(kSlot, kNs + 2, rune::protocol::NamespaceLimits{});   // another owner: the log's tag is stale
        rig.worker->applyNsThrottleDirect(kSlot, kNs + 2, ThrottleKind::FetchBytes, INT64_MAX);
        const int64_t id = rig.subscribe(kind, 0);
        EXPECT_EQ(rig.collect(id).records.size(), 3u) << "a stale tag is not paced by the new owner";
        EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u) << "nor charged to it";
        EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::RequestOps), 0u);
    }
}

TEST(NsFetchGateAdversarial, FilteredSubscriptionFetchBytesCountedAdversarialRegression) {
    VerdictRig rig;
    rig.typed();
    tagAndAssign(rig);
    ASSERT_EQ(rig.produce({{1, 10, 1}, {2, 20, 1}, {3, 30, 1}}), 0);
    const int64_t id = rig.subscribe(SubKind::Filtered, 0);
    ASSERT_EQ(rig.collect(id).records.size(), 3u);
    EXPECT_GT(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u);
}

// ADVERSARIAL (batch-2 union review, worker-ns x 2c): a subscription OPEN across the namespace's slot release and the
// slot's reuse by a new namespace.  Its cached tag (s.ns) goes stale: the new owner's fetch throttle must never pace it,
// its bytes must never be charged to the new owner, and the read path never counts a slot mismatch (only the stale
// throttle message would).  Each kind, held back by the OLD owner's throttle first so the reuse happens mid-subscription.
TEST(NsFetchGateAdversarial, OpenSubscriptionAcrossSlotReuseAdversarialRegression) {
    for (const SubKind kind : {SubKind::Raw, SubKind::PushRead, SubKind::Group}) {
        SCOPED_TRACE(kind == SubKind::Raw ? "Raw" : kind == SubKind::PushRead ? "PushRead" : "Group");
        VerdictRig rig;
        tagAndAssign(rig);
        ASSERT_EQ(rig.produce({{1, 10}, {2, 20}, {3, 30}}), 0);
        rig.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::FetchBytes, INT64_MAX);   // the old owner is paced
        const int64_t id = rig.subscribe(kind, 0);
        rig.passes(50);
        if (kind != SubKind::PushRead)
            ASSERT_EQ(rig.worker->subscriptionPositionForTest(id), 0) << "premise: held by the old owner";
        const uint64_t mismatches = rig.worker->nsSlotMismatchesForTest();
        ASSERT_TRUE(rig.worker->applyNsSlotReleaseDirect(kSlot, kNs));
        rune::protocol::NamespaceLimits l{};
        l.fetchBytesPerSec = 1;
        rig.worker->applyNsSlotAssignDirect(kSlot, kNs + 2, l);
        rig.worker->applyNsThrottleDirect(kSlot, kNs + 2, ThrottleKind::FetchBytes, INT64_MAX);   // the NEW owner is paced
        rig.worker->applyNsThrottleDirect(kSlot, kNs + 2, ThrottleKind::RequestOps, INT64_MAX);
        EXPECT_EQ(rig.collect(id).records.size(), 3u) << "the new owner's throttle paced the old namespace's subscription";
        EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::FetchBytes), 0u) << "the old subscription's bytes were charged to the new owner";
        EXPECT_EQ(rig.worker->nsCounterForTest(kSlot, NsMetric::RequestOps), 0u);
        EXPECT_EQ(rig.worker->nsSlotMismatchesForTest(), mismatches) << "a read counted a slot mismatch";
    }
}
