// LeaseNamespaceTest.cpp - lease topics Task 21: max-leases, the lease view's memory rule, and the suspension / delete cells that
// are not in the LeaseMatrix.  ONE real authenticating shard broker per test (LeaseBrokerRig), driven by real clients, on the DEFAULT
// config (the aggregator ticks at its default 10 ms; a worker reports its lease count on the same period).
//
// Interaction matrix (limit / state x op x timing):
//   | state                          | op                              | timing                         | expected                                   | test
//   |--------------------------------|---------------------------------|--------------------------------|--------------------------------------------|-----
//   | at max-leases (4 of 4)         | grant of a NEW key              | after the report + tick        | NamespaceLimitExceeded (52)                | MaxLeasesRefusesNewKeyGrantRegression
//   | at max-leases                  | keepalive / update / revoke     | sequential                     | served                                     | MaxLeasesRefusesNewKeyGrantRegression
//   | a revoke / an expiry frees one | grant                           | after the next report + tick   | served                                     | MaxLeasesRefusesNewKeyGrantRegression
//   | at max-leases                  | SetLimits max-leases=0          | sequential                     | grants served again                        | MaxLeasesSetLimitsTakesEffectRegression
//   | view memory over its limit     | grant                           | after the report + tick        | 52                                         | ViewMemoryOverRefusesNewGrantsOnlyRegression
//   | view memory over its limit     | keepalive / update / revoke / expiry / lease read | sequential   | served; the lease view is never paused     | ViewMemoryOverRefusesNewGrantsOnlyRegression
//   | suspended ReadOnly, resumed    | keepalives for 2 x TTL          | sequential                     | still live; ends within TTL + bound after  | ResumeNothingToRearmRegression
//   | namespace deleted              | grant / TimeToLive / expiry     | sequential                     | UnknownTopic; no lease counted any more    | NamespaceDeleteDropsLeaseTopicRegression
//
// The four suspension cells of the plan (ReadOnlyServesKeepAlivesRefusesWrites, ReadOnlyUnkeptStillExpires, RejectAllStillExpires,
// RejectAllRefusesKeepAlives) are the LeaseMatrixTiming rows in LeaseMatrixTest.cpp (see the plan's Task 21 "As built" note).
#include "support/LeaseBrokerRig.h"

#include <set>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/NamespaceLimitFields.h>
#include <rune/node/NsLimitAggregator.h>
#include <rune/node/Worker.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
namespace pc = rune::protocol::client;
using rune::gen::LeaseClaim;

namespace {
using Lt = rc::LeaseTopic<LeaseClaim>;
constexpr uint16_t kLimit = static_cast<uint16_t>(pc::ErrorCode::NamespaceLimitExceeded);
constexpr uint16_t kUnknownTopic = static_cast<uint16_t>(pc::ErrorCode::UnknownTopic);

void setLimit(LeaseBrokerRig& rig, const char* ns, const char* field, uint64_t v) {
    rc::NamespaceLimitsPatch p;
    const auto idx = rc::namespaceLimitIndex(field);
    ASSERT_TRUE(idx.has_value()) << field;
    ASSERT_TRUE(p.set(*idx, v));
    const auto r = rig.admin().setNamespaceLimits(ns, p);
    ASSERT_TRUE(r.has_value()) << field;
}

/// Polls `f` (real time, bounded) until it holds.  The flag reaches a worker one report interval + one aggregator tick after the
/// count changes (10 ms each by default); 5 s is the hard cap.
template <class F>
bool within(std::chrono::milliseconds cap, F&& f) {
    const auto end = std::chrono::steady_clock::now() + cap;
    while (std::chrono::steady_clock::now() < end) {
        if (f()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return f();
}

/// A grant of a throw-away key answers 52.  A probe that is granted is revoked at once, so probing never changes the count for long.
bool refusesGrant(LeaseBrokerRig& rig, Lt& lt, int& probe) {
    const LeaseClaim r = rig.row("probe-" + std::to_string(++probe), 1);
    const rc::LeaseResult g = lt.grantAndWait(r);
    if (g.ok()) { (void)lt.revokeAndWait(r, g.version); return false; }
    return g.errorCode == kLimit;
}
/// A grant of a throw-away key is served (and revoked again).
bool servesGrant(LeaseBrokerRig& rig, Lt& lt, int& probe) {
    const LeaseClaim r = rig.row("probe-" + std::to_string(++probe), 1);
    const rc::LeaseResult g = lt.grantAndWait(r);
    if (!g.ok()) return false;
    (void)lt.revokeAndWait(r, g.version);
    return true;
}
}  // namespace

// The limit is the node total of the live leases the node's leaders hold; at it, a NEW key is refused and nothing else is.
// Mutation: drop the facts_.leasesOver check in CheckedWriteHook.cpp -> the 5th grant is served, red.
TEST(LeaseNamespaceRegression, MaxLeasesRefusesNewKeyGrantRegression) {
    LeaseBrokerRig rig("lease-ns-max");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    setLimit(rig, "a", "max-leases", 4);
    rc::LeaseOptions ttl1s;
    ttl1s.ttlMs = 1000;
    const LeaseClaim k1 = rig.row("k1", 1), k2 = rig.row("k2", 2), k3 = rig.row("k3", 3), k4 = rig.row("k4", 4);
    const rc::LeaseResult g1 = lt.grantAndWait(k1), g2 = lt.grantAndWait(k2), g3 = lt.grantAndWait(k3), g4 = lt.grantAndWait(k4);
    ASSERT_TRUE(g1.ok() && g2.ok() && g3.ok() && g4.ok());

    int probe = 0;
    ASSERT_TRUE(within(5s, [&] { return refusesGrant(rig, lt, probe); })) << "the 5th grant is refused once the count has been reported";

    // Everything but a new grant continues at the limit.
    EXPECT_TRUE(lt.keepAliveOnceAndWait(k1, g1.version).ok()) << "keepalive";
    const rc::LeaseResult u2 = lt.updateAndWait(rig.row("k2", 22), g2.version);
    EXPECT_TRUE(u2.ok()) << "update: " << u2.errorCode;
    const rc::LeaseResult rv = lt.revokeAndWait(k3, g3.version);
    ASSERT_TRUE(rv.ok()) << "revoke: " << rv.errorCode;
    ASSERT_TRUE(lt.getAndWait(k1).has_value()) << "the lease view is still read";

    // The revoke freed a slot: a new grant is served within the next report + tick.
    const LeaseClaim k5 = rig.row("k5", 5);
    rc::LeaseResult g5;
    ASSERT_TRUE(within(5s, [&] { g5 = lt.grantAndWait(k5); return g5.ok(); })) << "after the revoke a grant is served again";

    // Back at the limit (k1, k2, k4, k5).  A key that expires frees a slot too: revoke k5, grant a 1 s key, expire it.
    ASSERT_TRUE(lt.revokeAndWait(k5, g5.version).ok());
    const LeaseClaim k9 = rig.row("k9", 9);
    rc::LeaseResult g9;
    ASSERT_TRUE(within(5s, [&] { g9 = lt.grantAndWait(k9, ttl1s); return g9.ok(); }));
    ASSERT_TRUE(within(5s, [&] { return refusesGrant(rig, lt, probe); })) << "k1 k2 k4 k9: at the limit again";
    rig.advance(3s);   // the injected lease clock: k9 (TTL 1 s) is due, the 60 s leases are not
    for (int i = 0; i < 600 && lt.getAndWait(k9).has_value(); ++i) std::this_thread::sleep_for(5ms);
    ASSERT_FALSE(lt.getAndWait(k9).has_value()) << "k9 expired";
    EXPECT_TRUE(within(5s, [&] { return servesGrant(rig, lt, probe); })) << "an expiry frees a slot";
    EXPECT_TRUE(lt.getAndWait(k1).has_value() && lt.getAndWait(rig.row("k2")).has_value() && lt.getAndWait(k4).has_value())
        << "the other leases are untouched";
}

// A limit change reaches the workers through the aggregator: 0 = unlimited.
// Mutation: the aggregator ignores a limit change (sentLeasesOver_ never re-evaluated) -> the grants stay refused, red.
TEST(LeaseNamespaceRegression, MaxLeasesSetLimitsTakesEffectRegression) {
    LeaseBrokerRig rig("lease-ns-set");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    setLimit(rig, "a", "max-leases", 2);
    ASSERT_TRUE(lt.grantAndWait(rig.row("k1", 1)).ok());
    ASSERT_TRUE(lt.grantAndWait(rig.row("k2", 2)).ok());
    int probe = 0;
    ASSERT_TRUE(within(5s, [&] { return refusesGrant(rig, lt, probe); }));
    setLimit(rig, "a", "max-leases", 0);
    EXPECT_TRUE(within(5s, [&] { return servesGrant(rig, lt, probe); })) << "unlimited: grants are served again";
    // And back: a limit below the count refuses (the existing leases are kept, nothing is revoked).
    setLimit(rig, "a", "max-leases", 1);
    EXPECT_TRUE(within(5s, [&] { return refusesGrant(rig, lt, probe); }));
    EXPECT_TRUE(lt.getAndWait(rig.row("k1")).has_value() && lt.getAndWait(rig.row("k2")).has_value());
}

// The lease view's memory counts toward max-view-memory-bytes, but the lease view is never paused: while over, new-key grants are
// refused (52) and keepalives, updates, revokes, expiry and reads continue.
// Mutation: refuse updates as well in the hook (intents.leaseGrants -> any lease record) -> the update step is red.
TEST(LeaseNamespaceRegression, ViewMemoryOverRefusesNewGrantsOnlyRegression) {
    LeaseBrokerRig rig("lease-ns-mem");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions ttl5s;
    ttl5s.ttlMs = 5000;
    const LeaseClaim k1 = rig.row("k1", 1), k2 = rig.row("k2", 2), k3 = rig.row("k3", 3), kx = rig.row("kx", 7);
    const rc::LeaseResult g1 = lt.grantAndWait(k1), g2 = lt.grantAndWait(k2), g3 = lt.grantAndWait(k3), gx = lt.grantAndWait(kx, ttl5s);
    ASSERT_TRUE(g1.ok() && g2.ok() && g3.ok() && gx.ok());
    // Drop the rig's ordinary user view `u` so the test isolates the lease view (a paused `u` is PausedUserViewNeverHoldsLeaseAcks).
    ASSERT_EQ(rig.admin().dropViewAndWait("a/u", {}, 30s).errorCode, 0);
    // The lease view of a leader always holds its event buffer, so a 1-byte limit is exceeded by the empty view alone.
    setLimit(rig, "a", "max-view-memory-bytes", 1);
    int probe = 0;
    ASSERT_TRUE(within(5s, [&] { return refusesGrant(rig, lt, probe); })) << "a grant is refused while the view memory is over";

    EXPECT_TRUE(lt.keepAliveOnceAndWait(k1, g1.version).ok()) << "keepalive continues";
    const rc::LeaseResult u2 = lt.updateAndWait(rig.row("k2", 22), g2.version);
    EXPECT_TRUE(u2.ok()) << "update continues: " << u2.errorCode;
    const rc::LeaseResult rv = lt.revokeAndWait(k3, g3.version);
    EXPECT_TRUE(rv.ok()) << "revoke continues: " << rv.errorCode;
    // The lease view is never paused: a read answers a row (getAndWait throws on a ViewPaused answer).
    decltype(lt.getAndWait(k1)) row;
    ASSERT_NO_THROW(row = lt.getAndWait(k1));
    EXPECT_TRUE(row.has_value());
    // Expiry continues: kx (TTL 5 s) ends under the injected clock while the namespace is over its view memory.
    rig.advance(7s);
    for (int i = 0; i < 600 && lt.getAndWait(kx).has_value(); ++i) std::this_thread::sleep_for(5ms);
    EXPECT_FALSE(lt.getAndWait(kx).has_value()) << "expiry continues while the view memory is over";
    ASSERT_NO_THROW(row = lt.getAndWait(rig.row("k2")));
    EXPECT_TRUE(row.has_value()) << "k2 (TTL 60 s) is still there";

    // Raising the limit clears it: grants are served again.
    setLimit(rig, "a", "max-view-memory-bytes", 0);
    EXPECT_TRUE(within(5s, [&] { return servesGrant(rig, lt, probe); }));
}

// An ordinary view over a lease topic pauses at max-view-memory-bytes (Task 12), and lease writes ask for View acks: the ack must
// not wait for the paused view (it folds nothing until the limit is raised; its reads answer ViewPaused, never a stale row).
// Mutation: drop the paused-view skip in WorkerViewSet::minFoldCursor -> the keepalive waits for the View ack timeout (20 s) and
// answers Internal -> red.
TEST(LeaseNamespaceRegression, PausedUserViewNeverHoldsLeaseAcksRegression) {
    LeaseBrokerRig rig("lease-ns-paused-view");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    const LeaseClaim k1 = rig.row("k1", 1), k2 = rig.row("k2", 2);
    const rc::LeaseResult g1 = lt.grantAndWait(k1), g2 = lt.grantAndWait(k2);
    ASSERT_TRUE(g1.ok() && g2.ok());
    ASSERT_TRUE(within(5s, [&] { return rig.userViewRow(k1).has_value(); })) << "u folds the grant before the limit";
    setLimit(rig, "a", "max-view-memory-bytes", 1);
    auto uPaused = [&] {
        const std::span<const uint8_t> key(reinterpret_cast<const uint8_t*>(k1.claim.data()), k1.claim.size());
        for (int32_t part = 0; part < rig.t().partitions; ++part) {
            rc::ViewReadOptions ro;
            ro.partition = part;
            if (rig.ua().getViewRowAndWait(rig.viewId(), key, ro, 10s).status == rc::ViewRowResult::kViewPaused) return true;
        }
        return false;
    };
    ASSERT_TRUE(within(10s, uPaused)) << "u pauses at the limit (the case under test)";

    using clk = std::chrono::steady_clock;
    const auto t0 = clk::now();
    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(k1, g1.version);
    const rc::LeaseResult up = lt.updateAndWait(rig.row("k2", 22), g2.version);
    const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - t0);
    EXPECT_TRUE(ka.ok()) << "keepalive under a paused view: " << ka.errorCode;
    EXPECT_TRUE(up.ok()) << "update under a paused view: " << up.errorCode;
    EXPECT_LT(took, 5s) << "the View acks waited for the paused view: " << took.count() << " ms";

    // Raising the limit resumes u from its offset.
    setLimit(rig, "a", "max-view-memory-bytes", 0);
    EXPECT_TRUE(within(10s, [&] { return !uPaused(); })) << "u resumes";
}

// Resuming a namespace re-arms nothing: expiry never paused (G3), so a lease kept alive across a ReadOnly suspend and a resume lives
// for as long as it is kept, and ends within TTL + a bound once the keepalives stop.  A guard test: there is no product code to revert.
TEST(LeaseNamespaceRegression, ResumeNothingToRearmRegression) {
    LeaseBrokerRig rig("lease-ns-resume");
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 3000;
    const LeaseClaim r = rig.row("rs", 1);
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    ASSERT_TRUE(g.ok());
    ASSERT_TRUE(lt.keepAliveOnceAndWait(r, g.version).ok());
    ASSERT_TRUE(rig.admin().suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value());
    ASSERT_TRUE(rig.admin().resumeNamespace("a").has_value());
    // Keep it alive for 2 x TTL of the injected clock: one second per step, a keepalive per step.
    for (int i = 0; i < 6; ++i) {
        rig.advance(1s);
        const rc::LeaseResult ka = lt.keepAliveOnceAndWait(r, g.version);
        ASSERT_TRUE(ka.ok()) << "step " << i << ": " << ka.errorCode;
    }
    rc::LeaseTtlInfo info;
    EXPECT_TRUE(lt.timeToLiveAndWait(r, &info).ok()) << "still live after 2 x TTL of keepalives across a resume";
    EXPECT_GT(info.remainingMs, 0);
    // Keepalives stop: the lease ends within TTL + a bound (2 s).
    rig.advance(5s);
    bool gone = false;
    for (int i = 0; i < 600 && !gone; ++i) { gone = !lt.getAndWait(r).has_value(); if (!gone) std::this_thread::sleep_for(5ms); }
    EXPECT_TRUE(gone) << "an unkept lease ends within TTL + bound after a resume";
}

// Deleting a namespace drops its lease topics with it: the topic is gone (UnknownTopic), expiry appends nothing to a dropped topic
// and the workers count no lease of the freed slot.  Mutation: skip the nsLeases_ reset in WorkerViewSet::onNsSlotFreed ->
// the worker-side count stays non-zero, red.
TEST(LeaseNamespaceRegression, NamespaceDeleteDropsLeaseTopicRegression) {
    LeaseBrokerRig rig("lease-ns-delete");
    auto lt = rig.ub().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    auto adminLt = rig.admin().openLeaseTopicAndWait<LeaseClaim>("b/t", {}, 30s);
    rc::LeaseOptions ttl2s;
    ttl2s.ttlMs = 2000;
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(lt.grantAndWait(rig.row("d" + std::to_string(i), uint64_t(i)), ttl2s).ok());
    const rc::LeaseResult live = adminLt.grantAndWait(rig.row("d-live", 9));
    ASSERT_TRUE(live.ok());

    // The leases of namespace b are counted on the workers that lead its partitions (the slot of b).
    const auto nsList = rig.admin().listNamespaces();
    ASSERT_TRUE(nsList.has_value());
    uint32_t bId = 0;
    for (const auto& n : *nsList) if (n.name == "b") bId = n.nsId;
    ASSERT_NE(bId, 0u);
    // The slot of b on each worker that leads one of its partitions, found while b is live: after the delete the SAME slot is read, so
    // a counter that survives the slot's release is seen (a liveness filter would hide it).
    std::vector<std::pair<int32_t, uint16_t>> bSlots;
    for (int32_t part = 0; part < rig.bt().partitions; ++part) {
        const int32_t w = rig.broker().node().workerForPartitionForTest(rig.bt().id, part);
        bool dup = false;
        for (const auto& p : bSlots) dup = dup || p.first == w;   // two partitions may share one worker
        if (dup) continue;
        const int32_t slot = rig.broker().node().onWorkerThreadForTest(w, [&](rune::node::Worker& wk) {
            for (const uint16_t sl : wk.nsSlotsForTest().liveSlots())
                if (wk.nsSlotsForTest().nsIdAt(sl) == bId) return int32_t(sl);
            return int32_t(-1);
        });
        ASSERT_GE(slot, 0) << "namespace b holds a slot on worker " << w;
        bSlots.emplace_back(w, static_cast<uint16_t>(slot));
    }
    auto countOf = [&] {
        uint64_t total = 0;
        for (const auto& [w, slot] : bSlots)
            total += rig.broker().node().onWorkerThreadForTest(w, [&](rune::node::Worker& wk) {
                return wk.nsCounterForTest(slot, rune::node::NsMetric::Leases);
            });
        return total;
    };
    ASSERT_TRUE(within(5s, [&] { return countOf() == 4; })) << "the workers count the 4 live leases of b: " << countOf();

    ASSERT_TRUE(rig.admin().deleteNamespace("b").has_value());
    // The topic goes with the namespace.
    const rc::LeaseResult g = adminLt.grantAndWait(rig.row("after", 1));
    EXPECT_FALSE(g.ok());
    EXPECT_EQ(g.errorCode, kUnknownTopic) << "grant on a deleted namespace's lease topic";
    rc::LeaseTtlInfo info;
    const rc::LeaseResult ttl = adminLt.timeToLiveAndWait(rig.row("d-live"), &info);
    EXPECT_FALSE(ttl.ok());
    EXPECT_EQ(ttl.errorCode, kUnknownTopic) << "TimeToLive on a deleted namespace's lease topic";
    EXPECT_TRUE(within(5s, [&] { return countOf() == 0; })) << "no lease of the freed slot is counted any more: " << countOf();
    // The leases' deadlines pass: nothing is appended to the dropped topic, and namespace a is unaffected.
    rig.advance(10s);
    auto ltA = rig.ua().openLeaseTopicAndWait<LeaseClaim>("t", {}, 30s);
    EXPECT_TRUE(ltA.grantAndWait(rig.row("still-works", 1)).ok());
    EXPECT_EQ(countOf(), 0u);
}
