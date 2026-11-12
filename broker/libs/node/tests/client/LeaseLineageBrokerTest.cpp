// LeaseLineageBrokerTest.cpp - lease topics Task 27 (spec A.6): the control thread posts the committed data lineage (token +
// generation) of a transient lease partition to its owning worker, over a REAL single-node broker (default config).  A process
// restart is a new lineage: generation 0 -> 1, a different token.
#include "support/LeaseBrokerRig.h"

#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/node/Worker.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/ViewPlan.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <map>
#include <string>
#include <optional>
#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;

namespace {

/// The lineage note of (topic, 0) on whichever worker owns it, polled (real time, hard cap) until a note exists.
std::optional<rune::node::LeaseLineageNote> noteOf(LeaseBrokerRig& rig, int32_t topic) {
    auto& node = rig.broker().node();
    const auto end = std::chrono::steady_clock::now() + 20s;
    do {
        for (int32_t w = 0; w < node.workerCountForTest(); ++w) {
            auto n = node.onWorkerThreadForTest(w, [topic](rune::node::Worker& wk) { return wk.leaseLineageForTest(topic, 0); });
            if (n.has_value()) return n;
        }
        std::this_thread::sleep_for(5ms);
    } while (std::chrono::steady_clock::now() < end);
    return std::nullopt;
}

using rune::gen::LeaseClaim;
constexpr uint16_t kThrottledCode = static_cast<uint16_t>(rune::protocol::client::ErrorCode::Throttled);
constexpr uint16_t kNotFoundCode  = static_cast<uint16_t>(rune::protocol::client::ErrorCode::LeaseNotFound);

/// What the owning worker's lease runtime of (topic, 0) shows; has == false while it has none.
struct RuntimeSnap {
    bool    has        = false;
    uint8_t fence      = 0;
    int64_t readyNs    = 0;
    int64_t deadlineNs = 0;   ///< the Fencing deadline (0 when not Fencing)
    int64_t holdNs     = 0;   ///< grantHoldNs at the snapshot's `nowNs`
};
RuntimeSnap runtimeOf(LeaseBrokerRig& rig, int32_t topic, int64_t nowNs) {
    auto& node = rig.broker().node();
    for (int32_t w = 0; w < node.workerCountForTest(); ++w) {
        const RuntimeSnap s = node.onWorkerThreadForTest(w, [topic, nowNs](rune::node::Worker& wk) {
            RuntimeSnap r;
            const auto* rt = wk.leaseRuntimeForTest(topic, 0);
            if (rt == nullptr) return r;
            r.has     = true;
            r.fence   = rt->fenceStateCode();
            r.readyNs = rt->readyNs();
            if (const auto* f = std::get_if<rune::node::lease::LeaseRuntime::FenceFencing>(&rt->fenceState())) r.deadlineNs = f->deadlineNs;
            r.holdNs = rt->grantHoldNs(nowNs);
            return r;
        });
        if (s.has) return s;
    }
    return {};
}
/// Polls (real time, hard cap) until the runtime's fence code is `code`.
RuntimeSnap awaitFence(LeaseBrokerRig& rig, int32_t topic, uint8_t code) {
    const auto end = std::chrono::steady_clock::now() + 30s;
    RuntimeSnap s;
    do {
        s = runtimeOf(rig, topic, rig.leaseNowNs());
        if (s.has && s.fence == code) return s;
        std::this_thread::sleep_for(5ms);
    } while (std::chrono::steady_clock::now() < end);
    return s;
}
/// A grant retried on Throttled only, under a hard wall-clock cap.
rc::LeaseResult grantRetrying(rc::LeaseTopic<LeaseClaim>& lt, const LeaseClaim& row, const rc::LeaseOptions& o) {
    const auto end = std::chrono::steady_clock::now() + 60s;
    rc::LeaseResult g;
    do {
        g = lt.grantAndWait(row, o);
        if (g.ok() || g.errorCode != kThrottledCode) return g;
        std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < end);
    return g;
}
rc::DefineTopicResult defineTransientLease(LeaseBrokerRig& rig, const char* name) {
    rc::TopicOptions to;
    to.lease = lbr::spec(60'000, 1000, 60'000);   // an explicit max_ttl: a transient lease topic needs one
    to.config.cleanupPolicy = 1;
    return rig.ua().defineTopicAndWait(name, 1, "claim", lbr::claimBlob(), rc::Persistence::Transient, to, {}, 60s);
}

/// A watch's event log: the live set (key -> version), each key's end count, and protocol violations (an end of an unheld key).
struct WLog {
    std::map<std::string, int64_t> live;
    std::map<std::string, int>     ended, granted;
    int                            bad = 0, readies = 0, failed = 0;
    static std::string keyOf(const rc::LeaseEvent& e) {
        const std::string raw(reinterpret_cast<const char*>(e.key.data()), e.key.size());
        return std::string(raw.c_str(), strnlen(raw.c_str(), raw.size()));
    }
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<WLog*>(c);
        const std::string k = keyOf(e);
        switch (e.kind) {
        case rc::LeaseEventKind::Granted: w.live[k] = e.version; ++w.granted[k]; break;
        case rc::LeaseEventKind::Updated: break;
        case rc::LeaseEventKind::Revoked:
        case rc::LeaseEventKind::Expired:
        case rc::LeaseEventKind::Vanished:
            if (w.live.erase(k) == 0) ++w.bad;
            ++w.ended[k];
            break;
        case rc::LeaseEventKind::Ready: ++w.readies; break;
        case rc::LeaseEventKind::Failed: ++w.failed; break;
        case rc::LeaseEventKind::Confirmed:    // a re-run's bookkeeping: the table is already what the event says
        case rc::LeaseEventKind::RerunBegin:
        case rc::LeaseEventKind::RerunEnd: break;
        }
    }
};
/// Drives the watch on this thread (real time, hard cap) until `done`.
template <class P>
bool driveWatch(rc::LeaseWatchBase& w, rc::Client& c, std::chrono::milliseconds within, P&& done) {
    const auto end = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < end) {
        w.doWork(c.nowNs());
        if (done()) return true;
        std::this_thread::sleep_for(2ms);
    }
    w.doWork(c.nowNs());
    return done();
}

}  // namespace

// Mutation: skip the LeaseLineageFanout step in NodeRuntimeAgent::doWork -> no note ever arrives, red.
TEST(LeaseLineageRegression, SingleNodeGenerationAcrossRestartRegression) {
    LeaseBrokerRig rig("lease-lineage-gen");
    rc::TopicOptions to;
    to.lease = lbr::spec(60'000, 1000, 60'000);   // an explicit max_ttl: a transient lease topic needs one
    to.config.cleanupPolicy = 1;
    const auto d = rig.ua().defineTopicAndWait("lin", 1, "claim", lbr::claimBlob(), rc::Persistence::Transient, to, {}, 60s);
    ASSERT_EQ(d.errorCode, 0) << d.detail;

    const auto first = noteOf(rig, d.topicId);
    ASSERT_TRUE(first.has_value()) << "the owning worker never learned the lineage";
    EXPECT_EQ(first->generation, 0u);
    EXPECT_NE(first->lineage, 0u);
    EXPECT_GE(first->leaderEpoch, 0);

    rig.restart();
    // The restarted worker is a new object: its note is empty until the fresh lineage is posted.
    const auto second = noteOf(rig, d.topicId);
    ASSERT_TRUE(second.has_value()) << "the owning worker never learned the lineage after the restart";
    EXPECT_EQ(second->generation, 1u) << "a process restart mints over the existing record: generation + 1";
    EXPECT_NE(second->lineage, first->lineage);
}

// A transient lease topic gets a judge on its ring (it was disk-only), so the lease index and runtime exist and the fence opens:
// grant, keepalive, revoke and a second grant all work on a real broker.
// Mutation: restore the isDiskBacked gate of Worker::driveViewsOf_ (no judge) -> the grant never leaves Throttled, red.
TEST(LeaseTransientBrokerRegression, GrantKeepaliveRevokeRegression) {
    LeaseBrokerRig rig("lease-tr-gkr");
    const auto d = defineTransientLease(rig, "tr");
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    const LeaseClaim row = rig.row("k-tr", 1, "x");
    rc::LeaseOptions o;
    o.holder = 3;

    const rc::LeaseResult g1 = grantRetrying(lt, row, o);
    ASSERT_TRUE(g1.ok()) << int(g1.status) << " " << g1.errorCode;
    EXPECT_GE(g1.version, 0);
    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(row, g1.version);
    EXPECT_TRUE(ka.ok()) << int(ka.status) << " " << ka.errorCode;
    const rc::LeaseResult rv = lt.revokeAndWait(row, g1.version);
    EXPECT_TRUE(rv.ok()) << int(rv.status) << " " << rv.errorCode;
    const rc::LeaseResult g2 = grantRetrying(lt, row, o);
    ASSERT_TRUE(g2.ok()) << int(g2.status) << " " << g2.errorCode;
    EXPECT_GT(g2.version, g1.version);

    const RuntimeSnap s = runtimeOf(rig, d.topicId, rig.leaseNowNs());
    ASSERT_TRUE(s.has) << "the owning worker has a lease runtime";
    EXPECT_EQ(s.fence, 3) << "generation 0: the fence is Open";
}

// A restart is a new lineage: the rebirth fence holds every grant until max_ttl after the runtime's ready time (old leases may still be
// believed by their holders), then opens; the old key is gone.  The lease clock is the rig's injected one: no wall-clock waits.
// Mutation: grantHoldNs returns 0 while Fencing -> the grant completes before the deadline, red.
TEST(LeaseTransientBrokerRegression, RestartFencesGrantsRegression) {
    LeaseBrokerRig rig("lease-tr-fence");
    const auto d = defineTransientLease(rig, "tr");
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    const LeaseClaim oldRow = rig.row("k-old", 1);
    int64_t oldVersion = -1;
    {
        auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
        const rc::LeaseResult g = grantRetrying(lt, oldRow, {});
        ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
        oldVersion = g.version;
    }
    rig.restart();
    constexpr int64_t kMaxTtlNs = 60'000'000'000;
    const RuntimeSnap fencing = awaitFence(rig, d.topicId, 2);
    ASSERT_TRUE(fencing.has);
    ASSERT_EQ(fencing.fence, 2) << "the restarted leader fences";
    EXPECT_GE(fencing.deadlineNs, fencing.readyNs + kMaxTtlNs) << "at least max_ttl after the runtime's ready time";
    EXPECT_GT(runtimeOf(rig, d.topicId, rig.leaseNowNs()).holdNs, 0) << "a grant is held with a positive retry-after";

    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);   // the default op timeout (ttl / 3)
    // During the fence the retry-after (the time left, about max_ttl) is far past the grant's op timeout: the grant answers Throttled at once - it is never
    // parked behind the fence, so it can neither block the partition nor time out in doubt.
    {
        const auto t0 = std::chrono::steady_clock::now();
        const rc::LeaseResult early = lt.grantAndWait(rig.row("k-early", 2), {});
        EXPECT_FALSE(early.ok());
        EXPECT_EQ(early.errorCode, kThrottledCode) << "status " << int(early.status);
        EXPECT_NE(early.status, rc::OpStatus::Timeout);
        EXPECT_LT(std::chrono::steady_clock::now() - t0, 500ms) << "answered at once, not held";
    }
    std::atomic<bool> done{false};
    auto fut = std::async(std::launch::async, [&] {
        const rc::LeaseResult g = grantRetrying(lt, rig.row("k-new", 2), {});
        done = true;
        return g;
    });
    // Up to 1 ms short of the deadline the fence still holds: the grant cannot finish (it keeps being answered Throttled).
    rig.advance(std::chrono::milliseconds((fencing.deadlineNs - rig.leaseNowNs()) / 1'000'000 - 1));
    std::this_thread::sleep_for(300ms);
    EXPECT_FALSE(done.load()) << "no grant before the deadline";
    EXPECT_EQ(runtimeOf(rig, d.topicId, rig.leaseNowNs()).fence, 2);
    rig.advance(5ms);   // past the deadline: the open record is appended and folds
    ASSERT_EQ(fut.wait_for(60s), std::future_status::ready) << "the grant completes after the fence opens";
    const rc::LeaseResult g = fut.get();
    EXPECT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    EXPECT_EQ(runtimeOf(rig, d.topicId, rig.leaseNowNs()).fence, 3);

    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(oldRow, oldVersion);
    EXPECT_FALSE(ka.ok());
    EXPECT_EQ(ka.errorCode, kNotFoundCode) << "the pre-restart lease died with the ring";
}

// SD9 still refuses every production constraint on a plain transient topic, so it gets no judge, no verdict feed and no lease index
// (#808 stays closed for it), even with a view folding it.
// Mutation: judgedReplica_ returns true for any write state -> a hook context and a verdict source appear, red.
TEST(LeaseTransientBrokerRegression, PlainTransientTypedUnchangedRegression) {
    LeaseBrokerRig rig("lease-tr-plain");
    const auto idBlob = rune::codec::serialize(rune::codec::DescriptorBuilder("pt", 1)
                                                   .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
                                                   .add("hp", rune::codec::PrimType::Int64)
                                                   .build());
    const auto d = rig.ua().defineTopicAndWait("pt", 1, "id", idBlob, rc::Persistence::Transient, {}, {}, 60s);
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = "ptv";
    p.key      = "id";
    p.sources.push_back(rv::SourceSpec{d.topicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    const auto v = rig.ua().createViewAndWait("ptv", "id", 0, rv::serializeAuthored(p), {}, 30s);
    ASSERT_EQ(v.errorCode, 0) << v.detail;
    // The lease topic of the same broker is judged: the seam can see the difference.
    const auto lease = defineTransientLease(rig, "tr");
    ASSERT_EQ(lease.errorCode, 0) << lease.detail;
    auto lt = rig.ua().openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    ASSERT_TRUE(grantRetrying(lt, rig.row("k-j"), {}).ok());

    auto& node = rig.broker().node();
    bool leaseHooked = false;
    for (int32_t w = 0; w < node.workerCountForTest(); ++w) {
        struct Seen { bool plainHook, plainJudge, leaseHook, leaseJudge; };
        const Seen s = node.onWorkerThreadForTest(w, [&](rune::node::Worker& wk) {
            return Seen{wk.rcVerdictHooksCtxForTest(d.topicId, 0) != nullptr, wk.verdictStatsForTest(d.topicId, 0) != nullptr,
                        wk.rcVerdictHooksCtxForTest(lease.topicId, 0) != nullptr, wk.verdictStatsForTest(lease.topicId, 0) != nullptr};
        });
        EXPECT_FALSE(s.plainHook) << "worker " << w;
        EXPECT_FALSE(s.plainJudge) << "worker " << w;
        leaseHooked = leaseHooked || (s.leaseHook && s.leaseJudge);
    }
    EXPECT_TRUE(leaseHooked) << "the transient lease topic has its hook context and its judge";
}

// A transient lease topic's node restarts under a live watch: the partition is a new lineage.  The watch ends the lost keys by a sibling path: a single node's restart ends the seed-session subscription (SessionLost), the watch re-runs, and the keys of the lost
// lineage end (exactly once each).  After the rebirth fence opens a new grant is reported, and the old keys are never reported again.
// (The PartitionDataReset path itself - a resumed subscribe on a node session - is pinned by LeaseWatchDataResetRegression.ResetRerunsRegression
// in the client unit tests; a single-node restart never reaches it.)
// Mutation: LeaseWatchBase::stepPart_ ignores a finished subscription -> k1, k2 and k0 stay live forever, red.
TEST(LeaseWatchDataResetRegression, RestartReportsOldKeysEndedRegression) {
    LeaseBrokerRig rig("lease-wd-restart");
    const auto d = defineTransientLease(rig, "tr");
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    rc::Client& c = rig.ua();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    ASSERT_TRUE(grantRetrying(lt, rig.row("k1", 1), {}).ok());
    ASSERT_TRUE(grantRetrying(lt, rig.row("k2", 2), {}).ok());

    rc::LeaseWatchOptions wo;
    wo.maxLive = 256;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(driveWatch(watch, c, 30s, [&] { return watch.isReady() && log.live.size() == 2; }));
    // A record delivered through the subscription: the resume has a position, so it claims the lineage it was noted.
    ASSERT_TRUE(grantRetrying(lt, rig.row("k0", 0), {}).ok());
    ASSERT_TRUE(driveWatch(watch, c, 30s, [&] { return log.live.size() == 3; }));
    ASSERT_EQ(watch.reruns(), 0u);

    rig.restartKeepingClients();
    const RuntimeSnap fencing = awaitFence(rig, d.topicId, 2);
    ASSERT_TRUE(fencing.has);
    ASSERT_EQ(fencing.fence, 2) << "the restarted leader fences";
    ASSERT_TRUE(driveWatch(watch, c, 90s, [&] { return log.live.empty() && log.ended.size() == 3 && watch.reruns() >= 1; }))
        << "the watch never noticed the new lineage: live=" << log.live.size() << " ended=" << log.ended.size()
        << " reruns=" << watch.reruns();
    for (const char* k : {"k0", "k1", "k2"}) EXPECT_EQ(log.ended[k], 1) << k << " ends exactly once";

    // The fence opens (the injected lease clock), then a new grant is reported; the old keys never come back.
    std::atomic<bool> granted{false};
    auto fut = std::async(std::launch::async, [&] {
        const rc::LeaseResult g = grantRetrying(lt, rig.row("k3", 3), {});
        granted = true;
        return g;
    });
    rig.advance(std::chrono::milliseconds((fencing.deadlineNs - rig.leaseNowNs()) / 1'000'000 + 5));
    ASSERT_TRUE(driveWatch(watch, c, 90s, [&] { return log.live.count("k3") == 1; })) << "k3 is reported once the fence opens";
    ASSERT_EQ(fut.wait_for(60s), std::future_status::ready);
    EXPECT_TRUE(fut.get().ok());
    driveWatch(watch, c, 1s, [] { return false; });
    EXPECT_EQ(log.live.size(), 1u);
    EXPECT_EQ(log.granted["k3"], 1);
    for (const char* k : {"k0", "k1", "k2"}) {
        EXPECT_EQ(log.ended[k], 1) << k << " is never reported again";
        EXPECT_EQ(log.granted[k], 1) << k;
    }
    EXPECT_EQ(log.bad, 0);
    EXPECT_FALSE(watch.failed());
    EXPECT_EQ(log.failed, 0);
}
