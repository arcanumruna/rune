// CatalogAllocRegressionTest.cpp — catalog history: the admin thread's catalog steady
// state never allocates.  Replaces the global operator new (binary-wide), so it lives in
// its own binary (rune_catalog_alloc_regression_tests).  Task 12 added the idle catalog
// subscription gate; Task 23 added the idle admin-pass gates (leader + follower), the
// produce/subscribe placement-gate gates and the GetViewRow gates (served + redirect).
#include "support/CatalogReadRig.h"
#include "support/CatalogRig.h"
#include "support/CatalogDdlRig.h"
#include "support/GatewayProduceRig.h"
#include "support/ViewReadRig.h"
#include <gtest/gtest.h>
#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <span>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {          // name the culprit (HotPathBAllocRegressionTest pattern)
            gArmed = false;                  // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "catalog alloc gate: allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {          // name the culprit (HotPathBAllocRegressionTest pattern)
            gArmed = false;                  // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "catalog alloc gate: allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using rune::node::test::CatalogRig;

TEST(CatalogAllocRegression, SteadyIdleApplyAllocatesNothingRegression) {
    CatalogRig rig("ap_alloc");
    rig.appendEvents(0, 18);
    rig.applier.applyCommitted(rig.replica());
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// A halted applier is idle too: it answers 0 without touching the log or allocating.
TEST(CatalogAllocRegression, HaltedApplierAllocatesNothingRegression) {
    CatalogRig rig("ap_alloc_halt");
    rig.appendEvents(0, 3);
    rig.appendTampered(3);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(rig.applier.halted());
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// Catalog history Task 12: a caught-up (Live) catalog subscription costs nothing
// while no event commits — the read server's idle pass reads and allocates nothing.
TEST(CatalogAllocRegression, IdleCatalogSubscriptionsAllocateNothingRegression) {
    rune::node::test::ReadRig r("rd_alloc");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();                                   // backlog + marker: the sub is Live
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)r.server.pump();
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// Fix round 0: a PAUSED catalog reader (back-pressured session) costs nothing on
// either thread — the server skips it, the control flow re-checks its room —
// and neither allocates.
#include <rune/node/CatalogReadFlow.h>
namespace {
struct NotYetSink {
    using Offer = rune::node::CatalogReadFlow::Offer;
    using Room  = rune::node::CatalogReadFlow::Room;
    Offer tryOffer(int64_t, const uint8_t*, std::size_t) { return Offer::BackPressured; }
    void  sendReliable(int64_t, const uint8_t*, std::size_t) {}
    Room  room(int64_t) { return Room::NotYet; }
    bool  postPaused(int64_t, int64_t, int64_t, bool) { return true; }
    bool  postResume(int64_t, int64_t) { return true; }
    bool  postSessionGone(int64_t) { return true; }
};
}   // namespace
TEST(CatalogAllocRegression, PausedCatalogReaderAllocatesNothingRegression) {
    rune::node::test::ReadRig r("rd_alloc_paused");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const int64_t sub = r.frames.empty() ? 0 : [&] {
        for (const auto& f : r.frames)
            if (auto s = rune::protocol::client::SubscribeResponse::decode(f.bytes.data(), f.bytes.size()))
                return s->subscriptionId;
        return int64_t{0};
    }();
    r.server.onPaused(7, sub, 0, false);
    ASSERT_STREQ(r.server.phaseName(sub), "Paused");
    rune::node::CatalogReadFlow flow(8);
    NotYetSink sink;
    uint8_t frame[24 + 64] = {};
    const int64_t s7 = 7, base = 0;
    std::memcpy(frame, &s7, 8);
    std::memcpy(frame + 8, &sub, 8);
    std::memcpy(frame + 16, &base, 8);
    flow.onFrame(frame, sizeof frame, sink);   // back-pressured → paused
    flow.onFrame(frame, 24, sink);             // its ack → awaiting room
    ASSERT_EQ(flow.paused(), 1u);
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) {
        (void)r.server.pump();
        (void)flow.service(sink);
    }
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// Fix round 3 (F3b): the session-liveness probes (one per subscription per
// second) allocate nothing on the admin thread.
TEST(CatalogAllocRegression, LivenessProbesAllocateNothingRegression) {
    rune::node::test::ReadRig r("rd_alloc_probe", false, 64);
    r.commitEvents(2);
    for (int64_t s = 1; s <= 32; ++s) r.server.subscribe(s, 1, -2);
    r.server.pump(0);
    uint64_t probes = 0;
    r.onFrame = [&probes](const rune::node::CatalogReadFrameHead& h, std::span<const uint8_t> f) {
        if (f.empty() && h.baseOffset == rune::node::kProbeOffset) ++probes;
        return true;
    };
    gArmedAllocs = 0;
    gArmed = true;
    int64_t now = 0;
    for (int i = 0; i < 1000; ++i) (void)r.server.pump(now += 10'000'000);   // 10 s of passes
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
    EXPECT_GE(probes, 32u * 9u) << "non-vacuous: each subscription probed about every second";
}

// ===========================================================================
// Catalog history Task 23 gates.
// ===========================================================================

// Spec §10 / constraint 4: an idle cluster's admin pass allocates nothing — a leader with a
// live (caught-up) catalog reader that is probed ~1/s.
TEST(CatalogAllocRegression, IdleCatalogAdminPassAllocatesNothingRegression) {
    rune::node::test::CatalogDdlRig rig("gate_alloc", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "t0", .partitions = 1, .typed = true}).code, 0);
    for (int i = 1; i < 50; ++i) ASSERT_EQ(rig.define({.name = "t" + std::to_string(i), .partitions = 1}).code, 0) << i;
    ASSERT_EQ(rig.createView("v0", "t0").code, 0);                      // 51 events, offsets 0..50
    rig.subscribeCatalog(/*session=*/9);                                // an idle catalog reader too
    for (int i = 0; i < rune::node::test::CatalogDdlRig::kCap && !rig.catalogReaderLive(); ++i) rig.pump();
    ASSERT_TRUE(rig.catalogReaderLive());
    ASSERT_EQ(rig.admin().applier().lastApplied().offset, 50);
    const uint64_t probes0 = rig.catalogProbes();
    int64_t t = rig.now();
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 10'000; ++i) (void)rig.admin().doWork(t += 1'000'000);   // 10 s of 1 ms passes
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the idle catalog leader's admin pass allocated (backtraces above)";
    EXPECT_EQ(rig.admin().roleName(), "LeaderServing");
    EXPECT_TRUE(rig.catalogReaderLive());
    EXPECT_GE(rig.catalogProbes() - probes0, 9u) << "non-vacuous: the idle reader was probed ~1/s for 10 s";
    EXPECT_EQ(rig.admin().applier().lastApplied().offset, 50) << "idle: nothing applied";
}

TEST(CatalogAllocRegression, IdleCatalogFollowerAdminPassAllocatesNothingRegression) {
    rune::node::test::CatalogDdlRig rig("gate_alloc_f", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "t0", .partitions = 1, .typed = true}).code, 0);
    for (int i = 1; i < 50; ++i) ASSERT_EQ(rig.define({.name = "t" + std::to_string(i), .partitions = 1}).code, 0) << i;
    ASSERT_EQ(rig.createView("v0", "t0").code, 0);
    rig.becomeFollower(/*leaderId=*/2);
    ASSERT_EQ(rig.admin().roleName(), "Follower");
    int64_t t = rig.now();
    for (int i = 0; i < 64; ++i) (void)rig.admin().doWork(t += 1'000'000);      // warm-up, disarmed
    gArmedAllocs = 0;
    int64_t followerWork = 0;
    gArmed = true;
    for (int i = 0; i < 10'000; ++i) followerWork += rig.admin().doWork(t += 1'000'000);
    gArmed = false;
    EXPECT_GT(followerWork, 0) << "non-vacuous: the follower pass did work in the armed window";
    EXPECT_EQ(gArmedAllocs, 0u) << "the idle catalog follower's admin pass allocated (backtraces above)";
    EXPECT_EQ(rig.admin().roleName(), "Follower");
    EXPECT_EQ(rig.admin().applier().lastApplied().offset, 50);
}

// Task 8 ruling 1 (binding): the per-produce placement check (ClientGateway::servedPartition_ ->
// placementGated && placementCommitted_) costs no allocation.
TEST(CatalogAllocRegression, PlacedCatalogTopicProducePathAllocatesNothingRegression) {
    using rune::node::test::GatewayProduceRig; using rune::node::test::ProduceTopicMode;
    GatewayProduceRig rig(ProduceTopicMode::CatalogPlaced);
    ASSERT_TRUE(rig.registry->placementGated(GatewayProduceRig::kTopic)) << "the gate runs on this path";
    std::vector<std::vector<uint8_t>> frames;
    for (int i = 0; i < 768; ++i) frames.push_back(rig.produceFrame(i + 1));      // built disarmed
    for (int i = 0; i < 256; ++i) ASSERT_TRUE(rig.produceAndAwait(frames[i]));    // warm-up
    gArmedAllocs = 0;
    gArmed = true;
    bool all = true;
    for (int i = 256; i < 768; ++i) all = rig.produceAndAwait(frames[i]) && all;
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the placement-gated produce path allocated (backtraces above)";
    EXPECT_TRUE(all);
    EXPECT_EQ(rig.produceOk, 768u);  EXPECT_EQ(rig.produceErr, 0u);  EXPECT_EQ(rig.otherFrames, 0u);
    EXPECT_EQ(rig.replica->logEndOffset(), 768);
}

// The waiting side: a catalog topic whose placement is not committed answers NotLeader on
// produce and subscribe, allocating nothing.
TEST(CatalogAllocRegression, UnplacedCatalogTopicRefusalsAllocateNothingRegression) {
    using rune::node::test::GatewayProduceRig; using rune::node::test::ProduceTopicMode;
    GatewayProduceRig rig(ProduceTopicMode::CatalogUnplaced);
    std::vector<std::vector<uint8_t>> pf, sf;
    for (int i = 0; i < 768; ++i) { pf.push_back(rig.produceFrame(2 * i + 1)); sf.push_back(rig.subscribeFrame(2 * i + 2)); }
    for (int i = 0; i < 256; ++i) { rig.produce(pf[i]); rig.subscribe(sf[i]); }
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 256; i < 768; ++i) { rig.produce(pf[i]); rig.subscribe(sf[i]); }
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the NotPlaced refusal path allocated (backtraces above)";
    EXPECT_EQ(rig.produceNotLeader, 768u);
    EXPECT_EQ(rig.produceOk, 0u);
    EXPECT_EQ(rig.errNotLeader, 768u) << "subscribe refusals are ErrorResponse NotLeader";
    EXPECT_EQ(rig.subscribeOk, 0u);
    EXPECT_EQ(rig.gw.subscriptionCount(), 0u);
}

// Task 8 N4 / Task 22 N-M2 (binding): the steady-state GetViewRow serve path (gateway viewRowGate_ +
// routing lookup, worker step 2b with a 64-replica owned-set scan) allocates nothing.
TEST(CatalogAllocRegression, ServedGetViewRowAllocatesNothingRegression) {
    using rune::node::test::GatewayViewReadRig;
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::ServedHere, /*fillers=*/63);   // 64 owned, source LAST
    std::vector<std::vector<uint8_t>> frames;
    for (int i = 0; i < 576; ++i) frames.push_back(rig.getViewRowFrame(i + 1, /*key=*/7));
    for (int i = 0; i < 64; ++i) ASSERT_TRUE(rig.readAndAwait(frames[i]));
    gArmedAllocs = 0;
    gArmed = true;
    bool all = true;
    for (int i = 64; i < 576; ++i) all = rig.readAndAwait(frames[i]) && all;
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the steady-state GetViewRow serve path allocated (backtraces above)";
    EXPECT_TRUE(all);
    EXPECT_EQ(rig.viewOk, 576u);  EXPECT_EQ(rig.viewOther, 0u);  EXPECT_EQ(rig.redirects, 0u);
    EXPECT_EQ(rig.lastHp, 70);
}

TEST(CatalogAllocRegression, RedirectedGetViewRowAllocatesNothingRegression) {
    using rune::node::test::GatewayViewReadRig;
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::SourceLedElsewhere, 0);
    std::vector<std::vector<uint8_t>> frames;
    for (int i = 0; i < 576; ++i) frames.push_back(rig.getViewRowFrame(i + 1, /*key=*/7));
    for (int i = 0; i < 64; ++i) rig.read(frames[i]);
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 64; i < 576; ++i) rig.read(frames[i]);
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the GetViewRow Redirect path allocated (backtraces above)";
    EXPECT_EQ(rig.redirects, 576u);
    EXPECT_EQ(rig.viewOk, 0u);
    EXPECT_EQ(rig.viewOther + rig.errOther + rig.otherFrames, 0u);
    EXPECT_EQ(rig.pool.worker(0).getViewRowsForTest(), 0u) << "nothing was forwarded to the worker";
}

// R-I1: with 1024 owned replicas (the source LAST) the serve path is still allocation-free (the
// (topicId, partition) lookup is an O(1) index, not a scan).
TEST(CatalogAllocRegression, ServedGetViewRowWith1024OwnedAllocatesNothingRegression) {
    using rune::node::test::GatewayViewReadRig;
    GatewayViewReadRig rig(GatewayViewReadRig::Mode::ServedHere, /*fillers=*/1023);
    std::vector<std::vector<uint8_t>> frames;
    for (int i = 0; i < 576; ++i) frames.push_back(rig.getViewRowFrame(i + 1, /*key=*/7));
    for (int i = 0; i < 64; ++i) ASSERT_TRUE(rig.readAndAwait(frames[i]));
    gArmedAllocs = 0;
    gArmed = true;
    bool all = true;
    for (int i = 64; i < 576; ++i) all = rig.readAndAwait(frames[i]) && all;
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "the 1024-owned GetViewRow serve path allocated (backtraces above)";
    EXPECT_TRUE(all);
    EXPECT_EQ(rig.viewOk, 576u);  EXPECT_EQ(rig.viewOther, 0u);  EXPECT_EQ(rig.redirects, 0u);
    EXPECT_EQ(rig.lastHp, 70);
}
