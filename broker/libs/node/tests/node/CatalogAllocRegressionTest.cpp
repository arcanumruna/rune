// CatalogAllocRegressionTest.cpp — catalog history: the admin thread's catalog steady
// state never allocates.  Replaces the global operator new (binary-wide), so it lives in
// its own binary (rune_catalog_alloc_regression_tests).  Tasks 12 and 23 append here.
#include "support/CatalogReadRig.h"
#include "support/CatalogRig.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <new>
#include <span>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) ++gArmedAllocs;
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
