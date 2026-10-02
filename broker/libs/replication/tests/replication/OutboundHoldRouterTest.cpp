// OutboundHoldRouterTest.cpp — F27 fix round 1, F1: the legacy outbound drain
// (AeronPartitionTransport, control thread) never drops a frame the outbound
// ring already accepted; one slow peer never blocks the others.
//
// The router is driven with a SCRIPTED Offer policy (template parameter) — no
// Aeron, no clock, no sleeps.  Each drive loop is capped and asserted.
#include <gtest/gtest.h>

#include <rune/consensus/IngressRing.h>
#include <rune/replication/OutboundHoldRouter.h>

#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <vector>

using namespace rune::replication;

namespace {

constexpr int64_t kNotConnected = -1;
constexpr int64_t kBackPressured = -2;
constexpr int64_t kAdminAction  = -3;

struct ScriptOffer {
    std::map<NodeId, std::deque<int64_t>>              script;   // next codes (<=0 refuses)
    std::map<NodeId, int64_t>                          sticky;   // persistent refusal (0 = none)
    std::map<NodeId, std::vector<std::vector<uint8_t>>> got;     // accepted frames, in order
    int64_t pos = 0;

    int64_t operator()(NodeId p, const uint8_t* b, std::size_t n) {
        if (sticky[p] != 0) return sticky[p];
        auto& s = script[p];
        if (!s.empty()) { const int64_t c = s.front(); s.pop_front(); if (c <= 0) return c; }
        got[p].emplace_back(b, b + n);
        return ++pos;
    }
};

using Router = OutboundHoldRouter<ScriptOffer>;

std::vector<uint8_t> frame(uint8_t id, std::size_t len = 16) {
    std::vector<uint8_t> f(len, id);
    return f;
}
std::vector<std::vector<uint8_t>> frames(std::initializer_list<uint8_t> ids, std::size_t len = 16) {
    std::vector<std::vector<uint8_t>> v;
    for (uint8_t i : ids) v.push_back(frame(i, len));
    return v;
}
bool route(Router& r, ScriptOffer& o, int32_t target, const std::vector<uint8_t>& f) {
    return r.route(o, target, f.data(), f.size());
}

} // namespace

TEST(OutboundHoldRouterRegression, DrainBackPressuredFrameHeldAndResentNextDrainRegression) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096);
    o.script[2] = {kBackPressured};
    EXPECT_TRUE(route(r, o, 2, frame(1))) << "consumed from the ring (held, not dropped)";
    EXPECT_TRUE(o.got[2].empty());
    EXPECT_EQ(r.flushHeld(o), 1) << "held frame re-sent on the next drain";
    EXPECT_EQ(o.got[2], frames({1}));
    EXPECT_EQ(r.heldFrames(2), 0u);
}

TEST(OutboundHoldRouterRegression, DrainHeldPeerNeverBlocksOtherPeersRegression) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096); r.addPeer(3, 4096);
    o.sticky[2] = kBackPressured;
    for (uint8_t i = 1; i <= 4; ++i) EXPECT_TRUE(route(r, o, (i % 2) ? 2 : 3, frame(i)));
    EXPECT_EQ(o.got[3], frames({2, 4})) << "peer 3 is served while peer 2 is back-pressured";
    EXPECT_EQ(r.heldFrames(2), 2u);
    o.sticky[2] = 0;
    EXPECT_EQ(r.flushHeld(o), 2);
    EXPECT_EQ(o.got[2], frames({1, 3}));
}

TEST(OutboundHoldRouterRegression, DrainPreservesPerPeerOrderBehindHeldFrameRegression) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096);
    o.script[2] = {kAdminAction};
    EXPECT_TRUE(route(r, o, 2, frame(1)));   // refused → held
    EXPECT_TRUE(route(r, o, 2, frame(2)));   // must queue BEHIND 1, never overtake it
    EXPECT_TRUE(o.got[2].empty()) << "a later frame must not overtake a held one (gap at the follower)";
    EXPECT_EQ(r.flushHeld(o), 2);
    EXPECT_EQ(o.got[2], frames({1, 2}));
}

TEST(OutboundHoldRouterRegression, DrainNotConnectedFrameHeldUntilImageConnectsRegression) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096);
    o.sticky[2] = kNotConnected;            // also: publication not resolved yet
    EXPECT_TRUE(route(r, o, 2, frame(1)));
    EXPECT_TRUE(route(r, o, 2, frame(2)));
    for (int i = 0; i < 5; ++i) EXPECT_EQ(r.flushHeld(o), 0);
    EXPECT_EQ(r.heldFrames(2), 2u);
    o.sticky[2] = 0;                         // image connects
    EXPECT_EQ(r.flushHeld(o), 2);
    EXPECT_EQ(o.got[2], frames({1, 2}));
}

TEST(OutboundHoldRouterRegression, DrainBroadcastHoldsOnlyRefusedPeerCopyRegression) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096); r.addPeer(3, 4096); r.addPeer(4, 4096);
    o.script[3] = {kBackPressured};
    EXPECT_TRUE(route(r, o, Router::kBroadcast, frame(7)));
    EXPECT_EQ(o.got[2], frames({7}));
    EXPECT_EQ(o.got[4], frames({7}));
    EXPECT_TRUE(o.got[3].empty());
    EXPECT_EQ(r.heldFrames(3), 1u);
    EXPECT_EQ(r.heldFrames(2), 0u);
    EXPECT_EQ(r.flushHeld(o), 1);
    EXPECT_EQ(o.got[3], frames({7}));
    EXPECT_EQ(o.got[2].size(), 1u) << "accepted peers never get a duplicate";
}

// A FIFO that is full makes the ring read ABORT: the frame stays in the ring
// (re-read next drain), nothing is dropped, and once the peer drains every frame
// arrives in order.  Uses the real outbound ring (IngressRing controlledDrain).
TEST(OutboundHoldRouterRegression, DrainFullHeldFifoAbortsRingWithoutDropRegression) {
    rune::consensus::IngressRing ring(4096);
    Router r; ScriptOffer o;
    r.addPeer(2, 256);                      // room for 2 held 100-byte frames
    r.addPeer(3, 256);
    o.sticky[2] = kBackPressured;

    constexpr int kFrames = 6;
    for (int i = 0; i < kFrames; ++i) {     // [int32 target][100 B payload]
        uint8_t buf[4 + 100];
        const int32_t target = 2;
        std::memcpy(buf, &target, 4);
        std::memset(buf + 4, i + 1, 100);
        ASSERT_TRUE(ring.write(buf, sizeof buf));
    }
    auto drain = [&] {
        return ring.controlledDrain(
            [&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                aeron::util::index_t len) {
                const uint8_t* p = b.buffer() + off;
                int32_t target; std::memcpy(&target, p, 4);
                return r.route(o, target, p + 4, static_cast<std::size_t>(len) - 4);
            }, 100);
    };
    EXPECT_EQ(drain(), 2) << "two frames held, the third aborts the read";
    EXPECT_GE(r.stats().aborts, 1u);
    EXPECT_LT(ring.consumerPosition(), ring.producerPosition()) << "the rest stays in the ring";
    // Peer 3 is unaffected while 2's backlog is below its FIFO (no ring frames for 3 here).
    o.sticky[2] = 0;
    bool done = false;
    for (int i = 0; i < 10 && !done; ++i) {
        r.flushHeld(o);
        drain();
        done = o.got[2].size() == static_cast<std::size_t>(kFrames);
    }
    ASSERT_TRUE(done) << "every frame delivered once the peer drains — nothing dropped";
    for (int i = 0; i < kFrames; ++i) EXPECT_EQ(o.got[2][i][0], i + 1) << "in order";
    EXPECT_EQ(ring.consumerPosition(), ring.producerPosition());
}

TEST(OutboundHoldRouterRegression, UnknownTargetIsCountedNotHeld) {
    Router r; ScriptOffer o;
    r.addPeer(2, 4096);
    EXPECT_TRUE(route(r, o, 9, frame(1)));
    EXPECT_EQ(r.stats().unroutable, 1u);
    EXPECT_EQ(r.heldFrames(2), 0u);
}

TEST(OutboundHoldRouterRegression, HeldFifoWrapsWithoutCorruption) {
    HeldFifo f; f.init(64);
    // Interleave pushes/pops so records wrap the byte ring many times.
    uint8_t next = 0, expect = 0;
    for (int round = 0; round < 200; ++round) {
        const std::size_t len = 1 + (round % 13);
        std::vector<uint8_t> v(len, next);
        if (f.hasRoom(len)) { ASSERT_TRUE(f.push(v.data(), len)); ++next; }
        if (round % 3 == 2) {
            while (!f.empty()) {
                const auto s = f.front();
                ASSERT_EQ(s[0], expect);
                ++expect;
                f.pop();
            }
        }
    }
}
