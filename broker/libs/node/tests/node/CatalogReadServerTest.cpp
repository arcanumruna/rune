// CatalogReadServerTest.cpp — catalog history Task 12 (spec §7, ruling 10, F19,
// F29): any broker serves a Subscribe on `__catalog` from its own replica, up to
// its own HW, then ONE caught-up marker, then live commits.  Every Subscribe is
// answered (from 0, the middle, the end, above the HW, beyond the cap, a re-send);
// a full egress never loses a frame or a position; a subscription that cannot be
// served is ENDED explicitly (never a silent stall).
#include "support/CatalogReadRig.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <numeric>
#include <span>
#include <string>
#include <vector>

using rune::node::test::ReadRig;
namespace pc = rune::protocol::client;

namespace {
std::vector<int64_t> range(int64_t from, int64_t to) {
    std::vector<int64_t> v(static_cast<std::size_t>(to - from));
    std::iota(v.begin(), v.end(), from);
    return v;
}
}   // namespace

TEST(CatalogReadServerRegression, BacklogThenCaughtUpMarkerThenLiveEvents) {
    ReadRig r("rd_basic");                             // catalog leader replica (ISR {self}) + recorder egress
    r.commitEvents(4);
    r.server.subscribe(/*session=*/7, /*corr=*/1, /*from=*/-2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4}));
    r.commitEvents(1);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7).back(), 4);
    EXPECT_EQ(r.markers(7).size(), 1u) << "one marker per subscription";
}

TEST(CatalogReadServerRegression, FollowerServesOnlyUpToItsHighWatermarkRegression) {
    ReadRig r("rd_follower", /*asFollower=*/true);
    r.followerAppend(6);                               // follower log holds 6 records …
    r.followerSetHighWatermark(4);                     // … only 4 are known committed
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4}));
}

TEST(CatalogReadServerRegression, SessionCloseFreesEverySubscription) {
    ReadRig r("rd_close");
    r.server.subscribe(7, 1, -2); r.server.subscribe(7, 2, -2);
    r.server.unsubscribe(7, -1);
    EXPECT_EQ(r.server.subscriptions(), 0u);
}

TEST(CatalogReadServerRegression, TooManySubscriptionsAreAnsweredNotDropped) {
    ReadRig r("rd_cap", false, /*maxSubs=*/2);
    r.server.subscribe(1, 1, -2); r.server.subscribe(2, 1, -2);
    r.server.subscribe(3, 1, -2);
    EXPECT_EQ(r.subscribeError(3, 1), rune::protocol::client::ErrorCode::Internal);
}

// ─── Every Subscribe is answered, from anywhere ──────────────────────────────

TEST(CatalogReadServerRegression, FromTheMiddleServesTheRestThenTheMarker) {
    ReadRig r("rd_mid");
    r.commitEvents(6);
    r.server.subscribe(7, 1, /*from=*/3);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{3, 4, 5}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{6}));
}

TEST(CatalogReadServerRegression, FromLatestAndAboveTheHwAnswerWithTheMarkerOnlyRegression) {
    ReadRig r("rd_end");
    r.commitEvents(3);
    r.server.subscribe(7, 1, /*latest=*/-1);
    r.server.subscribe(8, 1, /*beyond the HW=*/1'000);
    r.server.pump();
    for (int64_t s : {7, 8}) {
        ASSERT_TRUE(r.subscribeAnswered(s, 1)) << s;
        EXPECT_TRUE(r.deliveredOffsets(s).empty()) << s;
        EXPECT_EQ(r.markers(s), (std::vector<int64_t>{3})) << "clamped to the HW, never a stall above it: " << s;
    }
    r.commitEvents(2);
    r.server.pump();
    for (int64_t s : {7, 8}) EXPECT_EQ(r.deliveredOffsets(s), (std::vector<int64_t>{3, 4})) << s;
}

TEST(CatalogReadServerRegression, EmptyCatalogAnswersAndMarksCaughtUpAtZero) {
    ReadRig r("rd_empty");
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{0}));
    EXPECT_STREQ(r.server.phaseName(r.subscriptionIdOf(7, 1)), "Live");
}

TEST(CatalogReadServerRegression, FramesCarryTheCatalogTopicTheSubscriptionAndItsCorrelationId) {
    ReadRig r("rd_frames");
    r.commitEvents(2);
    r.server.subscribe(7, 42, -2);
    r.server.pump();
    const int64_t sub = r.subscriptionIdOf(7, 42);
    EXPECT_TRUE(rune::gateway::isCatalogSubscriptionId(sub)) << "never a gateway subscription id";
    EXPECT_EQ(sub & ((int64_t{1} << 62) - 1) & ~((int64_t{1} << 40) - 1), int64_t{ReadRig::kSelf + 1} << 40)
        << "the node prefix keeps the id cluster-unique";
    const auto recs = r.records(7);
    ASSERT_FALSE(recs.empty());
    for (const auto& rec : recs) {
        EXPECT_EQ(rec.topicId, -6);
        EXPECT_EQ(rec.partition, 0);
        EXPECT_EQ(rec.subscriptionId, sub);
        EXPECT_EQ(rec.correlationId, 42);
    }
    const auto t = r.templates(7);
    ASSERT_FALSE(t.empty());
    EXPECT_EQ(t.front(), static_cast<uint16_t>(pc::TemplateId::SubscribeResponse))
        << "the SubscribeResponse precedes every Records frame";
}

// A re-sent Subscribe (same session + correlation id) is one subscription: the
// same id re-answered, never a second stream replaying the catalog twice.
TEST(CatalogReadServerRegression, ResentSubscribeIsOneSubscriptionRegression) {
    ReadRig r("rd_dup");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.server.subscriptions(), 1u);
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2}));
    int answers = 0;
    for (uint16_t t : r.templates(7)) answers += t == static_cast<uint16_t>(pc::TemplateId::SubscribeResponse);
    EXPECT_EQ(answers, 2) << "the re-send is answered";
}

TEST(CatalogReadServerRegression, UnsubscribeEndsOnlyThatSubscriptionAndOnlyForItsOwner) {
    ReadRig r("rd_unsub");
    r.commitEvents(1);
    r.server.subscribe(7, 1, -2);
    r.server.subscribe(7, 2, -2);
    r.server.pump();
    const int64_t a = r.subscriptionIdOf(7, 1);
    r.server.unsubscribe(/*another session=*/9, a);
    EXPECT_EQ(r.server.subscriptions(), 2u) << "another session's Unsubscribe is ignored";
    r.server.unsubscribe(7, a);
    EXPECT_EQ(r.server.subscriptions(), 1u);
    r.commitEvents(1);
    const auto before = r.records(7).size();
    r.server.pump();
    EXPECT_EQ(r.records(7).size(), before + 1) << "only the remaining subscription is pushed";
}

// ─── Flow control: a full egress loses nothing and resumes where it stopped ──

TEST(CatalogReadServerRegression, FullEgressKeepsEveryPositionAndResumesRegression) {
    ReadRig r("rd_full");
    r.commitEvents(5);
    r.egressFull = true;
    r.server.subscribe(7, 1, -2);                      // the answer is owed (Answering)
    for (int i = 0; i < 10; ++i) EXPECT_EQ(r.server.pump(), 0) << "nothing fits, nothing is lost";
    EXPECT_TRUE(r.frames.empty());
    EXPECT_EQ(r.server.subscriptions(), 1u) << "accepted; its answer is owed (Answering)";
    r.egressFull = false;
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 5));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{5}));
}

// Many events, one frame at a time: every record exactly once, in order, the
// marker only after the backlog, and no frame above the egress frame bound.
TEST(CatalogReadServerRegression, LargeBacklogIsServedInOrderAcrossFramesAndPasses) {
    ReadRig r("rd_many");
    r.commitEvents(3000);                              // ~0.3 MB: several coalesced frames
    r.server.subscribe(7, 1, -2);
    for (int pass = 0; pass < 200 && r.markers(7).empty(); ++pass) {
        r.egressBudget = 1;                            // one frame per pass: a slow egress
        r.server.pump();
    }
    r.egressBudget = -1;
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{3000}));
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 3000));
    std::size_t biggest = 0;
    for (const auto& f : r.frames) biggest = std::max(biggest, f.bytes.size());
    EXPECT_LE(biggest, rune::node::kCatalogReadMaxFrameBytes);
    EXPECT_GT(r.records(7).size(), 3u) << "coalesced, but bounded per frame";
}

TEST(CatalogReadServerRegression, RefusalOwedWhileTheEgressIsFullIsDeliveredLater) {
    ReadRig r("rd_owed", false, /*maxSubs=*/1);
    r.server.subscribe(1, 1, -2);
    r.server.pump();
    r.egressFull = true;
    r.server.subscribe(2, 1, -2);                      // refused (cap) while the egress is full
    r.egressFull = false;
    r.server.pump();
    EXPECT_EQ(r.subscribeError(2, 1), pc::ErrorCode::Internal) << "owed, then delivered — never dropped";
    EXPECT_EQ(r.server.refusedSubscribes(), 1u);
}

// ─── Roles: a leader change never ends a subscription; reads stay committed ──

TEST(CatalogReadServerRegression, LeaderDemotedToFollowerKeepsServingItsCommittedPrefix) {
    ReadRig r("rd_demote");
    r.commitEvents(3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{3}));
    r.replica().becomeFollower(ReadRig::kEpoch + 1, ReadRig::kLeader);   // a leader change
    r.server.pump();
    EXPECT_EQ(r.server.subscriptions(), 1u) << "the subscription survives the role change";
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 3)) << "nothing past what was committed";
    EXPECT_TRUE(r.errors(7).empty());
}

TEST(CatalogReadServerRegression, FollowerHwAdvanceIsPushedLiveWithoutASecondMarker) {
    ReadRig r("rd_follow_live", /*asFollower=*/true);
    r.followerAppend(4);
    r.followerSetHighWatermark(2);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{2}));
    r.followerSetHighWatermark(4);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 4));
    EXPECT_EQ(r.markers(7).size(), 1u);
}

// ─── A subscription that cannot be served is ENDED explicitly ────────────────

// A stored batch no Records frame can carry (the leader never writes one —
// kCatalogMaxEventBytes — but a log may hold one) ends the subscription with
// MessageTooLarge on its Subscribe — never a subscription parked forever.
TEST(CatalogReadServerRegression, OversizeBatchEndsTheSubscriptionExplicitlyRegression) {
    ReadRig r("rd_oversize");
    r.commitEvents(2);
    const std::vector<uint8_t> big(rune::node::kCatalogReadMaxBatchBytes + 1024, 0xAB);
    const uint8_t key[8] = {9};
    ASSERT_GE(r.replica().appendKeyed(std::span<const uint8_t>(key, 8), big, true), 0);
    r.replica().onLeaderLogAppended();
    (void)r.replica().doWork(9'000'000'000);
    ASSERT_EQ(r.replica().highWatermark(), 3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1})) << "what fits is served first";
    const auto errs = r.errors(7);
    ASSERT_EQ(errs.size(), 1u);
    EXPECT_EQ(errs[0].correlationId, 1);
    EXPECT_EQ(errs[0].code, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(r.server.subscriptions(), 0u);
    EXPECT_TRUE(r.markers(7).empty()) << "never marked caught up past what it did not deliver";
}

// The leader bound and the read bound are one: every event the leader may
// append fits one Records frame of every catalog subscriber.
TEST(CatalogReadServerRegression, EveryAppendableEventFitsOneRecordsFrame) {
    static_assert(rune::node::kCatalogMaxEventBytes + 512 <= rune::node::kCatalogReadMaxBatchBytes);
    static_assert(rune::node::kCatalogReadMaxFrameBytes <= 256u * 1024u,
                  "one frame must fit the gateway's spill staging buffer");
    ReadRig r("rd_maxevent");
    const std::vector<uint8_t> big(rune::node::kCatalogMaxEventBytes, 0xCD);
    const uint8_t key[8] = {1};
    ASSERT_GE(r.replica().appendKeyed(std::span<const uint8_t>(key, 8), big, true), 0);
    r.replica().onLeaderLogAppended();
    (void)r.replica().doWork(9'000'000'000);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{1}));
    EXPECT_TRUE(r.errors(7).empty());
}

// ─── Fix round 0: flow control — a slow reader is paused, never expired ──────
#include <rune/node/CatalogReadFlow.h>

namespace {
/// The control thread's side in-process: each session has a publication window
/// (bytes in flight it can hold); the reader drains `drainPerPass` bytes a pass.
/// Pause / resume reports travel through a queue (the control → admin ring) and
/// reach the server only between its passes.
struct FlowHarness {
    struct Session {
        std::size_t window = 0, inFlight = 0, drainPerPass = 0;
        std::size_t maxQueued = 0;          // the most bytes ever held for this session
        uint64_t    reliableFrames = 0;
    };
    ReadRig&                      rig;
    rune::node::CatalogReadFlow   flow{64};
    std::map<int64_t, Session>    sessions;
    struct Report { bool pause; int64_t session, subId, base; bool marker; };
    std::vector<Report>           ring;     // control → admin
    std::map<int64_t, std::vector<std::vector<uint8_t>>> delivered;   // session → client frames
    int64_t nowNs = 0;   // the admin's clock (pass())

    using Offer = rune::node::CatalogReadFlow::Offer;
    using Room  = rune::node::CatalogReadFlow::Room;
    Offer tryOffer(int64_t s, const uint8_t* f, std::size_t n) {
        Session& ss = sessions.at(s);
        if (ss.inFlight + n > ss.window && ss.inFlight != 0) return Offer::BackPressured;
        ss.inFlight += n;
        ss.maxQueued = std::max(ss.maxQueued, ss.inFlight);
        delivered[s].emplace_back(f, f + n);
        return Offer::Sent;
    }
    void sendReliable(int64_t s, const uint8_t* f, std::size_t n) {
        ++sessions.at(s).reliableFrames;
        delivered[s].emplace_back(f, f + n);
    }
    Room room(int64_t s) {
        const auto it = sessions.find(s);
        if (it == sessions.end()) return Room::Gone;
        const std::size_t free = it->second.window - std::min(it->second.window, it->second.inFlight);
        return free >= std::min<std::size_t>(16 * 1024, it->second.window / 2) ? Room::Ready : Room::NotYet;
    }
    bool postPaused(int64_t s, int64_t sub, int64_t base, bool marker) {
        ring.push_back({true, s, sub, base, marker});
        return true;
    }
    bool postResume(int64_t s, int64_t sub) { ring.push_back({false, s, sub, -1, false}); return true; }
    std::vector<int64_t> gone;   // control → admin: sessions found gone (fix round 3)
    bool postSessionGone(int64_t s) { gone.push_back(s); return true; }

    explicit FlowHarness(ReadRig& r) : rig(r) {
        rig.onFrame = [this](const rune::node::CatalogReadFrameHead& h, std::span<const uint8_t> f) {
            std::vector<uint8_t> wire(rune::node::kCatalogReadHeadBytes + f.size());
            std::memcpy(wire.data(), &h.session, 8);
            std::memcpy(wire.data() + 8, &h.subscriptionId, 8);
            std::memcpy(wire.data() + 16, &h.baseOffset, 8);
            if (!f.empty()) std::memcpy(wire.data() + 24, f.data(), f.size());
            flow.onFrame(wire.data(), wire.size(), *this);
            return true;
        };
    }
    /// One broker pass: the admin pass, the control pass, the readers drain.
    void pass() {
        std::vector<Report> r;
        r.swap(ring);
        std::vector<int64_t> g;
        g.swap(gone);
        for (int64_t s : g) rig.server.onSessionGone(s);
        for (const Report& x : r) {
            if (x.pause) rig.server.onPaused(x.session, x.subId, x.base, x.marker);
            else         rig.server.onResume(x.session, x.subId);
        }
        rig.server.pump(nowNs);
        flow.service(*this);
        for (auto& [id, s] : sessions) s.inFlight -= std::min(s.inFlight, s.drainPerPass);
    }
    /// Every record offset / marker a session received.
    std::pair<std::vector<int64_t>, std::vector<int64_t>> seen(int64_t s) {
        std::vector<int64_t> offs, marks;
        for (const auto& f : delivered[s]) {
            auto r = pc::Records::decode(f.data(), f.size());
            if (!r) continue;
            if (r->recordBatch.size() == 8) {   // the epoch note (−1) / the caught-up marker (the HW)
                if (r->baseOffset >= 0) marks.push_back(r->baseOffset);
                continue;
            }
            std::size_t pos = 0;
            while (pos + 61 <= r->recordBatch.size()) {
                rune::protocol::RecordBatchReader rd(r->recordBatch.data() + pos, r->recordBatch.size() - pos);
                for (int32_t k = 0; k <= rd.lastOffsetDelta(); ++k) offs.push_back(rd.baseOffset() + k);
                pos += static_cast<std::size_t>(rd.totalLength());
            }
        }
        return {offs, marks};
    }
};
}   // namespace

// A reader whose window is a small fraction of the catalog reads it to the end:
// it is paused whenever its window is full and resumed from its own cursor
// (every record exactly once, in order, one marker); nothing is ever held for it
// beyond its window; it is never ended; a fast reader alongside it finishes as
// if it were alone.  RED (pre-fix): no pause — every frame was handed to the
// session's spill, which outgrew its quota (the session was expired).
TEST(CatalogReadServerRegression, SlowReaderIsPausedAndResumedFromItsCursorToTheEndRegression) {
    ReadRig r("rd_flow");
    r.commitEvents(3000);                                          // ~0.3 MB
    FlowHarness h(r);
    h.sessions[1] = {/*window=*/24 * 1024, 0, /*drainPerPass=*/2 * 1024};   // slow: 12x smaller than the catalog
    h.sessions[2] = {/*window=*/1 << 22, 0, /*drainPerPass=*/1 << 22};      // fast
    r.server.subscribe(1, 1, -2);
    r.server.subscribe(2, 1, -2);
    int fastPasses = -1, slowPasses = -1;
    for (int p = 0; p < 20'000 && slowPasses < 0; ++p) {
        h.pass();
        if (fastPasses < 0 && !h.seen(2).second.empty()) fastPasses = p;
        if (slowPasses < 0 && !h.seen(1).second.empty()) slowPasses = p;
    }
    ASSERT_GE(slowPasses, 0) << "the slow reader reached the end";
    const auto [slowOffs, slowMarks] = h.seen(1);
    EXPECT_EQ(slowOffs, range(0, 3000)) << "every record exactly once, in order";
    EXPECT_EQ(slowMarks, (std::vector<int64_t>{3000}));
    const auto [fastOffs, fastMarks] = h.seen(2);
    EXPECT_EQ(fastOffs, range(0, 3000));
    EXPECT_LE(fastPasses, 5) << "the fast reader is not held back";
    EXPECT_GT(h.flow.pauses(), 10u) << "non-vacuous: the slow reader was paused, many times";
    EXPECT_EQ(h.flow.resumes(), h.flow.pauses());
    EXPECT_EQ(h.flow.paused(), 0u);
    EXPECT_LE(h.sessions[1].maxQueued, 24u * 1024u + rune::node::kCatalogReadCoalesceBytes + 256u)
        << "never more than its window (+ one frame) held for it: no growth";
    EXPECT_EQ(h.sessions[1].reliableFrames, 1u) << "only its SubscribeResponse took the reliable path";
    EXPECT_EQ(r.server.endedSubscriptions(), 0u) << "never ended (or expired) for being slow";
    EXPECT_EQ(r.server.subscriptions(), 2u);
    // Live after the backlog: a new event reaches it too.
    r.commitEvents(1);
    for (int p = 0; p < 50; ++p) h.pass();
    EXPECT_EQ(h.seen(1).first.back(), 3000);
    EXPECT_EQ(h.seen(1).second.size(), 1u) << "still one marker";
}

// A pause that drops the caught-up marker re-sends it (once) after the resume.
TEST(CatalogReadServerRegression, PausedMarkerIsSentAgainOnceAfterTheResumeRegression) {
    ReadRig r("rd_flow_marker");
    r.commitEvents(2);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{2}));
    const int64_t sub = r.subscriptionIdOf(7, 1);
    r.server.onPaused(7, sub, /*the marker's baseOffset=*/2, /*droppedMarker=*/true);
    EXPECT_STREQ(r.server.phaseName(sub), "Paused") << "acknowledged";
    ASSERT_FALSE(r.frames.empty());
    EXPECT_TRUE(r.frames.back().bytes.empty()) << "the pause ack is an empty frame of the subscription";
    EXPECT_EQ(r.frames.back().subscriptionId, sub);
    const auto before = r.frames.size();
    for (int i = 0; i < 5; ++i) r.server.pump();
    EXPECT_EQ(r.frames.size(), before) << "a paused subscription sends nothing";
    r.server.onResume(7, sub);
    r.server.pump();
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{2, 2})) << "the dropped marker, once more";
    r.server.pump();
    EXPECT_EQ(r.markers(7).size(), 2u);
}

// The control-thread flow: frames of a paused subscription in flight are dropped
// until its ack; another subscription is delivered meanwhile; a session gone
// clears its entry.
TEST(CatalogReadServerRegression, FlowDropsOnlyThePausedSubscriptionsFramesUntilItsAckRegression) {
    ReadRig r("rd_flow_unit");
    FlowHarness h(r);
    h.sessions[1] = {100, 0, 0};
    h.sessions[2] = {1 << 20, 0, 0};
    auto frame = [](int64_t s, int64_t sub, int64_t base, std::size_t n) {
        std::vector<uint8_t> w(24 + n, 0xEE);
        std::memcpy(w.data(), &s, 8);
        std::memcpy(w.data() + 8, &sub, 8);
        std::memcpy(w.data() + 16, &base, 8);
        return w;
    };
    auto feed = [&](const std::vector<uint8_t>& w) { h.flow.onFrame(w.data(), w.size(), h); };
    feed(frame(1, 5, 0, 80));     // fits
    feed(frame(1, 5, 1, 80));     // window full → paused at 1
    feed(frame(1, 5, 2, 80));     // in flight: dropped
    feed(frame(2, 6, 0, 80));     // another subscription: delivered
    ASSERT_EQ(h.ring.size(), 1u);
    EXPECT_TRUE(h.ring[0].pause);
    EXPECT_EQ(h.ring[0].base, 1);
    EXPECT_EQ(h.delivered[1].size(), 1u);
    EXPECT_EQ(h.delivered[2].size(), 1u);
    EXPECT_EQ(h.flow.dropped(), 2u);
    h.sessions[1].inFlight = 0;   // room — but no ack yet: no resume
    h.flow.service(h);
    EXPECT_EQ(h.ring.size(), 1u) << "never resumed before the ack (frames may still be in flight)";
    feed(frame(1, 5, -1, 0));     // the ack
    h.flow.service(h);
    ASSERT_EQ(h.ring.size(), 2u);
    EXPECT_FALSE(h.ring[1].pause) << "resumed";
    EXPECT_EQ(h.flow.paused(), 0u);
    feed(frame(1, 5, 1, 80));     // the re-read frame is delivered
    EXPECT_EQ(h.delivered[1].size(), 2u);
    // A paused session that goes away is cleared.
    h.sessions[1].inFlight = 100;
    feed(frame(1, 5, 2, 80));
    feed(frame(1, 5, -1, 0));
    h.sessions.erase(1);
    h.flow.service(h);
    EXPECT_EQ(h.flow.paused(), 0u);
}

// ─── Fix round 1 ─────────────────────────────────────────────────────────────

// C1: a follower's HW reads 0 after every leader change (and stays 0 while the
// catalog is leaderless).  A subscription mid-backlog must neither be told it is
// caught up at a lower offset nor lose its place: the backlog is served up to
// the committed floor already known, and the marker waits until the HW is
// current again — then exactly one, at the true HW.  RED: a marker at 0.
TEST(CatalogReadServerRegression, LeaderChangeMidBacklogNeverSendsAFalseCaughtUpMarkerRegression) {
    ReadRig r("rd_c1_marker");
    r.commitEvents(6);
    r.server.subscribe(9, 1, -1);                      // another reader: the server learns HW 6
    r.server.pump();
    r.egressBudget = 2;                                // only the SubscribeResponse + the epoch note go out
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    ASSERT_STREQ(r.server.phaseName(r.subscriptionIdOf(7, 1)), "Backlog") << "mid-backlog at 0";
    r.egressBudget = -1;
    r.replica().becomeFollower(ReadRig::kEpoch + 1, /*leaderless=*/-1);
    ASSERT_EQ(r.replica().highWatermark(), 0) << "precondition: the HW regressed with the leader change";
    for (int i = 0; i < 5; ++i) r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 6)) << "the committed backlog is still served (the floor)";
    EXPECT_TRUE(r.markers(7).empty()) << "never 'caught up' while the HW is not current";
    EXPECT_EQ(r.server.servedHighWatermark(), 6);
    r.replica().becomeLeader(ReadRig::kEpoch + 2, {ReadRig::kSelf}, 1);
    r.commitEvents(0);                                 // the new leader's pass: its HW is current
    r.server.pump();
    r.server.pump();
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{6})) << "exactly one marker, at the true HW";
}

// C1: a Subscribe from an explicit offset (a client resuming) or from latest
// during a leader change is clamped to the committed floor, never to the
// regressed 0.  RED: offsets 0.. replayed (duplicates), a marker at 0.
TEST(CatalogReadServerRegression, SubscribeDuringALeaderChangeIsNeverClampedBackRegression) {
    ReadRig r("rd_c1_clamp");
    r.commitEvents(6);
    r.server.subscribe(9, 1, -2);
    r.server.pump();
    r.replica().becomeFollower(ReadRig::kEpoch + 1, -1);
    r.server.subscribe(7, 1, /*resume from=*/4);
    r.server.subscribe(8, 1, /*latest=*/-1);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{4, 5})) << "from its cursor, never from 0";
    EXPECT_TRUE(r.deliveredOffsets(8).empty()) << "latest = the committed floor, not a replay from 0";
    EXPECT_TRUE(r.markers(7).empty());
    EXPECT_TRUE(r.markers(8).empty());
}

// C1: a log truncated below a HW already served (an unclean / deficient
// election upstream) ends every catalog subscription with the retriable
// NotLeader — the client resubscribes — never served past it, never rewound
// silently.  RED: the subscriptions stayed, silently.
TEST(CatalogReadServerRegression, TruncationBelowAServedHwEndsEverySubscriptionRetriablyRegression) {
    ReadRig r("rd_c1_trunc");
    r.commitEvents(6);
    r.server.subscribe(7, 1, -2);
    r.server.subscribe(8, 1, -1);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{6}));
    r.replica().becomeFollower(ReadRig::kEpoch + 1, -1);
    ASSERT_TRUE(r.replica().log()->truncateTo(3));
    ASSERT_EQ(r.replica().logEndOffset(), 3);
    r.server.pump();
    EXPECT_EQ(r.server.subscriptions(), 0u);
    EXPECT_EQ(r.server.truncationEnds(), 2u);
    for (int64_t s : {7, 8}) {
        const auto e = r.errors(s);
        ASSERT_EQ(e.size(), 1u) << s;
        EXPECT_EQ(e[0].code, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "retriable: resubscribe";
        EXPECT_EQ(e[0].correlationId, 1);
    }
    EXPECT_LE(r.server.servedHighWatermark(), 3) << "the floor follows the truncated log";
    r.server.subscribe(7, 2, -2);                      // the resubscribe is served from what the log holds
    r.server.pump();
    EXPECT_TRUE(r.subscribeAnswered(7, 2));
}

// M5: below minIsr the leader's HW is frozen; reads are served up to it and the
// marker is at it — appended-but-uncommitted events are never served.
TEST(CatalogReadServerRegression, BelowMinIsrServesOnlyUpToTheFrozenHwRegression) {
    ReadRig r("rd_minisr");
    r.commitEvents(3);
    r.replica().setMinIsrForTest(2);                   // ISR {self} < 2: nothing more commits
    const uint8_t key[8] = {1};
    const std::vector<uint8_t> v(64, 'u');
    ASSERT_GE(r.replica().appendKeyed(std::span<const uint8_t>(key, 8), v, /*acksAll=*/false), 0);
    r.replica().onLeaderLogAppended();
    (void)r.replica().doWork(9'000'000'000);
    ASSERT_EQ(r.replica().logEndOffset(), 4);
    ASSERT_EQ(r.replica().highWatermark(), 3);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 3));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{3}));
}

// I1: the first live Records frame right after the marker is back-pressured
// (same baseOffset as the marker).  RED: the subscription resumed as Backlog
// and sent a second marker.
TEST(CatalogReadServerRegression, DataFrameDroppedRightAfterTheMarkerNeverRepeatsTheMarkerRegression) {
    ReadRig r("rd_i1");
    r.commitEvents(2);
    FlowHarness h(r);
    h.sessions[7] = {/*window=*/1 << 20, 0, /*drainPerPass=*/1 << 20};
    r.server.subscribe(7, 1, -2);
    h.pass();
    ASSERT_EQ(h.seen(7).second, (std::vector<int64_t>{2}));
    h.sessions[7].inFlight = h.sessions[7].window;     // the reader's window is full
    h.sessions[7].drainPerPass = 0;
    r.commitEvents(1);                                 // Records{base=2} is back-pressured
    h.pass();
    ASSERT_EQ(h.flow.pauses(), 1u);
    h.sessions[7].drainPerPass = 1 << 20;
    for (int i = 0; i < 10; ++i) h.pass();
    EXPECT_EQ(h.seen(7).first, range(0, 3)) << "every record once";
    EXPECT_EQ(h.seen(7).second, (std::vector<int64_t>{2})) << "one marker, never a second";
    EXPECT_EQ(h.flow.resumes(), 1u);
}

// I1 (the other side): a dropped MARKER is re-sent once.
TEST(CatalogReadServerRegression, DroppedMarkerThroughTheFlowIsSentOnceAgainRegression) {
    // The frame sizes of this scenario (a wide window): note, records, marker.
    std::size_t window = 0;
    {
        ReadRig r1("rd_i1_marker1");
        r1.commitEvents(2);
        FlowHarness h1(r1);
        h1.sessions[7] = {1 << 20, 0, 0};
        r1.server.subscribe(7, 1, -2);
        h1.pass();
        ASSERT_EQ(h1.delivered[7].size(), 4u) << "SubscribeResponse, note, records, marker";
        window = h1.delivered[7][1].size() + h1.delivered[7][2].size() + 8;
    }
    // Drop exactly the marker: the window holds the note and the records frame only.
    ReadRig r2("rd_i1_marker2");
    r2.commitEvents(2);
    FlowHarness h2(r2);
    h2.sessions[7] = {window, 0, 0};                   // the note + records frame fit, the marker does not
    r2.server.subscribe(7, 1, -2);
    h2.pass();
    ASSERT_EQ(h2.flow.pauses(), 1u);
    ASSERT_TRUE(h2.seen(7).second.empty()) << "the marker was dropped";
    h2.sessions[7].drainPerPass = 1 << 20;
    for (int i = 0; i < 10; ++i) h2.pass();
    EXPECT_EQ(h2.seen(7).first, range(0, 2));
    EXPECT_EQ(h2.seen(7).second, (std::vector<int64_t>{2})) << "re-sent, once";
}

// M1: a subscription removed while its pause is unacknowledged (egress ring
// full) still gets its ack sent; the control entry of a session gone before
// its ack is cleared.  RED: the control entry leaked (paused() stayed 1).
TEST(CatalogReadServerRegression, PauseOfARemovedSubscriptionNeverLeaksAFlowEntryRegression) {
    ReadRig r("rd_m1");
    r.commitEvents(2);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const int64_t sub = r.subscriptionIdOf(7, 1);
    r.egressFull = true;
    r.server.onPaused(7, sub, 0, false);
    ASSERT_STREQ(r.server.phaseName(sub), "PausePending");
    r.server.unsubscribe(7, sub);
    r.egressFull = false;
    const auto before = r.frames.size();
    r.server.pump();
    ASSERT_EQ(r.frames.size(), before + 1);
    EXPECT_TRUE(r.frames.back().bytes.empty()) << "the owed pause ack";
    EXPECT_EQ(r.frames.back().subscriptionId, sub);

    // Control side: AwaitingAck for a session that closed (its ack never comes).
    ReadRig r2("rd_m1b");
    FlowHarness h(r2);
    h.sessions[5] = {10, 10, 0};
    std::vector<uint8_t> w(24 + 80, 0);
    const int64_t s5 = 5, sid = 77, base = 0;
    std::memcpy(w.data(), &s5, 8); std::memcpy(w.data() + 8, &sid, 8); std::memcpy(w.data() + 16, &base, 8);
    h.flow.onFrame(w.data(), w.size(), h);
    ASSERT_EQ(h.flow.paused(), 1u);
    h.sessions.erase(5);
    h.flow.service(h);
    EXPECT_EQ(h.flow.paused(), 0u) << "cleared: the session is gone";
}

// M2: readers in backlog share a slow egress fairly — a later subscriber is not
// starved behind earlier ones.  RED: the first subscription took every frame.
TEST(CatalogReadServerRegression, BacklogReadersShareTheEgressRoundRobinRegression) {
    ReadRig r("rd_m2");
    r.commitEvents(3000);
    r.server.subscribe(1, 1, -2);
    r.server.subscribe(2, 1, -2);
    r.server.subscribe(3, 1, -2);
    for (int p = 0; p < 6; ++p) {
        r.egressBudget = 1;                            // one frame per pass
        r.server.pump();
    }
    r.egressBudget = -1;
    for (int64_t s : {1, 2, 3}) EXPECT_GE(r.records(s).size(), 2u) << "session " << s << " progressed";
}

// M3: the owed answers never outgrow their reservation — catalog intake pauses first.
TEST(CatalogReadServerRegression, OwedAnswersPauseIntakeInsteadOfGrowingRegression) {
    ReadRig r("rd_m3", false, /*maxSubs=*/2);
    r.server.subscribe(1, 1, -2);
    r.server.subscribe(2, 1, -2);
    r.server.pump();
    r.egressFull = true;
    int refused = 0;
    for (int c = 0; c < 1000 && !r.server.intakePaused(); ++c, ++refused) r.server.subscribe(3, c, -2);
    EXPECT_TRUE(r.server.intakePaused()) << "the intake pauses at a fixed bound";
    EXPECT_LT(refused, 64);
    r.egressFull = false;
    r.server.pump();
    EXPECT_FALSE(r.server.intakePaused());
    for (int c = 0; c < refused; ++c) EXPECT_EQ(r.subscribeError(3, c), pc::ErrorCode::Internal) << c;
}

// Fix round 2 (re-review N1): a truncation below the served floor (unclean /
// survivor election, deficient leader) ends the reads EXACTLY ONCE; the floor
// restarts at the post-truncation committed HW, so a resubscribe is served from
// the new history — never a NotLeader loop, never a byte above the current
// committed HW (the new leader's appended-but-uncommitted tail).  RED: a floor
// fed by a stale applied offset rose back to 6 every pass — every resubscribe
// ended again, and the new lineage's offsets 4.. were served uncommitted.
TEST(CatalogReadServerRegression, TruncationEndsReadsOnceAndTheResubscribeServesOnlyTheNewCommittedHistoryRegression) {
    ReadRig r("rd_n1", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{6}));
    r.replica().truncateLogForTest(3);                 // a deficient leader's lineage ends at 3
    ASSERT_EQ(r.replica().logEndOffset(), 3);
    r.server.pump();
    ASSERT_EQ(r.server.subscriptions(), 0u);
    ASSERT_EQ(r.errors(7).size(), 1u);
    EXPECT_EQ(r.errors(7)[0].code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    // A resubscribe right away (the log still ends at 3): served, never ended again.
    r.server.subscribe(7, 3, -2);
    for (int i = 0; i < 10; ++i) r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 3));
    ASSERT_EQ(r.errors(7).size(), 1u) << "no NotLeader loop on a quiet catalog";
    r.server.unsubscribe(7, r.subscriptionIdOf(7, 3));
    // The new leader appends its own 3..8; only up to 4 is committed.
    r.followerAppendWithHighWatermark(3, /*hw=*/4);   // the source holds 9: the follower gets 3..8
    ASSERT_EQ(r.replica().logEndOffset(), 9);
    ASSERT_EQ(r.replica().highWatermark(), 4);
    r.server.subscribe(7, 2, -2);                      // the client resubscribes
    for (int i = 0; i < 20; ++i) r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 2));
    EXPECT_EQ(r.errors(7).size(), 1u) << "ended exactly once — no NotLeader loop";
    EXPECT_EQ(r.server.subscriptions(), 1u);
    std::vector<int64_t> second;
    for (const auto& rec : r.records(7))
        if (rec.correlationId == 2 && !rec.recordBatch.empty()) {
            std::size_t pos = 0;
            while (pos + 61 <= rec.recordBatch.size()) {
                rune::protocol::RecordBatchReader rd(rec.recordBatch.data() + pos, rec.recordBatch.size() - pos);
                for (int32_t k = 0; k <= rd.lastOffsetDelta(); ++k) second.push_back(rd.baseOffset() + k);
                pos += static_cast<std::size_t>(rd.totalLength());
            }
        }
    EXPECT_EQ(second, range(0, 4)) << "the new committed history, nothing above its HW";
    EXPECT_EQ(r.markers(7).back(), 4) << "caught up at the new committed HW";
    EXPECT_EQ(r.server.servedHighWatermark(), 4);
    EXPECT_EQ(r.server.truncationEnds(), 1u);
}

// Fix round 2 (re-review N2): a truncation below the floor that REGROWS past it
// before the read server's next pass (the LEO alone looks fine) still ends the
// reads once — the replica's log generation + its cut point tell.  RED: the
// subscriptions kept their old-lineage prefix without being told.
TEST(CatalogReadServerRegression, TruncateAndRegrowWithinOnePassStillEndsTheReadsOnceRegression) {
    ReadRig r("rd_n2", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{6}));
    r.replica().truncateLogForTest(3);
    r.followerAppendWithHighWatermark(3, /*hw=*/4);    // the new leader's 3..8, before the next pass
    ASSERT_GE(r.replica().logEndOffset(), 6) << "precondition: the LEO is back above the floor";
    r.server.pump();
    EXPECT_EQ(r.server.subscriptions(), 0u);
    ASSERT_EQ(r.errors(7).size(), 1u);
    EXPECT_EQ(r.errors(7)[0].code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(r.server.servedHighWatermark(), 3) << "restarted at the cut (the new committed HW is not above it yet)";
    for (int i = 0; i < 5; ++i) r.server.pump();
    EXPECT_EQ(r.server.truncationEnds(), 1u) << "once";
}

// Fix round 3 (F3b): more closes than the gateway can queue (> 1024) while the
// admin's request ring is stalled — those closes never reach the read server.
// Every slot is still reclaimed: each subscription's liveness probe finds its
// session gone (the control thread's session table) within one interval.  No
// allocation in the probe path.  RED: the closed sessions' subscriptions held
// their slots forever (subscriptions() stayed 1100; new Subscribes refused).
TEST(CatalogReadServerRegression, ClosesBeyondTheGatewayQueueAreReclaimedByTheLivenessSweepRegression) {
    ReadRig r("rd_f3b", false, /*maxSubs=*/1200);
    r.commitEvents(1);
    FlowHarness h(r);
    constexpr int64_t kSessions = 1100;
    for (int64_t s = 1; s <= kSessions; ++s) {
        h.sessions[s] = {1 << 20, 0, 1 << 20};
        r.server.subscribe(s, 1, -2);
    }
    h.sessions[5000] = {1 << 20, 0, 1 << 20};          // one session stays
    r.server.subscribe(5000, 1, -2);
    for (int i = 0; i < 5; ++i) h.pass();
    ASSERT_EQ(r.server.subscriptions(), static_cast<std::size_t>(kSessions + 1));
    // Every one of the 1100 sessions closes; none of the closes reaches the admin.
    for (int64_t s = 1; s <= kSessions; ++s) h.sessions.erase(s);
    for (int i = 0; i < 5; ++i) h.pass();
    EXPECT_EQ(r.server.subscriptions(), static_cast<std::size_t>(kSessions + 1)) << "before the probe interval";
    h.nowNs += rune::node::CatalogReadServer::kProbeIntervalNs + 1;
    for (int i = 0; i < 5; ++i) h.pass();
    EXPECT_EQ(r.server.subscriptions(), 1u) << "every closed session's slot reclaimed";
    EXPECT_EQ(h.flow.sessionsGone(), static_cast<uint64_t>(kSessions));
    EXPECT_EQ(h.seen(5000).second, (std::vector<int64_t>{1})) << "the live session keeps its read";
    h.sessions[9000] = {1 << 20, 0, 1 << 20};
    r.server.subscribe(9000, 1, -2);
    h.pass();
    EXPECT_EQ(r.server.subscriptions(), 2u) << "slots are free again";
    EXPECT_EQ(r.server.refusedSubscribes(), 0u);
}

// ─── Task 13 fix round 2: the HISTORY EPOCH ─────────────────────────────────────
// A client caches (prefix, epoch).  Every catalog stream opens with the epoch
// note (Records{-1, epoch}) before any record, every caught-up marker carries the
// same epoch, and the epoch changes — durably — on every truncation below a
// served high watermark.  A restart keeps it unless the log no longer holds the
// batch it was anchored at.

namespace {
/// A second server over the same replica (what a broker restart reads back).
struct Restarted {
    std::vector<ReadRig::Frame> frames;
    rune::node::CatalogReadServer server;
    explicit Restarted(ReadRig& r)
        : server(r.replica(),
                 [this](const rune::node::CatalogReadFrameHead& h, std::span<const uint8_t> f) {
                     frames.push_back(ReadRig::Frame{h.session, std::vector<uint8_t>(f.begin(), f.end()),
                                                     h.subscriptionId, h.baseOffset});
                     return true;
                 },
                 64, int64_t{ReadRig::kSelf + 1} << 40) {}
};
std::string epochFileOf(ReadRig& r) {
    return (std::filesystem::path{r.replica().log()->dir()} / rune::node::kCatalogHistoryEpochFile).string();
}
}   // namespace

// RED (before fix round 2): the marker was an empty Records — no epoch anywhere;
// a client could not tell one committed history from another.
TEST(CatalogReadServerRegression, EveryStreamOpensWithItsEpochNoteAndTheMarkerCarriesTheSameEpochRegression) {
    ReadRig r("rd_ep_note");
    r.commitEvents(4);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e = r.server.historyEpoch();
    const auto ef = r.epochFrames(7);
    ASSERT_EQ(ef.size(), 2u) << "one note, one marker";
    EXPECT_EQ(ef[0], (std::pair<int64_t, uint64_t>{-1, e})) << "the note: baseOffset -1, the epoch";
    EXPECT_EQ(ef[1], (std::pair<int64_t, uint64_t>{4, e})) << "the marker: the HW, the same epoch";
    // The note precedes every record of the stream.
    const auto t = r.records(7);
    ASSERT_GE(t.size(), 3u);
    EXPECT_EQ(t.front().recordBatch.size(), 8u);
    EXPECT_EQ(t.front().baseOffset, -1);
    // Durable before served: the file names this epoch and the served floor.
    const auto d = rune::node::readCatalogHistoryEpoch(epochFileOf(r));
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->epoch, e);
    EXPECT_EQ(d->servedFloor, 4);
    EXPECT_EQ(d->anchorBase, 3);
    EXPECT_EQ(r.server.epochBumps(), 0u);
}

// RED: after a truncation below a served HW and a regrow, a resubscribe was served
// the new history under nothing that told the client it changed.
TEST(CatalogReadServerRegression, ATruncationBelowAServedHwChangesTheEpochDurablyRegression) {
    ReadRig r("rd_ep_trunc", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    ASSERT_EQ(r.markers(7), (std::vector<int64_t>{6}));
    r.sourceTruncate(3);
    r.replica().truncateLogForTest(3);
    r.followerAppendWithHighWatermark(4, /*hw=*/7);    // a different lineage 3..6, regrown past the old HW
    r.server.pump();
    ASSERT_EQ(r.errors(7).size(), 1u) << "the read ended once (NotLeader)";
    EXPECT_EQ(r.server.epochBumps(), 1u);
    EXPECT_NE(r.server.historyEpoch(), e0);
    const auto d = rune::node::readCatalogHistoryEpoch(epochFileOf(r));
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->epoch, r.server.historyEpoch()) << "the new epoch is durable at once";
    r.server.subscribe(7, 2, -2);
    for (int i = 0; i < 3; ++i) r.server.pump();
    const auto ef = r.epochFrames(7);
    ASSERT_GE(ef.size(), 4u);
    EXPECT_EQ(ef[ef.size() - 2], (std::pair<int64_t, uint64_t>{-1, r.server.historyEpoch()}));
    EXPECT_EQ(ef.back(), (std::pair<int64_t, uint64_t>{7, r.server.historyEpoch()}));
}

// The identical-bytes regrow: the replica re-fetches the very same bytes after the
// cut.  The history may still differ below (nothing at the cut proves the prefix):
// the epoch changes all the same.
TEST(CatalogReadServerRegression, AnIdenticalBytesRegrowStillChangesTheEpochRegression) {
    ReadRig r("rd_ep_same", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    r.replica().truncateLogForTest(3);
    r.followerAppendWithHighWatermark(0, /*hw=*/6);    // the source's own 3..5 again: identical bytes
    ASSERT_EQ(r.replica().logEndOffset(), 6);
    r.server.pump();
    EXPECT_EQ(r.server.epochBumps(), 1u);
    EXPECT_NE(r.server.historyEpoch(), e0);
}

// Only a cut below what was SERVED changes the epoch: a follower dropping its
// uncommitted tail (the normal case) keeps it.
TEST(CatalogReadServerRegression, ACutOfTheUncommittedTailKeepsTheEpochRegression) {
    ReadRig r("rd_ep_tail", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(4);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    r.replica().truncateLogForTest(5);
    r.server.pump();
    EXPECT_EQ(r.server.epochBumps(), 0u);
    EXPECT_EQ(r.server.historyEpoch(), e0);
    EXPECT_TRUE(r.errors(7).empty());
}

// With no subscription at all the cut still changes the epoch (durably): a crash
// later must not serve the regrown log under the old one.
TEST(CatalogReadServerRegression, ACutWithNoSubscriberStillChangesTheEpochDurablyRegression) {
    ReadRig r("rd_ep_nosub", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    r.server.unsubscribe(7, -1);
    const uint64_t e0 = r.server.historyEpoch();
    r.replica().truncateLogForTest(2);
    r.server.pump();
    EXPECT_EQ(r.server.epochBumps(), 1u);
    const auto d = rune::node::readCatalogHistoryEpoch(epochFileOf(r));
    ASSERT_TRUE(d.has_value());
    EXPECT_NE(d->epoch, e0);
    EXPECT_EQ(d->epoch, r.server.historyEpoch());
}

// A restart reads the epoch back (no re-read forced on every client), and a cut
// that happened while the old server could not see it — truncate-and-regrow
// with other bytes, then a crash before its pass — is caught by the anchor.
TEST(CatalogReadServerRegression, TheEpochSurvivesARestartUnlessItsAnchorBatchChangedRegression) {
    ReadRig r("rd_ep_restart", /*asFollower=*/true);
    r.followerAppend(6);
    r.followerSetHighWatermark(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    {
        Restarted b(r);
        EXPECT_EQ(b.server.historyEpoch(), e0) << "kept across a restart";
        EXPECT_EQ(b.server.epochBumps(), 0u);
    }
    // The old server never pumps again: the cut is visible only to a restart.
    r.sourceTruncate(3);
    r.replica().truncateLogForTest(3);
    r.followerAppendWithHighWatermark(4, /*hw=*/7);
    Restarted b(r);
    EXPECT_EQ(b.server.epochBumps(), 1u) << "the anchor batch at offset 5 holds other bytes now";
    EXPECT_NE(b.server.historyEpoch(), e0);
    b.server.subscribe(9, 1, -2);
    b.server.pump();
    const auto d = rune::node::readCatalogHistoryEpoch(epochFileOf(r));
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->epoch, b.server.historyEpoch()) << "durable before anything is served under it";
}

// A log that LOST the anchored batch (unsynced bytes gone in a crash) changes it too.
TEST(CatalogReadServerRegression, ALogShorterThanTheServedFloorAtBootChangesTheEpochRegression) {
    ReadRig r("rd_ep_lost");
    r.commitEvents(6);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    r.replica().becomeFollower(ReadRig::kEpoch + 1, -1);
    ASSERT_TRUE(r.replica().log()->truncateTo(4));
    Restarted b(r);
    EXPECT_EQ(b.server.epochBumps(), 1u);
    EXPECT_NE(b.server.historyEpoch(), e0);
}

// An absent or damaged epoch file is never trusted: a fresh epoch (every client re-reads).
TEST(CatalogReadServerRegression, ADamagedEpochFileStartsAFreshEpochRegression) {
    ReadRig r("rd_ep_damaged");
    r.commitEvents(2);
    r.server.subscribe(7, 1, -2);
    r.server.pump();
    const uint64_t e0 = r.server.historyEpoch();
    {
        std::FILE* f = std::fopen(epochFileOf(r).c_str(), "r+b");
        ASSERT_NE(f, nullptr);
        std::fseek(f, 12, SEEK_SET);
        std::fputc(0x5a, f);
        std::fclose(f);
    }
    ASSERT_FALSE(rune::node::readCatalogHistoryEpoch(epochFileOf(r)).has_value()) << "the checksum fails";
    Restarted b(r);
    EXPECT_NE(b.server.historyEpoch(), e0);
    std::filesystem::remove(epochFileOf(r));
    Restarted c(r);
    EXPECT_NE(c.server.historyEpoch(), e0);
    EXPECT_NE(c.server.historyEpoch(), b.server.historyEpoch()) << "never a re-used value";
}

// The epoch cannot be made durable (a read-only catalog dir): nothing is served —
// no note, no record, no marker — until it can; then the stream starts normally.
TEST(CatalogReadServerRegression, AnEpochThatCannotBeMadeDurableServesNothingUntilItIsRegression) {
    ReadRig r("rd_ep_ro");
    r.commitEvents(3);
    const std::filesystem::path dir = r.replica().log()->dir();
    std::filesystem::permissions(dir, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
    r.server.subscribe(7, 1, -2);
    for (int i = 0; i < 3; ++i) r.server.pump();
    EXPECT_TRUE(r.subscribeAnswered(7, 1)) << "the Subscribe is answered";
    EXPECT_TRUE(r.epochFrames(7).empty()) << "but nothing is served under a non-durable epoch";
    EXPECT_TRUE(r.deliveredOffsets(7).empty());
    EXPECT_GT(r.server.epochWriteFailures(), 0u);
    std::filesystem::permissions(dir, std::filesystem::perms::owner_all);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7), range(0, 3));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{3}));
}

// Flow control: a dropped NOTE is sent again (it is never a marker to the control
// thread), and only then the stream; a dropped data frame never repeats the note.
TEST(CatalogReadServerRegression, ADroppedNoteIsSentAgainAndADroppedDataFrameNeverRepeatsItRegression) {
    {
        ReadRig r("rd_ep_flow_note");
        r.commitEvents(2);
        FlowHarness h(r);
        h.sessions[7] = {/*window=*/1 << 20, /*inFlight=*/1 << 20, /*drainPerPass=*/0};   // full: the note drops
        r.server.subscribe(7, 1, -2);
        h.pass();
        ASSERT_EQ(h.flow.pauses(), 1u);
        h.sessions[7].drainPerPass = 1 << 20;
        for (int i = 0; i < 10; ++i) h.pass();
        int notes = 0, marks = 0;
        for (const auto& f : h.delivered[7])
            if (auto rec = pc::Records::decode(f.data(), f.size()); rec && rec->recordBatch.size() == 8)
                (rec->baseOffset < 0 ? notes : marks) += 1;
        EXPECT_EQ(notes, 1) << "the dropped note, once more";
        EXPECT_EQ(marks, 1);
        EXPECT_EQ(h.seen(7).first, range(0, 2));
        EXPECT_FALSE(rune::node::CatalogReadFlow::isCaughtUpMarker(h.delivered[7][1].data(), h.delivered[7][1].size()));
    }
    {
        ReadRig r("rd_ep_flow_data");
        r.commitEvents(2);
        FlowHarness h(r);
        h.sessions[7] = {/*window=*/1 << 20, 0, 1 << 20};
        r.server.subscribe(7, 1, -2);
        h.pass();
        h.sessions[7].inFlight = h.sessions[7].window;     // the next data frame drops
        h.sessions[7].drainPerPass = 0;
        r.commitEvents(1);
        h.pass();
        ASSERT_EQ(h.flow.pauses(), 1u);
        h.sessions[7].drainPerPass = 1 << 20;
        for (int i = 0; i < 10; ++i) h.pass();
        int notes = 0;
        for (const auto& f : h.delivered[7])
            if (auto rec = pc::Records::decode(f.data(), f.size()); rec && rec->recordBatch.size() == 8 && rec->baseOffset < 0)
                ++notes;
        EXPECT_EQ(notes, 1) << "the client holds the note: a data-frame resume never repeats it";
        EXPECT_EQ(h.seen(7).first, range(0, 3));
    }
}
