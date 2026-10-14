// CatalogReadServerTest.cpp — catalog history Task 12 (spec §7, ruling 10, F19,
// F29): any broker serves a Subscribe on `__catalog` from its own replica, up to
// its own HW, then ONE caught-up marker, then live commits.  Every Subscribe is
// answered (from 0, the middle, the end, above the HW, beyond the cap, a re-send);
// a full egress never loses a frame or a position; a subscription that cannot be
// served is ENDED explicitly (never a silent stall).
#include "support/CatalogReadRig.h"

#include <gtest/gtest.h>

#include <cstring>
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
    struct Report { bool pause; int64_t session, subId, base; };
    std::vector<Report>           ring;     // control → admin
    std::map<int64_t, std::vector<std::vector<uint8_t>>> delivered;   // session → client frames

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
    bool postPaused(int64_t s, int64_t sub, int64_t base) { ring.push_back({true, s, sub, base}); return true; }
    bool postResume(int64_t s, int64_t sub) { ring.push_back({false, s, sub, -1}); return true; }

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
        for (const Report& x : r) {
            if (x.pause) rig.server.onPaused(x.session, x.subId, x.base);
            else         rig.server.onResume(x.session, x.subId);
        }
        rig.server.pump();
        flow.service(*this);
        for (auto& [id, s] : sessions) s.inFlight -= std::min(s.inFlight, s.drainPerPass);
    }
    /// Every record offset / marker a session received.
    std::pair<std::vector<int64_t>, std::vector<int64_t>> seen(int64_t s) {
        std::vector<int64_t> offs, marks;
        for (const auto& f : delivered[s]) {
            auto r = pc::Records::decode(f.data(), f.size());
            if (!r) continue;
            if (r->recordBatch.empty()) { marks.push_back(r->baseOffset); continue; }
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
    r.server.onPaused(7, sub, /*the marker's baseOffset=*/2);
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
