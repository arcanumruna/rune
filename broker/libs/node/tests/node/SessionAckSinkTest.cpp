// SessionAckSinkTest.cpp — worker-side ack publication table + per-session hold
// state machines (spec §3.4, G-A6).  Fake publication policy; NO clock: the sink
// is time-independent (fix round 1, C1/C2) — acks drop only on state events.
#include <rune/node/SessionAckSink.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace rune::node;
namespace pc = rune::protocol::client;

namespace {
struct FakePub { int id = 0; };
constexpr int64_t kFatal = -4;   // == SessionAckSinkT<>::kClaimFatal (static_assert below)
struct FakePolicy {
    using PubHandle = std::shared_ptr<FakePub>;
    struct State {
        int64_t nextReg = 1;
        std::map<int64_t, std::pair<std::string, int32_t>> added;
        bool findReady = true, findError = false;
        bool errorOnceThenUnknown = false;                      // Aeron: find throws once, then the id is gone
        std::set<int64_t> erroredRegs;
        std::set<int> blocked;                                  // pub ids that back-pressure
        std::set<int> closed;                                   // pub ids that are CLOSED (fatal)
        std::set<int> notConnected;                             // pub ids whose subscriber is gone (NOT_CONNECTED)
        std::vector<std::pair<int, std::vector<uint8_t>>> sent; // (pub id, frame)
        std::vector<int64_t> foundRegs;                         // registrations find() resolved
    };
    std::shared_ptr<State> s = std::make_shared<State>();
    int64_t add(std::string_view ch, int32_t stream) noexcept {
        const int64_t r = s->nextReg++;
        s->added[r] = {std::string(ch), stream};
        return r;
    }
    PubFind find(int64_t reg, PubHandle& out) noexcept {
        if (s->errorOnceThenUnknown)
            return s->erroredRegs.insert(reg).second ? PubFind::Error : PubFind::Pending;
        if (s->findError) return PubFind::Error;
        if (!s->findReady) return PubFind::Pending;
        out = std::make_shared<FakePub>(FakePub{static_cast<int>(reg)});
        s->foundRegs.push_back(reg);
        return PubFind::Ready;
    }
    template <class E> int64_t claim(PubHandle& p, std::size_t len, E&& enc) noexcept {
        if (s->closed.count(p->id)) return kFatal;    // CLOSED / MAX_POSITION
        if (s->notConnected.count(p->id)) return -1;            // NOT_CONNECTED
        if (s->blocked.count(p->id)) return -2;                 // BACK_PRESSURED
        std::vector<uint8_t> b(len);
        enc(b.data());
        s->sent.push_back({p->id, std::move(b)});
        return 1;
    }
};
using Sink = SessionAckSinkT<FakePolicy>;
static_assert(Sink::kClaimFatal == kFatal);
AckEntry ack(int64_t sid, int64_t corr, int64_t base = 0) { return AckEntry{sid, corr, base, 0, 0}; }

// One pass with the lifecycle ring fully consumed (producer == consumer).
void pass(Sink& s) { s.flush(0, 0); }

// Announce + resolve (two flushes: Announced→Resolving, Resolving→Ready).
void ready(Sink& s, int64_t sid) {
    ASSERT_TRUE(s.announce(sid, "aeron:udp?endpoint=127.0.0.1:1", 7));
    pass(s);
    pass(s);
}
int64_t corrOf(const std::vector<uint8_t>& f) { return pc::ProduceResponse::decode(f.data(), f.size())->correlationId; }
} // namespace

TEST(SessionAckSinkRegression, AnnounceResolvesThenSendsEncodedProduceResponse) {
    FakePolicy pol; auto st = pol.s;
    Sink s(pol);
    ASSERT_TRUE(s.announce(7, "aeron:udp?endpoint=h:1", 42));
    ASSERT_TRUE(s.enqueue(AckEntry{7, 100, 5, 2, 0}));
    pass(s);                                                // Announced → Resolving; ack held
    EXPECT_TRUE(st->sent.empty());
    EXPECT_TRUE(s.holding(7)) << "G-A6: a session whose acks wait is paused";
    pass(s);                                                // Resolving → Ready; ack sent
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_FALSE(s.holding(7));
    EXPECT_EQ(s.heldAcks(), 0u);
    EXPECT_EQ(st->added.begin()->second.first, "aeron:udp?endpoint=h:1");
    EXPECT_EQ(st->added.begin()->second.second, 42);
    auto r = pc::ProduceResponse::decode(st->sent[0].second.data(), st->sent[0].second.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->sessionId, 7); EXPECT_EQ(r->correlationId, 100);
    EXPECT_EQ(r->baseOffset, 5); EXPECT_EQ(r->partition, 2); EXPECT_EQ(r->errorCode, 0);
}

TEST(SessionAckSinkRegression, CoalescesEveryAckOfAPassOnTheSessionPublication) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    s.enqueue(ack(7, 1)); s.enqueue(ack(7, 2)); s.enqueue(ack(8, 3)); s.enqueue(ack(7, 4));
    EXPECT_TRUE(st->sent.empty()) << "nothing leaves before the pass flush";
    pass(s);
    ASSERT_EQ(st->sent.size(), 4u);
    std::vector<int64_t> corrs7;
    for (auto& [pub, f] : st->sent)
        if (auto r = pc::ProduceResponse::decode(f.data(), f.size()); r && r->sessionId == 7) corrs7.push_back(r->correlationId);
    EXPECT_EQ(corrs7, (std::vector<int64_t>{1, 2, 4}));
}

TEST(SessionAckSinkRegression, BackPressureHoldsInOrderAndPausesOnlyThatSession) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    st->blocked.insert(1);                                  // session 7's pub (reg 1)
    s.enqueue(ack(7, 10)); s.enqueue(ack(8, 20)); s.enqueue(ack(7, 11));
    pass(s);
    EXPECT_TRUE(s.holding(7)) << "G-A6: back-pressured acks pause THAT client";
    EXPECT_FALSE(s.holding(8)) << "... and only that client";
    EXPECT_EQ(s.heldAcks(), 2u);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(corrOf(st->sent[0].second), 20);
    st->blocked.clear();
    pass(s);
    ASSERT_EQ(st->sent.size(), 3u);
    EXPECT_EQ(corrOf(st->sent[1].second), 10);
    EXPECT_EQ(corrOf(st->sent[2].second), 11);
    EXPECT_FALSE(s.holding(7));
}

TEST(SessionAckSinkRegression, CloseWhileHoldingDropsItsAcksAndResumes) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    st->blocked.insert(1);
    s.enqueue(ack(7, 10));
    pass(s);
    ASSERT_TRUE(s.holding(7));
    s.close(7);
    pass(s);
    EXPECT_EQ(s.heldAcks(), 0u) << "a closed session's acks must not stay queued";
    EXPECT_EQ(s.droppedForTest(), 1u);
}

// Fix round 1 (C2): no ack of an OPEN session is ever dropped on time — a slow
// or stalled client keeps every ack, however many passes go by, and gets them
// all, in order, once it reads again.  (Was: dropped after kStallDropNs, the
// session silently Failed while the runtime kept it alive.)
TEST(SessionAckSinkRegression, BlockedSessionKeepsItsAcksWithoutAnyTimer) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    st->blocked.insert(1);
    s.enqueue(ack(7, 1));
    for (int i = 0; i < 100'000; ++i) pass(s);
    s.enqueue(ack(8, 2));
    pass(s);
    ASSERT_EQ(st->sent.size(), 1u) << "other sessions keep flowing";
    EXPECT_TRUE(s.holding(7));
    ASSERT_NE(s.stateForTest(7), nullptr);
    EXPECT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(7))) << "never failed by a timer";
    st->blocked.clear();
    pass(s);
    ASSERT_EQ(st->sent.size(), 2u);
    EXPECT_EQ(corrOf(st->sent[1].second), 1);
    EXPECT_EQ(s.droppedForTest(), 0u);
}

TEST(SessionAckSinkRegression, ResolvingSessionNeverFailsOnTime) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findReady = false;
    ASSERT_TRUE(s.announce(7, "c", 1));
    s.enqueue(ack(7, 1));
    for (int i = 0; i < 10'000; ++i) pass(s);
    EXPECT_TRUE(std::holds_alternative<Sink::PubResolving>(*s.stateForTest(7)));
    EXPECT_TRUE(s.holding(7));
    st->findReady = true;
    pass(s);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(s.droppedForTest(), 0u);
}

// Fix round 1 (C2): a publication that can never deliver is REPORTED (the
// worker emits SessionFailed; the runtime closes the session) — exactly once,
// retried while the report cannot be delivered, never silent.
TEST(SessionAckSinkRegression, ClosedPublicationFailsAtOnceAndIsReportedExactlyOnce) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    st->closed.insert(1);
    s.enqueue(ack(7, 1)); s.enqueue(ack(7, 2));
    pass(s);
    ASSERT_NE(s.stateForTest(7), nullptr);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    EXPECT_EQ(s.droppedForTest(), 2u);
    EXPECT_FALSE(s.holding(7)) << "nothing held for a failed session";
    EXPECT_EQ(s.unreportedFailures(), 1u);
    std::vector<int64_t> reported;
    EXPECT_EQ(s.reportFailures([&](int64_t) { return false; }), 0) << "return ring full: retried later";
    EXPECT_EQ(s.unreportedFailures(), 1u);
    EXPECT_EQ(s.reportFailures([&](int64_t sid) { reported.push_back(sid); return true; }), 1);
    EXPECT_EQ(s.reportFailures([&](int64_t sid) { reported.push_back(sid); return true; }), 0);
    EXPECT_EQ(reported, (std::vector<int64_t>{7}));
}

TEST(SessionAckSinkRegression, AddAndFindErrorsAreReportedAndCloseForgetsAnUnreportedFailure) {
    {
        FakePolicy pol; auto st = pol.s; Sink s(pol);
        st->findError = true;
        ASSERT_TRUE(s.announce(7, "c", 1));
        pass(s); pass(s);
        EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
        EXPECT_EQ(s.unreportedFailures(), 1u);
        s.close(7);                                        // the runtime closed it meanwhile
        EXPECT_EQ(s.unreportedFailures(), 0u) << "nothing left to report for a closed session";
        EXPECT_EQ(s.reportFailures([](int64_t) { ADD_FAILURE(); return true; }), 0);
    }
    {
        struct AddFails : FakePolicy {
            int64_t add(std::string_view, int32_t) noexcept { return -1; }
        };
        AddFails pol; SessionAckSinkT<AddFails> s(pol);
        ASSERT_TRUE(s.announce(9, "c", 1));
        s.flush(0, 0);
        int64_t got = 0;
        EXPECT_EQ(s.reportFailures([&](int64_t sid) { got = sid; return true; }), 1);
        EXPECT_EQ(got, 9);
    }
}

// Fix round 1 (I1): a frame can be visible before its session's announcement is
// drained (aeron:ipc commits straight into the log buffer).  Its ack is HELD
// until the lifecycle ring is consumed past the producer position sampled at
// the flush that first saw it — then a still-unknown session is truly gone.
TEST(SessionAckSinkRegression, UnknownSessionAckHeldUntilTheLifecycleRingIsConsumedPastIt) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.enqueue(ack(9, 1));
    s.flush(/*producer=*/100, /*consumer=*/40);             // announcement may still be in the ring
    EXPECT_EQ(s.droppedForTest(), 0u);
    EXPECT_EQ(s.heldAcks(), 1u);
    ASSERT_TRUE(s.announce(9, "c", 1));                      // ...the next pass drains it
    s.flush(140, 140);                                       // Announced → Resolving
    s.flush(140, 140);                                       // Ready → sent
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(s.droppedForTest(), 0u);

    s.enqueue(ack(10, 2));                                   // never announced
    s.flush(200, 150);
    EXPECT_EQ(s.droppedForTest(), 0u);
    s.flush(260, 199);                                       // tag stays 200 (first sighting)
    EXPECT_EQ(s.droppedForTest(), 0u);
    s.flush(260, 200);
    EXPECT_EQ(s.droppedForTest(), 1u) << "consumed past every announcement that could name it";
    EXPECT_EQ(s.heldAcks(), 0u);
}

TEST(SessionAckSinkRegression, UnknownSessionAckWithTheRingConsumedIsDroppedAndCounted) {
    FakePolicy pol; Sink s(pol);
    s.enqueue(ack(99, 1));
    pass(s);
    EXPECT_EQ(s.heldAcks(), 0u);
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSinkRegression, OversizedChannelAnnouncementRefused) {
    FakePolicy pol; Sink s(pol);
    EXPECT_FALSE(s.announce(7, std::string(Sink::kMaxChannelBytes + 1, 'x'), 1));
}

TEST(SessionAckSinkRegression, NotYetConnectedPublicationHoldsAcksUntilItConnects) {
    // A freshly announced session's publication is NOT_CONNECTED until the
    // client's image is up (Aeron setup): its first acks wait, they are not lost.
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    st->blocked.insert(1);
    s.enqueue(ack(7, 1));
    for (int i = 0; i < 3; ++i) pass(s);
    EXPECT_TRUE(st->sent.empty());
    st->blocked.clear();
    pass(s);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(s.droppedForTest(), 0u);
}

TEST(SessionAckSinkRegression, ReannounceAfterCloseOpensAFreshPublication) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    s.close(7);
    EXPECT_EQ(s.stateForTest(7), nullptr);
    ready(s, 7);
    EXPECT_EQ(st->added.size(), 2u);
    s.enqueue(ack(7, 1));
    pass(s);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(st->sent[0].first, 2) << "acked on the new registration";
}

// m3: re-announcing a session still Announced / Resolving must not queue a
// second resolution (one add, one publication).
TEST(SessionAckSinkRegression, ReannounceWhileUnresolvedResolvesOnce) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ASSERT_TRUE(s.announce(7, "c", 1));
    ASSERT_TRUE(s.announce(7, "c", 1));
    pass(s); pass(s); pass(s);
    EXPECT_EQ(st->added.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(7)));
}

TEST(SessionAckSinkRegression, ManySessionsSurviveBackwardShiftDeletion) {
    // Open addressing with backward-shift deletion: closing every other
    // session must leave every remaining one findable.
    FakePolicy pol; Sink s(pol);
    constexpr int64_t kN = 600;
    for (int64_t sid = 1; sid <= kN; ++sid) ASSERT_TRUE(s.announce(sid, "c", 1));
    for (int i = 0; i < 18; ++i) pass(s);
    for (int64_t sid = 1; sid <= kN; sid += 2) s.close(sid);
    for (int64_t sid = 1; sid <= kN; ++sid) {
        if (sid % 2 == 1) EXPECT_EQ(s.stateForTest(sid), nullptr) << sid;
        else ASSERT_NE(s.stateForTest(sid), nullptr) << sid;
    }
}

// Fix round 2 (finding 1): the worker tells the runtime when a session STARTS
// and STOPS holding acks — exactly once per edge, retried while the return ring
// refuses it — so the runtime can pause that session's gateway-routed intake.
TEST(SessionAckSinkRegression, HoldEdgesAreReportedOncePerTransition) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    std::vector<std::pair<int64_t, bool>> got;
    auto take = [&](int64_t sid, bool held) { got.push_back({sid, held}); return true; };
    st->blocked.insert(1);                                  // session 7's publication
    s.enqueue(ack(7, 1)); s.enqueue(ack(8, 2));
    pass(s);
    EXPECT_EQ(s.reportHoldChanges(take), 1);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0], (std::pair<int64_t, bool>{7, true}));
    s.enqueue(ack(7, 3)); pass(s); pass(s);
    EXPECT_EQ(s.reportHoldChanges(take), 0) << "still held: no repeat";
    st->blocked.clear();
    pass(s);
    EXPECT_EQ(s.reportHoldChanges([](int64_t, bool) { return false; }), 0) << "return ring full";
    EXPECT_EQ(s.reportHoldChanges(take), 1) << "retried, not lost";
    EXPECT_EQ(got.back(), (std::pair<int64_t, bool>{7, false}));
    st->blocked.insert(2);                                  // session 8 held, then closed
    s.enqueue(ack(8, 4)); pass(s);
    s.close(8);
    pass(s);
    EXPECT_EQ(s.reportHoldChanges(take), 0) << "a closed session's state dies with the runtime's close";
}

// Fix round 2 (finding 8): a registration still resolving when its session is
// closed or re-announced is drained (find → drop) — never leaked in the
// worker's conductor / driver.
TEST(SessionAckSinkRegression, RegistrationOfASessionClosedOrReannouncedWhileResolvingIsReleased) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findReady = false;
    ASSERT_TRUE(s.announce(7, "c", 1));
    pass(s);                                                // Announced → Resolving(reg 1)
    s.close(7);
    ASSERT_TRUE(s.announce(9, "c", 1));
    pass(s);                                                // 9: Resolving(reg 2)
    ASSERT_TRUE(s.announce(9, "c", 2));                     // re-announced while resolving
    EXPECT_EQ(s.drainingRegistrations(), 2u);
    st->findReady = true;
    pass(s); pass(s);
    EXPECT_EQ(s.drainingRegistrations(), 0u);
    std::vector<int64_t> found = st->foundRegs;
    std::sort(found.begin(), found.end());
    EXPECT_EQ(found, (std::vector<int64_t>{1, 2, 3})) << "regs 1, 2 drained; reg 3 is 9's live one";
}

// Fix round 3 (re-review 2, finding 1): a registration that resolved to Error is
// already gone from the client conductor (Aeron erases it before throwing; every
// later find of it answers "not yet" forever).  It must NOT go on the drain list
// — it would stay there for good, cost a conductor lookup every pass and fill
// the list until real drains are dropped.
TEST(SessionAckSinkRegression, RegistrationThatResolvedToErrorIsNeverDrained) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->errorOnceThenUnknown = true;
    ASSERT_TRUE(s.announce(7, "c", 1));
    pass(s);                                                // Announced → Resolving(reg 1)
    pass(s);                                                // find → Error (once) → Failed
    ASSERT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    EXPECT_EQ(s.drainingRegistrations(), 0u) << "the driver already dropped it — nothing to release";
    s.close(7);
    for (int i = 0; i < 3; ++i) pass(s);
    EXPECT_EQ(s.drainingRegistrations(), 0u);
}

// D8b review CRITICAL-1: a paused client that DIES leaves its held acks queued forever (NOT_CONNECTED was treated
// as back-pressure, the gateway image never drains, the lease was refreshed by the stuck frame) — they count against
// kAckCapacity and a few dead slow clients starve the shard.  A publication that delivered at least once and then
// reports NOT_CONNECTED has lost its subscriber: fail the session (reported -> the runtime closes it) and drop its
// acks.  One that has never connected yet keeps waiting (connect in progress).
TEST(SessionAckSinkRegression, NotConnectedAfterDeliveringFailsTheSessionRegression) {
    FakePolicy pol;
    Sink s(pol);
    ready(s, 5);
    ASSERT_TRUE(s.enqueue(ack(5, 1)));
    pass(s);
    ASSERT_EQ(pol.s->sent.size(), 1u) << "delivered once: the publication was connected";
    const int pubId = pol.s->sent.back().first;
    pol.s->notConnected.insert(pubId);                         // the client process died
    ASSERT_TRUE(s.enqueue(ack(5, 2)));
    pass(s);
    EXPECT_EQ(s.pendingForTest(), 0u) << "the dead session's acks are released, not held forever";
    std::vector<int64_t> failed;
    s.reportFailures([&](int64_t sid) { failed.push_back(sid); return true; });
    EXPECT_EQ(failed, std::vector<int64_t>{5}) << "the runtime is told so it closes the session";
}

TEST(SessionAckSinkRegression, NotConnectedBeforeFirstDeliveryKeepsWaitingRegression) {
    FakePolicy pol;
    Sink s(pol);
    ready(s, 6);
    pol.s->notConnected.insert(static_cast<int>(pol.s->foundRegs.back()));   // subscriber not connected yet
    ASSERT_TRUE(s.enqueue(ack(6, 1)));
    pass(s);
    pass(s);
    EXPECT_EQ(s.pendingForTest(), 1u) << "never connected: keep holding (connect in progress)";
    std::vector<int64_t> failed;
    s.reportFailures([&](int64_t sid) { failed.push_back(sid); return true; });
    EXPECT_TRUE(failed.empty());
}
