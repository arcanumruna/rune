// SessionAckSinkTest.cpp — worker-side ack publication table + per-session hold
// state machines (spec §3.4, G-A6).  Fake publication policy; NO clock: the sink
// is time-independent (fix round 1, C1/C2) — acks drop only on state events.
#include <rune/node/SessionAckSink.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

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
        std::set<int> blocked;                                  // pub ids that back-pressure
        std::set<int> closed;                                   // pub ids that are CLOSED (fatal)
        std::vector<std::pair<int, std::vector<uint8_t>>> sent; // (pub id, frame)
    };
    std::shared_ptr<State> s = std::make_shared<State>();
    int64_t add(std::string_view ch, int32_t stream) noexcept {
        const int64_t r = s->nextReg++;
        s->added[r] = {std::string(ch), stream};
        return r;
    }
    PubFind find(int64_t reg, PubHandle& out) noexcept {
        if (s->findError) return PubFind::Error;
        if (!s->findReady) return PubFind::Pending;
        out = std::make_shared<FakePub>(FakePub{static_cast<int>(reg)});
        return PubFind::Ready;
    }
    template <class E> int64_t claim(PubHandle& p, std::size_t len, E&& enc) noexcept {
        if (s->closed.count(p->id)) return kFatal;    // CLOSED / MAX_POSITION
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
