// SessionAckSinkTest.cpp — worker-side ack publication table + per-session hold
// state machines (spec §3.4, G-A6).  Fake publication policy; NO clock: the sink
// is time-independent (fix round 1, C1/C2) — acks drop only on state events.
#include <rune/node/SessionAckSink.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <map>
#include <memory>
#include <optional>
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
        int  errorsBeforeReady = 0;                             // A10: the first N finds error (transient), then Ready
        std::set<int64_t> erroredRegs;
        std::set<int> blocked;                                  // pub ids that back-pressure
        std::set<int> closed;                                   // pub ids that are CLOSED (fatal)
        std::set<int> notConnected;                             // pub ids whose subscriber is gone (NOT_CONNECTED)
        std::map<int, int> budget;                              // pub id -> claims that succeed, then back-pressure
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
        if (s->errorsBeforeReady > 0) { --s->errorsBeforeReady; return PubFind::Error; }
        if (!s->findReady) return PubFind::Pending;
        out = std::make_shared<FakePub>(FakePub{static_cast<int>(reg)});
        s->foundRegs.push_back(reg);
        return PubFind::Ready;
    }
    template <class E> int64_t claim(PubHandle& p, std::size_t len, E&& enc) noexcept {
        if (s->closed.count(p->id)) return kFatal;    // CLOSED / MAX_POSITION
        if (s->notConnected.count(p->id)) return -1;            // NOT_CONNECTED
        if (s->blocked.count(p->id)) return -2;                 // BACK_PRESSURED
        if (auto b = s->budget.find(p->id); b != s->budget.end()) {
            if (b->second == 0) return -2;                      // the client's window is full
            --b->second;
        }
        std::vector<uint8_t> b(len);
        enc(b.data());
        s->sent.push_back({p->id, std::move(b)});
        return 1;
    }
};
using Sink = SessionAckSinkT<FakePolicy>;
static_assert(Sink::kClaimFatal == kFatal);
static_assert(Sink::kClaimNotConnected == -1);
using K = SessionSignalKind;
using Edge = std::pair<int64_t, K>;
AckEntry ack(int64_t sid, int64_t corr, int64_t base = 0) { return AckEntry{sid, corr, base, 0, 0}; }

// One pass with the lifecycle ring fully consumed (producer == consumer).
void pass(Sink& s) { s.flush(0, 0); }
// A10 review M2: re-adds back off 2, 4, ... 2^(kMax-1) passes — enough passes to run every attempt.
constexpr int kPassesToExhaustRetries = 2 * (1 << Sink::kMaxPubAddAttempts) + 2 * Sink::kMaxPubAddAttempts;

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
        for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);   // A10: bounded re-adds first
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
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    st->blocked.insert(1);                                  // session 7's publication
    s.enqueue(ack(7, 1)); s.enqueue(ack(8, 2));
    pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 1);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0], (Edge{7, K::Held}));
    s.enqueue(ack(7, 3)); pass(s); pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 0) << "still held: no repeat";
    st->blocked.clear();
    pass(s);
    EXPECT_EQ(s.reportSessionEdges([](int64_t, K) { return false; }), 0) << "return ring full";
    EXPECT_EQ(s.reportSessionEdges(take), 1) << "retried, not lost";
    EXPECT_EQ(got.back(), (Edge{7, K::Released}));
    st->blocked.insert(2);                                  // session 8 held, then closed
    s.enqueue(ack(8, 4)); pass(s);
    s.close(8);
    pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 0) << "a closed session's state dies with the runtime's close";
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
    // Announced → Resolving(reg 1); each find errors once (A10: re-added, bounded) → Failed.
    for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);
    ASSERT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    EXPECT_EQ(s.drainingRegistrations(), 0u) << "the driver already dropped it — nothing to release";
    s.close(7);
    for (int i = 0; i < 3; ++i) pass(s);
    EXPECT_EQ(s.drainingRegistrations(), 0u);
}

// ===========================================================================
// D6 — UnknownProducerId is version-gated per session: the announcement carries
// the session's ConnectRequest.clientVersion; a client that understands 28
// gets it with the logStartOffset, an older one gets OutOfOrderSequence.
// ===========================================================================
namespace {
AckEntry unknownPid(int64_t sid, int64_t corr, int64_t logStart) {
    AckEntry a{sid, corr, 0, 3, static_cast<int16_t>(pc::ErrorCode::UnknownProducerId)};
    a.logStartOffset = logStart;
    return a;
}
std::optional<pc::ProduceResponse> sentTo(const FakePolicy::State& st, int64_t sid) {
    for (const auto& [pub, f] : st.sent)
        if (auto r = pc::ProduceResponse::decode(f.data(), f.size()); r && r->sessionId == sid) return r;
    return std::nullopt;
}
} // namespace

TEST(SessionAckSinkUnknownProducerIdRegression, AnsweredOnlyToClientsThatUnderstandItRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ASSERT_TRUE(s.announce(7, "c7", 1, /*clientVersion=*/pc::kClientVersion));
    ASSERT_TRUE(s.announce(8, "c8", 1, /*clientVersion=*/1));
    pass(s); pass(s);
    s.enqueue(unknownPid(7, 70, 500));
    s.enqueue(unknownPid(8, 80, 500));
    pass(s);
    auto r7 = sentTo(*st, 7), r8 = sentTo(*st, 8);
    ASSERT_TRUE(r7 && r8);
    EXPECT_EQ(r7->errorCode, static_cast<int16_t>(pc::ErrorCode::UnknownProducerId));
    EXPECT_EQ(r7->logStartOffset, 500);
    EXPECT_EQ(r8->errorCode, static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence))
        << "an old client must never see UnknownProducerId";
    EXPECT_EQ(r7->partition, 3);
}

TEST(SessionAckSinkUnknownProducerIdRegression, OkAndOtherErrorsCarryNoLogStartRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    s.enqueue(AckEntry{7, 1, 42, 0, 0});
    pass(s);
    auto r = sentTo(*st, 7);
    ASSERT_TRUE(r);
    EXPECT_EQ(r->baseOffset, 42);
    EXPECT_EQ(r->logStartOffset, -1);
}

TEST(SessionAckSinkUnknownProducerIdRegression, ClientVersionSurvivesBackwardShiftDeletionRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    constexpr int64_t kN = 600;
    auto version = [](int64_t sid) { return (sid % 3 == 0) ? int32_t{1} : pc::kClientVersion; };
    for (int64_t sid = 1; sid <= kN; ++sid) ASSERT_TRUE(s.announce(sid, "c", 1, version(sid)));
    for (int i = 0; i < 18; ++i) pass(s);
    for (int64_t sid = 1; sid <= kN; sid += 2) s.close(sid);
    for (int64_t sid = 2; sid <= kN; sid += 2) s.enqueue(unknownPid(sid, sid, 9));
    pass(s);
    for (int64_t sid = 2; sid <= kN; sid += 2) {
        auto r = sentTo(*st, sid);
        ASSERT_TRUE(r) << sid;
        const auto want = version(sid) >= pc::kClientVersionUnknownProducerId ? pc::ErrorCode::UnknownProducerId
                                                                              : pc::ErrorCode::OutOfOrderSequence;
        EXPECT_EQ(r->errorCode, static_cast<int16_t>(want)) << sid;
    }
}

// D6 review M4: the shard-mode sink claims the pre-D6 38 B frame (blockLength
// 14) for a session below kClientVersionUnknownProducerId — a STRICT pre-D6
// decoder (exact length, exact blockLength) reads every ack it gets — and the
// 46 B frame only for version >= 2.  Survives backward-shift deletion.
TEST(SessionAckSinkUnknownProducerIdRegression, OldSessionsGetTheStrictPreD6FrameRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    constexpr int64_t kN = 300;
    auto version = [](int64_t sid) { return (sid % 2 == 0) ? int32_t{1} : pc::kClientVersion; };
    for (int64_t sid = 1; sid <= kN; ++sid) ASSERT_TRUE(s.announce(sid, "c", 1, version(sid)));
    for (int i = 0; i < 18; ++i) pass(s);
    for (int64_t sid = 1; sid <= kN; sid += 3) s.close(sid);
    for (int64_t sid = 1; sid <= kN; ++sid) {
        if (sid % 3 == 1) continue;
        s.enqueue(AckEntry{sid, sid, 40 + sid, 0, 0});
        s.enqueue(unknownPid(sid, 1000 + sid, 9));
    }
    pass(s);
    std::size_t seen = 0;
    for (const auto& [pub, f] : st->sent) {
        auto hdr = pc::MessageHeader::decode(f.data(), f.size());
        ASSERT_TRUE(hdr.has_value());
        const int64_t sid = hdr->sessionId;
        if (version(sid) < pc::kClientVersionUnknownProducerId) {
            ASSERT_EQ(f.size(), 38u) << "sid " << sid << ": a strict pre-D6 decoder drops this frame";
            ASSERT_EQ(pc::detail::getLE16(f.data(), 0), 14u) << "sid " << sid << ": blockLength";
            const int16_t ec = pc::detail::getLE16s(f.data(), 24);
            const int64_t corr = hdr->correlationId;
            EXPECT_EQ(ec, corr == sid ? 0 : static_cast<int16_t>(pc::ErrorCode::OutOfOrderSequence)) << sid;
            if (corr == sid) EXPECT_EQ(pc::detail::getLE64(f.data(), 30), 40 + sid);
        } else {
            ASSERT_EQ(f.size(), pc::ProduceResponse::encodedSizeFor(pc::kClientVersion)) << sid;
        }
        ++seen;
    }
    EXPECT_EQ(seen, 2u * (kN - (kN + 2) / 3));
}

// D8b round 2 (re-review MAJOR-A / MEDIUM-C): NOT_CONNECTED is a SUSPICION, never a verdict.  It means "no status
// message for the driver's publication-connection timeout (5 s)" — also a live invoker-mode client that paused its
// duty cycle, or a network blip.  The sink stays timer-free: the session is marked LOST (reported once, as an edge),
// its acks stay queued in order and are never dropped for it; the runtime reaps a session that stays Lost on the
// normal lease (gateway test).  A later claim that sees the publication connected reports Found and delivers in order.
TEST(SessionAckSinkRegression, NotConnectedMarksTheSessionLostAndKeepsItsAcksQueuedRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 5);
    ASSERT_TRUE(s.enqueue(ack(5, 1)));
    pass(s);
    ASSERT_EQ(st->sent.size(), 1u);
    const int pubId = st->sent.back().first;
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    st->notConnected.insert(pubId);                         // the client's SMs stopped (dead, paused or blip)
    ASSERT_TRUE(s.enqueue(ack(5, 2)));
    ASSERT_TRUE(s.enqueue(ack(5, 3)));
    pass(s);
    EXPECT_EQ(s.pendingForTest(), 2u) << "acks stay queued — never dropped for NOT_CONNECTED";
    EXPECT_EQ(s.droppedForTest(), 0u);
    EXPECT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(5))) << "not failed";
    EXPECT_EQ(s.reportSessionEdges([](int64_t, K) { return false; }), 0) << "return ring full";
    EXPECT_EQ(s.reportSessionEdges(take), 2) << "retried next pass, not lost";
    EXPECT_EQ(got, (std::vector<Edge>{{5, K::Lost}, {5, K::Held}}));
    std::vector<int64_t> failed;
    s.reportFailures([&](int64_t sid) { failed.push_back(sid); return true; });
    EXPECT_TRUE(failed.empty()) << "Lost is not Failed";
    got.clear();
    for (int i = 0; i < 3; ++i) pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 0) << "Lost is reported once per edge";
    st->notConnected.clear();                               // its SMs resumed
    pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 2);
    EXPECT_EQ(got, (std::vector<Edge>{{5, K::Found}, {5, K::Released}}));
    ASSERT_EQ(st->sent.size(), 3u);
    EXPECT_EQ(corrOf(st->sent[1].second), 2);
    EXPECT_EQ(corrOf(st->sent[2].second), 3) << "delivered in order";
}

// MEDIUM-C: a publication that never connected (unreachable egress endpoint, or the client died before its first
// ack) is Lost too — the runtime frees it on the normal lease, not after the 10-min held bound.
TEST(SessionAckSinkRegression, NeverConnectedPublicationIsLostRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 6);
    st->notConnected.insert(static_cast<int>(st->foundRegs.back()));
    ASSERT_TRUE(s.enqueue(ack(6, 1)));
    pass(s);
    pass(s);
    EXPECT_EQ(s.pendingForTest(), 1u) << "held, not dropped";
    std::vector<Edge> got;
    s.reportSessionEdges([&](int64_t sid, K k) { got.push_back({sid, k}); return true; });
    EXPECT_EQ(got, (std::vector<Edge>{{6, K::Lost}, {6, K::Held}}));
}

// BACK_PRESSURED is Aeron's "connected, window closed" verdict (it is only returned while the publication is
// connected): a Lost session whose client is back but still not reading is Found — it stays Held (10-min bound).
TEST(SessionAckSinkRegression, BackPressureAfterLostIsFoundAndStaysHeldRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 5);
    st->notConnected.insert(1);
    ASSERT_TRUE(s.enqueue(ack(5, 1)));
    pass(s);
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    s.reportSessionEdges(take);
    ASSERT_EQ(got, (std::vector<Edge>{{5, K::Lost}, {5, K::Held}}));
    got.clear();
    st->notConnected.clear();
    st->blocked.insert(1);
    pass(s);
    s.reportSessionEdges(take);
    EXPECT_EQ(got, (std::vector<Edge>{{5, K::Found}}));
    EXPECT_EQ(s.pendingForTest(), 1u);
}

// MEDIUM-A: a held session whose client reads (acks claimed while held) reports Progress so the runtime refreshes
// its lease — rate-limited by PASS COUNT (no clock): at most once per kProgressPassInterval passes, counted from the
// Held edge / the previous Progress.  The last progress is never lost: it is reported once the interval elapses even
// if the client stalls again meanwhile.  No progress → no Progress.
TEST(SessionAckSinkRegression, HeldSessionDrainingReportsProgressRateLimitedByPassesRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    constexpr uint64_t N = Sink::kProgressPassInterval;
    st->budget[1] = 0;                                      // 7: window full
    st->blocked.insert(2);                                  // 8: never reads
    for (int64_t c = 0; c < static_cast<int64_t>(3 * N); ++c) ASSERT_TRUE(s.enqueue(ack(7, c)));
    ASSERT_TRUE(s.enqueue(ack(8, 1)));
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    pass(s);
    s.reportSessionEdges(take);
    ASSERT_EQ(got, (std::vector<Edge>{{7, K::Held}, {8, K::Held}}));
    got.clear();
    std::vector<uint64_t> progressAt;                       // passes (since the Held edge) that reported Progress
    for (uint64_t p = 1; p <= 2 * N + 1; ++p) {
        st->budget[1] = 1;                                  // the client reads one ack per pass
        pass(s);
        s.reportSessionEdges(take);
        for (auto& e : got) {
            ASSERT_EQ(e, (Edge{7, K::Progress})) << "only 7 progresses; no other edge";
            progressAt.push_back(p);
        }
        got.clear();
    }
    EXPECT_EQ(progressAt, (std::vector<uint64_t>{N, 2 * N})) << "once per kProgressPassInterval passes";
    // One ack read, then the client stalls: the progress is still reported when the interval elapses.
    st->budget[1] = 1;
    pass(s);
    st->budget[1] = 0;
    uint64_t reported = 0, at = 0;
    for (uint64_t p = 1; p <= N; ++p) {
        pass(s);
        s.reportSessionEdges(take);
        if (!got.empty()) { ++reported; at = p; EXPECT_EQ(got.back(), (Edge{7, K::Progress})); got.clear(); }
    }
    EXPECT_EQ(reported, 1u) << "the last progress is never lost";
    EXPECT_EQ(at, N - 2) << "reported exactly kProgressPassInterval passes after the previous Progress";
    for (uint64_t p = 0; p < 2 * N; ++p) { pass(s); s.reportSessionEdges(take); }
    EXPECT_TRUE(got.empty()) << "no progress -> no Progress";
}

// A Progress that the return ring refuses is retried next pass (the session stays dirty) — never lost.
TEST(SessionAckSinkRegression, RefusedProgressIsRetriedRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7);
    constexpr uint64_t N = Sink::kProgressPassInterval;
    st->budget[1] = 0;
    for (int64_t c = 0; c < 8; ++c) ASSERT_TRUE(s.enqueue(ack(7, c)));
    pass(s);
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    s.reportSessionEdges(take);
    got.clear();
    st->budget[1] = 1;
    pass(s);
    st->budget[1] = 0;
    for (uint64_t p = 2; p <= N; ++p) { pass(s); s.reportSessionEdges([](int64_t, K) { return false; }); }
    EXPECT_EQ(s.reportSessionEdges(take), 1) << "refused at the interval, retried";
    EXPECT_EQ(got, (std::vector<Edge>{{7, K::Progress}}));
}

// Backward-shift deletion must move the new per-session edge flags (lost / reported lost / progress) WITH their
// session and reset the vacated slot: B (lost, reported) and C (held, reported) shift left when A is closed; B's
// Found and C's Released are still reported; a new session D in the freed slot inherits nothing.
TEST(SessionAckSinkRegression, BackwardShiftDeletionKeepsEdgeFlagsWithTheirSessionRegression) {
    constexpr int kBits = std::countr_zero(Sink::kMaxSessions);
    auto home = [](int64_t sid) { return (static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - kBits); };
    std::vector<int64_t> same;                              // four keys with the same home slot
    for (int64_t sid = 2; same.size() < 3; ++sid) if (home(sid) == home(1)) same.push_back(sid);
    const int64_t A = 1, B = same[0], C = same[1], D = same[2];
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, A); ready(s, B); ready(s, C);                  // pubs 1, 2, 3; slots h, h+1, h+2
    st->notConnected.insert(2);
    st->blocked.insert(3);
    ASSERT_TRUE(s.enqueue(ack(B, 1)));
    ASSERT_TRUE(s.enqueue(ack(C, 2)));
    pass(s);
    std::vector<Edge> got;
    auto take = [&](int64_t sid, K k) { got.push_back({sid, k}); return true; };
    s.reportSessionEdges(take);
    ASSERT_EQ(got, (std::vector<Edge>{{B, K::Lost}, {B, K::Held}, {C, K::Held}}));
    got.clear();
    s.close(A);                                             // B, C shift left
    pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 0) << "no spurious edge after the shift";
    st->notConnected.clear();
    st->blocked.clear();
    pass(s);
    s.reportSessionEdges(take);
    std::sort(got.begin(), got.end());
    std::vector<Edge> want{{B, K::Released}, {B, K::Found}, {C, K::Released}};
    std::sort(want.begin(), want.end());
    EXPECT_EQ(got, want) << "the reported flags moved with their sessions";
    got.clear();
    st->notConnected.insert(3);                             // C lost + held again, then closed with those flags set
    ASSERT_TRUE(s.enqueue(ack(C, 4)));
    pass(s);
    s.reportSessionEdges(take);
    ASSERT_EQ(got, (std::vector<Edge>{{C, K::Lost}, {C, K::Held}}));
    got.clear();
    s.close(C);                                             // C's slot freed (the tail of the run)
    ready(s, D);
    ASSERT_TRUE(s.enqueue(ack(D, 3)));
    pass(s);
    EXPECT_EQ(s.reportSessionEdges(take), 0) << "a new session in a vacated slot inherits no flags";
    EXPECT_EQ(s.pendingForTest(), 0u);
}

// ---------------------------------------------------------------------------
// D8c — produce credits of the gateway-routed path (reportCredits).
// ---------------------------------------------------------------------------
namespace {
AckEntry routedAck(int64_t sid, int64_t corr) {
    AckEntry a = ack(sid, corr);
    a.credit = 1;
    return a;
}
using Credit = std::pair<int64_t, uint32_t>;
} // namespace

// A gateway-routed ack returns its credit when it is CLAIMED — never at enqueue, never while it is held — and
// direct (worker-ingress) acks return none.  Counts coalesce: one report per session per pass.
TEST(SessionAckSinkRegression, CreditIsReturnedOnlyWhenARoutedAckIsClaimedRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ready(s, 7); ready(s, 8);
    std::vector<Credit> got;
    auto take = [&](int64_t sid, uint32_t n) { got.push_back({sid, n}); return true; };
    st->budget[1] = 2;                                      // 7: two acks fit its window, then back-pressure
    for (int64_t c = 0; c < 5; ++c) ASSERT_TRUE(s.enqueue(routedAck(7, c)));
    ASSERT_TRUE(s.enqueue(ack(7, 100)));                    // a direct (ingress) ack of the same session
    ASSERT_TRUE(s.enqueue(routedAck(8, 1)));
    ASSERT_TRUE(s.enqueue(ack(8, 2)));
    EXPECT_EQ(s.reportCredits(take), 0) << "nothing is claimed before the flush";
    pass(s);
    EXPECT_EQ(s.reportCredits(take), 2);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<Credit>{{7, 2u}, {8, 1u}})) << "claimed routed acks only, coalesced per session";
    got.clear();
    pass(s);
    EXPECT_EQ(s.reportCredits(take), 0) << "held acks return nothing";
    EXPECT_EQ(s.heldAcks(), 4u);
    st->budget[1] = 1000;                                   // the client reads again
    pass(s);
    EXPECT_EQ(s.reportCredits(take), 1);
    EXPECT_EQ(got, (std::vector<Credit>{{7, 3u}})) << "the direct ack (corr 100) carries no credit";
    EXPECT_EQ(s.reportCredits(take), 0) << "a reported count is zeroed";
    EXPECT_EQ(got.size(), 1u);
}

// A credit report the return ring refuses stays counted and is retried (later claims add to it) — never lost, never
// reported twice.
TEST(SessionAckSinkRegression, RefusedCreditReportIsRetriedNeverLostOrDoubledRegression) {
    FakePolicy pol; Sink s(pol);
    ready(s, 7); ready(s, 8);
    ASSERT_TRUE(s.enqueue(routedAck(7, 1)));
    ASSERT_TRUE(s.enqueue(routedAck(8, 1)));
    pass(s);
    int calls = 0;
    EXPECT_EQ(s.reportCredits([&](int64_t, uint32_t) { ++calls; return false; }), 0);
    EXPECT_EQ(calls, 1) << "stops at the first refusal";
    ASSERT_TRUE(s.enqueue(routedAck(7, 2)));
    ASSERT_TRUE(s.enqueue(routedAck(7, 3)));
    pass(s);
    std::vector<Credit> got;
    EXPECT_EQ(s.reportCredits([&](int64_t sid, uint32_t n) { got.push_back({sid, n}); return true; }), 2);
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<Credit>{{7, 3u}, {8, 1u}})) << "refused counts kept and merged with later claims";
    got.clear();
    pass(s);
    EXPECT_EQ(s.reportCredits([&](int64_t sid, uint32_t n) { got.push_back({sid, n}); return true; }), 0);
    EXPECT_TRUE(got.empty()) << "never reported twice";
}

// A closed session's unreported credits die with it (the runtime's close discards its whole budget); a session
// re-using the slot inherits none.
TEST(SessionAckSinkRegression, ClosedSessionsUnreportedCreditsAreDroppedRegression) {
    FakePolicy pol; Sink s(pol);
    ready(s, 7);
    ASSERT_TRUE(s.enqueue(routedAck(7, 1)));
    pass(s);
    ASSERT_EQ(s.unreportedCreditsForTest(7), 1u);
    s.close(7);
    std::vector<Credit> got;
    EXPECT_EQ(s.reportCredits([&](int64_t sid, uint32_t n) { got.push_back({sid, n}); return true; }), 0);
    ready(s, 7);                                            // re-announced (fresh publication)
    EXPECT_EQ(s.unreportedCreditsForTest(7), 0u);
    ASSERT_TRUE(s.enqueue(routedAck(7, 2)));
    pass(s);
    EXPECT_EQ(s.reportCredits([&](int64_t sid, uint32_t n) { got.push_back({sid, n}); return true; }), 1);
    EXPECT_EQ(got, (std::vector<Credit>{{7, 1u}}));
}

// ============================================================================
// D8d (D8c review LOW-R2) — the DIRECT (worker-ingress) produce budget.
// ============================================================================
namespace {
/// The answer of a direct produce admitted with `tag` (AckEntry::direct).
AckEntry directAck(int64_t sid, int64_t corr, uint8_t tag = Sink::kDirectCounted, int16_t err = 0) {
    AckEntry a{sid, corr, 0, 0, err};
    a.direct = tag;
    return a;
}
/// Review LOW-2: the session's publication delivers one (non-direct) ack — only
/// a session that has delivered can be expired.
void deliverOne(Sink& s, int64_t sid) {
    ASSERT_TRUE(s.enqueue(ack(sid, -1)));
    pass(s);
}
/// Admit `n` direct frames of `sid`; returns how many were admitted (stops at the first refusal).
int admitN(Sink& s, int64_t sid, int n) {
    int k = 0;
    while (k < n && s.admitDirect(sid) != Sink::kDirectRefused) ++k;
    return k;
}
} // namespace

// Every admitted direct produce is counted until its ANSWER leaves the queue — claimed (OK or a refusal answer),
// dropped by a fatal claim / failed publication, or dropped after the runtime's close (which forgets the session's
// share at once) — and an admitted frame without an answer is given back.  Exactly once each: no leak, no underflow.
TEST(SessionAckSinkRegression, DirectBudgetIsReturnedOnClaimRefusalDropAndCloseRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(8, 400);
    ready(s, 7); ready(s, 8); ready(s, 9);                  // pubs 1, 2, 3
    ASSERT_EQ(admitN(s, 7, 3), 3);
    EXPECT_EQ(s.directInFlight(), 3u);
    EXPECT_EQ(s.directInFlightOf(7), 3u);
    ASSERT_TRUE(s.enqueue(directAck(7, 1)));
    ASSERT_TRUE(s.enqueue(directAck(7, 2, Sink::kDirectCounted, static_cast<int16_t>(pc::ErrorCode::NotLeader))));
    pass(s);                                                // the third is still owed (a parked acks=all)
    EXPECT_EQ(s.directInFlight(), 1u) << "an OK and a refusal answer each return their unit when claimed";
    EXPECT_EQ(s.directInFlightOf(7), 1u);
    ASSERT_TRUE(s.enqueue(directAck(7, 3)));                // the parked answer fires
    pass(s);
    EXPECT_EQ(s.directInFlight(), 0u);
    EXPECT_EQ(s.directInFlightOf(7), 0u);

    st->closed.insert(2);                                   // session 8's publication: CLOSED (fatal)
    ASSERT_EQ(admitN(s, 8, 2), 2);
    ASSERT_TRUE(s.enqueue(directAck(8, 1)));
    ASSERT_TRUE(s.enqueue(directAck(8, 2)));
    pass(s);
    EXPECT_EQ(s.directInFlight(), 0u) << "fatal claim + failed publication drop both return their units";

    st->blocked.insert(3);                                  // session 9 stops reading
    ASSERT_EQ(admitN(s, 9, 3), 3);
    for (int64_t c = 1; c <= 3; ++c) ASSERT_TRUE(s.enqueue(directAck(9, c)));
    pass(s);
    ASSERT_EQ(s.directInFlight(), 3u) << "held acks keep their units";
    s.close(9);
    EXPECT_EQ(s.directInFlightOf(9), 0u) << "the close forgets the session's share at once";
    EXPECT_EQ(s.directInFlight(), 3u) << "... the worker total returns as its queued acks drop";
    pass(s);
    EXPECT_EQ(s.directInFlight(), 0u);

    EXPECT_EQ(s.admitDirect(50), Sink::kDirectTotalOnly) << "announcement not applied yet: total only";
    ready(s, 50);
    ASSERT_TRUE(s.enqueue(directAck(50, 1, Sink::kDirectTotalOnly)));
    pass(s);
    EXPECT_EQ(s.directInFlight(), 0u);
    EXPECT_EQ(s.directInFlightOf(50), 0u) << "never taken from a share it was not counted in";

    const uint8_t tag = s.admitDirect(7);
    ASSERT_EQ(tag, Sink::kDirectCounted);
    s.releaseDirect(7, tag);                                // an undecodable frame: no answer
    EXPECT_EQ(s.directInFlight(), 0u);
    EXPECT_EQ(s.directInFlightOf(7), 0u);
    EXPECT_EQ(s.directUnderflowsForTest(), 0u);
}

// Fairness: a session holding acks is paused (G-A6), a session at its budget is paused, and in the cap's last
// quarter only LIGHT sessions are admitted — so a stalled session (at its whole share) runs out first and a new
// session is still served up to the hard cap, which is never exceeded.
TEST(SessionAckSinkRegression, DirectBudgetPausesStalledSessionsFirstAndNeverExceedsTheCapRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(200, 400);                           // band = [300, 400)
    ready(s, 7); ready(s, 8); ready(s, 9); ready(s, 10); ready(s, 11);
    st->blocked.insert(1);                                  // session 7 does not read
    ASSERT_EQ(s.admitDirect(7), Sink::kDirectCounted);
    ASSERT_TRUE(s.enqueue(directAck(7, 1)));
    pass(s);
    ASSERT_TRUE(s.holding(7));
    EXPECT_EQ(s.admitDirect(7), Sink::kDirectRefused) << "a session holding acks is paused";
    EXPECT_EQ(s.directRefusedHeldForTest(), 1u);

    EXPECT_EQ(admitN(s, 8, 1000), 200) << "per-session budget";
    EXPECT_EQ(s.directRefusedBudgetForTest(), 1u);
    EXPECT_EQ(admitN(s, 9, 1000), 99) << "up to the band, then only light sessions";
    EXPECT_EQ(s.directRefusedBandForTest(), 1u);
    EXPECT_EQ(admitN(s, 10, 1000), 64) << "a light session is served inside the band";
    EXPECT_EQ(admitN(s, 11, 1000), 36) << "... up to the hard cap";
    EXPECT_EQ(s.directInFlight(), 400u);
    EXPECT_EQ(s.admitDirect(99), Sink::kDirectRefused) << "full: even an unknown session waits";
    EXPECT_EQ(s.directRefusedFullForTest(), 2u);

    for (int64_t c = 0; c < 50; ++c) ASSERT_TRUE(s.enqueue(directAck(8, c)));   // 50 of session 8's answers
    pass(s);
    EXPECT_EQ(s.directInFlight(), 350u);
    EXPECT_EQ(admitN(s, 9, 1000), 0) << "the heavy session stays paused in the band";
    EXPECT_EQ(admitN(s, 11, 1000), 28) << "the light one gets its band share (64)";
    EXPECT_EQ(admitN(s, 12, 1000), 22) << "an unannounced session counts as light, up to the cap";
    EXPECT_EQ(s.directInFlight(), 400u);
}

namespace {
struct Expiries {
    std::vector<std::pair<int64_t, uint32_t>> got;
    bool accept = true;
    int report(Sink& s) {
        return s.reportDirectExpiry([this](int64_t sid, uint32_t share) {
            if (!accept) return false;
            got.push_back({sid, share});
            return true;
        });
    }
};
} // namespace

// A full direct cap expires ONLY a session held here, the one with the largest share, after
// kDirectExpireAfterDeferredPasses deferred passes; one per episode, one in flight until the runtime's close; a
// session whose acks were claimed during the episode (a slow reader) is skipped; a refused report is retried.
TEST(SessionAckSinkRegression, DirectFullCapExpiresOnlyTheHeldLargestShareSessionOncePerEpisodeRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(300, 400);                           // band = [300, 400)
    ready(s, 7); ready(s, 8); ready(s, 9); ready(s, 10); ready(s, 11);   // pubs 1..5
    for (int64_t sid : {7, 8, 9, 10, 11}) deliverOne(s, sid);
    Expiries ex;
    ASSERT_EQ(admitN(s, 9, 300), 300);                      // 9: a full pipeline of parked acks=all — not held
    ASSERT_EQ(admitN(s, 7, 100), 64);
    ASSERT_EQ(admitN(s, 8, 100), 36);
    ASSERT_EQ(s.directInFlight(), 400u);
    const auto deferredPass = [&] { ASSERT_EQ(s.admitDirect(99), Sink::kDirectRefused); pass(s); };
    for (int i = 0; i < 3 * 64; ++i) deferredPass();
    EXPECT_EQ(s.directExpiries(), 0u) << "no session is held: nobody to blame, keep deferring";

    st->blocked.insert(1); st->blocked.insert(2);           // 7 and 8 stop reading; their answers queue
    for (int64_t c = 0; c < 64; ++c) ASSERT_TRUE(s.enqueue(directAck(7, c)));
    for (int64_t c = 0; c < 36; ++c) ASSERT_TRUE(s.enqueue(directAck(8, c)));
    for (int i = 0; i < 63; ++i) deferredPass();
    EXPECT_EQ(s.directExpiries(), 0u) << "the scan runs every 64 deferred passes";
    deferredPass();
    ASSERT_EQ(s.directExpiries(), 1u);
    EXPECT_EQ(s.lastDirectExpiredForTest(), 7) << "the HELD session with the largest share (64 > 36), not 9 (300)";
    ex.accept = false;
    EXPECT_EQ(ex.report(s), 0);
    ex.accept = true;
    EXPECT_EQ(ex.report(s), 1) << "a refused report is retried";
    EXPECT_EQ(ex.got, (std::vector<std::pair<int64_t, uint32_t>>{{7, 64u}}));
    EXPECT_EQ(ex.report(s), 0) << "reported once";
    for (int i = 0; i < 4 * 64; ++i) deferredPass();
    EXPECT_EQ(s.directExpiries(), 1u) << "one expiry in flight until the runtime closes it";

    s.close(7);                                             // the runtime's close
    pass(s);
    EXPECT_EQ(s.directInFlight(), 336u) << "its share is freed";
    EXPECT_EQ(admitN(s, 10, 100), 64);                      // refilled by a light session ...
    st->blocked.insert(4);
    for (int64_t c = 0; c < 64; ++c) ASSERT_TRUE(s.enqueue(directAck(10, c)));   // ... that stops reading too
    pass(s);
    ASSERT_TRUE(s.holding(10));
    deferredPass();                                         // a new episode (36 + 300 + 64 = 400: full)
    ASSERT_EQ(s.directInFlight(), 400u);
    st->blocked.erase(4); st->budget[4] = 1;                // 10 reads ONE ack during the episode, then stalls again
    pass(s);
    ASSERT_TRUE(s.holding(10));
    ASSERT_EQ(admitN(s, 11, 1), 1);                         // full again
    for (int i = 0; i < 2 * 64; ++i) deferredPass();
    ASSERT_EQ(s.directExpiries(), 2u);
    EXPECT_EQ(s.lastDirectExpiredForTest(), 8) << "10 (63) read during the episode: the smaller held 8 (36) goes";
    EXPECT_EQ(s.directUnderflowsForTest(), 0u);
}

// The episode ends when the share drops below the congested band: a later full cap needs a whole new
// kDirectExpireAfterDeferredPasses of deferred passes (no carry-over), and a session closed before its expiry is
// reported is never reported.
TEST(SessionAckSinkRegression, DirectEpisodeEndsBelowTheBandAndAClosedVictimIsNeverReportedRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(400, 400);                           // band = [300, 400)
    ready(s, 7); ready(s, 8); ready(s, 30);                 // pubs 1, 2, 3
    for (int64_t sid : {7, 8, 30}) deliverOne(s, sid);
    st->blocked.insert(1);
    ASSERT_EQ(admitN(s, 7, 400), 300);                      // the heavy session stops at the band
    for (int64_t c = 0; c < 300; ++c) ASSERT_TRUE(s.enqueue(directAck(7, c)));
    ASSERT_EQ(admitN(s, 20, 64), 64);                       // light (unannounced) sessions fill the band:
    ASSERT_EQ(admitN(s, 21, 64), 36);                       // owed, never answered here
    const auto deferredPass = [&] { ASSERT_EQ(s.admitDirect(8), Sink::kDirectRefused); pass(s); };
    for (int i = 0; i < 40; ++i) deferredPass();
    st->blocked.clear(); st->budget[1] = 200;               // 7 reads 200: below the band
    pass(s);
    ASSERT_EQ(s.directInFlight(), 200u);
    ASSERT_TRUE(s.holding(7)) << "7 stalled again with 100 queued";
    ASSERT_EQ(admitN(s, 30, 400), 100);                     // a new pipeline (owed) back to the band
    ASSERT_EQ(admitN(s, 22, 64), 64);
    ASSERT_EQ(admitN(s, 23, 64), 36);                       // full again: a NEW episode
    for (int i = 0; i < 63; ++i) deferredPass();
    EXPECT_EQ(s.directExpiries(), 0u) << "the old episode's 40 deferred passes do not carry over";
    deferredPass();
    ASSERT_EQ(s.directExpiries(), 1u);
    EXPECT_EQ(s.lastDirectExpiredForTest(), 7);
    s.close(7);                                             // closed (another reason) before the report
    Expiries ex;
    EXPECT_EQ(ex.report(s), 0);
    EXPECT_TRUE(ex.got.empty()) << "a closed victim is never reported";
    pass(s);
    EXPECT_EQ(s.directInFlight(), 300u) << "its 100 queued acks dropped";
    EXPECT_EQ(s.admitDirect(8), Sink::kDirectCounted) << "the share has room again";
    EXPECT_EQ(s.directUnderflowsForTest(), 0u);
}

// ---- D8d fix round (review MEDIUM-1 / MEDIUM-2 / LOW-1 / LOW-2) -------------

// MEDIUM-1: held sessions parking the share in its band (not full) while the band DEFERS a non-held session are
// expired like a full share's — after kDirectExpireAfterDeferredPasses deferred passes, held + largest only.
TEST(SessionAckSinkRegression, BandDeferralOfAHealthySessionExpiresTheHeldBandParkerRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(200, 400);                           // band = [300, 400)
    ready(s, 7); ready(s, 8); ready(s, 9);                  // pubs 1..3
    for (int64_t sid : {7, 8, 9}) deliverOne(s, sid);
    st->blocked.insert(1); st->blocked.insert(2);           // 7 and 8 stalled
    ASSERT_EQ(admitN(s, 7, 200), 200);
    ASSERT_EQ(admitN(s, 8, 200), 100);                      // to the band start
    for (int64_t c = 0; c < 200; ++c) ASSERT_TRUE(s.enqueue(directAck(7, c)));
    for (int64_t c = 0; c < 100; ++c) ASSERT_TRUE(s.enqueue(directAck(8, c)));
    ASSERT_EQ(admitN(s, 9, 1000), 64);                      // 9: healthy, a parked pipeline — capped by the band
    pass(s);
    ASSERT_LT(s.directInFlight(), 400u) << "precondition: in the band, not full";
    const auto bandPass = [&] { ASSERT_EQ(s.admitDirect(9), Sink::kDirectRefused); pass(s); };
    for (int i = 0; i < 62; ++i) bandPass();
    EXPECT_EQ(s.directExpiries(), 0u);
    bandPass();
    ASSERT_EQ(s.directExpiries(), 1u) << "band deferrals of a healthy session are deferred passes";
    EXPECT_EQ(s.lastDirectExpiredForTest(), 7) << "the held session with the largest share";
    EXPECT_EQ(s.directRefusedFullForTest(), 0u) << "the share was never full";
}

// MEDIUM-2: a session closed here — its still-buffered frames are dropped (consumed, never appended or counted);
// a session never known is still admitted (I1: its announcement may not be applied yet); a re-announced one is
// admitted again.
TEST(SessionAckSinkRegression, ClosedSessionsFramesAreDroppedNotAdmittedRegression) {
    FakePolicy pol; Sink s(pol);
    ready(s, 7);
    ASSERT_EQ(s.admitDirect(7), Sink::kDirectCounted);
    ASSERT_TRUE(s.enqueue(directAck(7, 1)));
    pass(s);
    s.close(7);
    EXPECT_EQ(s.admitDirect(7), Sink::kDirectDropClosed);
    EXPECT_EQ(s.admitDirect(7), Sink::kDirectDropClosed);
    EXPECT_EQ(s.directDroppedClosed(), 2u);
    EXPECT_EQ(s.directInFlight(), 0u) << "dropped frames are never counted";
    EXPECT_EQ(s.admitDirect(8), Sink::kDirectTotalOnly) << "a session never seen here is admitted (I1)";
    ready(s, 7);                                            // re-announced
    EXPECT_EQ(s.admitDirect(7), Sink::kDirectCounted);
}

// LOW-1: the chosen victim is re-checked when the Expire is reported — one that claimed an ack since it was chosen
// (or is no longer held) is not expired; the cancellation frees the one-in-flight slot for a later expiry.
// LOW-2: a session whose publication never delivered (still connecting / never connected) is never chosen.
TEST(SessionAckSinkRegression, ExpiryIsCancelledForAReaderAndNeverHitsANeverDeliveredSessionRegression) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    s.setDirectBudgets(400, 400);
    ready(s, 7); ready(s, 8); ready(s, 9);                  // pubs 1..3
    deliverOne(s, 8);                                       // 7 never delivered; 8 did
    st->notConnected.insert(1); st->blocked.insert(2);
    ASSERT_EQ(admitN(s, 7, 300), 300);                      // the larger share — but never delivered
    ASSERT_EQ(admitN(s, 8, 100), 64);
    ASSERT_EQ(admitN(s, 9, 100), 36);
    for (int64_t c = 0; c < 300; ++c) ASSERT_TRUE(s.enqueue(directAck(7, c)));
    for (int64_t c = 0; c < 64; ++c) ASSERT_TRUE(s.enqueue(directAck(8, c)));
    const auto deferredPass = [&] { ASSERT_EQ(s.admitDirect(10), Sink::kDirectRefused); pass(s); };
    for (int i = 0; i < 64; ++i) deferredPass();
    ASSERT_EQ(s.directExpiries(), 1u);
    EXPECT_EQ(s.lastDirectExpiredForTest(), 8) << "LOW-2: the never-delivered 7 (300) is not to blame";
    st->blocked.erase(2); st->budget[2] = 1;                // 8 reads one ack before the report goes out
    pass(s);
    Expiries ex;
    EXPECT_EQ(ex.report(s), 0) << "LOW-1: a victim that read since it was chosen is not expired";
    EXPECT_TRUE(ex.got.empty());
    EXPECT_EQ(s.directExpiryCancelsForTest(), 1u);
    ASSERT_EQ(admitN(s, 11, 1), 1);                         // full again (8 freed one)
    const auto deferredPass2 = [&] { ASSERT_EQ(s.admitDirect(12), Sink::kDirectRefused); pass(s); };
    for (int i = 0; i < 64; ++i) deferredPass2();           // 8 stalls again: a later expiry is possible
    ASSERT_EQ(s.directExpiries(), 2u);
    EXPECT_EQ(s.lastDirectExpiredForTest(), 8);
    EXPECT_EQ(ex.report(s), 1);
    EXPECT_EQ(s.directUnderflowsForTest(), 0u);
}

// ===========================================================================
// A10 (2026-10-04): a TRANSIENT driver error on the ack publication's add (EINTR
// from tmpfs fallocate on a worker thread with io_uring completions pending) must
// not kill the session: the add is retried on the same channel a bounded number of
// times; only a persistent error (bad channel) reports the session Failed.
// ===========================================================================
TEST(SessionAckSinkRegression, TransientPublicationAddErrorIsRetriedNotFatal) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->errorsBeforeReady = 3;
    ASSERT_TRUE(s.announce(7, "aeron:udp?endpoint=c:1", 2));
    ASSERT_TRUE(s.enqueue(ack(7, 100)));
    for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);
    ASSERT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(7))) << "a transient add error is not fatal";
    EXPECT_EQ(s.unreportedFailures(), 0u);
    EXPECT_EQ(s.pubAddRetries(), 3u);
    ASSERT_EQ(st->added.size(), 4u) << "1 add + 3 re-adds";
    for (const auto& [reg, chs] : st->added) {
        EXPECT_EQ(chs.first, "aeron:udp?endpoint=c:1") << "re-added on the announced channel";
        EXPECT_EQ(chs.second, 2);
    }
    ASSERT_TRUE(sentTo(*st, 7).has_value()) << "the queued ack is delivered on the recovered publication";
}

TEST(SessionAckSinkRegression, PersistentPublicationAddErrorFailsAfterBoundedRetries) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findError = true;
    ASSERT_TRUE(s.announce(7, "c", 1));
    for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    EXPECT_EQ(st->added.size(), static_cast<std::size_t>(Sink::kMaxPubAddAttempts)) << "bounded: never retried forever";
    EXPECT_EQ(s.freeChannelSlotsForTest(), Sink::kMaxChannelSlots) << "the Failed session's slot is returned";
    EXPECT_EQ(s.unreportedFailures(), 1u);
}

// A10 review M2: re-adds are spaced (pass-count backoff), not back to back.
TEST(SessionAckSinkRegression, PublicationReAddsBackOffByPasses) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findError = true;
    ASSERT_TRUE(s.announce(7, "c", 1));
    for (int i = 0; i < 4; ++i) pass(s);
    EXPECT_LE(st->added.size(), 2u) << "no burst of re-adds while the error condition persists";
}

// A10 review M3: every channel slot comes back — Ready, Failed, closed while a re-add is pending, re-announced.
TEST(SessionAckSinkRegression, ChannelSlotsAreAllReturned) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    ASSERT_TRUE(s.announce(1, "a", 1));                      // → Ready
    st->errorsBeforeReady = 1;
    ASSERT_TRUE(s.announce(2, "b", 1));                      // → one error, re-add, Ready
    for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);
    ASSERT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(1)));
    ASSERT_TRUE(std::holds_alternative<Sink::PubReady>(*s.stateForTest(2)));
    EXPECT_EQ(s.freeChannelSlotsForTest(), Sink::kMaxChannelSlots);
    st->findError = true;
    ASSERT_TRUE(s.announce(3, "c", 1));
    pass(s); pass(s);                                         // Resolving → error → a re-add pending
    EXPECT_EQ(s.freeChannelSlotsForTest(), Sink::kMaxChannelSlots - 1) << "kept for the re-add";
    s.close(3);
    EXPECT_EQ(s.freeChannelSlotsForTest(), Sink::kMaxChannelSlots) << "closed while a re-add was pending";
    ASSERT_TRUE(s.announce(1, "a2", 1));                      // re-announce a Ready session (still erroring)
    for (int i = 0; i < kPassesToExhaustRetries; ++i) pass(s);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(1)));
    EXPECT_EQ(s.freeChannelSlotsForTest(), Sink::kMaxChannelSlots);
    EXPECT_EQ(s.drainingRegistrations(), 0u) << "errored registrations are never drained";
}

// Lows batch C (D8d follow-up: tombstone limit).  A mass close of EVERY session the worker can know (the gateway
// reaps them in one cycle — e.g. after a network partition) must keep every closed session's buffered direct frames
// out of the log: the tombstones cover kMaxSessions closes.  (Was: a 1024-entry ring — the 1025th close overwrote
// the first, whose backlog was then appended unanswered and duplicated by the client's resend.)
TEST(SessionAckSinkRegression, MassCloseOfEverySessionKeepsEveryTombstoneRegression) {
    FakePolicy pol; Sink s(pol);
    constexpr int64_t kN = static_cast<int64_t>(Sink::kMaxSessions) - 1;   // the table's whole capacity
    for (int64_t sid = 1; sid <= kN; ++sid) ASSERT_TRUE(s.announce(sid, "aeron:udp?endpoint=127.0.0.1:1", 7));
    pass(s);
    pass(s);
    for (int64_t sid = 1; sid <= kN; ++sid) s.close(sid);
    for (int64_t sid = 1; sid <= kN; ++sid)
        ASSERT_EQ(s.admitDirect(sid), Sink::kDirectDropClosed) << "closed session " << sid << " lost its tombstone";
    EXPECT_EQ(s.directInFlight(), 0u);
    EXPECT_EQ(s.admitDirect(kN + 1), Sink::kDirectTotalOnly) << "a session never seen here is admitted (I1)";
    // FIFO: the oldest tombstone goes only after kClosedTombstones newer closes.
    for (int64_t sid = kN + 100; sid < kN + 100 + static_cast<int64_t>(Sink::kClosedTombstones) - kN; ++sid) {
        ASSERT_TRUE(s.announce(sid, "aeron:udp?endpoint=127.0.0.1:1", 7));
        s.close(sid);
    }
    EXPECT_EQ(s.admitDirect(1), Sink::kDirectDropClosed) << "still within the last kClosedTombstones closes";
    ASSERT_TRUE(s.announce(kN + 50, "aeron:udp?endpoint=127.0.0.1:1", 7));
    s.close(kN + 50);
    EXPECT_EQ(s.admitDirect(1), Sink::kDirectTotalOnly) << "the oldest tombstone is evicted first";
    EXPECT_EQ(s.admitDirect(2), Sink::kDirectDropClosed);
    EXPECT_EQ(s.admitDirect(kN + 50), Sink::kDirectDropClosed);
}
