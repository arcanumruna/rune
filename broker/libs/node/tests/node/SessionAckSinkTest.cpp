// SessionAckSinkTest.cpp — worker-side ack publication table + backlog state
// machines (spec §3.4, G-A6).  Fake publication policy; injected clock only.
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
constexpr int64_t kMs = 1'000'000;

AckEntry ack(int64_t sid, int64_t corr, int64_t base = 0) { return AckEntry{sid, corr, base, 0, 0}; }

// Announce + resolve (two flushes: Announced→Resolving, Resolving→Ready).
void ready(Sink& s, int64_t sid, int64_t& now) {
    ASSERT_TRUE(s.announce(sid, "aeron:udp?endpoint=127.0.0.1:1", 7));
    s.flush(now += kMs);
    s.flush(now += kMs);
}
} // namespace

TEST(SessionAckSinkRegression, AnnounceResolvesThenSendsEncodedProduceResponse) {
    FakePolicy pol; auto st = pol.s;
    Sink s(pol);
    ASSERT_TRUE(s.announce(7, "aeron:udp?endpoint=h:1", 42));
    ASSERT_TRUE(s.enqueue(AckEntry{7, 100, 5, 2, 0}));
    s.flush(1 * kMs);                                       // Announced → Resolving; ack held
    EXPECT_TRUE(st->sent.empty());
    EXPECT_FALSE(s.intakeAllowed());
    s.flush(2 * kMs);                                       // Resolving → Ready; ack sent
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(st->added.begin()->second.first, "aeron:udp?endpoint=h:1");
    EXPECT_EQ(st->added.begin()->second.second, 42);
    auto r = pc::ProduceResponse::decode(st->sent[0].second.data(), st->sent[0].second.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->sessionId, 7); EXPECT_EQ(r->correlationId, 100);
    EXPECT_EQ(r->baseOffset, 5); EXPECT_EQ(r->partition, 2); EXPECT_EQ(r->errorCode, 0);
}

TEST(SessionAckSinkRegression, CoalescesEveryAckOfAPassOnTheSessionPublication) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now); ready(s, 8, now);
    s.enqueue(ack(7, 1)); s.enqueue(ack(7, 2)); s.enqueue(ack(8, 3)); s.enqueue(ack(7, 4));
    EXPECT_TRUE(st->sent.empty()) << "nothing leaves before the pass flush";
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 4u);
    std::vector<int64_t> corrs7;
    for (auto& [pub, f] : st->sent)
        if (auto r = pc::ProduceResponse::decode(f.data(), f.size()); r && r->sessionId == 7) corrs7.push_back(r->correlationId);
    EXPECT_EQ(corrs7, (std::vector<int64_t>{1, 2, 4}));
}

TEST(SessionAckSinkRegression, BackPressureBacklogsPausesIntakeAndPreservesOrder) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->blocked.insert(1);                                  // the session's pub (reg 1)
    s.enqueue(ack(7, 10)); s.enqueue(ack(7, 11));
    s.flush(now += kMs);
    EXPECT_FALSE(s.intakeAllowed()) << "G-A6: back-pressured acks pause intake";
    EXPECT_EQ(s.pendingForTest(), 2u);
    st->blocked.clear();
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 2u);
    EXPECT_EQ(pc::ProduceResponse::decode(st->sent[0].second.data(), st->sent[0].second.size())->correlationId, 10);
    EXPECT_EQ(pc::ProduceResponse::decode(st->sent[1].second.data(), st->sent[1].second.size())->correlationId, 11);
    EXPECT_TRUE(s.intakeAllowed());
}

TEST(SessionAckSinkRegression, CloseWhileBackloggedClearsAndResumesIntake) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->blocked.insert(1);
    s.enqueue(ack(7, 10));
    s.flush(now += kMs);
    ASSERT_FALSE(s.intakeAllowed());
    s.close(7);
    s.flush(now += kMs);
    EXPECT_TRUE(s.intakeAllowed()) << "a closed session's acks must not pause the shard";
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSinkRegression, StalledSessionDroppedAfterStallWindow) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);                                       // pub id 1
    ready(s, 8, now);                                       // pub id 2
    st->blocked.insert(1);
    s.enqueue(ack(7, 1));
    const int64_t t0 = (now += kMs);
    s.flush(t0);
    s.enqueue(ack(8, 2));
    s.flush(t0 + kMs);
    EXPECT_FALSE(s.intakeAllowed());
    EXPECT_EQ(st->sent.size(), 1u) << "other sessions keep flowing";
    s.flush(t0 + Sink::kStallDropNs);
    EXPECT_TRUE(s.intakeAllowed()) << "a never-connecting client cannot block the shard";
    ASSERT_NE(s.stateForTest(7), nullptr);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    s.enqueue(ack(7, 3));
    s.flush(t0 + Sink::kStallDropNs + kMs);
    EXPECT_TRUE(s.intakeAllowed());
}

TEST(SessionAckSinkRegression, ResolveTimeoutFailsTheSession) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findReady = false;
    ASSERT_TRUE(s.announce(7, "c", 1));
    s.enqueue(ack(7, 1));
    s.flush(1 * kMs);
    s.flush(2 * kMs);
    EXPECT_FALSE(s.intakeAllowed());
    s.flush(1 * kMs + Sink::kResolveTimeoutNs + 1);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSinkRegression, UnknownSessionAckIsDroppedAndCounted) {
    FakePolicy pol; Sink s(pol);
    s.enqueue(ack(99, 1));
    s.flush(1 * kMs);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSinkRegression, OversizedChannelAnnouncementRefused) {
    FakePolicy pol; Sink s(pol);
    EXPECT_FALSE(s.announce(7, std::string(Sink::kMaxChannelBytes + 1, 'x'), 1));
}

TEST(SessionAckSinkRegression, ClosedPublicationFailsTheSessionAtOnce) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->closed.insert(1);
    s.enqueue(ack(7, 1)); s.enqueue(ack(7, 2));
    s.flush(now += kMs);
    EXPECT_TRUE(s.intakeAllowed()) << "a CLOSED publication can never deliver: fail now, not after the stall window";
    ASSERT_NE(s.stateForTest(7), nullptr);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    EXPECT_EQ(s.droppedForTest(), 2u);
}

TEST(SessionAckSinkRegression, NotYetConnectedPublicationHoldsAcksUntilItConnects) {
    // A freshly announced session's publication is NOT_CONNECTED until the
    // client's image is up (Aeron setup): its first acks wait, they are not lost.
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->blocked.insert(1);
    s.enqueue(ack(7, 1));
    for (int i = 0; i < 3; ++i) s.flush(now += kMs);
    EXPECT_TRUE(st->sent.empty());
    st->blocked.clear();
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(s.droppedForTest(), 0u);
}

TEST(SessionAckSinkRegression, ReannounceAfterCloseOpensAFreshPublication) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    s.close(7);
    EXPECT_EQ(s.stateForTest(7), nullptr);
    ready(s, 7, now);
    EXPECT_EQ(st->added.size(), 2u);
    s.enqueue(ack(7, 1));
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_EQ(st->sent[0].first, 2) << "acked on the new registration";
}

TEST(SessionAckSinkRegression, ManySessionsSurviveBackwardShiftDeletion) {
    // Open addressing with backward-shift deletion: closing every other
    // session must leave every remaining one findable.
    FakePolicy pol; Sink s(pol); int64_t now = 0;
    constexpr int64_t kN = 600;
    for (int64_t sid = 1; sid <= kN; ++sid) ASSERT_TRUE(s.announce(sid, "c", 1));
    s.flush(now += kMs); s.flush(now += kMs);   // all resolved (within kMaxUnresolved batches)
    for (int i = 0; i < 16; ++i) s.flush(now += kMs);
    for (int64_t sid = 1; sid <= kN; sid += 2) s.close(sid);
    for (int64_t sid = 1; sid <= kN; ++sid) {
        if (sid % 2 == 1) EXPECT_EQ(s.stateForTest(sid), nullptr) << sid;
        else ASSERT_NE(s.stateForTest(sid), nullptr) << sid;
    }
}
