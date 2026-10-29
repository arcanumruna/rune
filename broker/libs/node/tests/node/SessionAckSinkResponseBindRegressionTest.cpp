// SessionAckSinkResponseBindRegressionTest.cpp — NAT-safe egress Task 6: the PubAwaitingBind state, bind(),
// boundImage(), readyPub() and unbindImage() of the worker's ack sink (fake publication policy, no clock, no driver).
#include <rune/node/SessionAckSink.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

using namespace rune::node;

namespace {
struct FakePub { int id = 0; };
struct FakePolicy {
    using PubHandle = std::shared_ptr<FakePub>;
    struct State {
        int64_t nextReg = 1;
        std::map<int64_t, std::pair<std::string, int32_t>> added;
        bool findReady = true;
        std::set<int> closed;
        std::vector<std::pair<int, std::vector<uint8_t>>> sent;
    };
    std::shared_ptr<State> s = std::make_shared<State>();
    int64_t add(std::string_view ch, int32_t stream) noexcept {
        const int64_t r = s->nextReg++;
        s->added[r] = {std::string(ch), stream};
        return r;
    }
    PubFind find(int64_t reg, PubHandle& out) noexcept {
        if (!s->findReady) return PubFind::Pending;
        out = std::make_shared<FakePub>(FakePub{static_cast<int>(reg)});
        return PubFind::Ready;
    }
    template <class E> int64_t claim(PubHandle& p, std::size_t len, E&& enc) noexcept {
        if (s->closed.count(p->id)) return -4;
        std::vector<uint8_t> b(len);
        enc(b.data());
        s->sent.push_back({p->id, std::move(b)});
        return 1;
    }
};
using Sink = SessionAckSinkT<FakePolicy>;
using BR   = Sink::BindResult;

constexpr int64_t kSid   = 7;
constexpr int32_t kImage = 900;
constexpr int32_t kOther = 901;
constexpr int32_t kStream = 33;
const std::string kRespChan = "aeron:udp?control=127.0.0.1:41000|control-mode=response|response-correlation-id=5";

std::array<uint8_t, 16> tok() { std::array<uint8_t, 16> t{}; for (std::size_t i = 0; i < 16; ++i) t[i] = static_cast<uint8_t>(0xA0 + i); return t; }
std::array<uint8_t, 16> tokLastByteOff() { auto t = tok(); t[15] ^= 0x01; return t; }
std::array<uint8_t, 16> tokFirstByteOff() { auto t = tok(); t[0] ^= 0x80; return t; }
void pass(Sink& s) { s.flush(0, 0); }

enum class St { Unknown, Closed, Await, Annc, Resv, Ready, Failed, BoundAnnc, BoundResv, BoundReady };
enum class Kind { None, Await, Annc, Resv, Ready, Failed };
enum class Act { BindRight, BindWrong, BindTwice, AnnounceAgain, Close, Enqueue, UnbindMatch, UnbindOther };

Kind kindOf(const Sink& s, int64_t sid) {
    const auto* st = s.stateForTest(sid);
    if (!st) return Kind::None;
    if (std::holds_alternative<Sink::PubAwaitingBind>(*st)) return Kind::Await;
    if (std::holds_alternative<Sink::PubAnnounced>(*st)) return Kind::Annc;
    if (std::holds_alternative<Sink::PubResolving>(*st)) return Kind::Resv;
    if (std::holds_alternative<Sink::PubReady>(*st)) return Kind::Ready;
    return Kind::Failed;
}

struct Rig {
    FakePolicy pol;
    std::shared_ptr<FakePolicy::State> st = pol.s;
    Sink s{pol};
    void setup(St state) {
        switch (state) {
        case St::Unknown: break;
        case St::Closed:  ASSERT_TRUE(s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7)); s.close(kSid); break;
        case St::Await:   ASSERT_TRUE(s.announceResponse(kSid, tok(), kStream)); break;
        case St::Annc:    ASSERT_TRUE(s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7)); break;
        case St::Resv:    st->findReady = false; ASSERT_TRUE(s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7)); pass(s); break;
        case St::Ready:   ASSERT_TRUE(s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7)); pass(s); pass(s); break;
        case St::Failed:  ASSERT_TRUE(s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7)); pass(s); pass(s);
                          st->closed.insert(1); s.enqueue(AckEntry{kSid, 1, 0, 0, 0}); pass(s); break;
        case St::BoundAnnc:  ASSERT_TRUE(s.announceResponse(kSid, tok(), kStream)); ASSERT_EQ(s.bind(kSid, tok(), kImage, kRespChan), BR::Bound); break;
        case St::BoundResv:  st->findReady = false; ASSERT_TRUE(s.announceResponse(kSid, tok(), kStream));
                             ASSERT_EQ(s.bind(kSid, tok(), kImage, kRespChan), BR::Bound); pass(s); break;
        case St::BoundReady: ASSERT_TRUE(s.announceResponse(kSid, tok(), kStream)); ASSERT_EQ(s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
                             pass(s); pass(s); break;
        }
    }
};

struct Cell { std::optional<BR> br; Kind kind; std::optional<int32_t> image; std::size_t unreported = 0; };
constexpr auto N = std::nullopt;

// expected[state][action]
Cell expected(St st, Act a) {
    const bool bound = st == St::BoundAnnc || st == St::BoundResv || st == St::BoundReady;
    const Kind plain = st == St::Unknown || st == St::Closed ? Kind::None
                     : st == St::Await ? Kind::Await : st == St::Annc || st == St::BoundAnnc ? Kind::Annc
                     : st == St::Resv || st == St::BoundResv ? Kind::Resv
                     : st == St::Ready || st == St::BoundReady ? Kind::Ready : Kind::Failed;
    const std::optional<int32_t> img = bound ? std::optional<int32_t>(kImage) : std::nullopt;
    switch (a) {
    case Act::BindRight:
        if (st == St::Unknown || st == St::Closed) return {BR::UnknownSession, Kind::None, N};
        if (st == St::Await) return {BR::Bound, Kind::Annc, kImage};
        if (bound) return {BR::AlreadyBound, plain, img};   // I2: the repeat of a bind (same image, same token) is a no-op
        return {BR::NotAwaiting, plain, img};
    case Act::BindWrong:
        if (st == St::Unknown || st == St::Closed) return {BR::UnknownSession, Kind::None, N};
        if (st == St::Await) return {BR::BadToken, Kind::Await, N};
        return {BR::NotAwaiting, plain, img};
    case Act::BindTwice:   // the SECOND bind's result
        if (st == St::Unknown || st == St::Closed) return {BR::UnknownSession, Kind::None, N};
        if (st == St::Await) return {BR::NotAwaiting, Kind::Annc, kImage};   // never rebinds, even with the right token
        return {BR::NotAwaiting, plain, img};
    case Act::AnnounceAgain: return {N, Kind::Annc, N};
    case Act::Close:         return {N, Kind::None, N};
    // the fake policy resolves at once: Announced -> Resolving (pass 1) -> Ready (pass 2, the ack leaves in that pass)
    case Act::Enqueue:       return {N, st == St::Annc || st == St::BoundAnnc ? Kind::Ready : plain, img};
    case Act::UnbindMatch:
        if (bound) return {N, Kind::Failed, kImage, 1};
        return {N, plain, img};
    case Act::UnbindOther:   return {N, plain, img};
    }
    return {};
}
const char* name(St s) { static const char* n[] = {"Unknown","Closed","Await","Annc","Resv","Ready","Failed","BoundAnnc","BoundResv","BoundReady"}; return n[static_cast<int>(s)]; }
const char* name(Act a) { static const char* n[] = {"BindRight","BindWrong","BindTwice","AnnounceAgain","Close","Enqueue","UnbindMatch","UnbindOther"}; return n[static_cast<int>(a)]; }
} // namespace

TEST(SessionAckSinkResponseBindRegression, InteractionMatrixEveryStateEveryAction) {
    for (int si = 0; si <= static_cast<int>(St::BoundReady); ++si)
        for (int ai = 0; ai <= static_cast<int>(Act::UnbindOther); ++ai) {
            const St state = static_cast<St>(si);
            const Act act  = static_cast<Act>(ai);
            SCOPED_TRACE(std::string(name(state)) + " x " + name(act));
            Rig r;
            r.setup(state);
            if (::testing::Test::HasFatalFailure()) return;
            const Cell want = expected(state, act);
            std::optional<BR> got;
            switch (act) {
            case Act::BindRight: got = r.s.bind(kSid, tok(), kImage, kRespChan); break;
            case Act::BindWrong: got = r.s.bind(kSid, tokLastByteOff(), kImage, kRespChan); break;
            case Act::BindTwice: (void)r.s.bind(kSid, tok(), kImage, kRespChan); got = r.s.bind(kSid, tok(), kOther, kRespChan); break;
            case Act::AnnounceAgain: ASSERT_TRUE(r.s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:2", 8)); break;
            case Act::Close: r.s.close(kSid); break;
            case Act::Enqueue: ASSERT_TRUE(r.s.enqueue(AckEntry{kSid, 99, 5, 0, 0})); pass(r.s); pass(r.s); break;
            case Act::UnbindMatch: r.s.unbindImage(kImage); break;
            case Act::UnbindOther: r.s.unbindImage(kOther); break;
            }
            ASSERT_EQ(got.has_value(), want.br.has_value());
            if (want.br) EXPECT_EQ(*got, *want.br);
            EXPECT_EQ(kindOf(r.s, kSid), want.kind);
            EXPECT_EQ(r.s.boundImage(kSid), want.image);
            EXPECT_EQ(r.s.unreportedFailures(), want.kind == Kind::Failed ? 1u : 0u) << "every Failed state here is unreported";
            if (act == Act::Enqueue) {
                // Ready (plain or bound): the ack left; Unknown/Closed/Failed: dropped; every other state: HELD.
                const bool ready = state == St::Ready || state == St::BoundReady || state == St::Annc || state == St::BoundAnnc;
                const bool dropped = state == St::Unknown || state == St::Closed || state == St::Failed;
                EXPECT_EQ(r.st->sent.size(), ready ? 1u : 0u);
                EXPECT_EQ(r.s.heldAcks(), (ready || dropped) ? 0u : 1u);
            }
        }
}

TEST(SessionAckSinkResponseBindRegression, WrongTokenDifferingOnlyInTheLastOrFirstByteIsBadToken) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    EXPECT_EQ(r.s.bind(kSid, tokLastByteOff(), kImage, kRespChan), BR::BadToken);
    EXPECT_EQ(r.s.bind(kSid, tokFirstByteOff(), kImage, kRespChan), BR::BadToken);
    EXPECT_EQ(r.s.bind(kSid, std::array<uint8_t, 16>{}, kImage, kRespChan), BR::BadToken);
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
    EXPECT_FALSE(r.s.boundImage(kSid).has_value());
    EXPECT_TRUE(r.st->added.empty()) << "a refused bind never adds a publication";
    EXPECT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound) << "the right token still binds afterwards";
}

TEST(SessionAckSinkResponseBindRegression, AcksEnqueuedWhileAwaitingBindAreHeldThenFlushedInOrderAfterBind) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    for (int64_t c = 1; c <= 5; ++c) ASSERT_TRUE(r.s.enqueue(AckEntry{kSid, c, c * 10, 0, 0}));
    pass(r.s); pass(r.s);
    EXPECT_TRUE(r.st->sent.empty());
    EXPECT_EQ(r.s.heldAcks(), 5u);
    EXPECT_TRUE(r.s.holding(kSid)) << "the worker pauses this session's intake while it awaits the bind";
    EXPECT_EQ(r.s.readyPub(kSid), nullptr);
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    pass(r.s);                                                   // Announced -> Resolving: the publication is added now
    ASSERT_EQ(r.st->added.size(), 1u);
    EXPECT_EQ(r.st->added.begin()->second.first, kRespChan);
    EXPECT_EQ(r.st->added.begin()->second.second, kStream) << "the announced egress stream id";
    pass(r.s);
    ASSERT_EQ(r.st->sent.size(), 5u);
    for (std::size_t i = 0; i < 5; ++i) {
        const auto d = rune::protocol::client::ProduceResponse::decode(r.st->sent[i].second.data(), r.st->sent[i].second.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->correlationId, static_cast<int64_t>(i + 1));
    }
    EXPECT_EQ(r.s.heldAcks(), 0u);
    ASSERT_NE(r.s.readyPub(kSid), nullptr);
    EXPECT_EQ(r.s.boundImage(kSid), kImage);
}

TEST(SessionAckSinkResponseBindRegression, AwaitingBindUsesNoChannelSlotAndIsNotListedForResolution) {
    Rig r;
    const std::size_t freeBefore = r.s.freeChannelSlotsForTest();
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    EXPECT_EQ(r.s.freeChannelSlotsForTest(), freeBefore);
    pass(r.s); pass(r.s);
    EXPECT_TRUE(r.st->added.empty());
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    EXPECT_EQ(r.s.freeChannelSlotsForTest(), freeBefore - 1);
    pass(r.s); pass(r.s);
    EXPECT_EQ(r.s.freeChannelSlotsForTest(), freeBefore) << "the slot returns once the publication is Ready";
    r.s.close(kSid);
    EXPECT_EQ(r.s.readyPub(kSid), nullptr);
}

TEST(SessionAckSinkResponseBindRegression, SecondAnnounceWaveNeverUndoesABindOrAnAwaitingBind) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));              // the crypto-key wave before the bind
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    pass(r.s); pass(r.s);
    ASSERT_NE(r.s.readyPub(kSid), nullptr);
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));              // ... or after it
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Ready);
    EXPECT_EQ(r.s.boundImage(kSid), kImage);
}

TEST(SessionAckSinkResponseBindRegression, UnbindImageFailsEverySessionOfThatImageOnceAndReportsIt) {
    Rig r;
    for (int64_t sid : {10, 11, 12}) {
        ASSERT_TRUE(r.s.announceResponse(sid, tok(), kStream));
        ASSERT_EQ(r.s.bind(sid, tok(), sid == 12 ? kOther : kImage, kRespChan), BR::Bound);
    }
    pass(r.s); pass(r.s);
    EXPECT_EQ(r.s.unbindImage(kImage), 2);
    EXPECT_EQ(r.s.unbindImage(kImage), 0) << "idempotent";
    EXPECT_EQ(kindOf(r.s, 10), Kind::Failed);
    EXPECT_EQ(kindOf(r.s, 11), Kind::Failed);
    EXPECT_EQ(kindOf(r.s, 12), Kind::Ready);
    EXPECT_EQ(r.s.readyPub(10), nullptr);
    std::vector<int64_t> reported;
    r.s.reportFailures([&](int64_t sid) { reported.push_back(sid); return true; });
    std::sort(reported.begin(), reported.end());
    EXPECT_EQ(reported, (std::vector<int64_t>{10, 11}));
}

TEST(SessionAckSinkResponseBindRegression, BackwardShiftKeepsEveryNeighbourBoundImage) {
    Rig r;
    constexpr int64_t kN = 3000;                                  // ~73 % load: long probe chains
    for (int64_t sid = 1; sid <= kN; ++sid) {
        ASSERT_TRUE(r.s.announceResponse(sid, tok(), kStream));
        ASSERT_EQ(r.s.bind(sid, tok(), static_cast<int32_t>(1000 + sid), kRespChan), BR::Bound);
    }
    std::size_t longest = 0;
    for (int64_t sid = 1; sid <= kN; ++sid) longest = std::max(longest, r.s.probeChainForTest(sid));
    ASSERT_GT(longest, 2u) << "the table must actually collide for this test to mean anything";
    for (int64_t sid = 1; sid <= kN; sid += 3) r.s.close(sid);
    for (int64_t sid = 1; sid <= kN; ++sid) {
        if ((sid - 1) % 3 == 0) EXPECT_FALSE(r.s.boundImage(sid).has_value()) << sid;
        else                    EXPECT_EQ(r.s.boundImage(sid), static_cast<int32_t>(1000 + sid)) << sid;
    }
    for (int64_t sid = 2; sid <= kN; sid += 3) r.s.close(sid);
    for (int64_t sid = 3; sid <= kN; sid += 3) EXPECT_EQ(r.s.boundImage(sid), static_cast<int32_t>(1000 + sid)) << sid;
}

TEST(SessionAckSinkResponseBindRegression, PoolExhaustedBindStillRegistersThePublication) {
    Rig r;
    constexpr int64_t kN = Sink::kMaxChannelSlots + 20;
    for (int64_t sid = 1; sid <= kN; ++sid) {
        ASSERT_TRUE(r.s.announceResponse(sid, tok(), kStream));
        ASSERT_EQ(r.s.bind(sid, tok(), 5, kRespChan), BR::Bound);
    }
    EXPECT_EQ(r.st->added.size(), 20u) << "the overflow binds register immediately (addNow_)";
    for (int i = 0; i < 4; ++i) pass(r.s);
    for (int64_t sid = 1; sid <= kN; ++sid) EXPECT_NE(r.s.readyPub(sid), nullptr) << sid;
}

// ---- fix round 1a: I4 / I5 ----------------------------------------------------------------------------------

namespace {
constexpr int64_t kPlain = 50;
}

// I4: the bind's authorization half is allocation-free and side-effect-free; a refused bind leaves everything as it was.
TEST(SessionAckSinkRegression, BadTokenBindDoesNoWorkRegression) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    ASSERT_TRUE(r.s.announce(kPlain, "aeron:udp?endpoint=127.0.0.1:1", 7));
    const std::size_t freeBefore = r.s.freeChannelSlotsForTest();

    EXPECT_EQ(r.s.checkBind(kSid, tokLastByteOff()), BR::BadToken);
    EXPECT_EQ(r.s.checkBind(kSid, tokFirstByteOff()), BR::BadToken);
    EXPECT_EQ(r.s.checkBind(kSid, std::array<uint8_t, 16>{}), BR::BadToken);
    EXPECT_EQ(r.s.checkBind(kSid + 1, tok()), BR::UnknownSession);
    EXPECT_EQ(r.s.checkBind(kPlain, tok()), BR::NotAwaiting) << "a plain (non response-mode) session cannot bind";
    // No side effect: still awaiting, no channel slot taken, no publication added, not bound.
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
    EXPECT_EQ(r.s.freeChannelSlotsForTest(), freeBefore);
    pass(r.s);
    EXPECT_TRUE(r.st->added.size() <= 1u) << "only the plain session's own publication";
    EXPECT_FALSE(r.s.boundImage(kSid).has_value());

    // The eligible check agrees with bind() and itself changes nothing.
    EXPECT_EQ(r.s.checkBind(kSid, tok()), BR::Bound);
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
    EXPECT_EQ(r.s.bind(kSid, tokLastByteOff(), kImage, kRespChan), BR::BadToken) << "bind() still re-checks (defence)";
    EXPECT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    EXPECT_EQ(r.s.checkBind(kSid, tok()), BR::AlreadyBound) << "a bound session never binds again (same token = the repeat)";
    EXPECT_EQ(r.s.checkBind(kSid, tokLastByteOff()), BR::NotAwaiting) << "... and any other token is refused";
}

// I5: ONE word decides admission: awaiting -> every image dropped; bound -> only its image; plain -> admitted.
TEST(SessionAckSinkRegression, AwaitingBindDropsEveryImageWithOneLoadRegression) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    for (int32_t img : {0, kImage, kOther, -1, INT32_MIN, INT32_MAX}) {
        const auto tag = r.s.admitDirect(kSid, img);
        EXPECT_EQ(tag, Sink::kDirectDropSpoofed) << "awaiting its bind: image " << img;
    }
    EXPECT_EQ(r.s.directDroppedSpoofedForTest(), 6u);
    EXPECT_FALSE(r.s.boundImage(kSid).has_value()) << "awaiting is NOT bound";
    std::vector<int32_t> seen;
    r.s.forEachBoundImage([&](int32_t i) { seen.push_back(i); });
    EXPECT_TRUE(seen.empty()) << "forEachBoundImage skips an awaiting session";
    EXPECT_EQ(r.s.unbindImage(0), 0) << "no image can unbind an awaiting session";

    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    EXPECT_EQ(r.s.admitDirect(kSid, kOther), Sink::kDirectDropSpoofed);
    EXPECT_EQ(r.s.admitDirect(kSid, 0), Sink::kDirectDropSpoofed);
    const auto ok = r.s.admitDirect(kSid, kImage);
    EXPECT_TRUE(ok == Sink::kDirectCounted || ok == Sink::kDirectTotalOnly) << static_cast<int>(ok);
    EXPECT_EQ(r.s.boundImage(kSid), kImage);

    // A bound image that happens to be 0 / negative is still a real image key (never the awaiting marker).
    for (int32_t img : {0, -1, INT32_MIN}) {
        Rig q;
        ASSERT_TRUE(q.s.announceResponse(kSid, tok(), kStream));
        ASSERT_EQ(q.s.bind(kSid, tok(), img, kRespChan), BR::Bound);
        EXPECT_EQ(q.s.boundImage(kSid), img);
        EXPECT_NE(q.s.admitDirect(kSid, img), Sink::kDirectDropSpoofed) << img;
        EXPECT_EQ(q.s.admitDirect(kSid, img + 1), Sink::kDirectDropSpoofed) << img;
    }

    // Plain sessions are admitted from any image (a legacy client's frames carry no image meaning).
    ASSERT_TRUE(r.s.announce(kPlain, "aeron:udp?endpoint=127.0.0.1:1", 7));
    for (int32_t img : {0, kImage, kOther, INT32_MIN}) {
        const auto tag = r.s.admitDirect(kPlain, img);
        EXPECT_NE(tag, Sink::kDirectDropSpoofed) << img;
        EXPECT_NE(tag, Sink::kDirectRefused) << img;
    }
    // A re-announce as plain / response resets and restores the right word.
    ASSERT_TRUE(r.s.announce(kSid, "aeron:udp?endpoint=127.0.0.1:1", 7));
    EXPECT_NE(r.s.admitDirect(kSid, kOther), Sink::kDirectDropSpoofed) << "plain again";
    ASSERT_TRUE(r.s.announce(kSid + 2, "aeron:udp?endpoint=127.0.0.1:1", 7));
    r.s.close(kSid + 2);
    ASSERT_TRUE(r.s.announceResponse(kSid + 2, tok(), kStream));
    EXPECT_EQ(r.s.admitDirect(kSid + 2, kImage), Sink::kDirectDropSpoofed) << "reused slot: awaiting again";
}

TEST(SessionAckSinkResponseBindRegression, RepeatedBindSameImageSameTokenIsAlreadyBoundNoStateChangeRegression) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    pass(r.s); pass(r.s);
    ASSERT_NE(r.s.readyPub(kSid), nullptr);
    const std::size_t added = r.st->added.size();
    EXPECT_EQ(r.s.checkBind(kSid, tok()), BR::AlreadyBound);
    EXPECT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::AlreadyBound);
    EXPECT_EQ(r.s.bind(kSid, tok(), kOther, kRespChan), BR::NotAwaiting) << "another image never rebinds";
    EXPECT_EQ(r.s.bind(kSid, tokLastByteOff(), kImage, kRespChan), BR::NotAwaiting) << "a wrong token never reaches AlreadyBound";
    EXPECT_EQ(r.s.checkBind(kSid, tokFirstByteOff()), BR::NotAwaiting);
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Ready);
    EXPECT_EQ(r.s.boundImage(kSid), kImage);
    EXPECT_EQ(r.st->added.size(), added) << "no second publication";
}

TEST(SessionAckSinkResponseBindRegression, AnnounceResponseKeepsAnUnreportedFailureRegression) {
    Rig r;
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    ASSERT_EQ(r.s.unbindImage(kImage), 1);                     // Failed{reported = false}
    ASSERT_EQ(r.s.unreportedFailures(), 1u);
    EXPECT_EQ(r.s.checkBind(kSid, tok()), BR::NotAwaiting) << "a failed session is never AlreadyBound";
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));   // the keyReady wave arriving after the failure
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Failed) << "the failure must still be reported, not turned into AwaitingBind";
    EXPECT_EQ(r.s.unreportedFailures(), 1u);
    std::vector<int64_t> reported;
    r.s.reportFailures([&](int64_t sid) { reported.push_back(sid); return true; });
    EXPECT_EQ(reported, std::vector<int64_t>{kSid});
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));   // a re-announce after the REPORT starts afresh
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
}

// I-C: an all-zero token (the "no token" marker, and what a failed key derivation yields) authorizes nothing: it is
// refused at the announce, can never overwrite a real token, and never binds.
TEST(SessionAckSinkResponseBindRegression, ZeroTokenNeverBindsRegression) {
    const std::array<uint8_t, 16> zero{};
    Rig r;
    // A fresh session announced with a zero token: refused, no state, nothing to bind.
    EXPECT_FALSE(r.s.announceResponse(kSid + 1, zero, kStream));
    EXPECT_EQ(kindOf(r.s, kSid + 1), Kind::None);
    EXPECT_EQ(r.s.checkBind(kSid + 1, zero), BR::BadToken) << "a zero-token bind never reaches the lookup";

    // A real announce, then a zero re-announce (the keyReady wave of a failed derivation): refused, the real token stays.
    ASSERT_TRUE(r.s.announceResponse(kSid, tok(), kStream));
    EXPECT_FALSE(r.s.announceResponse(kSid, zero, kStream));
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await);
    EXPECT_EQ(r.s.checkBind(kSid, zero), BR::BadToken);
    EXPECT_EQ(r.s.bind(kSid, zero, kImage, kRespChan), BR::BadToken);
    EXPECT_EQ(kindOf(r.s, kSid), Kind::Await) << "a refused bind changes nothing";
    EXPECT_EQ(r.s.checkBind(kSid, tok()), BR::Bound) << "the real token is untouched by the refused zero announce";
    ASSERT_EQ(r.s.bind(kSid, tok(), kImage, kRespChan), BR::Bound);
    EXPECT_EQ(r.s.checkBind(kSid, zero), BR::BadToken);
}
