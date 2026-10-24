// PeerLinkAdversarialTest.cpp — review of PR #538 (issues #241 / #242): attacks on the peer link handshake and the
// frame crypto through their public APIs and wire frames.  Pure: an injected clock, seeded randomness, no sleeps.
#include <rune/crypto/Dh.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/peerlink/PeerHandshake.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <deque>
#include <random>
#include <string>
#include <vector>

namespace pl = rune::peerlink;
using OR = pl::PeerFrameCrypto::OpenResult;

namespace {

pl::ClusterKeys keysOf(char c) {
    std::string k(48, c);
    auto m = rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    return {*m, std::nullopt};
}
constexpr int64_t kMs = 1'000'000;

using Bytes = std::vector<uint8_t>;
Bytes frameOf(const pl::PeerHandshake::Out& o) { return o.frame ? Bytes(o.frame->data(), o.frame->data() + o.frame->size()) : Bytes{}; }

/// One broker end of a link: its handshake and ONE frame-crypto instance (same sender id at both ends).
struct End {
    int32_t self, peer;
    pl::PeerHandshake hs;
    pl::PeerFrameCrypto crypto;
    End(int32_t s, int32_t p, uint64_t inc) : self(s), peer(p), hs(s, p, inc, keysOf('k')), crypto(s, 0) {}
};

/// Two ends wired through a lossy, FIFO handshake stream and a reordering data stream (data frames travel on other
/// Aeron streams, so nothing orders them against the handshake frames).  Every delivered data frame MUST open.
struct Sim {
    std::mt19937_64 rng;
    End a{1, 2, 10}, b{2, 1, 20};   // a = initiator (lower id)
    std::deque<Bytes> hsTo[2];       // [0] -> a, [1] -> b
    struct Data { int to; Bytes bytes; int64_t sentNs; };
    std::vector<Data> data;
    int64_t now = 0, lastRekeyNs = -1'000'000'000'000;
    uint64_t opened = 0, rekeys = 0;
    std::string failure;

    std::string describe() const {
        const auto* ea = std::get_if<pl::Established>(&a.hs.state());
        const auto* eb = std::get_if<pl::Established>(&b.hs.state());
        return "a.state=" + std::to_string(a.hs.state().index()) + (ea ? " a.hsEpoch=" + std::to_string(ea->keys.epoch) : "") +
               " b.state=" + std::to_string(b.hs.state().index()) + (eb ? " b.hsEpoch=" + std::to_string(eb->keys.epoch) +
               (eb->unconfirmedAccept ? " (unconfirmed)" : "") : "") + " a.send=" + std::to_string(a.crypto.currentEpoch(2)) +
               " b.send=" + std::to_string(b.crypto.currentEpoch(1)) + " " + failure;
    }

    explicit Sim(uint64_t seed) : rng(seed) {}
    End& at(int i) { return i == 0 ? a : b; }
    bool chance(double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; }

    void absorb(int i, pl::PeerHandshake::Out o) {
        End& e = at(i);
        if (o.established && !e.crypto.installLink(e.peer, *o.established, now)) failure = "installLink refused";
        if (o.frame) hsTo[1 - i].push_back(frameOf(o));
    }
    void deliverHs(int to, const Bytes& f) {
        End& e = at(to);
        switch (pl::peekPeerMsg(f.data(), f.size())) {
            case 4: { pl::PeerAccept m;    if (pl::PeerAccept::decode(f.data(), f.size(), m))    absorb(to, e.hs.onAccept(m, now)); break; }
            case 3: { pl::PeerProof m;     if (pl::PeerProof::decode(f.data(), f.size(), m))     absorb(to, e.hs.onProof(m, now)); break; }
            case 5: { pl::PeerRefuse m;    if (pl::PeerRefuse::decode(f.data(), f.size(), m))    absorb(to, e.hs.onRefuse(m, now)); break; }
            case 6: { pl::PeerConfirm m;   if (pl::PeerConfirm::decode(f.data(), f.size(), m))   absorb(to, e.hs.onConfirm(m, now)); break; }
            case 7: { pl::PeerHelloV2 m;   if (pl::PeerHelloV2::decode(f.data(), f.size(), m))   absorb(to, e.hs.onHelloV2(m, now)); break; }
            case 8: { pl::PeerChallengeV2 m; if (pl::PeerChallengeV2::decode(f.data(), f.size(), m)) absorb(to, e.hs.onChallengeV2(m, now)); break; }
            case 9: { pl::PeerProofV2 m;   if (pl::PeerProofV2::decode(f.data(), f.size(), m))   absorb(to, e.hs.onProofV2(m, now)); break; }
            default: failure = "unexpected handshake tag " + std::to_string(f.empty() ? -1 : f[0]);
        }
    }
    void seal(int from) {
        End& e = at(from);
        if (!e.crypto.hasLink(e.peer)) return;
        Bytes out(8 + pl::kPeerFrameOverhead);
        const uint8_t plain[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        const auto n = e.crypto.seal(e.peer, plain, sizeof plain, out.data(), out.size());
        if (n == 0) { failure = "seal failed"; return; }
        out.resize(n);
        data.push_back({1 - from, std::move(out), now});
    }
    void open(std::size_t idx) {
        Data d = std::move(data[idx]);
        data.erase(data.begin() + static_cast<std::ptrdiff_t>(idx));
        End& e = at(d.to);
        uint8_t out[16]; std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
        const auto r = e.crypto.open(d.bytes.data(), d.bytes.size(), out, sizeof out, outLen, from, epoch);
        if (r != OR::Ok) {
            failure = std::string("a data frame to node ") + std::to_string(e.self) + " under epoch " + std::to_string(epoch) +
                      " was dropped: " + pl::PeerFrameCrypto::name(r) + " (send epochs a=" +
                      std::to_string(a.crypto.currentEpoch(2)) + " b=" + std::to_string(b.crypto.currentEpoch(1)) + ")";
            return;
        }
        ++opened;
        e.hs.onAuthenticated(now);
        int32_t peer = -1; uint32_t used = 0;   // BrokerNode::servicePeerLinks_ polls the latch every duty cycle
        if (e.crypto.takePeerUsedEpoch(peer, used)) absorb(d.to, e.hs.onPeerUsedEpoch(used, now));
    }
    void tick() { absorb(0, a.hs.tick(now)); absorb(1, b.hs.tick(now)); }

    /// Establish the first link with no loss (data starts only once both ends hold it).
    void link() {
        for (int i = 0; i < 64 && !(a.crypto.hasLink(2) && b.crypto.hasLink(1)) && failure.empty(); ++i) {
            tick();
            while (!hsTo[0].empty() || !hsTo[1].empty()) {
                for (int to : {0, 1}) if (!hsTo[to].empty()) { Bytes f = hsTo[to].front(); hsTo[to].pop_front(); deliverHs(to, f); }
            }
            now += pl::PeerHandshake::kRetryNs;
        }
    }

    /// One random step.  `maxDataAgeNs`: a data frame older than this is lost (a bounded Aeron stream latency).
    void step(double hsLoss, int64_t maxDataAgeNs) {
        const int k = std::uniform_int_distribution<int>(0, 9)(rng);
        if (k <= 2) {                                                     // a handshake frame moves (or is lost)
            const int to = static_cast<int>(rng() & 1);
            if (hsTo[to].empty()) return;
            Bytes f = hsTo[to].front(); hsTo[to].pop_front();
            if (!chance(hsLoss)) deliverHs(to, f);
        } else if (k <= 4) {
            seal(static_cast<int>(rng() & 1));
        } else if (k <= 6) {                                              // any in-flight data frame, reordered
            if (data.empty()) return;
            open(std::uniform_int_distribution<std::size_t>(0, data.size() - 1)(rng));
        } else if (k <= 8) {
            now += std::uniform_int_distribution<int64_t>(0, 60)(rng) * kMs;
            data.erase(std::remove_if(data.begin(), data.end(), [&](const Data& d) { return now - d.sentNs > maxDataAgeNs; }),
                       data.end());
            tick();
        } else if (now - lastRekeyNs >= pl::PeerHandshake::kRetryNs) {   // a same-incarnation re-handshake (the gate's rate)
            auto o = a.hs.rekeyForTest(now);
            if (o.frame) { lastRekeyNs = now; ++rekeys; }
            absorb(0, std::move(o));
        }
    }

    /// Lossless quiescence: drain everything, tick past every retry, until both ends seal under the same epoch.
    bool converge() {
        for (int round = 0; round < 64 && failure.empty(); ++round) {
            seal(0); seal(1);
            while ((!hsTo[0].empty() || !hsTo[1].empty() || !data.empty()) && failure.empty()) {
                for (int to : {0, 1}) if (!hsTo[to].empty()) { Bytes f = hsTo[to].front(); hsTo[to].pop_front(); deliverHs(to, f); }
                if (!data.empty()) open(0);
            }
            now += pl::PeerHandshake::kRetryNs;
            tick();
            // Issue #545: a stale unconfirmed epoch (its exchange abandoned) is legal and no longer re-sent, so quiet
            // queues after a tick, both ends Established and one send epoch are the convergence.
            if (data.empty() && hsTo[0].empty() && hsTo[1].empty() && std::holds_alternative<pl::Established>(b.hs.state()) &&
                std::holds_alternative<pl::Established>(a.hs.state()) && a.crypto.currentEpoch(2) == b.crypto.currentEpoch(1))
                return true;
        }
        return false;
    }
};

} // namespace

// ---- Data integrity / Network: a seeded model check of the receive-first re-handshake (issue #241). ----
// Both ends seal continuously; same-incarnation re-handshakes start at the hint gate's rate; handshake frames are lost
// at random; data frames are reordered against them (other streams) and live at most 150 ms.  EVERY data frame that is
// delivered must open (no UnknownEpoch, no BadTag), and the link must converge to one send epoch once the loss stops.
// Mutation: the responder installing SendAndReceive at the Proof (receiveFirst = false) turns this red.
// Issue #584: widened from 200 seeds (the responder starting a second epoch while one was unconfirmed, and the
// initiator evicting the responder's send epoch, failed ~1 seed in 200).
constexpr uint64_t kModelSeeds = 1'000;
TEST(PeerLinkAdversarialRegression, NoDeliveredFrameIsDroppedAcrossLossyReorderedRehandshakesAdversarialRegression) {
    uint64_t rekeys = 0, opened = 0;
    for (uint64_t seed = 1; seed <= kModelSeeds; ++seed) {
        Sim s(seed);
        s.link();
        ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1)) << "seed " << seed;
        s.now += 2'000 * kMs;
        for (int i = 0; i < 1'500 && s.failure.empty(); ++i) s.step(/*hsLoss=*/0.15, /*maxDataAgeNs=*/150 * kMs);
        ASSERT_TRUE(s.failure.empty()) << "seed " << seed << ": " << s.failure;
        ASSERT_TRUE(s.converge()) << "seed " << seed << ": the link never converged: " << s.describe();
        rekeys += s.rekeys; opened += s.opened;
    }
    EXPECT_GT(rekeys, 1'000u) << "the model must actually re-handshake";
    EXPECT_GT(opened, 20'000u);
}

namespace {
const pl::Established* estOf(const pl::PeerHandshake& h);
/// Pops the next handshake frame queued to `to`, asserts its tag and returns it (empty when the queue is empty).
Bytes popHs(Sim& s, int to, pl::PeerMsg want) {
    if (s.hsTo[to].empty()) { ADD_FAILURE() << "no handshake frame queued to " << to; return {}; }
    Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front();
    EXPECT_EQ(f[0], static_cast<uint8_t>(want)) << "to " << to;
    return f;
}
} // namespace

// ---- Issue #584: a responder holds at most ONE fresh unconfirmed epoch.  A re-handshake whose Confirm was lost, then ----
// a second same-incarnation exchange: the responder answers the second Proof with the SAME Accept (epoch E, no E+1),
// the initiator - now in ChallengeSent - answers it from its `prior` with E's Confirm, and the responder promotes E.
// Mutation: removing the deferral in establishAsResponder_ (fix 1) mints E+1 and leaves the responder sealing under
// E-1 -> red; removing the any-state answer in onAccept (fix 2) leaves the re-sent Accept unanswered -> red.
TEST(PeerLinkAdversarialRegression, AResponderNeverStartsASecondEpochWhileOneIsUnconfirmedRegression) {
    Sim s(584);
    s.link();
    ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1));
    const uint32_t e0 = s.b.crypto.currentEpoch(1);
    s.now += 2'000 * kMs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::ProofV2));
    const Bytes accept1 = popHs(s, 0, pl::PeerMsg::Accept);
    s.deliverHs(0, accept1);
    (void)popHs(s, 1, pl::PeerMsg::Confirm);   // lost
    ASSERT_TRUE(s.failure.empty()) << s.failure;
    const auto* eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    const uint32_t e1 = eb->keys.epoch;
    ASSERT_EQ(e1, e0 + 1);
    ASSERT_EQ(s.b.crypto.currentEpoch(1), e0) << "receive-first: B still seals under the old epoch";
    ASSERT_EQ(s.a.crypto.currentEpoch(2), e1);

    s.now += pl::PeerHandshake::kRetryNs;   // a second exchange at the gate's rate, inside B's kAwaitNs
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    ASSERT_TRUE(std::holds_alternative<pl::ChallengeSent>(s.a.hs.state()));
    pl::PeerProofV2 proof;
    {
        const Bytes f = popHs(s, 1, pl::PeerMsg::ProofV2);
        ASSERT_TRUE(pl::PeerProofV2::decode(f.data(), f.size(), proof));
    }
    auto o = s.b.hs.onProofV2(proof, s.now);
    EXPECT_FALSE(o.established) << "no new epoch while E is unconfirmed";
    ASSERT_TRUE(o.frame);
    EXPECT_EQ(Bytes(o.frame->data(), o.frame->data() + o.frame->size()), accept1) << "the SAME Accept (epoch E) again";
    eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    EXPECT_EQ(eb->keys.epoch, e1);
    s.absorb(1, std::move(o));
    // The initiator (ChallengeSent, prior = E) answers with E's Confirm and stays in its exchange.
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::Accept));
    EXPECT_TRUE(std::holds_alternative<pl::ChallengeSent>(s.a.hs.state()));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::Confirm));
    ASSERT_TRUE(s.failure.empty()) << s.failure;
    EXPECT_EQ(s.b.crypto.currentEpoch(1), e1) << "B promoted E";
    eb = estOf(s.b.hs);
    ASSERT_TRUE(eb);
    EXPECT_FALSE(eb->unconfirmedAccept);
    EXPECT_EQ(eb->keys.epoch, e1);
    ASSERT_TRUE(s.converge()) << s.describe();
    EXPECT_EQ(s.b.crypto.dropsOf(OR::UnknownEpoch) + s.a.crypto.dropsOf(OR::UnknownEpoch), 0u);
}

// ---- Issue #545: an unconfirmed Accept whose initiator abandoned the exchange stops being re-sent.  Every Accept is ----
// lost; the initiator's ChallengeSent gives up after kAwaitNs and restores its prior epoch.  Over 11 s the responder
// sends at most kAwaitNs / kRetryNs + 1 Accepts, and a later re-handshake still completes with one send epoch.
// Mutation: dropping the `nowNs - e->sinceNs < kAwaitNs` bound from PeerHandshake::tick (fix 3) -> ~55 Accepts -> red.
TEST(PeerLinkAdversarialRegression, AnAbandonedUnconfirmedAcceptStopsBeingResentRegression) {
    Sim s(545);
    s.link();
    ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1));
    const uint32_t e0 = s.a.crypto.currentEpoch(2);
    s.now += 2'000 * kMs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::ProofV2));
    ASSERT_TRUE(estOf(s.b.hs) && estOf(s.b.hs)->unconfirmedAccept);
    int accepts = 0;
    const int64_t end = s.now + 11'000 * kMs;
    for (int i = 0; i < 1'000 && s.now < end; ++i) {
        for (int to : {0, 1}) {
            while (!s.hsTo[to].empty()) {
                Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front();
                if (to == 0 && f[0] == static_cast<uint8_t>(pl::PeerMsg::Accept)) { ++accepts; continue; }   // lost
                s.deliverHs(to, f);
            }
        }
        s.now += 50 * kMs;
        s.tick();
    }
    ASSERT_TRUE(s.failure.empty()) << s.failure;
    ASSERT_TRUE(estOf(s.a.hs)) << "the initiator gave up and restored its prior epoch";
    EXPECT_EQ(s.a.crypto.currentEpoch(2), e0);
    EXPECT_LE(accepts, static_cast<int>(pl::PeerHandshake::kAwaitNs / pl::PeerHandshake::kRetryNs) + 1)
        << "the abandoned Accept must not be re-sent forever";
    EXPECT_GE(accepts, 1);
    // A new exchange replaces the stale unconfirmed epoch and both ends converge on it.
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    ASSERT_TRUE(s.converge()) << s.describe();
    const auto* eb = estOf(s.b.hs);
    ASSERT_TRUE(eb);
    EXPECT_FALSE(eb->unconfirmedAccept);
    EXPECT_GT(s.b.crypto.currentEpoch(1), e0);
    EXPECT_EQ(s.b.crypto.currentEpoch(1), s.a.crypto.currentEpoch(2));
    EXPECT_EQ(eb->keys.epoch, s.b.crypto.currentEpoch(1));
}

// ---- Input bounds: every new wire message truncated / extended at every length, and each tag under another body. ----
TEST(PeerLinkAdversarialRegression, NewMessagesTruncatedAtEveryOffsetNeverDecodeAdversarialRegression) {
    uint8_t buf[pl::kMaxPeerLinkFrame + 8]{};
    auto sweep = [&](auto msg, std::size_t size, auto decode) {
        msg.encode(buf);
        for (std::size_t len = 0; len <= sizeof buf; ++len) {
            decltype(msg) m;
            EXPECT_EQ(decode(buf, len, m), len == size) << "len " << len;
        }
    };
    pl::PeerConfirm c; c.from = 1; c.to = 2; c.linkEpoch = 3; c.confirm.fill(9);
    sweep(c, pl::PeerConfirm::kSize, [](const uint8_t* b, std::size_t n, pl::PeerConfirm& m) { return pl::PeerConfirm::decode(b, n, m); });
    pl::PeerHelloV2 h; h.from = 1; h.to = 2; h.shareA.fill(4);
    sweep(h, pl::PeerHelloV2::kSize, [](const uint8_t* b, std::size_t n, pl::PeerHelloV2& m) { return pl::PeerHelloV2::decode(b, n, m); });
    pl::PeerChallengeV2 ch; ch.from = 2; ch.to = 1; ch.shareB.fill(5);
    sweep(ch, pl::PeerChallengeV2::kSize, [](const uint8_t* b, std::size_t n, pl::PeerChallengeV2& m) { return pl::PeerChallengeV2::decode(b, n, m); });
    pl::PeerProofV2 pv; pv.from = 1; pv.to = 2; pv.nonceB.fill(6); pv.macA.fill(7);   // issue #18
    sweep(pv, pl::PeerProofV2::kSize, [](const uint8_t* b, std::size_t n, pl::PeerProofV2& m) { return pl::PeerProofV2::decode(b, n, m); });
    pl::PeerProof v1p; v1p.encode(buf); buf[0] = static_cast<uint8_t>(pl::PeerMsg::ProofV2);
    pl::PeerProofV2 asProofV2;
    EXPECT_FALSE(pl::PeerProofV2::decode(buf, pl::PeerProof::kSize, asProofV2)) << "a v1 Proof relabelled v2 is too short";
    // A v1 body under a v2 tag (and the reverse) never decodes, whatever its length.
    pl::PeerHello v1; v1.encode(buf); buf[0] = static_cast<uint8_t>(pl::PeerMsg::HelloV2);
    pl::PeerHelloV2 asV2;
    EXPECT_FALSE(pl::PeerHelloV2::decode(buf, pl::PeerHello::kSize, asV2)) << "a v1 Hello relabelled v2 is too short";
    h.encode(buf); buf[0] = static_cast<uint8_t>(pl::PeerMsg::Hello);
    pl::PeerHello asV1;
    EXPECT_FALSE(pl::PeerHello::decode(buf, pl::PeerHelloV2::kSize, asV1)) << "a v2 Hello relabelled v1 is too long";
    for (int tag = 0; tag < 256; ++tag) {
        buf[0] = static_cast<uint8_t>(tag);
        EXPECT_EQ(pl::peekPeerMsg(buf, 1), (tag >= 1 && tag <= 9) ? tag : 0) << tag;
    }
}

// ---- Auth / input bounds: a low-order X25519 share, even under a VALID transcript MAC (a malicious key holder), ----
// never yields a link at either end: the all-zero shared secret would make the link keys a function of the master alone.
// Mutation: dropping the all-zero check in rune::crypto::x25519Shared turns this red.
namespace {
/// The v2 transcript MAC exactly as PeerHandshake.h specifies it (a key holder can compute it for any shares).
std::array<uint8_t, 64> v2Mac(bool isB, const std::array<uint8_t, 32>& nA, const std::array<uint8_t, 32>& nB,
                              const std::array<uint8_t, 32>& sA, const std::array<uint8_t, 32>& sB, uint64_t incA, uint64_t incB) {
    const auto master = keysOf('k').current;
    std::vector<uint8_t> t;
    const std::string label = isB ? "rune-peer-v2|B" : "rune-peer-v2|A";
    t.insert(t.end(), label.begin(), label.end());
    auto put = [&](uint64_t v, int n) { for (int k = 0; k < n; ++k) t.push_back(static_cast<uint8_t>(v >> (8 * k))); };
    auto putN = [&](const std::array<uint8_t, 32>& x) { t.insert(t.end(), x.begin(), x.end()); };
    if (isB) { put(2, 4); put(1, 4); putN(nA); putN(nB); putN(sA); putN(sB); put(incA, 8); put(incB, 8); }
    else     { put(1, 4); put(2, 4); putN(nB); putN(nA); putN(sB); putN(sA); put(incB, 8); put(incA, 8); }
    put(master.keyId, 8);
    rune::crypto::DigestBytes d;
    EXPECT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, {master.master.data(), master.master.size()}, t, d));
    std::array<uint8_t, 64> out{}; std::copy_n(d.bytes.begin(), 64, out.begin());
    return out;
}
} // namespace

TEST(PeerLinkAdversarialRegression, ALowOrderShareUnderAValidMacNeverLinksAdversarialRegression) {
    // RFC 7748 low-order points: 0, 1, and the order-8 point.
    std::vector<std::array<uint8_t, 32>> lowOrder(3);
    lowOrder[1][0] = 1;
    const uint8_t order8[32] = {0xe0, 0xeb, 0x7a, 0x7c, 0x3b, 0x41, 0xb8, 0xae, 0x16, 0x56, 0xe3, 0xfa, 0xf1, 0x9f, 0xc4, 0x6a,
                                0xda, 0x09, 0x8d, 0xeb, 0x9c, 0x32, 0xb1, 0xfd, 0x86, 0x62, 0x05, 0x16, 0x5f, 0x49, 0xb8, 0x00};
    std::copy(std::begin(order8), std::end(order8), lowOrder[2].begin());
    const auto keyId = keysOf('k').current.keyId;
    for (std::size_t i = 0; i < lowOrder.size(); ++i) {
        {   // The responder: a HelloV2 carrying a low-order shareA, then a Proof with a VALID v2 macA over it.
            pl::PeerHandshake b(2, 1, 20, keysOf('k'));
            pl::PeerHelloV2 hello; hello.from = 1; hello.to = 2; hello.keyId = keyId; hello.incarnation = 10;
            hello.nonceA.fill(0x31); hello.shareA = lowOrder[i];
            auto c = b.onHelloV2(hello, kMs); ASSERT_TRUE(c.frame) << i;
            pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
            pl::PeerProofV2 pr; pr.from = 1; pr.to = 2;   // issue #18: the echoes re-derive the responder's cookie
            pr.keyId = keyId; pr.incarnation = 10; pr.nonceA = hello.nonceA; pr.shareA = hello.shareA; pr.nonceB = ch.nonceB;
            pr.macA = v2Mac(false, hello.nonceA, ch.nonceB, hello.shareA, ch.shareB, 10, 20);
            auto o = b.onProofV2(pr, 2 * kMs);
            EXPECT_FALSE(o.established) << "no link keys from a low-order share " << i;
            EXPECT_FALSE(o.frame) << "no Accept";
            ASSERT_TRUE(o.log) << i;
            EXPECT_NE(o.log->find("key share rejected"), std::string::npos) << o.log->str();
            EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state()));
            // Control: the same construction with a real share links - the refusal above is the share check.
            pl::PeerHandshake b2(2, 1, 20, keysOf('k'));
            std::array<uint8_t, 32> priv{}, pub{};
            ASSERT_TRUE(rune::crypto::x25519Keypair(priv, pub));
            hello.shareA = pub;
            auto c2 = b2.onHelloV2(hello, kMs); ASSERT_TRUE(c2.frame);
            ASSERT_TRUE(pl::PeerChallengeV2::decode(c2.frame->data(), c2.frame->size(), ch));
            pr.shareA = hello.shareA; pr.nonceB = ch.nonceB;
            pr.macA = v2Mac(false, hello.nonceA, ch.nonceB, hello.shareA, ch.shareB, 10, 20);
            EXPECT_TRUE(b2.onProofV2(pr, 2 * kMs).established) << "control";
        }
        {   // The initiator: a ChallengeV2 with a low-order shareB under a VALID v2 macB -> no Proof, the state stays.
            pl::PeerHandshake a(1, 2, 10, keysOf('k'));
            auto h = a.tick(0); ASSERT_TRUE(h.frame);
            pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
            pl::PeerChallengeV2 ch; ch.from = 2; ch.to = 1; ch.keyId = keyId; ch.incarnation = 20; ch.nonceB.fill(0x42);
            ch.shareB = lowOrder[i];
            ch.macB = v2Mac(true, hello.nonceA, ch.nonceB, hello.shareA, ch.shareB, 10, 20);
            auto o = a.onChallengeV2(ch, kMs);
            EXPECT_FALSE(o.frame) << "no Proof for a low-order share " << i;
            EXPECT_FALSE(o.established);
            ASSERT_TRUE(o.log) << i;
            EXPECT_NE(o.log->find("key share rejected"), std::string::npos) << o.log->str();
            EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state())) << "the state stays";
            std::array<uint8_t, 32> priv{}, pub{};
            ASSERT_TRUE(rune::crypto::x25519Keypair(priv, pub));
            ch.shareB = pub;
            ch.macB = v2Mac(true, hello.nonceA, ch.nonceB, hello.shareA, ch.shareB, 10, 20);
            EXPECT_TRUE(a.onChallengeV2(ch, kMs).frame) << "control: a real share under the same MAC construction";
        }
    }
}

// ---- Protocol state: replays across epochs and incarnations. ----
// A Confirm recorded from an earlier re-handshake, replayed while a LATER epoch is unconfirmed, promotes nothing; an
// Accept recorded from an earlier epoch, replayed to the Established initiator, draws no Confirm.
TEST(PeerLinkAdversarialRegression, ConfirmsAndAcceptsReplayedAcrossEpochsChangeNothingAdversarialRegression) {
    Sim s(7);
    s.link();
    std::vector<Bytes> confirms, accepts;
    auto rehandshake = [&](bool deliverConfirm) {
        s.now += 2'000 * kMs;
        s.absorb(0, s.a.hs.rekeyForTest(s.now));
        for (int guard = 0; guard < 16 && (!s.hsTo[0].empty() || !s.hsTo[1].empty()); ++guard) {
            for (int to : {0, 1}) {
                if (s.hsTo[to].empty()) continue;
                Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front();
                if (f[0] == static_cast<uint8_t>(pl::PeerMsg::Confirm)) { confirms.push_back(f); if (!deliverConfirm) continue; }
                if (f[0] == static_cast<uint8_t>(pl::PeerMsg::Accept)) accepts.push_back(f);
                s.deliverHs(to, f);
            }
        }
    };
    rehandshake(true);    // E2, confirmed
    ASSERT_EQ(s.b.crypto.currentEpoch(1), 2u);
    rehandshake(false);   // E3, its Confirm lost: B still seals under E2
    ASSERT_EQ(s.b.crypto.currentEpoch(1), 2u);
    ASSERT_TRUE(s.b.crypto.holdsEpoch(1, 3));
    ASSERT_EQ(confirms.size(), 2u);
    pl::PeerConfirm old; ASSERT_TRUE(pl::PeerConfirm::decode(confirms[0].data(), confirms[0].size(), old));
    EXPECT_EQ(old.linkEpoch, 2u);
    EXPECT_FALSE(s.b.hs.onConfirm(old, s.now).established) << "E2's Confirm never promotes E3";
    auto relabelled = old; relabelled.linkEpoch = 3;
    EXPECT_FALSE(s.b.hs.onConfirm(relabelled, s.now).established) << "E2's MAC relabelled E3";
    EXPECT_EQ(s.b.crypto.currentEpoch(1), 2u);
    pl::PeerAccept oldAccept; ASSERT_TRUE(pl::PeerAccept::decode(accepts[0].data(), accepts[0].size(), oldAccept));
    ASSERT_EQ(oldAccept.linkEpoch, 2u);
    EXPECT_FALSE(s.a.hs.onAccept(oldAccept, s.now).frame) << "an old Accept replayed to the Established initiator: no Confirm";
    // The real (lost) Confirm still promotes.
    pl::PeerConfirm real; ASSERT_TRUE(pl::PeerConfirm::decode(confirms[1].data(), confirms[1].size(), real));
    auto o = s.b.hs.onConfirm(real, s.now);
    ASSERT_TRUE(o.established);
    s.absorb(1, std::move(o));
    EXPECT_EQ(s.b.crypto.currentEpoch(1), 3u);
}

// ---- Crash and restart: the responder restarts while its re-handshake is unconfirmed; the initiator restarts while ----
// the responder awaits its Confirm.  Both relink, the restarted side installs send-and-receive, and no frame sealed
// after the relink is dropped.
TEST(PeerLinkAdversarialRegression, ARestartWhileARehandshakeIsUnconfirmedRelinksAdversarialRegression) {
    for (int restarted : {0, 1}) {
        Sim s(11 + static_cast<uint64_t>(restarted));
        s.link();
        s.now += 2'000 * kMs;
        s.absorb(0, s.a.hs.rekeyForTest(s.now));
        // Deliver Hello, Challenge, Proof, Accept; lose the Confirm.
        for (int guard = 0; guard < 16 && (!s.hsTo[0].empty() || !s.hsTo[1].empty()); ++guard)
            for (int to : {0, 1}) {
                if (s.hsTo[to].empty()) continue;
                Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front();
                if (f[0] != static_cast<uint8_t>(pl::PeerMsg::Confirm)) s.deliverHs(to, f);
            }
        const auto* eb = std::get_if<pl::Established>(&s.b.hs.state());
        ASSERT_TRUE(eb && eb->unconfirmedAccept);
        // Restart one side: a new handshake (new incarnation) and a crypto instance that holds nothing.
        if (restarted == 1) { s.b.hs = pl::PeerHandshake(2, 1, 21, keysOf('k')); s.b.crypto = pl::PeerFrameCrypto(2, 0); }
        else                { s.a.hs = pl::PeerHandshake(1, 2, 11, keysOf('k')); s.a.crypto = pl::PeerFrameCrypto(1, 0); }
        s.data.clear();
        // The survivor goes silent for the restarted side (it was down), then the relink runs.  A restarted RESPONDER is
        // relinked by its NoLink poke once the initiator has heard nothing from it for kSilenceNs (issue #17: no hint
        // cap any more); the loop allows 45 s of simulated time and exits as soon as the link converged.
        for (int i = 0; i < 150 && !(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1) &&
                                    s.a.crypto.currentEpoch(2) == s.b.crypto.currentEpoch(1) &&
                                    std::holds_alternative<pl::Established>(s.a.hs.state())); ++i) {
            s.now += 300 * kMs;
            s.tick();
            for (int g = 0; g < 16 && (!s.hsTo[0].empty() || !s.hsTo[1].empty()); ++g)
                for (int to : {0, 1}) if (!s.hsTo[to].empty()) { Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front(); s.deliverHs(to, f); }
        }
        ASSERT_TRUE(s.failure.empty()) << s.failure;
        ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1)) << "restarted " << restarted;
        ASSERT_TRUE(s.converge()) << "restarted " << restarted << ": " << s.describe();
        for (int i = 0; i < 20; ++i) { s.seal(0); s.seal(1); }
        while (!s.data.empty() && s.failure.empty()) s.open(s.data.size() - 1);   // reversed order
        EXPECT_TRUE(s.failure.empty()) << "restarted " << restarted << ": " << s.failure;
    }
}

// ---- Version skew: a legacy (v1) initiator re-handshakes with an upgraded responder.  It sends no Confirm and only ----
// knows v1; the responder installs receive-only and promotes on the first authenticated frame under the new epoch.
// Frames in both directions open throughout.
TEST(PeerLinkAdversarialRegression, ALegacyInitiatorRehandshakeIsPromotedByItsFirstFrameAdversarialRegression) {
    const auto master = keysOf('k').current;
    pl::PeerHandshake b(2, 1, 20, keysOf('k'));
    pl::PeerFrameCrypto bc(2, 0), legacy(1, 0);
    uint8_t nonceFill = 0x10;
    auto v1Mac = [&](bool isB, const std::array<uint8_t, 32>& nA, const std::array<uint8_t, 32>& nB) {
        std::vector<uint8_t> t;
        const std::string label = isB ? "rune-peer-v1|B" : "rune-peer-v1|A";
        t.insert(t.end(), label.begin(), label.end());
        auto put = [&](uint64_t v, int n) { for (int k = 0; k < n; ++k) t.push_back(static_cast<uint8_t>(v >> (8 * k))); };
        if (isB) { put(2, 4); put(1, 4); t.insert(t.end(), nA.begin(), nA.end()); t.insert(t.end(), nB.begin(), nB.end()); put(10, 8); put(20, 8); }
        else     { put(1, 4); put(2, 4); t.insert(t.end(), nB.begin(), nB.end()); t.insert(t.end(), nA.begin(), nA.end()); put(20, 8); put(10, 8); }
        put(master.keyId, 8);
        rune::crypto::DigestBytes d;
        EXPECT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, {master.master.data(), master.master.size()}, t, d));
        std::array<uint8_t, 64> out{}; std::copy_n(d.bytes.begin(), 64, out.begin());
        return out;
    };
    // The legacy initiator's whole v1 exchange; it installs the derived keys as a pre-#242 broker would (immediately).
    auto legacyHandshake = [&](int64_t now) -> pl::PeerHandshake::Out {
        pl::PeerHello h; h.from = 1; h.to = 2; h.keyId = master.keyId; h.incarnation = 10; h.nonceA.fill(nonceFill++);
        auto c = b.onHello(h, now);
        EXPECT_TRUE(c.frame);
        pl::PeerChallenge ch; EXPECT_TRUE(pl::PeerChallenge::decode(c.frame->data(), c.frame->size(), ch));
        EXPECT_EQ(ch.macB, v1Mac(true, h.nonceA, ch.nonceB));
        pl::PeerProof pr; pr.from = 1; pr.to = 2; pr.macA = v1Mac(false, h.nonceA, ch.nonceB);
        auto acc = b.onProof(pr, now);
        EXPECT_TRUE(acc.established);
        pl::PeerAccept a; EXPECT_TRUE(pl::PeerAccept::decode(acc.frame->data(), acc.frame->size(), a));
        auto lk = pl::deriveLinkKeys(master, 1, 2, true, h.nonceA, ch.nonceB, a.linkEpoch, std::nullopt);
        EXPECT_TRUE(lk);
        EXPECT_TRUE(legacy.installLink(2, *lk, now));
        return acc;
    };
    auto sealOpen = [](pl::PeerFrameCrypto& from, int32_t to, pl::PeerFrameCrypto& at, uint32_t& epoch) {
        uint8_t plain[4] = {1, 2, 3, 4}, wire[64], out[8];
        const auto n = from.seal(to, plain, sizeof plain, wire, sizeof wire);
        EXPECT_GT(n, 0u);
        std::size_t outLen = 0; int32_t f = -1;
        return at.open(wire, n, out, sizeof out, outLen, f, epoch);
    };
    auto first = legacyHandshake(0);
    ASSERT_EQ(first.established->install, pl::LinkInstall::SendAndReceive) << "a first link";
    ASSERT_TRUE(bc.installLink(1, *first.established, 0));
    // An E1 frame from B still in flight when the legacy initiator re-handshakes.
    uint8_t plain[4] = {9, 9, 9, 9}, lateWire[64], out[8];
    const auto lateLen = bc.seal(1, plain, sizeof plain, lateWire, sizeof lateWire);
    ASSERT_GT(lateLen, 0u);

    auto second = legacyHandshake(5'000 * kMs);   // same incarnation: receive-first
    ASSERT_EQ(second.established->install, pl::LinkInstall::ReceiveOnly);
    ASSERT_TRUE(bc.installLink(1, *second.established, 5'000 * kMs));
    uint32_t ep = 0;
    // B keeps sealing E1 (which the legacy initiator still holds) until the legacy side's first E2 frame.
    EXPECT_EQ(sealOpen(bc, 1, legacy, ep), OR::Ok); EXPECT_EQ(ep, 1u);
    // The re-sent Accept means nothing to a legacy initiator; B never promotes on a timer.
    for (int i = 1; i <= 5; ++i) EXPECT_FALSE(b.tick(5'000 * kMs + i * pl::PeerHandshake::kRetryNs).established);
    EXPECT_EQ(bc.currentEpoch(1), 1u);
    EXPECT_EQ(sealOpen(legacy, 2, bc, ep), OR::Ok); EXPECT_EQ(ep, 2u);
    int32_t peer = -1; uint32_t used = 0;
    ASSERT_TRUE(bc.takePeerUsedEpoch(peer, used));
    auto promoted = b.onPeerUsedEpoch(used, 6'000 * kMs);
    ASSERT_TRUE(promoted.established);
    ASSERT_TRUE(bc.installLink(1, *promoted.established, 6'000 * kMs));
    EXPECT_EQ(sealOpen(bc, 1, legacy, ep), OR::Ok); EXPECT_EQ(ep, 2u) << "promoted: B seals under E2";
    std::size_t outLen = 0; int32_t from = -1;
    EXPECT_EQ(legacy.open(lateWire, lateLen, out, sizeof out, outLen, from, ep), OR::Ok) << "the late E1 frame still opens";
    EXPECT_EQ(bc.dropsOf(OR::UnknownEpoch) + legacy.dropsOf(OR::UnknownEpoch), 0u);
}

// =====================================================================================================================
// Review of PR #551 (issues #18 / #17): the stateless v2 cookie (ProofV2, tag 9) and the removal of the 30 s hint cap.
// Pure: an injected clock, seeded randomness, no sleeps.
// =====================================================================================================================
namespace {

/// One real v2 exchange up to the initiator's ProofV2: `a` Hellos at `t`, `b` answers at `t`, `a` proves at `t`.
pl::PeerProofV2 realProofV2(pl::PeerHandshake& a, pl::PeerHandshake& b, int64_t t, pl::PeerHelloV2* helloOut = nullptr,
                            pl::PeerChallengeV2* chOut = nullptr) {
    auto h = a.tick(t);
    EXPECT_TRUE(h.frame);
    pl::PeerHelloV2 hello{};
    if (h.frame) EXPECT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHelloV2(hello, t);
    EXPECT_TRUE(c.frame);
    pl::PeerChallengeV2 ch{};
    if (c.frame) EXPECT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
    auto p = a.onChallengeV2(ch, t);
    EXPECT_TRUE(p.frame);
    pl::PeerProofV2 proof{};
    if (p.frame) EXPECT_TRUE(pl::PeerProofV2::decode(p.frame->data(), p.frame->size(), proof));
    if (helloOut) *helloOut = hello;
    if (chOut) *chOut = ch;
    return proof;
}

/// The v1 transcript MAC for initiator 1 (incarnation 10) and responder 2 (incarnation 20), as a legacy broker computes it.
std::array<uint8_t, 64> v1MacOf(bool isB, const std::array<uint8_t, 32>& nA, const std::array<uint8_t, 32>& nB) {
    const auto master = keysOf('k').current;
    std::vector<uint8_t> t;
    const std::string label = isB ? "rune-peer-v1|B" : "rune-peer-v1|A";
    t.insert(t.end(), label.begin(), label.end());
    auto put = [&](uint64_t v, int n) { for (int k = 0; k < n; ++k) t.push_back(static_cast<uint8_t>(v >> (8 * k))); };
    if (isB) { put(2, 4); put(1, 4); t.insert(t.end(), nA.begin(), nA.end()); t.insert(t.end(), nB.begin(), nB.end()); put(10, 8); put(20, 8); }
    else     { put(1, 4); put(2, 4); t.insert(t.end(), nB.begin(), nB.end()); t.insert(t.end(), nA.begin(), nA.end()); put(20, 8); put(10, 8); }
    put(master.keyId, 8);
    rune::crypto::DigestBytes d;
    EXPECT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, {master.master.data(), master.master.size()}, t, d));
    std::array<uint8_t, 64> out{}; std::copy_n(d.bytes.begin(), 64, out.begin());
    return out;
}

const pl::Established* estOf(const pl::PeerHandshake& h) { return std::get_if<pl::Established>(&h.state()); }

/// One cookie-window trial (seeded): the responder's grid is set by duty-cycle ticks at random times, the Challenge is
/// issued at a random point, more ticks land at random times before the Proof, which arrives `proofDelayNs` after the
/// Challenge.  The same seed replays the same call times exactly.
pl::PeerHandshake::Out cookieTrial(uint64_t seed, int64_t proofDelayNs) {
    std::mt19937_64 rng(seed);
    auto upTo = [&](int64_t n) { return std::uniform_int_distribution<int64_t>(0, n)(rng); };
    pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
    int64_t t = upTo(3 * pl::PeerHandshake::kAwaitNs);
    (void)b.tick(t);                                                   // the grid's origin: some duty cycle
    const int pre = static_cast<int>(upTo(6));
    for (int i = 0; i < pre; ++i) { t += upTo(pl::PeerHandshake::kAwaitNs * 3 / 2); (void)b.tick(t); }
    t += upTo(pl::PeerHandshake::kAwaitNs);                            // the Challenge, anywhere on the grid
    const int64_t issued = t;
    const auto proof = realProofV2(a, b, issued);
    const int mid = static_cast<int>(upTo(4));
    std::vector<int64_t> ticks;
    for (int i = 0; i < mid; ++i) ticks.push_back(issued + upTo(proofDelayNs));
    std::sort(ticks.begin(), ticks.end());
    for (int64_t x : ticks) (void)b.tick(x);
    return b.onProofV2(proof, issued + proofDelayNs);
}

} // namespace

// ---- Protocol state / cookie rotation boundaries (#18): a Challenge stays answerable for at least kAwaitNs and at ----
// most 2 x kAwaitNs, wherever on the rotation grid it was issued and whenever the responder's duty cycles run.
// Mutation: checking only cookieCur_ in onProofV2 (dropping cookiePrev_) turns the first half red.
TEST(PeerLinkAdversarialRegression, ACookieChallengeIsAnswerableForAWholeAwaitWhereverItIsIssuedAdversarialRegression) {
    int accepted = 0, refused = 0;
    for (uint64_t seed = 1; seed <= 300; ++seed) {
        auto ok = cookieTrial(seed, pl::PeerHandshake::kAwaitNs);
        EXPECT_TRUE(ok.established) << "seed " << seed << ": a Proof exactly kAwaitNs after its Challenge must link: "
                                    << (ok.log ? ok.log->str() : "");
        accepted += ok.established ? 1 : 0;
        auto late = cookieTrial(seed, 2 * pl::PeerHandshake::kAwaitNs);
        EXPECT_FALSE(late.established) << "seed " << seed << ": 2 x kAwaitNs after its Challenge, both secrets rotated out";
        EXPECT_FALSE(late.frame) << "seed " << seed;
        ASSERT_TRUE(late.log) << "seed " << seed;
        EXPECT_NE(late.log->find("stale or forged cookie"), std::string::npos) << "seed " << seed << ": " << late.log->str();
        refused += late.established ? 0 : 1;
    }
    EXPECT_EQ(accepted, 300);
    EXPECT_EQ(refused, 300);
}

// ---- Protocol state: reflection / misaddressing / unknown key (#18).  A ProofV2 reflected to the initiator, one naming ----
// other nodes, or one under a key id nobody holds computes NO MAC and changes no state; the initiator's own Hello and
// the responder's own Challenge reflected back are refused or ignored; the real Proof still links afterwards.
TEST(PeerLinkAdversarialRegression, AReflectedMisaddressedOrUnknownKeyProofV2CostsNothingAdversarialRegression) {
    pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
    pl::PeerHelloV2 hello; pl::PeerChallengeV2 ch;
    const auto proof = realProofV2(a, b, 0, &hello, &ch);
    ASSERT_TRUE(std::holds_alternative<pl::ChallengeSent>(a.state()));
    const uint64_t macsA = a.macComputations(), macsB = b.macComputations();

    auto r = a.onProofV2(proof, kMs);                                   // reflected to its own sender
    EXPECT_FALSE(r.frame); EXPECT_FALSE(r.established); EXPECT_FALSE(r.log);
    auto rh = a.onHelloV2(hello, kMs);                                  // its own Hello reflected: not a peer
    ASSERT_TRUE(rh.frame);
    pl::PeerRefuse refuse; ASSERT_TRUE(pl::PeerRefuse::decode(rh.frame->data(), rh.frame->size(), refuse));
    EXPECT_EQ(refuse.reason, pl::RefuseReason::NotAPeer);
    EXPECT_FALSE(b.onChallengeV2(ch, kMs).frame) << "the responder's own Challenge reflected: nothing";
    EXPECT_TRUE(std::holds_alternative<pl::ChallengeSent>(a.state())) << "the initiator's exchange is untouched";

    for (auto [from, to] : {std::pair{3, 2}, std::pair{1, 3}, std::pair{2, 1}, std::pair{-1, 2}}) {
        auto m = proof; m.from = from; m.to = to;
        auto o = b.onProofV2(m, kMs);
        EXPECT_FALSE(o.frame) << from << "->" << to; EXPECT_FALSE(o.established) << from << "->" << to;
    }
    auto unknown = proof; unknown.keyId ^= 0x5a5a;
    auto ou = b.onProofV2(unknown, kMs);
    EXPECT_FALSE(ou.established); EXPECT_FALSE(ou.frame);
    ASSERT_TRUE(ou.log); EXPECT_NE(ou.log->find("unknown key id"), std::string::npos) << ou.log->str();
    EXPECT_EQ(a.macComputations(), macsA) << "nothing reflected at the initiator computed a MAC";
    EXPECT_EQ(b.macComputations(), macsB) << "no misaddressed / unknown-key ProofV2 computed a MAC";
    EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state()));

    auto ok = b.onProofV2(proof, 2 * kMs);
    EXPECT_TRUE(ok.established) << "control: the real Proof still links";
}

// ---- Protocol state / replay across versions (#18): a captured Proof of EITHER version is void once the responder ----
// established through the OTHER version.  Mutation: dropping freshCookie_() from establishAsResponder_ turns the first
// half red (the v2 Proof would mint a second epoch after a v1 establishment).
TEST(PeerLinkAdversarialRegression, ACapturedProofIsVoidAfterAnEstablishmentThroughTheOtherVersionAdversarialRegression) {
    const auto master = keysOf('k').current;
    {   // A ProofV2 captured in flight; a legacy v1 exchange links first; the ProofV2 delivered later mints nothing.
        pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
        const auto captured = realProofV2(a, b, 0);
        pl::PeerHello h1; h1.from = 1; h1.to = 2; h1.keyId = master.keyId; h1.incarnation = 10; h1.nonceA.fill(0x41);
        auto c1 = b.onHello(h1, kMs); ASSERT_TRUE(c1.frame);
        pl::PeerChallenge ch1; ASSERT_TRUE(pl::PeerChallenge::decode(c1.frame->data(), c1.frame->size(), ch1));
        pl::PeerProof p1; p1.from = 1; p1.to = 2; p1.macA = v1MacOf(false, h1.nonceA, ch1.nonceB);
        ASSERT_TRUE(b.onProof(p1, 2 * kMs).established);
        ASSERT_NE(estOf(b), nullptr); ASSERT_EQ(estOf(b)->keys.epoch, 1u);
        auto o = b.onProofV2(captured, 3 * kMs);
        EXPECT_FALSE(o.established) << "a v2 Proof issued before a v1 establishment is void";
        ASSERT_TRUE(o.log); EXPECT_NE(o.log->find("stale or forged cookie"), std::string::npos) << o.log->str();
        ASSERT_NE(estOf(b), nullptr); EXPECT_EQ(estOf(b)->keys.epoch, 1u);
    }
    {   // A legacy v1 Proof captured; a v2 exchange links first; the v1 Proof delivered later mints nothing.
        pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
        pl::PeerHello h1; h1.from = 1; h1.to = 2; h1.keyId = master.keyId; h1.incarnation = 10; h1.nonceA.fill(0x42);
        auto c1 = b.onHello(h1, 0); ASSERT_TRUE(c1.frame);
        pl::PeerChallenge ch1; ASSERT_TRUE(pl::PeerChallenge::decode(c1.frame->data(), c1.frame->size(), ch1));
        pl::PeerProof p1; p1.from = 1; p1.to = 2; p1.macA = v1MacOf(false, h1.nonceA, ch1.nonceB);
        const auto proof = realProofV2(a, b, kMs);
        ASSERT_TRUE(b.onProofV2(proof, 2 * kMs).established);
        ASSERT_NE(estOf(b), nullptr); ASSERT_EQ(estOf(b)->keys.epoch, 1u);
        auto o = b.onProof(p1, 3 * kMs);
        EXPECT_FALSE(o.established) << "a v1 Proof issued before a v2 establishment is void";
        ASSERT_NE(estOf(b), nullptr); EXPECT_EQ(estOf(b)->keys.epoch, 1u);
    }
}

// ---- Version skew (#18): a main build between #242 and #18 answers a ChallengeV2 with a PLAIN Proof (tag 3) carrying a ----
// valid v2 macA.  The new responder never links with it, computes no MAC for it and keeps no state (the documented
// non-interoperability is a clean refusal, not a half-open link).  A legacy v1 Hello flood beside a v2 exchange does
// not disturb the v2 exchange (separate paths).
TEST(PeerLinkAdversarialRegression, APreCookieV2InitiatorsPlainProofIsRefusedCleanlyAdversarialRegression) {
    pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
    pl::PeerHelloV2 hello; pl::PeerChallengeV2 ch;
    const auto proofV2 = realProofV2(a, b, 0, &hello, &ch);
    pl::PeerProof plain; plain.from = 1; plain.to = 2; plain.macA = proofV2.macA;   // the pre-#18 initiator's frame
    ASSERT_EQ(plain.macA, v2Mac(false, hello.nonceA, ch.nonceB, hello.shareA, ch.shareB, 10, 20)) << "a valid v2 macA";
    const uint64_t macs = b.macComputations();
    auto o = b.onProof(plain, kMs);
    EXPECT_FALSE(o.established);
    EXPECT_FALSE(o.frame);
    ASSERT_TRUE(o.log); EXPECT_NE(o.log->find("no pending challenge matches"), std::string::npos) << o.log->str();
    EXPECT_EQ(b.macComputations(), macs);
    EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state()));
    // 100 legacy v1 Hellos (they churn the v1 table) land before the real ProofV2: it still links.
    const auto master = keysOf('k').current;
    for (int i = 0; i < 100; ++i) {
        pl::PeerHello h; h.from = 1; h.to = 2; h.keyId = master.keyId; h.incarnation = 10;
        h.nonceA.fill(static_cast<uint8_t>(i)); h.nonceA[31] = 0xEE;
        ASSERT_TRUE(b.onHello(h, 2 * kMs).frame) << i;
    }
    EXPECT_TRUE(b.onProofV2(proofV2, 3 * kMs).established);
}

// ---- Resources / DoS cost (#18): a forged HelloV2 flood keeps NO per-Hello state and costs exactly one transcript MAC ----
// per Hello (the Challenge's own macB - the old pending slot also precomputed the expected macA).  A misaddressed one
// costs none.  An Established responder stays Established with its keys.
TEST(PeerLinkAdversarialRegression, AForgedHelloV2FloodCostsOneMacEachAndKeepsNoStateAdversarialRegression) {
    pl::PeerHandshake a(1, 2, 10, keysOf('k')), b(2, 1, 20, keysOf('k'));
    ASSERT_TRUE(b.onProofV2(realProofV2(a, b, 0), kMs).established);
    ASSERT_NE(estOf(b), nullptr);
    const pl::LinkKeys live = estOf(b)->keys;
    const uint64_t macs = b.macComputations();
    std::mt19937_64 rng(551);
    constexpr int kN = 5'000;
    int misaddressed = 0;
    for (int i = 0; i < kN; ++i) {
        pl::PeerHelloV2 f; f.from = 1; f.to = 2; f.keyId = keysOf('k').current.keyId; f.incarnation = rng();
        for (auto& x : f.nonceA) x = static_cast<uint8_t>(rng());
        for (auto& x : f.shareA) x = static_cast<uint8_t>(rng());
        if (i % 10 == 0) { f.from = 7; ++misaddressed; }
        auto o = b.onHelloV2(f, 2 * kMs + i);
        ASSERT_TRUE(o.frame) << i;
        EXPECT_EQ(o.frame->data()[0], static_cast<uint8_t>(i % 10 == 0 ? pl::PeerMsg::Refuse : pl::PeerMsg::ChallengeV2)) << i;
    }
    EXPECT_EQ(b.macComputations() - macs, static_cast<uint64_t>(kN - misaddressed)) << "one macB per answered Hello, nothing else";
    ASSERT_NE(estOf(b), nullptr);
    EXPECT_EQ(estOf(b)->keys, live) << "the live link is untouched";
}

// ---- Crash and restart (#18): the responder restarts between its Challenge and the initiator's ProofV2.  The new ----
// process's cookie secrets are fresh, so the Proof is refused (no link under a dead process's Challenge), and the pair
// relinks on its own once the initiator's wait expires.
TEST(PeerLinkAdversarialRegression, AResponderRestartedBetweenChallengeAndProofRelinksAdversarialRegression) {
    Sim s(551);
    const auto proof = realProofV2(s.a.hs, s.b.hs, 0);
    s.b.hs = pl::PeerHandshake(2, 1, 21, keysOf('k'));
    s.b.crypto = pl::PeerFrameCrypto(2, 0);
    auto o = s.b.hs.onProofV2(proof, kMs);
    EXPECT_FALSE(o.established);
    ASSERT_TRUE(o.log); EXPECT_NE(o.log->find("stale or forged cookie"), std::string::npos) << o.log->str();
    s.now = 2 * kMs;
    ASSERT_TRUE(s.converge()) << s.describe();
    ASSERT_NE(estOf(s.a.hs), nullptr);
    EXPECT_EQ(estOf(s.a.hs)->peerIncarnation, 21u) << "linked with the restarted responder";
}

// ---- Network (rUDP) + Auth (#17): a seeded model.  Both ends seal an idle keepalive every 250 ms; each keepalive is ----
// lost, duplicated or delayed (reordered) at random.  A forged unknown-epoch hint and a forged NoLink poke hit the
// initiator every 100 ms.  Invariants: (1) the initiator starts a re-handshake ONLY when it opened no authenticated frame
// for kSilenceNs (no hint cap any more: the silence gate is the whole defence); (2) a duplicated keepalive never opens
// twice (it would fake liveness); (3) every delivered keepalive opens or is a counted replay - never UnknownEpoch /
// BadTag, across the re-handshakes loss forces; (4) loss-free runs never re-handshake at all.
// Mutation: dropping the `nowNs - e->lastAuthNs < kSilenceNs` gate from PeerHandshake::rekey_ turns (1)/(4) red.
TEST(PeerLinkAdversarialRegression, ForgedHintsNeverRekeyALinkWhoseKeepalivesArriveUnderLossDupReorderAdversarialRegression) {
    uint64_t rekeysLossy = 0, replays = 0, opened = 0;
    for (uint64_t seed = 1; seed <= 60; ++seed) {
        const bool lossy = seed % 3 != 0;
        const double pLoss = lossy ? 0.35 : 0.0, pDup = 0.2;
        Sim s(seed);
        s.link();
        ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1)) << "seed " << seed;
        struct InFlight { int to; Bytes bytes; int64_t at; uint64_t id; };
        std::vector<InFlight> net;
        std::vector<int> openedTimes;
        uint64_t nextId = 0;
        ASSERT_NE(estOf(s.a.hs), nullptr);
        int64_t lastAuthAtA = estOf(s.a.hs)->lastAuthNs;   // the establishment itself counts as heard
        int rekeys = 0;
        auto drainHs = [&] {
            for (int guard = 0; guard < 64 && (!s.hsTo[0].empty() || !s.hsTo[1].empty()); ++guard)
                for (int to : {0, 1}) if (!s.hsTo[to].empty()) { Bytes f = s.hsTo[to].front(); s.hsTo[to].pop_front(); s.deliverHs(to, f); }
        };
        auto inject = [&](pl::PeerHandshake::Out o) {
            if (o.frame) {
                ++rekeys;
                EXPECT_GE(s.now - lastAuthAtA, pl::PeerHandshake::kSilenceNs)
                    << "seed " << seed << ": a forged hint started a re-handshake of a peer heard " << (s.now - lastAuthAtA) / kMs << " ms ago";
            }
            s.absorb(0, std::move(o));
        };
        const int64_t start = s.now, end = start + 30'000 * kMs;
        pl::PeerRefuse poke; poke.from = 2; poke.to = 1; poke.reason = pl::RefuseReason::NoLink;
        for (; s.now <= end && s.failure.empty(); s.now += 10 * kMs) {
            const int64_t t = s.now - start;
            for (int from : {0, 1}) {
                if (t % (250 * kMs) != (from == 0 ? 0 : 120 * kMs)) continue;
                End& e = s.at(from);
                if (!e.crypto.hasLink(e.peer)) continue;
                Bytes w(8 + pl::kPeerFrameOverhead);
                const uint8_t ka[8] = {0x00, 0x00, 65, 0x00, 0x00, 0x00, 0x00, 0x00};   // a template-65 header-only frame
                const auto n = e.crypto.seal(e.peer, ka, sizeof ka, w.data(), w.size());
                ASSERT_GT(n, 0u);
                w.resize(n);
                const uint64_t id = nextId++;
                openedTimes.push_back(0);
                if (s.chance(pLoss)) continue;
                net.push_back({1 - from, w, s.now + std::uniform_int_distribution<int64_t>(0, 120)(s.rng) * kMs, id});
                if (s.chance(pDup)) net.push_back({1 - from, w, s.now + std::uniform_int_distribution<int64_t>(0, 120)(s.rng) * kMs, id});
            }
            // Deliver whatever is due, in a random order (reordering).
            std::shuffle(net.begin(), net.end(), s.rng);
            for (std::size_t i = 0; i < net.size();) {
                if (net[i].at > s.now) { ++i; continue; }
                InFlight f = std::move(net[i]);
                net.erase(net.begin() + static_cast<std::ptrdiff_t>(i));
                End& e = s.at(f.to);
                uint8_t out[16]; std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
                const auto r = e.crypto.open(f.bytes.data(), f.bytes.size(), out, sizeof out, outLen, from, epoch);
                if (r == OR::Replay) { ++replays; EXPECT_GE(openedTimes[f.id], 1) << "seed " << seed << ": a replay of a frame never opened"; continue; }
                ASSERT_EQ(r, OR::Ok) << "seed " << seed << ": a delivered keepalive was dropped: " << pl::PeerFrameCrypto::name(r);
                ++opened;
                EXPECT_EQ(++openedTimes[f.id], 1) << "seed " << seed << ": a keepalive opened twice";
                e.hs.onAuthenticated(s.now);
                if (f.to == 0) lastAuthAtA = s.now;
                int32_t peer = -1; uint32_t used = 0;
                if (e.crypto.takePeerUsedEpoch(peer, used)) s.absorb(f.to, e.hs.onPeerUsedEpoch(used, s.now));
            }
            if (t % (100 * kMs) == 0) {
                inject(s.a.hs.onUnknownEpoch(static_cast<uint32_t>(1'000 + t / kMs), s.now));
                inject(s.a.hs.onRefuse(poke, s.now));
            }
            s.tick();
            drainHs();
        }
        ASSERT_TRUE(s.failure.empty()) << "seed " << seed << ": " << s.failure;
        if (!lossy) EXPECT_EQ(rekeys, 0) << "seed " << seed << ": loss-free keepalives: never silent, never re-keyed";
        else rekeysLossy += static_cast<uint64_t>(rekeys);
    }
    EXPECT_GT(replays, 1'000u) << "the model must actually duplicate";
    EXPECT_GT(opened, 8'000u);
    std::printf("[keepalive model] opened %llu, replays refused %llu, silence-gated re-handshakes under 35%% loss %llu\n",
                static_cast<unsigned long long>(opened), static_cast<unsigned long long>(replays),
                static_cast<unsigned long long>(rekeysLossy));
}
