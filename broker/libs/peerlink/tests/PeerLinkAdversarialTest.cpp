// PeerLinkAdversarialTest.cpp — review of PR #538 (issues #241 / #242): attacks on the peer link handshake and the
// frame crypto through their public APIs and wire frames.  Pure: an injected clock, seeded randomness, no sleeps.
#include <rune/crypto/Dh.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/peerlink/PeerHandshake.h>

#include <gtest/gtest.h>

#include <algorithm>
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
            const auto* eb = std::get_if<pl::Established>(&b.hs.state());
            if (data.empty() && hsTo[0].empty() && hsTo[1].empty() && eb && !eb->unconfirmedAccept &&
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
TEST(PeerLinkAdversarialRegression, NoDeliveredFrameIsDroppedAcrossLossyReorderedRehandshakesAdversarialRegression) {
    uint64_t rekeys = 0, opened = 0;
    for (uint64_t seed = 1; seed <= 200; ++seed) {
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
        // relinked by its NoLink poke, which the initiator honours at most once per kHintRekeyGapNs (30 s) after the
        // re-handshake it just completed (the pre-existing forged-hint cap): the loop runs 45 s of simulated time.
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
