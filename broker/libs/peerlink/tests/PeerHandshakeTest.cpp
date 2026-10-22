// PeerHandshakeTest.cpp — authentication plan Task 15: the mutual HMAC challenge-response (pure, fake clock).
#include <rune/crypto/Dh.h>
#include <rune/peerlink/PeerHandshake.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <random>
#include <string>

namespace pl = rune::peerlink;

namespace {

pl::ClusterKeys keys(char c) {
    std::string k(48, c);
    auto m = rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
    return {*m, std::nullopt};
}

constexpr int64_t kMs = 1'000'000;

/// Two PeerHandshakes wired back to back: shuttles frames until quiet, dispatching by PeerMsg tag.
struct Pair {
    pl::PeerHandshake a, b;                       // a = node 1 (initiator), b = node 2 (responder)
    std::optional<pl::LinkKeys> ka, kb;
    std::vector<pl::LinkKeys> estA, estB;                      // every established Out, in order (issue #241)
    std::vector<std::pair<int, uint8_t>> dropOnce;             // (destination side, tag): the next such frame is lost
    std::vector<std::string> logsA, logsB;
    std::vector<std::pair<int, std::vector<uint8_t>>> trace;   // (destination side, frame): 0 -> a, 1 -> b
    std::deque<std::pair<int, std::vector<uint8_t>>> q;

    Pair(pl::ClusterKeys x, pl::ClusterKeys y, uint64_t incA = 10, uint64_t incB = 20)
        : a(1, 2, incA, x), b(2, 1, incB, y) {}

    void absorb(int producedBy, pl::PeerHandshake::Out o) {
        auto& logs = producedBy == 0 ? logsA : logsB;
        if (o.log) logs.push_back(o.log->str());
        if (o.established) {
            (producedBy == 0 ? ka : kb) = o.established;
            (producedBy == 0 ? estA : estB).push_back(*o.established);
        }
        if (o.frame) {
            std::vector<uint8_t> v(o.frame->data(), o.frame->data() + o.frame->size());
            q.emplace_back(1 - producedBy, v); trace.emplace_back(1 - producedBy, v);
        }
    }

    void deliver(int to, const std::vector<uint8_t>& f, int64_t now) {
        pl::PeerHandshake& h = to == 0 ? a : b;
        switch (pl::peekPeerMsg(f.data(), f.size())) {
            case 1: { pl::PeerHello m;     ASSERT_TRUE(pl::PeerHello::decode(f.data(), f.size(), m));     absorb(to, h.onHello(m, now)); break; }
            case 2: { pl::PeerChallenge m; ASSERT_TRUE(pl::PeerChallenge::decode(f.data(), f.size(), m)); absorb(to, h.onChallenge(m, now)); break; }
            case 3: { pl::PeerProof m;     ASSERT_TRUE(pl::PeerProof::decode(f.data(), f.size(), m));     absorb(to, h.onProof(m, now)); break; }
            case 4: { pl::PeerAccept m;    ASSERT_TRUE(pl::PeerAccept::decode(f.data(), f.size(), m));    absorb(to, h.onAccept(m, now)); break; }
            case 5: { pl::PeerRefuse m;    ASSERT_TRUE(pl::PeerRefuse::decode(f.data(), f.size(), m));    absorb(to, h.onRefuse(m, now)); break; }
            case 6: { pl::PeerConfirm m;   ASSERT_TRUE(pl::PeerConfirm::decode(f.data(), f.size(), m));   absorb(to, h.onConfirm(m, now)); break; }
            case 7: { pl::PeerHelloV2 m;   ASSERT_TRUE(pl::PeerHelloV2::decode(f.data(), f.size(), m));   absorb(to, h.onHelloV2(m, now)); break; }
            case 8: { pl::PeerChallengeV2 m; ASSERT_TRUE(pl::PeerChallengeV2::decode(f.data(), f.size(), m)); absorb(to, h.onChallengeV2(m, now)); break; }
            case 9: { pl::PeerProofV2 m;   ASSERT_TRUE(pl::PeerProofV2::decode(f.data(), f.size(), m));   absorb(to, h.onProofV2(m, now)); break; }
            default: FAIL() << "unknown tag";
        }
    }

    void run(int64_t now = 0) {
        absorb(0, a.tick(now));
        absorb(1, b.tick(now));
        for (int guard = 0; !q.empty() && guard < 64; ++guard) {
            auto [to, f] = q.front(); q.pop_front();
            const auto lost = std::find(dropOnce.begin(), dropOnce.end(), std::make_pair(to, f.empty() ? uint8_t{0} : f[0]));
            if (lost != dropOnce.end()) { dropOnce.erase(lost); continue; }
            deliver(to, f, now);
        }
    }
};

template <class M> M lastOf(const Pair& p, int to) {
    for (auto it = p.trace.rbegin(); it != p.trace.rend(); ++it) {
        M m;
        if (it->first == to && M::decode(it->second.data(), it->second.size(), m)) return m;
    }
    ADD_FAILURE() << "no such frame";
    return {};
}

} // namespace

TEST(PeerHandshakeRegression, SameKeyEstablishesMatchingLinkKeys) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    EXPECT_EQ(p.ka->sendMaster, p.kb->recvMaster);
    EXPECT_EQ(p.ka->recvMaster, p.kb->sendMaster);
    EXPECT_NE(p.ka->sendMaster, p.ka->recvMaster) << "one key per direction";
    EXPECT_EQ(p.ka->epoch, p.kb->epoch);
    EXPECT_EQ(p.kb->epoch, 1u) << "the responder's counter + 1";
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
    ASSERT_FALSE(p.logsA.empty());
    EXPECT_NE(p.logsA.back().find("peer link with node 2 established (epoch 1, key id "), std::string::npos) << p.logsA.back();
    EXPECT_NE(p.logsB.back().find("peer link with node 1 established (epoch 1, key id "), std::string::npos) << p.logsB.back();
}

TEST(PeerLinkRegression, WrongKeyIsRefusedAndLoggedOnBothSidesRegression) {
    Pair p(keys('k'), keys('x')); p.run();
    EXPECT_FALSE(p.ka || p.kb);
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(p.a.state())) << "a Refuse is logged only: the state stays";
    ASSERT_FALSE(p.logsA.empty()); ASSERT_FALSE(p.logsB.empty());
    EXPECT_NE(p.logsA.back().find("REFUSED by 2"), std::string::npos) << p.logsA.back();
    EXPECT_NE(p.logsA.back().find("no common cluster key"), std::string::npos) << p.logsA.back();
    EXPECT_NE(p.logsB.back().find("REFUSED"), std::string::npos) << p.logsB.back();
    EXPECT_NE(p.logsB.back().find("peer link from node 1 REFUSED: no common cluster key (key id "), std::string::npos) << p.logsB.back();
}

TEST(PeerHandshakeRegression, TheNextKeyIsAcceptedForRotation) {
    pl::ClusterKeys rolled = keys('n'); rolled.next = keys('k').current;   // step 2 of §8: current = next, old kept
    Pair p(keys('k'), rolled); p.run();
    EXPECT_TRUE(p.ka && p.kb) << "the responder accepts the initiator's (old) current key as its next";
    ASSERT_TRUE(p.ka);
    EXPECT_EQ(p.ka->sendMaster, p.kb->recvMaster);
}

TEST(PeerHandshakeRegression, TheRolledInitiatorIsAcceptedByAResponderHoldingItAsNext) {
    pl::ClusterKeys step1 = keys('k'); step1.next = keys('n').current;      // step 1: next = the new key
    pl::ClusterKeys rolled = keys('n'); rolled.next = keys('k').current;    // step 2 on the initiator
    Pair p(rolled, step1); p.run();
    EXPECT_TRUE(p.ka && p.kb);
}

TEST(PeerHandshakeRegression, AReplayedHelloOrProofNeverEstablishes) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const auto hello = lastOf<pl::PeerHelloV2>(p, 1);
    const auto proof = lastOf<pl::PeerProofV2>(p, 1);

    pl::PeerHandshake fresh(2, 1, 20, keys('k'));        // a fresh responder (same key): new nonceB for the replayed Hello
    auto c = fresh.onHelloV2(hello, 0);
    ASSERT_TRUE(c.frame);
    pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
    EXPECT_NE(ch.nonceB, lastOf<pl::PeerChallengeV2>(p, 0).nonceB) << "a replayed Hello meets a new nonceB";
    EXPECT_NE(ch.shareB, lastOf<pl::PeerChallengeV2>(p, 0).shareB) << "and a new ephemeral share";
    auto r = fresh.onProofV2(proof, 0);                  // the old Proof is bound to the old nonceB (another cookie)
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.frame) << "a bad Proof is logged, never answered";
    EXPECT_FALSE(std::holds_alternative<pl::Established>(fresh.state()));
    ASSERT_TRUE(r.log);
    EXPECT_TRUE(r.logIsError);
}

TEST(PeerHandshakeRegression, AReflectedChallengeMacIsNotAValidProof) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHelloV2(hello, 0); ASSERT_TRUE(c.frame);
    pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));

    pl::PeerProofV2 reflected; reflected.from = 1; reflected.to = 2; reflected.macA = ch.macB;   // B's own MAC as A's Proof
    reflected.keyId = hello.keyId; reflected.incarnation = hello.incarnation; reflected.nonceA = hello.nonceA;
    reflected.shareA = hello.shareA; reflected.nonceB = ch.nonceB;   // the cookie matches: only macA can refuse it
    const uint64_t macsBefore = b.macComputations();
    auto r = b.onProofV2(reflected, 0);
    EXPECT_EQ(b.macComputations(), macsBefore + 1) << "the cookie matched, so the transcript MAC ran";
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.frame);
    EXPECT_TRUE(r.logIsError);
}

TEST(PeerHandshakeRegression, ARefusedInitiatorKeepsRetryingTheSameHelloEvery200ms) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('x'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 first; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), first));
    auto r = b.onHelloV2(first, 1 * kMs); ASSERT_TRUE(r.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(r.frame->data(), r.frame->size(), ref));
    auto lg = a.onRefuse(ref, 5 * kMs);
    EXPECT_TRUE(lg.log && lg.logIsError) << "logged";
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state())) << "a Refuse carries no MAC: the state stays";
    EXPECT_FALSE(a.tick(199 * kMs).frame);
    EXPECT_TRUE(a.tick(200 * kMs).frame);
}

TEST(PeerHandshakeRegression, AnUnansweredHelloIsRepeatedWithTheSameNonce) {
    pl::PeerHandshake a(1, 2, 10, keys('k'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    EXPECT_FALSE(a.tick(199 * kMs).frame);
    auto again = a.tick(200 * kMs);
    ASSERT_TRUE(again.frame);
    EXPECT_EQ(*again.frame, *h.frame) << "the same Hello: a late Challenge for the first still matches";
}

TEST(PeerHandshakeRegression, APeerRestartWithAHigherIncarnationReestablishesWithANewEpoch) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const pl::LinkKeys old = *p.ka;

    // b restarts: a fresh responder with incarnation 21 and no link.  It pokes; a (initiator) starts over.
    p.b = pl::PeerHandshake(2, 1, 21, keys('k'));
    p.ka.reset(); p.kb.reset();
    p.run(2'000 * kMs);   // b's NoLink poke reaches a ...
    p.run(2'000 * kMs);   // ... and a's next duty pass sends the Hello
    ASSERT_TRUE(p.ka && p.kb) << "re-established";
    EXPECT_NE(p.ka->sendMaster, old.sendMaster) << "new keys replace the old ones";
    EXPECT_NE(p.ka->recvMaster, old.recvMaster);
    EXPECT_EQ(p.ka->sendMaster, p.kb->recvMaster);
    const auto* est = std::get_if<pl::Established>(&p.a.state());
    ASSERT_NE(est, nullptr);
    EXPECT_EQ(est->peerIncarnation, 21u);

}

TEST(PeerHandshakeRegression, AnInitiatorRestartReestablishesWithAHigherIncarnationHello) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    p.a = pl::PeerHandshake(1, 2, 11, keys('k'));
    p.ka.reset(); p.kb.reset();
    p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    EXPECT_EQ(p.kb->epoch, 2u) << "the responder kept its counter: the epoch advanced";
    const auto* est = std::get_if<pl::Established>(&p.b.state());
    ASSERT_NE(est, nullptr);
    EXPECT_EQ(est->peerIncarnation, 11u);
}

TEST(PeerHandshakeRegression, AnUnknownEpochFromASilentPeerRekeysWithoutDroppingTheLiveKeys) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const auto firstHello = lastOf<pl::PeerHelloV2>(p, 1);
    p.ka.reset(); p.kb.reset();
    auto o = p.a.onUnknownEpoch(7, 5'000 * kMs);   // silent for 5 s: the hint may start an exchange
    ASSERT_TRUE(o.frame) << "a fresh Hello";
    const auto* hs = std::get_if<pl::HelloSent>(&p.a.state());
    ASSERT_NE(hs, nullptr);
    ASSERT_TRUE(hs->prior) << "the old keys stay live until the exchange completes";
    pl::PeerHelloV2 again; ASSERT_TRUE(pl::PeerHelloV2::decode(o.frame->data(), o.frame->size(), again));
    EXPECT_NE(again.nonceA, firstHello.nonceA);
    EXPECT_NE(again.shareA, firstHello.shareA) << "a rekey draws a fresh ephemeral keypair";
    p.absorb(0, std::move(o));
    p.run(5'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    EXPECT_EQ(p.ka->epoch, 2u);
    EXPECT_EQ(p.kb->epoch, 2u);
}

TEST(PeerHandshakeRegression, TheCurrentEpochNamedAsUnknownIsIgnored) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka);
    EXPECT_FALSE(p.a.onUnknownEpoch(p.ka->epoch, 5'000 * kMs).frame);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
}

TEST(PeerHandshakeRegression, AForgedAcceptNeverEstablishesTheInitiator) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    auto h = a.tick(0);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHelloV2(hello, 0);
    pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
    auto pr = a.onChallengeV2(ch, 0); ASSERT_TRUE(pr.frame);
    ASSERT_TRUE(std::holds_alternative<pl::ChallengeSent>(a.state()));
    pl::PeerAccept forged; forged.from = 2; forged.to = 1; forged.linkEpoch = 99;     // no valid confirm
    auto f = a.onAccept(forged, 0);
    EXPECT_FALSE(f.established);
    EXPECT_TRUE(std::holds_alternative<pl::ChallengeSent>(a.state())) << "still waiting for the real Accept";
}

TEST(PeerHandshakeRegression, ADuplicateHelloGetsTheSameChallengeAndNeverLeavesEstablished) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const auto* before = std::get_if<pl::Established>(&p.b.state());
    ASSERT_NE(before, nullptr);

    pl::PeerHelloV2 hello = lastOf<pl::PeerHelloV2>(p, 1);
    hello.nonceA.fill(0x42);
    auto c1 = p.b.onHelloV2(hello, 300 * kMs);
    auto c2 = p.b.onHelloV2(hello, 350 * kMs);
    ASSERT_TRUE(c1.frame && c2.frame);
    EXPECT_EQ(*c1.frame, *c2.frame) << "idempotent for a repeated Hello (the same nonceB and the same shareB)";
    hello.shareA[0] ^= 1;
    auto c3 = p.b.onHelloV2(hello, 360 * kMs);
    ASSERT_TRUE(c3.frame);
    EXPECT_FALSE(*c3.frame == *c1.frame) << "another share is another Hello: a new Challenge";
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state())) << "an unproven Hello never leaves Established";
}

TEST(PeerHandshakeRegression, AResponderWithoutALinkPokesAndAnEstablishedInitiatorRestarts) {
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    auto poke = b.tick(0);
    ASSERT_TRUE(poke.frame);
    EXPECT_FALSE(poke.log) << "a poke is not a refusal";
    EXPECT_FALSE(poke.logIsError);
    EXPECT_FALSE(b.tick(199 * kMs).frame);
    EXPECT_TRUE(b.tick(200 * kMs).frame) << "every 200 ms";
}

TEST(PeerHandshakeRegression, ANoLinkPokeFromASilentPeerRekeysAndFromALivePeerIsIgnored) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka);
    pl::PeerRefuse poke; poke.from = 2; poke.to = 1; poke.reason = pl::RefuseReason::NoLink;
    p.a.onAuthenticated(1'900 * kMs);
    EXPECT_FALSE(p.a.onRefuse(poke, 2'000 * kMs).frame) << "an authenticated frame 100 ms ago: the peer is alive";
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    auto o = p.a.onRefuse(poke, 3'000 * kMs);
    EXPECT_TRUE(o.frame) << "silent for 1.1 s: a Hello, with the old keys kept";
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(p.a.state()));
}

TEST(PeerHandshakeRegression, AFrameFromAHigherIdThanTheInitiatorIsNotAPeer) {
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    pl::PeerHello h; h.from = 5; h.to = 2; h.keyId = keys('k').current.keyId; h.incarnation = 1;
    auto o = b.onHello(h, 0);
    ASSERT_TRUE(o.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(o.frame->data(), o.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::NotAPeer);
}

TEST(PeerHandshakeRegression, MessagesRoundTripAndRejectWrongSizes) {
    pl::PeerHello h; h.from = -1; h.to = 7; h.keyId = 0x0123456789abcdefULL; h.incarnation = 99; h.nonceA.fill(3);
    uint8_t buf[pl::kMaxPeerLinkFrame];
    h.encode(buf);
    pl::PeerHello d; ASSERT_TRUE(pl::PeerHello::decode(buf, pl::PeerHello::kSize, d));
    EXPECT_EQ(d.from, -1); EXPECT_EQ(d.to, 7); EXPECT_EQ(d.keyId, h.keyId); EXPECT_EQ(d.incarnation, 99u); EXPECT_EQ(d.nonceA, h.nonceA);
    EXPECT_FALSE(pl::PeerHello::decode(buf, pl::PeerHello::kSize - 1, d));
    { pl::PeerChallenge junk; EXPECT_FALSE(pl::PeerChallenge::decode(buf, pl::PeerHello::kSize, junk)) << "wrong tag/size"; }
    buf[9] = 0;
    pl::PeerRefuse r; r.reason = pl::RefuseReason::BadProof; r.encode(buf);
    buf[9] = 9;
    EXPECT_FALSE(pl::PeerRefuse::decode(buf, pl::PeerRefuse::kSize, r)) << "an unknown reason is not a frame";
}

// ---- Task 11 (M12): unauthenticated input never changes link state ----

TEST(PeerLinkRegression, AForgedUnknownEpochNeverRehandshakesRegression) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka);
    // The peer is alive (authenticated frames keep arriving) while a forger names epochs.
    for (int i = 1; i <= 50; ++i) {
        const int64_t now = 10'000 * kMs + i * 10 * kMs;
        p.a.onAuthenticated(now - kMs);
        EXPECT_FALSE(p.a.onUnknownEpoch(1000 + i, now).frame) << i;
        ASSERT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    }
    // Even a silent peer is rekeyed at most once per 200 ms, and the keys survive each attempt.
    const auto first = p.a.onUnknownEpoch(9, 100'000 * kMs);
    ASSERT_TRUE(first.frame);
    const auto* hs = std::get_if<pl::HelloSent>(&p.a.state());
    ASSERT_NE(hs, nullptr);
    EXPECT_TRUE(hs->prior);
}

TEST(PeerLinkRegression, ForgedRefuseChallengeHelloNoLinkNeverDisruptALinkRegression) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    const pl::LinkKeys liveA = p.ka->epoch ? *p.ka : *p.ka;
    p.a.onAuthenticated(1'500 * kMs);
    const int64_t now = 1'600 * kMs;
    // At the initiator: Refuse of every reason, a Challenge with a garbage mac.
    for (auto reason : {pl::RefuseReason::NoCommonKey, pl::RefuseReason::BadProof, pl::RefuseReason::NotAPeer,
                        pl::RefuseReason::StaleIncarnation, pl::RefuseReason::NoLink}) {
        pl::PeerRefuse r; r.from = 2; r.to = 1; r.reason = reason;
        (void)p.a.onRefuse(r, now);
        ASSERT_TRUE(std::holds_alternative<pl::Established>(p.a.state())) << static_cast<int>(reason);
    }
    pl::PeerChallenge ch; ch.from = 2; ch.to = 1; ch.keyId = keys('k').current.keyId; ch.incarnation = 1;
    (void)p.a.onChallenge(ch, now);
    pl::PeerChallengeV2 ch2; ch2.from = 2; ch2.to = 1; ch2.keyId = keys('k').current.keyId; ch2.incarnation = 1; ch2.shareB.fill(9);
    (void)p.a.onChallengeV2(ch2, now);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    EXPECT_EQ(std::get<pl::Established>(p.a.state()).keys, liveA);

    // Mid-exchange, a forged Refuse / Challenge / Accept does not abort it.
    pl::PeerHandshake a2(1, 2, 10, keys('k'));
    auto h = a2.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerRefuse r; r.from = 2; r.to = 1; r.reason = pl::RefuseReason::BadProof;
    (void)a2.onRefuse(r, 1 * kMs);
    ch.keyId = keys('k').current.keyId;
    (void)a2.onChallenge(ch, 2 * kMs);
    (void)a2.onChallengeV2(ch2, 2 * kMs);
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a2.state()));

    // At the responder: a forged Proof and an unknown-key Hello leave the live link alone.
    pl::PeerProof pr; pr.from = 1; pr.to = 2;
    (void)p.b.onProof(pr, now);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
    pl::PeerProofV2 pr2; pr2.from = 1; pr2.to = 2; pr2.keyId = keys('k').current.keyId; pr2.nonceB.fill(3);
    (void)p.b.onProofV2(pr2, now);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
    pl::PeerHello bad; bad.from = 1; bad.to = 2; bad.keyId = 0xdead; bad.incarnation = 1;
    (void)p.b.onHello(bad, now);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
    // A forged Hello with the REAL public key id is answered with a Challenge but leaves the responder Established.
    pl::PeerHello real; real.from = 1; real.to = 2; real.keyId = keys('k').current.keyId; real.incarnation = 1; real.nonceA.fill(7);
    auto ans = p.b.onHello(real, now);
    EXPECT_TRUE(ans.frame);
    ASSERT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
    EXPECT_EQ(std::get<pl::Established>(p.b.state()).keys, *p.kb) << "same keys, same epoch";
}

TEST(PeerLinkRegression, ARestartedPeerRelinksUnderAFloodRegression) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    // b restarts (no link, no keys).  A forger floods a with Refuse / NoLink / bad Challenges every 5 ms.
    p.b = pl::PeerHandshake(2, 1, 21, keys('k'));
    p.ka.reset(); p.kb.reset();
    p.q.clear();
    bool relinked = false;
    for (int i = 0; i < 400 && !relinked; ++i) {
        const int64_t now = (3'000 + i * 5) * kMs;
        pl::PeerRefuse r; r.from = 2; r.to = 1; r.reason = (i % 2) ? pl::RefuseReason::BadProof : pl::RefuseReason::NoCommonKey;
        (void)p.a.onRefuse(r, now);
        pl::PeerChallenge ch; ch.from = 2; ch.to = 1; ch.keyId = keys('k').current.keyId; ch.incarnation = 9;
        (void)p.a.onChallenge(ch, now);
        pl::PeerChallengeV2 ch2; ch2.from = 2; ch2.to = 1; ch2.keyId = keys('k').current.keyId; ch2.incarnation = 9;
        ch2.shareB.fill(static_cast<uint8_t>(i + 1));
        (void)p.a.onChallengeV2(ch2, now);
        pl::PeerHello f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = 3;   // forged Hello at b
        f.nonceA.fill(static_cast<uint8_t>(i + 1));
        (void)p.b.onHello(f, now);
        p.run(now);   // b's NoLink poke (every 200 ms), a's Hello, the real handshake
        relinked = p.ka && p.kb;
    }
    EXPECT_TRUE(relinked) << "the real handshake completes through the flood";
    ASSERT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    EXPECT_EQ(std::get<pl::Established>(p.a.state()).peerIncarnation, 21u);
}

TEST(PeerLinkRegression, AFreshDataDirPeerRejoinsRegression) {
    // A rebuilt broker has a LOWER incarnation than the one its peers last saw (fresh data dir / clock): it still joins.
    Pair p(keys('k'), keys('k'), /*incA=*/1'000, /*incB=*/2'000); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    p.b = pl::PeerHandshake(2, 1, 5, keys('k'));        // responder rebuilt: incarnation 5 < 2000
    p.ka.reset(); p.kb.reset();
    for (int i = 0; i < 3 && !(p.ka && p.kb); ++i) p.run((3'000 + i * 1'000) * kMs);
    EXPECT_TRUE(p.ka && p.kb) << "a lower incarnation at the initiator is not refused";
    p.a = pl::PeerHandshake(1, 2, 3, keys('k'));        // the initiator rebuilt too: Hello with incarnation 3 < 1000
    p.ka.reset(); p.kb.reset();
    for (int i = 0; i < 3 && !(p.ka && p.kb); ++i) p.run((10'000 + i * 1'000) * kMs);
    EXPECT_TRUE(p.ka && p.kb) << "a lower incarnation at the responder is not refused";
}

TEST(PeerLinkRegression, AForgedHelloFloodAtTheResponderNeverHoldsARestartedPeerOutRegression) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    p.a = pl::PeerHandshake(1, 2, 11, keys('k'));   // the initiator restarts: its Hello must get through the flood
    p.ka.reset(); p.kb.reset(); p.q.clear();
    for (int k = 0; k < 20; ++k) {   // the flood is already under way: every pending slot is taken by a forgery
        pl::PeerHello f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = 5;
        f.nonceA.fill(static_cast<uint8_t>(100 + k));
        (void)p.b.onHello(f, 2'999 * kMs);
    }
    bool relinked = false;
    for (int i = 0; i < 400 && !relinked; ++i) {
        const int64_t now = (3'000 + i) * kMs;
        for (int k = 0; k < 3; ++k) {   // three forged Hellos with the real key id every millisecond
            pl::PeerHello f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = 5;
            f.nonceA.fill(static_cast<uint8_t>(i * 3 + k + 1));
            (void)p.b.onHello(f, now);
            EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(p.b.state()) || std::holds_alternative<pl::Established>(p.b.state()));
        }
        p.run(now);
        relinked = p.ka && p.kb;
    }
    EXPECT_TRUE(relinked) << "the real Hello is answered and its Proof matches its own pending slot";
}

TEST(PeerLinkRegression, ForgedHintsRekeyAnIdleLinkAtMostOncePer30sRegression) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka);
    pl::PeerRefuse poke; poke.from = 2; poke.to = 1; poke.reason = pl::RefuseReason::NoLink;
    int rekeys = 0;
    for (int i = 1; i <= 100; ++i) {   // 10 s of forged hints on a link with no authenticated traffic
        const int64_t now = (2'000 + i * 100) * kMs;
        auto o = p.a.onRefuse(poke, now);
        if (!o.frame) o = p.a.onUnknownEpoch(500 + i, now);
        if (o.frame) { ++rekeys; p.absorb(0, std::move(o)); }
        p.run(now);   // the real responder answers: the exchange completes at once
    }
    EXPECT_EQ(rekeys, 1);
}

TEST(PeerLinkRegression, AForgedProofAgainstAFullTablePerformsNoMacComputationRegression) {
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    for (int k = 0; k < static_cast<int>(pl::PeerHandshake::kPending); ++k) {
        pl::PeerHello f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = 5;
        f.nonceA.fill(static_cast<uint8_t>(k + 1));
        ASSERT_TRUE(b.onHello(f, 1 * kMs).frame);
    }
    const uint64_t before = b.macComputations();
    for (int i = 0; i < 100; ++i) {
        pl::PeerProof pr; pr.from = 1; pr.to = 2; pr.macA.fill(static_cast<uint8_t>(i));
        auto o = b.onProof(pr, 2 * kMs);
        EXPECT_TRUE(o.logIsError);
        EXPECT_FALSE(o.frame);
    }
    EXPECT_EQ(b.macComputations(), before) << "a forged Proof costs constant-time compares only";
    EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state()));
}

namespace {
// One real round trip with `forgedBetween` forged Hellos landing between the Challenge and the Proof, against a
// responder whose table is already full of forgeries (a flood under way).  True when the real peer links.
bool oneTrial(int trial, int forgedBetween) {
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    pl::PeerHandshake a(1, 2, 10, keys('k'));
    auto forge = [&](int n, int64_t now) {
        pl::PeerHello f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = 5;
        f.nonceA.fill(static_cast<uint8_t>(n)); f.nonceA[1] = static_cast<uint8_t>(trial); f.nonceA[2] = 0x77;
        (void)b.onHello(f, now);
    };
    int64_t now = 1'000 * kMs;
    for (std::size_t k = 0; k < pl::PeerHandshake::kPending; ++k) forge(static_cast<int>(k + 1), now);
    auto hello = a.tick(now);
    pl::PeerHelloV2 h;
    if (!hello.frame || !pl::PeerHelloV2::decode(hello.frame->data(), hello.frame->size(), h)) return false;
    auto ch = b.onHelloV2(h, now);
    pl::PeerChallengeV2 c;
    if (!ch.frame || !pl::PeerChallengeV2::decode(ch.frame->data(), ch.frame->size(), c)) return false;
    for (int k = 0; k < forgedBetween; ++k) forge(100 + k, now);     // the round trip: forgeries keep arriving
    auto pr = a.onChallengeV2(c, now + kMs);
    pl::PeerProofV2 p;
    if (!pr.frame || !pl::PeerProofV2::decode(pr.frame->data(), pr.frame->size(), p)) return false;
    return b.onProofV2(p, now + 2 * kMs).established.has_value();
}
} // namespace

TEST(PeerLinkRegression, ARealPeerSurvivesForgedHellosArrivingDuringItsRoundTripRegression) {
    static_assert(pl::PeerHandshake::kPending == 8, "the table size is part of the contract");
    constexpr int kTrials = 300;
    int modest = 0, heavy = 0;
    for (int t = 0; t < kTrials; ++t) { modest += oneTrial(t, 2); heavy += oneTrial(t, 8); }
    EXPECT_GE(modest, kTrials * 60 / 100) << "2 forgeries per round trip: (7/8)^2 = 77% expected";
    EXPECT_GE(heavy, kTrials * 15 / 100) << "8 forgeries per round trip: oldest-first evicted the real slot EVERY time; random survives 34%";
}

TEST(PeerLinkRegression, ATimedOutHintRekeyDoesNotDelayTheNextOneRegression) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka);
    // A hint rekeys; the responder is gone, so the Hello is never answered and the attempt times out.
    auto o = p.a.onUnknownEpoch(999, 3'000 * kMs);
    ASSERT_TRUE(o.frame);
    (void)p.a.tick(3'000 * kMs + pl::PeerHandshake::kAwaitNs);
    ASSERT_TRUE(std::holds_alternative<pl::Established>(p.a.state())) << "the prior keys are restored";
    // The responder restarts and pokes: the next rekey is due within the retry interval, not after 30 s.
    pl::PeerRefuse poke; poke.from = 2; poke.to = 1; poke.reason = pl::RefuseReason::NoLink;
    auto again = p.a.onRefuse(poke, 3'000 * kMs + pl::PeerHandshake::kAwaitNs + pl::PeerHandshake::kRetryNs);
    EXPECT_TRUE(again.frame);
}

// ---- issue #241: a re-handshake with the same initiator incarnation installs the new epoch receive-first ----

namespace {
constexpr auto kRecvOnly = pl::LinkInstall::ReceiveOnly;
constexpr auto kBoth     = pl::LinkInstall::SendAndReceive;

/// A linked pair, then a same-incarnation re-handshake (a hint from a silent peer) started at `now` - not yet delivered.
Pair rehandshaking(int64_t now) {
    Pair p(keys('k'), keys('k')); p.run();
    EXPECT_TRUE(p.ka && p.kb);
    p.estA.clear(); p.estB.clear(); p.trace.clear();
    auto o = p.a.onUnknownEpoch(7, now);
    EXPECT_TRUE(o.frame);
    p.absorb(0, std::move(o));
    return p;
}
const pl::Established* est(const pl::PeerHandshake& h) { return std::get_if<pl::Established>(&h.state()); }
} // namespace

TEST(PeerHandshakeRegression, ASameIncarnationRehandshakeIsReceiveFirstUntilTheConfirmRegression) {
    const int64_t now = 5'000 * kMs;
    Pair p = rehandshaking(now);
    p.run(now);
    ASSERT_EQ(p.estB.size(), 2u) << "receive-only at the Proof, then the promotion at the Confirm";
    EXPECT_EQ(p.estB[0].epoch, 2u);
    EXPECT_EQ(p.estB[0].install, kRecvOnly) << "the responder must not seal under E2 before the initiator holds it";
    EXPECT_EQ(p.estB[1].epoch, 2u);
    EXPECT_EQ(p.estB[1].install, kBoth);
    EXPECT_EQ(p.estB[1].sendMaster, p.estB[0].sendMaster);
    ASSERT_EQ(p.estA.size(), 1u);
    EXPECT_EQ(p.estA[0].install, kBoth) << "the initiator seals at once: the responder already holds E2";
    EXPECT_EQ(p.estA[0].sendMaster, p.estB[1].recvMaster);
    const auto confirm = lastOf<pl::PeerConfirm>(p, 1);
    EXPECT_EQ(confirm.linkEpoch, 2u);
    EXPECT_EQ(confirm.from, 1); EXPECT_EQ(confirm.to, 2);
    ASSERT_NE(est(p.b), nullptr);
    EXPECT_FALSE(est(p.b)->unconfirmedAccept) << "confirmed";
}

TEST(PeerHandshakeRegression, ALostConfirmIsRecoveredByTheResentAcceptRegression) {
    const int64_t now = 5'000 * kMs;
    Pair p = rehandshaking(now);
    p.dropOnce.push_back({1, static_cast<uint8_t>(pl::PeerMsg::Confirm)});
    p.run(now);
    ASSERT_EQ(p.estB.size(), 1u);
    EXPECT_EQ(p.estB[0].install, kRecvOnly);
    ASSERT_NE(est(p.b), nullptr);
    ASSERT_TRUE(est(p.b)->unconfirmedAccept) << "still awaiting the Confirm";
    ASSERT_NE(est(p.a), nullptr);
    EXPECT_EQ(est(p.a)->keys.epoch, 2u);
    p.run(now + pl::PeerHandshake::kRetryNs - 1);
    EXPECT_EQ(p.estB.size(), 1u) << "nothing re-sent before kRetryNs";
    p.run(now + pl::PeerHandshake::kRetryNs);     // the Accept again -> the Established initiator re-sends its Confirm
    ASSERT_EQ(p.estB.size(), 2u);
    EXPECT_EQ(p.estB[1].install, kBoth);
    EXPECT_EQ(p.estB[1].epoch, 2u);
    EXPECT_EQ(p.estA.size(), 1u) << "the re-sent Accept changes nothing at the initiator but the Confirm";
    EXPECT_FALSE(est(p.b)->unconfirmedAccept);
    p.run(now + 3 * pl::PeerHandshake::kRetryNs);
    EXPECT_EQ(p.estB.size(), 2u) << "confirmed: no more Accepts";
}

TEST(PeerHandshakeRegression, AForgedStaleOrMisaddressedConfirmPromotesNothingRegression) {
    const int64_t now = 5'000 * kMs;
    Pair p = rehandshaking(now);
    p.dropOnce.push_back({1, static_cast<uint8_t>(pl::PeerMsg::Confirm)});
    p.run(now);
    const auto real = lastOf<pl::PeerConfirm>(p, 1);
    ASSERT_EQ(real.linkEpoch, 2u);
    auto forged = real; forged.confirm[0] ^= 1;
    EXPECT_FALSE(p.b.onConfirm(forged, now).established) << "wrong MAC";
    auto stale = real; stale.linkEpoch = 1;
    EXPECT_FALSE(p.b.onConfirm(stale, now).established) << "wrong epoch";
    auto future = real; future.linkEpoch = 3;
    EXPECT_FALSE(p.b.onConfirm(future, now).established) << "wrong epoch";
    auto other = real; other.from = 3;
    EXPECT_FALSE(p.b.onConfirm(other, now).established) << "from another node";
    auto misaddressed = real; misaddressed.to = 5;
    EXPECT_FALSE(p.b.onConfirm(misaddressed, now).established) << "to another node";
    EXPECT_FALSE(p.a.onConfirm(real, now).established) << "the initiator never takes a Confirm";
    ASSERT_TRUE(est(p.b)->unconfirmedAccept) << "nothing changed";
    const auto ok = p.b.onConfirm(real, now);
    ASSERT_TRUE(ok.established);
    EXPECT_EQ(ok.established->install, kBoth);
    EXPECT_FALSE(p.b.onConfirm(real, now).established) << "a duplicate Confirm is a no-op";
}

TEST(PeerHandshakeRegression, AFirstLinkAndANewInitiatorIncarnationInstallSendAndReceiveAtOnceRegression) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_EQ(p.estB.size(), 1u);
    EXPECT_EQ(p.estB[0].install, kBoth) << "a first link: nothing to protect";
    ASSERT_NE(est(p.b), nullptr);
    EXPECT_FALSE(est(p.b)->unconfirmedAccept);
    // The initiator restarts (a new incarnation holds no old epoch): immediate install, as before.
    p.a = pl::PeerHandshake(1, 2, 11, keys('k'));
    p.estA.clear(); p.estB.clear();
    p.run(1'000 * kMs);
    ASSERT_EQ(p.estB.size(), 1u);
    EXPECT_EQ(p.estB[0].epoch, 2u);
    EXPECT_EQ(p.estB[0].install, kBoth);
    EXPECT_FALSE(est(p.b)->unconfirmedAccept);
}

TEST(PeerHandshakeRegression, AnAuthenticatedFrameUnderTheUnconfirmedEpochPromotesItRegression) {
    const int64_t now = 5'000 * kMs;
    Pair p = rehandshaking(now);
    p.dropOnce.push_back({1, static_cast<uint8_t>(pl::PeerMsg::Confirm)});   // a legacy initiator sends none
    p.run(now);
    ASSERT_TRUE(est(p.b) && est(p.b)->unconfirmedAccept);
    EXPECT_FALSE(p.b.onPeerUsedEpoch(1, now).established) << "the old epoch proves nothing";
    EXPECT_FALSE(p.b.onPeerUsedEpoch(3, now).established);
    EXPECT_FALSE(p.a.onPeerUsedEpoch(2, now).established) << "the initiator has nothing unconfirmed";
    const auto o = p.b.onPeerUsedEpoch(2, now);
    ASSERT_TRUE(o.established);
    EXPECT_EQ(o.established->epoch, 2u);
    EXPECT_EQ(o.established->install, kBoth);
    EXPECT_FALSE(est(p.b)->unconfirmedAccept);
    EXPECT_FALSE(p.b.onPeerUsedEpoch(2, now).established) << "promoted once";
}

TEST(PeerHandshakeRegression, AnAcceptMacIsNeverAValidConfirmRegression) {
    const int64_t now = 5'000 * kMs;
    Pair p = rehandshaking(now);
    p.dropOnce.push_back({1, static_cast<uint8_t>(pl::PeerMsg::Confirm)});
    p.run(now);
    const auto accept = lastOf<pl::PeerAccept>(p, 0);
    ASSERT_EQ(accept.linkEpoch, 2u);
    pl::PeerConfirm reflected; reflected.from = 1; reflected.to = 2; reflected.linkEpoch = accept.linkEpoch;
    reflected.confirm = accept.confirm;
    EXPECT_FALSE(p.b.onConfirm(reflected, now).established) << "the Accept's MAC reflected as a Confirm";
    ASSERT_TRUE(est(p.b)->unconfirmedAccept);
}

TEST(PeerHandshakeRegression, TheConfirmMessageRoundTripsAndRejectsWrongSizesRegression) {
    pl::PeerConfirm c; c.from = 1; c.to = 2; c.linkEpoch = 0xA1B2C3D4u; c.confirm.fill(0x5A);
    uint8_t buf[pl::kMaxPeerLinkFrame];
    c.encode(buf);
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerConfirm::kSize), 6);
    pl::PeerConfirm d;
    ASSERT_TRUE(pl::PeerConfirm::decode(buf, pl::PeerConfirm::kSize, d));
    EXPECT_EQ(d.from, 1); EXPECT_EQ(d.to, 2); EXPECT_EQ(d.linkEpoch, 0xA1B2C3D4u); EXPECT_EQ(d.confirm, c.confirm);
    EXPECT_FALSE(pl::PeerConfirm::decode(buf, pl::PeerConfirm::kSize - 1, d));
    EXPECT_FALSE(pl::PeerConfirm::decode(buf, pl::PeerConfirm::kSize + 1, d));
    pl::PeerAccept asAccept;
    EXPECT_FALSE(pl::PeerAccept::decode(buf, pl::PeerAccept::kSize, asAccept)) << "same size, other tag";
    buf[0] = 10;  // 7 / 8 are HelloV2 / ChallengeV2 (issue #242), 9 ProofV2 (issue #18)
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerConfirm::kSize), 0) << "an unknown tag";
}

// ---- issue #242: forward secrecy - an X25519 ephemeral secret mixed into the link keys ----

namespace {
/// The v1 transcript MAC exactly as a pre-#242 broker computes it (PeerHandshake.h): lets a test play a legacy peer.
std::array<uint8_t, 64> v1Mac(const pl::ClusterKey& key, bool isB, int32_t idA, int32_t idB, const std::array<uint8_t, 32>& nonceA,
                              const std::array<uint8_t, 32>& nonceB, uint64_t incA, uint64_t incB, uint64_t keyId) {
    std::vector<uint8_t> buf;
    const std::string label = isB ? "rune-peer-v1|B" : "rune-peer-v1|A";
    buf.insert(buf.end(), label.begin(), label.end());
    auto put = [&](uint64_t v, int n) { for (int i = 0; i < n; ++i) buf.push_back(static_cast<uint8_t>(v >> (8 * i))); };
    auto putN = [&](const std::array<uint8_t, 32>& a) { buf.insert(buf.end(), a.begin(), a.end()); };
    if (isB) { put(static_cast<uint32_t>(idB), 4); put(static_cast<uint32_t>(idA), 4); putN(nonceA); putN(nonceB); put(incA, 8); put(incB, 8); }
    else     { put(static_cast<uint32_t>(idA), 4); put(static_cast<uint32_t>(idB), 4); putN(nonceB); putN(nonceA); put(incB, 8); put(incA, 8); }
    put(keyId, 8);
    rune::crypto::DigestBytes d;
    EXPECT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, {key.master.data(), key.master.size()}, buf, d));
    std::array<uint8_t, 64> out{};
    std::copy_n(d.bytes.begin(), 64, out.begin());
    return out;
}
const std::array<uint8_t, 32> kZero32{};
} // namespace

TEST(PeerHandshakeRegression, AHandshakeCarriesEphemeralSharesCoveredByTheMacsRegression) {
    {   // The wire: a new pair speaks only v2, with non-zero shares, and links.
        Pair p(keys('k'), keys('k')); p.run();
        ASSERT_TRUE(p.ka && p.kb);
        for (const auto& [to, f] : p.trace) {
            ASSERT_FALSE(f.empty());
            EXPECT_NE(f[0], static_cast<uint8_t>(pl::PeerMsg::Hello)) << "no v1 Hello";
            EXPECT_NE(f[0], static_cast<uint8_t>(pl::PeerMsg::Challenge)) << "no v1 Challenge";
        }
        EXPECT_NE(lastOf<pl::PeerHelloV2>(p, 1).shareA, kZero32);
        EXPECT_NE(lastOf<pl::PeerChallengeV2>(p, 0).shareB, kZero32);
        EXPECT_NE(lastOf<pl::PeerHelloV2>(p, 1).shareA, lastOf<pl::PeerChallengeV2>(p, 0).shareB);
    }
    {   // shareB flipped in transit: macB covers it - the initiator refuses (BadProof), no Proof, no keys.
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        auto h = a.tick(0); ASSERT_TRUE(h.frame);
        pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
        auto c = b.onHelloV2(hello, 0); ASSERT_TRUE(c.frame);
        pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
        auto tampered = ch; tampered.shareB[7] ^= 0x01;
        auto o = a.onChallengeV2(tampered, 0);
        EXPECT_FALSE(o.frame) << "no Proof for a tampered share";
        EXPECT_FALSE(o.established);
        ASSERT_TRUE(o.log); EXPECT_TRUE(o.logIsError);
        EXPECT_NE(o.log->find("bad proof"), std::string::npos) << o.log->str();
        EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state())) << "the state stays, as for a bad MAC";
        auto ok = a.onChallengeV2(ch, 0);
        EXPECT_TRUE(ok.frame) << "the untouched Challenge is still good";
    }
    {   // shareA flipped on the way to the responder: its Challenge MAC covers the flipped share, the initiator refuses.
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        auto h = a.tick(0); ASSERT_TRUE(h.frame);
        pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
        hello.shareA[3] ^= 0x80;
        auto c = b.onHelloV2(hello, 0); ASSERT_TRUE(c.frame);
        pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
        auto o = a.onChallengeV2(ch, 0);
        EXPECT_FALSE(o.frame);
        EXPECT_FALSE(o.established);
        EXPECT_TRUE(o.logIsError);
        EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state()));
    }
}

TEST(PeerHandshakeRegression, RecordedTrafficAndTheMasterDoNotYieldTheLinkKeysRegression) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    // Everything a passive recorder saw, plus the cluster key it stole later.
    const auto hello = lastOf<pl::PeerHelloV2>(p, 1);
    const auto ch = lastOf<pl::PeerChallengeV2>(p, 0);
    const auto accept = lastOf<pl::PeerAccept>(p, 0);
    const auto master = keys('k').current;
    ASSERT_EQ(accept.linkEpoch, p.ka->epoch);
    std::vector<std::optional<pl::LinkKeys>> guesses;
    guesses.push_back(pl::deriveLinkKeys(master, 1, 2, true, hello.nonceA, ch.nonceB, accept.linkEpoch, std::nullopt));
    guesses.push_back(pl::deriveLinkKeys(master, 1, 2, true, hello.nonceA, ch.nonceB, accept.linkEpoch,
                                         std::span<const uint8_t, 32>(hello.shareA)));
    guesses.push_back(pl::deriveLinkKeys(master, 1, 2, true, hello.nonceA, ch.nonceB, accept.linkEpoch,
                                         std::span<const uint8_t, 32>(ch.shareB)));
    std::array<uint8_t, 32> x{};
    for (std::size_t i = 0; i < 32; ++i) x[i] = hello.shareA[i] ^ ch.shareB[i];
    guesses.push_back(pl::deriveLinkKeys(master, 1, 2, true, hello.nonceA, ch.nonceB, accept.linkEpoch,
                                         std::span<const uint8_t, 32>(x)));
    for (std::size_t g = 0; g < guesses.size(); ++g) {
        ASSERT_TRUE(guesses[g]) << g;
        EXPECT_NE(guesses[g]->sendMaster, p.ka->sendMaster) << "guess " << g;
        EXPECT_NE(guesses[g]->recvMaster, p.ka->recvMaster) << "guess " << g;
        EXPECT_NE(guesses[g]->sendMaster, p.kb->recvMaster) << "guess " << g;
    }
    // The real ends still agree with each other.
    EXPECT_EQ(p.ka->sendMaster, p.kb->recvMaster);
    EXPECT_EQ(p.ka->recvMaster, p.kb->sendMaster);
}

TEST(PeerHandshakeRegression, AV1HelloStillLinksWithNoForwardSecrecyRegression) {
    // A legacy (pre-#242) initiator: a v1 Hello, a v1 Proof.  The new responder answers in v1 and derives v1 keys.
    const auto master = keys('k').current;
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    pl::PeerHello h; h.from = 1; h.to = 2; h.keyId = master.keyId; h.incarnation = 10; h.nonceA.fill(0x5c);
    auto c = b.onHello(h, 0);
    ASSERT_TRUE(c.frame);
    ASSERT_EQ(c.frame->data()[0], static_cast<uint8_t>(pl::PeerMsg::Challenge)) << "a v1 Hello gets a v1 Challenge";
    pl::PeerChallenge ch; ASSERT_TRUE(pl::PeerChallenge::decode(c.frame->data(), c.frame->size(), ch));
    EXPECT_EQ(ch.macB, v1Mac(master, true, 1, 2, h.nonceA, ch.nonceB, 10, 20, master.keyId)) << "the v1 macB, byte-identical";
    auto again = b.onHello(h, kMs);
    ASSERT_TRUE(again.frame);
    EXPECT_EQ(*again.frame, *c.frame) << "a repeated v1 Hello gets the same v1 Challenge";
    pl::PeerProof pr; pr.from = 1; pr.to = 2; pr.macA = v1Mac(master, false, 1, 2, h.nonceA, ch.nonceB, 10, 20, master.keyId);
    auto acc = b.onProof(pr, 2 * kMs);
    ASSERT_TRUE(acc.established) << "the legacy initiator links";
    const auto want = pl::deriveLinkKeys(master, 2, 1, false, h.nonceA, ch.nonceB, acc.established->epoch, std::nullopt);
    ASSERT_TRUE(want);
    EXPECT_EQ(acc.established->sendMaster, want->sendMaster) << "v1 keys: no forward secrecy for a legacy peer";
    EXPECT_EQ(acc.established->recvMaster, want->recvMaster);
}

TEST(PeerHandshakeRegression, AV1ChallengeToAV2HelloIsRefusedRegression) {
    const auto master = keys('k').current;
    pl::PeerHandshake a(1, 2, 10, keys('k'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    // A v1 Challenge with a VALID v1 MAC over our nonce (what a stripping attacker holding the master could forge).
    pl::PeerChallenge ch; ch.from = 2; ch.to = 1; ch.keyId = master.keyId; ch.incarnation = 20; ch.nonceB.fill(0x6d);
    ch.macB = v1Mac(master, true, 1, 2, hello.nonceA, ch.nonceB, 10, 20, master.keyId);
    auto o = a.onChallenge(ch, kMs);
    EXPECT_FALSE(o.frame) << "no v1 Proof: the downgrade is refused";
    EXPECT_FALSE(o.established);
    ASSERT_TRUE(o.log);
    EXPECT_TRUE(o.logIsError) << "logged through the (rate-limited) refusal path";
    EXPECT_NE(o.log->find("downgrade"), std::string::npos) << o.log->str();
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state()));
    // The real v2 responder still links afterwards.
    pl::PeerHandshake b(2, 1, 20, keys('k'));
    auto c = b.onHelloV2(hello, kMs); ASSERT_TRUE(c.frame);
    pl::PeerChallengeV2 ch2; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch2));
    EXPECT_TRUE(a.onChallengeV2(ch2, 2 * kMs).frame);
}

TEST(PeerHandshakeRegression, TheV2MessagesRoundTripAndRejectWrongSizesRegression) {
    pl::PeerHelloV2 h; h.from = 1; h.to = 2; h.keyId = 0x0123456789abcdefULL; h.incarnation = 7; h.nonceA.fill(3); h.shareA.fill(4);
    pl::PeerChallengeV2 c; c.from = 2; c.to = 1; c.keyId = 9; c.incarnation = 8; c.nonceB.fill(5); c.macB.fill(6); c.shareB.fill(7);
    static_assert(pl::kMaxPeerLinkFrame == pl::PeerProofV2::kSize);   // issue #18: the largest frame
    static_assert(pl::kMaxPeerLinkFrame <= pl::PeerHandshake::FrameBuf::kCap);
    uint8_t buf[pl::kMaxPeerLinkFrame];
    h.encode(buf);
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerHelloV2::kSize), 7);
    pl::PeerHelloV2 hd; ASSERT_TRUE(pl::PeerHelloV2::decode(buf, pl::PeerHelloV2::kSize, hd));
    EXPECT_EQ(hd.keyId, h.keyId); EXPECT_EQ(hd.nonceA, h.nonceA); EXPECT_EQ(hd.shareA, h.shareA); EXPECT_EQ(hd.incarnation, 7u);
    EXPECT_FALSE(pl::PeerHelloV2::decode(buf, pl::PeerHelloV2::kSize - 1, hd));
    { pl::PeerHello v1; EXPECT_FALSE(pl::PeerHello::decode(buf, pl::PeerHello::kSize, v1)) << "another tag"; }
    c.encode(buf);
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerChallengeV2::kSize), 8);
    pl::PeerChallengeV2 cd; ASSERT_TRUE(pl::PeerChallengeV2::decode(buf, pl::PeerChallengeV2::kSize, cd));
    EXPECT_EQ(cd.nonceB, c.nonceB); EXPECT_EQ(cd.macB, c.macB); EXPECT_EQ(cd.shareB, c.shareB); EXPECT_EQ(cd.keyId, 9u);
    EXPECT_FALSE(pl::PeerChallengeV2::decode(buf, pl::PeerChallengeV2::kSize + 1, cd));
    pl::PeerProofV2 pv; pv.from = 1; pv.to = 2; pv.keyId = 11; pv.incarnation = 12; pv.nonceA.fill(1); pv.shareA.fill(2);
    pv.nonceB.fill(3); pv.macA.fill(4);
    pv.encode(buf);
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerProofV2::kSize), 9);
    pl::PeerProofV2 pd; ASSERT_TRUE(pl::PeerProofV2::decode(buf, pl::PeerProofV2::kSize, pd));
    EXPECT_EQ(pd.from, 1); EXPECT_EQ(pd.to, 2); EXPECT_EQ(pd.keyId, 11u); EXPECT_EQ(pd.incarnation, 12u);
    EXPECT_EQ(pd.nonceA, pv.nonceA); EXPECT_EQ(pd.shareA, pv.shareA); EXPECT_EQ(pd.nonceB, pv.nonceB); EXPECT_EQ(pd.macA, pv.macA);
    EXPECT_FALSE(pl::PeerProofV2::decode(buf, pl::PeerProofV2::kSize - 1, pd));
    { pl::PeerProof v1; EXPECT_FALSE(pl::PeerProof::decode(buf, pl::PeerProof::kSize, v1)) << "another tag"; }
}

// ---- issue #18: a v2 Hello is answered with a stateless cookie, not a pending slot ----

namespace {
/// Hand a Proof frame (whatever its tag) to the responder.
pl::PeerHandshake::Out deliverProof(pl::PeerHandshake& b, const pl::PeerHandshake::FrameBuf& f, int64_t now) {
    pl::PeerProofV2 m;
    EXPECT_TRUE(pl::PeerProofV2::decode(f.data(), f.size(), m));
    return b.onProofV2(m, now);
}

/// A HelloV2 from `a` answered by `b` at `now`; returns the Proof frame `a` sends for it.
pl::PeerHandshake::FrameBuf proofFor(pl::PeerHandshake& a, pl::PeerHandshake& b, int64_t now) {
    auto h = a.tick(now);
    EXPECT_TRUE(h.frame);
    pl::PeerHelloV2 hello; EXPECT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHelloV2(hello, now);
    EXPECT_TRUE(c.frame);
    pl::PeerChallengeV2 ch; EXPECT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
    auto pr = a.onChallengeV2(ch, now);
    EXPECT_TRUE(pr.frame);
    return pr.frame.value_or(pl::PeerHandshake::FrameBuf{});
}
} // namespace

TEST(PeerHandshakeRegression, AHelloV2FloodNeverEvictsARealExchangeRegression) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    const int64_t now = 1'000 * kMs;
    auto h = a.tick(now); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHelloV2(hello, now); ASSERT_TRUE(c.frame);
    pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch));
    // 10,000 forged HelloV2 (from the peer, the real public key id, seeded random nonces and shares) land during the
    // real exchange's round trip.  Each is answered; none may cost the real initiator its Challenge.
    std::mt19937_64 rng(18);
    for (int i = 0; i < 10'000; ++i) {
        pl::PeerHelloV2 f; f.from = 1; f.to = 2; f.keyId = keys('k').current.keyId; f.incarnation = rng();
        for (auto& x : f.nonceA) x = static_cast<uint8_t>(rng());
        for (auto& x : f.shareA) x = static_cast<uint8_t>(rng());
        ASSERT_TRUE(b.onHelloV2(f, now + kMs).frame) << i;
    }
    auto pr = a.onChallengeV2(ch, now + 2 * kMs); ASSERT_TRUE(pr.frame);
    auto acc = deliverProof(b, *pr.frame, now + 3 * kMs);
    EXPECT_TRUE(acc.established) << "the real Proof must still match: " << (acc.log ? acc.log->str() : "");
    EXPECT_TRUE(std::holds_alternative<pl::Established>(b.state()));
}

TEST(PeerHandshakeRegression, ACookieOutlivesOneRotationButNotTwoRegression) {
    {   // The Proof arrives kAwaitNs + 1 ms after the Challenge: the secret rotated once, the previous one still holds.
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        const auto proof = proofFor(a, b, 0);
        (void)b.tick(pl::PeerHandshake::kAwaitNs);   // the responder's duty cycle rotates on time
        auto acc = deliverProof(b, proof, pl::PeerHandshake::kAwaitNs + kMs);
        EXPECT_TRUE(acc.established) << (acc.log ? acc.log->str() : "");
    }
    {   // 2 x kAwaitNs + 1 ms: two rotations - the Challenge's secret is gone.
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        const auto proof = proofFor(a, b, 0);
        (void)b.tick(pl::PeerHandshake::kAwaitNs);
        (void)b.tick(2 * pl::PeerHandshake::kAwaitNs);
        auto acc = deliverProof(b, proof, 2 * pl::PeerHandshake::kAwaitNs + kMs);
        EXPECT_FALSE(acc.established);
        EXPECT_FALSE(acc.frame);
        EXPECT_TRUE(acc.logIsError);
    }
    {   // The same without a duty cycle in between: the rotation is on the clock's grid, not on the calls.
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        const auto proof = proofFor(a, b, 0);
        auto acc = deliverProof(b, proof, 2 * pl::PeerHandshake::kAwaitNs + kMs);
        EXPECT_FALSE(acc.established);
        ASSERT_TRUE(acc.log);
        EXPECT_NE(acc.log->find("stale or forged cookie"), std::string::npos) << acc.log->str();
    }
}

TEST(PeerHandshakeRegression, AReplayedProofV2NeverEstablishesASecondEpochRegression) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    ASSERT_NE(est(p.b), nullptr);
    ASSERT_EQ(est(p.b)->keys.epoch, 1u);
    const auto first = lastOf<pl::PeerProofV2>(p, 1);
    // Replayed at once, well inside the secret's lifetime: the establishment rotated both secrets.
    auto r = p.b.onProofV2(first, kMs);
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.frame);
    ASSERT_TRUE(r.log);
    EXPECT_TRUE(r.logIsError);
    EXPECT_NE(r.log->find("stale or forged cookie"), std::string::npos) << r.log->str();
    ASSERT_NE(est(p.b), nullptr);
    EXPECT_EQ(est(p.b)->keys.epoch, 1u) << "no second epoch";
    // The initiator re-handshakes once more; both captured Proofs replayed afterwards still establish nothing.
    const int64_t now = 5'000 * kMs;
    p.absorb(0, p.a.rekeyForTest(now));
    p.run(now);
    ASSERT_NE(est(p.b), nullptr);
    ASSERT_EQ(est(p.b)->keys.epoch, 2u);
    const auto second = lastOf<pl::PeerProofV2>(p, 1);
    for (const auto& proof : {first, second}) {
        auto o = p.b.onProofV2(proof, now + kMs);
        EXPECT_FALSE(o.established);
        EXPECT_TRUE(o.logIsError);
        ASSERT_NE(est(p.b), nullptr);
        EXPECT_EQ(est(p.b)->keys.epoch, 2u);
    }
}

TEST(PeerHandshakeRegression, AForgedProofV2CostsNoTranscriptMacRegression) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    ASSERT_TRUE(b.onHelloV2(hello, 0).frame);
    const uint64_t before = b.macComputations();
    std::mt19937_64 rng(1818);
    for (int i = 0; i < 200; ++i) {
        // Half echo the real Hello exactly (only nonceB is guessed), half are random throughout.
        pl::PeerProofV2 f; f.from = 1; f.to = 2; f.keyId = hello.keyId;
        if (i % 2 == 0) { f.incarnation = hello.incarnation; f.nonceA = hello.nonceA; f.shareA = hello.shareA; }
        else { f.incarnation = rng(); for (auto& x : f.nonceA) x = static_cast<uint8_t>(rng()); for (auto& x : f.shareA) x = static_cast<uint8_t>(rng()); }
        for (auto& x : f.nonceB) x = static_cast<uint8_t>(rng());
        for (auto& x : f.macA) x = static_cast<uint8_t>(rng());
        auto o = b.onProofV2(f, kMs);
        EXPECT_FALSE(o.established) << i;
        EXPECT_FALSE(o.frame) << i;
        EXPECT_TRUE(o.logIsError) << i;
    }
    EXPECT_EQ(b.macComputations(), before) << "a forged ProofV2 costs cookie HMACs and compares only";
    EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state()));
}

TEST(PeerHandshakeRegression, ATamperedProofV2EchoIsRefusedRegression) {
    const char* fields[] = {"keyId", "incarnation", "nonceA", "shareA", "nonceB", "macA"};
    for (int field = 0; field < 6; ++field) {
        pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
        const auto frame = proofFor(a, b, 0);
        pl::PeerProofV2 real; ASSERT_TRUE(pl::PeerProofV2::decode(frame.data(), frame.size(), real));
        auto t = real;
        switch (field) {
            case 0: t.keyId ^= 1; break;
            case 1: t.incarnation ^= 1; break;
            case 2: t.nonceA[5] ^= 1; break;
            case 3: t.shareA[5] ^= 1; break;
            case 4: t.nonceB[5] ^= 1; break;
            default: t.macA[5] ^= 1; break;
        }
        const uint64_t macs = b.macComputations();
        auto o = b.onProofV2(t, kMs);
        // An echo field is bound by the cookie: tampering fails there, before any transcript MAC.  Only a tampered
        // macA (with intact echoes) reaches the transcript MAC.
        EXPECT_EQ(b.macComputations() - macs, field == 5 ? 1u : 0u) << fields[field];
        EXPECT_FALSE(o.established) << fields[field];
        EXPECT_FALSE(o.frame) << fields[field];
        EXPECT_TRUE(o.logIsError) << fields[field];
        EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(b.state())) << fields[field];
        EXPECT_TRUE(b.onProofV2(real, 2 * kMs).established) << "control: the untouched Proof still links (" << fields[field] << ")";
    }
}

TEST(PeerHandshakeRegression, TheSameHelloV2TwiceGetsTheSameChallengeRegression) {
    const auto master = keys('k').current;
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    // A legacy v1 Hello holds a pending slot: the v2 path must leave it alone.
    pl::PeerHello v1; v1.from = 1; v1.to = 2; v1.keyId = master.keyId; v1.incarnation = 10; v1.nonceA.fill(0x5c);
    auto v1c = b.onHello(v1, 0); ASSERT_TRUE(v1c.frame);
    pl::PeerChallenge v1ch; ASSERT_TRUE(pl::PeerChallenge::decode(v1c.frame->data(), v1c.frame->size(), v1ch));

    auto h = a.tick(10 * kMs); ASSERT_TRUE(h.frame);
    pl::PeerHelloV2 hello; ASSERT_TRUE(pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello));
    auto c1 = b.onHelloV2(hello, 10 * kMs);
    for (int i = 0; i < 50; ++i) {   // other Hellos in between change nothing (no table)
        auto other = hello; other.nonceA[0] = static_cast<uint8_t>(i); other.nonceA[1] = 0xEE;
        ASSERT_TRUE(b.onHelloV2(other, 20 * kMs).frame);
    }
    auto c2 = b.onHelloV2(hello, 900 * kMs);
    ASSERT_TRUE(c1.frame && c2.frame);
    EXPECT_EQ(*c1.frame, *c2.frame) << "the same nonceB and shareB, without a stored slot";
    pl::PeerChallengeV2 ch; ASSERT_TRUE(pl::PeerChallengeV2::decode(c1.frame->data(), c1.frame->size(), ch));
    auto other = hello; other.nonceA[0] ^= 1;
    auto c3 = b.onHelloV2(other, 900 * kMs); ASSERT_TRUE(c3.frame);
    pl::PeerChallengeV2 ch3; ASSERT_TRUE(pl::PeerChallengeV2::decode(c3.frame->data(), c3.frame->size(), ch3));
    EXPECT_NE(ch3.nonceB, ch.nonceB);
    EXPECT_NE(ch3.shareB, ch.shareB);
    // The legacy v1 exchange still completes against its own slot.
    pl::PeerProof pr; pr.from = 1; pr.to = 2; pr.macA = v1Mac(master, false, 1, 2, v1.nonceA, v1ch.nonceB, 10, 20, master.keyId);
    EXPECT_TRUE(b.onProof(pr, 950 * kMs).established) << "the v1 pending slot survived the v2 Hellos";
}

// Issue #18: the responder re-derives shareB with rune::crypto::x25519Public.  The same RFC 7748 §6.1 vector as
// crypto/tests/DhTest.cpp, here too because the broker build does not build the crypto tests (issue #542).
TEST(PeerHandshakeRegression, X25519PublicMatchesRfc7748InThePeerlinkBuildRegression) {
    auto hex = [](std::string_view h) {
        std::array<uint8_t, 32> v{};
        for (std::size_t i = 0; i < 32; ++i) v[i] = static_cast<uint8_t>(std::stoi(std::string(h.substr(2 * i, 2)), nullptr, 16));
        return v;
    };
    std::array<uint8_t, 32> pub{};
    ASSERT_TRUE(rune::crypto::x25519Public(hex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a"), pub));
    EXPECT_EQ(pub, hex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"));
    ASSERT_TRUE(rune::crypto::x25519Public(hex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb"), pub));
    EXPECT_EQ(pub, hex("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"));
}
