// PeerHandshakeTest.cpp — authentication plan Task 15: the mutual HMAC challenge-response (pure, fake clock).
#include <rune/peerlink/PeerHandshake.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
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
    const auto hello = lastOf<pl::PeerHello>(p, 1);
    const auto proof = lastOf<pl::PeerProof>(p, 1);

    pl::PeerHandshake fresh(2, 1, 20, keys('k'));        // a fresh responder (same key): new nonceB for the replayed Hello
    auto c = fresh.onHello(hello, 0);
    ASSERT_TRUE(c.frame);
    pl::PeerChallenge ch; ASSERT_TRUE(pl::PeerChallenge::decode(c.frame->data(), c.frame->size(), ch));
    EXPECT_NE(ch.nonceB, lastOf<pl::PeerChallenge>(p, 0).nonceB) << "a replayed Hello meets a new nonceB";
    auto r = fresh.onProof(proof, 0);                    // the old Proof is bound to the old nonceB
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.frame) << "a bad Proof is logged, never answered";
    EXPECT_FALSE(std::holds_alternative<pl::Established>(fresh.state()));
    ASSERT_TRUE(r.log);
    EXPECT_TRUE(r.logIsError);
}

TEST(PeerHandshakeRegression, AReflectedChallengeMacIsNotAValidProof) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('k'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHello hello; ASSERT_TRUE(pl::PeerHello::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHello(hello, 0); ASSERT_TRUE(c.frame);
    pl::PeerChallenge ch; ASSERT_TRUE(pl::PeerChallenge::decode(c.frame->data(), c.frame->size(), ch));

    pl::PeerProof reflected; reflected.from = 1; reflected.to = 2; reflected.macA = ch.macB;   // B's own MAC as A's Proof
    auto r = b.onProof(reflected, 0);
    EXPECT_FALSE(r.established);
    EXPECT_FALSE(r.frame);
    EXPECT_TRUE(r.logIsError);
}

TEST(PeerHandshakeRegression, ARefusedInitiatorKeepsRetryingTheSameHelloEvery200ms) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('x'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHello first; ASSERT_TRUE(pl::PeerHello::decode(h.frame->data(), h.frame->size(), first));
    auto r = b.onHello(first, 1 * kMs); ASSERT_TRUE(r.frame);
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
    const auto firstHello = lastOf<pl::PeerHello>(p, 1);
    p.ka.reset(); p.kb.reset();
    auto o = p.a.onUnknownEpoch(7, 5'000 * kMs);   // silent for 5 s: the hint may start an exchange
    ASSERT_TRUE(o.frame) << "a fresh Hello";
    const auto* hs = std::get_if<pl::HelloSent>(&p.a.state());
    ASSERT_NE(hs, nullptr);
    ASSERT_TRUE(hs->prior) << "the old keys stay live until the exchange completes";
    pl::PeerHello again; ASSERT_TRUE(pl::PeerHello::decode(o.frame->data(), o.frame->size(), again));
    EXPECT_NE(again.nonceA, firstHello.nonceA);
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
    pl::PeerHello hello; ASSERT_TRUE(pl::PeerHello::decode(h.frame->data(), h.frame->size(), hello));
    auto c = b.onHello(hello, 0);
    pl::PeerChallenge ch; ASSERT_TRUE(pl::PeerChallenge::decode(c.frame->data(), c.frame->size(), ch));
    auto pr = a.onChallenge(ch, 0); ASSERT_TRUE(pr.frame);
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

    pl::PeerHello hello = lastOf<pl::PeerHello>(p, 1);
    hello.nonceA.fill(0x42);
    auto c1 = p.b.onHello(hello, 300 * kMs);
    auto c2 = p.b.onHello(hello, 350 * kMs);
    ASSERT_TRUE(c1.frame && c2.frame);
    EXPECT_EQ(*c1.frame, *c2.frame) << "idempotent for a repeated Hello";
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
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    EXPECT_EQ(std::get<pl::Established>(p.a.state()).keys, liveA);

    // Mid-exchange, a forged Refuse / Challenge / Accept does not abort it.
    pl::PeerHandshake a2(1, 2, 10, keys('k'));
    auto h = a2.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerRefuse r; r.from = 2; r.to = 1; r.reason = pl::RefuseReason::BadProof;
    (void)a2.onRefuse(r, 1 * kMs);
    ch.keyId = keys('k').current.keyId;
    (void)a2.onChallenge(ch, 2 * kMs);
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a2.state()));

    // At the responder: a forged Proof and an unknown-key Hello leave the live link alone.
    pl::PeerProof pr; pr.from = 1; pr.to = 2;
    (void)p.b.onProof(pr, now);
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
    pl::PeerHello h;
    if (!hello.frame || !pl::PeerHello::decode(hello.frame->data(), hello.frame->size(), h)) return false;
    auto ch = b.onHello(h, now);
    pl::PeerChallenge c;
    if (!ch.frame || !pl::PeerChallenge::decode(ch.frame->data(), ch.frame->size(), c)) return false;
    for (int k = 0; k < forgedBetween; ++k) forge(100 + k, now);     // the round trip: forgeries keep arriving
    auto pr = a.onChallenge(c, now + kMs);
    pl::PeerProof p;
    if (!pr.frame || !pl::PeerProof::decode(pr.frame->data(), pr.frame->size(), p)) return false;
    return b.onProof(p, now + 2 * kMs).established.has_value();
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
    buf[0] = 7;
    EXPECT_EQ(pl::peekPeerMsg(buf, pl::PeerConfirm::kSize), 0) << "an unknown tag";
}
