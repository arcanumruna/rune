// PeerHandshakeTest.cpp — authentication plan Task 15: the mutual HMAC challenge-response (pure, fake clock).
#include <rune/peerlink/PeerHandshake.h>

#include <gtest/gtest.h>

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
    std::vector<std::string> logsA, logsB;
    std::vector<std::pair<int, std::vector<uint8_t>>> trace;   // (destination side, frame): 0 -> a, 1 -> b
    std::deque<std::pair<int, std::vector<uint8_t>>> q;

    Pair(pl::ClusterKeys x, pl::ClusterKeys y, uint64_t incA = 10, uint64_t incB = 20)
        : a(1, 2, incA, x), b(2, 1, incB, y) {}

    void absorb(int producedBy, pl::PeerHandshake::Out o) {
        auto& logs = producedBy == 0 ? logsA : logsB;
        if (o.log) logs.push_back(*o.log);
        if (o.established) (producedBy == 0 ? ka : kb) = o.established;
        if (o.frame) { q.emplace_back(1 - producedBy, *o.frame); trace.emplace_back(1 - producedBy, *o.frame); }
    }

    void deliver(int to, const std::vector<uint8_t>& f, int64_t now) {
        pl::PeerHandshake& h = to == 0 ? a : b;
        switch (pl::peekPeerMsg(f.data(), f.size())) {
            case 1: { pl::PeerHello m;     ASSERT_TRUE(pl::PeerHello::decode(f.data(), f.size(), m));     absorb(to, h.onHello(m, now)); break; }
            case 2: { pl::PeerChallenge m; ASSERT_TRUE(pl::PeerChallenge::decode(f.data(), f.size(), m)); absorb(to, h.onChallenge(m, now)); break; }
            case 3: { pl::PeerProof m;     ASSERT_TRUE(pl::PeerProof::decode(f.data(), f.size(), m));     absorb(to, h.onProof(m, now)); break; }
            case 4: { pl::PeerAccept m;    ASSERT_TRUE(pl::PeerAccept::decode(f.data(), f.size(), m));    absorb(to, h.onAccept(m, now)); break; }
            case 5: { pl::PeerRefuse m;    ASSERT_TRUE(pl::PeerRefuse::decode(f.data(), f.size(), m));    absorb(to, h.onRefuse(m, now)); break; }
            default: FAIL() << "unknown tag";
        }
    }

    void run(int64_t now = 0) {
        absorb(0, a.tick(now));
        absorb(1, b.tick(now));
        for (int guard = 0; !q.empty() && guard < 64; ++guard) {
            auto [to, f] = q.front(); q.pop_front();
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
    EXPECT_TRUE(std::holds_alternative<pl::Refused>(p.a.state()));
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
    ASSERT_TRUE(r.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(r.frame->data(), r.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::BadProof);
    EXPECT_FALSE(std::holds_alternative<pl::Established>(fresh.state()));
    ASSERT_TRUE(r.log);
    EXPECT_TRUE(r.logIsError);

    // An older incarnation than the one established: Refuse{StaleIncarnation}; the live link is untouched.
    pl::PeerHello old = hello; old.incarnation = 9;
    auto s = p.b.onHello(old, 0);
    ASSERT_TRUE(s.frame);
    ASSERT_TRUE(pl::PeerRefuse::decode(s.frame->data(), s.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::StaleIncarnation);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
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
    ASSERT_TRUE(r.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(r.frame->data(), r.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::BadProof);
}

TEST(PeerHandshakeRegression, RefusedRetriesEvery200ms) {
    pl::PeerHandshake a(1, 2, 10, keys('k')), b(2, 1, 20, keys('x'));
    auto h = a.tick(0); ASSERT_TRUE(h.frame);
    pl::PeerHello first; ASSERT_TRUE(pl::PeerHello::decode(h.frame->data(), h.frame->size(), first));
    auto r = b.onHello(first, 1 * kMs); ASSERT_TRUE(r.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(r.frame->data(), r.frame->size(), ref));
    (void)a.onRefuse(ref, 5 * kMs);
    ASSERT_TRUE(std::holds_alternative<pl::Refused>(a.state()));
    EXPECT_FALSE(a.tick(204 * kMs + 999'999).frame) << "nothing before +200 ms";
    auto again = a.tick(205 * kMs);
    ASSERT_TRUE(again.frame) << "a fresh Hello at +200 ms";
    pl::PeerHello second; ASSERT_TRUE(pl::PeerHello::decode(again.frame->data(), again.frame->size(), second));
    EXPECT_NE(second.nonceA, first.nonceA) << "a new nonce";
    EXPECT_TRUE(std::holds_alternative<pl::HelloSent>(a.state()));
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

    // Afterwards an OLDER incarnation never displaces the live link — as Hello at the responder ...
    pl::PeerHello oldHello; oldHello.from = 1; oldHello.to = 2; oldHello.keyId = keys('k').current.keyId; oldHello.incarnation = 9;
    auto s = p.b.onHello(oldHello, 3'000 * kMs);
    ASSERT_TRUE(s.frame);
    pl::PeerRefuse ref; ASSERT_TRUE(pl::PeerRefuse::decode(s.frame->data(), s.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::StaleIncarnation);
    // ... and as Challenge at the initiator.
    pl::PeerChallenge oldCh; oldCh.from = 2; oldCh.to = 1; oldCh.keyId = keys('k').current.keyId; oldCh.incarnation = 19;
    auto t = p.a.onChallenge(oldCh, 3'000 * kMs);
    ASSERT_TRUE(t.frame);
    ASSERT_TRUE(pl::PeerRefuse::decode(t.frame->data(), t.frame->size(), ref));
    EXPECT_EQ(ref.reason, pl::RefuseReason::StaleIncarnation);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.b.state()));
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

TEST(PeerHandshakeRegression, AnUnknownEpochFrameTriggersARehandshake) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const auto firstHello = lastOf<pl::PeerHello>(p, 1);
    p.ka.reset(); p.kb.reset();
    p.a.onUnknownEpoch(7, 5'000 * kMs);
    auto o = p.a.tick(5'000 * kMs);
    ASSERT_TRUE(o.frame) << "a fresh Hello at once";
    pl::PeerHello again; ASSERT_TRUE(pl::PeerHello::decode(o.frame->data(), o.frame->size(), again));
    EXPECT_NE(again.nonceA, firstHello.nonceA);
    p.q.emplace_back(1, *o.frame);
    p.run(5'000 * kMs);
    ASSERT_TRUE(p.ka && p.kb);
    EXPECT_EQ(p.ka->epoch, 2u);
    EXPECT_EQ(p.kb->epoch, 2u);
}

TEST(PeerHandshakeRegression, TheCurrentEpochNamedAsUnknownIsIgnored) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka);
    p.a.onUnknownEpoch(p.ka->epoch, 5'000 * kMs);
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

TEST(PeerHandshakeRegression, AnUnprovenHelloNeverRaisesTheIncarnationHighWater) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    pl::PeerHello huge = lastOf<pl::PeerHello>(p, 1); huge.incarnation = ~uint64_t{0} >> 1;   // never followed by a Proof
    (void)p.b.onHello(huge, 10 * kMs);
    p.ka.reset(); p.kb.reset();
    // The real initiator (incarnation 10) can still re-handshake: the bogus Hello proved nothing.
    p.q.clear();
    p.a.onUnknownEpoch(42, 3'000 * kMs);
    p.run(3'000 * kMs);
    EXPECT_TRUE(p.ka && p.kb);
}

TEST(PeerHandshakeRegression, ADuplicateHelloGetsTheSameChallengeAndAnUnansweredOneExpiresToThePriorLink) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    const auto* before = std::get_if<pl::Established>(&p.b.state());
    ASSERT_NE(before, nullptr);
    const pl::LinkKeys liveKeys = before->keys;

    pl::PeerHello hello = lastOf<pl::PeerHello>(p, 1);
    hello.nonceA.fill(0x42);
    auto c1 = p.b.onHello(hello, 100 * kMs);
    auto c2 = p.b.onHello(hello, 150 * kMs);
    ASSERT_TRUE(c1.frame && c2.frame);
    EXPECT_EQ(*c1.frame, *c2.frame) << "idempotent for a repeated Hello";
    EXPECT_TRUE(std::holds_alternative<pl::ChallengeSent>(p.b.state()));
    (void)p.b.tick(100 * kMs + pl::PeerHandshake::kAwaitNs + 1);   // nobody proved anything
    const auto* after = std::get_if<pl::Established>(&p.b.state());
    ASSERT_NE(after, nullptr) << "the live link is restored";
    EXPECT_EQ(after->keys, liveKeys);
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

TEST(PeerHandshakeRegression, AStaleNoLinkRightAfterEstablishmentIsIgnored) {
    Pair p(keys('k'), keys('k')); p.run(1'000 * kMs);
    ASSERT_TRUE(p.ka);
    pl::PeerRefuse poke; poke.from = 2; poke.to = 1; poke.reason = pl::RefuseReason::NoLink;
    (void)p.a.onRefuse(poke, 1'000 * kMs + 10 * kMs);
    EXPECT_TRUE(std::holds_alternative<pl::Established>(p.a.state()));
    (void)p.a.onRefuse(poke, 1'000 * kMs + pl::PeerHandshake::kNoLinkGuardNs + kMs);
    EXPECT_TRUE(std::holds_alternative<pl::LinkIdle>(p.a.state()));
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
