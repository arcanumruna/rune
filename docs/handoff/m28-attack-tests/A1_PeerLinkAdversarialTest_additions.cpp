// TARGET FILE:   broker/libs/peerlink/tests/PeerLinkAdversarialTest.cpp   (append at the end; uses Sim, popHs, estOf, kMs, OR)
// TARGET BINARY: rune_peerlink_tests      (ctest -R 'PeerLinkAdversarialRegression' )
// Adversarial review of PR #619 (#584 deferral).  Pure: injected clock, no sleeps, capped loops.

// ---- A1 (finding F1, EXPECTED RED on ccc8a12) Protocol state / replay: the Proof that the #584 deferral answered ----
// with the pending Accept is never consumed (no freshCookie_, no v1 slot clear), so an on-path replay of it after the
// initiator gave up on that exchange mints an orphan epoch E+1 nobody will confirm, which then defers the initiator's
// NEXT real exchange for a further kAwaitNs.  A deferred exchange can never complete, so its Proof must be voided.
// Mutation (proves the fix): the fix = `for (auto& c : pending_) {c.used=false; c.ephPriv.scrub();} freshCookie_(nowNs);`
// inside the deferral branch of establishAsResponder_; removing it again -> red.
TEST(PeerLinkAdversarialRegression, ADeferredProofReplayedAfterTheInitiatorGaveUpMintsNoOrphanEpochAdversarialRegression) {
    Sim s(6193);
    s.link();
    ASSERT_TRUE(s.a.crypto.hasLink(2) && s.b.crypto.hasLink(1));
    s.now += 2'000 * kMs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));                       // exchange 1 -> E1, its Confirm lost
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::ProofV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::Accept));
    (void)popHs(s, 1, pl::PeerMsg::Confirm);
    const auto* eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    const uint32_t e1 = eb->keys.epoch;

    s.now += pl::PeerHandshake::kRetryNs;                           // exchange 2, inside B's window: deferred
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    const Bytes proof2 = popHs(s, 1, pl::PeerMsg::ProofV2);         // the attacker records it
    s.deliverHs(1, proof2);
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::Accept));               // E1's Accept again -> A answers E1's Confirm
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::Confirm));
    ASSERT_TRUE(s.failure.empty()) << s.failure;
    ASSERT_EQ(s.b.crypto.currentEpoch(1), e1);
    ASSERT_TRUE(std::holds_alternative<pl::ChallengeSent>(s.a.hs.state()));

    s.now += pl::PeerHandshake::kAwaitNs + 50 * kMs;                // A gives exchange 2 up (still < 2 kAwaitNs of the cookie)
    s.tick();
    ASSERT_NE(estOf(s.a.hs), nullptr);
    ASSERT_EQ(s.a.crypto.currentEpoch(2), e1);
    s.hsTo[0].clear(); s.hsTo[1].clear();

    pl::PeerProofV2 p;
    ASSERT_TRUE(pl::PeerProofV2::decode(proof2.data(), proof2.size(), p));
    auto o = s.b.hs.onProofV2(p, s.now);                            // the replay
    EXPECT_FALSE(o.established) << "a deferred (never completable) Proof replayed later must mint nothing";
    eb = estOf(s.b.hs);
    ASSERT_TRUE(eb);
    EXPECT_EQ(eb->keys.epoch, e1);
    EXPECT_FALSE(eb->unconfirmedAccept) << "no orphan unconfirmed epoch to defer the next real exchange against";
    s.absorb(1, std::move(o));

    // The initiator's next real exchange (the gate's rate) is not deferred against an orphan: it completes at once.
    s.now += pl::PeerHandshake::kRetryNs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    ASSERT_TRUE(s.converge()) << s.describe();
    EXPECT_GT(s.a.crypto.currentEpoch(2), e1) << "the next exchange completed instead of stalling for kAwaitNs";
    EXPECT_EQ(s.a.crypto.currentEpoch(2), s.b.crypto.currentEpoch(1));
}

namespace {
/// One full same-incarnation exchange from a FRESH initiator object (incarnation `inc`) into responder `b`, up to and
/// including b's answer to the Proof.  Lets a test drive Proofs at any cadence without the real initiator's give-up.
pl::PeerHandshake::Out proofInto(pl::PeerHandshake& b, uint64_t inc, int64_t now) {
    pl::PeerHandshake a(1, 2, inc, keysOf('k'));
    auto h = a.tick(now);                                           // LinkIdle initiator: a fresh HelloV2
    pl::PeerHelloV2 hello;
    if (!h.frame || !pl::PeerHelloV2::decode(h.frame->data(), h.frame->size(), hello)) { ADD_FAILURE() << "no hello"; return {}; }
    auto c = b.onHelloV2(hello, now);
    pl::PeerChallengeV2 ch;
    if (!c.frame || !pl::PeerChallengeV2::decode(c.frame->data(), c.frame->size(), ch)) { ADD_FAILURE() << "no challenge"; return {}; }
    auto pr = a.onChallengeV2(ch, now);
    pl::PeerProofV2 proof;
    if (!pr.frame || !pl::PeerProofV2::decode(pr.frame->data(), pr.frame->size(), proof)) { ADD_FAILURE() << "no proof"; return {}; }
    return b.onProofV2(proof, now);
}
} // namespace

// ---- A2 Protocol state / timing: the deferral window is anchored at the unconfirmed epoch's install and is NEVER ----
// extended by further Proofs.  A same-incarnation Proof every 100 ms is deferred (the same Accept, no epoch) until
// kAwaitNs after the install; the first Proof at or after it mints E+1.  The Accept re-send also stops there.
// Mutation: refreshing `cur->sinceNs = nowNs` in the deferral branch (a "keep alive" of the window) -> the Proof at
// install + kAwaitNs is still deferred -> red.  Mutation 2: dropping `nowNs - cur->sinceNs < kAwaitNs` -> red.
TEST(PeerLinkAdversarialRegression, TheDeferralWindowIsNeverExtendedByRepeatedProofsAdversarialRegression) {
    Sim s(6194);
    s.link();
    s.now += 2'000 * kMs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::ProofV2));
    const Bytes accept1 = popHs(s, 0, pl::PeerMsg::Accept);         // lost: E1 stays unconfirmed
    const auto* eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    const uint32_t e1 = eb->keys.epoch;
    const int64_t installNs = eb->sinceNs;
    int deferred = 0;
    for (int k = 1; k < 10; ++k) {                                  // install + 100 ms ... + 900 ms
        s.now = installNs + k * 100 * kMs;
        auto o = proofInto(s.b.hs, 10, s.now);
        ASSERT_FALSE(o.established) << "Proof at +" << k * 100 << " ms must be deferred";
        ASSERT_TRUE(o.frame);
        EXPECT_EQ(Bytes(o.frame->data(), o.frame->data() + o.frame->size()), accept1);
        ++deferred;
    }
    EXPECT_EQ(deferred, 9);
    s.now = installNs + pl::PeerHandshake::kAwaitNs;
    {   // the window closed: the stale Accept is no longer re-sent ...
        auto t = s.b.hs.tick(s.now);
        EXPECT_FALSE(t.frame) << "no Accept re-send past install + kAwaitNs";
    }
    auto o = proofInto(s.b.hs, 10, s.now);                          // ... and a new Proof mints E1 + 1
    ASSERT_TRUE(o.established) << "the window is anchored at the install, never refreshed by deferred Proofs";
    EXPECT_EQ(o.established->epoch, e1 + 1);
    EXPECT_EQ(o.established->install, pl::LinkInstall::ReceiveOnly);
    eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    EXPECT_EQ(eb->keys.epoch, e1 + 1);
}

// ---- A3 Crash/restart: a RESTARTED initiator (new incarnation) is never deferred behind the old incarnation's ----
// unconfirmed epoch - it holds no old epoch, so it links at once, send-and-receive.
// Mutation: dropping `cur->peerIncarnation == peerIncarnation` from the deferral -> the restarted node gets the old
// incarnation's Accept (which it cannot verify) and no link for kAwaitNs -> red.
TEST(PeerLinkAdversarialRegression, ARestartedInitiatorIsNeverDeferredBehindAnUnconfirmedEpochAdversarialRegression) {
    Sim s(6195);
    s.link();
    s.now += 2'000 * kMs;
    s.absorb(0, s.a.hs.rekeyForTest(s.now));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::HelloV2));
    s.deliverHs(0, popHs(s, 0, pl::PeerMsg::ChallengeV2));
    s.deliverHs(1, popHs(s, 1, pl::PeerMsg::ProofV2));
    (void)popHs(s, 0, pl::PeerMsg::Accept);
    const auto* eb = estOf(s.b.hs);
    ASSERT_TRUE(eb && eb->unconfirmedAccept);
    const uint32_t e1 = eb->keys.epoch;
    s.now += pl::PeerHandshake::kRetryNs;                           // well inside the window
    auto o = proofInto(s.b.hs, /*inc=*/11, s.now);                  // node 1 restarted (incarnation 10 -> 11)
    ASSERT_TRUE(o.established) << "a restarted initiator links at once";
    EXPECT_EQ(o.established->epoch, e1 + 1);
    EXPECT_EQ(o.established->install, pl::LinkInstall::SendAndReceive);
    eb = estOf(s.b.hs);
    ASSERT_TRUE(eb);
    EXPECT_EQ(eb->peerIncarnation, 11u);
    EXPECT_FALSE(eb->unconfirmedAccept);
}
