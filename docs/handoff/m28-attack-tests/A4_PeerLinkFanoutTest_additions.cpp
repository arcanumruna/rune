// TARGET FILE:   broker/libs/node/tests/node/PeerLinkFanoutTest.cpp   (append; uses pool, drainAll, keysAt, workerHolds)
// TARGET BINARY: rune_session_announcer_tests   (ctest -R PeerLinkFanoutRegression)
// Adversarial review of PR #619 (#540 cap).  Deterministic: no clock, capped loops.

// ---- A4 (finding F4, EXPECTED RED on ccc8a12) Data integrity / resources: the per-peer cap must never drop a ----
// SEND-AND-RECEIVE entry (a promotion) while it keeps newer RECEIVE-ONLY ones.  A worker whose ring is wedged while
// E2 is promoted and three later exchanges each install receive-only: dropping the oldest entry (E2 SR) leaves the
// worker sealing under E1 forever (no destination ever evicts its send epoch), while every other plane of this node
// seals under E2 and the peer may have evicted E1.  The cap must drop the oldest RECEIVE-ONLY entry instead.
// Mutation (proves the fix): the fix = in post(), pick the oldest entry with install == ReceiveOnly (falling back to
// the oldest) when count >= kMaxPendingPerPeer; reverting to "drop the oldest entry" -> red.
TEST(PeerLinkFanoutRegression, TheCapNeverDropsAPromotionWhileKeepingReceiveOnlyEntriesAdversarialRegression) {
    using rune::peerlink::LinkInstall;
    auto p = pool(1, /*ringBytes=*/4096);
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));                                        // E1, send-and-receive, delivered
    (void)f.pump(p.get(), nullptr);
    drainAll(*p);
    ASSERT_EQ(p->worker(0).peerLinkEpochForTest(2), 1u);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}                    // the worker's ring is wedged
    f.post(2, keysAt(2, 2, LinkInstall::ReceiveOnly));               // E2 receive-first ...
    f.post(2, keysAt(2, 2, LinkInstall::SendAndReceive));            // ... then promoted (supersedes in place)
    (void)f.pump(p.get(), nullptr);
    for (uint32_t e = 3; e <= 5; ++e) {                             // three later exchanges, each receive-only
        f.post(2, keysAt(e, static_cast<uint8_t>(10 * e), LinkInstall::ReceiveOnly));
        (void)f.pump(p.get(), nullptr);
    }
    EXPECT_LE(f.pendingForTest(), static_cast<std::size_t>(rune::peerlink::PeerFrameCrypto::kEpochSlots));
    for (int i = 0; i < 16 && f.pendingForTest() > 0; ++i) { p->worker(0).doWork(1); (void)f.pump(p.get(), nullptr); }
    ASSERT_EQ(f.pendingForTest(), 0u);
    drainAll(*p);
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 2u)
        << "the worker must seal under the promoted epoch E2, not stay on E1";
    EXPECT_TRUE(workerHolds(*p, 0, 2, 5)) << "the newest receive-only epoch still arrives";
}

// ---- A5 Data integrity: a promotion posted while the receive-only entry of the SAME epoch has already reached some ----
// workers re-sends the full keys to every worker (supersede resets workersDone), in order behind any earlier entry.
// Mutation: superseding without resetting workersDone -> worker 0 keeps E2 receive-only, sealing under E1 -> red.
TEST(PeerLinkFanoutRegression, APromotionAfterAPartialReceiveOnlyDeliveryReachesEveryWorkerAdversarialRegression) {
    using rune::peerlink::LinkInstall;
    auto p = pool(2, /*ringBytes=*/4096);
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    (void)f.pump(p.get(), nullptr);
    drainAll(*p);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(1).tryEnqueueOp(filler)) {}                    // worker 1 wedged, worker 0 free
    f.post(2, keysAt(2, 2, LinkInstall::ReceiveOnly));
    (void)f.pump(p.get(), nullptr);                                 // E2 RO reaches worker 0 only
    drainAll(*p);   // NB: worker 1 drains its fillers here too; re-wedge it so the promotion must be retried
    while (p->worker(1).tryEnqueueOp(filler)) {}
    ASSERT_TRUE(workerHolds(*p, 0, 2, 2));
    ASSERT_EQ(p->worker(0).peerLinkEpochForTest(2), 1u) << "receive-only: still sealing under E1";
    f.post(2, keysAt(2, 2, LinkInstall::SendAndReceive));            // the promotion
    for (int i = 0; i < 16 && f.pendingForTest() > 0; ++i) {
        for (int32_t w = 0; w < 2; ++w) p->worker(w).doWork(1);
        (void)f.pump(p.get(), nullptr);
    }
    ASSERT_EQ(f.pendingForTest(), 0u);
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).peerLinkEpochForTest(2), 2u) << "worker " << w;
}
