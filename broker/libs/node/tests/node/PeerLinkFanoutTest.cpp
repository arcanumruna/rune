// PeerLinkFanoutTest.cpp — authentication plan Task 15 (D33): the control thread hands an established link's keys to
// every worker (lifecycle ring) and the admin thread (control -> admin ring); resumable, every epoch in order (#540).
#include <rune/node/PeerLinkFanout.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/WorkerReplicationTransport.h>

#include <gtest/gtest.h>

using namespace rune::node;

namespace {
/// Each worker owns a replication plane built PeerSealed (never attached to Aeron: only its crypto is used here).
struct SealedPool {
    std::unique_ptr<WorkerPool> pool;
    std::vector<std::unique_ptr<rune::replication::WorkerReplicationTransport>> planes;
    explicit SealedPool(int32_t n, std::size_t ringBytes) {
        pool = std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
        for (int32_t w = 0; w < n; ++w) {
            planes.push_back(std::make_unique<rune::replication::WorkerReplicationTransport>(
                w, "aeron:ipc", 600 + w, rune::replication::WorkerReplicationTransport::DeferredAttach{},
                rune::peerlink::PeerFrameCrypto(1, static_cast<uint8_t>(3 + w))));
            pool->worker(w).setReplicationTransport(planes.back().get());
        }
    }
    WorkerPool* operator->() { return pool.get(); }
    WorkerPool& operator*() { return *pool; }
    WorkerPool* get() { return pool.get(); }
};
SealedPool pool(int32_t n, std::size_t ringBytes = 1 << 16) { return SealedPool(n, ringBytes); }
void drainAll(WorkerPool& p) {
    for (int pass = 0; pass < 2; ++pass)
        for (int32_t w = 0; w < p.workerCount(); ++w) p.worker(w).doWork(1);
}
rune::peerlink::LinkKeys keysAt(uint32_t epoch, uint8_t fill,
                                rune::peerlink::LinkInstall install = rune::peerlink::LinkInstall::SendAndReceive) {
    rune::peerlink::LinkKeys k; k.epoch = epoch; k.sendMaster.fill(fill); k.recvMaster.fill(static_cast<uint8_t>(fill + 1));
    k.install = install;
    return k;
}
bool workerHolds(WorkerPool& p, int32_t w, int32_t peer, uint32_t epoch) {
    const auto* t = p.worker(w).replicationTransport();
    return t != nullptr && t->peerCrypto() != nullptr && t->peerCrypto()->holdsEpoch(peer, epoch);
}
/// Every kMsgPeerLinkKeys on the admin ring, decoded.
std::vector<PeerLinkKeysMsg> adminMsgs(OneToOneByteRing& admin) {
    std::vector<PeerLinkKeysMsg> v;
    admin.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        EXPECT_EQ(tid, kMsgPeerLinkKeys);
        PeerLinkKeysMsg m;
        EXPECT_TRUE(decodePeerLinkKeys(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len), m));
        v.push_back(m);
    }, 16);
    return v;
}
} // namespace

TEST(PeerLinkFanoutRegression, KeysReachEveryWorkerAndTheAdminRing) {
    auto p = pool(3);
    OneToOneByteRing admin(1 << 12);
    PeerLinkFanout f;
    f.post(2, keysAt(4, 10));
    EXPECT_GT(f.pump(p.get(), &admin), 0);
    EXPECT_EQ(f.pendingForTest(), 0u) << "delivered everywhere: the entry (and its key bytes) is gone";
    drainAll(*p);
    for (int32_t w = 0; w < 3; ++w) {
        EXPECT_EQ(p->worker(w).peerLinkEpochForTest(2), 4u) << "worker " << w;
        EXPECT_EQ(p->worker(w).peerLinkKeysSeenForTest(), 1u);
    }
    int seen = 0;
    admin.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        ++seen;
        EXPECT_EQ(tid, kMsgPeerLinkKeys);
        PeerLinkKeysMsg m;
        ASSERT_TRUE(decodePeerLinkKeys(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len), m));
        EXPECT_EQ(m.peer, 2); EXPECT_EQ(m.epoch, 4u);
        EXPECT_EQ(m.sendMaster[0], 10); EXPECT_EQ(m.recvMaster[0], 11);
    }, 8);
    EXPECT_EQ(seen, 1);
}

TEST(PeerLinkFanoutRegression, AFullWorkerRingIsRetriedAndOnlyTheMissingDestinationIsResent) {
    auto p = pool(2, /*ringBytes=*/4096);
    OneToOneByteRing admin(1 << 12);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(1).tryEnqueueOp(filler)) {}
    PeerLinkFanout f;
    f.post(3, keysAt(1, 5));
    (void)f.pump(p.get(), &admin);
    EXPECT_EQ(f.pendingForTest(), 1u) << "worker 1 has not got it yet";
    p->worker(1).doWork(1);
    (void)f.pump(p.get(), &admin);
    EXPECT_EQ(f.pendingForTest(), 0u);
    drainAll(*p);
    EXPECT_EQ(p->worker(0).peerLinkKeysSeenForTest(), 1u) << "worker 0 must not get a duplicate";
    EXPECT_EQ(p->worker(1).peerLinkKeysSeenForTest(), 1u);
    EXPECT_EQ(f.deliveredForTest(), 3u);
}

// Issue #540: two re-handshakes complete while a worker's ring is full.  The worker must still install EVERY epoch, in
// order (E2 then E3): the peer's plane may already seal under E2.  Mutation: the old "newest supersedes" post -> the
// worker never holds E2 -> red.
TEST(PeerLinkFanoutRegression, EveryEpochReachesAWorkerInOrderAcrossBackPressureRegression) {
    for (int32_t workers : {1, 2}) {
        auto p = pool(workers, /*ringBytes=*/4096);
        OneToOneByteRing admin(1 << 12);
        PeerLinkFanout f;
        f.post(2, keysAt(1, 1));
        (void)f.pump(p.get(), &admin);
        drainAll(*p);
        (void)adminMsgs(admin);
        WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
        while (p->worker(workers - 1).tryEnqueueOp(filler)) {}
        f.post(2, keysAt(2, 2));
        (void)f.pump(p.get(), &admin);           // the last worker's ring is full
        f.post(2, keysAt(3, 3));
        (void)f.pump(p.get(), &admin);
        EXPECT_EQ(f.pendingForTest(), 2u) << workers << " workers: both epochs are pending";
        for (int i = 0; i < 8 && f.pendingForTest() > 0; ++i) {
            for (int32_t w = 0; w < workers; ++w) p->worker(w).doWork(1);
            (void)f.pump(p.get(), &admin);
        }
        EXPECT_EQ(f.pendingForTest(), 0u);
        drainAll(*p);
        for (int32_t w = 0; w < workers; ++w) {
            EXPECT_TRUE(workerHolds(*p, w, 2, 2)) << "worker " << w << " skipped E2";
            EXPECT_TRUE(workerHolds(*p, w, 2, 3)) << "worker " << w;
            EXPECT_EQ(p->worker(w).peerLinkEpochForTest(2), 3u) << "worker " << w << ": E2 then E3, in order";
            EXPECT_EQ(p->worker(w).peerLinkKeysSeenForTest(), 3u) << "worker " << w << ": E1, E2, E3 once each";
        }
        const auto msgs = adminMsgs(admin);
        ASSERT_EQ(msgs.size(), 2u);
        EXPECT_EQ(msgs[0].epoch, 2u);
        EXPECT_EQ(msgs[1].epoch, 3u);
    }
}

// Issue #540: more than kEpochSlots epochs pending for one peer - a destination would evict the oldest anyway, so the
// fanout drops (scrubs) the oldest pending entry and delivers the newest kEpochSlots in order.
TEST(PeerLinkFanoutRegression, MoreThanKEpochSlotsPendingDropsTheOldestRegression) {
    constexpr uint32_t kSlots = static_cast<uint32_t>(rune::peerlink::PeerFrameCrypto::kEpochSlots);
    auto p = pool(1, /*ringBytes=*/4096);
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    (void)f.pump(p.get(), nullptr);
    drainAll(*p);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}
    for (uint32_t e = 2; e <= 2 + kSlots; ++e) {
        f.post(2, keysAt(e, static_cast<uint8_t>(e)));
        (void)f.pump(p.get(), nullptr);
    }
    EXPECT_EQ(f.pendingForTest(), static_cast<std::size_t>(kSlots)) << "capped at kEpochSlots per peer";
    f.post(3, keysAt(1, 9));                     // another peer is never capped by peer 2's entries
    EXPECT_EQ(f.pendingForTest(), static_cast<std::size_t>(kSlots) + 1);
    for (int i = 0; i < 8 && f.pendingForTest() > 0; ++i) { p->worker(0).doWork(1); (void)f.pump(p.get(), nullptr); }
    EXPECT_EQ(f.pendingForTest(), 0u);
    drainAll(*p);
    EXPECT_FALSE(workerHolds(*p, 0, 2, 2)) << "the oldest pending epoch was dropped";
    for (uint32_t e = 3; e <= 2 + kSlots; ++e) EXPECT_TRUE(workerHolds(*p, 0, 2, e)) << e;
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 2 + kSlots);
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(3), 1u);
    EXPECT_EQ(p->worker(0).peerLinkKeysSeenForTest(), 1u + kSlots + 1u);
}

TEST(PeerLinkFanoutRegression, NoAdminRingNoWorkersStillDrains) {
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    EXPECT_EQ(f.pump(nullptr, nullptr), 0);
    EXPECT_EQ(f.pendingForTest(), 0u);
}

TEST(PeerLinkFanoutRegression, TheAdminWireRoundTripsAndRejectsGarbage) {
    PeerLinkKeysMsg m; m.peer = 7; m.epoch = 9; m.sendMaster.fill(1); m.recvMaster.fill(2);
    uint8_t buf[kPeerLinkKeysWireLen];
    writePeerLinkKeys(buf, m);
    PeerLinkKeysMsg d;
    ASSERT_TRUE(decodePeerLinkKeys(buf, sizeof buf, d));
    EXPECT_EQ(d.peer, 7); EXPECT_EQ(d.epoch, 9u); EXPECT_EQ(d.sendMaster, m.sendMaster); EXPECT_EQ(d.recvMaster, m.recvMaster);
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf - 1, d));
    m.epoch = 0; writePeerLinkKeys(buf, m);
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf, d)) << "epoch 0 means no link";
}

// ---- issue #241: the install mode (receive-only / send-and-receive) reaches every worker and the admin thread ----

TEST(PeerLinkFanoutRegression, TheInstallModeReachesEveryWorkerAndTheAdminMessageRegression) {
    auto p = pool(2);
    OneToOneByteRing admin(1 << 12);
    PeerLinkFanout f;
    f.post(2, keysAt(4, 10));                                                       // the live link
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    f.post(2, keysAt(5, 20, rune::peerlink::LinkInstall::ReceiveOnly));             // a re-handshake, receive-first
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) {
        EXPECT_EQ(p->worker(w).peerLinkEpochForTest(2), 4u) << "worker " << w << ": still SEALS under epoch 4";
        EXPECT_TRUE(workerHolds(*p, w, 2, 5)) << "worker " << w << ": already OPENS epoch 5";
    }
    auto msgs = adminMsgs(admin);
    ASSERT_EQ(msgs.size(), 2u);
    EXPECT_EQ(msgs[0].install, rune::peerlink::LinkInstall::SendAndReceive);
    EXPECT_EQ(msgs[1].epoch, 5u);
    EXPECT_EQ(msgs[1].install, rune::peerlink::LinkInstall::ReceiveOnly);
    // The promotion.
    f.post(2, keysAt(5, 20, rune::peerlink::LinkInstall::SendAndReceive));
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).peerLinkEpochForTest(2), 5u) << "worker " << w;
    msgs = adminMsgs(admin);
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].install, rune::peerlink::LinkInstall::SendAndReceive);
}

TEST(PeerLinkFanoutRegression, ASendAndReceiveSupersedesAnUndeliveredReceiveOnlyOfTheSameEpochRegression) {
    auto p = pool(1, /*ringBytes=*/4096);
    OneToOneByteRing admin(1 << 12);
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    (void)adminMsgs(admin);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}
    f.post(2, keysAt(2, 7, rune::peerlink::LinkInstall::ReceiveOnly));
    (void)f.pump(p.get(), &admin);                                                  // the worker ring is full
    f.post(2, keysAt(2, 7, rune::peerlink::LinkInstall::SendAndReceive));           // promoted before it landed
    EXPECT_EQ(f.pendingForTest(), 1u);
    p->worker(0).doWork(1);
    (void)f.pump(p.get(), &admin);
    EXPECT_EQ(f.pendingForTest(), 0u);
    drainAll(*p);
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 2u) << "the newer install carries the full keys";
    EXPECT_TRUE(workerHolds(*p, 0, 2, 1)) << "the old epoch is still held for frames in flight";
    EXPECT_EQ(p->worker(0).peerLinkKeysSeenForTest(), 2u) << "epoch 1, then epoch 2 once (the receive-only never sent)";
    const auto msgs = adminMsgs(admin);
    ASSERT_FALSE(msgs.empty());
    EXPECT_EQ(msgs.back().epoch, 2u);
    EXPECT_EQ(msgs.back().install, rune::peerlink::LinkInstall::SendAndReceive);
}

TEST(PeerLinkFanoutRegression, TheAdminWireCarriesTheModeAndRefusesAnUnknownOneRegression) {
    EXPECT_EQ(kPeerLinkKeysWireLen, 73u);
    PeerLinkKeysMsg m; m.peer = 3; m.epoch = 2; m.sendMaster.fill(4); m.recvMaster.fill(5);
    m.install = rune::peerlink::LinkInstall::ReceiveOnly;
    uint8_t buf[kPeerLinkKeysWireLen];
    writePeerLinkKeys(buf, m);
    PeerLinkKeysMsg d;
    ASSERT_TRUE(decodePeerLinkKeys(buf, sizeof buf, d));
    EXPECT_EQ(d.install, rune::peerlink::LinkInstall::ReceiveOnly);
    buf[kPeerLinkKeysWireLen - 1] = 2;
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf, d)) << "an unknown install mode is a malformed message";
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf - 1, d)) << "the old 72-byte layout";
}

TEST(PeerLinkFanoutRegression, AWorkerRefusesAnUnknownInstallModeRegression) {
    auto p = pool(1);
    WorkerOp op;
    op.type = WorkerOp::Type::PeerLinkKeys; op.topicId = 2; op.partition = 3;
    op.peerKeys.fill(9);
    op.isolation = 7;                                                               // not a LinkInstall value
    ASSERT_EQ(p->worker(0).enqueueOp(std::move(op)), EnqueueResult::Enqueued);
    drainAll(*p);
    EXPECT_FALSE(workerHolds(*p, 0, 2, 3)) << "refused, never installed";
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 0u);
}
