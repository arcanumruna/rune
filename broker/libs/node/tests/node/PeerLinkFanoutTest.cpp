// PeerLinkFanoutTest.cpp — authentication plan Task 15 (D33): the control thread hands an established link's keys to
// every worker (lifecycle ring) and the admin thread (control -> admin ring); resumable, newest epoch wins.
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
rune::peerlink::LinkKeys keysAt(uint32_t epoch, uint8_t fill) {
    rune::peerlink::LinkKeys k; k.epoch = epoch; k.sendMaster.fill(fill); k.recvMaster.fill(static_cast<uint8_t>(fill + 1)); return k;
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

TEST(PeerLinkFanoutRegression, ANewerEpochSupersedesAnUndeliveredOlderOne) {
    auto p = pool(1, /*ringBytes=*/4096);
    OneToOneByteRing admin(1 << 12);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    (void)f.pump(p.get(), &admin);
    f.post(2, keysAt(2, 2));                 // the peer restarted before worker 0 took epoch 1
    EXPECT_EQ(f.pendingForTest(), 1u);
    p->worker(0).doWork(1);
    (void)f.pump(p.get(), &admin);
    drainAll(*p);
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 2u);
    EXPECT_EQ(p->worker(0).peerLinkKeysSeenForTest(), 1u) << "the stale epoch was never sent to the worker";
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
