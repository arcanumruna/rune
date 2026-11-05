// PeerLinkFanoutTest.cpp — authentication plan Task 15 (D33): the control thread hands an established link's keys to
// every worker (lifecycle ring) and the admin thread (control -> admin ring); resumable, every epoch in order (#540).
#include <rune/node/PeerLinkFanout.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/replication/WorkerReplicationTransport.h>

#include <gtest/gtest.h>

#include <type_traits>

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

// Issue #540: a later epoch of a peer never reaches a worker before an earlier epoch of that peer did.  In production
// worker 1's ring can drain (its own thread) between E1's attempt and E2's attempt inside ONE pump call; the fake pool
// models that: worker 1 answers Full once (for E1) and accepts afterwards.  E2 must still not reach worker 1 first.
// Mutation: PeerLinkFanout::pumpWith `upTo = pool->workerCount()` (ignoring workerLimit) -> worker 1 gets {E2} first.
namespace {
struct FakeWorker {
    std::vector<uint32_t> epochs;
    int fullBudget = 0;
    EnqueueResult enqueueOp(const WorkerOp& op) {
        if (fullBudget > 0) { --fullBudget; return EnqueueResult::Full; }
        epochs.push_back(static_cast<uint32_t>(op.partition));
        return EnqueueResult::Enqueued;
    }
};
struct FakePool {
    FakeWorker workers[2];
    int32_t workerCount() const { return 2; }
    FakeWorker& worker(int32_t w) { return workers[w]; }
};
} // namespace

TEST(PeerLinkFanoutRegression, OrderingLimitHoldsALaterEpochBehindAnEarlierOneRegression) {
    FakePool pool;
    pool.workers[1].fullBudget = 1;
    PeerLinkFanout f;
    f.post(2, keysAt(1, 1));
    f.post(2, keysAt(2, 2));
    (void)f.pumpWith(&pool, nullptr);
    EXPECT_EQ(pool.workers[0].epochs, (std::vector<uint32_t>{1, 2}));
    EXPECT_TRUE(pool.workers[1].epochs.empty()) << "E2 reached worker 1 before E1 did";
    EXPECT_EQ(f.pendingForTest(), 2u);
    (void)f.pumpWith(&pool, nullptr);
    EXPECT_EQ(pool.workers[1].epochs, (std::vector<uint32_t>{1, 2}));
    EXPECT_EQ(pool.workers[0].epochs, (std::vector<uint32_t>{1, 2})) << "no duplicate to worker 0";
    EXPECT_EQ(f.pendingForTest(), 0u);
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
    EXPECT_EQ(kPeerLinkKeysWireLen, 81u);
    PeerLinkKeysMsg m; m.peer = 3; m.epoch = 2; m.sendMaster.fill(4); m.recvMaster.fill(5); m.seq = 0x0102030405060708ULL;
    m.install = rune::peerlink::LinkInstall::ReceiveOnly;
    uint8_t buf[kPeerLinkKeysWireLen];
    writePeerLinkKeys(buf, m);
    PeerLinkKeysMsg d;
    ASSERT_TRUE(decodePeerLinkKeys(buf, sizeof buf, d));
    EXPECT_EQ(d.install, rune::peerlink::LinkInstall::ReceiveOnly);
    EXPECT_EQ(d.seq, 0x0102030405060708ULL);
    buf[kPeerLinkKeysWireLen - 1] = 2;
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf, d)) << "an unknown install mode is a malformed message";
    EXPECT_FALSE(decodePeerLinkKeys(buf, sizeof buf - 1, d)) << "the old 80-byte layout";
    EXPECT_FALSE(decodePeerLinkKeys(buf, 73, d)) << "the pre-sequence 73-byte layout";
}

// The handshake frame that lets a peer seal under a new epoch is held until every plane RAN its keys (a worker drains
// its lifecycle ring once per pass, so an enqueue proves nothing).  Each install carries a sequence number every
// destination sends back; `applied` is true only once every worker and the admin thread acknowledged it or a later one.
// Mutation: `applied` answering from the deliveries (pump) instead of the acknowledgements -> red.
TEST(PeerLinkFanoutRegression, AnInstallIsAppliedOnlyOnceEveryPlaneRanItRegression) {
    auto p = pool(3);
    OneToOneByteRing admin(1 << 12);
    PeerLinkFanout f;
    const uint64_t s1 = f.post(2, keysAt(4, 10, rune::peerlink::LinkInstall::ReceiveOnly));
    EXPECT_GT(f.pump(p.get(), &admin), 0);
    EXPECT_EQ(f.pendingForTest(), 0u) << "delivered to every ring";
    EXPECT_FALSE(f.applied(2, s1, 3, true)) << "delivered is not run: no plane acknowledged yet";
    drainAll(*p);   // every worker runs the op and answers PeerLinkKeysApplied
    std::vector<std::pair<int32_t, uint64_t>> acks;
    p->drainAllReturns([&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::SessionSignal &&
            static_cast<SessionSignalKind>(r.lastOffset) == SessionSignalKind::PeerLinkKeysApplied) {
            EXPECT_EQ(r.sessionId, 2) << "the peer";
            acks.emplace_back(r.partition, static_cast<uint64_t>(r.baseOffset));
        }
    });
    ASSERT_EQ(acks.size(), 3u) << "one acknowledgement per worker";
    for (std::size_t i = 0; i < 2; ++i) f.onWorkerApplied(acks[i].first, 2, acks[i].second);
    EXPECT_FALSE(f.applied(2, s1, 3, true)) << "worker " << acks[2].first << " has not acknowledged";
    f.onWorkerApplied(acks[2].first, 2, acks[2].second);
    EXPECT_FALSE(f.applied(2, s1, 3, true)) << "the admin thread has not acknowledged";
    EXPECT_TRUE(f.applied(2, s1, 3, false)) << "no admin thread: the workers are every plane";
    const auto msgs = adminMsgs(admin);
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].seq, s1) << "the admin message carries the install's sequence number";
    f.onAdminApplied(2, msgs[0].seq);
    EXPECT_TRUE(f.applied(2, s1, 3, true));
    EXPECT_FALSE(f.applied(3, s1, 3, true)) << "another peer's installs are its own";
    EXPECT_TRUE(f.applied(3, s1, 0, false)) << "no plane to wait for";

    // The promotion of the same epoch is a new install (a new number): the old acknowledgements do not cover it.
    const uint64_t s2 = f.post(2, keysAt(4, 10, rune::peerlink::LinkInstall::SendAndReceive));
    EXPECT_GT(s2, s1);
    EXPECT_FALSE(f.applied(2, s2, 3, true));
    // A later acknowledgement covers an earlier install (each plane runs a peer's installs in post order).
    for (int32_t w = 0; w < 3; ++w) f.onWorkerApplied(w, 2, s2 + 1);
    f.onAdminApplied(2, s2 + 1);
    EXPECT_TRUE(f.applied(2, s2, 3, true));
    // An older acknowledgement arriving late never moves a plane back.
    f.onWorkerApplied(1, 2, s1);
    EXPECT_TRUE(f.applied(2, s2, 3, true));
}

// A worker acknowledges a PeerLinkKeys op it refused too (malformed install mode): the go-ahead must not wait forever.
TEST(PeerLinkFanoutRegression, ARefusedInstallIsStillAcknowledgedRegression) {
    auto p = pool(1);
    WorkerOp op;
    op.type          = WorkerOp::Type::PeerLinkKeys;
    op.topicId       = 2;
    op.partition     = 5;
    op.isolation     = 9;   // no such LinkInstall
    op.correlationId = 77;
    ASSERT_EQ(p->worker(0).enqueueOp(op), EnqueueResult::Enqueued);
    drainAll(*p);
    EXPECT_EQ(p->worker(0).peerLinkEpochForTest(2), 0u) << "refused: nothing installed";
    int acks = 0;
    p->drainAllReturns([&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::SessionSignal &&
            static_cast<SessionSignalKind>(r.lastOffset) == SessionSignalKind::PeerLinkKeysApplied) {
            ++acks;
            EXPECT_EQ(r.baseOffset, 77);
        }
    });
    EXPECT_EQ(acks, 1);
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

// Issue #546: the ring zeroes every consumed record (O2ORingBuffer::read -> setMemory), so a consumed PeerLinkKeys
// record leaves no master bytes in the ring; no consumer-side scrub of ring memory is needed.  Non-vacuous: the
// pattern is in the ring before the consumer reads it.  Mutation: removing the setMemory in O2ORingBuffer::read leaves
// the masters behind -> red.
TEST(PeerLinkFanoutRegression, AConsumedKeysRecordLeavesNoMasterBytesInTheRingRegression) {
    constexpr int kCap = 1 << 12;
    OneToOneByteRing admin(kCap);
    PeerLinkFanout f;
    f.post(2, keysAt(5, 0xC3));          // sendMaster = 32 x 0xC3, recvMaster = 32 x 0xC4
    (void)f.pump(nullptr, &admin);
    auto countOf = [&](uint8_t v) {
        std::size_t run = 0, hits = 0;
        const uint8_t* b = admin.base();
        for (int i = 0; i < kCap; ++i) {
            run = b[i] == v ? run + 1 : 0;
            if (run == 32) ++hits;
        }
        return hits;
    };
    EXPECT_EQ(countOf(0xC3), 1u) << "precondition: the send master is in the ring before it is consumed";
    EXPECT_EQ(countOf(0xC4), 1u) << "precondition: the recv master is in the ring before it is consumed";
    const auto msgs = adminMsgs(admin);
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(countOf(0xC3), 0u) << "the consumed record left the send master in the ring";
    EXPECT_EQ(countOf(0xC4), 0u) << "the consumed record left the recv master in the ring";
}

// Issue #546 sibling: Worker::enqueueOp took its WorkerOp BY VALUE, leaving a copy of a PeerLinkKeys op's masters in an
// unscrubbed parameter (the caller can only scrub its own op).  It now takes const& (the encode reads the op and writes
// the ring slot directly: there is no scratch copy), so the caller's scrub covers every copy.  tryEnqueueOp likewise.
// Mutation: either signature back to by-value -> does not compile (red at these asserts).
TEST(PeerLinkFanoutRegression, EnqueueOpNeverCopiesAKeyCarryingOpRegression) {
    static_assert(std::is_same_v<decltype(&Worker::enqueueOp), EnqueueResult (Worker::*)(const WorkerOp&)>,
                  "enqueueOp must not copy the op: its peerKeys would outlive the caller's scrub");
    static_assert(std::is_same_v<decltype(&Worker::tryEnqueueOp), bool (Worker::*)(const WorkerOp&)>,
                  "tryEnqueueOp must not copy the op");
    auto p = pool(1);
    WorkerOp op;
    op.type = WorkerOp::Type::PeerLinkKeys; op.topicId = 2; op.partition = 3;
    op.peerKeys.fill(0x5A);
    ASSERT_EQ(p->worker(0).enqueueOp(op), EnqueueResult::Enqueued);
    // The caller scrubs its (only) copy, exactly as PeerLinkFanout::pump does; nothing else holds the masters.
    std::memset(op.peerKeys.data(), 0, op.peerKeys.size());
    drainAll(*p);
    EXPECT_TRUE(workerHolds(*p, 0, 2, 3)) << "the keys still reached the worker";
}

// Review of 8d726892, adversarial: the promotion of an epoch supersedes its receive-only install after that install
// reached only SOME workers (worker 1 back-pressured).  Worker 0 then runs both (acks s1, then s2), worker 1 only the
// promotion (acks s2).  `applied(s1)` must wait for worker 1's s2 (it holds the epoch only then) and never answer from
// worker 0's acks alone; `applied(s2)` likewise.  Every op carries its own entry's sequence number.
namespace {
struct SeqWorker {
    std::vector<std::pair<uint8_t, uint64_t>> ops;   // (install, seq)
    int fullBudget = 0;
    EnqueueResult enqueueOp(const WorkerOp& op) {
        if (fullBudget > 0) { --fullBudget; return EnqueueResult::Full; }
        ops.emplace_back(op.isolation, static_cast<uint64_t>(op.correlationId));
        return EnqueueResult::Enqueued;
    }
};
struct SeqPool {
    SeqWorker workers[2];
    int32_t workerCount() const { return 2; }
    SeqWorker& worker(int32_t w) { return workers[w]; }
};
} // namespace

TEST(PeerLinkFanoutRegression, APromotionSupersedingAPartlyDeliveredInstallIsAckedPerPlaneAdversarialRegression) {
    using rune::peerlink::LinkInstall;
    SeqPool pool;
    pool.workers[1].fullBudget = 1;
    PeerLinkFanout f;
    const uint64_t s1 = f.post(2, keysAt(4, 1, LinkInstall::ReceiveOnly));
    (void)f.pumpWith(&pool, nullptr);
    ASSERT_EQ(pool.workers[0].ops.size(), 1u);
    ASSERT_TRUE(pool.workers[1].ops.empty());
    const uint64_t s2 = f.post(2, keysAt(4, 1, LinkInstall::SendAndReceive));   // same epoch: supersedes the entry
    ASSERT_GT(s2, s1);
    (void)f.pumpWith(&pool, nullptr);
    EXPECT_EQ(f.pendingForTest(), 0u);
    ASSERT_EQ(pool.workers[0].ops.size(), 2u);
    ASSERT_EQ(pool.workers[1].ops.size(), 1u);
    EXPECT_EQ(pool.workers[0].ops[0].second, s1);
    EXPECT_EQ(pool.workers[0].ops[1].second, s2) << "the superseding entry carries its own sequence number";
    EXPECT_EQ(pool.workers[1].ops[0].second, s2);
    // The workers run them and ack, worker 0 first.
    f.onWorkerApplied(0, 2, s1);
    f.onWorkerApplied(0, 2, s2);
    EXPECT_FALSE(f.applied(2, s1, 2, false)) << "worker 1 holds nothing of the epoch yet";
    f.onWorkerApplied(1, 2, s2);
    EXPECT_TRUE(f.applied(2, s1, 2, false));
    EXPECT_TRUE(f.applied(2, s2, 2, false));
    // An ack for another peer, or a stale one, never satisfies this peer.
    EXPECT_FALSE(f.applied(2, s2 + 1, 2, false));
    f.onWorkerApplied(0, 3, s2 + 5);
    f.onWorkerApplied(1, 3, s2 + 5);
    EXPECT_FALSE(f.applied(2, s2 + 1, 2, false));
}
