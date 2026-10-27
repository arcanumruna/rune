// RecordEgressLifetimeTest.cpp — the per-(session, worker) record-egress
// publication a worker pushes through must outlive every worker reference to
// it (2026-10-05, DR e2e broker crash).
//
// The gateway hands worker w the RAW pointer of the session's record-egress pub
// (WorkerOp::egressPub); the worker stores it in its subscription table and
// writes through it on every push pass.  closeSession_ used to destroy the pub
// with the session and only THEN tell the workers — a cross-thread
// use-after-free ("Records frame exceeds the egress maxMessageLength 0 B" →
// aeron OutOfBoundsException / SIGSEGV).  Fix: retire-on-ack — the pub stays
// alive until its worker executed the release op (FIFO behind every Subscribe
// that carried it) and acknowledged it (SessionSignal::EgressReleased).
//
// The publication here is a FAKE: a shared_ptr over inert storage with a
// counting deleter.  Neither the gateway nor the worker dereferences it on
// these paths (the logs are empty, so the push loop never reaches a send);
// the deleter count is the lifetime probe.  Deterministic: no sleeps, no
// Aeron — the gateway and the worker duty cycles are driven by hand.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

// Inert stand-in for an aeron::ExclusivePublication: never dereferenced; the
// deleter counts destructions (the lifetime probe).
struct FakePub {
    alignas(64) unsigned char storage[256]{};
    int destroyed = 0;
    std::shared_ptr<aeron::ExclusivePublication> make() {
        return {reinterpret_cast<aeron::ExclusivePublication*>(storage),
                [this](aeron::ExclusivePublication*) { ++destroyed; }};
    }
    [[nodiscard]] aeron::ExclusivePublication* ptr() { return reinterpret_cast<aeron::ExclusivePublication*>(storage); }
};

bool workerReferences(rune::node::Worker& w, aeron::ExclusivePublication* p) {
    for (const auto& s : w.subscriptionsForTest())
        if (s.egressPub == p) return true;
    return false;
}

std::vector<uint8_t> subscribeFrame(int64_t sid, int32_t topicId, int32_t partition) {
    pc::Subscribe s;
    s.sessionId      = sid;
    s.correlationId  = 77;
    s.topicId        = topicId;
    s.partition      = partition;
    s.partitionKnown = 1;
    s.fromOffset     = 0;
    s.isolation      = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    return buf;
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-egress-lifetime-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Gateway + N-worker pool + a 1-topic registry (topic 0, `partitions` partitions),
// per-worker egress active (Subscribe frames are routed to the owning worker).
struct Rig {
    TempDir                         dir;
    std::unique_ptr<TopicRegistry>  registry;
    rune::node::WorkerPool          pool;
    rune::replication::InMemoryTransport transport;   // CP1-I4: worker-owned logs are served via a replica's HW
    rune::node::ReplicaRegistry     replicas;
    ClientGateway                   gw;

    explicit Rig(int32_t workers, int32_t partitions = 1, std::size_t ringBytes = 1 << 16)
        : pool(workers, rune::metadata::ShardRouter(workers), ringBytes, 1 << 16, 16, 16)
        , gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        rune::node::TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = partitions;
        ts.topicId        = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setOnEgressFrame([](int64_t, const uint8_t*, std::size_t) {});
        gw.setWorkerPool(&pool);
        gw.setShardMode(true);
        gw.setPerWorkerEgressActive(true);
        for (int32_t p = 0; p < partitions; ++p) {
            rune::replication::PartitionConfig c;
            c.topicId = 0; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
            c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
            auto rep = std::make_unique<rune::replication::PartitionReplica>(
                c, rune::replication::Role::Leader, 1, std::vector<int32_t>{1}, std::vector<int32_t>{1},
                registry->log(0, p), &transport);
            rep->setExternalFollowerFlush(true);
            replicas.add(0, p, std::move(rep));
        }
        gw.setReplicaRegistry(&replicas);
    }
    void subscribe(int64_t sid, int32_t partition) {
        const auto f = subscribeFrame(sid, 0, partition);
        gw.applySubscribeForTest(f.data(), f.size());
    }
    int32_t owner(int32_t partition) { return pool.ownerWorkerId(0, partition); }
};

} // namespace

// THE crash (RED on 6a67487): a session with a worker-pushed subscription on its
// record-egress pub closes; the worker has not yet seen any drop and runs push
// passes with the pointer.  The pub must stay alive for as long as any worker
// subscription references it, and be destroyed exactly once after the worker
// acknowledged the release.
TEST(RecordEgressLifetimeRegression, CloseKeepsThePubAliveUntilTheWorkerReleasesIt) {
    Rig r(1);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.subscribe(sid, 0);
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);                                              // the worker registers the subscription
    ASSERT_TRUE(workerReferences(w, pub.ptr())) << "the worker pushes through the session's unicast pub";

    r.gw.closeSessionForTest(sid);                            // reap / Disconnect / SessionFailed path
    ASSERT_EQ(r.gw.sessionCount(), 0u);
    // The worker has not processed anything since: it still holds the pointer
    // and every push pass writes through it.  It must still be alive.
    EXPECT_TRUE(workerReferences(w, pub.ptr()));
    EXPECT_EQ(pub.destroyed, 0) << "use-after-free: the gateway destroyed a publication a worker still pushes through";
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 1u);
    r.gw.drainWorkerReturnsForTest(2);                        // nothing acked yet → still alive
    EXPECT_EQ(pub.destroyed, 0);

    w.doWork(3);                                              // release op: drop + acknowledge
    EXPECT_FALSE(workerReferences(w, pub.ptr())) << "the release dropped every subscription on it";
    EXPECT_TRUE(w.subscriptionsForTest().empty()) << "no cursor outlives its session";
    EXPECT_EQ(w.egressReleasesForTest(), 1u);
    EXPECT_EQ(pub.destroyed, 0) << "freed only by the gateway, on the acknowledgement";
    r.gw.drainWorkerReturnsForTest(4);
    EXPECT_EQ(pub.destroyed, 1) << "the acknowledgement destroys it (no leak)";
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u) << "the retiring list drains to 0";
}

// Ordering: the Subscribe carrying the pointer is still IN the worker's op ring
// when the session closes.  The release op is FIFO behind it, so the worker
// registers and then drops the subscription — and the pub is destroyed only
// after both.
TEST(RecordEgressLifetimeRegression, ReleaseRunsBehindASubscribeStillInTheRing) {
    Rig r(1);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.subscribe(sid, 0);                                      // queued on the ring, not executed
    r.gw.closeSessionForTest(sid);
    EXPECT_EQ(pub.destroyed, 0) << "a Subscribe op on the ring carries the pointer";
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);
    EXPECT_TRUE(w.subscriptionsForTest().empty()) << "Subscribe, then the session's Unsubscribe + release";
    EXPECT_EQ(pub.destroyed, 0);
    r.gw.drainWorkerReturnsForTest(2);
    EXPECT_EQ(pub.destroyed, 1);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u);
}

// A release op the worker's full op ring refuses is never dropped: it is
// retried every gateway cycle; the pub lives until the (late) acknowledgement.
TEST(RecordEgressLifetimeRegression, ReleaseRefusedByAFullRingIsRetriedNeverDropped) {
    Rig r(1, 1, /*ringBytes=*/1 << 12);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.subscribe(sid, 0);
    // Fill the worker's op ring (direct Subscribe forwards; extras queue in pendingSubOps_).
    for (int64_t i = 1000; i < 1200 && r.gw.pendingSubOpsForTest() == 0; ++i)
        r.gw.forwardSubscribeForTest(i, /*sessionId=*/0, 0, 0, 0, 0, false, false);
    ASSERT_GT(r.gw.pendingSubOpsForTest(), 0u) << "the ring is full";
    r.gw.closeSessionForTest(sid);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 1u);
    EXPECT_EQ(r.gw.retiringRecordEgressUnsentForTest(), 1u) << "the full ring refused the release op";
    auto& w = r.pool.worker(r.owner(0));
    const int cap = 64;
    for (int i = 0; i < cap && (r.gw.retiringRecordEgressCountForTest() != 0); ++i) {
        EXPECT_EQ(pub.destroyed, 0) << "pass " << i;
        w.doWork(10 + i);
        r.gw.drainWorkerReturnsForTest(10 + i);
        (void)r.gw.pumpRetryQueuesForTest();
    }
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u) << "the release got through and was acknowledged";
    EXPECT_EQ(pub.destroyed, 1);
    EXPECT_FALSE(workerReferences(w, pub.ptr()));
}

// Two workers, one pub each: each pub is freed by ITS worker's acknowledgement
// only; an acknowledgement never frees another worker's pub.
TEST(RecordEgressLifetimeRegression, EachWorkersPubIsFreedByItsOwnAcknowledgement) {
    Rig r(2, /*partitions=*/2);
    ASSERT_NE(r.owner(0), r.owner(1));
    FakePub p0, p1;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), p0.make());
    r.gw.injectRecordEgressForTest(sid, r.owner(1), p1.make());
    r.subscribe(sid, 0);
    r.subscribe(sid, 1);
    auto& w0 = r.pool.worker(r.owner(0));
    auto& w1 = r.pool.worker(r.owner(1));
    w0.doWork(1);
    w1.doWork(1);
    ASSERT_TRUE(workerReferences(w0, p0.ptr()));
    ASSERT_TRUE(workerReferences(w1, p1.ptr()));
    r.gw.closeSessionForTest(sid);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 2u);
    w0.doWork(2);                                             // only worker 0 acknowledges
    r.gw.drainWorkerReturnsForTest(2);
    EXPECT_EQ(p0.destroyed, 1);
    EXPECT_EQ(p1.destroyed, 0) << "worker 1 still pushes through its pub";
    EXPECT_TRUE(workerReferences(w1, p1.ptr()));
    w1.doWork(3);
    r.gw.drainWorkerReturnsForTest(3);
    EXPECT_EQ(p1.destroyed, 1);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u);
}

// Re-resolve: a resolve completing for a (session, worker) that already has a
// cached pub a worker holds must NOT replace it (replacing destroyed the held
// one).  The extra pub dies unhanded; the buffered subscribe gets the cached one.
TEST(RecordEgressLifetimeRegression, AResolveNeverReplacesAPubAWorkerHolds) {
    Rig r(1);
    FakePub held, extra;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), held.make());
    r.subscribe(sid, 0);
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);
    ASSERT_TRUE(workerReferences(w, held.ptr()));
    r.gw.resolveRecordEgressForTest(sid, r.owner(0), extra.make(), /*subId=*/5000, 0, 0);
    EXPECT_EQ(held.destroyed, 0) << "the held pub is never replaced";
    EXPECT_EQ(extra.destroyed, 1) << "the extra resolve is released unhanded";
    w.doWork(2);
    EXPECT_FALSE(workerReferences(w, extra.ptr())) << "no worker ever got the extra pointer";
    EXPECT_EQ(w.subscriptionsForTest().size(), 2u);
    for (const auto& s : w.subscriptionsForTest()) EXPECT_EQ(s.egressPub, held.ptr());
    // Close: one release frees the held pub after both subscriptions are dropped.
    r.gw.closeSessionForTest(sid);
    w.doWork(3);
    r.gw.drainWorkerReturnsForTest(3);
    EXPECT_EQ(held.destroyed, 1);
    EXPECT_TRUE(w.subscriptionsForTest().empty());
}

// A resolve completing after its session closed hands NO pointer to a worker
// (it used to hand the raw pointer of a pub destroyed at the end of the scope).
TEST(RecordEgressLifetimeRegression, AResolveAfterTheSessionClosedHandsNothingOut) {
    Rig r(1);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.closeSessionForTest(sid);
    r.gw.resolveRecordEgressForTest(sid, r.owner(0), pub.make(), /*subId=*/6000, 0, 0);
    EXPECT_EQ(pub.destroyed, 1);
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);
    EXPECT_FALSE(workerReferences(w, pub.ptr())) << "a destroyed pub's pointer reached a worker";
    EXPECT_TRUE(w.subscriptionsForTest().empty());
}

// Session close also drops the worker cursors of its MDC (no unicast pub)
// subscriptions — they used to outlive the session and push forever.
TEST(RecordEgressLifetimeRegression, CloseDropsTheSessionsWorkerCursors) {
    Rig r(1);
    const int64_t sid = r.gw.injectTestSession(0);
    const int64_t other = r.gw.injectTestSession(0);
    r.subscribe(sid, 0);                                      // no record-egress pub → MDC
    r.subscribe(other, 0);
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 2u);
    r.gw.closeSessionForTest(sid);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u) << "no unicast pub to retire";
    w.doWork(2);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_EQ(w.subscriptionsForTest().front().sessionId, other);
}

// Shutdown with pubs still retiring (no acknowledgement ever came — the
// workers are joined first in BrokerNode::stop): the gateway's destruction
// releases each exactly once.
TEST(RecordEgressLifetimeRegression, ShutdownWithRetiringPubsReleasesEachOnce) {
    FakePub a, b;
    {
        Rig r(2, 2);
        const int64_t s1 = r.gw.injectTestSession(0);
        const int64_t s2 = r.gw.injectTestSession(0);
        r.gw.injectRecordEgressForTest(s1, r.owner(0), a.make());
        r.gw.injectRecordEgressForTest(s2, r.owner(1), b.make());
        r.subscribe(s1, 0);
        r.subscribe(s2, 1);
        r.gw.closeSessionForTest(s1);
        r.gw.closeSessionForTest(s2);
        EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 2u);
        EXPECT_EQ(a.destroyed + b.destroyed, 0);
    }
    EXPECT_EQ(a.destroyed, 1);
    EXPECT_EQ(b.destroyed, 1);
}

// An acknowledgement naming an unknown token (or the wrong worker) frees nothing.
TEST(RecordEgressLifetimeRegression, AnUnknownAcknowledgementFreesNothing) {
    Rig r(2, 2);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.gw.closeSessionForTest(sid);
    ASSERT_EQ(r.gw.retiringRecordEgressCountForTest(), 1u);
    rune::node::WorkerReturn ack;
    ack.type       = rune::node::WorkerReturn::Type::SessionSignal;
    ack.sessionId  = sid;
    ack.lastOffset = static_cast<int64_t>(rune::node::SessionSignalKind::EgressReleased);
    ack.partition  = r.owner(1);                              // wrong worker
    ack.baseOffset = 1;
    r.gw.handleWorkerReturnForTest(ack);
    ack.partition  = r.owner(0);
    ack.baseOffset = 999;                                     // unknown token
    r.gw.handleWorkerReturnForTest(ack);
    EXPECT_EQ(pub.destroyed, 0);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 1u);
    r.pool.worker(r.owner(0)).doWork(1);
    r.gw.drainWorkerReturnsForTest(1);
    EXPECT_EQ(pub.destroyed, 1);
}

// Review MEDIUM-2: the closing session's OWN Subscribe is waiting in
// pendingSubOps_ (ring full) when the session closes.  The queued entry must
// never carry the pointer (it re-resolves at send time → the session is gone →
// moot), so the worker never references the pub and it is freed exactly once.
// A future change that stores the pointer in the queue turns this RED.
TEST(RecordEgressLifetimeRegression, AQueuedSubscribeOfTheClosedSessionNeverCarriesItsPub) {
    Rig r(1, 1, /*ringBytes=*/1 << 12);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    for (int64_t i = 1000; i < 1200 && r.gw.pendingSubOpsForTest() == 0; ++i)
        r.gw.forwardSubscribeForTest(i, /*sessionId=*/0, 0, 0, 0, 0, false, false);
    ASSERT_GT(r.gw.pendingSubOpsForTest(), 0u) << "the ring is full";
    const std::size_t before = r.gw.pendingSubOpsForTest();
    r.subscribe(sid, 0);
    ASSERT_GT(r.gw.pendingSubOpsForTest(), before) << "the session's Subscribe is queued, not on the ring";
    r.gw.closeSessionForTest(sid);
    auto& w = r.pool.worker(r.owner(0));
    for (int i = 0; i < 128 && (r.gw.retiringRecordEgressCountForTest() != 0 || r.gw.pendingSubOpsForTest() != 0); ++i) {
        w.doWork(10 + i);
        EXPECT_FALSE(workerReferences(w, pub.ptr())) << "a queued Subscribe handed the closed session's pub out (pass " << i << ")";
        r.gw.drainWorkerReturnsForTest(10 + i);
        (void)r.gw.pumpRetryQueuesForTest();
    }
    EXPECT_EQ(r.gw.pendingSubOpsForTest(), 0u);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u);
    EXPECT_EQ(pub.destroyed, 1);
}

// D10-L (review LOW-4, lows batch C): the NON-shard runtime (per-worker egress without worker-side acks) closes
// sessions through the same closeSession_: the pub a worker pushes through is retired on ack there too.
TEST(RecordEgressLifetimeRegression, NonShardModeCloseKeepsThePubAliveUntilTheWorkerReleasesIt) {
    Rig r(1);
    r.gw.setShardMode(false);
    FakePub pub;
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.subscribe(sid, 0);
    auto& w = r.pool.worker(r.owner(0));
    w.doWork(1);
    ASSERT_TRUE(workerReferences(w, pub.ptr()));
    r.gw.closeSessionForTest(sid);
    EXPECT_TRUE(workerReferences(w, pub.ptr()));
    EXPECT_EQ(pub.destroyed, 0) << "use-after-free in the non-shard runtime";
    r.gw.drainWorkerReturnsForTest(2);
    EXPECT_EQ(pub.destroyed, 0);
    w.doWork(3);
    EXPECT_FALSE(workerReferences(w, pub.ptr()));
    EXPECT_EQ(pub.destroyed, 0) << "freed only on the acknowledgement";
    r.gw.drainWorkerReturnsForTest(4);
    EXPECT_EQ(pub.destroyed, 1);
    EXPECT_EQ(r.gw.retiringRecordEgressCountForTest(), 0u);
}

// Auth Task 18 (seeded stress): a revoke ends a subscription whose Subscribe still waits for the session's record-egress
// pub.  The unsubscribe must cancel the waiter - else the pub's resolution enqueues the Subscribe to the worker, a zombie
// cursor the gateway no longer knows (it pushed after the revoke, and again after a re-grant of READ).
TEST(RecordEgressLifetimeRegression, UnsubscribeCancelsASubscribeStillAwaitingItsEgressPub) {
    Rig r(1);
    const int64_t sid = r.gw.injectTestSession(0);
    auto& w = r.pool.worker(r.owner(0));
    r.gw.addSubscriptionAwaitingEgressForTest(/*subId=*/41, sid, 0, 0);
    ASSERT_TRUE(r.gw.subscriptionAwaitsEgressForTest(41));
    ASSERT_EQ(r.gw.pendingEgressWaitersForTest(), 1);

    r.gw.forwardUnsubscribeForTest(41, 0, 0);                  // the revoke ends it while the pub is still pending
    EXPECT_EQ(r.gw.pendingEgressWaitersForTest(), 0) << "the waiter is cancelled where it waits";
    EXPECT_FALSE(r.gw.subscriptionAwaitsEgressForTest(41));

    r.gw.pollPendingRecordEgressForTest(1);                    // the pub resolves (here: the MDC fallback drain)
    r.gw.pumpRetryQueuesForTest();
    w.doWork(1);
    w.doWork(2);
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u) << "the pending resolve drained";
    EXPECT_EQ(r.gw.pendingSubOpsForTest(), 0u) << "no Subscribe op was queued for the ended subscription";
    EXPECT_TRUE(w.subscriptionsForTest().empty()) << "the worker holds no cursor for an ended subscription";
}

// The positive control of the test above: without an unsubscribe the same parked Subscribe DOES reach the worker.
TEST(RecordEgressLifetimeRegression, ASubscribeAwaitingItsEgressPubReachesTheWorkerOnceItResolves) {
    Rig r(1);
    const int64_t sid = r.gw.injectTestSession(0);
    auto& w = r.pool.worker(r.owner(0));
    r.gw.addSubscriptionAwaitingEgressForTest(/*subId=*/42, sid, 0, 0);
    r.gw.pollPendingRecordEgressForTest(1);
    r.gw.pumpRetryQueuesForTest();
    w.doWork(1);
    w.doWork(2);
    EXPECT_FALSE(r.gw.subscriptionAwaitsEgressForTest(42));
    EXPECT_EQ(w.subscriptionsForTest().size(), 1u) << "the harness path delivers (so the cancel test is not vacuous)";
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #118 (X020, combined-fix M10): under authentication a Subscribe NEVER falls back to the worker's shared
// dynamic-MDC publication - every party joined to that worker's control port would receive the Records, READ or not.
// A Subscribe whose per-session record egress cannot be created or resolved is retired retriably instead (the position
// note + NotLeader on its correlation id: the client re-subscribes from its position), and the worker never holds a
// cursor without its own egress.  Outside authentication the MDC fallback stays (the tests above).
// ─────────────────────────────────────────────────────────────────────────────
namespace {

struct AuthRigEgress : Rig {
    std::vector<pc::ErrorResponse> errors;
    explicit AuthRigEgress(int32_t workers = 1) : Rig(workers) {
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            const auto hdr = pc::MessageHeader::decode(b, n);
            if (hdr && hdr->templateId == static_cast<uint16_t>(pc::TemplateId::ErrorResponse))
                if (auto e = pc::ErrorResponse::decode(b, n)) errors.push_back(*e);
            if (hdr && hdr->templateId == static_cast<uint16_t>(pc::TemplateId::SubscribeResponse))   // PR #441 F4
                if (auto sr = pc::SubscribeResponse::decode(b, n); sr && sr->errorCode != 0) {
                    pc::ErrorResponse e;
                    e.correlationId = sr->correlationId;
                    e.code          = sr->errorCode;
                    errors.push_back(e);
                }
        });
        gw.setAuthMode(GatewayAuthMode::Required, nullptr);
    }
    int64_t authenticatedSession() {
        const int64_t sid = gw.injectTestSession(0);
        gw.setSessionAuthForTest(sid, AuthAuthenticated{1, "reader", PermissionTable::allowAll(), {}});
        return sid;
    }
    [[nodiscard]] int errorOn(int64_t corr) const {
        for (const auto& e : errors) if (e.correlationId == corr) return e.code;
        return -1;
    }
    [[nodiscard]] static bool anyCursorWithoutItsOwnEgress(rune::node::Worker& w) {
        for (const auto& s : w.subscriptionsForTest()) if (s.egressPub == nullptr) return true;
        return false;
    }
};

}  // namespace

// The issue's repro at the gateway: consumer A (READ) subscribes, its unicast record egress cannot be resolved (the
// session has no egress endpoint here, the same path as a failed add or a resolve timeout).  RED before the fix: the
// Subscribe reached the worker with egressPub = null - the worker pushed A's topic on the shared MDC publication that
// every joiner of the worker's advertised control port (B, without READ) receives.
TEST(AuthEnforcementRegression, NoRecordIsEverBroadcastToAnUnauthorizedSubscriberRegression) {
    AuthRigEgress r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticatedSession();
    r.subscribe(sid, 0);                                       // corr 77
    for (int pass = 0; pass < 4; ++pass) {
        r.gw.pollPendingRecordEgressForTest(1 + pass);
        r.gw.pumpRetryQueuesForTest();
        w.doWork(1 + pass);
    }
    EXPECT_FALSE(AuthRigEgress::anyCursorWithoutItsOwnEgress(w)) << "a cursor on the shared MDC egress broadcasts";
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_EQ(r.errorOn(77), static_cast<int>(pc::ErrorCode::RecordEgressUnavailable))
        << "the subscription is retired retriably on its correlation id, never left silent";
    EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "the gateway holds no subscription it cannot serve";
}

// The same rule on the deferred path: a Subscribe parked for its egress pub whose resolve fails (regId -1 / timeout).
TEST(AuthEnforcementRegression, AFailedRecordEgressResolveNeverFallsBackToTheSharedEgressRegression) {
    AuthRigEgress r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticatedSession();
    r.gw.addSubscriptionAwaitingEgressForTest(/*subId=*/43, sid, 0, 0);
    r.gw.pollPendingRecordEgressForTest(1);                    // the resolve failed: the fallback branch
    r.gw.pumpRetryQueuesForTest();
    w.doWork(1);
    w.doWork(2);
    EXPECT_FALSE(AuthRigEgress::anyCursorWithoutItsOwnEgress(w));
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
}

// Positive control: under authentication a session WITH its own record egress is forwarded with that pub.
TEST(AuthEnforcementRegression, ASessionWithItsOwnRecordEgressStillReachesTheWorkerUnderAuthRegression) {
    AuthRigEgress r;
    FakePub pub;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticatedSession();
    r.gw.injectRecordEgressForTest(sid, r.owner(0), pub.make());
    r.subscribe(sid, 0);
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_EQ(w.subscriptionsForTest()[0].egressPub, pub.ptr());
    EXPECT_EQ(r.errorOn(77), -1);
    r.gw.closeSessionForTest(sid);
    w.doWork(2);
    r.gw.drainWorkerReturnsForTest(3);
}

// Issue #118, the worker's own check: an authenticating broker's worker (requirePerSessionEgress) never registers a
// cursor without its session's egress, even if a Subscribe op without one reached it (here: an InProcess gateway sends
// it).  RED before the fix: the worker registered it and pushed on the shared MDC publication.
TEST(AuthEnforcementRegression, AnAuthenticatingWorkerNeverRegistersACursorWithoutItsOwnEgressRegression) {
    Rig r(1);
    auto& w = r.pool.worker(r.owner(0));
    w.requirePerSessionEgress();
    r.gw.forwardSubscribeForTest(51, /*sessionId=*/0, 0, 0, 0, 0, false, false, /*corr=*/9);
    w.doWork(1);
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_EQ(w.subscribesRefusedWithoutEgressForTest(), 1u);
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #259 (X108): the shard workers learn ONE table per grant set and one bind per session - never one full bitmap
// per session - and a created topic ships the one changed word of each live table.  RED before the fix: every session's
// full WRITE bitmap went to every worker (200 sessions x 4 workers x 32 words), and every session that gained WRITE on a
// new topic re-shipped its whole bitmap.
// ─────────────────────────────────────────────────────────────────────────────
TEST(PermissionShippingRegression, ARoleOfManySessionsShipsOneTableAndOneBindEachRegression) {
    constexpr int32_t kWorkers = 4, kSessions = 200, kTopicsHere = 2000;
    Rig r(kWorkers);
    r.gw.setAuthMode(GatewayAuthMode::Required, nullptr);
    for (int32_t t = 0; t < kTopicsHere; ++t) r.gw.onCatalogObjectApplied(t, "world." + std::to_string(t), false);
    namespace rs = rune::security;
    std::vector<rs::GrantSpec> role = {
        {rs::Operation::Write, {rs::ResourceType::Topic, rs::PatternKind::Prefix, "world."}}};
    for (int i = 0; i < kSessions; ++i) {
        const int64_t sid = r.gw.injectTestSession(0);
        r.gw.authenticateWithGrantsForTest(sid, static_cast<uint64_t>(100 + i), role);
        for (int32_t w = 0; w < kWorkers; ++w) r.pool.worker(w).doWork(1);   // keep the lifecycle rings drained
        (void)r.gw.servicePermissionShippingForTest();
    }
    const uint64_t tableWords = (kTopicsHere + 63) / 64;
    EXPECT_EQ(r.gw.permissionCompilesForTest(), 1u) << "one compile for the role's set";
    EXPECT_EQ(r.gw.permissionTablesForTest(), 1u);
    EXPECT_EQ(r.gw.permissionWordsShippedForTest(), kWorkers * tableWords)
        << "the table once per worker; a bind carries no bitmap";
    EXPECT_EQ(r.gw.permissionOpsShippedForTest(), static_cast<uint64_t>(kWorkers) * (1 + kSessions));

    const uint64_t wordsBefore = r.gw.permissionWordsShippedForTest();
    r.gw.onCatalogObjectApplied(kTopicsHere, "world.new", false);   // the prefix grant now covers it
    EXPECT_EQ(r.gw.permissionWordsShippedForTest() - wordsBefore, static_cast<uint64_t>(kWorkers))
        << "one word per worker (a patch of the shared table), not every session's bitmap";
    EXPECT_EQ(r.gw.permissionCompilesForTest(), 1u) << "a created object recompiles nothing";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// PR #441 adversarial review (issue #118 area): no Records on the shared egress under authentication.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
struct AnswerLog : Rig {
    struct Answer { bool isError; int64_t corr; int code; int64_t subId; };
    std::vector<Answer> answers;   // in send order
    explicit AnswerLog(int32_t workers = 1) : Rig(workers) {
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            const auto hdr = pc::MessageHeader::decode(b, n);
            if (!hdr) return;
            if (hdr->templateId == static_cast<uint16_t>(pc::TemplateId::ErrorResponse)) {
                if (auto e = pc::ErrorResponse::decode(b, n)) answers.push_back({true, e->correlationId, e->code, 0});
            } else if (hdr->templateId == static_cast<uint16_t>(pc::TemplateId::SubscribeResponse)) {
                if (auto s = pc::SubscribeResponse::decode(b, n))
                    answers.push_back({false, s->correlationId, s->errorCode, s->subscriptionId});
            }
        });
        gw.setAuthMode(GatewayAuthMode::Required, nullptr);
    }
    int64_t authenticated() {
        const int64_t sid = gw.injectTestSession(0);
        gw.setSessionAuthForTest(sid, AuthAuthenticated{1, "reader", PermissionTable::allowAll(), {}});
        return sid;
    }
    void subscribeCorr(int64_t sid, int64_t corr) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = corr; s.topicId = 0; s.partition = 0; s.partitionKnown = 1;
        s.fromOffset = 0; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        buf.resize(s.encode(buf.data()));
        gw.applySubscribeForTest(buf.data(), buf.size());
    }
    void unsubscribe(int64_t sid, int64_t subId) {
        pc::Unsubscribe u;
        u.sessionId = sid; u.correlationId = 999; u.subscriptionId = subId;
        std::vector<uint8_t> buf(pc::Unsubscribe::ENCODED_SIZE);
        u.encode(buf.data());
        gw.applyUnsubscribeForTest(buf.data(), buf.size());
    }
    void cycle(int64_t now) {
        gw.pollPendingRecordEgressForTest(now);
        gw.pumpRetryQueuesForTest();
        for (int32_t w = 0; w < pool.workerCount(); ++w) pool.worker(w).doWork(now);
    }
    [[nodiscard]] int64_t okSubId(int64_t corr) const {
        for (const auto& a : answers) if (!a.isError && a.corr == corr && a.code == 0) return a.subId;
        return 0;
    }
    [[nodiscard]] bool refused(int64_t corr) const {
        for (const auto& a : answers)
            if ((a.isError && a.corr == corr && a.code == static_cast<int>(pc::ErrorCode::NotLeader)) ||
                (!a.isError && a.corr == corr &&
                 a.code == static_cast<int>(pc::ErrorCode::RecordEgressUnavailable)))   // F4: the held Subscribe's own cause
                return true;
        return false;
    }
};
}  // namespace

// Attack (issue #118, liveness of the retirement): a session whose record egress can NEVER be built (here: no egress
// endpoint; in production a persistently failing add or resolve) re-subscribes as the client does after each retirement
// (rearmMovedAway: a fresh correlation id each time).  The retirement must end in an explicit failure within a bounded
// number of attempts.  It does not: every attempt is answered Subscribe OK and only THEN NotLeader on the bound
// correlation id, which the client takes as a MOVED subscription - re-armed as a new pending Subscribe with a fresh
// retry budget (SubscriberHub::rearmMovedAway: nlRetries = 0) - so the budget never runs out: an endless
// subscribe / OK / NotLeader loop with no records and no error for the application.
TEST(AuthEnforcementAdversarialRegression, AnEgressFailedSubscribeIsNeverAnsweredOkBeforeItsRetirementAdversarialRegression) {
    AnswerLog r;
    const int64_t sid = r.authenticated();
    int okThenRetired = 0;
    for (int attempt = 0; attempt < 16; ++attempt) {
        const int64_t corr = 1000 + attempt;
        r.subscribeCorr(sid, corr);
        for (int p = 0; p < 3; ++p) r.cycle(1 + attempt * 3 + p);
        okThenRetired += r.okSubId(corr) != 0 && r.refused(corr) ? 1 : 0;
        ASSERT_TRUE(r.okSubId(corr) != 0 || r.refused(corr)) << "every attempt is answered";
    }
    EXPECT_EQ(okThenRetired, 0)
        << "an OK followed by NotLeader is a MOVE on the client (fresh budget each time): the loop never ends explicitly";
}

// #450: a held Subscribe retired for want of a record egress is answered RecordEgressUnavailable (49) only to a
// client that understands it (version >= 15); an older client parks an unknown fetch-refusal code and re-subscribes
// forever, so it is answered NotAuthorized, which every older client ends the subscription / group consumer on.
namespace {
int heldSubscribeAnswer(int32_t clientVersion) {
    AnswerLog r;
    const int64_t sid = r.gw.injectTestSession(0, clientVersion);
    r.gw.setSessionAuthForTest(sid, AuthAuthenticated{1, "reader", PermissionTable::allowAll(), {}});
    r.subscribeCorr(sid, 4242);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    for (const auto& a : r.answers)
        if (!a.isError && a.corr == 4242) return a.code;
    return -1;
}
}  // namespace

TEST(RecordEgressRegression, AnOlderClientIsAnsweredNotAuthorizedNotEgressUnavailableRegression) {
    EXPECT_EQ(heldSubscribeAnswer(pc::kClientVersionRecordEgressUnavailable - 1),
              static_cast<int>(pc::ErrorCode::NotAuthorized));
}

TEST(RecordEgressRegression, ACurrentClientIsStillAnsweredEgressUnavailableRegression) {
    EXPECT_EQ(heldSubscribeAnswer(pc::kClientVersion), static_cast<int>(pc::ErrorCode::RecordEgressUnavailable));
}

// C3 adversarial review (#450 x namespaces merge, version skew): main took client version 14 for namespaces, so #450
// gates at 15.  A namespace-era client (14, which never heard of code 49) and every older one are answered
// NotAuthorized; 15 and the current client get 49.  Mutation: gate clientUnderstandsRecordEgressUnavailable at
// kClientVersionNamespaces (14) -> the 14 row answers 49 and the test goes red.
TEST(RecordEgressRegression, ANamespaceEraClientNeverSeesCode49AdversarialRegression) {
    static_assert(pc::kClientVersionRecordEgressUnavailable == pc::kClientVersionNamespaces + 1);
    static_assert(pc::kClientVersion >= pc::kClientVersionRecordEgressUnavailable);
    static_assert(!pc::clientUnderstandsRecordEgressUnavailable(pc::kClientVersionNamespaces));
    for (const int32_t v : {0, pc::kClientVersionExplicitGroupSubscriptions, pc::kClientVersionTxnVerify,
                            pc::kClientVersionNamespaces}) {
        EXPECT_EQ(heldSubscribeAnswer(v), static_cast<int>(pc::ErrorCode::NotAuthorized)) << "client version " << v;
    }
    EXPECT_EQ(heldSubscribeAnswer(pc::kClientVersionRecordEgressUnavailable),
              static_cast<int>(pc::ErrorCode::RecordEgressUnavailable));
}

// Resources + no fallback over many attempts: whatever the number of failed re-subscribes, no cursor ever reaches the
// worker, the gateway keeps no subscription and no pending resolve, and the counter counts every retirement.
TEST(AuthEnforcementAdversarialRegression, RepeatedReSubscribesWithoutEgressLeaveNothingBehindAdversarialRegression) {
    AnswerLog r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticated();
    for (int attempt = 0; attempt < 64; ++attempt) {
        r.subscribeCorr(sid, 2000 + attempt);
        r.cycle(1 + attempt);
        ASSERT_TRUE(w.subscriptionsForTest().empty()) << "attempt " << attempt;
    }
    r.cycle(1000);
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(r.gw.subscribesWithoutRecordEgressForTest(), 64u);
}

// Ordering race: the client unsubscribes the parked subscription before the retirement pass.  Nothing is refused for
// it afterwards (no stray NotLeader re-arming a subscription the app ended), nothing reaches the worker.
TEST(AuthEnforcementAdversarialRegression, AnUnsubscribeBeforeTheRetirementLeavesNoRefusalAndNoCursorAdversarialRegression) {
    AnswerLog r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticated();
    r.subscribeCorr(sid, 77);
    // PR #441 F1: under authentication the OK is held until the egress resolves, so the client does not know the id
    // yet; the gateway's own (an end of the held subscription by id, e.g. a stale client retry) is used instead.
    const int64_t subId = r.okSubId(77) != 0 ? r.okSubId(77) : r.gw.subscriptionIdOfCorrForTest(sid, 77);
    ASSERT_NE(subId, 0);
    r.unsubscribe(sid, subId);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    EXPECT_FALSE(r.refused(77)) << "an ended subscription is not retired again";
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u);
}

// Restart-in-the-middle: the session closes while its Subscribe is parked for the egress; the next passes hand nothing
// to the worker and leave no pending resolve behind.
TEST(AuthEnforcementAdversarialRegression, ASessionClosedWhileItsSubscribeIsParkedLeavesNothingAdversarialRegression) {
    AnswerLog r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticated();
    r.subscribeCorr(sid, 77);
    r.gw.closeSessionForTest(sid);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u);
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
}

// Sibling path: a GROUP member's partition subscription without its own egress under authentication never reaches the
// worker either (the same funnel; the group rebalance path).
TEST(AuthEnforcementAdversarialRegression, AGroupMemberSubscribeWithoutEgressNeverReachesTheWorkerAdversarialRegression) {
    AnswerLog r;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticated();
    r.gw.forwardSubscribeForTest(61, sid, 0, 0, 0, 0, /*isGroup=*/true, /*groupCarrierReceived=*/true, /*corr=*/0);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    EXPECT_TRUE(w.subscriptionsForTest().empty());
    EXPECT_FALSE(AuthRigEgress::anyCursorWithoutItsOwnEgress(w));
}

// The worker's own check against a replayed / re-sent Subscribe: an EXISTING cursor (with its session's egress) that is
// re-subscribed under the same id without an egress is dropped - its records never move to the shared publication.
TEST(AuthEnforcementAdversarialRegression, AnExistingCursorReSubscribedWithoutEgressIsDroppedByTheWorkerAdversarialRegression) {
    Rig r(1);   // an InProcess gateway (sends what the worker must refuse)
    FakePub pub;
    auto& w = r.pool.worker(r.owner(0));
    w.requirePerSessionEgress();
    const int64_t a = r.gw.injectTestSession(0);
    const int64_t b = r.gw.injectTestSession(0);
    r.gw.injectRecordEgressForTest(a, r.owner(0), pub.make());
    r.gw.forwardSubscribeForTest(51, a, 0, 0, 0, 0, false, false, /*corr=*/9);
    w.doWork(1);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    ASSERT_EQ(w.subscriptionsForTest()[0].egressPub, pub.ptr());
    r.gw.forwardSubscribeForTest(51, b, 0, 0, 0, 0, false, false, /*corr=*/10);   // same id, no egress
    w.doWork(2);
    EXPECT_TRUE(w.subscriptionsForTest().empty()) << "fail closed: the cursor is dropped, never moved to the shared egress";
    EXPECT_EQ(w.subscribesRefusedWithoutEgressForTest(), 1u);
    r.gw.closeSessionForTest(a);
    w.doWork(3);
    r.gw.drainWorkerReturnsForTest(4);
}

// PR #441 re-check adversarial (the held OK): while a Subscribe's OK is held for its egress, a duplicate of it gets no
// early OK; the resolve sends exactly ONE OK and hands the worker the session's own pub; the stale failed-resolve entry
// that follows never retires the now-served subscription (no NotLeader after an OK).
TEST(AuthEnforcementAdversarialRegression, AHeldOkIsSentOnceOnTheResolveAndNeverFollowedByARefusalAdversarialRegression) {
    AnswerLog r;
    FakePub pub;
    auto& w = r.pool.worker(r.owner(0));
    const int64_t sid = r.authenticated();
    r.subscribeCorr(sid, 77);
    r.subscribeCorr(sid, 77);   // a client resend while the OK is held
    EXPECT_EQ(r.okSubId(77), 0) << "no OK before the egress exists";
    EXPECT_FALSE(r.refused(77));
    const int64_t subId = r.gw.subscriptionIdOfCorrForTest(sid, 77);
    ASSERT_NE(subId, 0);
    r.gw.resolveRecordEgressForTest(sid, r.owner(0), pub.make(), subId, 0, 0);
    int oks = 0;
    for (const auto& a : r.answers) oks += !a.isError && a.corr == 77 && a.code == 0 ? 1 : 0;
    EXPECT_EQ(oks, 1) << "exactly one OK";
    EXPECT_EQ(r.okSubId(77), subId);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    EXPECT_FALSE(r.refused(77)) << "never a NotLeader after the OK (that was the fresh-budget move loop)";
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    EXPECT_EQ(w.subscriptionsForTest()[0].egressPub, pub.ptr());
    r.gw.closeSessionForTest(sid);
    w.doWork(10);
    r.gw.drainWorkerReturnsForTest(11);
}

// Many held Subscribes of one session whose egress never resolves: each is refused once (NotLeader, never an OK) and
// nothing is left held - the held state is bounded by the resolve timeout, never accumulates.
TEST(AuthEnforcementAdversarialRegression, ManyHeldSubscribesAreEachRefusedOnceAndNothingStaysHeldAdversarialRegression) {
    AnswerLog r;
    const int64_t sid = r.authenticated();
    for (int i = 0; i < 200; ++i) r.subscribeCorr(sid, 5000 + i);
    for (int p = 0; p < 4; ++p) r.cycle(1 + p);
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(r.okSubId(5000 + i), 0);
        int refusals = 0;
        for (const auto& a : r.answers) refusals += a.corr == 5000 + i && (a.isError || a.code != 0) ? 1 : 0;   // F4: SubscribeResponse{49}
        EXPECT_EQ(refusals, 1) << i;
    }
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
    EXPECT_EQ(r.gw.pendingRecordEgressCountForTest(), 0u);
}
