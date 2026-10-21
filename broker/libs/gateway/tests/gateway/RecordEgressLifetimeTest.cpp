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
    EXPECT_EQ(r.errorOn(77), static_cast<int>(pc::ErrorCode::NotLeader))
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
