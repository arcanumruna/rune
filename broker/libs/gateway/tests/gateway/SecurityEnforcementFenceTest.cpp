// SecurityEnforcementFenceTest.cpp — issues #196 / #109 / #110: a security DDL's OK (the D24 gate) means "THIS broker
// already enforces it", and with shard workers the workers enforce: their WRITE bitmaps / binds (session-lifecycle
// ring) and their push cursors (op ring).  The gateway's enforced offset therefore advances only once EVERY worker
// acknowledged the enforcement fence that followed the event's ops on each of its rings - never when the ops were
// merely accepted by a ring (#196), never on a global "shipper queue empty" (#110), and an ended worker-pushed
// subscription is ended by its worker, not by the gateway (#109).
//
// Deterministic: no Aeron, no sleeps - the gateway and the worker duty cycles are driven by hand.  The record-egress
// publication is a fake (the logs stay empty, so no push pass ever writes through it).
#include "support/SecurityEventChain.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/SecurityView.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/SessionSignalKind.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/security/Permissions.h>
#include <rune/security/SecurityEvent.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace rune::gateway;
using namespace rune::node::test;
namespace pc  = rune::protocol::client;
namespace sec = rune::security;
namespace fs  = std::filesystem;

namespace {

struct FakePub {
    alignas(64) unsigned char storage[256]{};
    std::shared_ptr<aeron::ExclusivePublication> make() {
        return {reinterpret_cast<aeron::ExclusivePublication*>(storage), [](aeron::ExclusivePublication*) {}};
    }
};

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-sec-fence-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

sec::GrantSpec topicGrant(sec::Operation op, const std::string& name) {
    return sec::GrantSpec{op, sec::ResourcePattern{sec::ResourceType::Topic, sec::PatternKind::Exact, name}};
}

/// An authenticating shard gateway (per-worker egress) over `workers` workers, topic 0 "events" (1 partition), and a
/// SecurityView over a chain: the bootstrap admin, writer "w" (WRITE events) and reader "r" (READ events).
struct FenceRig {
    TempDir                              dir;
    std::unique_ptr<TopicRegistry>       registry;
    rune::node::WorkerPool               pool;
    rune::replication::InMemoryTransport transport;
    rune::node::ReplicaRegistry          replicas;
    SecurityEventChain                   chain;
    std::unique_ptr<SecurityView>        view;
    ClientGateway                        gw;
    std::vector<pc::ErrorResponse>       errors;   // what the GATEWAY sent on its own egress

    explicit FenceRig(int32_t workers)
        : pool(workers, rune::metadata::ShardRouter(workers), 1 << 16, 1 << 16, 16, 16)
        , gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        rune::node::TopicSpec ts;
        ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            const auto hdr = pc::MessageHeader::decode(b, n);
            if (hdr && hdr->templateId == static_cast<uint16_t>(pc::TemplateId::ErrorResponse))
                if (auto e = pc::ErrorResponse::decode(b, n)) errors.push_back(*e);
        });
        gw.setWorkerPool(&pool);
        gw.setShardMode(true);
        gw.setPerWorkerEgressActive(true);
        rune::replication::PartitionConfig c;
        c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
        auto rep = std::make_unique<rune::replication::PartitionReplica>(
            c, rune::replication::Role::Leader, 1, std::vector<int32_t>{1}, std::vector<int32_t>{1},
            registry->log(0, 0), &transport);
        rep->setExternalFollowerFlush(true);
        replicas.add(0, 0, std::move(rep));
        gw.setReplicaRegistry(&replicas);

        sec::BootstrapOps b; b.verifier = testVerifier(9); b.fakeSaltSeed.fill(7);
        (void)chain.emit(b, "admin", "BOOTSTRAP", "bootstrap");
        (void)chain.createUser("w");
        (void)chain.emit(sec::GrantOps{sec::GranteeKind::User, topicGrant(sec::Operation::Write, "events")}, "w",
                         "GRANT WRITE ON TOPIC events TO USER w;");
        (void)chain.createUser("r");
        (void)chain.emit(sec::GrantOps{sec::GranteeKind::User, topicGrant(sec::Operation::Read, "events")}, "r",
                         "GRANT READ ON TOPIC events TO USER r;");
        view = std::make_unique<SecurityView>(chain.state());
        gw.setAuthMode(GatewayAuthMode::Required, view.get());
        gw.onCatalogObjectApplied(0, "events", false);
    }

    uint64_t userId(const std::string& name) const { return chain.state().user(name)->objectId; }

    /// A session logged in as `name` with exactly its effective grants (as a login compiles them).
    int64_t login(const std::string& name) {
        const int64_t sid = gw.injectTestSession(0);
        gw.authenticateWithGrantsForTest(sid, userId(name), sec::effectiveGrants(chain.state(), userId(name)));
        return sid;
    }

    /// Apply the chain's next event to the view and the gateway, as the control thread does; returns its offset.
    int64_t apply(sec::Ops ops, const std::string& name, std::string statement) {
        const auto ev = chain.emit(std::move(ops), name, std::move(statement));
        const auto bytes = sec::encode(ev);
        EXPECT_TRUE(bytes.has_value());
        const auto offset = static_cast<int64_t>(chain.count()) - 1;
        auto applied = view->onApplied(offset, *bytes);
        EXPECT_TRUE(applied.has_value()) << (applied ? "" : applied.error().detail);
        gw.onSecurityApplied(offset, *applied);
        return offset;
    }
    int64_t revoke(sec::Operation op, const std::string& user) {
        return apply(sec::RevokeOps{sec::GranteeKind::User, topicGrant(op, "events")}, user,
                     "REVOKE ... ON TOPIC events FROM USER " + user + ";");
    }

    /// One worker pass, then the gateway reads that worker's returns (its fence acks among them).
    void runWorker(int32_t w, int64_t now) {
        pool.worker(w).doWork(now);
        gw.drainWorkerReturnsForTest(now);
    }
    void runAll(int64_t now) {
        (void)gw.servicePermissionShippingForTest();
        (void)gw.pumpRetryQueuesForTest();
        for (int32_t w = 0; w < pool.workerCount(); ++w) runWorker(w, now);
    }
    [[nodiscard]] int errorOn(int64_t corr) const {
        for (const auto& e : errors) if (e.correlationId == corr) return e.code;
        return -1;
    }
};

}  // namespace

// Issue #196 (RED before the fix: the enforced offset reached the REVOKE as soon as the shipper had HANDED the bind to
// every worker ring - the worker had installed nothing, and the D24 gate released the OK; a produce on the old slot was
// then appended and acked).  Two workers, each acknowledging in turn: the event is enforced only after BOTH acked.
TEST(SecurityEnforcementFenceRegression, AShardRevokeIsEnforcedOnlyOnceEveryWorkerInstalledItRegression) {
    FenceRig r(2);
    const int64_t ws = r.login("w");
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    const int64_t before = r.gw.securityEnforcedOffset();

    const int64_t off = r.revoke(sec::Operation::Write, "w");
    (void)r.gw.servicePermissionShippingForTest();
    ASSERT_EQ(r.gw.permissionPendingUpdatesForTest(), 0u) << "every op is ON the rings: accepted, not installed";
    EXPECT_LT(r.gw.securityEnforcedOffset(), off) << "ring acceptance is not enforcement (#196)";
    EXPECT_EQ(r.gw.securityEnforcedOffset(), before);
    EXPECT_EQ(r.gw.securityPendingEnforceForTest(), off);

    r.runWorker(0, 10);
    EXPECT_LT(r.gw.securityEnforcedOffset(), off) << "worker 1 has not installed the revoke yet";
    EXPECT_GE(r.gw.enforceFenceAckedForTest(0, 1), 1u) << "worker 0 acknowledged the lifecycle fence";
    EXPECT_EQ(r.gw.enforceFenceAckedForTest(0, 0), 0u) << "no Unsubscribe was queued: no op-ring fence is owed";

    r.runWorker(1, 11);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off) << "every worker acknowledged the fence behind the revoke";
    EXPECT_EQ(r.gw.securityPendingEnforceForTest(), -1);
    (void)ws;
}

// Issue #196, the session-ending kinds (DISABLE / DROP USER / TERMINATE SESSIONS): the revoked session's direct
// produce must be refused by every worker before the OK, whatever its (separately queued) SessionClose does - the
// session is bound to nothing on the permission FIFO first, and the fence follows that bind.
TEST(SecurityEnforcementFenceRegression, ARevokedSessionIsBoundToNothingAheadOfItsFenceRegression) {
    FenceRig r(1);
    const int64_t ws = r.login("w");
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    const uint64_t opsBefore = r.gw.permissionOpsShippedForTest();

    const int64_t off = r.apply(sec::AlterUserOps{sec::DisableUser{}}, "w", "ALTER USER w DISABLE;");
    EXPECT_FALSE(r.gw.hasSessionForTest(ws)) << "the session was revoked and closed";
    EXPECT_EQ(r.gw.permissionOpsShippedForTest() - opsBefore, 2u)
        << "a bind to nothing for the revoked session, then the fence (one worker)";
    EXPECT_LT(r.gw.securityEnforcedOffset(), off);
    r.runAll(10);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off);
}

// Issue #110 (RED before the fix: the gate waited for the shipper's GLOBAL queue to be empty, so a steady stream of
// other sessions' logins kept it non-empty and the REVOKE's OK was held indefinitely).  The fence is FIFO in the
// shipper's queue: once the items AHEAD of it are installed the event is enforced, whatever is queued behind it.
TEST(SecurityEnforcementFenceRegression, OtherSessionsPermissionShippingCannotStarveTheGateRegression) {
    FenceRig r(1);
    (void)r.login("w");
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    // A backlog: more binds than the lifecycle ring holds (the worker is not running).
    int64_t now = 10;
    auto loginMany = [&](int n) { for (int i = 0; i < n; ++i) (void)r.login(i % 2 == 0 ? "w" : "r"); };
    loginMany(4000);
    ASSERT_GT(r.gw.permissionPendingUpdatesForTest(), 0u) << "the lifecycle ring is full: binds wait in the shipper";
    const int64_t off = r.revoke(sec::Operation::Write, "w");
    // Steady login traffic: every cycle the worker drains its ring and MORE binds arrive behind the fence.
    bool enforced = false;
    for (int cycle = 0; cycle < 200 && !enforced; ++cycle) {
        r.runAll(++now);
        loginMany(300);
        enforced = r.gw.securityEnforcedOffset() >= off;
    }
    EXPECT_TRUE(enforced) << "the REVOKE is enforced once the items ahead of its fence are installed";
    EXPECT_GT(r.gw.permissionPendingUpdatesForTest(), 0u) << "(the queue never emptied: the old rule would still hold it)";
}

// Issue #109 (RED before the fix: the gateway sent the 45 on its own egress at once and erased the subscription, while
// the WORKER kept pushing Records on the subscription's own record egress - Records could follow the 45, and the D24
// gate counted no Unsubscribe).  A worker-pushed subscription is ended by its worker: the gateway sends nothing on the
// subscription's correlation id, and the REVOKE is enforced only once the worker acknowledged the op-ring fence that
// follows the ending Unsubscribe.
TEST(SecurityEnforcementFenceRegression, AWorkerPushedSubscriptionIsEndedByItsWorkerNotByTheGatewayRegression) {
    FenceRig r(1);
    FakePub pub;
    const int64_t rs = r.login("r");
    r.gw.injectRecordEgressForTest(rs, 0, pub.make());
    pc::Subscribe s;
    s.sessionId = rs; s.correlationId = 77; s.topicId = 0; s.partition = 0; s.partitionKnown = 1; s.fromOffset = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    r.gw.applySubscribeForTest(buf.data(), buf.size());
    r.runAll(1);
    ASSERT_EQ(r.pool.worker(0).subscriptionsForTest().size(), 1u) << "the worker pushes the subscription";
    ASSERT_EQ(r.errorOn(77), -1);

    const int64_t off = r.revoke(sec::Operation::Read, "r");
    EXPECT_EQ(r.errorOn(77), -1) << "the gateway never answers the end of a subscription its worker pushes";
    EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "the gateway forgot it (a racing PushRead of its id is dropped)";
    EXPECT_LT(r.gw.securityEnforcedOffset(), off) << "the ending Unsubscribe still sits in the worker's op ring";

    // The worker is not driven here (its push pass would write the terminal frame through the fake publication): its
    // acknowledgements are delivered as it would send them - the lifecycle ring first, then the op ring.
    rune::node::WorkerReturn ack;
    ack.type       = rune::node::WorkerReturn::Type::SessionSignal;
    ack.partition  = 0;
    ack.lastOffset = static_cast<int64_t>(rune::node::SessionSignalKind::FenceAck);
    ack.baseOffset = static_cast<int64_t>(r.gw.enforceFencesIssuedForTest());
    ack.sessionId  = 1;
    r.gw.handleWorkerReturnForTest(ack);
    EXPECT_LT(r.gw.securityEnforcedOffset(), off) << "the op ring (the end) is not acknowledged yet";
    ack.sessionId  = 0;
    r.gw.handleWorkerReturnForTest(ack);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off);
    EXPECT_EQ(r.errorOn(77), -1) << "still nothing from the gateway: the worker sends the 45 after its last Records";
}

// Issues #109 / #196: an event that queues nothing for any worker (a CREATE USER, a GRANT to a user with no session)
// is enforced at once - only what an event queued for a worker is waited for, so a busy or slow worker never delays
// an unrelated OK.
TEST(SecurityEnforcementFenceRegression, AnEventThatQueuesNothingForTheWorkersIsEnforcedAtOnceRegression) {
    FenceRig r(2);
    (void)r.login("w");
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    const uint64_t fences = r.gw.enforceFencesIssuedForTest();
    const auto ev = r.chain.createUser("nobody-logged-in");
    const auto bytes = sec::encode(ev);
    ASSERT_TRUE(bytes.has_value());
    const auto off = static_cast<int64_t>(r.chain.count()) - 1;
    auto applied = r.view->onApplied(off, *bytes);
    ASSERT_TRUE(applied.has_value());
    r.gw.onSecurityApplied(off, *applied);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off) << "nothing to wait for";
    EXPECT_EQ(r.gw.enforceFencesIssuedForTest(), fences) << "no fence issued";
    // ... and an event behind a still-fenced one waits for it (the enforced offset moves forward, in order).
    const int64_t revoke = r.revoke(sec::Operation::Write, "w");
    const auto ev2 = r.chain.createUser("another");
    const auto off2 = static_cast<int64_t>(r.chain.count()) - 1;
    auto applied2 = r.view->onApplied(off2, *sec::encode(ev2));
    ASSERT_TRUE(applied2.has_value());
    r.gw.onSecurityApplied(off2, *applied2);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off) << "behind the REVOKE still awaiting its fence";
    r.runAll(20);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off2);
    (void)revoke;
}

// Issue #109 control: a subscription whose Subscribe never reached a worker (it waited for its record egress) has no
// worker to end it - the gateway answers the 45 itself, and nothing waits for a worker that never had it.
TEST(SecurityEnforcementFenceRegression, ASubscriptionThatNeverReachedAWorkerIsEndedByTheGatewayRegression) {
    FenceRig r(1);
    const int64_t rs = r.login("r");   // no record egress yet: its Subscribe parks AwaitingEgress
    pc::Subscribe s;
    s.sessionId = rs; s.correlationId = 88; s.topicId = 0; s.partition = 0; s.partitionKnown = 1; s.fromOffset = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    r.gw.applySubscribeForTest(buf.data(), buf.size());
    ASSERT_EQ(r.gw.subscriptionCount(), 1u);
    const int64_t off = r.revoke(sec::Operation::Read, "r");
    EXPECT_EQ(r.errorOn(88), static_cast<int>(pc::ErrorCode::NotAuthorized)) << "answered by the gateway, at once";
    EXPECT_EQ(r.gw.pendingEgressWaitersForTest(), 0) << "the parked Subscribe never reaches a worker";
    r.runAll(5);
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off);
    EXPECT_TRUE(r.pool.worker(0).subscriptionsForTest().empty());
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #459: the fence acks out of order, replayed, stale or from a worker that does not exist.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
rune::node::WorkerReturn fenceAck(int32_t worker, int ring, uint64_t seq) {
    rune::node::WorkerReturn a;
    a.type       = rune::node::WorkerReturn::Type::SessionSignal;
    a.partition  = worker;
    a.lastOffset = static_cast<int64_t>(rune::node::SessionSignalKind::FenceAck);
    a.baseOffset = static_cast<int64_t>(seq);
    a.sessionId  = ring;
    return a;
}
}  // namespace

// Ordering: event 1 (REVOKE READ: an ending Unsubscribe on the op ring + the lifecycle) then event 2 (REVOKE WRITE:
// lifecycle only).  The lifecycle ack of event 2 arrives first - it covers event 1's lifecycle fence too (FIFO) - but
// event 1's op ring has not acknowledged: NEITHER event is enforced (the offset moves only forward, in event order).
TEST(SecurityEnforcementFenceRegression, AckOfALaterEventNeverReleasesAnEarlierOneStillFencedAdversarialRegression) {
    FenceRig r(1);
    FakePub pub;
    const int64_t rs = r.login("r");
    (void)r.login("w");
    r.gw.injectRecordEgressForTest(rs, 0, pub.make());
    pc::Subscribe s;
    s.sessionId = rs; s.correlationId = 61; s.topicId = 0; s.partition = 0; s.partitionKnown = 1; s.fromOffset = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    r.gw.applySubscribeForTest(buf.data(), buf.size());
    r.runAll(1);
    ASSERT_EQ(r.pool.worker(0).subscriptionsForTest().size(), 1u);
    const int64_t before = r.gw.securityEnforcedOffset();
    const uint64_t f0 = r.gw.enforceFencesIssuedForTest();
    const int64_t e1 = r.revoke(sec::Operation::Read, "r");
    const int64_t e2 = r.revoke(sec::Operation::Write, "w");
    ASSERT_EQ(r.gw.enforceFencesIssuedForTest(), f0 + 2);
    r.gw.handleWorkerReturnForTest(fenceAck(0, 1, f0 + 2));   // lifecycle: event 2's fence (and so event 1's)
    EXPECT_EQ(r.gw.securityEnforcedOffset(), before) << "event 1's op ring has not acknowledged";
    r.gw.handleWorkerReturnForTest(fenceAck(0, 0, f0 + 1));   // the op ring: event 1's fence
    EXPECT_EQ(r.gw.securityEnforcedOffset(), e2) << "both enforced, in order";
    (void)e1;
}

// Replays and garbage: a replayed old ack, an ack for a fence never issued on that ring, and acks naming a worker that
// does not exist (negative, past the pool, past the 64-worker cap) never enforce anything they should not, never crash.
TEST(SecurityEnforcementFenceRegression, StaleReplayedAndOutOfRangeAcksEnforceNothingAdversarialRegression) {
    FenceRig r(2);
    (void)r.login("w");
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    const int64_t before = r.gw.securityEnforcedOffset();
    const uint64_t f0 = r.gw.enforceFencesIssuedForTest();
    const int64_t off = r.revoke(sec::Operation::Write, "w");
    const uint64_t seq = f0 + 1;
    for (const int32_t w : {-1, 2, 63, 64, 1000, INT32_MIN, INT32_MAX})
        r.gw.handleWorkerReturnForTest(fenceAck(w, 1, seq));
    r.gw.handleWorkerReturnForTest(fenceAck(0, 1, f0));        // a replay of an older fence
    r.gw.handleWorkerReturnForTest(fenceAck(1, 0, seq));       // the right seq on the WRONG ring
    EXPECT_EQ(r.gw.securityEnforcedOffset(), before) << "no ack of the event's own fence from every worker yet";
    r.gw.handleWorkerReturnForTest(fenceAck(0, 1, seq));
    EXPECT_EQ(r.gw.securityEnforcedOffset(), before) << "worker 1 has not acknowledged";
    r.gw.handleWorkerReturnForTest(fenceAck(1, 1, seq));
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off);
    r.gw.handleWorkerReturnForTest(fenceAck(1, 1, seq));       // duplicate: harmless
    r.gw.handleWorkerReturnForTest(fenceAck(0, 1, 0));
    EXPECT_EQ(r.gw.securityEnforcedOffset(), off) << "never moves backward";
}

// Issue #197 (RED before the fix: nothing told the DDL answer gate that the new object's WRITE-bit patches were still
// queued in the gateway's permission shipper, so the DefineTopic OK left before every worker could authorize it).
// objectPermissionsShipped(id) is false while the patch queued by onCatalogObjectApplied is not yet accepted by every
// worker ring, true once the shipper passed it; a dropped object and an unknown id are true.
TEST(SecurityEnforcementFenceRegression, ObjectPermissionsShippedOnlyAfterEveryWorkerAcceptedThePatchRegression) {
    FenceRig r(2);
    const int64_t sid = r.gw.injectTestSession(0);
    r.gw.authenticateWithGrantsForTest(sid, 77, {sec::GrantSpec{sec::Operation::Write, sec::ResourcePattern{
        sec::ResourceType::Topic, sec::PatternKind::Prefix, "w."}}});
    for (int i = 0; i < 3; ++i) r.runAll(1 + i);
    EXPECT_TRUE(r.gw.objectPermissionsShipped(5)) << "unknown id: nothing owed";

    r.gw.holdPermissionShippingForTest(true);
    r.gw.onCatalogObjectApplied(5, "w.new", false);
    r.gw.onCatalogObjectApplied(6, "other.x", false);   // no grant covers it: no patch, but the call is conservative
    EXPECT_GT(r.gw.permissionPendingUpdatesForTest(), 0u) << "the patch waits in the shipper";
    EXPECT_FALSE(r.gw.objectPermissionsShipped(5)) << "held: no worker has the patch";
    r.runAll(10);
    EXPECT_FALSE(r.gw.objectPermissionsShipped(5)) << "still held after a service pass";

    r.gw.holdPermissionShippingForTest(false);
    r.runAll(11);
    EXPECT_TRUE(r.gw.objectPermissionsShipped(5)) << "every worker ring accepted the patch";
    EXPECT_TRUE(r.gw.objectPermissionsShipped(5)) << "idempotent";
    EXPECT_TRUE(r.gw.objectPermissionsShipped(6));

    r.gw.holdPermissionShippingForTest(true);
    r.gw.onCatalogObjectApplied(7, "w.gone", false);
    EXPECT_FALSE(r.gw.objectPermissionsShipped(7));
    r.gw.onCatalogObjectDropped(7);
    EXPECT_TRUE(r.gw.objectPermissionsShipped(7)) << "a dropped object never serves: nothing to wait for";
    r.gw.holdPermissionShippingForTest(false);
}
