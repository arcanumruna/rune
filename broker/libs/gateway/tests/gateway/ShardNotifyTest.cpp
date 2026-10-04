// ShardNotifyTest.cpp — the runtime consumes post-ack notifications (PSM mirror +
// session liveness) and never sends a ProduceResponse for them; it closes a
// session on every worker when it reaps it (spec §3.4).
//
// F15 reconciliation: the notification is the facts-only ProduceResult
// (deferAck == true), not WorkerReturn PRODUCE_NOTIFY (retired, F15 §7).
#include <rune/gateway/ClientGateway.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <span>
#include <vector>

#include <gtest/gtest.h>

using namespace rune::gateway;

namespace {
rune::node::WorkerReturn notify(int64_t sessionId, int64_t producerId) {
    rune::node::WorkerReturn n;
    n.type = rune::node::WorkerReturn::Type::ProduceResult;
    n.deferAck = true;              // facts only — never an answer
    n.sessionId = sessionId;
    n.topicId = 1; n.partition = 0; n.producerId = producerId; n.producerEpoch = 0;
    n.baseSequence = 0; n.recordCount = 1; n.baseOffset = 0;
    return n;
}
} // namespace

// m2 (fix round 1): in shard mode the owning worker's psm_ is the single owner
// for user partitions and the runtime mirror is never read for them — the
// notification updates nothing there (one unordered_map write per ack saved)
// and sends nothing.
TEST(GatewayShardNotifyRegression, UserPartitionNotifySendsNothingAndSkipsTheUnreadMirror) {
    int frames = 0;
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    gw.setOnEgressFrame([&](int64_t, const uint8_t*, std::size_t) { ++frames; });
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t sid = gw.injectTestSession(0);
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(notify(sid, 11)));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(frames, 0) << "the ack was already sent by the worker";
    EXPECT_FALSE(gw.producerStateManagerForTest().validate(1, 11, 0, 0, 0, 0).isDuplicate())
        << "shard mode: the runtime mirror is not maintained for user partitions (the worker psm_ owns them)";
}

TEST(GatewayShardNotifyRegression, NotifyRefreshesSessionLiveness) {
    // Shard-mode produces never pass through the runtime: the post-ack
    // notification is what keeps a producing session from being reaped.
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t sid = gw.injectTestSession(0);
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(notify(sid, -1)));
    gw.drainWorkerReturnsForTest(7'000);
    EXPECT_EQ(gw.sessionLastSeenNsForTest(sid), 7'000);
}

TEST(GatewayShardNotifyRegression, ReapedSessionIsClosedOnEveryWorker) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t sid = gw.injectTestSession(0);
    gw.reapExpiredForTest(ClientGateway::kDefaultSessionTimeoutNs + 1);
    ASSERT_EQ(gw.sessionCount(), 0u);
    for (int32_t w = 0; w < 2; ++w) {
        pool.worker(w).doWork(1);
        EXPECT_EQ(pool.worker(w).sessionClosesSeenForTest(), 1u) << "worker " << w << " session " << sid;
    }
}

// C2 (fix round 1): a worker reports a session whose ack publication can never
// deliver → the runtime closes it EVERYWHERE (table + every worker) so the
// client reconnects, and a later notification cannot keep it alive.
TEST(GatewayShardNotifyRegression, SessionFailedClosesTheSessionEverywhereAndStopsItsLiveness) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t sid = gw.injectTestSession(0);
    rune::node::WorkerReturn f;
    f.type = rune::node::WorkerReturn::Type::SessionSignal;
    f.sessionId = sid; f.partition = 1;
    ASSERT_TRUE(pool.worker(1).injectReturnForTest(f));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(gw.sessionCount(), 0u) << "the failed session is closed";
    for (int32_t w = 0; w < 2; ++w) {
        pool.worker(w).doWork(1);
        EXPECT_EQ(pool.worker(w).sessionClosesSeenForTest(), 1u) << "worker " << w;
    }
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(notify(sid, -1)));
    gw.drainWorkerReturnsForTest(9'000);
    EXPECT_EQ(gw.sessionLastSeenNsForTest(sid), -1) << "no notification refreshes a failed session";
}

// m4 (fix round 1): a Disconnect for a session this node does not have sends
// no SessionClose to the workers.
TEST(GatewayShardNotifyRegression, DisconnectOfAnUnknownSessionSendsNoWorkerTraffic) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    rune::protocol::client::Disconnect d; d.sessionId = 424242; d.correlationId = 1;
    std::vector<uint8_t> buf(rune::protocol::client::Disconnect::ENCODED_SIZE);
    d.encode(buf.data());
    gw.applyDisconnectForTest(buf.data(), buf.size());
    pool.worker(0).doWork(1);
    EXPECT_EQ(pool.worker(0).sessionClosesSeenForTest(), 0u);
    EXPECT_EQ(gw.pendingSessionClosesForTest(), 0u);
}

// I2 (fix round 1): the shard session cap counts every slot a worker ack table
// can hold — including connects already announced (fully or partly) but not yet
// accepted (their ConnectResponse send still failing).  Uncounted, the table
// overflowed and an ACCEPTED session's announcement was refused (never acked).
TEST(GatewayShardNotifyRegression, SessionCapCountsAnnouncedButNotAcceptedConnects) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    for (std::size_t i = 0; i + 1 < ClientGateway::kShardMaxSessions; ++i) (void)gw.injectTestSession(0);
    gw.injectPendingConnectForTest(/*sessionId=*/900'001, /*announceBegun=*/true);
    gw.injectPendingConnectForTest(/*sessionId=*/900'002, /*announceBegun=*/false);
    EXPECT_EQ(gw.shardSessionSlotsInUseForTest(), ClientGateway::kShardMaxSessions)
        << "4094 accepted + 1 announced-pending fill the worker table; the un-announced one does not";
}

// I2 (fix round 1): evicting an announced pending connect at the 64-entry cap
// closes it on every worker (its table slots were leaked before).
TEST(GatewayShardNotifyRegression, EvictedAnnouncedPendingConnectIsClosedOnEveryWorker) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    gw.injectPendingConnectForTest(/*sessionId=*/900'001, /*announceBegun=*/true);
    gw.evictOldestPendingConnectForTest();
    for (int32_t w = 0; w < 2; ++w) {
        pool.worker(w).doWork(1);
        EXPECT_EQ(pool.worker(w).sessionClosesSeenForTest(), 1u) << "worker " << w;
    }
}

// C2 follow-up (found on sight, fix round 1): the worker can report a session
// whose publication failed BEFORE the runtime accepted it (announce done, the
// ConnectResponse send still retrying).  The pending connect must be dropped
// and closed everywhere — accepting it afterwards would create a session no
// worker can ever ack.
TEST(GatewayShardNotifyRegression, SessionFailedBeforeAcceptDropsThePendingConnect) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    gw.injectPendingConnectForTest(/*sessionId=*/900'001, /*announceBegun=*/true);
    rune::node::WorkerReturn f;
    f.type = rune::node::WorkerReturn::Type::SessionSignal;
    f.sessionId = 900'001; f.partition = 0;
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(f));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(gw.pendingConnectCount(), 0u) << "never accept a session no worker can ack";
    for (int32_t w = 0; w < 2; ++w) {
        pool.worker(w).doWork(1);
        EXPECT_EQ(pool.worker(w).sessionClosesSeenForTest(), 1u) << "worker " << w;
    }
}

// ===========================================================================
// Task 13 fix round 2.
// ===========================================================================
namespace {
rune::node::WorkerReturn signal(int64_t sid, int32_t worker, rune::node::SessionSignalKind k) {
    rune::node::WorkerReturn r;
    r.type = rune::node::WorkerReturn::Type::SessionSignal;
    r.sessionId = sid; r.partition = worker; r.lastOffset = static_cast<int64_t>(k);
    return r;
}
std::vector<uint8_t> produceFrame(int64_t sid) {
    rune::protocol::client::Produce m;
    m.sessionId = sid; m.correlationId = 1; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
    static const std::vector<uint8_t> batch(80, 0);
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
std::vector<uint8_t> metadataFrame(int64_t sid) {
    rune::protocol::client::MetadataRequest m; m.sessionId = sid; m.correlationId = 2;
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
} // namespace

// Finding 1 (critical): the gateway pauses a session's PRODUCE frames while ANY
// worker reports it Held (controlled ABORT on its image) and resumes once every
// such worker reported Released.  Other sessions are never paused, and a non-
// produce frame at the head of a held session's image is not aborted — though,
// ABORT stopping the whole image, the frames behind an aborted Produce wait for
// the release (or a close / lease reap).
TEST(GatewayShardNotifyRegression, HeldSessionsProduceFramesArePausedUntilEveryWorkerReleases) {
    using K = rune::node::SessionSignalKind;
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(2, rune::metadata::ShardRouter(2), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t a = gw.injectTestSession(0), b = gw.injectTestSession(0);
    const auto pa = produceFrame(a), pb = produceFrame(b), ma = metadataFrame(a);
    EXPECT_FALSE(gw.clientFrameHeldForTest(pa.data(), pa.size()));
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(signal(a, 0, K::Held)));
    ASSERT_TRUE(pool.worker(1).injectReturnForTest(signal(a, 1, K::Held)));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_TRUE(gw.clientFrameHeldForTest(pa.data(), pa.size())) << "a held session's produce is paused";
    EXPECT_FALSE(gw.clientFrameHeldForTest(pb.data(), pb.size())) << "other sessions are not";
    EXPECT_FALSE(gw.clientFrameHeldForTest(ma.data(), ma.size()))
        << "a non-produce frame at the head of the held session's image is not aborted";
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(signal(a, 0, K::Released)));
    gw.drainWorkerReturnsForTest(2'000);
    EXPECT_TRUE(gw.clientFrameHeldForTest(pa.data(), pa.size())) << "still held by worker 1";
    ASSERT_TRUE(pool.worker(1).injectReturnForTest(signal(a, 1, K::Released)));
    gw.drainWorkerReturnsForTest(3'000);
    EXPECT_FALSE(gw.clientFrameHeldForTest(pa.data(), pa.size()));
    ASSERT_TRUE(pool.worker(1).injectReturnForTest(signal(b, 1, K::Held)));   // then b disconnects
    gw.drainWorkerReturnsForTest(4'000);
    EXPECT_TRUE(gw.clientFrameHeldForTest(pb.data(), pb.size()));
    rune::protocol::client::Disconnect d; d.sessionId = b; d.correlationId = 3;
    std::vector<uint8_t> db(rune::protocol::client::Disconnect::ENCODED_SIZE);
    d.encode(db.data());
    gw.applyDisconnectForTest(db.data(), db.size());
    EXPECT_FALSE(gw.clientFrameHeldForTest(pb.data(), pb.size())) << "the close clears its held state";
}

// D8b (found 2026-10-04, ShardModeRegression.SlowEndpointUnawareClientCannotStarveTheShard ~1/3 on Linux): a
// HELD session (paused by back-pressure, not yet reading its acks) was reaped on the normal 10 s lease and every
// queued ack dropped as orphaned (16-22k lost).  A held session is now reaped only after maxHeldIdleNs without
// progress (Kafka: connections.max.idle.ms, 10 min) — reading acks refreshes the lease; a held client that DIES is
// failed by the worker (NOT_CONNECTED after delivering, SessionAckSink test).  Non-held sessions keep the 10 s lease.
TEST(GatewayShardNotifyRegression, HeldSessionKeepsItsLeaseUpToMaxHeldIdleRegression) {
    using K = rune::node::SessionSignalKind;
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    constexpr int64_t T = ClientGateway::kDefaultSessionTimeoutNs;
    constexpr int64_t H = ClientGateway::kDefaultMaxHeldIdleNs;
    static_assert(H > T);
    const int64_t held = gw.injectTestSession(0), plain = gw.injectTestSession(0);
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(signal(held, 0, K::Held)));
    gw.drainWorkerReturnsForTest(1'000);
    gw.reapExpiredForTest(T + 500'000'000);                  // past the normal lease
    EXPECT_GE(gw.sessionLastSeenNsForTest(held), 0) << "a paused session must not be reaped on the normal lease";
    EXPECT_EQ(gw.sessionLastSeenNsForTest(plain), -1) << "a non-held idle session is reaped on the normal lease";
    gw.reapExpiredForTest(H + 500'000'000);                  // past the held-idle bound with no progress
    EXPECT_EQ(gw.sessionLastSeenNsForTest(held), -1) << "a paused session with no progress is reaped at the bound";
}

// Finding 2: a SessionFailed close tells the client (best effort, on the
// gateway's own publication) that its session expired — the rune client
// reconnects only on SessionExpired / UnknownSession.
TEST(GatewayShardNotifyRegression, SessionFailedTellsTheClientItsSessionExpired) {
    std::vector<std::vector<uint8_t>> frames;
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    gw.setOnEgressFrame([&](int64_t, const uint8_t* p, std::size_t n) { frames.emplace_back(p, p + n); });
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t sid = gw.injectTestSession(0);
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(signal(sid, 0, rune::node::SessionSignalKind::Failed)));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(gw.sessionCount(), 0u);
    ASSERT_EQ(frames.size(), 1u) << "exactly one frame: the expiry notice";
    auto e = rune::protocol::client::ErrorResponse::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->sessionId, sid);
    EXPECT_EQ(e->code, static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired));
}

// Finding 4: one close path — Disconnect and SessionFailed purge the session's
// in-flight record-egress resolves exactly like the lease reap.
TEST(GatewayShardNotifyRegression, EveryCloseOfASessionPurgesItsPendingRecordEgress) {
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    const int64_t a = gw.injectTestSession(0), b = gw.injectTestSession(0), keep = gw.injectTestSession(0);
    gw.injectPendingRecordEgressForTest(a, 0);
    gw.injectPendingRecordEgressForTest(b, 0);
    gw.injectPendingRecordEgressForTest(keep, 0);
    rune::protocol::client::Disconnect d; d.sessionId = a; d.correlationId = 3;
    std::vector<uint8_t> db(rune::protocol::client::Disconnect::ENCODED_SIZE);
    d.encode(db.data());
    gw.applyDisconnectForTest(db.data(), db.size());
    EXPECT_EQ(gw.pendingRecordEgressCountForTest(), 2u) << "Disconnect purges it";
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(signal(b, 0, rune::node::SessionSignalKind::Failed)));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(gw.pendingRecordEgressCountForTest(), 1u) << "SessionFailed purges it";
}
