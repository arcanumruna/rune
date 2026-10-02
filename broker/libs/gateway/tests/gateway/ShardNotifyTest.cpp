// ShardNotifyTest.cpp — the runtime consumes post-ack notifications (PSM mirror +
// session liveness) and never sends a ProduceResponse for them; it closes a
// session on every worker when it reaps it (spec §3.4).
//
// F15 reconciliation: the notification is the facts-only ProduceResult
// (deferAck == true), not WorkerReturn PRODUCE_NOTIFY (retired, F15 §7).
#include <rune/gateway/ClientGateway.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>

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
    f.type = rune::node::WorkerReturn::Type::SessionFailed;
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
    f.type = rune::node::WorkerReturn::Type::SessionFailed;
    f.sessionId = 900'001; f.partition = 0;
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(f));
    gw.drainWorkerReturnsForTest(1'000);
    EXPECT_EQ(gw.pendingConnectCount(), 0u) << "never accept a session no worker can ack";
    for (int32_t w = 0; w < 2; ++w) {
        pool.worker(w).doWork(1);
        EXPECT_EQ(pool.worker(w).sessionClosesSeenForTest(), 1u) << "worker " << w;
    }
}
