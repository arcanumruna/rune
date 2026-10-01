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

TEST(GatewayShardNotifyRegression, ProduceNotifyUpdatesPsmMirrorAndSendsNothing) {
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
    EXPECT_TRUE(gw.producerStateManagerForTest().validate(1, 11, 0, 0, 0, 0).isDuplicate())
        << "PSM mirror updated from the notification";
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
