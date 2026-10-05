// SubscribeDedupeTest.cpp — (session, Subscribe.correlationId) identifies ONE
// subscription (2026-10-05).
//
// The client binds exactly one instance per correlationId (CG round 3) and
// re-sends a Subscribe on its redirect / reconnect retry paths.  The gateway
// used to register a SECOND subscription for the re-sent frame: the client
// never binds it, yet the broker pushed its stream for the whole session (an
// Earliest subscription replayed the log twice).  A duplicate Subscribe must
// re-answer the existing subscription's SubscribeResponse (same
// subscriptionId) and create nothing — on the gateway-inline push path, the
// owning worker's Subscribe op, and its queued (pendingSubOps_) retry.  The
// same correlationId on a NEW session (after reconnect) is a new subscription.
//
// Deterministic: no Aeron, no sleeps — the gateway and worker duty cycles are
// driven by hand; null-egress test sessions are observed via onEgressFrame.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
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

constexpr int64_t kCorr = 4242;   // the client's Subscribe correlationId (= instance identity)

std::vector<uint8_t> subscribeFrame(int64_t sid, int64_t corr, int32_t partition, int64_t fromOffset) {
    pc::Subscribe s;
    s.sessionId      = sid;
    s.correlationId  = corr;
    s.topicId        = 0;
    s.partition      = partition;
    s.partitionKnown = 1;
    s.fromOffset     = fromOffset;
    s.isolation      = 0;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    return buf;
}

std::vector<uint8_t> unsubscribeFrame(int64_t sid, int64_t subId) {
    pc::Unsubscribe u;
    u.sessionId      = sid;
    u.correlationId  = 0;
    u.subscriptionId = subId;
    std::vector<uint8_t> buf(64);
    buf.resize(u.encode(buf.data()));
    return buf;
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-sub-dedupe-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Gateway + 1-topic registry (topic 0, `partitions` partitions); optionally a
// worker pool with per-worker egress active (Subscribes routed to the owner).
struct Rig {
    TempDir                                 dir;
    std::unique_ptr<TopicRegistry>          registry;
    std::unique_ptr<rune::node::WorkerPool> pool;
    ClientGateway                           gw;
    std::vector<pc::SubscribeResponse>      responses;
    std::vector<pc::Records>                records;

    explicit Rig(bool workers, std::size_t ringBytes = 1 << 16, int32_t partitions = 1)
        : gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        rune::node::TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = partitions;
        ts.topicId        = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            if (auto r = pc::SubscribeResponse::decode(b, n)) responses.push_back(*r);
            else if (auto rec = pc::Records::decode(b, n)) records.push_back(*rec);
        });
        if (workers) {
            pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1), ringBytes,
                                                            1 << 16, 16, 16);
            gw.setWorkerPool(pool.get());
            gw.setShardMode(true);
            gw.setPerWorkerEgressActive(true);
        }
    }
    void subscribe(int64_t sid, int64_t corr, int64_t fromOffset = 0, int32_t partition = 0) {
        const auto f = subscribeFrame(sid, corr, partition, fromOffset);
        gw.applySubscribeForTest(f.data(), f.size());
    }
    void unsubscribe(int64_t sid, int64_t subId) {
        const auto f = unsubscribeFrame(sid, subId);
        gw.applyUnsubscribeForTest(f.data(), f.size());
    }
    void append(int32_t records) {
        rune::storage::Log* log = registry->log(0, 0);
        const int64_t base = log->logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, /*baseTimestamp=*/1000LL);
        for (int32_t i = 0; i < records; ++i) b.append("k", "v", 1000LL + i);
        log->append(b.build(), base + records - 1);
    }
    std::size_t workerSubsOf(int64_t sid) {
        std::size_t n = 0;
        for (const auto& s : pool->worker(0).subscriptionsForTest()) n += (s.sessionId == sid);
        return n;
    }
};

} // namespace

// Gateway-inline push path (no workers): an Earliest Subscribe re-sent with the
// same correlationId is re-answered with the SAME subscriptionId and the log is
// pushed ONCE (RED on 382c65d: a second subscription replayed it a second time).
TEST(SubscribeDedupeRegression, DuplicateSubscribeInlineIsOneSubscriptionOneStream) {
    Rig r(/*workers=*/false);
    for (int i = 0; i < 3; ++i) r.append(2);   // offsets 0..5 in 3 batches
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, kCorr, /*Earliest=*/-2);
    r.subscribe(sid, kCorr, /*Earliest=*/-2);   // the client's resend

    ASSERT_EQ(r.responses.size(), 2u) << "every Subscribe is answered";
    EXPECT_EQ(r.responses[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r.responses[1].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(r.responses[1].subscriptionId, r.responses[0].subscriptionId)
        << "the duplicate re-answers the existing subscription";
    EXPECT_EQ(r.responses[1].correlationId, kCorr);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u) << "a duplicate Subscribe creates nothing";

    for (int pass = 0; pass < 8; ++pass) r.gw.pushSubscriptionsForTest();
    ASSERT_EQ(r.records.size(), 3u) << "ONE stream: each batch pushed exactly once";
    for (std::size_t i = 0; i < r.records.size(); ++i) {
        EXPECT_EQ(r.records[i].baseOffset, static_cast<int64_t>(2 * i));
        EXPECT_EQ(r.records[i].subscriptionId, r.responses[0].subscriptionId);
    }
}

// Per-worker (shard) path: the duplicate never reaches the owning worker as a
// second Subscribe op — the worker holds exactly one cursor for the session.
TEST(SubscribeDedupeRegression, DuplicateSubscribeWorkerPathRegistersOneWorkerCursor) {
    Rig r(/*workers=*/true);
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, kCorr);
    r.pool->worker(0).doWork(1);
    r.subscribe(sid, kCorr);   // resend after the first was already applied by the worker
    r.subscribe(sid, kCorr);   // and once more
    r.pool->worker(0).doWork(2);

    ASSERT_EQ(r.responses.size(), 3u);
    EXPECT_EQ(r.responses[1].subscriptionId, r.responses[0].subscriptionId);
    EXPECT_EQ(r.responses[2].subscriptionId, r.responses[0].subscriptionId);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u);
    EXPECT_EQ(r.workerSubsOf(sid), 1u) << "one worker cursor = one pushed stream";
}

// Queued retry path: the worker's op ring is full, so the first Subscribe op is
// parked in pendingSubOps_; the resend must not queue a second one.
TEST(SubscribeDedupeRegression, DuplicateSubscribeWhileQueuedForAFullRingQueuesNothing) {
    Rig r(/*workers=*/true, /*ringBytes=*/1 << 12);
    const int64_t sid = r.gw.injectTestSession(0);
    for (int64_t i = 1000; i < 1200 && r.gw.pendingSubOpsForTest() == 0; ++i)
        r.gw.forwardSubscribeForTest(i, /*sessionId=*/0, 0, 0, 0, 0, false, false);
    ASSERT_GT(r.gw.pendingSubOpsForTest(), 0u) << "the ring is full";
    const std::size_t queuedBefore = r.gw.pendingSubOpsForTest();
    r.subscribe(sid, kCorr);
    ASSERT_EQ(r.gw.pendingSubOpsForTest(), queuedBefore + 1) << "the first Subscribe op is queued";
    r.subscribe(sid, kCorr);
    EXPECT_EQ(r.gw.pendingSubOpsForTest(), queuedBefore + 1) << "the resend queues no second op";

    for (int i = 0; i < 64 && r.gw.pendingSubOpsForTest() != 0; ++i) {
        r.pool->worker(0).doWork(10 + i);
        (void)r.gw.pumpRetryQueuesForTest();
    }
    for (int i = 0; i < 64; ++i) r.pool->worker(0).doWork(100 + i);   // drain the ring
    ASSERT_EQ(r.gw.pendingSubOpsForTest(), 0u);
    EXPECT_EQ(r.workerSubsOf(sid), 1u);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u);
    ASSERT_EQ(r.responses.size(), 2u);
    EXPECT_EQ(r.responses[1].subscriptionId, r.responses[0].subscriptionId);
}

// Reconnect: the same correlationId on a NEW session is a NEW subscription (the
// old session's subscription is dropped with the session).
TEST(SubscribeDedupeRegression, SameCorrelationIdOnANewSessionIsANewSubscription) {
    Rig r(/*workers=*/true);
    const int64_t sid1 = r.gw.injectTestSession(0);
    r.subscribe(sid1, kCorr);
    r.pool->worker(0).doWork(1);
    r.gw.closeSessionForTest(sid1);                       // the client reconnects
    const int64_t sid2 = r.gw.injectTestSession(0);
    ASSERT_NE(sid2, sid1);
    r.subscribe(sid2, kCorr);
    r.pool->worker(0).doWork(2);

    ASSERT_EQ(r.responses.size(), 2u);
    EXPECT_EQ(r.responses[1].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_NE(r.responses[1].subscriptionId, r.responses[0].subscriptionId) << "a new subscription";
    EXPECT_EQ(r.gw.subscriptionCount(), 1u) << "the old session's subscription died with it";
    EXPECT_EQ(r.workerSubsOf(sid1), 0u);
    EXPECT_EQ(r.workerSubsOf(sid2), 1u);
}

// Two live sessions using the same correlationId are independent subscriptions.
TEST(SubscribeDedupeRegression, SameCorrelationIdOnTwoLiveSessionsIsTwoSubscriptions) {
    Rig r(/*workers=*/false);
    const int64_t a = r.gw.injectTestSession(0);
    const int64_t b = r.gw.injectTestSession(0);
    r.subscribe(a, kCorr);
    r.subscribe(b, kCorr);
    ASSERT_EQ(r.responses.size(), 2u);
    EXPECT_NE(r.responses[1].subscriptionId, r.responses[0].subscriptionId);
    EXPECT_EQ(r.gw.subscriptionCount(), 2u);
}

// After an Unsubscribe the correlationId no longer names a live subscription:
// a later Subscribe with it creates a new one (and is pushed again).
TEST(SubscribeDedupeRegression, SubscribeAfterUnsubscribeWithTheSameCorrelationIdIsNew) {
    Rig r(/*workers=*/false);
    r.append(2);
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, kCorr, -2);
    const int64_t first = r.responses.at(0).subscriptionId;
    r.unsubscribe(sid, first);
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
    r.subscribe(sid, kCorr, -2);
    ASSERT_EQ(r.responses.size(), 2u);
    EXPECT_NE(r.responses[1].subscriptionId, first);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u);
    r.gw.pushSubscriptionsForTest();
    ASSERT_EQ(r.records.size(), 1u);
    EXPECT_EQ(r.records[0].subscriptionId, r.responses[1].subscriptionId);
}

// Untagged Subscribes (the client's group carriers send correlationId 0 for
// every assigned partition) for DIFFERENT partitions are different
// subscriptions — the dedupe key includes (topic, partition).
TEST(SubscribeDedupeRegression, SameCorrelationIdForDifferentPartitionsIsTwoSubscriptions) {
    Rig r(/*workers=*/false, 1 << 16, /*partitions=*/2);
    const int64_t sid = r.gw.injectTestSession(0);
    r.subscribe(sid, 0, 0, /*partition=*/0);
    r.subscribe(sid, 0, 0, /*partition=*/1);
    r.subscribe(sid, 0, 0, /*partition=*/1);   // a resend of the partition-1 one
    ASSERT_EQ(r.responses.size(), 3u);
    EXPECT_NE(r.responses[1].subscriptionId, r.responses[0].subscriptionId);
    EXPECT_EQ(r.responses[1].partition, 1);
    EXPECT_EQ(r.responses[2].subscriptionId, r.responses[1].subscriptionId);
    EXPECT_EQ(r.gw.subscriptionCount(), 2u);
}

// Group members: a re-sent isolation carrier stays an update of the ONE group
// subscription (no client subscription is ever created next to it).
TEST(SubscribeDedupeRegression, DuplicateGroupCarrierSubscribeKeepsOneGroupSubscription) {
    Rig r(/*workers=*/true);
    const int64_t sid  = r.gw.injectTestSession(0);
    const int64_t gsub = r.gw.addGroupSubscription(sid, 0, 0, 0, /*isolation=*/1, /*pendingCarrier=*/true);
    ASSERT_GT(gsub, 0);
    r.subscribe(sid, kCorr);
    r.subscribe(sid, kCorr);
    r.pool->worker(0).doWork(1);
    ASSERT_EQ(r.responses.size(), 2u);
    EXPECT_EQ(r.responses[0].subscriptionId, gsub);
    EXPECT_EQ(r.responses[1].subscriptionId, gsub);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u);
    EXPECT_EQ(r.workerSubsOf(sid), 1u);
}
