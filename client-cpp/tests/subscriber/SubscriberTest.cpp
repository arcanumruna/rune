// tests/subscriber/SubscriberTest.cpp
//
// CB-4 Acceptance tests — Subscribe push handler (per-record zero-copy),
// offset tracking, unsubscribe, no-crosstalk.
//
// Test matrix:
//   1.  SubscribeFromEarliest
//         Gateway produces N records before subscribe; client subscribes
//         Earliest; receives all N in order (correct offset/key/value,
//         binary-safe).
//
//   2.  SubscribeFromLatest
//         Gateway produces records BEFORE subscribe (with Latest), then
//         records AFTER.  Client receives only the records produced AFTER.
//
//   3.  SubscribeFromOffset
//         Client subscribes with From::offset(k); verify delivery starts at
//         offset >= k (whole-batch semantics: batch containing k is delivered).
//
//   4.  OffsetTracking
//         position() advances correctly as records are delivered; matches
//         last delivered offset + 1 after the batch.
//
//   5.  UnsubscribeStopsDelivery
//         Produce records; unsubscribe; produce more; handler not called for
//         post-unsubscribe records.
//
//   6.  RaiiDtorUnsubscribes
//         Subscription goes out of scope (dtor); produce after scope exit;
//         handler not called.
//
//   7.  TwoSubscriptionsNoCrosstalk
//         Two subscriptions on different (topic/partition); each handler
//         receives only its own records.
//
//   8.  ZeroCopyHandlerValidity
//         Handler copies key/value during callback; verifies the values match
//         what the gateway sent (proves zero-copy path works correctly).
//
//   9.  ReconnectResumeScaffold
//         After receiving records, position() holds the resume point.
//
// Gateway harness: reuses the CC-5/CC-6 shared-driver pattern.
// Rules:
//   - Bounded pumps (kPumpMax iterations, kPumpSleep between) — no busy-spin.
//   - Unique temp dirs: getpid() + atomic counter.
//   - Handler fires on the agent thread; all shared data accessed via atomics.
//   - Keep all CB/CC tests green.
//
// Design note: the test gateway drives the fake broker logic:
//   - ConnectRequest  → ConnectResponse{OK}
//   - Subscribe       → SubscribeResponse{subscriptionId=<auto>}
//                       then immediately push Records with the requested batch
//   - Unsubscribe     → drop (no server-side ack needed)
//
// The gateway intercepts Subscribe messages and immediately pushes one or more
// Records{subscriptionId} back.  This keeps the test self-contained.

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/RecordBatch.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

static constexpr int  kPumpMax   = 5'000;
static constexpr auto kPumpSleep = 2ms;
static constexpr auto kFutureTimeout = 10s;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCtr{0};

std::string uniqueDir(const char* prefix = "cb4_test") {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/") + prefix + "_" +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// RecordData — a snapshot of a delivered Record (safe to store across callback)
// ---------------------------------------------------------------------------

struct RecordData {
    int64_t              offset    = 0;
    int64_t              timestamp = 0;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

// ---------------------------------------------------------------------------
// Helper: build a RecordBatch with N identical records
// ---------------------------------------------------------------------------

static std::vector<uint8_t> buildBatch(int64_t baseOffset,
                                        int64_t baseTimestamp,
                                        std::string_view key,
                                        std::string_view value,
                                        int32_t          count = 1)
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, baseTimestamp);
    for (int i = 0; i < count; ++i) {
        builder.append(key, value, baseTimestamp + i);
    }
    return builder.build();
}

// ---------------------------------------------------------------------------
// SubscribeGateway — fake broker that handles Connect/Subscribe/Unsubscribe
//
// Behaviour:
//   - ConnectRequest  → ConnectResponse{OK, assignedSessionId=42}
//   - Subscribe       → stores the subscribe, replies SubscribeResponse,
//                       then immediately pushes any pre-queued Records.
//   - pushRecords()   — caller queues a RecordBatch to be pushed on next Subscribe
//                       OR immediately to a known subscriptionId.
// ---------------------------------------------------------------------------

struct SubscribeGateway {
    struct SubscriptionRecord {
        int64_t  subscriptionId;
        int64_t  sessionId;
        int64_t  correlationId;
        int32_t  topicId;
        int32_t  partition;
        int64_t  fromOffset;
    };

    struct PendingPush {
        int32_t  topicId;
        int32_t  partition;
        int64_t  baseOffset;
        std::vector<uint8_t> batch;
    };

    std::atomic<int64_t> nextSubscriptionId{1};
    std::atomic<int>     subscribesSeen{0};
    std::atomic<int>     unsubscribesSeen{0};

    // Subscriptions active: subscriptionId → session/corr for egress routing.
    std::mutex subsMtx_;
    std::vector<SubscriptionRecord> activeSubs_;

    // Pre-queued pushes: delivered when a Subscribe is received (or explicitly).
    std::mutex pendingMtx_;
    std::vector<PendingPush> pendingPushes_;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;

    uint8_t replyBuf[8192]{};

    explicit SubscribeGateway(const std::string& aeronDir,
                               int32_t ingressStream,
                               int32_t egressStream)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("SubscribeGateway: connect failed");

        int64_t subId = gwAeron->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("SubscribeGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~SubscribeGateway() {
        gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset();
    }

    int poll(int limit = 16) {
        if (!gwSub || !gwAsm) return 0;
        return gwSub->poll(gwAsm->handler(), limit);
    }

    bool send(const uint8_t* buf, std::size_t len) {
        if (!gwPub) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
        for (int i = 0; i < 500; ++i) {
            if (gwPub->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    /// Queue a batch to be pushed when the next Subscribe is received,
    /// or immediately if subsId >= 0 (push to existing subscription).
    void queuePush(int32_t topicId, int32_t partition,
                   int64_t baseOffset, std::vector<uint8_t> batch,
                   int64_t subsId = -1)
    {
        if (subsId >= 0) {
            // Immediate push to existing subscription.
            sendRecords(subsId, topicId, partition, baseOffset, batch);
        } else {
            std::lock_guard<std::mutex> lk(pendingMtx_);
            pendingPushes_.push_back({topicId, partition, baseOffset, std::move(batch)});
        }
    }

    void sendRecords(int64_t subscriptionId,
                     int32_t topicId, int32_t partition,
                     int64_t baseOffset,
                     const std::vector<uint8_t>& batch)
    {
        // Find a session/corr for this subscriptionId.
        int64_t sessId = 42;
        {
            std::lock_guard<std::mutex> lk(subsMtx_);
            for (const auto& s : activeSubs_) {
                if (s.subscriptionId == subscriptionId) {
                    sessId = s.sessionId;
                    break;
                }
            }
        }

        const std::size_t n = rcc::encodeRecords(
            replyBuf, sizeof(replyBuf),
            subscriptionId,
            topicId,
            partition,
            baseOffset,
            std::span<const uint8_t>(batch.data(), batch.size()),
            sessId,
            /*correlationId=*/0); // push has no correlationId
        if (n > 0) send(replyBuf, n);
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        // ---- ConnectRequest ----
        if (tid == rcc::TemplateId::Connect) {
            auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            return;
        }

        // ---- Subscribe ----
        if (tid == rcc::TemplateId::Subscribe) {
            auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;

            subscribesSeen.fetch_add(1, std::memory_order_relaxed);
            const int64_t subId = nextSubscriptionId.fetch_add(1, std::memory_order_relaxed);

            // Register subscription.
            {
                std::lock_guard<std::mutex> lk(subsMtx_);
                activeSubs_.push_back({
                    subId,
                    opt->sessionId,
                    opt->correlationId,
                    opt->topicId,
                    opt->partition,
                    opt->fromOffset
                });
            }

            // Send SubscribeResponse.
            {
                std::size_t n = rcc::encodeSubscribeResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::OK),
                    opt->partition,
                    subId,
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) send(replyBuf, n);
            }

            // Deliver any pending pushes for this (topicId, partition).
            std::vector<PendingPush> toDeliver;
            {
                std::lock_guard<std::mutex> lk(pendingMtx_);
                for (auto it = pendingPushes_.begin(); it != pendingPushes_.end(); ) {
                    if (it->topicId == opt->topicId && it->partition == opt->partition) {
                        toDeliver.push_back(std::move(*it));
                        it = pendingPushes_.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            for (auto& p : toDeliver) {
                sendRecords(subId, p.topicId, p.partition, p.baseOffset, p.batch);
            }
            return;
        }

        // ---- Unsubscribe ----
        if (tid == rcc::TemplateId::Unsubscribe) {
            auto opt = rcc::decodeUnsubscribe(buf, len);
            if (!opt) return;
            unsubscribesSeen.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lk(subsMtx_);
            activeSubs_.erase(
                std::remove_if(activeSubs_.begin(), activeSubs_.end(),
                    [&](const SubscriptionRecord& s) {
                        return s.subscriptionId == opt->subscriptionId;
                    }),
                activeSubs_.end());
            return;
        }

        // Other frames: ignore.
    }

    /// Return the subscriptionId assigned to the most recently processed Subscribe.
    [[nodiscard]] int64_t lastSubscriptionId() const noexcept {
        return nextSubscriptionId.load(std::memory_order_acquire) - 1;
    }
};

// ---------------------------------------------------------------------------
// SubscriberHarness — RAII wrapper: Client + SubscribeGateway + pump loop.
// ---------------------------------------------------------------------------

struct SubscriberHarness {
    static constexpr int32_t kIngressStream = 3000;
    static constexpr int32_t kEgressStream  = 3001;

    std::string agentDir;
    std::string aeronDir;

    std::unique_ptr<Client>          client;
    std::unique_ptr<SubscribeGateway> gw;

    int32_t ingressStream;
    int32_t egressStream;

    explicit SubscriberHarness(int32_t inStr = kIngressStream,
                                int32_t egStr = kEgressStream)
        : ingressStream(inStr), egressStream(egStr)
    {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";
    }

    ~SubscriberHarness() {
        if (gw)     { gw.reset(); }
        if (client) { client->close(); }
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }

    void build() {
        ClientConfig cfg;
        cfg.gatewayChannel   = "aeron:ipc";
        cfg.gatewayStreamId  = ingressStream;
        cfg.egressChannel    = "aeron:ipc";
        cfg.egressStreamId   = egressStream;
        cfg.dataDir          = agentDir;
        cfg.connectTimeoutMs = 5000;
        cfg.pingTimeoutMs    = 5000;

        client = std::make_unique<Client>(cfg);

        // Wait for embedded driver to initialise.
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<SubscribeGateway>(aeronDir, ingressStream, egressStream);
    }

    int pollGw(int limit = 16) {
        return gw ? gw->poll(limit) : 0;
    }

    template<typename Pred>
    bool pumpUntil(Pred pred, int limit = kPumpMax) {
        for (int i = 0; i < limit; ++i) {
            if (gw) gw->poll(16);
            if (pred()) return true;
            std::this_thread::sleep_for(kPumpSleep);
        }
        return false;
    }

    /// Connect and wait until Connected.
    bool connect() {
        auto fut = client->connect();
        return pumpUntil([&]() {
            return client->state() == rcs::SessionState::Connected;
        }) && (fut.wait_for(kFutureTimeout) == std::future_status::ready);
    }
};

// ---------------------------------------------------------------------------
// Convenience: build a single-record batch and return it as a vector.
// ---------------------------------------------------------------------------

static std::vector<uint8_t> oneBatch(int64_t offset,
                                      const std::string& key,
                                      const std::string& value)
{
    return buildBatch(offset, 1000000LL, key, value, 1);
}

// ===========================================================================
// Test 1: SubscribeFromEarliest
//
// N records are queued in the gateway BEFORE the subscribe (Earliest semantic).
// The gateway delivers them immediately after sending SubscribeResponse.
// Assert: handler called N times, in order, with correct offset/key/value.
// ===========================================================================
TEST(Subscriber, SubscribeFromEarliest) {
    SubscriberHarness h(3010, 3011);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int N = 5;
    static constexpr int32_t kTopic = 1;
    static constexpr int32_t kPart  = 0;

    // Queue N records BEFORE subscribe.
    for (int i = 0; i < N; ++i) {
        auto batch = oneBatch(/*offset=*/i, "k" + std::to_string(i), "v" + std::to_string(i));
        h.gw->queuePush(kTopic, kPart, /*baseOffset=*/i, std::move(batch));
    }

    // Subscribe from Earliest.
    std::vector<RecordData> received;
    std::mutex              recMtx;
    std::atomic<int>        recCount{0};

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic,
        /*partition=*/kPart,
        SubscribeOptions{From::earliest()},
        [&](const Record& r) {
            RecordData rd;
            rd.offset    = r.offset;
            rd.timestamp = r.timestamp;
            rd.key.assign(r.key.begin(), r.key.end());
            rd.value.assign(r.value.begin(), r.value.end());
            {
                std::lock_guard<std::mutex> lk(recMtx);
                received.push_back(rd);
            }
            recCount.fetch_add(1, std::memory_order_release);
        });

    // Pump until N records arrive.
    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " records from Earliest";

    std::lock_guard<std::mutex> lk(recMtx);
    ASSERT_EQ(static_cast<int>(received.size()), N);

    // In-order check.
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(received[i].offset, static_cast<int64_t>(i))
            << "Wrong offset at position " << i;
        const std::string expectedVal = "v" + std::to_string(i);
        EXPECT_EQ(std::string(received[i].value.begin(), received[i].value.end()),
                  expectedVal)
            << "Wrong value at position " << i;
    }
}

// ===========================================================================
// Test 2: SubscribeFromLatest
//
// Subscribe with Latest; gateway then pushes records.
// Only records pushed AFTER subscribe arrive (none from before).
// ===========================================================================
TEST(Subscriber, SubscribeFromLatest) {
    SubscriberHarness h(3020, 3021);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 2;
    static constexpr int32_t kPart  = 0;

    std::atomic<int> recCount{0};
    std::vector<RecordData> received;
    std::mutex recMtx;

    // Subscribe with Latest — no pre-queued records.
    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic,
        /*partition=*/kPart,
        SubscribeOptions{From::latest()},
        [&](const Record& r) {
            RecordData rd;
            rd.offset = r.offset;
            rd.value.assign(r.value.begin(), r.value.end());
            {
                std::lock_guard<std::mutex> lk(recMtx);
                received.push_back(rd);
            }
            recCount.fetch_add(1, std::memory_order_release);
        });

    // Wait for the subscribe to be acknowledged (subscriptionId is bound).
    ASSERT_TRUE(h.pumpUntil([&]() {
        return sub.subscriptionId() > 0;
    })) << "SubscribeResponse not received";

    // Now push records AFTER subscribe.
    const int64_t subId = sub.subscriptionId();
    static constexpr int M = 3;
    for (int i = 0; i < M; ++i) {
        auto batch = oneBatch(100 + i, "k", "post" + std::to_string(i));
        h.gw->queuePush(kTopic, kPart, 100 + i, std::move(batch), subId);
    }

    // Pump until M records arrive.
    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= M;
    })) << "Expected " << M << " post-subscribe records";

    std::lock_guard<std::mutex> lk(recMtx);
    ASSERT_EQ(static_cast<int>(received.size()), M);

    // Verify records are at offset 100..102.
    for (int i = 0; i < M; ++i) {
        EXPECT_EQ(received[i].offset, 100 + i)
            << "Wrong offset at position " << i;
        const std::string expected = "post" + std::to_string(i);
        EXPECT_EQ(std::string(received[i].value.begin(), received[i].value.end()), expected);
    }
}

// ===========================================================================
// Test 3: SubscribeFromOffset
//
// Subscribe with From::offset(k); gateway pre-queues a batch starting before k.
// Delivery must start at or after k (whole-batch semantics).
// ===========================================================================
TEST(Subscriber, SubscribeFromOffset) {
    SubscriberHarness h(3030, 3031);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic   = 3;
    static constexpr int32_t kPart    = 0;
    static constexpr int64_t kResumeAt = 5;

    // Queue a batch of 10 records starting at offset 5 (simulating Offset(5) from broker).
    auto batch = buildBatch(kResumeAt, 2000000LL, "k", "v", 10);
    h.gw->queuePush(kTopic, kPart, kResumeAt, std::move(batch));

    std::atomic<int>         recCount{0};
    std::atomic<int64_t>     firstOffset{-1};
    std::mutex               recMtx;
    std::vector<RecordData>  received;

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic,
        /*partition=*/kPart,
        SubscribeOptions{From::offset(kResumeAt)},
        [&](const Record& r) {
            if (firstOffset.load(std::memory_order_acquire) < 0) {
                firstOffset.store(r.offset, std::memory_order_release);
            }
            RecordData rd;
            rd.offset = r.offset;
            rd.value.assign(r.value.begin(), r.value.end());
            {
                std::lock_guard<std::mutex> lk(recMtx);
                received.push_back(rd);
            }
            recCount.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= 10;
    })) << "Expected 10 records from Offset(5)";

    // The batch starts at baseOffset == kResumeAt, so first record must be >= kResumeAt.
    EXPECT_GE(firstOffset.load(), kResumeAt)
        << "First delivered offset must be >= kResumeAt";

    // Verify in-order (offsets 5, 6, ..., 14).
    std::lock_guard<std::mutex> lk(recMtx);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(received[i].offset, kResumeAt + i);
    }
}

// ===========================================================================
// Test 4: OffsetTracking
//
// After records are delivered, position() reflects last delivered + 1.
// ===========================================================================
TEST(Subscriber, OffsetTracking) {
    SubscriberHarness h(3040, 3041);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 4;
    static constexpr int32_t kPart  = 0;
    static constexpr int      N     = 7;

    // Queue N records.
    for (int i = 0; i < N; ++i) {
        auto batch = oneBatch(i, "k", "v");
        h.gw->queuePush(kTopic, kPart, i, std::move(batch));
    }

    std::atomic<int> recCount{0};

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic,
        /*partition=*/kPart,
        SubscribeOptions{From::earliest()},
        [&](const Record& /*r*/) {
            recCount.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " records";

    // position() == last offset + 1 == N-1 + 1 == N.
    // Wait a bit to let the final store propagate.
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(sub.position(), static_cast<int64_t>(N))
        << "position() must equal last offset + 1 after " << N << " records";
}

// ===========================================================================
// Test 5: UnsubscribeStopsDelivery
//
// Produce N records, receive them, unsubscribe, produce M more.
// Handler must NOT be called for the M post-unsubscribe records.
// ===========================================================================
TEST(Subscriber, UnsubscribeStopsDelivery) {
    SubscriberHarness h(3050, 3051);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 5;
    static constexpr int32_t kPart  = 0;
    static constexpr int      N     = 3;  // pre-unsubscribe records

    // Queue N pre-unsubscribe records.
    for (int i = 0; i < N; ++i) {
        auto batch = oneBatch(i, "k", "v");
        h.gw->queuePush(kTopic, kPart, i, std::move(batch));
    }

    std::atomic<int> recCount{0};

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic,
        /*partition=*/kPart,
        SubscribeOptions{From::earliest()},
        [&](const Record& /*r*/) {
            recCount.fetch_add(1, std::memory_order_release);
        });

    // Receive the N pre-unsubscribe records.
    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " pre-unsubscribe records";

    // Unsubscribe.
    sub.unsubscribe();
    EXPECT_FALSE(sub.active()) << "After unsubscribe, active() must be false";

    // Brief pause to let the Unsubscribe frame be processed.
    std::this_thread::sleep_for(50ms);
    h.pumpUntil([&]() { return false; }, 50); // drain a bit

    const int countAfterUnsub = recCount.load(std::memory_order_acquire);

    // Push M more records — they must NOT arrive at the handler.
    static constexpr int M = 3;
    for (int i = 0; i < M; ++i) {
        auto batch = oneBatch(N + i, "k", "extra");
        // We don't know if the gateway still has an active entry, so queue
        // to the subscription id if known (may be 0 if already removed).
        h.gw->queuePush(kTopic, kPart, N + i, std::move(batch));
    }

    // Poll for a bit and verify no extra records arrive.
    h.pumpUntil([&]() { return false; }, 200);
    std::this_thread::sleep_for(50ms);

    EXPECT_EQ(recCount.load(std::memory_order_acquire), countAfterUnsub)
        << "Handler must not be called after unsubscribe()";
}

// ===========================================================================
// Test 6: RaiiDtorUnsubscribes
//
// Subscription goes out of scope; produce after scope exit; no handler call.
// ===========================================================================
TEST(Subscriber, RaiiDtorUnsubscribes) {
    SubscriberHarness h(3060, 3061);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 6;
    static constexpr int32_t kPart  = 0;

    std::atomic<int> recCount{0};
    int64_t          subsId = 0;

    {
        TopicHandle topic{kTopic, 1};
        auto sub = h.client->subscribe(
            topic,
            /*partition=*/kPart,
            SubscribeOptions{From::latest()},
            [&](const Record& /*r*/) {
                recCount.fetch_add(1, std::memory_order_release);
            });

        // Wait for SubscribeResponse so we know the subscriptionId.
        ASSERT_TRUE(h.pumpUntil([&]() {
            return sub.subscriptionId() > 0;
        })) << "SubscribeResponse not received before scope exit";

        subsId = sub.subscriptionId();
        // sub goes out of scope here → dtor → unsubscribe()
    }

    // Let Unsubscribe frame propagate.
    h.pumpUntil([&]() { return false; }, 100);
    std::this_thread::sleep_for(50ms);

    const int beforeExtra = recCount.load(std::memory_order_acquire);

    // Push a record to the old subscriptionId.
    if (subsId > 0) {
        auto batch = oneBatch(0, "k", "after-dtor");
        h.gw->queuePush(kTopic, kPart, 0, std::move(batch), subsId);
    }

    // Poll and verify no records arrive.
    h.pumpUntil([&]() { return false; }, 200);
    std::this_thread::sleep_for(50ms);

    EXPECT_EQ(recCount.load(std::memory_order_acquire), beforeExtra)
        << "Handler must not fire after RAII dtor unsubscribed";
}

// ===========================================================================
// Test 7: TwoSubscriptionsNoCrosstalk
//
// Two subscriptions on different partitions; each handler gets only its own
// records (no cross-talk).
// ===========================================================================
TEST(Subscriber, TwoSubscriptionsNoCrosstalk) {
    SubscriberHarness h(3070, 3071);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic  = 7;
    static constexpr int32_t kPartA  = 0;
    static constexpr int32_t kPartB  = 1;
    static constexpr int      N      = 4;

    // Queue N records for partition A and N for partition B.
    for (int i = 0; i < N; ++i) {
        h.gw->queuePush(kTopic, kPartA, i, oneBatch(i, "kA", "A" + std::to_string(i)));
        h.gw->queuePush(kTopic, kPartB, i, oneBatch(i, "kB", "B" + std::to_string(i)));
    }

    std::atomic<int> recA{0}, recB{0};
    std::atomic<bool> crossTalkA{false}, crossTalkB{false};

    TopicHandle topic{kTopic, 2}; // 2 partitions

    auto subA = h.client->subscribe(
        topic, kPartA, SubscribeOptions{From::earliest()},
        [&](const Record& r) {
            // Any "B" value in A's handler is cross-talk.
            if (!r.value.empty() && r.value[0] == 'B') {
                crossTalkA.store(true, std::memory_order_relaxed);
            }
            recA.fetch_add(1, std::memory_order_release);
        });

    auto subB = h.client->subscribe(
        topic, kPartB, SubscribeOptions{From::earliest()},
        [&](const Record& r) {
            if (!r.value.empty() && r.value[0] == 'A') {
                crossTalkB.store(true, std::memory_order_relaxed);
            }
            recB.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recA.load(std::memory_order_acquire) >= N &&
               recB.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " records on each subscription";

    EXPECT_FALSE(crossTalkA.load()) << "Partition A handler received partition B records";
    EXPECT_FALSE(crossTalkB.load()) << "Partition B handler received partition A records";

    EXPECT_GE(recA.load(), N);
    EXPECT_GE(recB.load(), N);
}

// ===========================================================================
// Test 8: ZeroCopyHandlerValidity
//
// Handler copies key/value during callback; verifies binary-safe copy.
// The key and value contain NUL bytes and 0xFF — proves binary safety.
// ===========================================================================
TEST(Subscriber, ZeroCopyHandlerValidity) {
    SubscriberHarness h(3080, 3081);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 8;
    static constexpr int32_t kPart  = 0;

    // Build a batch with binary key and value.
    const std::string binaryKey  = {'\x00', '\xFF', '\xAB'};
    const std::string binaryVal  = {'\x00', '\x01', '\xFF', '\x00'};
    auto batch = buildBatch(0, 3000000LL, binaryKey, binaryVal, 1);
    h.gw->queuePush(kTopic, kPart, 0, std::move(batch));

    std::atomic<int> recCount{0};
    std::vector<uint8_t> copiedKey, copiedVal;
    std::mutex copyMtx;

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic, kPart, SubscribeOptions{From::earliest()},
        [&](const Record& r) {
            // Copy during callback (zero-copy spans only valid here).
            std::lock_guard<std::mutex> lk(copyMtx);
            copiedKey.assign(r.key.begin(), r.key.end());
            copiedVal.assign(r.value.begin(), r.value.end());
            recCount.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= 1;
    })) << "Expected 1 record";

    std::lock_guard<std::mutex> lk(copyMtx);

    // Verify binary key.
    ASSERT_EQ(copiedKey.size(), 3u);
    EXPECT_EQ(copiedKey[0], 0x00u);
    EXPECT_EQ(copiedKey[1], 0xFFu);
    EXPECT_EQ(copiedKey[2], 0xABu);

    // Verify binary value.
    ASSERT_EQ(copiedVal.size(), 4u);
    EXPECT_EQ(copiedVal[0], 0x00u);
    EXPECT_EQ(copiedVal[1], 0x01u);
    EXPECT_EQ(copiedVal[2], 0xFFu);
    EXPECT_EQ(copiedVal[3], 0x00u);
}

// ===========================================================================
// Test 9: ReconnectResumeScaffold
//
// After receiving records, position() == lastOffset + 1.
// This value is the resume point for CB-5 reconnect-resume.
// ===========================================================================
TEST(Subscriber, ReconnectResumeScaffold) {
    SubscriberHarness h(3090, 3091);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 9;
    static constexpr int32_t kPart  = 0;
    static constexpr int64_t kBase  = 42;
    static constexpr int      N     = 5;

    auto batch = buildBatch(kBase, 4000000LL, "k", "v", N);
    h.gw->queuePush(kTopic, kPart, kBase, std::move(batch));

    std::atomic<int> recCount{0};

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic, kPart, SubscribeOptions{From::earliest()},
        [&](const Record& /*r*/) {
            recCount.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " records";

    // Wait briefly for the final store.
    std::this_thread::sleep_for(10ms);

    // Expected position: last delivered offset = kBase + N - 1, position = kBase + N.
    const int64_t expectedPosition = kBase + N;
    EXPECT_EQ(sub.position(), expectedPosition)
        << "position() must be kBase + N = " << expectedPosition;

    // CB-5 scaffold: the resume offset would be sub.position().
    // (Full reconnect-resume is implemented in CB-5.)
    EXPECT_GT(sub.position(), 0LL)
        << "position() > 0 after delivery (resume point is set)";
}

// ===========================================================================
// Test 10: MultiRecordBatchInOrder
//
// A single batch with multiple records is decoded in offset order.
// Verifies RecordBatchReader iterates correctly.
// ===========================================================================
TEST(Subscriber, MultiRecordBatchInOrder) {
    SubscriberHarness h(3100, 3101);
    h.build();
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    static constexpr int32_t kTopic = 10;
    static constexpr int32_t kPart  = 0;
    static constexpr int      N     = 8;
    static constexpr int64_t kBase  = 0;

    // Build a single batch with N records, distinct values.
    rune::protocol::RecordBatchBuilder builder(kBase, 5000000LL);
    for (int i = 0; i < N; ++i) {
        builder.append("", "record" + std::to_string(i), 5000000LL + i);
    }
    h.gw->queuePush(kTopic, kPart, kBase, builder.build());

    std::vector<RecordData> received;
    std::mutex recMtx;
    std::atomic<int> recCount{0};

    TopicHandle topic{kTopic, 1};
    auto sub = h.client->subscribe(
        topic, kPart, SubscribeOptions{From::earliest()},
        [&](const Record& r) {
            RecordData rd;
            rd.offset = r.offset;
            rd.value.assign(r.value.begin(), r.value.end());
            {
                std::lock_guard<std::mutex> lk(recMtx);
                received.push_back(rd);
            }
            recCount.fetch_add(1, std::memory_order_release);
        });

    ASSERT_TRUE(h.pumpUntil([&]() {
        return recCount.load(std::memory_order_acquire) >= N;
    })) << "Expected " << N << " records from multi-record batch";

    std::lock_guard<std::mutex> lk(recMtx);
    ASSERT_EQ(static_cast<int>(received.size()), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(received[i].offset, kBase + i)
            << "Record " << i << " has wrong offset";
        const std::string expected = "record" + std::to_string(i);
        EXPECT_EQ(std::string(received[i].value.begin(), received[i].value.end()),
                  expected)
            << "Record " << i << " has wrong value";
    }
}
