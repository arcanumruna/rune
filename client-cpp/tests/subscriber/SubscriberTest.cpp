// tests/subscriber/SubscriberTest.cpp
//
// CB-4 Acceptance tests — Subscribe push handler (per-record zero-copy),
// offset tracking, unsubscribe, no-crosstalk.
//
// Test matrix:
//   1.  SubscribeFromEarliest     — every pushed record arrives in order
//                                    (offset / key / value, binary-safe); the
//                                    Subscribe carries the Earliest sentinel.
//   2.  SubscribeFromLatest       — records pushed after the bind arrive; the
//                                    Subscribe carries the Latest sentinel.
//   3.  SubscribeFromOffset       — the Subscribe carries k; delivery starts at k.
//   4.  OffsetTracking            — position() == last delivered + 1.
//   5.  UnsubscribeStopsDelivery  — no handler call after unsubscribe().
//   6.  RaiiDtorUnsubscribes      — the handle's destructor unsubscribes.
//   7.  TwoSubscriptionsNoCrosstalk
//   8.  ZeroCopyHandlerValidity   — binary key / value copied in the callback.
//   9.  ReconnectResumeScaffold   — position() is the resume point.
//  10.  MultiRecordBatchInOrder
//  11.  UnsubscribeResponseRace   — unsubscribe() before the SubscribeResponse:
//                                    the late response sends the real
//                                    Unsubscribe, nothing is delivered.
//
// C-F24 (2026-10-09): rebuilt on the Invoker runner + the single-threaded
// InvokerGateway.  The test thread is the client's duty thread AND the fake
// broker: no gateway thread, no sleeps, no atomics, no mutex.  Every wait is a
// drive loop bounded by a deadline only a FAILING test reaches; an "absence"
// check (no record after unsubscribe) runs a fixed number of full client +
// gateway cycles after the frame that would carry the record was sent.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/subscriber/Subscription.h>

#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::client;

namespace {

int g_dirCtr = 0;  // the gtest main thread only

std::string uniqueDir() {
    return std::string("/tmp/cb4_test_") + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(g_dirCtr++);
}

/// A snapshot of a delivered Record (safe to keep after the callback).
struct RecordData {
    int64_t              offset    = 0;
    int64_t              timestamp = 0;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

std::vector<uint8_t> buildBatch(int64_t baseOffset, int64_t baseTimestamp, std::string_view key,
                                std::string_view value, int32_t count = 1) {
    rune::protocol::RecordBatchBuilder builder(baseOffset, baseTimestamp);
    for (int i = 0; i < count; ++i) builder.append(key, value, baseTimestamp + i);
    return builder.build();
}

std::vector<uint8_t> oneBatch(int64_t offset, const std::string& key, const std::string& value) {
    return buildBatch(offset, 1'000'000LL, key, value, 1);
}

/// Client (Invoker runner: the test thread drives it) + the fake broker.
struct SubscriberHarness {
    std::string                  dir;
    ClientConfig                 cfg;
    Client                       client;
    test_support::InvokerGateway gw;

    static ClientConfig makeCfg(const std::string& dir, int32_t in, int32_t eg) {
        ClientConfig c;
        c.gatewayChannel   = "aeron:ipc";
        c.gatewayStreamId  = in;
        c.egressChannel    = "aeron:ipc";
        c.egressStreamId   = eg;
        c.dataDir          = dir;
        c.connectTimeoutMs = 5000;
        c.pingTimeoutMs    = 5000;
        c.runnerMode       = RunnerMode::Invoker;
        return c;
    }

    SubscriberHarness(int32_t in, int32_t eg)
        : dir(uniqueDir()), cfg(makeCfg(dir, in, eg)), client(cfg),
          gw(dir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId, cfg.egressChannel, cfg.egressStreamId) {}

    ~SubscriberHarness() {
        client.close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    /// Drive client + gateway until pred holds; the 10 s deadline only ends a FAILING test.
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    /// A fixed number of full client + gateway cycles (lets in-flight frames land).
    void settle(int cycles = 2'000) {
        for (int i = 0; i < cycles; ++i) {
            client.doWork();
            gw.pump();
        }
    }

    test_support::TestCq cq{client};
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        auto f = cq.issue([&](Completions& q, Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }

    /// The broker-side record of the n-th Subscribe (waits until it arrived).
    const test_support::InvokerGateway::Subscribed& subscribed(std::size_t n) {
        EXPECT_TRUE(drive([&] { return gw.subscribed.size() > n; })) << "Subscribe #" << n << " never arrived";
        return gw.subscribed.at(n);
    }
};

/// Records collected by a handler (it runs on this thread: the Invoker owner).
struct Collector {
    std::vector<RecordData> got;
    RecordHandler handler() {
        return [this](const Record& r) {
            RecordData rd;
            rd.offset    = r.offset;
            rd.timestamp = r.timestamp;
            rd.key.assign(r.key.begin(), r.key.end());
            rd.value.assign(r.value.begin(), r.value.end());
            got.push_back(std::move(rd));
        };
    }
    [[nodiscard]] int count() const { return static_cast<int>(got.size()); }
};

std::string str(const std::vector<uint8_t>& v) { return std::string(v.begin(), v.end()); }

constexpr int64_t kWireEarliest = -2;
constexpr int64_t kWireLatest   = -1;

} // namespace

// ===========================================================================
// 1. SubscribeFromEarliest — the broker pushes N single-record batches right
// after the Subscribe; all arrive in order with the right offset / value.
// ===========================================================================
TEST(Subscriber, SubscribeFromEarliest) {
    SubscriberHarness h(3010, 3011);
    h.connect();
    constexpr int N = 5;
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{1, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    const auto s = h.subscribed(0);
    EXPECT_EQ(s.fromOffset, kWireEarliest);
    for (int i = 0; i < N; ++i) h.gw.pushBatch(s, i, oneBatch(i, "k" + std::to_string(i), "v" + std::to_string(i)));

    ASSERT_TRUE(h.drive([&] { return c.count() >= N; })) << "expected " << N << " records from Earliest";
    ASSERT_EQ(c.count(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(c.got[i].offset, i) << "wrong offset at " << i;
        EXPECT_EQ(str(c.got[i].key), "k" + std::to_string(i));
        EXPECT_EQ(str(c.got[i].value), "v" + std::to_string(i)) << "wrong value at " << i;
    }
}

// ===========================================================================
// 2. SubscribeFromLatest — records pushed after the bind arrive.
// ===========================================================================
TEST(Subscriber, SubscribeFromLatest) {
    SubscriberHarness h(3020, 3021);
    h.connect();
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{2, 1}, 0, SubscribeOptions{From::latest()}, c.handler());
    ASSERT_TRUE(h.drive([&] { return sub.subscriptionId() > 0; })) << "SubscribeResponse not received";
    const auto s = h.subscribed(0);
    EXPECT_EQ(s.fromOffset, kWireLatest);
    constexpr int M = 3;
    for (int i = 0; i < M; ++i) h.gw.pushBatch(s, 100 + i, oneBatch(100 + i, "k", "post" + std::to_string(i)));

    ASSERT_TRUE(h.drive([&] { return c.count() >= M; })) << "expected " << M << " post-subscribe records";
    ASSERT_EQ(c.count(), M);
    for (int i = 0; i < M; ++i) {
        EXPECT_EQ(c.got[i].offset, 100 + i);
        EXPECT_EQ(str(c.got[i].value), "post" + std::to_string(i));
    }
}

// ===========================================================================
// 3. SubscribeFromOffset — the Subscribe carries k; a batch starting at k is
// delivered from k, in order.
// ===========================================================================
TEST(Subscriber, SubscribeFromOffset) {
    SubscriberHarness h(3030, 3031);
    h.connect();
    constexpr int64_t kResumeAt = 5;
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{3, 1}, 0, SubscribeOptions{From::offset(kResumeAt)}, c.handler());
    const auto s = h.subscribed(0);
    EXPECT_EQ(s.fromOffset, kResumeAt);
    h.gw.pushBatch(s, kResumeAt, buildBatch(kResumeAt, 2'000'000LL, "k", "v", 10));

    ASSERT_TRUE(h.drive([&] { return c.count() >= 10; })) << "expected 10 records from Offset(5)";
    for (int i = 0; i < 10; ++i) EXPECT_EQ(c.got[i].offset, kResumeAt + i);
}

// ===========================================================================
// 4. OffsetTracking — position() == last delivered offset + 1.
// ===========================================================================
TEST(Subscriber, OffsetTracking) {
    SubscriberHarness h(3040, 3041);
    h.connect();
    constexpr int N = 7;
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{4, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    const auto s = h.subscribed(0);
    for (int i = 0; i < N; ++i) h.gw.pushBatch(s, i, oneBatch(i, "k", "v"));

    ASSERT_TRUE(h.drive([&] { return c.count() >= N; }));
    // Read on the duty thread (this one): the duty side's own value, no lag.
    EXPECT_EQ(sub.position(), static_cast<int64_t>(N));
}

// ===========================================================================
// 5. UnsubscribeStopsDelivery — after unsubscribe() the handler never fires,
// even for records the broker still pushes to the old subscription.
// ===========================================================================
TEST(Subscriber, UnsubscribeStopsDelivery) {
    SubscriberHarness h(3050, 3051);
    h.connect();
    constexpr int N = 3;
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{5, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    const auto s = h.subscribed(0);
    for (int i = 0; i < N; ++i) h.gw.pushBatch(s, i, oneBatch(i, "k", "v"));
    ASSERT_TRUE(h.drive([&] { return c.count() >= N; }));

    sub.unsubscribe();
    EXPECT_FALSE(sub.active()) << "after unsubscribe(), active() must be false";
    ASSERT_TRUE(h.drive([&] { return !h.gw.unsubscribed.empty(); })) << "no Unsubscribe reached the broker";
    EXPECT_EQ(h.gw.unsubscribed[0], s.subscriptionId);

    for (int i = 0; i < 3; ++i) h.gw.pushBatch(s, N + i, oneBatch(N + i, "k", "extra"));
    h.settle();
    EXPECT_EQ(c.count(), N) << "handler called after unsubscribe()";
}

// ===========================================================================
// 6. RaiiDtorUnsubscribes — the handle's destructor unsubscribes.
// ===========================================================================
TEST(Subscriber, RaiiDtorUnsubscribes) {
    SubscriberHarness h(3060, 3061);
    h.connect();
    Collector c;
    int64_t subsId = 0;
    {
        auto sub = h.client.subscribe(TopicHandle{6, 1}, 0, SubscribeOptions{From::latest()}, c.handler());
        ASSERT_TRUE(h.drive([&] { return sub.subscriptionId() > 0; })) << "SubscribeResponse not received";
        subsId = sub.subscriptionId();
    }  // dtor -> unsubscribe
    ASSERT_TRUE(h.drive([&] { return !h.gw.unsubscribed.empty(); })) << "the destructor sent no Unsubscribe";
    EXPECT_EQ(h.gw.unsubscribed[0], subsId);

    h.gw.pushBatch(h.gw.subscribed.at(0), 0, oneBatch(0, "k", "after-dtor"));
    h.settle();
    EXPECT_EQ(c.count(), 0) << "handler fired after the destructor unsubscribed";
}

// ===========================================================================
// 7. TwoSubscriptionsNoCrosstalk — each handler gets only its own partition.
// ===========================================================================
TEST(Subscriber, TwoSubscriptionsNoCrosstalk) {
    SubscriberHarness h(3070, 3071);
    h.connect();
    constexpr int N = 4;
    const TopicHandle topic{7, 2};
    Collector a, b;
    auto subA = h.client.subscribe(topic, 0, SubscribeOptions{From::earliest()}, a.handler());
    auto subB = h.client.subscribe(topic, 1, SubscribeOptions{From::earliest()}, b.handler());
    const auto s0 = h.subscribed(0);
    const auto s1 = h.subscribed(1);
    const auto& sA = s0.partition == 0 ? s0 : s1;
    const auto& sB = s0.partition == 0 ? s1 : s0;
    ASSERT_EQ(sA.partition, 0);
    ASSERT_EQ(sB.partition, 1);
    for (int i = 0; i < N; ++i) {
        h.gw.pushBatch(sA, i, oneBatch(i, "kA", "A" + std::to_string(i)));
        h.gw.pushBatch(sB, i, oneBatch(i, "kB", "B" + std::to_string(i)));
    }

    ASSERT_TRUE(h.drive([&] { return a.count() >= N && b.count() >= N; }))
        << "expected " << N << " records on each subscription";
    h.settle(200);
    EXPECT_EQ(a.count(), N);
    EXPECT_EQ(b.count(), N);
    for (const auto& r : a.got) EXPECT_EQ(r.value.at(0), 'A') << "partition A handler got partition B records";
    for (const auto& r : b.got) EXPECT_EQ(r.value.at(0), 'B') << "partition B handler got partition A records";
}

// ===========================================================================
// 8. ZeroCopyHandlerValidity — binary key / value (NUL, 0xFF) copied inside
// the callback match what the broker sent.
// ===========================================================================
TEST(Subscriber, ZeroCopyHandlerValidity) {
    SubscriberHarness h(3080, 3081);
    h.connect();
    const std::string binaryKey = {'\x00', '\xFF', '\xAB'};
    const std::string binaryVal = {'\x00', '\x01', '\xFF', '\x00'};
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{8, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    h.gw.pushBatch(h.subscribed(0), 0, buildBatch(0, 3'000'000LL, binaryKey, binaryVal, 1));

    ASSERT_TRUE(h.drive([&] { return c.count() >= 1; })) << "expected 1 record";
    EXPECT_EQ(c.got[0].key, (std::vector<uint8_t>{0x00, 0xFF, 0xAB}));
    EXPECT_EQ(c.got[0].value, (std::vector<uint8_t>{0x00, 0x01, 0xFF, 0x00}));
}

// ===========================================================================
// 9. ReconnectResumeScaffold — position() == kBase + N (the resume point).
// ===========================================================================
TEST(Subscriber, ReconnectResumeScaffold) {
    SubscriberHarness h(3090, 3091);
    h.connect();
    constexpr int64_t kBase = 42;
    constexpr int     N     = 5;
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{9, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    h.gw.pushBatch(h.subscribed(0), kBase, buildBatch(kBase, 4'000'000LL, "k", "v", N));

    ASSERT_TRUE(h.drive([&] { return c.count() >= N; }));
    EXPECT_EQ(sub.position(), kBase + N) << "position() must be kBase + N";
}

// ===========================================================================
// 10. MultiRecordBatchInOrder — one batch of N records decodes in offset order.
// ===========================================================================
TEST(Subscriber, MultiRecordBatchInOrder) {
    SubscriberHarness h(3100, 3101);
    h.connect();
    constexpr int N = 8;
    rune::protocol::RecordBatchBuilder builder(0, 5'000'000LL);
    for (int i = 0; i < N; ++i) builder.append("", "record" + std::to_string(i), 5'000'000LL + i);
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{10, 1}, 0, SubscribeOptions{From::earliest()}, c.handler());
    h.gw.pushBatch(h.subscribed(0), 0, builder.build());

    ASSERT_TRUE(h.drive([&] { return c.count() >= N; })) << "expected " << N << " records";
    ASSERT_EQ(c.count(), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(c.got[i].offset, i);
        EXPECT_EQ(str(c.got[i].value), "record" + std::to_string(i));
    }
}

// ===========================================================================
// 11. CB-5 A2: UnsubscribeResponseRace — unsubscribe() BEFORE the broker even
// saw the Subscribe (deterministic here: nothing ran in between).  The late
// SubscribeResponse must not resurrect the subscription: the client sends an
// Unsubscribe with the real subscriptionId and delivers nothing.
// ===========================================================================
TEST(Subscriber, UnsubscribeResponseRace) {
    SubscriberHarness h(3110, 3111);
    h.connect();
    Collector c;
    auto sub = h.client.subscribe(TopicHandle{11, 1}, 0, SubscribeOptions{From::latest()}, c.handler());
    sub.unsubscribe();
    EXPECT_FALSE(sub.active()) << "sub must be inactive after unsubscribe()";
    EXPECT_TRUE(h.gw.subscribed.empty()) << "the race needs the Subscribe still unanswered";

    const auto s = h.subscribed(0);  // the broker answers it now: the late SubscribeResponse
    ASSERT_TRUE(h.drive([&] { return !h.gw.unsubscribed.empty(); }))
        << "the late SubscribeResponse sent no Unsubscribe (CB-5 A2)";
    EXPECT_EQ(h.gw.unsubscribed[0], s.subscriptionId) << "the Unsubscribe must carry the real id";

    h.gw.pushBatch(s, 0, oneBatch(0, "k", "should-not-arrive"));
    h.settle();
    EXPECT_EQ(c.count(), 0) << "handler fired after unsubscribe() with a late SubscribeResponse";
    EXPECT_TRUE(sub.finished());
}

// ===========================================================================
// Phase 2b Part C regression: subscribe(h8, Key{}) binds to the SAME partition
// Producer::send(h8, Key{}) writes to.
//
// Producer::send routes client-side: key.partition(h.partitionCount).
// subscribe(TopicHandle, const Key&) sends partitionKnown=0 + the key bytes and
// the BROKER resolves the partition via TopicRegistry::partitionForKey ->
// rune::protocol::partitionForKey.  This fixture is a fake gateway (no
// TopicRegistry), so the broker step is applied here at the gateway boundary
// with the very function the broker calls.  The first Task 5 cut special-cased
// the empty key in Key::partition only: produce -> 0, broker-resolved
// subscribe -> fnv1a64("") % 8 = 5.
// ===========================================================================
TEST(SubscriberKeyRoutingRegression, EmptyKeySubscribeBindsToProducerSendPartition) {
    SubscriberHarness h(3130, 3131);
    h.connect();

    const TopicHandle h8{1, 8};
    const Key         empty{};
    Collector         c;
    auto sub = h.client.subscribe(h8, empty, SubscribeOptions{From::earliest()}, c.handler());
    const auto s = h.subscribed(0);

    ASSERT_EQ(s.partitionKnown, 0u) << "a keyed subscribe leaves partition resolution to the broker";
    EXPECT_TRUE(s.key.empty());

    // What the broker binds (TopicRegistry::partitionForKey's body) ...
    const int32_t brokerBound = rune::protocol::partitionForKey(
        std::span<const uint8_t>(s.key.data(), s.key.size()), h8.partitionCount);
    // ... and what Producer::send(h8, Key{}) writes to.
    const int32_t producerWrites = empty.partition(h8.partitionCount);
    EXPECT_EQ(brokerBound, producerWrites);
    EXPECT_EQ(brokerBound, 0);
}

// No-hang wave: a SubscribeResponse carrying an error (InvalidRequest 39 for a
// Subscribe the broker could not decode; UnknownTopic; UnknownPartition) binds
// NOTHING broker-side.  The client used to bind it like an OK - subscriptionId 0,
// active forever, never delivering: a silent hang.  It must finish with
// RejectedByBroker and the broker's code.
TEST(SubscriberRegression, SubscribeErrorResponseFinishesTheSubscriptionRegression) {
    namespace rcc = rune::client::codec;
    for (const auto code : {rcc::ErrorCode::InvalidRequest, rcc::ErrorCode::UnknownTopic}) {
        const int32_t base = code == rcc::ErrorCode::InvalidRequest ? 3190 : 3192;
        SubscriberHarness h(base, base + 1);
        h.connect();
        h.gw.subscribeErrorCode = static_cast<int16_t>(code);
        Collector c;
        auto sub = h.client.subscribe(TopicHandle{9, 1}, /*partition=*/0, SubscribeOptions{From::earliest()},
                                      c.handler());
        ASSERT_TRUE(h.drive([&] { return sub.finished(); }))
            << "an error SubscribeResponse left the subscription pending/active";
        EXPECT_FALSE(sub.active());
        EXPECT_EQ(sub.failReason(), SubscriptionFailReason::RejectedByBroker);
        EXPECT_EQ(sub.brokerErrorCode(), static_cast<uint16_t>(code));
        EXPECT_EQ(sub.subscriptionId(), 0);
    }
}
