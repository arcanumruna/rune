// tests/client/ClaimClientTest.cpp — Phase 2b Part B end to end: a real Client
// (Invoker and Thread runner modes) over Aeron IPC to the in-process
// InvokerGateway.  Claims, claim batches and sends arrive in submission order;
// an acked claim completes; batch-ack credit is exact for committed AND
// aborted claims.  Drive loops are deadline-bounded (only a failing test
// reaches the deadline); nothing sleeps.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using rc::SendResult;

namespace {
int g_ctr = 0;  // single test thread
int64_t          g_fakeNow = 0;  // test thread only; Invoker mode only (a Thread-mode runner would read it too)
int64_t fakeNow() noexcept { return g_fakeNow; }

rc::ClientConfig cfgFor(rc::RunnerMode mode, int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/claim_client_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = mode;
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    bool                         invoker;
    std::unique_ptr<rc::Client>  client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : invoker(c.runnerMode == rc::RunnerMode::Invoker),
          client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.keepValues = true;
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (invoker) client->doWork();  // Thread mode: the runner works by itself
            gw.pump();
            if (pred()) return true;
            if (!invoker) std::this_thread::yield();
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

std::vector<uint8_t> bytes(std::string_view s) { return {s.begin(), s.end()}; }

void fill(std::span<uint8_t> b, std::string_view s) {
    std::memcpy(b.data(), s.data(), std::min(b.size(), s.size()));
}

std::vector<std::string> valuesOf(const test_support::InvokerGateway& gw, int32_t partition) {
    std::vector<std::string> out;
    const auto it = gw.values.find(partition);
    if (it == gw.values.end()) return out;
    for (const auto& v : it->second) out.emplace_back(v.begin(), v.end());
    return out;
}
}  // namespace

TEST(ClaimClient, ClaimsBatchesAndSendsArriveInSubmissionOrderInBothModes) {
    int32_t stream = 5810;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(mode == rc::RunnerMode::Invoker ? "Invoker" : "Thread");
        Rig r(cfgFor(mode, stream, stream + 1));
        stream += 2;
        r.connect();
        rc::ProducerConfig pc;
        pc.acks     = rc::Acks::Leader;
        pc.lingerUs = 0;
        auto p = r.client->producer(pc);
        const auto topic = r.client->topic("t");
        const rc::Key key(uint64_t{9});
        ASSERT_EQ(p.trySend(topic, key, bytes("A")), SendResult::Ok);
        {
            rc::BatchClaim b = p.claimBatch(topic, key, 3, 2);
            ASSERT_TRUE(b) << static_cast<int>(b.result());
            fill(b.record(0), "B1");
            fill(b.record(1), "B2");
            fill(b.record(2), "B3");
            ASSERT_TRUE(b.commit());
        }
        {
            rc::Claim c = p.tryClaim(topic, key, 1);
            ASSERT_TRUE(c) << static_cast<int>(c.result());
            fill(c.buffer(), "C");
            ASSERT_TRUE(c.commit());
        }
        rc::Completions cq(*r.client);
        int64_t ackedOffset = -2;
        {
            rc::Claim d = p.tryClaim(topic, key, 1, cq, 77);
            ASSERT_TRUE(d) << static_cast<int>(d.result());
            fill(d.buffer(), "D");
            ASSERT_TRUE(d.commit());
        }
        p.flush();
        ASSERT_TRUE(r.drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 77) ackedOffset = c.value;
            });
            return valuesOf(r.gw, 0).size() >= 6 && ackedOffset != -2;
        }));
        EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"A", "B1", "B2", "B3", "C", "D"}));
        EXPECT_GE(ackedOffset, 0) << "the acked claim completes Ok with its offset";
        r.client->close();
    }
}

// Review Focus 5.
TEST(ClaimClient, BatchAckCreditIsReturnedForCommittedAndAbortedClaims) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, 5814, 5815));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    rc::Completions bq(*r.client);
    p.batchAcksTo(bq);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{4});
    {
        rc::BatchClaim a = p.claimBatch(topic, key, 3, 2);
        ASSERT_TRUE(a);
        EXPECT_EQ(bq.outstanding(), 3u) << "one unit per record, like three sends";
        a.abort();
        EXPECT_EQ(bq.outstanding(), 0u) << "an aborted batch returns every unit";
    }
    {
        rc::Claim c = p.tryClaim(topic, key, 2);
        ASSERT_TRUE(c);
        EXPECT_EQ(bq.outstanding(), 1u);
    }  // the destructor aborts
    EXPECT_EQ(bq.outstanding(), 0u);
    {
        rc::BatchClaim b = p.claimBatch(topic, key, 3, 2);
        ASSERT_TRUE(b);
        fill(b.record(0), "x1");
        fill(b.record(1), "x2");
        fill(b.record(2), "x3");
        ASSERT_TRUE(b.commit());
    }
    EXPECT_EQ(bq.outstanding(), 3u);
    std::vector<rc::BatchAck> acks;
    ASSERT_TRUE(r.drive([&] {
        bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.op == rc::OpKind::BatchAck) acks.push_back(rc::batchAckOf(c));
        });
        return !acks.empty();
    }));
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].recordCount, 3);
    EXPECT_EQ(acks[0].errorCode, 0);
    EXPECT_EQ(bq.outstanding(), 0u) << "one BatchAck of 3 returns the 3 units";
    EXPECT_EQ(bq.reservedBytes(), 0u);
    r.client->close();
}

// A ClaimHeld refusal of tryClaim(.., Completions&, Token) returns the queue
// credit it reserved: exact counters.
TEST(ClaimClient, ClaimHeldRefusalOfACompletionsClaimReturnsItsQueueCredit) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, 5816, 5817));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{3});
    rc::Completions cq(*r.client);
    ASSERT_EQ(cq.outstanding(), 0u);
    ASSERT_EQ(cq.reservedBytes(), 0u);
    rc::Claim held = p.tryClaim(topic, key, 1);
    ASSERT_TRUE(held);
    for (int i = 0; i < 3; ++i) {  // repeated refusals must not drift the counters
        rc::Claim refused = p.tryClaim(topic, key, 1, cq, 55);
        EXPECT_FALSE(refused);
        EXPECT_EQ(refused.result(), SendResult::ClaimHeld);
        EXPECT_EQ(cq.outstanding(), 0u) << "the refused claim's reservation is returned";
        EXPECT_EQ(cq.reservedBytes(), 0u);
    }
    fill(held.buffer(), "H");
    ASSERT_TRUE(held.commit());
    rc::Claim next = p.tryClaim(topic, key, 1, cq, 56);
    ASSERT_TRUE(next) << "the queue still has all its credit";
    EXPECT_EQ(cq.outstanding(), 1u);
    fill(next.buffer(), "N");
    ASSERT_TRUE(next.commit());
    p.flush();
    int64_t off = -2;
    ASSERT_TRUE(r.drive([&] {
        cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.token == 56) off = c.value;
        });
        return off != -2;
    }));
    EXPECT_GE(off, 0);
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"H", "N"}));
    r.client->close();
}

// Graceful close() while an Invoker claim is held: commit() returns false,
// nothing is sent, close completes (bounded: measured against a 5 s bound, with
// the ctest timeout as the hang backstop).
TEST(ClaimClientRegression, GracefulCloseWithAnInvokerClaimHeldAbandonsItPromptlyRegression) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, 5818, 5819));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{3});
    rc::Claim held = p.tryClaim(topic, key, 4);
    ASSERT_TRUE(held);
    fill(held.buffer(), "LOST");

    const auto t0 = std::chrono::steady_clock::now();
    r.client->close();
    const auto closeMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
    EXPECT_LT(closeMs.count(), 5000) << "the drain must not wait out deliveryTimeoutMs (30 s) for a claim "
                                         "its own thread holds";
    EXPECT_FALSE(held.commit()) << "the claim was abandoned under the caller";
    for (int i = 0; i < 200; ++i) r.gw.pump();
    EXPECT_TRUE(valuesOf(r.gw, 0).empty()) << "nothing was sent";
}

// An abandoned (destroyed uncommitted) BatchClaim returns n credit units and
// the next send succeeds.
TEST(ClaimClient, AbandonedBatchClaimReturnsItsCreditAndTheNextSendSucceeds) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, 5820, 5821));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    rc::Completions bq(*r.client);
    p.batchAcksTo(bq);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{5});
    {
        rc::BatchClaim b = p.claimBatch(topic, key, 4, 2);
        ASSERT_TRUE(b);
        EXPECT_EQ(bq.outstanding(), 4u);
        fill(b.record(0), "zz");
    }  // destroyed uncommitted
    EXPECT_EQ(bq.outstanding(), 0u) << "n units returned";
    EXPECT_EQ(bq.reservedBytes(), 0u);
    ASSERT_EQ(p.trySend(topic, key, bytes("ok")), SendResult::Ok);
    EXPECT_EQ(bq.outstanding(), 1u);
    p.flush();
    int got = 0;
    ASSERT_TRUE(r.drive([&] {
        bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.op == rc::OpKind::BatchAck) ++got;
        });
        return got == 1;
    }));
    EXPECT_EQ(bq.outstanding(), 0u);
    EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"ok"})) << "the abandoned records never left";
    r.client->close();
}

// Delivery-timeout-driven abandon: an open Invoker claim sits in a partition
// builder holding an earlier unsent record; once the (injected) clock passes
// deliveryTimeoutMs the duty pass fails the builder, which drops the claim.
TEST(ClaimClient, DeliveryTimeoutAbandonsAnOpenInvokerClaim) {
    rc::ClientConfig c = cfgFor(rc::RunnerMode::Invoker, 5822, 5823);
    g_fakeNow   = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now().time_since_epoch()).count();
    c.nanoClock = &fakeNow;
    Rig r(c);
    r.connect();
    rc::ProducerConfig pc;
    pc.acks              = rc::Acks::Leader;
    pc.lingerUs          = 0;
    pc.deliveryTimeoutMs = 100;
    auto p = r.client->producer(pc);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{6});
    rc::Completions cq(*r.client);
    ASSERT_EQ(p.trySend(topic, key, bytes("early"), cq, 11), SendResult::Ok);
    rc::Claim held = p.tryClaim(topic, key, 4);
    ASSERT_TRUE(held);
    fill(held.buffer(), "LATE");
    g_fakeNow += 500'000'000;  // 500 ms > deliveryTimeoutMs
    rc::OpStatus st = rc::OpStatus::Ok;
    bool         seen = false;
    ASSERT_TRUE(r.drive([&] {
        cq.poll([&](const rc::Completion& cc, std::span<const uint8_t>) {
            if (cc.token == 11) { st = cc.status; seen = true; }
        });
        return seen;
    }));
    EXPECT_EQ(st, rc::OpStatus::DeliveryFailed);
    EXPECT_FALSE(held.commit()) << "the timed-out builder dropped the open claim";
    EXPECT_TRUE(valuesOf(r.gw, 0).empty());
    ASSERT_EQ(p.trySend(topic, key, bytes("after")), SendResult::Ok) << "the producer is usable again";
    p.flush();
    ASSERT_TRUE(r.drive([&] { return valuesOf(r.gw, 0).size() >= 1; }));
    EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"after"}));
    r.client->close();
}

// Blocking Producer::close() / closeAndWait() on the owner thread with an open
// Invoker claim: the owner is blocked inside close and cannot commit, so the
// claim is abandoned at once.  acks=None: the earlier record needs no broker
// answer, so the single-threaded close can finish without pumping the gateway.
namespace {
void blockingCloseAbandonsTheOwnersClaim(int32_t in, bool withBound) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, in, in + 1));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::None;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{8});
    ASSERT_EQ(p.trySend(topic, key, bytes("early")), SendResult::Ok);
    rc::Claim held = p.tryClaim(topic, key, 4);
    ASSERT_TRUE(held);
    fill(held.buffer(), "LOST");
    const auto t0 = std::chrono::steady_clock::now();
    const rc::OpStatus st = withBound ? p.closeAndWait(std::chrono::seconds(20)) : p.close();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
    EXPECT_EQ(st, rc::OpStatus::Ok);
    EXPECT_LT(ms.count(), 5000) << "must not wait out deliveryTimeoutMs (30 s)";
    EXPECT_FALSE(held.commit()) << "abandoned under the caller";
    ASSERT_TRUE(r.drive([&] { return valuesOf(r.gw, 0).size() >= 1; }));
    EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"early"})) << "the earlier record flushed normally";
    r.client->close();
}
}  // namespace

TEST(ClaimClientRegression, BlockingProducerCloseAbandonsTheOwnersOpenInvokerClaimRegression) {
    blockingCloseAbandonsTheOwnersClaim(5824, false);
}

TEST(ClaimClientRegression, ProducerCloseAndWaitAbandonsTheOwnersOpenInvokerClaimRegression) {
    blockingCloseAbandonsTheOwnersClaim(5826, true);
}
