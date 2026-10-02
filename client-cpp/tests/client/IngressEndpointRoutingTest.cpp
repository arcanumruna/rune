// tests/client/IngressEndpointRoutingTest.cpp — per-partition worker ingress
// endpoints (shard-mode brokers).  Invoker client + InvokerGateway on one thread;
// the duty-cycle clock is frozen so no retransmit timer can mask a routing bug.
#include "../support/InvokerGateway.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>

#include <gtest/gtest.h>

#include <chrono>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {
int g_ctr = 0;  // test thread only
int64_t frozenNow() noexcept { return 1'000'000'000'000LL; }
const std::vector<uint8_t> kVal{7, 7, 7};
// The clock is frozen: linger must be 0 or a batch would wait forever.
rc::ProducerConfig noLinger() {
    rc::ProducerConfig p;
    p.lingerUs = 0;
    return p;
}

struct Rig {
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;
    Rig(int32_t in, int32_t eg)
        : c(makeCfg(in, eg)),
          client(c),
          gw(c.dataDir + "/aeron", "aeron:ipc", in, "aeron:ipc", eg) {}
    static rc::ClientConfig makeCfg(int32_t in, int32_t eg) {
        rc::ClientConfig c;
        c.gatewayChannel = "aeron:ipc";
        c.gatewayStreamId = in;
        c.egressChannel = "aeron:ipc";
        c.egressStreamId = eg;
        c.dataDir = "/tmp/ingress_ep_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        c.nanoClock = &frozenNow;
        rc::TopicSpec ts;
        ts.name = "t";
        ts.topicId = 1;
        ts.partitionCount = 1;
        c.topics.push_back(ts);
        return c;
    }
    template <class P>
    bool drive(P&& pred, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    // F8c: completion tokens polled while driving (no future).  The frozen
    // clock never expires an op, so the drive cap is the only bound.
    rc::Completions cq{client};  // this (owner) thread's queue
    rc::Token       nextToken = 1;
    // Drive until `t` completes; returns its Completion (status Closed if the cap hit).
    rc::Completion awaitToken(rc::Token t) {
        rc::Completion got{};
        got.status = rc::OpStatus::Closed;
        bool done = false;
        (void)drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == t) { done = true; got = c; }
            });
            return done;
        });
        return got;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        const rc::Token t = nextToken++;
        ASSERT_EQ(client.connect(cq, t), rc::OpResult::Accepted);
        ASSERT_EQ(awaitToken(t).status, rc::OpStatus::Ok);
    }
    // One acked send into `cq`; returns its Completion.
    rc::Completion sendAndAwait(rc::Producer& p) {
        const rc::Token t = nextToken++;
        EXPECT_EQ(p.trySend(client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), cq, t),
                  rc::SendResult::Ok);
        return awaitToken(t);
    }
};

// onAck handler target (F8c: plain function pointer + context, duty thread).
struct AckSeen {
    int     fired  = 0;
    int64_t offset = -2;
    static void on(void* ctx, rc::Token, int64_t off, rc::OpStatus, uint16_t) noexcept {
        auto* s = static_cast<AckSeen*>(ctx);
        ++s->fired;
        s->offset = off;
    }
};
} // namespace

TEST(IngressEndpointRouting, ProduceGoesToTheAdvertisedWorkerEndpoint) {
    Rig r(3900, 3901);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24751", 5001);
    r.gw.partitions = {{1, 0, 0, 1, 5001, "127.0.0.1:24751"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    EXPECT_EQ(r.client.testDataChannelFor(1, 0), "aeron:udp?endpoint=127.0.0.1:24751");
    auto p = r.client.producer(noLinger());
    const rc::Completion c = r.sendAndAwait(p);
    ASSERT_EQ(c.status, rc::OpStatus::Ok);
    EXPECT_EQ(c.value, 100) << "acked by the worker's own publication";
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, w) << "produce must reach the worker, not the node";
    EXPECT_EQ(r.gw.produced[0].sessionId, r.gw.assignedSessionId) << "session id from the node Connect";
}

TEST(IngressEndpointRouting, EmptyEndpointKeepsNodeRouting) {
    Rig r(3902, 3903);
    r.gw.partitions = {{1, 0, 0, 1, 0, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.gw.metadataRequests > 0; }));
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, 0);
    EXPECT_TRUE(r.client.testDataChannelFor(1, 0).empty());
}

// Regression (Review Focus 5): a metadata refresh moves the partition to another
// worker while a batch is in flight at the old one.  The batch must be re-offered
// to the new worker immediately (the clock is frozen: no retransmit timer), and a
// late ack from the old worker must not complete the record twice.
TEST(IngressEndpointRouting, EndpointMoveReoffersInFlightBatchesRegression) {
    Rig r(3904, 3905);
    const int wA = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24761", 5002);
    const int wB = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24762", 5003);
    r.gw.partitions = {{1, 0, 0, 1, 5002, "127.0.0.1:24761"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0) == "aeron:udp?endpoint=127.0.0.1:24761"; }));
    r.gw.holdPartitions = {0};
    auto p = r.client.producer(noLinger());
    AckSeen seen;
    p.onAck(&AckSeen::on, &seen);  // setup phase: before the first AckToken send
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal),
                        rc::AckToken{1}),
              rc::SendResult::Ok);
    const int& fired = seen.fired;
    const int64_t& offset = seen.offset;
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, wA);

    r.gw.partitions = {{1, 0, 0, 2, 5003, "127.0.0.1:24762"}};  // epoch 2: moved to B
    r.gw.holdPartitions.clear();
    r.gw.sendMetadata(r.gw.assignedSessionId, /*correlationId=*/0);  // unsolicited refresh
    ASSERT_TRUE(r.drive([&] { return fired == 1; })) << "in-flight batch not re-offered to the new worker";
    EXPECT_EQ(r.gw.produced.back().endpoint, wB);
    EXPECT_EQ(r.gw.produced.back().corrId, r.gw.held[0].corrId) << "same batch (idempotent re-offer)";

    r.gw.releaseHeld();  // the old worker acks late
    for (int i = 0; i < 20'000; ++i) {
        r.client.doWork();
        r.gw.pump();
    }
    EXPECT_EQ(fired, 1) << "exactly one completion per record";
    EXPECT_EQ(offset, 100);
}

// A refresh that drops the partition's worker endpoint (the broker left shard
// mode / a legacy leader took over) must drop the worker route too: produce
// returns to the node endpoint, and the in-flight batch follows it at once.
TEST(IngressEndpointRouting, EndpointClearedFallsBackToNode) {
    Rig r(3906, 3907);
    const int w = r.gw.addEndpoint("aeron:udp?endpoint=127.0.0.1:24771", 3906);  // shard mode: the client stream
    r.gw.partitions = {{1, 0, 0, 1, 3906, "127.0.0.1:24771"}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    r.gw.holdPartitions = {0};
    auto p = r.client.producer(noLinger());
    AckSeen seen;
    p.onAck(&AckSeen::on, &seen);
    ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal),
                        rc::AckToken{1}),
              rc::SendResult::Ok);
    const int& fired = seen.fired;
    ASSERT_TRUE(r.drive([&] { return r.gw.held.size() == 1; }));
    ASSERT_EQ(r.gw.held[0].endpoint, w);

    r.gw.partitions = {{1, 0, 0, 2, 3906, ""}};  // epoch 2: no worker endpoint
    r.gw.holdPartitions.clear();
    r.gw.sendMetadata(r.gw.assignedSessionId, /*correlationId=*/0);
    ASSERT_TRUE(r.drive([&] { return r.client.testDataChannelFor(1, 0).empty(); }))
        << "worker route kept after the endpoint was withdrawn";
    ASSERT_TRUE(r.drive([&] { return fired == 1; })) << "in-flight batch not re-offered to the node";
    EXPECT_EQ(r.gw.produced.back().endpoint, 0);
    EXPECT_EQ(r.gw.produced.back().corrId, r.gw.held[0].corrId);

    const std::size_t before = r.gw.produced.size();
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), before + 1);
    EXPECT_EQ(r.gw.produced.back().endpoint, 0) << "new batches go to the node";
}

// Regression: a producer created AFTER the MetadataResponse never learnt the
// partition's advertised ingressStreamId (updateIngressRoute reached only the
// producers registered at that moment), so it published on the default client
// stream instead of the advertised one.
TEST(IngressEndpointRouting, ProducerCreatedAfterMetadataUsesAdvertisedStreamRegression) {
    Rig r(3908, 3909);
    const int s = r.gw.addEndpoint("aeron:ipc", 5004);  // the node, on the advertised stream
    r.gw.partitions = {{1, 0, 0, 1, 5004, ""}};
    r.connect();
    ASSERT_TRUE(r.drive([&] { return r.client.testIngressStreamIdFor(1, 0) == 5004; }));  // metadata landed
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAndAwait(p).status, rc::OpStatus::Ok);
    ASSERT_EQ(r.gw.produced.size(), 1u);
    EXPECT_EQ(r.gw.produced[0].endpoint, s) << "published on the default stream, not the advertised one";
}
