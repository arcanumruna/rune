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
