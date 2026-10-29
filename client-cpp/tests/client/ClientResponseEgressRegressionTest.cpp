// tests/client/ClientResponseEgressRegressionTest.cpp — NAT-safe egress (Task 7): the real Client against the
// InvokerGateway fake in RESPONSE mode.  Every reply of a session at a response-capable endpoint leaves ONLY on an
// Aeron response publication correlated with the request image the client opened; the fake's declared egress
// publication points at an address nothing listens on, so a reply on the declared path is a lost reply and the
// test fails.  Worker endpoints mirror the broker's WorkerBind rule (first frame on the image, plaintext, valid token;
// produce from an unbound image is dropped).  Real UDP on 127.0.0.1, one thread, bounded drive loops, no sleeps.
#include <filesystem>
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/transport/ResponseChannel.h>

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rp = rune::protocol::client;

namespace {

int g_ctr = 0;
const std::vector<uint8_t> kVal{7, 7, 7};
constexpr uint16_t kBindTemplate    = static_cast<uint16_t>(rp::TemplateId::WorkerBind);
constexpr uint16_t kProduceTemplate = static_cast<uint16_t>(rp::TemplateId::Produce);

uint16_t freePort() { return rune::client::testing::allocFreeUdpPort(); }
std::string loopback(uint16_t port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ProducerConfig noLinger() {
    rc::ProducerConfig p;
    p.lingerUs = 0;
    return p;
}

std::string testPem(const char* name) {
    std::ifstream f(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

struct Options {
    bool tls               = false;   ///< sessions are TLS: the fake serves the test certificate
    int  partitions        = 1;
    bool declaredReachable = false;   ///< the fake's declared egress publication reaches the client (an older broker)
    bool unroutableDeclared() const { return !declaredReachable; }
};

struct Rig {
    uint16_t                     gwPort = freePort();
    uint16_t                     gwResp = freePort();
    int32_t                      in, eg;
    rc::ClientConfig             c;
    rc::Client                   client;
    test_support::InvokerGateway gw;

    Rig(int32_t inStream, int32_t egStream, const Options& o = {})
        : in(inStream), eg(egStream), c(makeCfg(inStream, egStream, gwPort, gwResp, o)), client(c),
          gw(c.dataDir + "/aeron", loopback(gwPort), inStream,
             o.declaredReachable ? c.egressChannel : ("aeron:udp?endpoint=192.0.2.1:" + std::to_string(freePort())),
             egStream) {
        gw.responseCtl[0]      = "127.0.0.1:" + std::to_string(gwResp);
        gw.issueBindToken      = true;
        gw.uniqueSessionIds    = true;
        gw.newIdOnResume       = true;
        gw.replyFromArrival    = true;   // a real broker answers a session on that session's own node
        gw.brokerHost          = "127.0.0.1";
        gw.brokerPort          = gwPort;
        gw.brokerResponsePort  = o.declaredReachable ? 0 : gwResp;
        if (o.tls) {
            gw.tlsCertPem = testPem("server.pem");
            gw.tlsKeyPem  = testPem("server.key");
        }
    }

    static rc::ClientConfig makeCfg(int32_t in, int32_t eg, uint16_t gwPort, uint16_t gwResp, const Options& o) {
        rc::ClientConfig c;
        c.gatewayChannel             = loopback(gwPort);
        c.gatewayStreamId            = in;
        c.egressChannel              = loopback(freePort());   // the client's declared sub: bound, but never served
        c.egressStreamId             = eg;
        c.gatewayResponseControlPort = gwResp;
        c.dataDir = std::filesystem::temp_directory_path().string() + "/resp_egress_" + std::to_string(::getpid()) + "_" +
                    std::to_string(g_ctr++);
        c.runnerMode = rc::RunnerMode::Invoker;
        if (o.tls) {
            c.offerCrypto = true;
            c.tlsTrustPem = testPem("ca.pem");
        }
        rc::TopicSpec ts;
        ts.name           = "t";
        ts.topicId        = 1;
        ts.partitionCount = static_cast<uint32_t>(o.partitions);
        c.topics.push_back(ts);
        return c;
    }

    // One worker serving `partition`: its ingress endpoint (own stream), response-control port and bind gate.
    struct Worker { int endpoint; uint16_t port; uint16_t resp; int32_t stream; };
    Worker addWorker(int partition, int32_t stream, bool responseMode = true) {
        Worker w{0, freePort(), freePort(), stream};
        w.endpoint = gw.addEndpoint(loopback(w.port), stream);
        if (responseMode) {
            gw.responseCtl[w.endpoint] = "127.0.0.1:" + std::to_string(w.resp);
            gw.bindGate.insert(w.endpoint);
        }
        test_support::InvokerGateway::PartitionAd ad;
        ad.topicId             = 1;
        ad.partition           = partition;
        ad.leaderId            = 0;
        ad.leaderEpoch         = 1;
        ad.ingressStreamId     = stream;
        ad.ingressEndpoint     = "127.0.0.1:" + std::to_string(w.port);
        ad.responseControlPort = responseMode ? w.resp : 0;
        gw.partitions.push_back(ad);
        return w;
    }

    template <class P>
    bool drive(P&& pred, int cap = -1) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (int i = 0; cap < 0 ? std::chrono::steady_clock::now() < deadline : i < cap; ++i) {
            client.doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    rc::Completions cq{client};
    rc::Token       nextToken = 1;
    rc::Completion awaitToken(rc::Token t) {
        rc::Completion got{};
        got.status = rc::OpStatus::Closed;
        bool done  = false;
        (void)drive([&] {
            cq.poll([&](const rc::Completion& cm, std::span<const uint8_t>) {
                if (cm.token == t) { done = true; got = cm; }
            });
            return done;
        });
        return got;
    }
    bool connect() {
        if (!drive([&] { return gw.ready(); })) return false;
        const rc::Token t = nextToken++;
        if (client.connect(cq, t) != rc::OpResult::Accepted) return false;
        return awaitToken(t).status == rc::OpStatus::Ok;
    }

    // `n` acked sends of keys that hash to `partition`; returns how many completed Ok.
    int sendAcked(rc::Producer& p, int partition, int n) {
        std::vector<rc::Token> tokens;
        for (int i = 0, k = 1; i < n; ++k) {
            const rc::Key key(static_cast<uint64_t>(k));
            if (key.partition(c.topics[0].partitionCount == 0 ? 1 : static_cast<int32_t>(c.topics[0].partitionCount)) != partition) continue;
            const rc::Token t = nextToken++;
            if (p.trySend(client.topic("t"), key, std::span<const uint8_t>(kVal), cq, t) != rc::SendResult::Ok) return -1;
            tokens.push_back(t);
            ++i;
        }
        int ok = 0;
        std::vector<bool> seen(tokens.size(), false);
        (void)drive([&] {
            cq.poll([&](const rc::Completion& cm, std::span<const uint8_t>) {
                for (std::size_t i = 0; i < tokens.size(); ++i)
                    if (!seen[i] && tokens[i] == cm.token) { seen[i] = true; if (cm.status == rc::OpStatus::Ok) ++ok; }
            });
            for (bool s : seen) if (!s) return false;
            return true;
        });
        return ok;
    }

    // The templates that reached `endpoint` on its (single) request image, in arrival order.
    std::vector<uint16_t> framesAt(int endpoint) const {
        for (const auto& [key, frames] : gw.framesOnImage)
            if (key.first == endpoint) return frames;
        return {};
    }
    int producesAt(int endpoint) const {
        int n = 0;
        for (const auto& pr : gw.produced) n += pr.endpoint == endpoint ? 1 : 0;
        return n;
    }
};

} // namespace

// The seed's connect rides the response publication: the fake's declared egress is unreachable, so a ConnectResponse
// that came back at all came back on the response channel.
TEST(ClientResponseEgressRegression, SeedConnectRidesTheResponsePublication) {
    Rig r(4100, 4101);
    ASSERT_TRUE(r.connect()) << "no ConnectResponse: the reply did not ride the response publication";
    EXPECT_TRUE(r.client.testSeedResponseCorrelated());
    ASSERT_FALSE(r.gw.connectFlags.empty());
    EXPECT_TRUE(r.gw.connectFlags.front()) << "ConnectRequest.responseCorrelated";
    EXPECT_EQ(r.gw.responseConnects, 1);
    EXPECT_NE(r.client.testTransport().seedPublicationChannelForTest().find("|response-correlation-id="),
              std::string::npos);
    EXPECT_EQ(r.gw.lastConnectClientVersion, 16);
}

TEST(ClientResponseEgressRegression, WorkerBindIsTheFirstFrameOnEveryWorker) {
    Options o;
    o.partitions = 2;
    Rig r(4110, 4111, o);
    const auto w0 = r.addWorker(0, 4112);
    const auto w1 = r.addWorker(1, 4113);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 2; })) << "binds sent: " << r.gw.binds.size();
    const int64_t sid = r.gw.binds[0].sessionId;
    ASSERT_NE(r.gw.bindTokens.count(sid), 0u);
    for (const auto& b : r.gw.binds) {
        EXPECT_TRUE(b.accepted) << "a valid token on the session's own id";
        EXPECT_TRUE(b.firstOnImage) << "WorkerBind must be the FIRST frame on the worker image";
        EXPECT_EQ(b.sessionId, sid);
        EXPECT_EQ(b.token, r.gw.bindTokens[sid]);
    }
    // Plaintext, not sealed: the frame decodes as a WorkerBind (the fake dropped nothing as undecodable).
    ASSERT_EQ(r.gw.workerBindFrames.size(), 2u);
    for (const auto& f : r.gw.workerBindFrames) EXPECT_EQ(f.size(), rp::WorkerBind::ENCODED_SIZE);
    EXPECT_EQ(r.gw.bindsRefused, 0);

    auto p = r.client.producer(noLinger());
    EXPECT_EQ(r.sendAcked(p, 0, 10), 10);
    EXPECT_EQ(r.sendAcked(p, 1, 10), 10);
    for (const int ep : {w0.endpoint, w1.endpoint}) {
        const auto frames = r.framesAt(ep);
        ASSERT_FALSE(frames.empty());
        EXPECT_EQ(frames.front(), kBindTemplate) << "endpoint " << ep;
        EXPECT_GE(r.producesAt(ep), 1);
        for (std::size_t i = 1; i < frames.size(); ++i) EXPECT_EQ(frames[i], kProduceTemplate);
    }
    EXPECT_EQ(r.gw.producesBeforeBind, 0);
    EXPECT_EQ(r.client.testWorkerBindCount(), 2u);
    EXPECT_EQ(r.client.testWorkerBindsSent(), 2u);
}

// A produce offered before the bind went out is HELD (the worker would drop it from an unbound image): with the
// bind blocked no produce frame reaches the worker; once it goes out, the produces follow it, in order.
// Under TLS the bindToken still rides the FIRST ConnectResponse leg (with the server's handshake flight), and the
// WorkerBind is PLAINTEXT even on an encrypted session: the worker has no key for the image before it is bound.
TEST(ClientResponseEgressRegression, WorkerBindIsPlaintextUnderTlsAndTheTokenRidesTheFirstLeg) {
    Options o;
    o.tls = true;
    Rig r(4180, 4181, o);
    const auto w = r.addWorker(0, 4182);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.gw.tlsSessions == 1; })) << "the session is encrypted";
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1; })) << "no WorkerBind: the token did not arrive on the first leg";
    EXPECT_TRUE(r.gw.binds[0].accepted) << "a sealed bind would not carry the token in clear";
    EXPECT_TRUE(r.gw.binds[0].firstOnImage);
    ASSERT_EQ(r.gw.workerBindFrames.size(), 1u);
    const auto parsed = rp::WorkerBind::decode(r.gw.workerBindFrames[0].data(), r.gw.workerBindFrames[0].size());
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->token, r.gw.bindTokens[parsed->sessionId]);
    auto p = r.client.producer(noLinger());
    EXPECT_EQ(r.sendAcked(p, 0, 3), 3) << "produce follows the bind under TLS too";
    EXPECT_EQ(r.framesAt(w.endpoint).front(), kBindTemplate);
}

TEST(ClientResponseEgressRegression, ProduceHeldUntilBound) {
    Rig r(4120, 4121);
    const auto w = r.addWorker(0, 4122);
    r.client.testHoldWorkerBinds(true);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return !r.client.testDataChannelFor(1, 0).empty(); }));
    auto p = r.client.producer(noLinger());
    std::vector<rc::Token> tokens;
    for (int i = 0; i < 3; ++i) {
        const rc::Token t = r.nextToken++;
        ASSERT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), r.cq, t),
                  rc::SendResult::Ok);
        tokens.push_back(t);
    }
    ASSERT_TRUE(r.drive([&] { return r.client.testHeldOffers() > 0; })) << "the produce was never even attempted";
    (void)r.drive([] { return false; }, 2000);   // a bounded burst of further duty cycles
    EXPECT_TRUE(r.gw.produced.empty()) << "no produce may reach a worker before its WorkerBind";
    EXPECT_EQ(r.producesAt(w.endpoint), 0);
    EXPECT_TRUE(r.gw.binds.empty());
    EXPECT_EQ(r.gw.producesBeforeBind, 0);

    r.client.testHoldWorkerBinds(false);
    int ok = 0;
    ASSERT_TRUE(r.drive([&] {
        r.cq.poll([&](const rc::Completion& cm, std::span<const uint8_t>) { if (cm.status == rc::OpStatus::Ok) ++ok; });
        return ok == 3;
    }));
    const auto frames = r.framesAt(w.endpoint);
    ASSERT_GE(frames.size(), 2u);
    EXPECT_EQ(frames.front(), kBindTemplate);
    EXPECT_EQ(r.gw.producesBeforeBind, 0);
    ASSERT_GE(r.gw.produced.size(), 1u);
    for (std::size_t i = 1; i < r.gw.produced.size(); ++i)
        EXPECT_LT(r.gw.produced[i - 1].corrId, r.gw.produced[i].corrId) << "produces arrive in order";
}

TEST(ClientResponseEgressRegression, ReconnectRebindsWithTheNewToken) {
    Rig r(4130, 4131);
    const auto w = r.addWorker(0, 4132);
    ASSERT_TRUE(r.connect());
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAcked(p, 0, 1), 1);
    ASSERT_EQ(r.gw.binds.size(), 1u);
    const int64_t oldSid = r.gw.binds[0].sessionId;
    const auto    oldTok = r.gw.binds[0].token;

    r.gw.expireSession(oldSid);   // the broker drops the session; the client reconnects with a NEW session
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 2; })) << "no re-bind after the reconnect";
    const auto& nb = r.gw.binds[1];
    EXPECT_NE(nb.sessionId, oldSid);
    EXPECT_NE(nb.token, oldTok) << "a new connect issues a new token";
    EXPECT_EQ(nb.token, r.gw.bindTokens[nb.sessionId]);
    EXPECT_TRUE(nb.accepted);
    ASSERT_EQ(r.sendAcked(p, 0, 1), 1);
    const auto frames = r.framesAt(w.endpoint);
    ASSERT_EQ(frames.size(), 4u);
    EXPECT_EQ(frames[0], kBindTemplate);
    EXPECT_EQ(frames[1], kProduceTemplate);
    EXPECT_EQ(frames[2], kBindTemplate) << "the new bind precedes the next produce";
    EXPECT_EQ(frames[3], kProduceTemplate);
    EXPECT_EQ(r.gw.producesBeforeBind, 0);
}

// An older broker (< 9) ignores responseCorrelated and answers on the declared channel: no v16 tail, so no token,
// no WorkerBind, produce unchanged.
TEST(ClientResponseEgressRegression, OldBrokerUsesTheDeclaredPath) {
    Options o;
    o.declaredReachable = true;
    Rig r(4140, 4141, o);
    r.gw.advertisedBrokerVersion = 8;
    const auto w = r.addWorker(0, 4142, /*responseMode=*/false);
    ASSERT_TRUE(r.connect()) << "the declared path must still work";
    ASSERT_FALSE(r.gw.connectFlags.empty());
    EXPECT_TRUE(r.gw.connectFlags.front()) << "the client still asks for response egress";
    EXPECT_EQ(r.gw.responseConnects, 0) << "an older broker never uses a response publication";
    auto p = r.client.producer(noLinger());
    EXPECT_EQ(r.sendAcked(p, 0, 5), 5);
    EXPECT_TRUE(r.gw.binds.empty());
    EXPECT_EQ(r.client.testWorkerBindsSent(), 0u);
    EXPECT_GE(r.producesAt(w.endpoint), 1) << "produce unchanged: straight to the worker";
}

// A worker that only owns a SUBSCRIBED partition still gets its WorkerBind (eager, from Metadata) and its Records
// reach the subscriber on the worker's response publication.
TEST(ClientResponseEgressRegression, ConsumeOnlyWorkerIsBound) {
    Rig r(4150, 4151);
    const auto w = r.addWorker(0, 4152);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1; }));
    EXPECT_TRUE(r.gw.binds[0].accepted);
    EXPECT_TRUE(r.gw.binds[0].firstOnImage);
    int got = 0;
    auto sub = r.client.subscribe(r.client.topic("t"), 0, rc::SubscribeOptions{rc::From::earliest()},
                                  [&](const rc::Record&) { ++got; });
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() > 0; }));
    r.gw.pushRecordsFrom(r.gw.subscribed[0], 0, 2, w.endpoint);
    ASSERT_TRUE(r.drive([&] { return got >= 2; })) << "Records from the worker's response publication";
    EXPECT_EQ(r.producesAt(w.endpoint), 0) << "nothing was produced: the pair exists for the consumer";
}

// A connect-time Redirect "<endpoint>#<port>": the client reconnects to the BARE endpoint with a response
// subscription on that port, and the second gateway answers on it.
TEST(ClientResponseEgressRegression, RedirectPortRetargetsTheResponsePair) {
    Rig r(4160, 4161);
    const uint16_t bPort = freePort(), bResp = freePort();
    const int      b     = r.gw.addNode(loopback(bPort), r.in);
    r.gw.responseCtl[b]  = "127.0.0.1:" + std::to_string(bResp);
    r.gw.connectRedirect[0] = "127.0.0.1:" + std::to_string(bPort) + "#" + std::to_string(bResp);
    ASSERT_TRUE(r.connect()) << "the second gateway's reply did not arrive on the redirected response pair";
    EXPECT_GE(r.gw.connectsAt[b], 1);
    EXPECT_EQ(r.client.testTransport().ingressChannel(), loopback(bPort)) << "the bare endpoint, no '#port'";
    const std::string pubCh = r.client.testTransport().seedPublicationChannelForTest();
    EXPECT_EQ(pubCh.rfind(loopback(bPort) + "|response-correlation-id=", 0), 0u) << pubCh;
    EXPECT_TRUE(r.gw.connectFlags.back());
    EXPECT_GE(r.gw.responseConnects, 1);
}

// A produce Redirect carrying "#port" routes to the BARE endpoint (the Metadata node it names) and the new node's
// session is response-correlated although Metadata advertised no port for it: the Redirect's port is enough.
TEST(ClientResponseEgressRegression, DataOpRedirectWithPortStillRoutes) {
    Options o;
    o.partitions = 2;
    Rig r(4170, 4171, o);
    const uint16_t bPort = freePort(), bResp = freePort();
    const int      b     = r.gw.addNode(loopback(bPort), r.in);
    r.gw.responseCtl[b]  = "127.0.0.1:" + std::to_string(bResp);
    r.gw.moreBrokers     = {{1, "127.0.0.1", static_cast<int32_t>(bPort), 0}};
    // partition 0 is led by the seed node; partition 1 by node 1 (a legacy node route, so routing is multi-node).
    r.gw.partitions = {{1, 0, 0, 1, 0, ""}, {1, 1, 1, 1, r.in, ""}};
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    r.gw.redirectNext = 1;
    r.gw.redirectTo   = "127.0.0.1:" + std::to_string(bPort) + "#" + std::to_string(bResp);
    auto p = r.client.producer(noLinger());
    EXPECT_EQ(r.sendAcked(p, 0, 1), 1) << "the redirected produce must be re-offered to node 1 and acked";
    EXPECT_GE(r.producesAt(b), 1) << "routed to the bare endpoint";
    EXPECT_GE(r.gw.responseConnects, 2) << "node 1's session rode a response publication (port from the Redirect)";
    EXPECT_TRUE(r.gw.connectFlags.back());
}
