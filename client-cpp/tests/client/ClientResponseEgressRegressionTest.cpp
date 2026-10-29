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

// I2: an injected duty clock (the Client's ClientConfig::nanoClock); the test advances it to each bind deadline.
// The clock follows real time until the test freezes it (a connect needs real time: lost-datagram retries); a frozen
// clock only moves when the test advances g_fakeNowNs.
int64_t g_fakeNowNs = 0;
bool    g_clockFrozen = false;
int64_t steadyNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
int64_t g_clockOffsetNs = 0;   // an unfrozen clock continues from the frozen value (monotonic)
int64_t fakeNow() noexcept { return g_clockFrozen ? g_fakeNowNs : steadyNs() + g_clockOffsetNs; }

struct Options {
    bool fakeClock         = false;   ///< duty clock = g_fakeNowNs (advanced by the test)
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
        if (o.fakeClock) { g_clockFrozen = false; c.nanoClock = &fakeNow; g_clockOffsetNs = 0; }
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
// Under TLS the bindToken never rides the wire (I3: derived from the exported keys on both ends), and the
// WorkerBind is PLAINTEXT even on an encrypted session: the worker has no key for the image before it is bound.
TEST(ClientResponseEgressRegression, WorkerBindIsPlaintextUnderTlsAndTheTokenIsDerivedNotSentRegression) {
    Options o;
    o.tls = true;
    Rig r(4180, 4181, o);
    const auto w = r.addWorker(0, 4182);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.gw.tlsSessions == 1; })) << "the session is encrypted";
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1; })) << "no WorkerBind: the client did not derive its token";
    // I3: the first-leg ConnectResponse carried NO token (all-zero); the bind used the key-derived one.
    ASSERT_FALSE(r.gw.connectWireTokens.empty());
    for (const auto& wt : r.gw.connectWireTokens) EXPECT_EQ(wt, (std::array<uint8_t, 16>{})) << "a TLS first leg must carry no bind token";
    EXPECT_NE(r.gw.binds[0].token, (std::array<uint8_t, 16>{}));
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
    // The reconnect builds a FRESH pair (a spent response subscription never connects again): the old session's
    // [bind, produce] sit on the first worker image, the new session's [bind, produce] on a second one.
    std::vector<std::vector<uint16_t>> perImage;
    for (const auto& [key, fr] : r.gw.framesOnImage) if (key.first == w.endpoint) perImage.push_back(fr);
    ASSERT_EQ(perImage.size(), 2u);
    for (const auto& frames : perImage) {
        ASSERT_EQ(frames.size(), 2u);
        EXPECT_EQ(frames[0], kBindTemplate) << "the bind is the first frame on its image";
        EXPECT_EQ(frames[1], kProduceTemplate);
    }
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

// ---- I2: the worker acknowledges the bind; the client holds produce until the ack, resends, then reconnects ----------

namespace {
// Offer one acked send of a key on partition 0 without waiting (the produce is held until the bind is acknowledged).
// Connect with the binds held, then freeze the duty clock at "now" and release them: the first WorkerBind is sent at
// exactly g_fakeNowNs, so every later deadline is g_fakeNowNs + k * 250 ms.
void connectThenFreeze(Rig& r) {
    r.client.testHoldWorkerBinds(true);
    ASSERT_TRUE(r.connect());
    g_fakeNowNs   = steadyNs() + g_clockOffsetNs;
    g_clockFrozen = true;
    r.client.testHoldWorkerBinds(false);
}
// Advance the frozen duty clock by `ns` in 100 ms chunks, pumping client + fake gateway after each, so the seed's
// keepalives (answered by the fake) keep the session live across a long advance.
void advanceClock(Rig& r, int64_t ns) {
    while (ns > 0) {
        const int64_t step = ns < 100'000'000LL ? ns : 100'000'000LL;
        g_fakeNowNs += step;
        ns -= step;
        (void)r.drive([] { return false; }, 300);
    }
}
rc::Token sendOne(Rig& r, rc::Producer& p) {
    const rc::Token t = r.nextToken++;
    EXPECT_EQ(p.trySend(r.client.topic("t"), rc::Key(uint64_t{1}), std::span<const uint8_t>(kVal), r.cq, t), rc::SendResult::Ok);
    return t;
}
bool completedOk(Rig& r, rc::Token t) {
    bool done = false;
    (void)r.drive([&] {
        r.cq.poll([&](const rc::Completion& cm, std::span<const uint8_t>) { if (cm.token == t && cm.status == rc::OpStatus::Ok) done = true; });
        return done;
    });
    return done;
}
} // namespace

// No ack ever: the bind is resent with capped exponential backoff (250, 500, 1000, 2000, 4000, 4000 ms gaps on the duty
// clock, nothing earlier) and NEVER gives up: no reconnect (same session id, same Connect count), the produce stays held.
TEST(ClientAgentRegression, WorkerBindUnackedResendsWithBackoffRegression) {
    Options o; o.fakeClock = true;
    Rig r(4200, 4201, o);
    const auto w = r.addWorker(0, 4202);
    r.gw.noBindAcks = true;
    r.gw.replyPongs = true;   // keepalives stay answered across the long clock advances
    connectThenFreeze(r);
    if (::testing::Test::HasFatalFailure()) return;
    auto p = r.client.producer(noLinger());
    const rc::Token t = sendOne(r, p);
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1; })) << "the first WorkerBind never went out";
    const int64_t firstSid = r.gw.binds[0].sessionId;
    const int64_t seedSid  = r.client.testSeedSessionId();
    const int connectsBefore = r.gw.connectsArrived;
    const int64_t gapsMs[] = {250, 500, 1000, 2000, 4000, 4000};
    std::size_t k = 0;
    for (const int64_t gap : gapsMs) {
        ++k;
        advanceClock(r, (gap - 1) * 1'000'000LL);   // short of the deadline: nothing is resent
        ASSERT_EQ(r.gw.binds.size(), k) << "resent before the backoff gap of " << gap << " ms elapsed";
        g_fakeNowNs += 1'000'000LL;                 // exactly at the deadline
        ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == k + 1; })) << "resend " << k << " never went out";
        EXPECT_EQ(r.gw.binds.back().sessionId, firstSid);
    }
    EXPECT_EQ(r.client.testWorkerBindResends(), 6u);
    EXPECT_EQ(r.gw.connectsArrived, connectsBefore) << "no reconnect: a bind is never abandoned by a timer";
    EXPECT_EQ(r.client.testSeedSessionId(), seedSid);
    EXPECT_TRUE(r.gw.produced.empty()) << "the produce must stay held while the bind is unacknowledged";
    EXPECT_EQ(r.producesAt(w.endpoint), 0);
    // The worker finally answers: the held produce completes on the SAME session.
    const auto b = rp::WorkerBind::decode(r.gw.workerBindFrames.back().data(), r.gw.workerBindFrames.back().size());
    ASSERT_TRUE(b.has_value());
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId);
    EXPECT_TRUE(completedOk(r, t));
    EXPECT_EQ(r.client.testSeedSessionId(), seedSid);
}

// The ack arrives -> the held produce is offered at once, with no resend.
TEST(ClientAgentRegression, WorkerBindAckReleasesHeldProduceRegression) {
    Options o; o.fakeClock = true;
    Rig r(4210, 4211, o);
    const auto w = r.addWorker(0, 4212);
    r.gw.noBindAcks = true;
    connectThenFreeze(r);
    if (::testing::Test::HasFatalFailure()) return;
    auto p = r.client.producer(noLinger());
    const rc::Token t = sendOne(r, p);
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1 && r.client.testHeldOffers() > 0; }));
    (void)r.drive([] { return false; }, 500);
    EXPECT_TRUE(r.gw.produced.empty()) << "held while unacknowledged";
    EXPECT_EQ(r.producesAt(w.endpoint), 0);
    // The worker's answer arrives now (a forged-in-place copy of what the real worker sends).
    const auto b = rp::WorkerBind::decode(r.gw.workerBindFrames[0].data(), r.gw.workerBindFrames[0].size());
    ASSERT_TRUE(b.has_value());
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId);
    EXPECT_TRUE(completedOk(r, t));
    EXPECT_EQ(r.gw.binds.size(), 1u) << "no resend: the ack bound it";
    EXPECT_EQ(r.client.testWorkerBindAcksAccepted(), 1u);
    EXPECT_EQ(r.producesAt(w.endpoint), 1);
    EXPECT_EQ(r.gw.producesBeforeBind, 0);
}

// An ack with an old / foreign correlation id never binds (the fake routes replies by session, so a foreign sid cannot
// be forged here); the exact (sid, corr) does.
TEST(ClientAgentRegression, StaleWorkerBindAckIgnoredRegression) {
    Options o; o.fakeClock = true;
    Rig r(4220, 4221, o);
    const auto w = r.addWorker(0, 4222);
    r.gw.noBindAcks = true;
    connectThenFreeze(r);
    if (::testing::Test::HasFatalFailure()) return;
    auto p = r.client.producer(noLinger());
    const rc::Token t = sendOne(r, p);
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 1; }));
    const auto b = rp::WorkerBind::decode(r.gw.workerBindFrames[0].data(), r.gw.workerBindFrames[0].size());
    ASSERT_TRUE(b.has_value());
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId + 1000);   // an old / foreign correlation
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId - 1);      // an older bind's correlation
    ASSERT_TRUE(r.drive([&] { return r.client.testWorkerBindAcksIgnored() == 2u; })) << "the stale acks were not even seen";
    (void)r.drive([] { return false; }, 500);
    EXPECT_EQ(r.client.testWorkerBindAcksAccepted(), 0u);
    EXPECT_TRUE(r.gw.produced.empty()) << "a stale ack must not release the produce";
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId);
    EXPECT_TRUE(completedOk(r, t));
    EXPECT_EQ(r.client.testWorkerBindAcksAccepted(), 1u);
    // a late duplicate of the accepted ack changes nothing
    r.gw.sendBindAck(w.endpoint, b->sessionId, b->correlationId);
    ASSERT_TRUE(r.drive([&] { return r.client.testWorkerBindAcksIgnored() == 3u; }));
}

// A lost FIRST ack (datagram) is recovered by the resend: the worker answers a repeated bind again.
TEST(ClientAgentRegression, LostWorkerBindAckIsRecoveredByResendRegression) {
    Options o; o.fakeClock = true;
    Rig r(4230, 4231, o);
    (void)r.addWorker(0, 4232);
    r.gw.bindAcksToDrop = 1;
    connectThenFreeze(r);
    if (::testing::Test::HasFatalFailure()) return;
    auto p = r.client.producer(noLinger());
    const rc::Token t = sendOne(r, p);
    ASSERT_TRUE(r.drive([&] { return r.gw.bindAcksDropped == 1; }));
    g_fakeNowNs += 250'000'000LL;
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 2; }));
    EXPECT_TRUE(completedOk(r, t));
    EXPECT_EQ(r.client.testWorkerBindResends(), 1u);
}

// Aeron pins a response subscription to the FIRST response publication session that reaches it, so a second response
// publication to the same request image never connects: every new connect episode and every session bind needs a FRESH
// pair (new response subscription + new request publication = a new image).  The fake answers per image, so the
// observable is the image: the reconnect's Connect and the rebind's WorkerBind arrive on images the first session never used.
TEST(ClientResponseEgressRegression, ReconnectBuildsAFreshResponsePairRegression) {
    Rig r(4250, 4251);
    const auto w = r.addWorker(0, 4252);
    ASSERT_TRUE(r.connect());
    auto p = r.client.producer(noLinger());
    ASSERT_EQ(r.sendAcked(p, 0, 2), 2);
    auto imagesAt = [&](int endpoint) {
        std::vector<int32_t> out;
        for (const auto& [key, frames] : r.gw.framesOnImage) if (key.first == endpoint) out.push_back(key.second);
        return out;
    };
    ASSERT_EQ(r.gw.binds.size(), 1u);
    const int64_t oldSid = r.gw.binds[0].sessionId;
    const auto gwImages1 = imagesAt(0), wkImages1 = imagesAt(w.endpoint);
    ASSERT_EQ(gwImages1.size(), 1u);
    ASSERT_EQ(wkImages1.size(), 1u);

    r.gw.expireSession(oldSid);   // the broker ends the session: the client reconnects and the worker is rebound
    ASSERT_TRUE(r.drive([&] { return r.gw.binds.size() == 2 && r.client.testWorkerBindAcksAccepted() == 2u; }))
        << "no rebind after the reconnect: binds=" << r.gw.binds.size() << " connects=" << r.gw.connectsArrived
        << " acks=" << r.client.testWorkerBindAcksAccepted();
    EXPECT_NE(r.gw.binds[1].sessionId, oldSid);
    const auto gwImages2 = imagesAt(0), wkImages2 = imagesAt(w.endpoint);
    EXPECT_EQ(gwImages2.size(), 2u) << "the reconnect's Connect must ride a fresh request image (a spent response pair never connects again)";
    EXPECT_EQ(wkImages2.size(), 2u) << "the new session's WorkerBind must ride a fresh worker image";
    EXPECT_EQ(r.gw.binds[1].imageSid, [&] { for (int32_t i : wkImages2) if (i != wkImages1[0]) return i; return 0; }())
        << "the second bind arrived on the new worker image";
    EXPECT_NE(r.gw.binds[1].imageSid, r.gw.binds[0].imageSid);
    EXPECT_TRUE(r.gw.binds[1].firstOnImage);
    ASSERT_EQ(r.sendAcked(p, 0, 2), 2) << "produce completes on the fresh pair";
}

// ---- C-A: a response pair is spent when a Connect is ACCEPTED by its publication ----------------------------------------

// The gateway answers Connect corr1 only after the client's connect timeout already sent corr2 on the same pair.  A real
// gateway opens one response publication per ConnectRequest; Aeron pins the client's response subscription to the FIRST
// one that connects, so a second Connect on the same pair can never be answered.  The pair is spent the moment a Connect
// is accepted, so corr2 must ride a NEW request image.
TEST(ResponsePairRegression, LateAnswerToFirstConnectDoesNotWedgeTheSeedRegression) {
    Options o; o.fakeClock = true;
    Rig r(4300, 4301, o);
    r.gw.holdConnectAnswers = 2;   // corr1 and corr2 are both held
    r.gw.replyPongs = true;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(r.client.connect(r.cq, t), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsArrived >= 1; })) << "the first Connect never arrived";
    g_fakeNowNs   = steadyNs() + g_clockOffsetNs;   // freeze: only the test moves the duty clock from here
    g_clockFrozen = true;
    const int connectsAfterFirst = r.gw.connectsArrived;
    g_fakeNowNs += 5'001'000'000LL;   // past connectTimeoutNs: the machine sends Connect corr2
    // The fresh pair resolves asynchronously: the Connect is refused until it is Ready and the machine's fast resend
    // (200 ms cadence on the duty clock) delivers it.
    for (int k = 0; k < 16 && r.gw.connectsArrived == connectsAfterFirst; ++k) {
        g_fakeNowNs += 250'000'000LL;
        (void)r.drive([&] { return r.gw.connectsArrived > connectsAfterFirst; }, 20000);
    }
    ASSERT_GT(r.gw.connectsArrived, connectsAfterFirst) << "no second Connect after the timeout";
    ASSERT_GE(r.gw.connectCorrIds.size(), 2u);
    EXPECT_NE(r.gw.connectCorrIds[0], r.gw.connectCorrIds[1]);
    auto imagesAt0 = [&] {
        std::size_t n = 0;
        for (const auto& [key, frames] : r.gw.framesOnImage) n += key.first == 0 ? 1 : 0;
        return n;
    };
    EXPECT_EQ(imagesAt0(), 2u) << "corr2 must arrive on a NEW request image (the first pair was spent by corr1)";
    // The late answers: corr1 first (it pins the first response subscription), then corr2.
    r.gw.releaseHeldConnectAnswers();
    g_clockOffsetNs = g_fakeNowNs - steadyNs();   // the rest of the connect runs on real time again
    g_clockFrozen   = false;
    // (The connect OP's own 5 s deadline passed with the clock jump: its Timeout completion is not the verdict - the
    // session is.)
    EXPECT_TRUE(r.drive([&] { return r.client.testSeedSessionId() != 0; }))
        << "the session never connected: it is wedged on a spent pair";
    EXPECT_TRUE(r.client.testSeedResponseCorrelated());
    const rc::Token pt = r.nextToken++;
    const uint8_t payload[] = {1, 2, 3};
    ASSERT_EQ(r.client.ping(std::span<const uint8_t>(payload), r.cq, pt), rc::OpResult::Accepted);
    EXPECT_EQ(r.awaitToken(pt).status, rc::OpStatus::Ok) << "a ping must round-trip on the re-paired session";
}

// I-C: a TLS session whose bind-token derivation fails (or yields all-zero) must never send a WorkerBind with a zero
// token: the session is reconnected on the evidence, the failure is counted, and once derivation works the bind goes.
TEST(ClientResponseEgressRegression, ZeroDerivedBindTokenReconnectsAndNeverBindsRegression) {
    Options o;
    o.tls = true;
    Rig r(4330, 4331, o);
    (void)r.addWorker(0, 4332);
    r.client.testForceBindDeriveFailure(true);
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.client.testBindTokenDeriveFailures() >= 1 && r.gw.connectsArrived >= 2; }))
        << "a failed derivation must end the session (reconnect); failures=" << r.client.testBindTokenDeriveFailures()
        << " connects=" << r.gw.connectsArrived;
    EXPECT_TRUE(r.gw.binds.empty()) << "a WorkerBind went out although the token could not be derived";
    EXPECT_TRUE(r.gw.workerBindFrames.empty());
    r.client.testForceBindDeriveFailure(false);
    ASSERT_TRUE(r.drive([&] { return r.client.testWorkerBindAcksAccepted() >= 1; })) << "no bind once derivation works";
    for (const auto& b : r.gw.binds) EXPECT_NE(b.token, (std::array<uint8_t, 16>{}));
}

// A NON-seed (leader) session's Connect is answered with ErrorResponse{Redirect} naming its own endpoint: the machine
// resends on the same endpoint.  The gateway opened a response publication for the refused Connect, which pinned the
// session's response subscription, so the resend must ride a NEW request image (a fresh pair).
TEST(ResponsePairRegression, ConnectErrorRedirectOnANonSeedSessionRePairsRegression) {
    Options o;
    o.partitions = 2;
    Rig r(4310, 4311, o);
    const uint16_t bPort = freePort(), bResp = freePort();
    const int      b     = r.gw.addNode(loopback(bPort), r.in);
    r.gw.responseCtl[b]  = "127.0.0.1:" + std::to_string(bResp);
    r.gw.moreBrokers     = {{1, "127.0.0.1", static_cast<int32_t>(bPort), static_cast<int32_t>(bResp)}};
    r.gw.partitions = {{1, 0, 0, 1, 0, ""}, {1, 1, 1, 1, r.in, ""}};
    r.gw.connectRedirect[b] = "127.0.0.1:" + std::to_string(bPort) + "#" + std::to_string(bResp);   // its OWN endpoint
    ASSERT_TRUE(r.connect());
    ASSERT_TRUE(r.drive([&] { return r.client.testMetadataResponses() > 0; }));
    auto p = r.client.producer(noLinger());
    rc::Token t = 0;
    for (uint64_t k = 1; k < 1000 && t == 0; ++k) {
        const rc::Key key(k);
        if (key.partition(2) != 1) continue;
        t = r.nextToken++;
        ASSERT_EQ(p.trySend(r.client.topic("t"), key, std::span<const uint8_t>(kVal), r.cq, t), rc::SendResult::Ok);
    }
    ASSERT_NE(t, 0);
    ASSERT_TRUE(r.drive([&] { return r.gw.connectsAt[b] >= 1; })) << "the leader session never connected to node 1";
    r.gw.connectRedirect.erase(b);   // the next Connect there is answered normally
    EXPECT_TRUE(completedOk(r, t)) << "the resend rode the pinned pair: the session to node 1 never connected";
    std::size_t images = 0;
    for (const auto& [key, frames] : r.gw.framesOnImage) images += key.first == b ? 1 : 0;
    EXPECT_GE(images, 2u) << "the resend after the Redirect must ride a NEW request image";
}

// A Connect the transport REFUSES (the request publication is not connected yet) never reached the gateway, so it spent
// nothing: the machine's fast resend reuses the same pair (no new response subscription is registered).
TEST(ResponsePairRegression, RefusedConnectDoesNotSpendThePairRegression) {
    Options o; o.fakeClock = true;
    Rig r(4320, 4321, o);
    g_fakeNowNs   = steadyNs() + g_clockOffsetNs;
    g_clockFrozen = true;
    const rc::Token t = r.nextToken++;
    ASSERT_EQ(r.client.connect(r.cq, t), rc::OpResult::Accepted);
    auto clientOnly = [&] { for (int i = 0; i < 100; ++i) r.client.doWork(); };   // the fake gateway is NOT pumped: nothing connects
    clientOnly();
    const uint64_t base = r.client.testTransport().responseSubsAddedForTest();
    for (int k = 0; k < 3; ++k) {
        g_fakeNowNs += 250'000'000LL;   // connectRetryIntervalNs: the machine resends the refused Connect
        clientOnly();
    }
    EXPECT_EQ(r.gw.connectsArrived, 0);
    EXPECT_EQ(r.client.testTransport().responseSubsAddedForTest(), base) << "a refused Connect must not re-pair";
    // The gateway comes up: the next resend is accepted and the session connects on the very same pair.
    for (int k = 0; k < 40 && r.gw.connectsArrived == 0; ++k) {
        g_fakeNowNs += 250'000'000LL;
        (void)r.drive([&] { return r.gw.connectsArrived > 0; }, 20000);
    }
    ASSERT_GT(r.gw.connectsArrived, 0);
    g_clockOffsetNs = g_fakeNowNs - steadyNs();
    g_clockFrozen   = false;
    EXPECT_EQ(r.awaitToken(t).status, rc::OpStatus::Ok);
    EXPECT_EQ(r.client.testTransport().responseSubsAddedForTest(), base) << "the accepted first Connect rode the original pair";
}
