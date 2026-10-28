// ResponseEgressSpikeRegressionTest.cpp — SPIKE (reference + guard) that de-risks NAT-safe egress via Aeron
// response channels (control-mode=response), against the exact Aeron version this repo links (1.51.0).
//
// Mechanic (exact channel strings, as built by aeron::ChannelUriStringBuilder):
//   server : request subscription  aeron:udp?endpoint=127.0.0.1:<S_REQ>                          (stream 3001)
//   client : response subscription aeron:udp?control=127.0.0.1:<S_RSP>|control-mode=response      (stream 3002)
//   client : request publication   aeron:udp?endpoint=127.0.0.1:<S_REQ>|response-correlation-id=<rsp sub registrationId>
//   server : per request image, EXCLUSIVE response publication
//            aeron:udp?control=127.0.0.1:<S_RSP>|control-mode=response|response-correlation-id=<image->correlationId()>
//
// Findings (each guarded by a test below):
//   * OneControlPortServesManyClients: N exclusive response publications share ONE response-control port
//     (<S_RSP>); each reaches only its correlated client (no cross-delivery, in order, no loss).
//   * ReplyFollowsTheResponseFlow: the reply goes to the address the client's response-subscription flow
//     comes from, never to a declared address and not to the request flow's source port (the request
//     publication and the response subscription are different client sockets; Aeron 1.51 has no `bind`).
//   * ClientGoneEndsTheResponsePublication: when the client vanishes the response publication goes
//     not-connected after Aeron's publication connection timeout and releasing it frees its counters.
//
// CONCLUSION: N exclusive response publications share ONE response-control port; each reaches only its
// correlated client; replies follow the client's response-subscription flow (no bind/declared address); a
// vanished client's publication goes not-connected after Aeron's publication connection timeout and frees
// its counters.
//
// Wall time: test 3 waits for Aeron's publication connection timeout (the driver clock is not injectable
// through EmbeddedDriver); that wait is bounded by a steady_clock deadline AND an iteration cap.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <ChannelUriStringBuilder.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <atomic>
#include <vector>

using namespace rune::node;
namespace fs = std::filesystem;

namespace {

constexpr int32_t kReqStream = 3001;
constexpr int32_t kRspStream = 3002;

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_rspk_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); fs::remove(path.string() + ".lock", ec); }
};

EmbeddedDriverConfig cfgAt(const fs::path& p) {
    EmbeddedDriverConfig c;
    c.dir = p.string();
    c.termBufferBytes = 64 * 1024;
    c.clientLivenessTimeoutNs = 2'000'000'000;  // 2 s; must exceed the 1 s driver timer
    return c;
}

struct Node {
    TmpDir dir;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> aeron;
    Node() {
        drv = std::make_unique<EmbeddedDriver>(cfgAt(dir.path), DriverMode::Invoker);
        aeron::Context ctx;
        ctx.aeronDir(dir.path.string());
        ctx.useConductorAgentInvoker(true);
        aeron = aeron::Aeron::connect(ctx);
    }
    void pump() {
        drv->doWork();
        aeron->conductorAgentInvoker().invoke();
    }
};

// Client side: response subscription + request publication (declared AFTER node => destroyed BEFORE it).
struct Client {
    Node node;
    std::shared_ptr<aeron::Subscription> rsp;
    std::shared_ptr<aeron::Publication> req;
    int expectedMsg = 0;
    int received = 0;
    int foreign = 0;
    int outOfOrder = 0;
    int replies = 0;
};

std::string ep(uint16_t port) { return "127.0.0.1:" + std::to_string(port); }

std::string respSubChannel(const std::string& rspCtl) {
    return aeron::ChannelUriStringBuilder().media("udp").controlMode(aeron::CONTROL_MODE_RESPONSE)
        .controlEndpoint(rspCtl).build();
}
std::string reqPubChannel(const std::string& reqEp, int64_t rspSubRegId) {
    return aeron::ChannelUriStringBuilder().media("udp").endpoint(reqEp)
        .responseCorrelationId(rspSubRegId).build();
}
std::string rspPubChannel(const std::string& rspCtl, int64_t imageCorrelationId) {
    return aeron::ChannelUriStringBuilder().media("udp").controlMode(aeron::CONTROL_MODE_RESPONSE)
        .controlEndpoint(rspCtl).responseCorrelationId(imageCorrelationId).build();
}

// Resolve an async id exactly once (find throws for an already-resolved id).  `pump` drives everything.
template <class Pump, class Find>
auto resolveOnce(Pump&& pump, Find&& find, int cap = 200'000) {
    decltype(find()) r{};
    for (int i = 0; i < cap && !r; ++i) {
        r = find();
        if (!r) pump();
    }
    return r;
}

size_t countersCount(aeron::Aeron& a) {
    size_t n = 0;
    a.countersReader().forEach([&](int32_t, int32_t, const aeron::concurrent::AtomicBuffer&, const std::string&) { ++n; });
    return n;
}

constexpr auto kWallDeadline = std::chrono::seconds(15);

}  // namespace

TEST(ResponseEgressSpikeRegression, OneControlPortServesManyClientsRegression) {
    constexpr int N = 8;
    constexpr int kMsgs = 1000;
    const std::string reqEp = ep(rune::aeronutil::testing::allocFreeUdpPort());
    const std::string rspCtl = ep(rune::aeronutil::testing::allocFreeUdpPort());

    Node server;
    std::vector<std::unique_ptr<Client>> clients;
    std::shared_ptr<aeron::Subscription> reqSub;
    std::map<int32_t, std::shared_ptr<aeron::ExclusivePublication>> pubBySession;
    std::vector<std::shared_ptr<aeron::ExclusivePublication>> pubByClient(N);

    auto pumpAll = [&] {
        server.pump();
        for (auto& c : clients) c->node.pump();
    };
    auto pumpUntil = [&](auto cond, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) {
            if (cond()) return true;
            pumpAll();
        }
        return cond();
    };
    auto pollClients = [&] {
        for (int i = 0; i < N; ++i) {
            Client& c = *clients[i];
            c.rsp->poll([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                            aeron::Header&) {
                if (len != 8) { ++c.foreign; return; }
                const int32_t tag = b.getInt32(off), msg = b.getInt32(off + 4);
                if (tag != i) { ++c.foreign; return; }
                if (msg != c.expectedMsg) ++c.outOfOrder;
                c.expectedMsg = msg + 1;
                ++c.received;
            }, 64);
        }
    };

    int64_t subId = server.aeron->addSubscription("aeron:udp?endpoint=" + reqEp, kReqStream);
    reqSub = resolveOnce(pumpAll, [&] { return server.aeron->findSubscription(subId); });
    ASSERT_TRUE(reqSub) << "server request subscription never resolved";

    for (int i = 0; i < N; ++i) {
        clients.push_back(std::make_unique<Client>());
        Client& c = *clients.back();
        int64_t rid = c.node.aeron->addSubscription(respSubChannel(rspCtl), kRspStream);
        c.rsp = resolveOnce(pumpAll, [&] { return c.node.aeron->findSubscription(rid); });
        ASSERT_TRUE(c.rsp) << "client " << i << " response subscription never resolved";
        int64_t pid = c.node.aeron->addPublication(reqPubChannel(reqEp, c.rsp->registrationId()), kReqStream);
        c.req = resolveOnce(pumpAll, [&] { return c.node.aeron->findPublication(pid); });
        ASSERT_TRUE(c.req) << "client " << i << " request publication never resolved";
    }

    ASSERT_TRUE(pumpUntil([&] { return reqSub->imageCount() == N; })) << "imageCount=" << reqSub->imageCount();

    // One response publication per request image; ALL on the single response-control endpoint.
    std::vector<std::pair<int32_t, int64_t>> regIds;  // sessionId, registrationId
    std::map<int64_t, int32_t> sessionByReg;
    for (int k = 0; k < reqSub->imageCount(); ++k) {
        auto image = reqSub->imageByIndex(k);
        ASSERT_TRUE(image);
        int64_t id = server.aeron->addExclusivePublication(rspPubChannel(rspCtl, image->correlationId()), kRspStream);
        sessionByReg[id] = image->sessionId();
    }
    for (auto& [id, session] : sessionByReg) {
        auto p = resolveOnce(pumpAll, [&] { return server.aeron->findExclusivePublication(id); });
        ASSERT_TRUE(p) << "response publication never resolved for session " << session;
        pubBySession[session] = p;
    }
    ASSERT_EQ(pubBySession.size(), static_cast<size_t>(N));
    ASSERT_TRUE(pumpUntil([&] {
        for (auto& [s, p] : pubBySession) if (!p->isConnected()) return false;
        return true;
    })) << "response publications never connected";

    // Identify which publication belongs to which client: each client sends its index.
    uint8_t buf[8];
    aeron::concurrent::AtomicBuffer ab(buf, sizeof(buf));
    for (int i = 0; i < N; ++i) {
        ab.putInt32(0, i);
        ASSERT_TRUE(pumpUntil([&] { return clients[i]->req->offer(ab, 0, 4) > 0; })) << "request offer " << i;
    }
    int mapped = 0;
    ASSERT_TRUE(pumpUntil([&] {
        reqSub->poll([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                         aeron::Header& h) {
            if (len != 4) return;
            const int32_t idx = b.getInt32(off);
            if (idx < 0 || idx >= N || pubByClient[idx]) return;
            auto it = pubBySession.find(h.sessionId());
            if (it == pubBySession.end()) return;
            pubByClient[idx] = it->second;
            ++mapped;
        }, 16);
        return mapped == N;
    })) << "mapped=" << mapped;

    // Fan out kMsgs per client through the one control port.
    for (int msg = 0; msg < kMsgs; ++msg) {
        for (int i = 0; i < N; ++i) {
            ab.putInt32(0, i);
            ab.putInt32(4, msg);
            bool sent = false;
            for (int t = 0; t < 200'000 && !sent; ++t) {
                const int64_t r = pubByClient[i]->offer(ab, 0, 8);
                if (r > 0) { sent = true; break; }
                pumpAll();
                pollClients();
            }
            ASSERT_TRUE(sent) << "offer stuck, client " << i << " msg " << msg;
        }
        pollClients();
    }
    ASSERT_TRUE(pumpUntil([&] {
        pollClients();
        for (auto& c : clients) if (c->received < kMsgs) return false;
        return true;
    })) << "not all responses delivered";
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(clients[i]->received, kMsgs) << "client " << i;
        EXPECT_EQ(clients[i]->foreign, 0) << "client " << i << " saw another client's payload";
        EXPECT_EQ(clients[i]->outOfOrder, 0) << "client " << i;
    }
    pollClients();
    for (int i = 0; i < N; ++i) EXPECT_EQ(clients[i]->received, kMsgs) << "client " << i << " got extras";

    pubByClient.clear();
    pubBySession.clear();
    reqSub.reset();
}

TEST(ResponseEgressSpikeRegression, ReplyFollowsTheResponseFlowRegression) {
    const std::string reqEp = ep(rune::aeronutil::testing::allocFreeUdpPort());
    const std::string rspCtl = ep(rune::aeronutil::testing::allocFreeUdpPort());

    Node server;
    Client client;
    auto pumpAll = [&] { server.pump(); client.node.pump(); };
    auto pumpUntil = [&](auto cond, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) { if (cond()) return true; pumpAll(); }
        return cond();
    };

    int64_t subId = server.aeron->addSubscription("aeron:udp?endpoint=" + reqEp, kReqStream);
    auto reqSub = resolveOnce(pumpAll, [&] { return server.aeron->findSubscription(subId); });
    ASSERT_TRUE(reqSub);
    int64_t rid = client.node.aeron->addSubscription(respSubChannel(rspCtl), kRspStream);
    client.rsp = resolveOnce(pumpAll, [&] { return client.node.aeron->findSubscription(rid); });
    ASSERT_TRUE(client.rsp);
    int64_t pid = client.node.aeron->addPublication(
        reqPubChannel(reqEp, client.rsp->registrationId()), kReqStream);
    client.req = resolveOnce(pumpAll, [&] { return client.node.aeron->findPublication(pid); });
    ASSERT_TRUE(client.req);

    ASSERT_TRUE(pumpUntil([&] { return reqSub->imageCount() == 1; }));
    auto image = reqSub->imageByIndex(0);
    ASSERT_TRUE(image);
    const std::string src = image->sourceIdentity();
    auto portOf = [](const std::string& hp) { return hp.substr(hp.rfind(':') + 1); };
    const std::string reqSrcPort = portOf(src);

    int64_t pubId = server.aeron->addExclusivePublication(rspPubChannel(rspCtl, image->correlationId()), kRspStream);
    auto rspPub = resolveOnce(pumpAll, [&] { return server.aeron->findExclusivePublication(pubId); });
    ASSERT_TRUE(rspPub);
    ASSERT_TRUE(pumpUntil([&] { return rspPub->isConnected() && client.rsp->isConnected() && client.req->isConnected(); }));

    // The reply goes to the address the client's response-subscription flow comes from — never to a declared
    // address, and not to the request flow's source port: they are different client sockets.
    std::vector<std::string> addrs;
    ASSERT_TRUE(pumpUntil([&] { addrs = client.rsp->localSocketAddresses(); return !addrs.empty(); }))
        << "response subscription reports no local socket address";
    ASSERT_EQ(addrs.size(), 1u) << "addrs[0]=" << addrs[0];
    ASSERT_NE(portOf(addrs[0]), reqSrcPort)
        << "response flow " << addrs[0] << " vs request source " << src;

    uint8_t buf[8] = {};
    aeron::concurrent::AtomicBuffer ab(buf, sizeof(buf));
    ab.putInt32(0, 42);
    ASSERT_TRUE(pumpUntil([&] { return client.req->offer(ab, 0, 4) > 0; }));
    int gotReq = 0;
    ASSERT_TRUE(pumpUntil([&] {
        reqSub->poll([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                         aeron::Header&) { if (len == 4 && b.getInt32(off) == 42) ++gotReq; }, 4);
        return gotReq == 1;
    })) << "request never reached the server";

    ab.putInt32(0, 4242);
    ASSERT_TRUE(pumpUntil([&] { return rspPub->offer(ab, 0, 4) > 0; }));
    ASSERT_TRUE(pumpUntil([&] {
        client.rsp->poll([&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                             aeron::Header&) { if (len == 4 && b.getInt32(off) == 4242) ++client.replies; }, 4);
        return client.replies == 1;
    })) << "reply never arrived on the client's response subscription";

    rspPub.reset();
    reqSub.reset();
    image.reset();
}

TEST(ResponseEgressSpikeRegression, ClientGoneEndsTheResponsePublicationRegression) {
    const std::string reqEp = ep(rune::aeronutil::testing::allocFreeUdpPort());
    const std::string rspCtl = ep(rune::aeronutil::testing::allocFreeUdpPort());

    Node server;
    auto client = std::make_unique<Client>();
    auto pumpBoth = [&] { server.pump(); if (client) client->node.pump(); };
    auto pumpServer = [&] { server.pump(); };
    auto pumpUntil = [&](auto cond, int cap = 200'000) {
        for (int i = 0; i < cap; ++i) { if (cond()) return true; pumpBoth(); }
        return cond();
    };
    // The one wall-time wait: Aeron's publication connection timeout runs on the driver clock, which is not
    // injectable through EmbeddedDriver.  Bounded by BOTH a steady_clock deadline and an iteration cap.
    auto pumpServerUntilWall = [&](auto cond) {
        const auto deadline = std::chrono::steady_clock::now() + kWallDeadline;
        for (long i = 0; i < 400'000'000L; ++i) {
            if (cond()) return true;
            if (std::chrono::steady_clock::now() > deadline) return cond();
            pumpServer();
        }
        return cond();
    };

    int64_t subId = server.aeron->addSubscription("aeron:udp?endpoint=" + reqEp, kReqStream);
    auto reqSub = resolveOnce(pumpBoth, [&] { return server.aeron->findSubscription(subId); });
    ASSERT_TRUE(reqSub);
    int64_t rid = client->node.aeron->addSubscription(respSubChannel(rspCtl), kRspStream);
    client->rsp = resolveOnce(pumpBoth, [&] { return client->node.aeron->findSubscription(rid); });
    ASSERT_TRUE(client->rsp);
    int64_t pid = client->node.aeron->addPublication(reqPubChannel(reqEp, client->rsp->registrationId()), kReqStream);
    client->req = resolveOnce(pumpBoth, [&] { return client->node.aeron->findPublication(pid); });
    ASSERT_TRUE(client->req);
    ASSERT_TRUE(pumpUntil([&] { return reqSub->imageCount() == 1; }));
    auto image = reqSub->imageByIndex(0);
    ASSERT_TRUE(image);

    const size_t baseline = countersCount(*server.aeron);

    int64_t pubId = server.aeron->addExclusivePublication(rspPubChannel(rspCtl, image->correlationId()), kRspStream);
    auto rspPub = resolveOnce(pumpBoth, [&] { return server.aeron->findExclusivePublication(pubId); });
    ASSERT_TRUE(rspPub);
    ASSERT_TRUE(pumpUntil([&] { return rspPub->isConnected() && client->rsp->isConnected(); }));

    // Round trip first.
    uint8_t buf[4] = {};
    aeron::concurrent::AtomicBuffer ab(buf, sizeof(buf));
    ab.putInt32(0, 7);
    int got = 0;
    ASSERT_TRUE(pumpUntil([&] { return rspPub->offer(ab, 0, 4) > 0; }));
    ASSERT_TRUE(pumpUntil([&] {
        client->rsp->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t len,
                              aeron::Header&) { if (len == 4) ++got; }, 4);
        return got == 1;
    })) << "round trip failed";

    // The client vanishes completely: publication, subscription, Aeron client, driver.
    client.reset();
    image.reset();

    ASSERT_TRUE(pumpServerUntilWall([&] { return !rspPub->isConnected(); }))
        << "response publication stayed connected after the client vanished";

    rspPub.reset();
    size_t after = countersCount(*server.aeron);
    const bool back = pumpServerUntilWall([&] { return (after = countersCount(*server.aeron)) == baseline; });
    EXPECT_TRUE(back) << "counters baseline=" << baseline << " after=" << after;

    reqSub.reset();
}
