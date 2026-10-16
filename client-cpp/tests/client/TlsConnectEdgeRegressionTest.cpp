// DR client bugs fix round 4 (client-failover-review.md "Fix round 3 check", the TLS
// handshake class): the seed is Connected only when its TLS handshake completes, and
// every connect-time action (connect() Ok, the connect-time MetadataRequest, pending
// Subscribes, a group consumer's JoinGroup, onConnected) runs on THAT edge — never at
// ConnectResponse{OK} while the session is still Handshaking.  Under require_tls no
// frame leaves the client before its keys exist.
//
// The rig: a real Aeron (IPC) fake gateway driving a real TLS 1.3 server, which splits
// its first flight over TWO legs — half on the ConnectResponse, the rest on a
// HandshakeContinueResponse the test releases explicitly — so the client sits in
// Handshaking for as long as the test wants.  Every non-handshake frame the gateway
// receives is checked: it must arrive after the broker side holds keys and open under
// them (sealed, not plaintext).
#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/CryptoFrame.h>
#include <rune/crypto/TlsSession.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;

namespace {

constexpr int  kPumpMax   = 6'000;
constexpr auto kPumpSleep = 2ms;
std::atomic<int> g_ctr{0};

std::string uniqueDir() {
    return "/tmp/tls_edge_" + std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(g_ctr.fetch_add(1, std::memory_order_relaxed));
}
std::string slurpFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

struct TwoLegTlsGateway {
    // Observations.
    int connectsSeen = 0, plaintextConnects = 0;
    int framesBeforeKeys = 0;     // a non-handshake frame before the broker side had keys
    int unopenable = 0;           // a non-handshake frame that does not open under the keys (plaintext)
    int subscribesSeen = 0, joinGroupsSeen = 0, metadataSeen = 0, initPidsSeen = 0;
    std::vector<uint16_t> earlyTids;   // the templates of framesBeforeKeys (diagnostics)

    // The withheld second leg.
    bool secondLegPending = false;
    bool release = false;
    int64_t legCorr = 0;
    std::vector<uint8_t> secondLeg;

    std::string serverPem, serverKey;
    std::optional<rune::crypto::TlsSession> srv;
    rune::crypto::Cipher cipher{};
    int64_t sid = 0;
    rcs::CryptoState broker;
    std::shared_ptr<aeron::Aeron> gwAeron;
    std::shared_ptr<aeron::Subscription> gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler> gwAsm;

    TwoLegTlsGateway(const std::string& aeronDir, int32_t in, int32_t eg) {
        serverPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.pem");
        serverKey = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.key");
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", in);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", eg);
        for (const auto dl = std::chrono::steady_clock::now() + 5s; std::chrono::steady_clock::now() < dl;) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            });
    }
    ~TwoLegTlsGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int limit = 16) {
        int w = gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), limit) : 0;
        if (secondLegPending && release) {
            secondLegPending = false;
            std::vector<uint8_t> resp;
            const std::size_t rn = rcc::encodeHandshakeContinueResponse(
                resp, 0, std::string_view(reinterpret_cast<const char*>(secondLeg.data()), secondLeg.size()), sid,
                legCorr);
            sendRaw(resp.data(), rn);
            ++w;
        }
        return w;
    }
    void sendRaw(const uint8_t* b, std::size_t n) {
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), static_cast<aeron::util::index_t>(n));
        for (int i = 0; i < 500 && gwPub->offer(ab, 0, static_cast<aeron::util::index_t>(n)) <= 0; ++i)
            std::this_thread::sleep_for(1ms);
    }
    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid) return;
        uint8_t out[16384];
        switch (static_cast<rcc::TemplateId>(*tid)) {
        case rcc::TemplateId::Connect: {
            auto c = rcc::decodeConnectRequest(buf, len);
            if (!c) return;
            ++connectsSeen;
            if (c->cipherList.empty() || c->handshakeBytes.empty()) { ++plaintextConnects; return; }
            sid = 40 + connectsSeen;
            srv.emplace(rune::crypto::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str()));
            cipher = static_cast<rune::crypto::Cipher>(c->cipherList[0]);
            broker = rcs::CryptoState{};
            const auto& hb = c->handshakeBytes;
            (void)srv->readInbound(reinterpret_cast<const uint8_t*>(hb.data()), hb.size());
            const std::size_t n = srv->writeOutbound(out, sizeof out);
            // Leg 1: only the first half of the server's flight — the client's TLS needs more.
            const std::size_t half = n / 2;
            secondLeg.assign(out + half, out + n);
            legCorr = c->correlationId;
            secondLegPending = true;
            uint8_t resp[16384 + 128];
            const std::size_t rn = rcc::encodeConnectResponse(
                resp, sizeof resp, 0, 0, sid, c->sessionId, c->correlationId, rune::protocol::client::kBrokerVersion,
                c->cipherList[0], std::string_view(reinterpret_cast<const char*>(out), half));
            sendRaw(resp, rn);
            return;
        }
        case rcc::TemplateId::HandshakeContinue: {
            auto hc = rcc::decodeHandshakeContinue(buf, len);
            if (!hc || !srv) return;
            (void)srv->readInbound(reinterpret_cast<const uint8_t*>(hc->handshakeBytes.data()),
                                   hc->handshakeBytes.size());
            const std::size_t n = srv->writeOutbound(out, sizeof out);
            if (srv->done() && !broker.encrypted) {
                const auto keys = srv->exportKeys(cipher, sid);
                broker.encrypted = true;
                broker.cipher    = keys.cipher;
                broker.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
                broker.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
            }
            if (n > 0) {
                std::vector<uint8_t> resp;
                const std::size_t rn = rcc::encodeHandshakeContinueResponse(
                    resp, 0, std::string_view(reinterpret_cast<const char*>(out), n), sid, hc->correlationId);
                sendRaw(resp.data(), rn);
            }
            return;
        }
        default: {
            if (!broker.encrypted) {
                ++framesBeforeKeys;
                earlyTids.push_back(*tid);
                return;
            }
            uint8_t opened[16384];
            const std::size_t on = rcc::openEgressFrame(broker, buf, len, opened, sizeof opened);
            if (on == 0) { ++unopenable; return; }
            const auto t = static_cast<rcc::TemplateId>(*tid);
            if (t == rcc::TemplateId::Subscribe) ++subscribesSeen;
            if (t == rcc::TemplateId::MetadataRequest) ++metadataSeen;
            if (*tid == static_cast<uint16_t>(rune::protocol::client::TemplateId::JoinGroup)) ++joinGroupsSeen;
            if (*tid == static_cast<uint16_t>(rune::protocol::client::TemplateId::InitProducerId)) ++initPidsSeen;
            return;   // nothing answered: the assertions are about what the client sends
        }
        }
    }
};

struct Rig {
    static constexpr int32_t kInStr = 4114, kEgStr = 4115;
    std::string agentDir = uniqueDir();
    std::unique_ptr<Client> client;
    std::unique_ptr<TwoLegTlsGateway> gw;
    Rig() {
        ClientConfig cfg;
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = kInStr;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = kEgStr;
        cfg.dataDir = agentDir; cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 8000;
        cfg.idleMode = IdleMode::LowLatency;
        cfg.offerCrypto = true; cfg.requireCrypto = true;
        cfg.tlsTrustPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/ca.pem");
        cfg.tlsExpectHost = "localhost";
        TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        std::this_thread::sleep_for(150ms);
        gw = std::make_unique<TwoLegTlsGateway>(agentDir + "/aeron", kInStr, kEgStr);
    }
    ~Rig() {
        client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
    void pumpFor(std::chrono::milliseconds d) {
        for (const auto dl = std::chrono::steady_clock::now() + d; std::chrono::steady_clock::now() < dl;) {
            gw->poll(16);
            std::this_thread::sleep_for(kPumpSleep);
        }
    }
    template <class Pred> bool pumpWithin(std::chrono::milliseconds d, Pred p) {
        for (const auto dl = std::chrono::steady_clock::now() + d; std::chrono::steady_clock::now() < dl;) {
            if (p()) return true;
            gw->poll(16);
            std::this_thread::sleep_for(kPumpSleep);
        }
        return p();
    }
    template <class Pred> bool pumpUntil(Pred p) {
        for (int i = 0; i < kPumpMax; ++i) {
            if (p()) return true;
            gw->poll(16);
            std::this_thread::sleep_for(kPumpSleep);
        }
        return p();
    }
};

}  // namespace

// connect() reports Ok only once the TLS handshake has completed — never at the
// ConnectResponse{OK} that leaves the session Handshaking.  RED (before): connect() is
// Ok while the second leg is still withheld (the client is not encrypted yet).
TEST(TlsConnectEdgeRegression, ConnectIsOkOnlyAfterTheHandshakeCompletesRegression) {
    Rig r;
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpUntil([&] { return r.gw->secondLegPending; })) << "the first leg never went out";
    r.pumpFor(400ms);
    EXPECT_FALSE(conn.ready()) << "connect() completed while the session was still Handshaking";
    EXPECT_NE(r.client->state(), rcs::SessionState::Connected);
    r.gw->release = true;
    ASSERT_TRUE(r.pumpUntil([&] { return conn.ready(); })) << "connect() never completed after the handshake";
    EXPECT_NO_THROW(conn.check());
    EXPECT_TRUE(r.pumpUntil([&] { return r.gw->broker.encrypted; })) << "the client's Finished never arrived";
    EXPECT_EQ(r.gw->plaintextConnects, 0);
}

// Under require_tls nothing but the handshake leaves the client before its keys exist,
// and every connect-time send happens on the Connected edge, sealed: the pending
// Subscribe, the group consumer's JoinGroup, the connect-time MetadataRequest.  RED
// (before): the JoinGroup goes out plaintext at ConnectResponse{OK} (mid-handshake),
// and the pending Subscribe re-sent there is dropped (never reaches the gateway).
TEST(TlsConnectEdgeRegression, NoFrameLeavesBeforeTheHandshakeAndConnectTimeSendsFollowItRegression) {
    Rig r;
    auto gc = r.client->subscribeGroup("g", {"inventory"});
    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;
    auto sub = r.client->subscribe(r.client->topic("inventory"), /*partition=*/0, opts, [](const Record&) {});
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpUntil([&] { return r.gw->secondLegPending; }));
    r.pumpFor(400ms);
    EXPECT_EQ(r.gw->framesBeforeKeys, 0) << "a frame left the client mid-handshake (first template "
                                         << (r.gw->earlyTids.empty() ? -1 : int(r.gw->earlyTids.front())) << ")";
    r.gw->release = true;
    ASSERT_TRUE(r.pumpUntil([&] { return conn.ready(); }));
    EXPECT_NO_THROW(conn.check());
    // On the Connected edge itself — not a later timer-driven retry (the subscribe /
    // group timers are seconds): bounded well below them.
    EXPECT_TRUE(r.pumpWithin(2s, [&] {
        return r.gw->subscribesSeen >= 1 && r.gw->joinGroupsSeen >= 1 && r.gw->metadataSeen >= 1;
    })) << "connect-time sends missing after the handshake: Subscribe " << r.gw->subscribesSeen << ", JoinGroup "
        << r.gw->joinGroupsSeen << ", Metadata " << r.gw->metadataSeen;
    EXPECT_EQ(r.gw->framesBeforeKeys, 0);
    EXPECT_EQ(r.gw->unopenable, 0) << "a plaintext (or wrongly sealed) frame under require_tls";
    EXPECT_EQ(r.gw->plaintextConnects, 0);
}

// The direct (non-ring) send paths — a transactional producer's InitProducerId through
// the coordinator send, like control ops and group requests — hold the session's id from
// ConnectResponse{OK} on, mid-handshake.  Under offerCrypto the agent's single egress
// choke point (sealForDestination_) refuses every non-handshake frame of a session that
// is not Connected; the sender's own retry carries it once the keys exist.  RED (no
// gate): InitProducerId leaves plaintext while the session is Handshaking.
TEST(TlsConnectEdgeRegression, DirectSendsWaitForTheHandshakeRegression) {
    Rig r;
    auto tp = r.client->transactionalProducer("txn-a");
    test_support::TestCq cq(*r.client);
    auto init = cq.issue([&](Completions& q, Token t) { return tp.initTransactions(q, t); });
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpUntil([&] { return r.gw->secondLegPending; }));
    r.pumpFor(400ms);
    EXPECT_EQ(r.gw->framesBeforeKeys, 0) << "a frame left the client mid-handshake (first template "
                                         << (r.gw->earlyTids.empty() ? -1 : int(r.gw->earlyTids.front())) << ")";
    r.gw->release = true;
    ASSERT_TRUE(r.pumpUntil([&] { return conn.ready(); }));
    EXPECT_TRUE(r.pumpWithin(2s, [&] { return r.gw->initPidsSeen >= 1; })) << "InitProducerId never followed the handshake";
    EXPECT_EQ(r.gw->framesBeforeKeys, 0);
    EXPECT_EQ(r.gw->unopenable, 0);
}
