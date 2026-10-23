// tests/client/RedirectTest.cpp
//
// CB-5 B — Redirect-follow test: data-op redirect from follower to leader.
//
// Scenario:
//   A single fake gateway acts as both "follower" and "leader" on the same
//   aeron:ipc stream pair.  The client sends a Produce.  The gateway (acting
//   as follower) replies ErrorResponse{Redirect, "aeron:ipc"} (same channel).
//   The client must:
//     1. Retarget transport to the new channel (aeron:ipc → same, but the
//        retarget logic exercises the full path including pub swap + resolve).
//     2. Submit a ConnectRequest to re-establish the session on the "leader".
//     3. Resend the pending Produce using the stored wireFrame.
//   The gateway (now acting as leader) answers the ConnectRequest with OK and
//   the second Produce with ProduceResponse{OK, baseOffset=200}.
//   The original produce future must resolve with baseOffset >= 200.
//
// Client.cpp redirect handling now accepts full Aeron channel URIs (strings
// starting with "aeron:") as-is so that ipc-based tests don't have UDP URIs
// forced onto them.

#include "../support/TestCompletions.h"
#include "../support/PlainInitPid.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/codec/CryptoFrame.h>
#include <rune/crypto/TlsSession.h>
#include <fstream>
#include <optional>
#include <sstream>

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
#include <memory>
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

static constexpr int  kPumpMax   = 6'000;
static constexpr auto kPumpSleep = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_rdirCtr{0};

std::string uniqueDir(const char* prefix = "cb5b_test") {
    const int c = g_rdirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string(std::filesystem::temp_directory_path().string() + "/") + prefix + "_" +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// RedirectGateway — single gateway that acts as follower first, then leader.
//
// State machine:
//   ConnectRequest (any)  → ConnectResponse{OK}
//   Produce #1            → ErrorResponse{Redirect, "aeron:ipc"}
//   Produce #2 (resend)   → ProduceResponse{OK, baseOffset=200}
// ---------------------------------------------------------------------------

struct RedirectGateway {
    // Observed counters.
    int producesSeen  = 0;
    int connectsSeen  = 0;
    int subscribesSeen = 0;
    int64_t lastBaseOffset = -1;

    // CB-5 B: assign a DIFFERENT sessionId on the post-redirect reconnect so the
    // test can prove the resent Produce/Subscribe carries the NEW sessionId
    // (re-stamped) rather than the stale first-session id.
    static constexpr int64_t kFirstSessionId  = 42;
    static constexpr int64_t kLeaderSessionId = 99;
    int64_t lastConnectAssignedSessionId = 0;

    // sessionId observed on the LAST Produce / Subscribe the gateway received.
    int64_t lastProduceSessionId   = -1;
    int64_t lastSubscribeSessionId = -1;

    static constexpr int64_t kBaseOffset = 200;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;

    uint8_t replyBuf[2048]{};

    explicit RedirectGateway(const std::string& aeronDir,
                              int32_t ingressStream,
                              int32_t egressStream)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron)
            throw std::runtime_error("RedirectGateway: Aeron::connect failed");

        const int64_t subId = gwAeron->addSubscription(
            "aeron:ipc", ingressStream);
        const int64_t pubId = gwAeron->addExclusivePublication(
            "aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("RedirectGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(
                    reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                    static_cast<std::size_t>(len));
            });
    }

    ~RedirectGateway() {
        gwAsm.reset();
        gwPub.reset();
        gwSub.reset();
        gwAeron.reset();
    }

    int poll(int limit = 16) {
        if (!gwSub || !gwAsm) return 0;
        return gwSub->poll(gwAsm->handler(), limit);
    }

    bool gwSend(const uint8_t* buf, std::size_t len) {
        if (!gwPub) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(buf), ilen);
        for (int i = 0; i < 500; ++i) {
            if (gwPub->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        { uint8_t ipr[64]; if (const std::size_t ipn = test_support::plainInitPidReply(buf, len, 9001, ipr); ipn > 0) { gwSend(ipr, ipn); return; } }  // FD4: broker-assigned id
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            ++connectsSeen;

            // Assign the first session on the initial connect and a DIFFERENT
            // session on the post-redirect reconnect (proves re-stamp).
            const int64_t assigned =
                (connectsSeen == 1) ? kFirstSessionId : kLeaderSessionId;
            lastConnectAssignedSessionId = assigned;

            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/assigned,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::Subscribe) {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;
            ++subscribesSeen;
            lastSubscribeSessionId = opt->sessionId;

            if (subscribesSeen == 1) {
                // First Subscribe: reply ErrorResponse{Redirect}.
                const std::string_view endpoint = "aeron:ipc";
                const std::size_t n = rcc::encodeErrorResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::Redirect),
                    endpoint,
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) gwSend(replyBuf, n);
            } else {
                // Resend (post-redirect): reply SubscribeResponse{OK}.
                const std::size_t n = rcc::encodeSubscribeResponse(
                    replyBuf, sizeof(replyBuf),
                    /*errorCode=*/0,
                    /*partition=*/opt->partition,
                    /*subscriptionId=*/7,
                    opt->sessionId,
                    opt->correlationId);
                if (n > 0) gwSend(replyBuf, n);
            }
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;

        const auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;

        ++producesSeen;
        lastProduceSessionId = opt->sessionId;

        if (producesSeen == 1) {
            // First Produce: reply ErrorResponse{Redirect}.
            // Endpoint is a full Aeron URI so Client.cpp accepts it without
            // prepending "aeron:udp?endpoint=" (see the rfind("aeron:",0) guard).
            const std::string_view endpoint = "aeron:ipc";
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::Redirect),
                endpoint,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
        } else {
            // Second Produce (post-redirect resend): reply ProduceResponse{OK}.
            lastBaseOffset = kBaseOffset;

            const std::size_t n = rcc::encodeProduceResponse(
                replyBuf, sizeof(replyBuf),
                /*errorCode=*/0,
                opt->partition,
                lastBaseOffset,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
        }
    }
};

// ===========================================================================
// Test: ProduceRedirectFollow
//
// Client sends a Produce.  The gateway (acting as follower) replies with
// ErrorResponse{Redirect, "aeron:ipc"}.  The client must retarget + resend.
// The gateway (now acting as leader) responds with ProduceResponse{OK}.
// The produce future must resolve with baseOffset = 200.
// ===========================================================================
TEST(Redirect, ProduceRedirectFollow) {
    static constexpr int32_t kInStr = 4000, kEgStr = 4001;

    const std::string agentDir = uniqueDir();
    const std::string aeronDir = agentDir + "/aeron";

    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = kInStr;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = kEgStr;
    cfg.dataDir          = agentDir;
    cfg.connectTimeoutMs = 8000;
    cfg.pingTimeoutMs    = 8000;
    // LowLatency: YieldingIdleStrategy → fast redirect round-trip.
    cfg.idleMode         = IdleMode::LowLatency;

    // Static topic map.
    TopicSpec ts;
    ts.name           = "inventory";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);

    // Give the embedded driver time to initialise before the gateway attaches.
    std::this_thread::sleep_for(150ms);

    auto gw = std::make_unique<RedirectGateway>(aeronDir, kInStr, kEgStr);

    // Connect.
    test_support::TestCq connFutCq(*client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    bool connected = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (connFut.ready()) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(connected) << "Failed to connect to redirect gateway";
    ASSERT_NO_THROW(connFut.check()) << "connect() must not throw";

    // Create a producer with minimal linger so the Produce fires quickly.
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 12'000; // 12 s — accommodate redirect round-trip

    auto p  = client->producer(pcfg);

    test_support::TestCq cqP(*client);
    auto th = client->topic("inventory");
    ASSERT_TRUE(th.valid()) << "topic 'inventory' must be valid";

    Key key(uint64_t{42});
    const std::vector<uint8_t> val = {0xDE, 0xAD, 0xBE, 0xEF};
    auto fut = cqP.send(p, th, key, std::span<const uint8_t>(val));

    // Pump gateway until the future resolves.
    // Flow:
    //   1. Client sends Produce      → gateway replies Redirect
    //   2. Client retargets (ipc → ipc same channel, pub swap)
    //   3. Client submits ConnectRequest → gateway replies ConnectResponse{OK}
    //   4. Client resends Produce    → gateway replies ProduceResponse{OK, 200}
    //   5. Client resolves future
    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (fut.ready()) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done)
        << "Produce future must resolve after redirect-follow (gateway saw "
        << gw->producesSeen << " Produce(s), "
        << gw->connectsSeen << " Connect(s))";

    int64_t baseOffset = -1;
    ASSERT_NO_THROW({ baseOffset = fut.offset(); })
        << "Produce future must not throw after redirect-follow";

    EXPECT_GE(baseOffset, RedirectGateway::kBaseOffset)
        << "baseOffset must match gateway-assigned value";

    // Gateway must have seen exactly 2 Produces:
    //   #1 → Redirect reply, #2 → ProduceResponse OK.
    EXPECT_EQ(gw->producesSeen, 2)
        << "Gateway must see 2 Produces (1 redirected + 1 resend)";

    // CB-5 B bug-2 fix: the resent Produce must carry the LEADER's freshly
    // assigned sessionId (re-stamped), NOT the stale first-session id.  The
    // reconnect assigned kLeaderSessionId; the resend must use it.
    EXPECT_EQ(gw->lastProduceSessionId, RedirectGateway::kLeaderSessionId)
        << "Resent Produce must carry the NEW (leader) sessionId after redirect";

    // CB-5 B bug-4: exactly ONE reconnect (2 connects total: initial + redirect).
    EXPECT_EQ(gw->connectsSeen, 2)
        << "Exactly one reconnect after redirect (no session churn)";

    client->close();

    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// Fix round 2 (C-1 / M-1): the redirect reconnect's Connected edge re-offers the
// redirected batch ONCE (resendAllRedirected) — the seed's generic Connected-edge
// re-offer must not send it a second time.  RED (the edge not marked handled by
// the redirect resend): the gateway sees 3 Produces.
TEST(Redirect, RedirectReconnectDoesNotDoubleReOfferRegression) {
    static constexpr int32_t kInStr = 4012, kEgStr = 4013;
    const std::string agentDir = uniqueDir();
    const std::string aeronDir = agentDir + "/aeron";
    ClientConfig cfg;
    cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = kInStr;
    cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = kEgStr;
    cfg.dataDir = agentDir; cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 8000;
    cfg.idleMode = IdleMode::LowLatency;
    TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);
    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    auto gw = std::make_unique<RedirectGateway>(aeronDir, kInStr, kEgStr);
    test_support::TestCq connCq(*client);
    auto conn = connCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    for (int i = 0; i < kPumpMax && !conn.ready(); ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }
    ASSERT_NO_THROW(conn.check());
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1; pcfg.maxInFlight = 4;
    pcfg.produceTimeoutMs = 12'000;
    auto p = client->producer(pcfg);
    test_support::TestCq cqP(*client);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto fut = cqP.send(p, client->topic("inventory"), Key(uint64_t{42}), std::span<const uint8_t>(val));
    for (int i = 0; i < kPumpMax && !fut.ready(); ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }
    ASSERT_TRUE(fut.ready());
    for (int i = 0; i < 100; ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }   // any late extra re-offer
    EXPECT_EQ(gw->producesSeen, 2) << "1 redirected + exactly 1 re-offer on the new session";
    EXPECT_EQ(gw->lastProduceSessionId, RedirectGateway::kLeaderSessionId);
    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ---------------------------------------------------------------------------
// Fix round 3 (N-M1): the redirect follow with TLS ON.  The gateway completes a real
// TLS 1.3 handshake on every Connect (ConnectResponse{OK, cipher, flight} then
// HandshakeContinue legs) and seals every reply under the session's keys.  The
// redirect reconnect's ConnectResponse{OK} then arrives while the seed is still
// HANDSHAKING: the in-flight batch must be re-offered exactly ONCE (on the seed's
// Connected edge), never twice.
// ---------------------------------------------------------------------------
namespace {
std::string slurpFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}
struct TlsRedirectGateway {
    int producesSeen = 0, connectsSeen = 0;
    int64_t lastProduceSessionId = -1;
    std::string serverPem, serverKey;
    std::optional<rune::crypto::TlsSession> srv;
    rune::crypto::Cipher cipher{};
    int64_t sid = 0;
    rcs::CryptoState broker;
    std::shared_ptr<aeron::Aeron> gwAeron;
    std::shared_ptr<aeron::Subscription> gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler> gwAsm;

    TlsRedirectGateway(const std::string& aeronDir, int32_t in, int32_t eg) {
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
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            });
    }
    ~TlsRedirectGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }
    int poll(int limit = 16) { return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), limit) : 0; }
    void sendRaw(const uint8_t* b, std::size_t n) {
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), static_cast<aeron::util::index_t>(n));
        for (int i = 0; i < 500 && gwPub->offer(ab, 0, static_cast<aeron::util::index_t>(n)) <= 0; ++i)
            std::this_thread::sleep_for(1ms);
    }
    void sendSealed(const uint8_t* plain, std::size_t n) {
        uint8_t sealed[1024];
        const std::size_t sn = rcc::sealFrame(broker, plain, n, sealed, sizeof sealed);
        if (sn > 0) sendRaw(sealed, sn);
    }
    void onFrame(const uint8_t* buf, std::size_t len) {
        { uint8_t ipr[64]; if (const std::size_t ipn = test_support::plainInitPidReply(buf, len, 9001, ipr, /*headerOnly=*/true); ipn > 0) { sendSealed(ipr, ipn); return; } }  // FD4: broker-assigned id
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid) return;
        uint8_t out[16384];
        switch (static_cast<rcc::TemplateId>(*tid)) {
        case rcc::TemplateId::Connect: {
            auto c = rcc::decodeConnectRequest(buf, len);
            if (!c || c->cipherList.empty() || c->handshakeBytes.empty()) return;
            ++connectsSeen;
            sid = connectsSeen == 1 ? 42 : 99;
            srv.emplace(rune::crypto::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str()));
            cipher = static_cast<rune::crypto::Cipher>(c->cipherList[0]);
            broker = rcs::CryptoState{};
            const auto& hb = c->handshakeBytes;
            (void)srv->readInbound(reinterpret_cast<const uint8_t*>(hb.data()), hb.size());
            const std::size_t n = srv->writeOutbound(out, sizeof out);
            uint8_t resp[16384 + 128];
            const std::size_t rn = rcc::encodeConnectResponse(resp, sizeof resp, 0, 0, sid, c->sessionId,
                                                              c->correlationId, rune::protocol::client::kBrokerVersion,
                                                              c->cipherList[0],
                                                              std::string_view(reinterpret_cast<const char*>(out), n));
            sendRaw(resp, rn);
            return;
        }
        case rcc::TemplateId::HandshakeContinue: {
            auto hc = rcc::decodeHandshakeContinue(buf, len);
            if (!hc || !srv) return;
            (void)srv->readInbound(reinterpret_cast<const uint8_t*>(hc->handshakeBytes.data()), hc->handshakeBytes.size());
            const std::size_t n = srv->writeOutbound(out, sizeof out);
            std::vector<uint8_t> resp;
            const std::size_t rn = rcc::encodeHandshakeContinueResponse(
                resp, 0, std::string_view(reinterpret_cast<const char*>(out), n), sid, hc->correlationId);
            if (srv->done() && !broker.encrypted) {
                const auto keys = srv->exportKeys(cipher, sid);
                broker.encrypted = true;
                broker.cipher    = keys.cipher;
                broker.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
                broker.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
            }
            sendRaw(resp.data(), rn);
            return;
        }
        case rcc::TemplateId::Produce: {
            ++producesSeen;
            lastProduceSessionId = static_cast<int64_t>(rune::protocol::client::detail::getLE64(buf, 8));
            const int64_t corr   = static_cast<int64_t>(rune::protocol::client::detail::getLE64(buf, 16));
            uint8_t plain[512];
            std::size_t pn = 0;
            if (producesSeen == 1) {
                pn = rcc::encodeErrorResponse(plain, sizeof plain,
                                              static_cast<int16_t>(rcc::ErrorCode::Redirect), "aeron:ipc",
                                              lastProduceSessionId, corr);
            } else {
                pn = rcc::encodeProduceResponse(plain, sizeof plain, 0, 0, 200, lastProduceSessionId, corr);
            }
            if (pn > 0) sendSealed(plain, pn);
            return;
        }
        default:
            return;   // keepalives / metadata: not answered (the test is short)
        }
    }
};
}  // namespace

// N-M1: RED (round 2's marking: connectedEpoch(0) read while Handshaking = 0): the
// redirected batch is re-offered twice (3 Produces).
TEST(Redirect, TlsRedirectReconnectDoesNotDoubleReOfferRegression) {
    static constexpr int32_t kInStr = 4014, kEgStr = 4015;
    const std::string agentDir = uniqueDir();
    const std::string aeronDir = agentDir + "/aeron";
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
    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    auto gw = std::make_unique<TlsRedirectGateway>(aeronDir, kInStr, kEgStr);
    test_support::TestCq connCq(*client);
    auto conn = connCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    for (int i = 0; i < kPumpMax && !conn.ready(); ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }
    ASSERT_NO_THROW(conn.check()) << "precondition: an encrypted session";
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1; pcfg.maxInFlight = 4;
    pcfg.produceTimeoutMs = 12'000;
    auto p = client->producer(pcfg);
    test_support::TestCq cqP(*client);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto fut = cqP.send(p, client->topic("inventory"), Key(uint64_t{42}), std::span<const uint8_t>(val));
    for (int i = 0; i < kPumpMax && !fut.ready(); ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }
    ASSERT_TRUE(fut.ready()) << "the redirected batch never completed (" << gw->producesSeen << " Produces, "
                             << gw->connectsSeen << " Connects)";
    EXPECT_TRUE(fut.ok()) << fut.describe();
    for (int i = 0; i < 100; ++i) { gw->poll(16); std::this_thread::sleep_for(kPumpSleep); }
    EXPECT_EQ(gw->connectsSeen, 2) << "one reconnect after the redirect, a fresh TLS handshake";
    EXPECT_EQ(gw->producesSeen, 2) << "1 redirected + exactly 1 re-offer on the new (encrypted) session";
    EXPECT_EQ(gw->lastProduceSessionId, 99);
    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// Test: SubscribeRedirectFollow (CB-5 B bug-1 + bug-2)
//
// Client sends a Subscribe.  The gateway (follower) replies Redirect.  The
// client must retarget + reconnect + RESEND the Subscribe (bug-1: subscribe
// redirect was previously dropped) re-stamped with the leader's new sessionId
// (bug-2).  The gateway (leader) then answers SubscribeResponse{OK}.  We prove
// the resend happened AND carried the new sessionId.
// ===========================================================================
TEST(Redirect, SubscribeRedirectFollow) {
    static constexpr int32_t kInStr = 4100, kEgStr = 4101;

    const std::string agentDir = uniqueDir("cb5b_sub");
    const std::string aeronDir = agentDir + "/aeron";

    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = kInStr;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = kEgStr;
    cfg.dataDir          = agentDir;
    cfg.connectTimeoutMs = 8000;
    cfg.pingTimeoutMs    = 8000;
    cfg.idleMode         = IdleMode::LowLatency;

    TopicSpec ts;
    ts.name           = "inventory";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);

    auto gw = std::make_unique<RedirectGateway>(aeronDir, kInStr, kEgStr);

    // Connect.
    test_support::TestCq connFutCq(*client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    bool connected = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (connFut.ready()) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(connected) << "Failed to connect to redirect gateway";
    ASSERT_NO_THROW(connFut.check());

    auto th = client->topic("inventory");
    ASSERT_TRUE(th.valid());

    std::atomic<bool> bound{false};
    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;

    auto sub = client->subscribe(th, /*partition=*/0, opts,
                                 [](const Record&) { /* no records in this test */ });

    // Pump until the subscription is bound (subscriptionId != 0), which only
    // happens after the SubscribeResponse — i.e. after the redirect+resend.
    bool subBound = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (sub.subscriptionId() != 0) { subBound = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(subBound)
        << "Subscription must bind after redirect-follow (gateway saw "
        << gw->subscribesSeen << " Subscribe(s), "
        << gw->connectsSeen << " Connect(s))";

    // bug-1: the Subscribe must have been RESENT (2 seen: redirect + resend).
    EXPECT_EQ(gw->subscribesSeen, 2)
        << "Gateway must see 2 Subscribes (1 redirected + 1 resend)";

    // bug-2: the resent Subscribe must carry the NEW leader sessionId.
    EXPECT_EQ(gw->lastSubscribeSessionId, RedirectGateway::kLeaderSessionId)
        << "Resent Subscribe must carry the NEW (leader) sessionId after redirect";

    // bug-4: exactly one reconnect.
    EXPECT_EQ(gw->connectsSeen, 2)
        << "Exactly one reconnect after subscribe redirect (no churn)";

    sub.unsubscribe();
    client->close();

    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// Test: M1 — Produce + Subscribe submitted DURING the redirect-reconnect window
// are gated (not emitted with sessionId=0) and flushed with the new sessionId
// once ConnectResponse{OK} arrives.
//
// Scenario:
//   The gateway triggers a redirect on the FIRST Produce.  While the client is
//   in Connecting state (between the Redirect and the second ConnectResponse),
//   the test submits a SECOND Produce and a Subscribe.  These must NOT be sent
//   with sessionId=0; they must be held and then emitted with the leader's new
//   sessionId once Connected.
//
// Gateway state machine:
//   ConnectRequest #1 → ConnectResponse{OK, session=42}
//   Produce #1        → ErrorResponse{Redirect, "aeron:ipc"}
//   ConnectRequest #2 → ConnectResponse{OK, session=99}  [after delay]
//   Produce #2 (resend of #1)  → ProduceResponse{OK}
//   Produce #3 (new during reconnect) → ProduceResponse{OK}
//   Subscribe (new during reconnect)  → SubscribeResponse{OK}
//
// Assertions:
//   - Produce #3 session id == kLeaderSessionId (99), not 0
//   - Subscribe session id  == kLeaderSessionId (99), not 0
//   - produce future #2 and #3 both resolve
//   - subscription binds
// ===========================================================================

struct M1Gateway {
    // State counters
    std::atomic<int> connectsSeen{0};
    std::atomic<int> producesSeen{0};
    std::atomic<int> subscribesSeen{0};

    static constexpr int64_t kFirstSession  = 42;
    static constexpr int64_t kLeaderSession = 99;

    // sessionId observed on each produce / subscribe
    std::atomic<int64_t> lastProduceSession{-1};
    std::atomic<int64_t> lastSubSession{-1};

    // track corrIds so we can reply correctly
    int64_t produce1CorrId = 0;
    int64_t produce2CorrId = 0; // resend of #1 — same corrId
    int64_t produce3CorrId = 0; // new produce during reconnect
    int64_t subCorrId = 0;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;
    uint8_t replyBuf[2048]{};

    M1Gateway(const std::string& aeronDir, int32_t inStr, int32_t egStr)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(1ms);
        }
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer()+off),
                        static_cast<std::size_t>(len));
            });
    }
    ~M1Gateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int lim=16) { return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), lim) : 0; }

    bool gwSend(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 500; ++i) {
            if (gwPub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        { uint8_t ipr[64]; if (const std::size_t ipn = test_support::plainInitPidReply(buf, len, 9001, ipr); ipn > 0) { gwSend(ipr, ipn); return; } }  // FD4: broker-assigned id
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            const int c = connectsSeen.fetch_add(1) + 1;
            const int64_t assigned = (c == 1) ? kFirstSession : kLeaderSession;
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK), 0, assigned,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::Subscribe) {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;
            subscribesSeen.fetch_add(1);
            lastSubSession.store(opt->sessionId, std::memory_order_release);
            subCorrId = opt->correlationId;
            // Reply SubscribeResponse{OK}
            const std::size_t n = rcc::encodeSubscribeResponse(
                replyBuf, sizeof(replyBuf),
                0, opt->partition, /*subscriptionId=*/55,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;
        const auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;
        const int p = producesSeen.fetch_add(1) + 1;
        lastProduceSession.store(opt->sessionId, std::memory_order_release);

        if (p == 1) {
            // First produce: send Redirect
            produce1CorrId = opt->correlationId;
            const std::string_view ep = "aeron:ipc";
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::Redirect),
                ep, opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
        } else {
            // All subsequent produces (resend of #1 + new ones): reply OK
            const std::size_t n = rcc::encodeProduceResponse(
                replyBuf, sizeof(replyBuf),
                0, opt->partition, /*baseOffset=*/300,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
        }
    }
};

TEST(Redirect, M1SendsDuringReconnectWindowHeldNotSessionId0) {
    static constexpr int32_t kInStr = 4200, kEgStr = 4201;
    const std::string agentDir = uniqueDir("cb5_m1");
    const std::string aeronDir = agentDir + "/aeron";

    ClientConfig cfg;
    cfg.gatewayChannel    = "aeron:ipc";
    cfg.gatewayStreamId   = kInStr;
    cfg.egressChannel     = "aeron:ipc";
    cfg.egressStreamId    = kEgStr;
    cfg.dataDir           = agentDir;
    cfg.connectTimeoutMs  = 8000;
    cfg.idleMode          = IdleMode::LowLatency;
    cfg.subscribeTimeoutMs = 15000;

    TopicSpec ts;
    ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    auto gw = std::make_unique<M1Gateway>(aeronDir, kInStr, kEgStr);

    // Connect
    test_support::TestCq connFutCq(*client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    for (int i = 0; i < kPumpMax && !connFut.ready(); ++i) {
        gw->poll(16);
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_NO_THROW(connFut.check()) << "Initial connect must succeed";

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 8; pcfg.produceTimeoutMs = 15000;
    auto p = client->producer(pcfg);
    test_support::TestCq cqP(*client);
    auto th = client->topic("t");

    Key key(uint64_t{1});
    const std::vector<uint8_t> val = {0xAA};

    // Send first produce (will be redirected by gateway)
    auto fut1 = cqP.send(p, th, key, std::span<const uint8_t>(val));

    // Pump until gateway sends the Redirect (produce #1 seen) AND the client
    // has entered Connecting state (i.e. sessionId() == 0).
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (gw->producesSeen.load() >= 1 &&
            client->state() == rcs::SessionState::Connecting)
        {
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    // Confirm we caught the reconnect window
    ASSERT_EQ(client->state(), rcs::SessionState::Connecting)
        << "Client must be Connecting (mid-redirect) before new sends";

    // --- Submit new Produce and Subscribe DURING the reconnect window ---
    // These must NOT go out with sessionId=0; they must be held.
    auto fut2 = cqP.send(p, th, key, std::span<const uint8_t>(val));
    SubscribeOptions opts;
    opts.from = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;
    auto sub = client->subscribe(th, 0, opts, [](const Record&){});

    // Pump until fut1 AND fut2 resolve and subscription is bound
    bool fut1Done = false, fut2Done = false, subBound = false;
    for (int i = 0; i < kPumpMax * 2; ++i) {
        gw->poll(16);
        if (!fut1Done && fut1.ready()) fut1Done = true;
        if (!fut2Done && fut2.ready()) fut2Done = true;
        if (!subBound && sub.subscriptionId() != 0) subBound = true;
        if (fut1Done && fut2Done && subBound) break;
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fut1Done) << "Redirected produce future must resolve";
    ASSERT_TRUE(fut2Done) << "New-during-reconnect produce future must resolve";
    ASSERT_TRUE(subBound) << "New-during-reconnect subscribe must bind";

    ASSERT_NO_THROW({ (void)fut1.offset(); }) << "fut1 must not throw";
    ASSERT_NO_THROW({ (void)fut2.offset(); }) << "fut2 must not throw";

    // Key M1 assertion: no frame was emitted with sessionId==0
    // The gateway would have seen a Produce with sessionId=0 if the guard
    // was missing; instead all produces after the redirect use the leader session.
    EXPECT_EQ(gw->lastProduceSession.load(), M1Gateway::kLeaderSession)
        << "All produces after redirect must carry the NEW leader sessionId (not 0)";
    EXPECT_EQ(gw->lastSubSession.load(), M1Gateway::kLeaderSession)
        << "Subscribe submitted during reconnect must carry the NEW leader sessionId";

    sub.unsubscribe();
    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// Test: M2 — Redirect to a dead/unresponsive endpoint fails produce promptly
// AND subscribe promptly (no hang).  Both must resolve in well under
// produceTimeoutMs/subscribeTimeoutMs.
//
// Scenario:
//   Gateway answers ConnectRequest → ConnectResponse{OK}.
//   Client sends a Produce and a Subscribe.
//   Gateway replies ErrorResponse{Redirect, "aeron:ipc"} (same channel but
//   the gateway then IGNORES subsequent ConnectRequests, simulating a dead
//   endpoint that the retarget succeeds to but nobody answers).
//   After connectTimeoutMs * (maxRetries+1), the session transitions to
//   Disconnected.  sweepTimeouts() detects redirectPending_+Disconnected and
//   promptly fails both the pending produce future and the pending subscribe.
//
// Assertions:
//   - produce future fails within kDeadlineMs (< produceTimeoutMs full value)
//   - subscription is deactivated (active=false) within kDeadlineMs
//   - no hang
// ===========================================================================

struct DeadGateway {
    std::atomic<int> connectsSeen{0};
    std::atomic<int> producesSeen{0};
    std::atomic<int> subscribesSeen{0};

    static constexpr int64_t kSessionId = 77;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;
    uint8_t replyBuf[2048]{};

    DeadGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(1ms);
        }
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer()+off),
                        static_cast<std::size_t>(len));
            });
    }
    ~DeadGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int lim=16) { return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), lim) : 0; }

    bool gwSend(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 200; ++i) {
            if (gwPub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        { uint8_t ipr[64]; if (const std::size_t ipn = test_support::plainInitPidReply(buf, len, 9001, ipr); ipn > 0) { gwSend(ipr, ipn); return; } }  // FD4: broker-assigned id
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            connectsSeen.fetch_add(1);
            // Only answer the FIRST ConnectRequest with OK.
            // Subsequent ConnectRequests (the reconnect after redirect) get
            // NO response — simulating a dead / unreachable endpoint.
            if (connectsSeen.load() == 1) {
                const std::size_t n = rcc::encodeConnectResponse(
                    replyBuf, sizeof(replyBuf),
                    static_cast<int16_t>(rcc::ErrorCode::OK), 0, kSessionId,
                    opt->sessionId, opt->correlationId);
                if (n > 0) gwSend(replyBuf, n);
            }
            // Subsequent connects: drop silently (dead endpoint simulation).
            return;
        }

        if (tid == rcc::TemplateId::Subscribe) {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;
            subscribesSeen.fetch_add(1);
            // Reply Redirect to the same aeron:ipc endpoint (which will then
            // drop the reconnect — simulating dead endpoint via the no-response path).
            const std::string_view dead = "aeron:ipc";
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::Redirect),
                dead, opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::Produce) {
            const auto opt = rcc::decodeProduce(buf, len);
            if (!opt) return;
            producesSeen.fetch_add(1);
            // Reply Redirect to the same aeron:ipc endpoint (dead — no response).
            const std::string_view dead = "aeron:ipc";
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::Redirect),
                dead, opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
        }
    }
};

TEST(Redirect, M2DeadEndpointProduceFutureFails_SubscribeDoesNotHang) {
    static constexpr int32_t kInStr = 4300, kEgStr = 4301;
    const std::string agentDir = uniqueDir("cb5_m2dead");
    const std::string aeronDir = agentDir + "/aeron";

    // Use very short connect timeout so the dead reconnect fails quickly.
    // The sweep detects Disconnected+redirectPending_ and fails everything.
    ClientConfig cfg;
    cfg.gatewayChannel    = "aeron:ipc";
    cfg.gatewayStreamId   = kInStr;
    cfg.egressChannel     = "aeron:ipc";
    cfg.egressStreamId    = kEgStr;
    cfg.dataDir           = agentDir;
    // Very short timeouts so the dead reconnect fails fast in the test.
    cfg.connectTimeoutMs  = 300;  // 300 ms connect timeout (agent-level)
    cfg.produceTimeoutMs  = 30000; // long enough not to fire before our prompt-fail
    cfg.subscribeTimeoutMs = 30000;
    cfg.idleMode          = IdleMode::LowLatency;

    TopicSpec ts;
    ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    auto gw = std::make_unique<DeadGateway>(aeronDir, kInStr, kEgStr);

    // Connect (first ConnectRequest → OK)
    test_support::TestCq connFutCq(*client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    for (int i = 0; i < kPumpMax && !connFut.ready(); ++i) {
        gw->poll(16);
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_NO_THROW(connFut.check()) << "Initial connect must succeed";

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 30000;
    auto p = client->producer(pcfg);
    test_support::TestCq cqP(*client);
    auto th = client->topic("t");

    Key key(uint64_t{1});
    const std::vector<uint8_t> val = {0xBB};

    // Submit a Produce and a Subscribe — both will get Redirect to dead endpoint
    auto prodFut = cqP.send(p, th, key, std::span<const uint8_t>(val));
    SubscribeOptions opts;
    opts.from = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;
    auto sub = client->subscribe(th, 0, opts, [](const Record&){});

    // Record start time; both must fail in well under kDeadlineMs.
    // The deadline is: connectTimeoutMs * (maxConnectRetries+1) + sweep interval.
    // With connectTimeoutMs=300ms, maxConnectRetries=3: ≤ 300*4 + 200 = 1.4 s.
    // We wait up to 8 s (allowing for slow CI) but assert elapsed < 8 s strictly.
    const auto startTime = std::chrono::steady_clock::now();
    constexpr auto kDeadlineMs = std::chrono::milliseconds(8000);

    bool prodFailed = false, subDeactivated = false;
    for (int i = 0; i < kPumpMax * 4; ++i) {
        gw->poll(16);
        if (!prodFailed && prodFut.ready())
            prodFailed = true;
        if (!subDeactivated && !sub.active())
            subDeactivated = true;
        if (prodFailed && subDeactivated) break;
        std::this_thread::sleep_for(kPumpSleep);
        if (std::chrono::steady_clock::now() - startTime > kDeadlineMs) break;
    }

    const auto elapsed = std::chrono::steady_clock::now() - startTime;

    ASSERT_TRUE(prodFailed)
        << "Produce future must fail (not hang) on dead-endpoint redirect";
    ASSERT_TRUE(subDeactivated)
        << "Subscribe must be deactivated (not hang forever) on dead-endpoint redirect";

    // Verify it failed promptly (under kDeadlineMs)
    EXPECT_LT(elapsed, kDeadlineMs)
        << "Dead-endpoint redirect must fail PROMPTLY (not wait for full produce timeout)";

    // Verify produce future threw (not resolved with a value)
    bool prodThrew = false;
    try { (void)prodFut.offset(); } catch (...) { prodThrew = true; }
    EXPECT_TRUE(prodThrew) << "Produce future must throw on dead-endpoint redirect";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// Test: M2b — >kMaxRedirects (3) redirect episodes fail cleanly.
//
// Scenario:
//   Every Produce the gateway sees is answered with ErrorResponse{Redirect, ipc}.
//   Since the redirect target is "aeron:ipc" (same), the client reconnects each
//   time.  After kMaxRedirects redirects, the per-frame redirect counter is
//   exhausted and the produce future must fail (not loop infinitely).
//
// Note: kMaxRedirects caps per-frame redirect attempts in resendAllRedirected
// (ProducerImpl.cpp).  The test drives 4 redirect episodes (> 3) and verifies
// the future fails.
// ===========================================================================

struct MaxRedirectGateway {
    std::atomic<int> connectsSeen{0};
    std::atomic<int> producesSeen{0};

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;
    uint8_t replyBuf[2048]{};

    MaxRedirectGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(1ms);
        }
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer()+off),
                        static_cast<std::size_t>(len));
            });
    }
    ~MaxRedirectGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int lim=16) { return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), lim) : 0; }

    bool gwSend(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 300; ++i) {
            if (gwPub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        { uint8_t ipr[64]; if (const std::size_t ipn = test_support::plainInitPidReply(buf, len, 9001, ipr); ipn > 0) { gwSend(ipr, ipn); return; } }  // FD4: broker-assigned id
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            const int c = connectsSeen.fetch_add(1) + 1;
            const int64_t assigned = static_cast<int64_t>(100 + c);
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK), 0, assigned,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;
        const auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;
        producesSeen.fetch_add(1);
        // Always redirect (never resolve) so the per-frame redirect counter exhausts
        const std::string_view ep = "aeron:ipc";
        const std::size_t n = rcc::encodeErrorResponse(
            replyBuf, sizeof(replyBuf),
            static_cast<int16_t>(rcc::ErrorCode::Redirect),
            ep, opt->sessionId, opt->correlationId);
        if (n > 0) gwSend(replyBuf, n);
    }
};

TEST(Redirect, M2bMaxRedirectsExhaustedFailsCleanly) {
    static constexpr int32_t kInStr = 4400, kEgStr = 4401;
    const std::string agentDir = uniqueDir("cb5_m2b");
    const std::string aeronDir = agentDir + "/aeron";

    ClientConfig cfg;
    cfg.gatewayChannel    = "aeron:ipc";
    cfg.gatewayStreamId   = kInStr;
    cfg.egressChannel     = "aeron:ipc";
    cfg.egressStreamId    = kEgStr;
    cfg.dataDir           = agentDir;
    cfg.connectTimeoutMs  = 8000;
    // Use a shorter produce timeout so the test doesn't take too long if something
    // goes wrong, but long enough that the redirect-exhausted path fires first.
    cfg.produceTimeoutMs  = 20000;
    cfg.idleMode          = IdleMode::LowLatency;

    TopicSpec ts;
    ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);
    auto gw = std::make_unique<MaxRedirectGateway>(aeronDir, kInStr, kEgStr);

    test_support::TestCq connFutCq(*client);

    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    for (int i = 0; i < kPumpMax && !connFut.ready(); ++i) {
        gw->poll(16);
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_NO_THROW(connFut.check()) << "Initial connect must succeed";

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 20000;
    auto p = client->producer(pcfg);
    test_support::TestCq cqP(*client);
    auto th = client->topic("t");
    Key key(uint64_t{1});
    const std::vector<uint8_t> val = {0xCC};

    auto fut = cqP.send(p, th, key, std::span<const uint8_t>(val));

    // Pump until the future resolves (either error or value).
    // The gateway always redirects, so after kMaxRedirects the frame is failed.
    bool done = false;
    for (int i = 0; i < kPumpMax * 6; ++i) {
        gw->poll(16);
        if (fut.ready()) { done = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done) << "Produce future must resolve after kMaxRedirects exhausted";

    // The future must throw (redirect exhausted, not a valid offset).
    bool threw = false;
    try { (void)fut.offset(); } catch (...) { threw = true; }
    EXPECT_TRUE(threw) << "Produce future must throw when kMaxRedirects is exhausted";

    // The gateway must have seen > kMaxRedirects produce attempts
    EXPECT_GT(gw->producesSeen.load(), rune::client::producer::ProducerImpl::kMaxRedirects)
        << "Gateway should see > kMaxRedirects produce attempts before exhaustion";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}
