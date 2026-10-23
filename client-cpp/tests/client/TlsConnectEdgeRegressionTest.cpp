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
#include "../support/PlainInitPid.h"
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
#include <functional>
#include <map>
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
    return std::filesystem::temp_directory_path().string() + "/tls_edge_" + std::to_string(static_cast<long>(::getpid())) + "_" +
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
    int framesBeforeKeys = 0;     // a non-handshake frame before the broker side had keys for its session
    int unopenable = 0;           // a non-handshake frame that does not open under its session's keys
    int subscribesSeen = 0, joinGroupsSeen = 0, metadataSeen = 0, initPidsSeen = 0, pingsSeen = 0;
    std::vector<uint16_t> earlyTids;   // the templates of framesBeforeKeys (diagnostics)

    // Knobs.
    bool twoLeg = true;           // split the server's first flight; the test releases the second half
    bool release = false;
    /// Fix round 5 (N4-I1): fail the FINAL leg (answer the client's last HandshakeContinue with
    /// HandshakeContinueResponse{HandshakeFailed}, re-offered `failRepeats` more times — the real
    /// broker re-offers it reliably) for connects whose corrId is >= failFinalLegFromCorr.
    bool failFinalLeg = false;
    /// Review B I2: the ConnectResponse is v9 with authMode Required / MustAuthenticate — after its TLS the client logs in.
    bool requireAuth = false;
    int64_t failFinalLegFromCorr = 0;
    int failRepeats = 3;
    int finalLegFailuresSent = 0;

    // The withheld second leg.
    bool secondLegPending = false;
    int64_t legCorr = 0, legSid = 0;
    std::vector<uint8_t> secondLeg;
    struct Failing { int64_t sid, corr; int left; };
    std::vector<Failing> failing;

    struct Conn {
        std::optional<rune::crypto::TlsSession> srv;
        rune::crypto::Cipher cipher{};
        int64_t corr = 0;
        rcs::CryptoState keys;
        bool failed = false;
    };
    std::map<int64_t, Conn> conns;   // by assigned session id
    int64_t seedSid = 0;             // the first connect's id (corr < the node-session stride)

    std::string serverPem, serverKey;
    std::shared_ptr<aeron::Aeron> gwAeron;
    std::shared_ptr<aeron::Subscription> gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler> gwAsm;

    /// `idle` runs while the gateway waits for its publications: an Invoker client's embedded
    /// driver only works while its owner pumps it.
    TwoLegTlsGateway(const std::string& aeronDir, int32_t in, int32_t eg, const std::function<void()>& idle = {}) {
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
            if (idle) idle();
            std::this_thread::sleep_for(kPumpSleep);
        }
        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            });
    }
    ~TwoLegTlsGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    bool seedEncrypted() const {
        auto it = conns.find(seedSid);
        return it != conns.end() && it->second.keys.encrypted;
    }
    int poll(int limit = 16) {
        int w = gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), limit) : 0;
        if (secondLegPending && release) {
            secondLegPending = false;
            std::vector<uint8_t> resp;
            const std::size_t rn = rcc::encodeHandshakeContinueResponse(
                resp, 0, std::string_view(reinterpret_cast<const char*>(secondLeg.data()), secondLeg.size()), legSid,
                legCorr);
            sendRaw(resp.data(), rn);
            ++w;
        }
        for (auto& f : failing) {
            if (f.left <= 0) continue;
            --f.left;
            sendFinalLegFailure(f.sid, f.corr);
            ++w;
        }
        return w;
    }
    void sendFinalLegFailure(int64_t sid, int64_t corr) {
        std::vector<uint8_t> resp;
        const std::size_t rn = rcc::encodeHandshakeContinueResponse(
            resp, static_cast<int16_t>(rune::protocol::client::ErrorCode::HandshakeFailed), {}, sid, corr);
        sendRaw(resp.data(), rn);
        ++finalLegFailuresSent;
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
            const int64_t sid = 40 + connectsSeen;
            if (seedSid == 0 && c->correlationId < 1'000'000'000'000LL) seedSid = sid;
            Conn& cn = conns[sid];
            cn.srv.emplace(rune::crypto::TlsSession::makeServer(serverPem.c_str(), serverKey.c_str()));
            cn.cipher = static_cast<rune::crypto::Cipher>(c->cipherList[0]);
            cn.corr = c->correlationId;
            const auto& hb = c->handshakeBytes;
            (void)cn.srv->readInbound(reinterpret_cast<const uint8_t*>(hb.data()), hb.size());
            const std::size_t n = cn.srv->writeOutbound(out, sizeof out);
            // Leg 1: with twoLeg, only the first half of the server's flight — the client's TLS needs more.
            const std::size_t first = twoLeg ? n / 2 : n;
            if (twoLeg) {
                secondLeg.assign(out + first, out + n);
                legCorr = c->correlationId;
                legSid = sid;
                secondLegPending = true;
            }
            if (requireAuth) {
                std::vector<uint8_t> v9;
                const std::size_t rn9 = rcc::encodeConnectResponseV9(
                    v9, 0, 0, sid, c->sessionId, c->correlationId, rune::protocol::client::AuthMode::Required,
                    rune::protocol::client::AuthState::MustAuthenticate, rune::protocol::client::kBrokerVersion,
                    c->cipherList[0], std::string_view(reinterpret_cast<const char*>(out), first));
                sendRaw(v9.data(), rn9);
                return;
            }
            uint8_t resp[16384 + 128];
            const std::size_t rn = rcc::encodeConnectResponse(
                resp, sizeof resp, 0, 0, sid, c->sessionId, c->correlationId, rune::protocol::client::kBrokerVersion,
                c->cipherList[0], std::string_view(reinterpret_cast<const char*>(out), first));
            sendRaw(resp, rn);
            return;
        }
        case rcc::TemplateId::HandshakeContinue: {
            auto hc = rcc::decodeHandshakeContinue(buf, len);
            if (!hc) return;
            auto it = conns.find(hc->sessionId);
            if (it == conns.end() || !it->second.srv) return;
            Conn& cn = it->second;
            (void)cn.srv->readInbound(reinterpret_cast<const uint8_t*>(hc->handshakeBytes.data()),
                                      hc->handshakeBytes.size());
            const std::size_t n = cn.srv->writeOutbound(out, sizeof out);
            if (cn.srv->done() && failFinalLeg && cn.corr >= failFinalLegFromCorr && !cn.failed) {
                // The broker's failPendingHandshake_: the session is never promoted.
                cn.failed = true;
                sendFinalLegFailure(hc->sessionId, hc->correlationId);
                failing.push_back({hc->sessionId, hc->correlationId, failRepeats});
                return;
            }
            if (cn.srv->done() && !cn.keys.encrypted) {
                const auto keys = cn.srv->exportKeys(cn.cipher, hc->sessionId);
                cn.keys.encrypted = true;
                cn.keys.cipher    = keys.cipher;
                cn.keys.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
                cn.keys.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
            }
            if (n > 0) {
                std::vector<uint8_t> resp;
                const std::size_t rn = rcc::encodeHandshakeContinueResponse(
                    resp, 0, std::string_view(reinterpret_cast<const char*>(out), n), hc->sessionId, hc->correlationId);
                sendRaw(resp.data(), rn);
            }
            return;
        }
        default: {
            const int64_t sid = len >= 16 ? static_cast<int64_t>(rune::protocol::client::detail::getLE64(buf, 8)) : 0;
            auto it = conns.find(sid);
            if (it == conns.end() || !it->second.keys.encrypted) {
                ++framesBeforeKeys;
                earlyTids.push_back(*tid);
                return;
            }
            uint8_t opened[16384];
            const std::size_t on = rcc::openEgressFrame(it->second.keys, buf, len, opened, sizeof opened);
            if (on == 0) { ++unopenable; return; }
            const auto t = static_cast<rcc::TemplateId>(*tid);
            if (t == rcc::TemplateId::Subscribe) ++subscribesSeen;
            if (t == rcc::TemplateId::MetadataRequest) ++metadataSeen;
            if (*tid == static_cast<uint16_t>(rune::protocol::client::TemplateId::JoinGroup)) ++joinGroupsSeen;
            if (*tid == static_cast<uint16_t>(rune::protocol::client::TemplateId::InitProducerId)) ++initPidsSeen;
            if (*tid == static_cast<uint16_t>(rune::protocol::client::TemplateId::InitProducerId)) {
                uint8_t plain[128], sealed[256];   // FD4: a plain producer's id from the "broker"
                if (const std::size_t pn = test_support::plainInitPidReply(opened, on, 9001, plain); pn > 0) {
                    const std::size_t sn = rcc::sealFrame(it->second.keys, plain, pn, sealed, sizeof sealed);
                    if (sn > 0) sendRaw(sealed, sn);
                }
            }
            if (t == rcc::TemplateId::Ping) {
                ++pingsSeen;
                if (auto pg = rcc::decodePing(opened, on)) {
                    uint8_t plain[512], sealed[1024];
                    const std::size_t pn = rcc::encodePong(plain, sizeof plain, pg->echo, sid, pg->correlationId);
                    const std::size_t sn = rcc::sealFrame(it->second.keys, plain, pn, sealed, sizeof sealed);
                    if (sn > 0) sendRaw(sealed, sn);
                }
            }
            return;   // nothing else answered: the assertions are about what the client sends
        }
        }
    }
};

struct Rig {
    static constexpr int32_t kInStr = 4114, kEgStr = 4115;
    std::string agentDir = uniqueDir();
    bool invoker = false;
    std::unique_ptr<Client> client;
    std::unique_ptr<TwoLegTlsGateway> gw;
    explicit Rig(bool invokerMode = false, bool twoLeg = true, bool login = false) : invoker(invokerMode) {
        ClientConfig cfg;
        if (login) { cfg.username = "ws1"; cfg.password = "correct horse battery"; }
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = kInStr;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = kEgStr;
        cfg.dataDir = agentDir; cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 8000;
        cfg.idleMode = IdleMode::LowLatency;
        cfg.offerCrypto = true; cfg.requireCrypto = true;
        cfg.tlsTrustPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/ca.pem");
        cfg.tlsExpectHost = "localhost";
        if (invoker) cfg.runnerMode = RunnerMode::Invoker;
        TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        if (invoker) {
            for (int i = 0; i < 75; ++i) { (void)client->doWork(); std::this_thread::sleep_for(kPumpSleep); }
        } else {
            std::this_thread::sleep_for(150ms);
        }
        std::function<void()> idle;
        if (invoker) idle = [this] { (void)client->doWork(); };
        gw = std::make_unique<TwoLegTlsGateway>(agentDir + "/aeron", kInStr, kEgStr, idle);
        gw->twoLeg = twoLeg;
    }
    ~Rig() {
        client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
    void step() {
        gw->poll(16);
        if (invoker) (void)client->doWork();
        std::this_thread::sleep_for(kPumpSleep);
    }
    void pumpFor(std::chrono::milliseconds d) {
        for (const auto dl = std::chrono::steady_clock::now() + d; std::chrono::steady_clock::now() < dl;) step();
    }
    template <class Pred> bool pumpWithin(std::chrono::milliseconds d, Pred p) {
        for (const auto dl = std::chrono::steady_clock::now() + d; std::chrono::steady_clock::now() < dl;) {
            if (p()) return true;
            step();
        }
        return p();
    }
    template <class Pred> bool pumpUntil(Pred p) {
        for (int i = 0; i < kPumpMax; ++i) {
            if (p()) return true;
            step();
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
    EXPECT_TRUE(r.pumpUntil([&] { return r.gw->seedEncrypted(); })) << "the client's Finished never arrived";
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
    ProducerConfig tcfg;
    tcfg.acks            = Acks::All;
    tcfg.transactionalId = "txn-a";
    auto tp = r.client->producer(tcfg);
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

// ===========================================================================
// Fix round 5 (N4-I1), superseded by the PR #476 review (issues #14 / #469): the broker
// fails the handshake on its FINAL leg - after the client already went Connected (a TLS
// 1.3 client is done once it has SENT its Finished).  That frame is in clear and names
// only guessable ids, so it is never terminal: before a sealed frame proved the keys it
// is a transient connect failure (a fresh TLS episode, maxConnectRetries times, then a
// growing Backoff - never a reconnect storm, never Rejected); once the keys are proven it
// is a forgery and ignored (ClientAuthErrorsRegression.AForgedFinalLegFailure...).
// Acting only on a frame naming the session's current id AND its connect corrId.
// ===========================================================================

// A broker that fails EVERY final leg: the seed is never Rejected, reports the failure,
// retries a bounded number of times and is never reported live.  RED: a terminal Rejected
// (the old ruling), or an unbounded reconnect storm (the failover treating the optimistic
// Connected as success).
TEST(TlsConnectEdgeRegression, FinalLegHandshakeFailedFailsTheSeedExplicitlyRegression) {
    Rig r(/*invokerMode=*/true, /*twoLeg=*/false);
    r.gw->failFinalLeg = true;
    std::vector<std::string> errors;
    r.client->onError([&](const std::string& e) { errors.push_back(e); });
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpUntil([&] { return conn.ready(); }));
    EXPECT_NO_THROW(conn.check()) << "connect() completes when the client's TLS is done (optimistically)";
    ASSERT_TRUE(r.pumpWithin(2s, [&] { return r.client->testHandshakeFinalLegFailures() >= 1; }))
        << "the final-leg verdict was never applied";
    const int connectsAtFailure = r.gw->connectsSeen;
    bool everRejected = false;
    (void)r.pumpWithin(3s, [&] { everRejected |= r.client->state() == rcs::SessionState::Rejected; return false; });
    EXPECT_FALSE(everRejected) << "an unauthenticated final-leg verdict ended the client for good";
    EXPECT_GT(r.gw->connectsSeen, connectsAtFailure) << "retried as a fresh TLS episode";
    EXPECT_LE(r.gw->connectsSeen - connectsAtFailure, 20) << "a reconnect storm (bounded retries, then a doubling Backoff)";
    ASSERT_FALSE(errors.empty()) << "onError reports the failure";
    EXPECT_NE(errors[0].find("29"), std::string::npos) << errors[0];
    EXPECT_FALSE(conn.failed()) << "connect() is never re-answered";
    // Re-check of PR #476 round 1 (non-weakening): work issued to a seed that never proves its keys still completes -
    // explicitly, by its deadline - never hangs (the old ruling asserted the pending ping was failed).
    auto ping = cq.issue([&](Completions& q, Token t) { return r.client->ping(Bytes(4, 7), q, t); });
    EXPECT_TRUE(r.pumpWithin(30s, [&] { return ping.ready(); })) << "a ping on a never-proven TLS seed hung";
    if (ping.ready()) EXPECT_FALSE(ping.ok()) << "the broker never served it: " << ping.describe();
}

// Review B I2: with authentication a TLS 1.3 client has sent its Finished and sits in Authenticating when the broker's
// final-leg HandshakeFailed arrives.  The pending connect must complete with HandshakeFailed once the retries are spent
// - not hang to its timeout, and never a terminal Rejected.
TEST(TlsConnectEdgeRegression, FinalLegHandshakeFailedWhileAuthenticatingFailsTheConnectRegression) {
    Rig r(/*invokerMode=*/true, /*twoLeg=*/false, /*login=*/true);
    r.gw->failFinalLeg = true;
    r.gw->requireAuth  = true;
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpWithin(6s, [&] { return conn.ready(); })) << "the connect hung (the verdict was ignored)";
    EXPECT_TRUE(conn.failed()) << conn.describe();
    EXPECT_EQ(conn.outcome().c.errorCode, 29) << "HandshakeFailed: " << conn.describe();
    EXPECT_NE(r.client->state(), rcs::SessionState::Rejected) << "never terminal";
    const int connects = r.gw->connectsSeen;
    r.pumpFor(1s);
    EXPECT_LE(r.gw->connectsSeen - connects, 12) << "a reconnect storm (the doubling Backoff paces the retries)";
}

// A node session (a partition route to another node) fails its final leg: only THAT
// session is affected - the frame is resolved by (session id, connect corrId); the seed
// stays Connected and keeps answering; the node session is never Rejected and its
// retries are bounded.  RED (no (sid, corr) resolution): the frame falls back to the seed.
TEST(TlsConnectEdgeRegression, NodeSessionFinalLegHandshakeFailedRejectsOnlyThatSessionRegression) {
    Rig r(/*invokerMode=*/true, /*twoLeg=*/false);
    r.gw->failFinalLeg = true;
    r.gw->failFinalLegFromCorr = 1'000'000'000'000LL;   // node sessions' corrId range only
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpUntil([&] { return conn.ready(); }));
    ASSERT_NO_THROW(conn.check());
    ASSERT_TRUE(r.pumpUntil([&] { return r.gw->seedEncrypted(); }));
    r.client->testSetRoute(1, 0, "aeron:ipc?alias=node1");   // p0 led by "node 1": its own session
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    auto p = r.client->producer(pcfg);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto fut = cq.send(p, r.client->topic("inventory"), Key(uint64_t{42}), std::span<const uint8_t>(val));
    ASSERT_TRUE(r.pumpUntil([&] { return r.client->testSessionStateAt(1) != rcs::SessionState::Disconnected; }))
        << "precondition: the route opened a node session";
    ASSERT_TRUE(r.pumpWithin(2s, [&] { return r.client->testHandshakeFinalLegFailures() >= 1; }));
    bool nodeRejected = false;
    (void)r.pumpWithin(2s, [&] { nodeRejected |= r.client->testSessionStateAt(1) == rcs::SessionState::Rejected; return false; });
    EXPECT_FALSE(nodeRejected) << "an unauthenticated final-leg verdict ended the node session for good";
    EXPECT_LE(r.client->testHandshakeFinalLegFailures(), 12u) << "a reconnect storm on the node session";
    EXPECT_EQ(r.client->state(), rcs::SessionState::Connected) << "the seed was hit by the node's verdict";
    auto ping = cq.issue([&](Completions& q, Token t) { return r.client->ping(Bytes(4, 7), q, t); });
    EXPECT_TRUE(r.pumpWithin(2s, [&] { return ping.ready(); }));
    EXPECT_TRUE(ping.ok()) << (ping.ready() ? ping.describe() : std::string("pending"));
}
