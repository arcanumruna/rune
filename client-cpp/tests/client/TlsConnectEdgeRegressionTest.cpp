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
#include "../support/InvokerGateway.h"
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
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;

namespace {

constexpr int kPumpMax = 6'000;
std::atomic<int> g_ctr{0};

// The Rig's injected clock (one Rig at a time per test; reset in the Rig constructor).  Nothing here sleeps or reads
// wall time: simulated time passes only through Rig::pumpFor / pumpWithin.
int64_t g_nowNs = 1'000'000'000;
int64_t manualNow() noexcept { return g_nowNs; }
constexpr int64_t kTickNs = 1'000'000;   // one pump step = 1 ms of simulated time

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
    std::function<void()> idle_;   // pumps the Invoker client while the gateway waits for the ring (no sleeping)

    /// `idle` runs while the gateway waits for its publications: an Invoker client's embedded
    /// driver only works while its owner pumps it.
    TwoLegTlsGateway(const std::string& aeronDir, int32_t in, int32_t eg, const std::function<void()>& idle = {})
        : idle_(idle) {
        serverPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.pem");
        serverKey = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.key");
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        const int64_t subId = gwAeron->addSubscription("aeron:ipc", in);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", eg);
        for (int i = 0; i < kPumpMax * 10; ++i) {   // hard iteration cap: a failing setup ends, never hangs
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            if (idle) idle();
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
        for (int i = 0; i < 5'000 && gwPub->offer(ab, 0, static_cast<aeron::util::index_t>(n)) <= 0; ++i)
            if (idle_) idle_();   // back-pressured: let the client drain its egress (no sleeping)
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
    std::unique_ptr<Client> client;
    std::unique_ptr<TwoLegTlsGateway> gw;
    explicit Rig(bool twoLeg = true, bool login = false) {
        g_nowNs = 1'000'000'000;
        ClientConfig cfg;
        if (login) { cfg.username = "ws1"; cfg.password = "correct horse battery"; }
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = kInStr;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = kEgStr;
        cfg.dataDir = agentDir; cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 8000;
        cfg.idleMode = IdleMode::LowLatency;
        cfg.offerCrypto = true; cfg.requireCrypto = true;
        cfg.tlsTrustPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/ca.pem");
        cfg.tlsExpectHost = "localhost";
        cfg.runnerMode = RunnerMode::Invoker;   // every test drives the client itself, on the injected clock
        cfg.nanoClock  = &manualNow;
        TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        // The gateway waits for the client's embedded driver to create the rings: pump the client meanwhile.
        gw = std::make_unique<TwoLegTlsGateway>(agentDir + "/aeron", kInStr, kEgStr,
                                                [this] { (void)client->doWork(); });
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
        (void)client->doWork();
    }
    /// Simulated time: one step per millisecond, the clock advancing with each.
    void pumpFor(std::chrono::milliseconds d) {
        for (int64_t i = 0; i < d.count(); ++i) {
            g_nowNs += kTickNs;
            step();
        }
    }
    /// Returns as soon as p() holds; the step count (d / 1 ms) is the hard cap, so a failure ends, never hangs.
    template <class Pred> bool pumpWithin(std::chrono::milliseconds d, Pred p) {
        for (int64_t i = 0; i < d.count(); ++i) {
            if (p()) return true;
            g_nowNs += kTickNs;
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
    Rig r(/*twoLeg=*/false);
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
    Rig r(/*twoLeg=*/false, /*login=*/true);
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

// Issue #481: once the app's connect has failed on unproven final legs, a session that does not reconnect forever stops:
// Disconnected, and the gateway sees no further ConnectRequest.  RED (old): Backoff kept reconnecting in the background.
TEST(TlsConnectEdgeRegression, ANonForeverSessionEndsDisconnectedAfterUnprovenFinalLegFailuresRegression) {
    Rig r(/*twoLeg=*/false, /*login=*/true);
    r.gw->failFinalLeg = true;
    r.gw->requireAuth  = true;
    test_support::TestCq cq(*r.client);
    auto conn = cq.issue([&](Completions& q, Token t) { return r.client->connect(q, t); });
    ASSERT_TRUE(r.pumpWithin(6s, [&] { return conn.ready(); })) << "the connect hung";
    ASSERT_TRUE(conn.failed()) << conn.describe();
    // The machine spends what is left of its connect budget, then stops for good.
    ASSERT_TRUE(r.pumpWithin(6s, [&] { return r.client->state() == rcs::SessionState::Disconnected; }))
        << "never settled: state " << static_cast<int>(r.client->state());
    const int connects = r.gw->connectsSeen;
    r.pumpFor(2s);   // past several doubling Backoff delays (10 ms .. 1 s): any Backoff would have reconnected
    EXPECT_EQ(r.gw->connectsSeen, connects) << "background retries after the app was told the connect failed";
    EXPECT_EQ(r.client->state(), rcs::SessionState::Disconnected);
}

// A node session (a partition route to another node) fails its final leg: only THAT
// session is affected - the frame is resolved by (session id, connect corrId); the seed
// stays Connected and keeps answering; the node session is never Rejected and its
// retries are bounded.  RED (no (sid, corr) resolution): the frame falls back to the seed.
TEST(TlsConnectEdgeRegression, NodeSessionFinalLegHandshakeFailedRejectsOnlyThatSessionRegression) {
    Rig r(/*twoLeg=*/false);
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

// ===========================================================================
// #426: crypto OFFERED (not required).  A new connect episode drops the session's keys while the broker still holds
// the old session (its lease has not expired) and may answer on it, sealed under the discarded key.  Such a frame
// names a session id no live key opens: it is never decoded as plaintext.
// ===========================================================================
namespace {

constexpr uint64_t kStaleBase = 1ull;

struct StaleRig {
    static constexpr int32_t kIn = 4120, kEg = 4121;
    std::string dir = uniqueDir();
    std::unique_ptr<Client> client;
    std::unique_ptr<test_support::InvokerGateway> gw;
    explicit StaleRig(bool newIdOnResume = true) {
        g_nowNs = 1'000'000'000;
        ClientConfig cfg;
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = kIn;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = kEg;
        cfg.dataDir = dir;
        cfg.runnerMode = RunnerMode::Invoker;
        cfg.nanoClock  = &manualNow;
        cfg.offerCrypto = true; cfg.requireCrypto = false;
        cfg.tlsTrustPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/ca.pem");
        cfg.keepaliveIntervalNs = 20'000'000;   // a silent broker is noticed within ~0.1 s of simulated time
        cfg.responseTimeoutNs   = 100'000'000;
        cfg.connectTimeoutMs    = 60'000;
        cfg.reconnectForeverAfterLive = true;
        TopicSpec ts; ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        gw = std::make_unique<test_support::InvokerGateway>(dir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId,
                                                            cfg.egressChannel, cfg.egressStreamId);
        gw->tlsCertPem = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.pem");
        gw->tlsKeyPem  = slurpFile(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/server.key");
        gw->uniqueSessionIds = true;   // every episode is a NEW broker session (a fresh id)
        gw->newIdOnResume    = newIdOnResume;   // true: a resume hint still gets a NEW session id
        gw->replyPongs       = true;
        gw->initPidReply     = true;   // the idempotent producer's InitProducerId is answered
    }
    ~StaleRig() {
        client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(dir); } catch (...) {}
    }
    void step() { gw->pump(); (void)client->doWork(); }
    template <class Pred> bool within(int64_t ms, Pred p) {   // simulated ms; the count is the hard cap
        for (int64_t i = 0; i < ms; ++i) {
            if (p()) return true;
            g_nowNs += kTickNs;
            step();
        }
        return p();
    }
    template <class Pred> bool until(Pred p) {   // event-driven, no simulated time
        for (int i = 0; i < 200'000; ++i) {
            if (p()) return true;
            step();
        }
        return p();
    }
    void connect(test_support::TestCq& cq) {
        ASSERT_TRUE(until([&] { return gw->ready(); }));
        auto f = cq.issue([&](Completions& q, Token t) { return client->connect(q, t); });
        ASSERT_TRUE(until([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

}  // namespace

TEST(TlsConnectEdgeRegression, AnAckSealedUnderTheDiscardedKeyIsNeverReadInClearRegression) {
    StaleRig r;
    test_support::TestCq cq(*r.client);
    r.connect(cq);
    r.gw->holdPartitions.insert(0);
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 600'000; pcfg.deliveryTimeoutMs = 600'000;
    auto p = r.client->producer(pcfg);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto rec = cq.send(p, r.client->topic("t"), Key(kStaleBase), std::span<const uint8_t>(val));
    ASSERT_TRUE(r.within(5'000, [&] { return !r.gw->held.empty(); })) << "the record never reached the broker: produced " << r.gw->produced.size() << " pids " << r.gw->initPids
        << " rec " << rec.ready() << " " << (rec.ready() ? rec.describe() : std::string()) << " state " << int(r.client->state());
    const auto stale = r.gw->held.front();   // the broker's ack for it, sealed under THIS episode's key
    const int64_t oldSid = stale.sessionId;
    // The broker answers it on the old session right before it answers the new episode's ConnectRequest.
    r.gw->ackBeforeNextConnect.push_back(stale);
    const int connectsBefore = r.gw->connects;
    // The broker goes silent (no pongs): the session times out and a new connect episode starts.
    r.gw->replyPongs = false;
    ASSERT_TRUE(r.within(30'000, [&] { return r.gw->connects > connectsBefore; })) << "no new connect episode";
    r.gw->replyPongs = true;
    ASSERT_TRUE(r.within(5'000, [&] { return r.client->state() == rcs::SessionState::Connected && r.gw->lastUniqueSessionId() != oldSid; }))
        << "state " << static_cast<int>(r.client->state()) << " connects " << r.gw->connects << " oldSid " << oldSid
        << " last " << r.gw->lastUniqueSessionId();
    r.within(200, [] { return false; });
    EXPECT_FALSE(rec.ready()) << "the stale sealed ack completed the record: " << rec.describe();
    // The new episode: the held batch is resent on the new session and acked there.
    ASSERT_TRUE(r.within(10'000, [&] { return r.gw->held.size() >= 2; })) << "the batch was never resent on the new session";
    const auto resent = r.gw->held.back();
    EXPECT_NE(resent.sessionId, oldSid) << "resent on the new session";
    EXPECT_FALSE(rec.ready()) << rec.describe();
    r.gw->holdPartitions.clear();
    r.gw->ack(resent);   // the broker acks the resend, on the new session
    ASSERT_TRUE(r.within(10'000, [&] { return rec.ready(); })) << "the record never completed after the new episode";
    EXPECT_EQ(rec.status(), OpStatus::Ok) << rec.describe();
    EXPECT_EQ(cq.timesCompleted(rec.token()), 1);
}

// The exemption stays: a CLEAR ErrorResponse answering the new episode's in-flight ConnectRequest (a pre-TLS
// NotLeader / Redirect / refusal) is still dispatched even though its header names the discarded session's id.
TEST(TlsConnectEdgeRegression, AClearConnectRefusalStillReachesTheNewEpisodeRegression) {
    StaleRig r;
    test_support::TestCq cq(*r.client);
    r.connect(cq);
    const int connectsBefore = r.gw->connects;
    const int64_t oldSid     = r.gw->lastUniqueSessionId();
    r.gw->replyConnects = false;   // the new episode's ConnectRequest stays unanswered
    r.gw->replyPongs    = false;   // the session times out: a new episode starts
    ASSERT_TRUE(r.within(30'000, [&] { return r.gw->connects > connectsBefore; })) << "no new connect episode";
    ASSERT_FALSE(r.gw->connectCorrIds.empty());
    const int connectsInFlight = r.gw->connects;
    // A clear NotLeader (no endpoint) naming the old session id and the in-flight connect's correlation id.
    uint8_t out[128];
    const std::size_t n = rcc::encodeErrorResponse(out, sizeof out,
                                                   static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader),
                                                   {}, oldSid, r.gw->connectCorrIds.back());
    ASSERT_GT(n, 0u);
    ASSERT_TRUE(r.gw->sendInClear(0, std::vector<uint8_t>(out, out + n)));
    // The machine acts on it at once (it resends the ConnectRequest), long before the 60 s connect timeout.
    EXPECT_TRUE(r.within(50, [&] { return r.gw->connects > connectsInFlight; }))
        << "the clear refusal answering the in-flight connect never reached the session machine";
}

// A plaintext episode after an encrypted one: the broker answers the new episode in clear and RESUMES the same session id
// (no TLS offered back).  The retired marker is cleared at that Connected edge, so the clear ack naming the id is read.
TEST(TlsConnectEdgeRegression, APlaintextEpisodeAfterAnEncryptedOneReadsClearFramesRegression) {
    StaleRig r(/*newIdOnResume=*/false);
    test_support::TestCq cq(*r.client);
    r.connect(cq);
    r.gw->holdPartitions.insert(0);
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 600'000; pcfg.deliveryTimeoutMs = 600'000;
    auto p = r.client->producer(pcfg);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto rec = cq.send(p, r.client->topic("t"), Key(kStaleBase), std::span<const uint8_t>(val));
    ASSERT_TRUE(r.within(5'000, [&] { return !r.gw->held.empty(); }));
    const int64_t sid = r.gw->held.front().sessionId;
    const int connectsBefore = r.gw->connects;
    r.gw->tlsCertPem.clear();   // the next episode is answered in clear (no TLS flight, no keys)
    r.gw->replyPongs = false;
    ASSERT_TRUE(r.within(30'000, [&] { return r.gw->connects > connectsBefore; })) << "no new connect episode";
    r.gw->replyPongs = true;
    ASSERT_TRUE(r.within(5'000, [&] { return r.client->state() == rcs::SessionState::Connected; }));
    ASSERT_TRUE(r.within(10'000, [&] { return r.gw->held.size() >= 2; })) << "the batch was never resent";
    const auto resent = r.gw->held.back();
    EXPECT_EQ(resent.sessionId, sid) << "the same session id was resumed in clear";
    EXPECT_FALSE(rec.ready()) << rec.describe();
    r.gw->holdPartitions.clear();
    r.gw->ack(resent);   // a clear ack naming the id the client once held sealed
    ASSERT_TRUE(r.within(10'000, [&] { return rec.ready(); })) << "a clear frame of a plaintext episode was dropped";
    EXPECT_EQ(rec.status(), OpStatus::Ok) << rec.describe();
}

// #426 review — FAILING (not committed).  Encrypted episode A, then a PLAINTEXT episode with a NEW id: the retired
// marker is cleared unconditionally at that Connected edge, so the broker's late ack for A (sealed under A's key,
// naming A's id, which no plaintext session holds) is read in clear.  Only a plaintext resume of the SAME id may clear it.
TEST(TlsConnectEdgeRegression, APlaintextEpisodeWithANewIdNeverReadsTheOldSealedAckAdversarialRegression) {
    StaleRig r(/*newIdOnResume=*/true);
    test_support::TestCq cq(*r.client);
    r.connect(cq);
    r.gw->holdPartitions.insert(0);
    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 600'000; pcfg.deliveryTimeoutMs = 600'000;
    auto p = r.client->producer(pcfg);
    const std::vector<uint8_t> val = {1, 2, 3};
    auto rec = cq.send(p, r.client->topic("t"), Key(kStaleBase), std::span<const uint8_t>(val));
    ASSERT_TRUE(r.within(5'000, [&] { return !r.gw->held.empty(); }));
    const auto staleA = r.gw->held.front();
    const int64_t sidA = staleA.sessionId;
    const int connectsBefore = r.gw->connects;
    r.gw->tlsCertPem.clear();   // the next episode is plaintext
    // A's ack (sealed under A's key) arrives right AFTER the plaintext ConnectResponse, before the batch is resent.
    r.gw->ackAfterNextConnect.push_back(staleA);
    r.gw->replyPongs = false;
    ASSERT_TRUE(r.within(30'000, [&] { return r.gw->connects > connectsBefore; })) << "no new connect episode";
    r.gw->replyPongs = true;
    ASSERT_TRUE(r.within(5'000, [&] { return r.client->state() == rcs::SessionState::Connected &&
                                             r.gw->lastUniqueSessionId() != sidA; }));
    r.within(200, [] { return false; });
    EXPECT_FALSE(rec.ready()) << "an ack sealed under episode A's key was read in clear: " << rec.describe();
}
