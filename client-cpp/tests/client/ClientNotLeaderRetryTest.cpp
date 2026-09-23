// tests/client/ClientNotLeaderRetryTest.cpp
//
// Milestone D Phase-2 Task 14 — Note A: a BARE NotLeader ErrorResponse
// (ErrorCode::NotLeader with an EMPTY message — no leader endpoint to follow)
// is RETRIABLE.  The client must:
//   1. NOT fire cbError_ immediately.
//   2. Arm a background metadata refresh (scheduleMetadataRefresh → the agent
//      emits a MetadataRequest under exponential backoff).
//   3. Re-enqueue / resend the in-flight Produce (bounded by kMaxRedirects).
// Only when the retry budget is exhausted may the pending ack fail.
//
// This mirrors the RedirectTest.cpp gateway harness: a single fake gateway on an
// aeron:ipc stream pair drives the REAL Client.cpp ErrorResponse handler with a
// genuinely-decoded ErrorResponse{NotLeader}.  The gateway answers the FIRST
// Produce with a bare NotLeader and the RESEND with ProduceResponse{OK}; it also
// answers any MetadataRequest so the background refresh completes cleanly.
//
// Assertions (the Note A contract):
//   - the produce future RESOLVES successfully (proves cbError_ was NOT the
//     terminal outcome — a bare NotLeader is retried, not surfaced as an error).
//   - the gateway saw >= 2 Produces (proves the op was re-enqueued/resent).
//   - the gateway saw >= 1 MetadataRequest (proves metadata refresh was armed).
//   - a separate case: a gateway that ALWAYS returns bare NotLeader exhausts the
//     redirect/retry budget and the future then fails (cbError only after budget).

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>

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
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;

static constexpr int  kPumpMax   = 6'000;
static constexpr auto kPumpSleep = 2ms;

namespace {
static std::atomic<int> g_nlCtr{0};
std::string uniqueDir(const char* prefix) {
    const int c = g_nlCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/") + prefix + "_" +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}
} // namespace

// ---------------------------------------------------------------------------
// NotLeaderGateway — first Produce → ErrorResponse{NotLeader, ""} (bare, no
// endpoint); resend → ProduceResponse{OK}.  Answers MetadataRequests so the
// armed background refresh completes.
// ---------------------------------------------------------------------------
struct NotLeaderGateway {
    std::atomic<int> connectsSeen{0};
    std::atomic<int> producesSeen{0};
    std::atomic<int> metadataReqsSeen{0};

    static constexpr int64_t kSessionId = 55;
    static constexpr int64_t kBaseOffset = 321;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;
    uint8_t replyBuf[2048]{};

    NotLeaderGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr) {
        aeron::Context ctx;
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
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }
    ~NotLeaderGateway() { gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset(); }

    int poll(int lim = 16) { return gwSub && gwAsm ? gwSub->poll(gwAsm->handler(), lim) : 0; }

    bool gwSend(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 500; ++i) {
            if (gwPub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    virtual void onProduce(const rcc::ProduceView& opt) {
        const int p = producesSeen.fetch_add(1) + 1;
        if (p == 1) {
            // BARE NotLeader — empty endpoint (message).  This is the retriable form.
            const std::size_t n = rcc::encodeErrorResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader),
                /*message=*/std::string_view{},   // BARE: no endpoint
                opt.sessionId, opt.correlationId);
            if (n > 0) gwSend(replyBuf, n);
        } else {
            const std::size_t n = rcc::encodeProduceResponse(
                replyBuf, sizeof(replyBuf),
                /*errorCode=*/0, opt.partition, kBaseOffset,
                opt.sessionId, opt.correlationId);
            if (n > 0) gwSend(replyBuf, n);
        }
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            connectsSeen.fetch_add(1);
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK), 0, kSessionId,
                opt->sessionId, opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::MetadataRequest) {
            // Count the armed background refresh.  We do not need to reply: the
            // produce retry succeeds independently of the metadata reconcile, and
            // the Client tolerates an unanswered refresh (it simply re-arms with
            // backoff).  Observing the MetadataRequest is the Note A signal.
            metadataReqsSeen.fetch_add(1);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;
        const auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;
        onProduce(*opt);
    }
};

// A gateway that ALWAYS returns a bare NotLeader — used to prove budget exhaustion.
struct AlwaysNotLeaderGateway : public NotLeaderGateway {
    using NotLeaderGateway::NotLeaderGateway;
    void onProduce(const rcc::ProduceView& opt) override {
        producesSeen.fetch_add(1);
        const std::size_t n = rcc::encodeErrorResponse(
            replyBuf, sizeof(replyBuf),
            static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader),
            std::string_view{}, opt.sessionId, opt.correlationId);
        if (n > 0) gwSend(replyBuf, n);
    }
};

template <class GW>
static std::unique_ptr<Client> makeConnectedClient(const std::string& agentDir,
                                                   int32_t inStr, int32_t egStr,
                                                   std::unique_ptr<GW>& gwOut,
                                                   std::function<void(const std::string&)> onErr = {}) {
    const std::string aeronDir = agentDir + "/aeron";
    ClientConfig cfg;
    cfg.gatewayChannel    = "aeron:ipc";
    cfg.gatewayStreamId   = inStr;
    cfg.egressChannel     = "aeron:ipc";
    cfg.egressStreamId    = egStr;
    cfg.dataDir           = agentDir;
    cfg.connectTimeoutMs  = 8000;
    cfg.pingTimeoutMs     = 8000;
    cfg.idleMode          = IdleMode::LowLatency;

    TopicSpec ts;
    ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    // onError() MUST be registered BEFORE connect().
    if (onErr) client->onError(std::move(onErr));
    std::this_thread::sleep_for(150ms);
    gwOut = std::make_unique<GW>(aeronDir, inStr, egStr);

    auto connFut = client->connect();
    bool connected = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gwOut->poll(16);
        if (connFut.wait_for(0s) == std::future_status::ready) { connected = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }
    EXPECT_TRUE(connected) << "Failed to connect to NotLeader gateway";
    connFut.get();
    return client;
}

// ===========================================================================
// Case 1: bare NotLeader triggers refresh + retry, NOT immediate cbError_.
// ===========================================================================
TEST(ClientNotLeaderRetry, BareNotLeaderTriggersRefreshNotError) {
    static constexpr int32_t kInStr = 4500, kEgStr = 4501;
    const std::string agentDir = uniqueDir("nl_retry");

    // Observe cbError_: it must NOT fire for a bare NotLeader that is retried.
    std::atomic<bool> errorFired{false};
    std::unique_ptr<NotLeaderGateway> gw;
    auto client = makeConnectedClient<NotLeaderGateway>(
        agentDir, kInStr, kEgStr, gw,
        [&](const std::string&) { errorFired.store(true); });

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 12'000;
    auto p  = client->producer(pcfg);
    auto th = client->topic("inventory");
    ASSERT_TRUE(th.valid());

    Key key(uint64_t{7});
    const std::vector<uint8_t> val = {0x01, 0x02, 0x03};
    auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);

    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (fut.wait_for(0s) == std::future_status::ready) { done = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done)
        << "Produce future must resolve after bare-NotLeader retry (gateway saw "
        << gw->producesSeen.load() << " produce(s))";

    int64_t baseOffset = -1;
    ASSERT_NO_THROW({ baseOffset = fut.get(); })
        << "Produce future must NOT throw — bare NotLeader is retried, not errored";
    EXPECT_GE(baseOffset, NotLeaderGateway::kBaseOffset);

    // The op was re-enqueued/resent: at least 2 produces reached the gateway.
    EXPECT_GE(gw->producesSeen.load(), 2)
        << "Bare NotLeader must trigger a resend (>= 2 produces seen)";

    // A metadata refresh was armed and fired.
    EXPECT_GE(gw->metadataReqsSeen.load(), 1)
        << "Bare NotLeader must arm a background metadata refresh (MetadataRequest seen)";

    // cbError_ must NOT have fired for the retried op.
    EXPECT_FALSE(errorFired.load())
        << "cbError_ must NOT fire immediately on a bare NotLeader (it is retriable)";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// Case 2: persistent bare NotLeader exhausts the retry budget and then fails.
// (cbError_/promise error only AFTER the redirect/retry budget is spent.)
// ===========================================================================
TEST(ClientNotLeaderRetry, PersistentNotLeaderFailsAfterBudgetExhausted) {
    static constexpr int32_t kInStr = 4600, kEgStr = 4601;
    const std::string agentDir = uniqueDir("nl_exhaust");

    std::unique_ptr<AlwaysNotLeaderGateway> gw;
    auto client = makeConnectedClient<AlwaysNotLeaderGateway>(agentDir, kInStr, kEgStr, gw);

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4; pcfg.produceTimeoutMs = 20'000;
    auto p  = client->producer(pcfg);
    auto th = client->topic("inventory");
    ASSERT_TRUE(th.valid());

    Key key(uint64_t{9});
    const std::vector<uint8_t> val = {0xAA};
    auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);

    bool done = false;
    for (int i = 0; i < kPumpMax * 6; ++i) {
        gw->poll(16);
        if (fut.wait_for(0s) == std::future_status::ready) { done = true; break; }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(done) << "Produce future must resolve (fail) after retry budget exhausted";

    bool threw = false;
    try { fut.get(); } catch (...) { threw = true; }
    EXPECT_TRUE(threw)
        << "Produce future must throw once the bare-NotLeader retry budget is exhausted";

    // > kMaxRedirects produce attempts were made before giving up.
    EXPECT_GT(gw->producesSeen.load(),
              rune::client::producer::ProducerImpl::kMaxRedirects)
        << "Gateway must see > kMaxRedirects produce attempts before failing";

    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}
