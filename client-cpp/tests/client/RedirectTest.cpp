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
    return std::string("/tmp/") + prefix + "_" +
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
    int64_t lastBaseOffset = -1;

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
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            ++connectsSeen;

            // Always reply OK — handles initial connect and post-redirect reconnect.
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;

        const auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;

        ++producesSeen;

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
    auto connFut = client->connect();
    bool connected = false;
    for (int i = 0; i < kPumpMax; ++i) {
        gw->poll(16);
        if (connFut.wait_for(0s) == std::future_status::ready) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(connected) << "Failed to connect to redirect gateway";
    ASSERT_NO_THROW(connFut.get()) << "connect() must not throw";

    // Create a producer with minimal linger so the Produce fires quickly.
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 12'000; // 12 s — accommodate redirect round-trip

    auto p  = client->producer(pcfg);
    auto th = client->topic("inventory");
    ASSERT_TRUE(th.valid()) << "topic 'inventory' must be valid";

    Key key(uint64_t{42});
    const std::vector<uint8_t> val = {0xDE, 0xAD, 0xBE, 0xEF};
    auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);

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
        if (fut.wait_for(0s) == std::future_status::ready) {
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
    ASSERT_NO_THROW({ baseOffset = fut.get(); })
        << "Produce future must not throw after redirect-follow";

    EXPECT_GE(baseOffset, RedirectGateway::kBaseOffset)
        << "baseOffset must match gateway-assigned value";

    // Gateway must have seen exactly 2 Produces:
    //   #1 → Redirect reply, #2 → ProduceResponse OK.
    EXPECT_EQ(gw->producesSeen, 2)
        << "Gateway must see 2 Produces (1 redirected + 1 resend)";

    client->close();

    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}
