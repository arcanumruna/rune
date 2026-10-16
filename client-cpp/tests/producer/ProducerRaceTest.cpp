// tests/producer/ProducerRaceTest.cpp
//
// DR-BUG-1 regression test — first-produce-after-connect SIGSEGV race.
//
// Hypothesis confirmed by TSan:
//   Client::producer() calls ProducerHub::add() (impls_.push_back) on the
//   APP THREAD while the runner thread is simultaneously iterating impls_ in
//   ProducerHub::drainAndFlush(). This is a data race on the std::vector that
//   manifests as a SIGSEGV on the first produce ~80% of the time.
//
// This test:
//   1. Connects a Client (runner thread already spinning).
//   2. Immediately calls producer() and p.send() with NO settle delay.
//   3. Repeats ≥ 200 iterations in a tight loop.
//   4. Asserts success — no SIGSEGV, no race under TSan.
//
// Before the fix: this test crashes or TSan reports a DATA RACE on impls_.
// After the fix: all iterations pass cleanly with zero TSan races.

#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>

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
// Shared constants
// ---------------------------------------------------------------------------

static constexpr int  kRacePumpMax   = 6'000;
static constexpr auto kRacePumpSleep = 2ms;
static constexpr auto kFutureTimeout = 10s;

// ---------------------------------------------------------------------------
// Unique temp dir
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_raceDirCtr{0};

std::string uniqueRaceDir() {
    const int c = g_raceDirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/dr_bug1_test_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// RaceGateway — minimal fake broker that:
//   • Answers ConnectRequest  → ConnectResponse{OK, sessionId=42}
//   • Answers Produce         → ProduceResponse{OK, baseOffset=100+n}
//   • Ignores everything else
// ---------------------------------------------------------------------------

struct RaceGateway {
    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::unique_ptr<aeron::FragmentAssembler>    asm_;

    int64_t nextOffset = 100;
    uint8_t replyBuf[2048]{};

    explicit RaceGateway(const std::string& aeronDir,
                         int32_t ingressStream, int32_t egressStream)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) throw std::runtime_error("RaceGateway: connect failed");

        int64_t subId = aeron_->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = aeron_->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub_) sub_ = aeron_->findSubscription(subId);
            if (!pub_) pub_ = aeron_->findExclusivePublication(pubId);
            if (sub_ && pub_) break;
            std::this_thread::sleep_for(kRacePumpSleep);
        }
        if (!sub_ || !pub_)
            throw std::runtime_error("RaceGateway: timed out resolving sub/pub");

        asm_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~RaceGateway() {
        asm_.reset(); pub_.reset(); sub_.reset(); aeron_.reset();
    }

    int poll(int limit = 16) {
        if (!sub_ || !asm_) return 0;
        return sub_->poll(asm_->handler(), limit);
    }

    bool send(const uint8_t* buf, std::size_t len) {
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
        for (int i = 0; i < 500; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);

        if (tid == rcc::TemplateId::Connect) {
            auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            return;
        }

        if (tid == rcc::TemplateId::Produce) {
            auto opt = rcc::decodeProduce(buf, len);
            if (!opt) return;
            if (opt->correlationId == 0) return; // acks=None
            const int64_t baseOff = nextOffset++;
            std::size_t n = rcc::encodeProduceResponse(
                replyBuf, sizeof(replyBuf),
                /*errorCode=*/0,
                opt->partition,
                baseOff,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
        }
    }
};

// ===========================================================================
// TEST: ConnectThenImmediateFirstProduceSingleIteration
//
// Baseline: one connect + one produce with NO settle. Must not crash / race.
// ===========================================================================
TEST(ProducerRace, ConnectThenImmediateFirstProduceSingleIteration) {
    static constexpr int32_t kIn  = 3100;
    static constexpr int32_t kEg  = 3101;

    std::string agentDir = uniqueRaceDir();
    std::string aeronDir = agentDir + "/aeron";

    ClientConfig cfg;
    cfg.gatewayChannel   = "aeron:ipc";
    cfg.gatewayStreamId  = kIn;
    cfg.egressChannel    = "aeron:ipc";
    cfg.egressStreamId   = kEg;
    cfg.dataDir          = agentDir;
    cfg.connectTimeoutMs = 8000;
    cfg.idleMode         = IdleMode::LowLatency; // hot runner → stress the race

    TopicSpec ts; ts.name = "events"; ts.topicId = 0; ts.partitionCount = 1;
    cfg.topics.push_back(ts);

    auto client = std::make_unique<Client>(cfg);
    std::this_thread::sleep_for(150ms);

    auto gw = std::make_unique<RaceGateway>(aeronDir, kIn, kEg);

    // Gateway poll thread (drives ConnectResponse and ProduceResponse).
    std::atomic<bool> gwStop{false};
    std::thread gwThread([&]() {
        while (!gwStop.load(std::memory_order_acquire)) {
            gw->poll(16);
            std::this_thread::sleep_for(1ms);
        }
    });

    // Connect (future-based, wait for completion).
    test_support::TestCq connFutCq(*client);
    auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
    ASSERT_TRUE(connFut.waitReady())
        << "connect() timed out";
    ASSERT_NO_THROW(connFut.check());

    // --- THE RACE: call producer() and send() immediately after connect() ---
    // NO sleep. NO settle. This is the exact scenario that crashed ~80% of runs.
    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;
    pcfg.batchBytes  = 1;
    pcfg.maxInFlight = 4;
    pcfg.produceTimeoutMs = 8000;

    auto p = client->producer(pcfg);

    test_support::TestCq cqP(*client);

    TopicHandle th{0, 1};
    Key key(uint64_t{42});
    const std::vector<uint8_t> val = {0xAB, 0xCD};

    auto fut = cqP.send(p, th, key, std::span<const uint8_t>(val));

    ASSERT_TRUE(fut.waitReady(kFutureTimeout))
        << "first produce future timed out";
    ASSERT_NO_THROW((void)fut.offset())
        << "first produce after connect failed";

    gwStop.store(true, std::memory_order_release);
    gwThread.join();
    client->close();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// TEST: ConnectThenImmediateFirstProduceTightLoop (the main regression gate)
//
// 200 iterations of: connect → producer() → send() with NO settle.
// Before the fix: crashes ~80% of runs or TSan reports DATA RACE on impls_.
// After the fix:  all 200 iterations pass cleanly (TSan clean).
// ===========================================================================
TEST(ProducerRace, ConnectThenImmediateFirstProduceTightLoop) {
    // Each iteration creates a fresh Client+gateway pair (own unique dataDir).
    // This ensures the race is exercised from a cold start every time.
    // Stream IDs are partitioned into non-overlapping ranges per iteration.
    static constexpr int kIterations = 200;

    int passed = 0;

    for (int iter = 0; iter < kIterations; ++iter) {
        // Use a unique stream-id pair per iteration to avoid cross-talk.
        const int32_t kIn = 3200 + iter * 2;
        const int32_t kEg = 3201 + iter * 2;

        std::string agentDir = uniqueRaceDir();
        std::string aeronDir = agentDir + "/aeron";

        ClientConfig cfg;
        cfg.gatewayChannel   = "aeron:ipc";
        cfg.gatewayStreamId  = kIn;
        cfg.egressChannel    = "aeron:ipc";
        cfg.egressStreamId   = kEg;
        cfg.dataDir          = agentDir;
        cfg.connectTimeoutMs = 8000;
        cfg.idleMode         = IdleMode::LowLatency; // hot runner maximises race window
        cfg.produceTimeoutMs = 8000;

        TopicSpec ts; ts.name = "events"; ts.topicId = 0; ts.partitionCount = 1;
        cfg.topics.push_back(ts);

        std::unique_ptr<Client>      client;
        std::unique_ptr<RaceGateway> gw;

        ASSERT_NO_THROW(client = std::make_unique<Client>(cfg))
            << "Client ctor threw at iter " << iter;

        // Give the embedded driver a moment to start.
        std::this_thread::sleep_for(100ms);

        ASSERT_NO_THROW(gw = std::make_unique<RaceGateway>(aeronDir, kIn, kEg))
            << "RaceGateway ctor threw at iter " << iter;

        // Gateway poll thread.
        std::atomic<bool> gwStop{false};
        std::thread gwThread([&]() {
            while (!gwStop.load(std::memory_order_acquire)) {
                gw->poll(16);
                std::this_thread::sleep_for(1ms);
            }
        });

        // 1. Connect.
        test_support::TestCq connFutCq(*client);
        auto connFut = connFutCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(connFut.waitReady())
            << "connect() timed out at iter " << iter;
        ASSERT_NO_THROW(connFut.check())
            << "connect() threw at iter " << iter;

        // 2. Immediately create a producer and send — NO sleep, NO settle.
        ProducerConfig pcfg;
        pcfg.acks             = Acks::Leader;
        pcfg.lingerUs         = 1;
        pcfg.batchBytes       = 1;
        pcfg.maxInFlight      = 4;
        pcfg.produceTimeoutMs = 8000;

        auto p = client->producer(pcfg); // RACE POINT: push_back while runner iterates

        test_support::TestCq cqP(*client);

        TopicHandle th{0, 1};
        Key key(uint64_t{static_cast<uint64_t>(iter)});
        const std::vector<uint8_t> val = {static_cast<uint8_t>(iter & 0xFF)};

        auto fut = cqP.send(p, th, key, std::span<const uint8_t>(val));

        // 3. Wait for the ack.
        ASSERT_TRUE(fut.waitReady(kFutureTimeout))
            << "first produce future timed out at iter " << iter;
        ASSERT_NO_THROW((void)fut.offset())
            << "first produce after connect failed at iter " << iter;

        // Teardown.
        gwStop.store(true, std::memory_order_release);
        gwThread.join();
        client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}

        ++passed;
    }

    EXPECT_EQ(passed, kIterations)
        << "Only " << passed << "/" << kIterations
        << " iterations passed (first-produce race still present)";
}
