// tests/producer/ProducerBatchingTest.cpp
//
// Gate-2 instrumentation test — framesSent/recordsSent counters + batchRecords knob.
//
// Test:
//   CoalescesRecordsIntoFewerFrames
//     640 records with batchRecords=64 → exactly 640 records reported by
//     recordsSent(), fewer than 640 frames reported by framesSent(), and the
//     coalescing ratio >= 8 records/frame.
//
// Harness reuses the ProducerTest.cpp / BatchBuilderTest.cpp pattern
// (AgentHarness + fake gateway driving doWork directly).
//
// Threading decision:
//   ProducerImpl::framesSent_ and recordsSent_ are plain uint64_t incremented
//   ONLY on the agent thread (single-writer).  We read them here after calling
//   p.flush() + pumpUntilBatches(), which synchronises because pumpUntilBatches
//   calls agent->doWork() in a loop and the agent thread is the same caller
//   (no background runner in this harness — doWork is driven from the test
//   thread).  No atomics needed; reading after the last doWork is safe.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>

#include <rune/protocol/PartitionHash.h>
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

namespace rcc   = rune::client::codec;
namespace rca   = rune::client::agent;
namespace rcs   = rune::client::session;
namespace rprod = rune::client::producer;
namespace rc    = rune::client;

using rc::Key;
using rc::TopicHandle;
using rc::Producer;
using rc::ProducerConfig;
using rc::Acks;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

static constexpr int  kPumpMax   = 5'000;
static constexpr auto kPumpSleep = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_batchDirCtr{0};

std::string uniqueDir(const char* prefix = "batching_test") {
    const int c = g_batchDirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/") + prefix + "_" +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}

int64_t monoNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// BatchGateway — minimal fake broker: handles Connect + Produce (acks=None).
// ---------------------------------------------------------------------------

struct BatchGateway {
    std::atomic<int>                              batchCount{0};
    std::shared_ptr<aeron::Aeron>                 gwAeron;
    std::shared_ptr<aeron::Subscription>          gwSub;
    std::shared_ptr<aeron::ExclusivePublication>  gwPub;
    std::unique_ptr<aeron::FragmentAssembler>     gwAsm;
    uint8_t replyBuf[2048]{};

    explicit BatchGateway(const std::string& aeronDir,
                          int32_t ingressStream,
                          int32_t egressStream)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("BatchGateway: connect failed");

        const int64_t subId = gwAeron->addSubscription("aeron:ipc", ingressStream);
        const int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("BatchGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~BatchGateway() {
        gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset();
    }

    int poll(int limit = 16) {
        if (!gwSub || !gwAsm) return 0;
        return gwSub->poll(gwAsm->handler(), limit);
    }

    bool gwSend(const uint8_t* buf, std::size_t len) {
        if (!gwPub) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
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

        if (tid == rcc::TemplateId::Produce) {
            batchCount.fetch_add(1, std::memory_order_release);
            // acks=None: no reply needed.
        }
    }
};

// ---------------------------------------------------------------------------
// BatchingHarness — wraps ClientAgent + BatchGateway.
// Tests drive doWork() directly (no background runner).
// ---------------------------------------------------------------------------

struct BatchingHarness {
    std::string agentDir;
    std::string aeronDir;

    std::unique_ptr<rca::ClientAgent> agent;
    std::unique_ptr<BatchGateway>     gw;

    rprod::ProducerHub    hub;
    std::atomic<int64_t>  nextCorrId_{1};

    explicit BatchingHarness(int32_t ingressStream, int32_t egressStream,
                              int32_t ringCap = 2 * 1024 * 1024)
    {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";

        rca::AgentConfig cfg;
        cfg.transport.ingressChannel  = "aeron:ipc";
        cfg.transport.ingressStreamId = ingressStream;
        cfg.transport.egressChannel   = "aeron:ipc";
        cfg.transport.egressStreamId  = egressStream;
        cfg.transport.dataDir         = agentDir;
        cfg.session.egressChannel     = "aeron:ipc";
        cfg.session.egressStreamId    = egressStream;
        cfg.session.keepaliveIntervalNs = 10'000'000'000LL;
        cfg.ringCapacity              = ringCap;

        agent = std::make_unique<rca::ClientAgent>(cfg);

        agent->setProducerDrainFn(
            [this](int64_t nowNs, int64_t sessionId,
                   rca::ClientAgent::SendFn sendFn) -> int {
                return hub.drainAndFlush(nowNs, sessionId, std::move(sendFn));
            });
        agent->setProduceResponseFn(
            [this](int64_t correlationId, int64_t baseOffset) {
                hub.onProduceResponse(correlationId, baseOffset);
            });
        agent->setEgressObserver([](rcc::TemplateId, const uint8_t*, std::size_t){});

        agent->start();
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<BatchGateway>(aeronDir, ingressStream, egressStream);
    }

    ~BatchingHarness() {
        gw.reset();
        if (agent) agent->stop();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }

    Producer makeProducer(ProducerConfig pcfg = {}) {
        auto impl = std::make_shared<rprod::ProducerImpl>(pcfg, nextCorrId_);
        hub.add(impl);
        return Producer(std::move(impl));
    }

    bool connect(int limit = kPumpMax) {
        agent->submitConnect();
        for (int i = 0; i < limit; ++i) {
            agent->doWork(monoNs());
            gw->poll();
            if (agent->sessionState() == rcs::SessionState::Connected)
                return true;
            std::this_thread::sleep_for(kPumpSleep);
        }
        return false;
    }

    bool pumpUntilBatches(int target, int limit = kPumpMax) {
        for (int i = 0; i < limit; ++i) {
            agent->doWork(monoNs());
            gw->poll();
            if (gw->batchCount.load(std::memory_order_acquire) >= target)
                return true;
            std::this_thread::sleep_for(kPumpSleep);
        }
        return false;
    }
};

static TopicHandle makeTopic(int32_t topicId, int32_t partitionCount) {
    return TopicHandle{topicId, partitionCount};
}

// ===========================================================================
// CoalescesRecordsIntoFewerFrames
//
// 640 records with lingerUs=1000 (1 ms) and batchRecords=64 should produce
// exactly 10 frames (640 / 64 = 10) and report recordsSent()==640,
// framesSent() < 640, and recordsSent()/framesSent() >= 8.
//
// Design: acks=None, single partition (1-partition topic + fixed key so all
// 640 records go to partition 0), all sends queued before doWork is called.
// The batchRecords trigger fires after every 64 records are accumulated on
// the agent thread, producing exactly 10 frames.
// ===========================================================================
TEST(ProducerBatching, CoalescesRecordsIntoFewerFrames) {
    static constexpr int32_t kInStr = 2043, kEgStr = 2044;

    BatchingHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect to gateway";

    ProducerConfig cfg;
    cfg.acks         = Acks::None;
    cfg.lingerUs     = 1'000'000'000LL; // 1000 s — will NOT fire during test
    cfg.batchBytes   = 64 * 1024 * 1024; // 64 MB — will NOT trigger on size alone
    cfg.batchRecords = 64;              // flush every 64 records
    cfg.maxInFlight  = 64;

    auto topic = makeTopic(100, 1);  // 1 partition: all records go to partition 0
    Key  key(uint64_t{1});

    // 32-byte payload per record (small — ensures ring and transport headroom).
    static const std::vector<uint8_t> kPayload(32, 0xAB);

    auto p = h.makeProducer(cfg);

    // Queue all 640 records into the ring before calling doWork.
    // No p.flush() — we rely solely on batchRecords=64 as the flush trigger.
    // Calling flush() would set globalFlushPending_=true immediately, causing
    // every record to flush individually (1 frame per record) instead of
    // coalescing 64 records per frame.
    for (int i = 0; i < 640; ++i) {
        p.send(topic, key, std::span<const uint8_t>(kPayload));
    }

    // Wait for all 10 batches (640 / 64 = 10).
    // Each batch flushes after 64 records accumulate (batchRecords=64 trigger).
    // With limit=1 drain per doWork call, this requires 640 doWork iterations
    // (one record drain per iteration, flush every 64th).
    ASSERT_TRUE(h.pumpUntilBatches(10))
        << "Expected 10 frames (640 records / 64 per batch) to be delivered";

    // Pump a few more iterations to ensure all counters are settled.
    for (int i = 0; i < 50; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    // --- Counter assertions ---
    const uint64_t frames  = p.framesSent();
    const uint64_t records = p.recordsSent();

    EXPECT_EQ(records, 640u)
        << "recordsSent() must equal total records enqueued";

    EXPECT_LT(frames, 640u)
        << "framesSent() must be < recordsSent() (coalescing occurred)";

    ASSERT_GT(frames, 0u)
        << "framesSent() must be > 0 (at least one frame sent)";

    const uint64_t ratio = records / frames;
    EXPECT_GE(ratio, 8u)
        << "records/frame ratio must be >= 8 (expected ~64 per batch); "
        << "got records=" << records << " frames=" << frames;
}

// ===========================================================================
// ProducerConfigFromEnv — Task 1: linger=0 must be honored
// ===========================================================================

TEST(ProducerConfigFromEnv, LingerZeroIsHonored) {
    ::setenv("RUNE_LINGER_US", "0", 1);
    auto cfg = rune::client::ProducerConfig::fromEnv();
    ::unsetenv("RUNE_LINGER_US");
    EXPECT_EQ(cfg.lingerUs, 0);   // 0 must be honored, not treated as "unset"
}
TEST(ProducerConfigFromEnv, LingerUnsetKeepsDefault) {
    ::unsetenv("RUNE_LINGER_US");
    auto cfg = rune::client::ProducerConfig::fromEnv();
    EXPECT_EQ(cfg.lingerUs, 5000);
}

// ===========================================================================
// ProducerConfigFromEnv — Task 2: maxFrameBytes / deliveryTimeoutMs
// ===========================================================================

TEST(ProducerConfigFromEnv, FrameCapAndDeliveryTimeoutDefaults) {
    ::unsetenv("RUNE_MAX_FRAME_BYTES"); ::unsetenv("RUNE_DELIVERY_TIMEOUT_MS");
    auto cfg = rune::client::ProducerConfig::fromEnv();
    EXPECT_EQ(cfg.maxFrameBytes, 61440);
    EXPECT_EQ(cfg.deliveryTimeoutMs, 30000);
}
TEST(ProducerConfigFromEnv, FrameCapOverride) {
    ::setenv("RUNE_MAX_FRAME_BYTES", "16384", 1);
    auto cfg = rune::client::ProducerConfig::fromEnv();
    ::unsetenv("RUNE_MAX_FRAME_BYTES");
    EXPECT_EQ(cfg.maxFrameBytes, 16384);
}

// ===========================================================================
// EnvKnobsApplied
//
// ProducerConfig::fromEnv() correctly reads RUNE_LINGER_US, RUNE_BATCH_BYTES,
// RUNE_BATCH_RECORDS from environment variables.  Tested without a running
// agent: only the config struct is inspected.
// ===========================================================================
TEST(ProducerBatching, EnvKnobsApplied) {
    // Stash current env values (so we can restore them — avoid leaking).
    // setenv/unsetenv are POSIX.
    const bool hadLinger  = (std::getenv("RUNE_LINGER_US")     != nullptr);
    const bool hadBytes   = (std::getenv("RUNE_BATCH_BYTES")   != nullptr);
    const bool hadRecords = (std::getenv("RUNE_BATCH_RECORDS") != nullptr);

    const std::string oldLinger  = hadLinger  ? std::getenv("RUNE_LINGER_US")     : "";
    const std::string oldBytes   = hadBytes   ? std::getenv("RUNE_BATCH_BYTES")   : "";
    const std::string oldRecords = hadRecords ? std::getenv("RUNE_BATCH_RECORDS") : "";

    // Set test values.
    ::setenv("RUNE_LINGER_US",     "12345",  1);
    ::setenv("RUNE_BATCH_BYTES",   "98765",  1);
    ::setenv("RUNE_BATCH_RECORDS", "32",     1);

    const ProducerConfig cfg = ProducerConfig::fromEnv();

    EXPECT_EQ(cfg.lingerUs,     12345LL) << "RUNE_LINGER_US must override lingerUs";
    EXPECT_EQ(cfg.batchBytes,   98765)   << "RUNE_BATCH_BYTES must override batchBytes";
    EXPECT_EQ(cfg.batchRecords, 32)      << "RUNE_BATCH_RECORDS must override batchRecords";

    // Restore env.
    if (hadLinger)  ::setenv("RUNE_LINGER_US",     oldLinger.c_str(),  1);
    else            ::unsetenv("RUNE_LINGER_US");
    if (hadBytes)   ::setenv("RUNE_BATCH_BYTES",   oldBytes.c_str(),   1);
    else            ::unsetenv("RUNE_BATCH_BYTES");
    if (hadRecords) ::setenv("RUNE_BATCH_RECORDS", oldRecords.c_str(), 1);
    else            ::unsetenv("RUNE_BATCH_RECORDS");
}
