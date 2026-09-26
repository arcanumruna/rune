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
    // I3: default maxFrameBytes == default batchBytes (65536) → frameCapped_=false
    // → frame-cap pre-flight collapses to zero cost on the default config.
    EXPECT_EQ(cfg.maxFrameBytes, 65536);
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

// ===========================================================================
// Task 3: Cap per-frame wire size — split oversize batches
//
// Tests:
//   LargeBatchSplitsIntoCappedFrames
//     2000 records × 256 bytes ≈ 512 KiB into one partition with
//     batchBytes=1 MiB and maxFrameBytes=16 KiB. Expects ≥32 frames,
//     each RecordBatch ≤ 16384 bytes, and total 2000 records preserved.
//
//   FrameAtCapBoundaryEmitsOne_PlusOneEmitsTwo
//     Compute the exact wire size of 64 × 256B records.  Produce exactly
//     that many records (one frame) then one more (two frames).
//
// Gateway design:
//   CapGateway extends BatchGateway's Aeron setup pattern but captures
//   per-frame RecordBatch byte counts and record counts for assertions.
// ===========================================================================

namespace {

// ---------------------------------------------------------------------------
// CapGateway — like BatchGateway but captures per-frame RecordBatch info.
// Not thread-safe: gwAsm handler runs on the test thread (via gw->poll()).
// ---------------------------------------------------------------------------

struct FrameInfo {
    int32_t rbWireBytes = 0;  // RecordBatch portion of the Produce frame
    int32_t recordCount  = 0;  // records decoded from the RecordBatch header
};

struct CapGateway {
    std::atomic<int>                              batchCount{0};
    std::vector<FrameInfo>                        frames;   // guarded by test thread
    std::shared_ptr<aeron::Aeron>                 gwAeron;
    std::shared_ptr<aeron::Subscription>          gwSub;
    std::shared_ptr<aeron::ExclusivePublication>  gwPub;
    std::unique_ptr<aeron::FragmentAssembler>     gwAsm;
    uint8_t replyBuf[2048]{};

    explicit CapGateway(const std::string& aeronDir,
                        int32_t ingressStream,
                        int32_t egressStream)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("CapGateway: connect failed");

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
            throw std::runtime_error("CapGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~CapGateway() {
        gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset();
    }

    int poll(int limit = 32) {
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
            const auto pv = rcc::decodeProduce(buf, len);
            if (!pv) return;

            FrameInfo fi;
            fi.rbWireBytes = static_cast<int32_t>(pv->recordBatch.size());

            // Decode record count from the RecordBatch header.
            if (pv->recordBatch.size() >=
                    static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
                rune::protocol::RecordBatchReader reader(
                    pv->recordBatch.data(), pv->recordBatch.size());
                fi.recordCount = reader.recordCount();
            }
            frames.push_back(fi);
        }
    }

    int totalRecords() const {
        int n = 0;
        for (const auto& f : frames) n += f.recordCount;
        return n;
    }
};

// ---------------------------------------------------------------------------
// CapHarness — mirrors BatchingHarness but uses CapGateway.
// ---------------------------------------------------------------------------

struct CapHarness {
    std::string agentDir;
    std::string aeronDir;

    std::unique_ptr<rca::ClientAgent> agent;
    std::unique_ptr<CapGateway>       gw;

    rprod::ProducerHub    hub;
    std::atomic<int64_t>  nextCorrId_{1};

    explicit CapHarness(int32_t ingressStream, int32_t egressStream,
                        int32_t ringCap = 4 * 1024 * 1024)
    {
        agentDir = uniqueDir("cap_test");
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

        gw = std::make_unique<CapGateway>(aeronDir, ingressStream, egressStream);
    }

    ~CapHarness() {
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

    // Pump until gw->batchCount >= target, then drain remaining frames.
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

} // anonymous namespace

// ---------------------------------------------------------------------------
// LargeBatchSplitsIntoCappedFrames
//
// batchBytes=1 MiB, maxFrameBytes=16 KiB.  Produce 2000 × 256B records
// into 1 partition.  Wire total ≈ 512 KiB.
//
// Expected (post-implementation):
//   - At least 32 frames (512 KiB / 16 KiB).
//   - Each frame's RecordBatch ≤ 16384 bytes.
//   - Total records across all frames == 2000.
//
// Current (pre-implementation): ONE frame of ≈512 KiB → test fails.
// ---------------------------------------------------------------------------
TEST(ProducerFrameCap, LargeBatchSplitsIntoCappedFrames) {
    static constexpr int32_t kInStr = 2051, kEgStr = 2052;
    static constexpr int32_t kMaxFrameBytes = 16384;
    static constexpr int      kRecords      = 2000;
    static constexpr int      kValBytes     = 256;

    CapHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect to cap-gateway";

    ProducerConfig cfg;
    cfg.acks          = Acks::None;
    cfg.batchBytes    = 1 << 20;        // 1 MiB logical — much larger than cap
    cfg.maxFrameBytes = kMaxFrameBytes; // 16 KiB per-frame cap
    cfg.lingerUs      = 5'000;          // short linger so size trigger dominates
    cfg.batchRecords  = 0;
    cfg.maxInFlight   = 64;

    auto topic = makeTopic(200, 1);
    Key  key(uint64_t{1});

    const std::vector<uint8_t> payload(kValBytes, 0xAB);
    auto p = h.makeProducer(cfg);

    for (int i = 0; i < kRecords; ++i) {
        p.send(topic, key, std::span<const uint8_t>(payload));
    }
    p.flush();

    // Rough minimum frame count: total wire / cap.
    // Each frame overhead = kRecordBatchOverhead = 61 bytes; payload per record ≈ 270B wire.
    // We expect ≥ 32 frames for 2000 × 256B.
    const int minFrames = (kRecords * (kValBytes + 20)) / kMaxFrameBytes;

    ASSERT_TRUE(h.pumpUntilBatches(minFrames))
        << "Timed out waiting for " << minFrames << " frames";

    // Drain any remaining frames.
    for (int i = 0; i < 200; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    const auto& frames = h.gw->frames;
    ASSERT_GE(static_cast<int>(frames.size()), minFrames)
        << "Expected >= " << minFrames << " frames, got " << frames.size();

    for (std::size_t i = 0; i < frames.size(); ++i) {
        EXPECT_LE(frames[i].rbWireBytes, kMaxFrameBytes)
            << "Frame " << i << " RecordBatch wire bytes " << frames[i].rbWireBytes
            << " exceeds cap " << kMaxFrameBytes;
    }

    EXPECT_EQ(h.gw->totalRecords(), kRecords)
        << "Total records across all frames must equal " << kRecords
        << " (got " << h.gw->totalRecords() << ")";
}

// ---------------------------------------------------------------------------
// FrameAtCapBoundaryEmitsOnePerFrame
//
// Set maxFrameBytes to a cap that fits exactly ONE record of 256B value +
// 8B key (kRecordBatchOverhead + one record body ≈ 335-345 bytes at typical
// runtime tsDelta).  Use maxFrameBytes=400 which fits 1 record and NOT 2.
//
// Send 3 records into 1 partition: expect exactly 3 frames (one record each).
// Each frame's RecordBatch wire bytes must be ≤ 400.
//
// This validates:
//   1. The frame cap splits correctly at tight boundaries.
//   2. No record is dropped (total records = 3).
//   3. Each emitted frame is within the cap.
// ---------------------------------------------------------------------------
TEST(ProducerFrameCap, FrameAtCapBoundaryEmitsOnePerFrame) {
    static constexpr int32_t kInStr    = 2053, kEgStr = 2054;
    static constexpr int32_t kCap      = 400;   // fits 1 × 256B record, not 2
    static constexpr int      kValBytes = 256;
    static constexpr int      kRecords  = 3;

    CapHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect (boundary test)";

    ProducerConfig cfg;
    cfg.acks          = Acks::None;
    cfg.batchBytes    = 1 << 20;    // large — size trigger comes from maxFrameBytes
    cfg.maxFrameBytes = kCap;
    cfg.lingerUs      = 1'000'000;  // 1 s — linger must NOT fire during test
    cfg.batchRecords  = 0;
    cfg.maxInFlight   = 64;

    auto topic = makeTopic(203, 1);
    Key  key(uint64_t{1});
    const std::vector<uint8_t> payload(kValBytes, 0xAB);
    auto p = h.makeProducer(cfg);

    for (int i = 0; i < kRecords; ++i)
        p.send(topic, key, std::span<const uint8_t>(payload));
    p.flush();

    ASSERT_TRUE(h.pumpUntilBatches(kRecords))
        << "Timed out waiting for " << kRecords << " frames";

    // Extra pump to catch any stray frames.
    for (int i = 0; i < 100; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    const auto& frames = h.gw->frames;

    // Each 256B record + 8B key fits in ~335-345 bytes wire; two would exceed kCap=400.
    EXPECT_EQ(static_cast<int>(frames.size()), kRecords)
        << "Each record must be in its own frame (got " << frames.size() << " frames)";

    for (std::size_t i = 0; i < frames.size(); ++i) {
        EXPECT_LE(frames[i].rbWireBytes, kCap)
            << "Frame " << i << " rbWireBytes=" << frames[i].rbWireBytes
            << " exceeds cap=" << kCap;
        EXPECT_EQ(frames[i].recordCount, 1)
            << "Frame " << i << " should have exactly 1 record";
    }

    EXPECT_EQ(h.gw->totalRecords(), kRecords)
        << "All " << kRecords << " records must be preserved";
}

// ---------------------------------------------------------------------------
// ByteExactBoundaryNoOvershoot  (M1 regression for C1 — varintLen zigzag fix)
//
// Strategy: two-stage.  The 65th record in a batch has offsetDelta=64.
//
// The C1 bug: varintLen(64) uses PLAIN unsigned varint (1 byte), but
// writeVarint(64) in RecordBatch.h uses ZIGZAG (zigzag(64)=128 → 2 bytes).
// This under-counts offsetDelta by 1 byte.  Additionally, the buggy
// varintLen(bodySize) is wrong when bodySize crosses a zigzag boundary:
// if buggy bodySize_buggy=66 (plain 1 byte) but correct bodySize_correct=67
// (zigzag(67)=134 → 2 bytes), the total under-count is 2 bytes.
//
// The pre-flight has a +1 byte slack built in (line: "+1 // wallNs() jitter").
// The 1-byte slack masks a 1-byte discrepancy, so we need a 2-byte discrepancy
// to bypass it.  This is achieved by choosing valueLen=57, keyLen=0 (empty key,
// from Key{}) — at offsetDelta=64, tsDelta≈4 bytes (k*2ms record k=64 → 128ms
// → zigzag(256M) → 4 bytes):
//   bodySize_buggy  = 1+4+varintLen_plain(64)+1+0+varintLen_plain(57)+57+1 = 66
//   bodySize_correct= 1+4+varintLen_zigzag(64)+1+0+varintLen_zigzag(57)+57+1 = 67
//   contrib_buggy   = varintLen_plain(66)  + 66 = 1+66 = 67
//   contrib_correct = varintLen_zigzag(67) + 67 = 2+67 = 69
//   discrepancy = 2 bytes > slack = 1 byte → bug bypasses slack ✓
//
// Stage 1: Produce 65 records (kN+1=65) with unlimited cap + batchRecords=65.
//   Captures single frame rbWireBytes = S65 (actual correct zigzag encoding).
// Stage 2: cap = S65-1 and batchRecords=65.  Produce 65 records.
//   Pre-fix (buggy varintLen): builderBytes_64 + (contrib_buggy+1) = S65-1+1-1 = cap-1
//     → NOT > cap → record ALLOWED → frame has 65 records, rbWireBytes = S65 > cap.
//     ASSERTION FAILS: rbWireBytes > cap.
//   Post-fix (correct varintLen): builderBytes_64 + contrib_correct = S65 > cap → ABORT.
//     2 frames, each ≤ cap.  ASSERTION PASSES.
//
// Note: effective check is (builderBytes + contrib + 1 > cap) due to the slack.
// For buggy code: contrib_buggy=67; 67+1=68. builderBytes_64 = S65-69.
//   (S65-69)+68 = S65-1 = cap → NOT > cap → allowed ✓ (bug exposed)
// For correct code: contrib_correct=69. (S65-69)+69 = S65 > cap → ABORT ✓
//
// Stream ids: 2059/2060 (Stage 1), 2061/2062 (Stage 2).
// ---------------------------------------------------------------------------
TEST(ProducerFrameCap, ByteExactBoundaryNoOvershoot) {
    static constexpr int kN   = 64;   // target records per frame; 65th has offsetDelta=64
    static constexpr int kVal = 57;   // valueLen chosen so bodySize_buggy=66 (2-byte discrepancy)
    // Key{}: empty key (keyLen=0) — required for the bodySize=66/67 computation above.

    // -- Stage 1: measure actual wire size of kN+1=65 records (no cap) ---------
    int32_t S65 = 0;
    {
        static constexpr int32_t kIn1 = 2059, kEg1 = 2060;
        CapHarness h(kIn1, kEg1);
        ASSERT_TRUE(h.connect()) << "Stage1: failed to connect";

        ProducerConfig cfg;
        cfg.acks          = Acks::None;
        cfg.batchBytes    = 1 << 20;        // 1 MiB — far beyond any frame
        cfg.maxFrameBytes = 1 << 20;        // same → frameCapped_=false → no pre-flight
        cfg.lingerUs      = 600'000'000LL;  // 10 min — must NOT fire
        cfg.batchRecords  = kN + 1;         // flush exactly at 65 records
        cfg.maxInFlight   = 64;

        auto topic = makeTopic(301, 1);
        Key  key{};  // empty key: keyLen=0 (required for the 2-byte discrepancy)
        const std::vector<uint8_t> payload(kVal, 0x42);
        auto p = h.makeProducer(cfg);

        for (int i = 0; i < kN + 1; ++i)
            p.send(topic, key, std::span<const uint8_t>(payload));
        // NO p.flush(): batchRecords=65 is the sole flush trigger.

        ASSERT_TRUE(h.pumpUntilBatches(1))
            << "Stage1: timed out waiting for 1 frame of " << (kN+1) << " records";

        for (int i = 0; i < 50; ++i) {
            h.agent->doWork(monoNs());
            h.gw->poll();
            std::this_thread::sleep_for(kPumpSleep);
        }

        ASSERT_EQ(static_cast<int>(h.gw->frames.size()), 1)
            << "Stage1: expected exactly 1 frame for " << (kN+1) << " records";
        ASSERT_EQ(h.gw->frames[0].recordCount, kN + 1)
            << "Stage1: frame must contain exactly " << (kN+1) << " records";

        S65 = h.gw->frames[0].rbWireBytes;
        ASSERT_GT(S65, 0) << "Stage1: measured wire size must be > 0";
        std::fprintf(stderr, "[M1] Stage1: S65=%d (wire size of 65 records)\n", S65);
    }

    // -- Stage 2: cap = S65-1, batchRecords=65 — the C1 regression check -------
    // Pre-fix:  65th record ALLOWED (2-byte discrepancy exceeds 1-byte slack)
    //           → 1 frame with rbWireBytes = S65 = cap+1 > cap → FAILS assertion.
    // Post-fix: 65th record ABORTed → 2 frames, both ≤ cap → PASSES.
    {
        static constexpr int32_t kIn2 = 2061, kEg2 = 2062;
        const int32_t cap = S65 - 1;

        CapHarness h(kIn2, kEg2);
        ASSERT_TRUE(h.connect()) << "Stage2: failed to connect";

        ProducerConfig cfg;
        cfg.acks          = Acks::None;
        cfg.batchBytes    = 1 << 20;
        cfg.maxFrameBytes = cap;            // exact boundary: S65-1
        cfg.lingerUs      = 600'000'000LL;  // 10 min — must NOT fire
        cfg.batchRecords  = kN + 1;         // flush trigger at exactly 65 records
        cfg.maxInFlight   = 64;

        auto topic = makeTopic(302, 1);
        Key  key{};  // empty key: keyLen=0
        const std::vector<uint8_t> payload(kVal, 0x42);
        auto p = h.makeProducer(cfg);

        for (int i = 0; i < kN + 1; ++i)
            p.send(topic, key, std::span<const uint8_t>(payload));
        // NO p.flush().

        // Wait for at least 1 frame (buggy: 1 frame with 65 records; correct: 2 frames).
        ASSERT_TRUE(h.pumpUntilBatches(1))
            << "Stage2: timed out (cap=" << cap << ")";

        // With correct code, frame 1 has 64 records and record 65 is queued in a
        // new builder waiting for batchRecords=65 or linger.  Flush explicitly so
        // both frames arrive before we assert.  With buggy code, frame 1 already
        // has 65 records, so this flush is a no-op (empty builder).
        p.flush();
        for (int i = 0; i < 50; ++i) {
            h.agent->doWork(monoNs());
            h.gw->poll();
            std::this_thread::sleep_for(kPumpSleep);
        }

        const auto& frames = h.gw->frames;
        std::fprintf(stderr, "[M1] Stage2: cap=%d S65=%d frames=%zu total_records=%d\n",
                     cap, S65, frames.size(), h.gw->totalRecords());
        for (std::size_t i = 0; i < frames.size(); ++i) {
            std::fprintf(stderr, "[M1] Stage2 frame[%zu]: rbWireBytes=%d recordCount=%d\n",
                         i, frames[i].rbWireBytes, frames[i].recordCount);
        }

        ASSERT_GE(static_cast<int>(frames.size()), 1)
            << "Stage2: no frames received";

        // C1 regression: if varintLen uses plain (buggy), the 65th record's
        // contribution is under-estimated by 2 bytes, bypassing the 1-byte slack,
        // allowing the 65th record into frame 1 → rbWireBytes = S65 = cap+1 > cap.
        for (std::size_t i = 0; i < frames.size(); ++i) {
            EXPECT_LE(frames[i].rbWireBytes, cap)
                << "Stage2 frame " << i << " rbWireBytes=" << frames[i].rbWireBytes
                << " exceeds cap=" << cap << " (S65=" << S65 << ")"
                << " — C1 REGRESSION: varintLen must use zigzag (plain under-counts"
                << " offsetDelta=64 and bodySize by 1 byte each, bypassing +1 slack)";
        }
        EXPECT_EQ(h.gw->totalRecords(), kN + 1)
            << "Stage2: all " << (kN+1) << " records must be preserved";
    }
}

// ---------------------------------------------------------------------------
// FrameCapDoesNotSplitWhenBelowCap
//
// Verify the normal path: when maxFrameBytes >= batchBytes (or effectively
// when batchBytes is the tighter constraint), no split occurs and records
// coalesce normally.  Uses batchRecords=5 to flush every 5 records with
// maxFrameBytes=1MiB (larger than any real batch).
// ---------------------------------------------------------------------------
TEST(ProducerFrameCap, FrameCapDoesNotSplitWhenBelowCap) {
    static constexpr int32_t kInStr = 2057, kEgStr = 2058;

    CapHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect (no-split test)";

    ProducerConfig cfg;
    cfg.acks          = Acks::None;
    cfg.batchBytes    = 64 * 1024;
    cfg.maxFrameBytes = 1 << 20;  // 1 MiB — much larger than batchBytes → no frame split
    cfg.lingerUs      = 1'000'000;
    cfg.batchRecords  = 5;        // flush every 5 records
    cfg.maxInFlight   = 64;

    auto topic = makeTopic(204, 1);
    Key  key(uint64_t{1});
    const std::vector<uint8_t> payload(32, 0xCD);  // small payload, 5 easily fit
    auto p = h.makeProducer(cfg);

    for (int i = 0; i < 15; ++i)  // 3 batches of 5
        p.send(topic, key, std::span<const uint8_t>(payload));

    ASSERT_TRUE(h.pumpUntilBatches(3))
        << "Timed out waiting for 3 frames";

    for (int i = 0; i < 50; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    EXPECT_EQ(static_cast<int>(h.gw->frames.size()), 3)
        << "With batchRecords=5 and 15 records, expected 3 frames (got "
        << h.gw->frames.size() << ")";
    EXPECT_EQ(h.gw->totalRecords(), 15)
        << "All 15 records must be preserved";
}
