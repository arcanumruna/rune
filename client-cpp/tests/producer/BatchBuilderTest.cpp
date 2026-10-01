// tests/producer/BatchBuilderTest.cpp
//
// CB-3 Acceptance tests — Producer Tier-2 (explicit atomic batch).
//
// Test matrix:
//   1.  AtomicBatchContiguousOffsets  — batch(topic,key) + add*K + send() → future
//                                       resolves with base; broker receives ONE
//                                       RecordBatch of K records at contiguous
//                                       offsets base..base+K-1 (one append).
//   2.  SingleAppendAtomicity         — K records appear in ONE batch, not split.
//   3.  EmptyBatchRejected            — send() with 0 records → std::invalid_argument.
//   4.  AddAfterSendGuard             — add() after send() → std::logic_error.
//   5.  BinaryValues                  — NUL+0xFF values survive round-trip; large-ish
//                                       batch (multiple records) works correctly.
//   6.  Tier1Tier2SamePartitionOrdered — Tier-1 send then Tier-2 batch to same
//                                        partition → both succeed, correct ordered
//                                        offsets (T1 before T2).
//   7.  IdempotentSeqContinuity       — baseSequence advances monotonically, gap-free
//                                       across Tier-1 and Tier-2 batches for same partition.
//
// Harness reuses the ProducerTest.cpp AgentHarness / ProduceGateway pattern.
// Bounded, unique temp dirs, injected clock where relevant, no busy-spin.

#include "../support/AeronTestErrors.h"
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/BatchBuilder.h>
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
#include <future>
#include <memory>
#include <set>
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
using rc::BatchBuilder;
using rc::ProducerConfig;
using rc::Acks;
using rc::want_future;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

static constexpr int  kPumpMax   = 5'000;
static constexpr auto kPumpSleep = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_cb3DirCtr{0};

std::string uniqueDir(const char* prefix = "cb3_test") {
    const int c = g_cb3DirCtr.fetch_add(1, std::memory_order_relaxed);
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
// ProduceGateway — fake broker (shared design with ProducerTest.cpp).
// ---------------------------------------------------------------------------

struct CB3Gateway {
    struct BatchRecord {
        int32_t topicId   = 0;
        int32_t partition = 0;
        int64_t corrId    = 0;
        int32_t baseSeq   = -1;
        int32_t recCount  = 0;
        std::vector<std::string> values;
    };

    std::vector<BatchRecord> batches;
    std::atomic<int>         batchCount {0};
    int64_t                  nextOffset = 1000;

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;

    uint8_t replyBuf[2048]{};

    explicit CB3Gateway(const std::string& aeronDir,
                         int32_t ingressStream,
                         int32_t egressStream)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("CB3Gateway: connect failed");

        int64_t subId = gwAeron->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("CB3Gateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~CB3Gateway() {
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

    void sendAck(int32_t partition, int32_t recCount,
                 int64_t sessionId, int64_t corrId) {
        const int64_t baseOff = nextOffset;
        nextOffset += recCount;
        std::size_t n = rcc::encodeProduceResponse(
            replyBuf, sizeof(replyBuf),
            /*errorCode=*/0, partition, baseOff, sessionId, corrId);
        if (n > 0) gwSend(replyBuf, n);
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
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;

        auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;

        BatchRecord rec;
        rec.topicId   = opt->topicId;
        rec.partition = opt->partition;
        rec.corrId    = opt->correlationId;

        const auto& rb = opt->recordBatch;
        if (rb.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            const uint8_t* d = rb.data();
            rec.baseSeq = static_cast<int32_t>(
                static_cast<uint32_t>(d[53])        |
               (static_cast<uint32_t>(d[54]) <<  8) |
               (static_cast<uint32_t>(d[55]) << 16) |
               (static_cast<uint32_t>(d[56]) << 24));
            rec.recCount = static_cast<int32_t>(
                static_cast<uint32_t>(d[57])        |
               (static_cast<uint32_t>(d[58]) <<  8) |
               (static_cast<uint32_t>(d[59]) << 16) |
               (static_cast<uint32_t>(d[60]) << 24));

            // CB-5 A3: assert RecordBatch CRC and bounds are valid on the gateway
            // side — ensures the producer encodes a well-formed batch.
            rune::protocol::RecordBatchReader reader(rb.data(), rb.size());
            ASSERT_TRUE(reader.ensureValid())
                << "CB3Gateway: RecordBatch failed ensureValid() (CRC or bounds error)";

            reader.forEach([&](std::string_view /*key*/, std::string_view val,
                               int64_t /*off*/, int64_t /*ts*/) {
                rec.values.emplace_back(val.data(), val.size());
            });
        }

        batches.push_back(rec);
        batchCount.fetch_add(1, std::memory_order_release);

        if (opt->correlationId == 0) return; // acks=None

        sendAck(opt->partition, rec.recCount, opt->sessionId, opt->correlationId);
    }
};

// ---------------------------------------------------------------------------
// CB3AgentHarness — drives ClientAgent + CB3Gateway.
// ---------------------------------------------------------------------------

struct CB3AgentHarness {
    std::string agentDir;
    std::string aeronDir;

    std::unique_ptr<rca::ClientAgent> agent;
    std::unique_ptr<CB3Gateway>       gw;

    rprod::ProducerHub    hub;

    explicit CB3AgentHarness(int32_t ingressStream, int32_t egressStream,
                              int32_t ringCap = 512 * 1024)
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

        gw = std::make_unique<CB3Gateway>(aeronDir, ingressStream, egressStream);
    }

    ~CB3AgentHarness() {
        gw.reset();
        if (agent) agent->stop();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }

    Producer makeProducer(ProducerConfig pcfg = {}) {
        auto impl = std::make_shared<rprod::ProducerImpl>(pcfg);
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
// Test 1: AtomicBatchContiguousOffsets
//   batch(topic,key) + add*K + send() → future resolves with base;
//   broker receives ONE RecordBatch of K records.
//   Records land at contiguous offsets base..base+K-1.
// ===========================================================================
TEST(BatchBuilder, AtomicBatchContiguousOffsets) {
    static constexpr int32_t kInStr = 3001, kEgStr = 3002;
    static constexpr int kK = 5;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 5'000'000; // 5 s — no accidental Tier-1 flush
    pcfg.batchBytes  = 64 * 1024;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(20, 1); // single partition for simplicity
    Key  key(uint64_t{99});

    auto bb = p.batch(topic, key);
    const std::vector<uint8_t> val = {0x01, 0x02, 0x03};
    for (int i = 0; i < kK; ++i) {
        bb.add(std::span<const uint8_t>(val));
    }
    auto fut = bb.send();

    // Pump until the future resolves.
    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (fut.wait_for(0s) == std::future_status::ready) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done) << "Tier-2 future must resolve within timeout";
    const int64_t base = fut.get();
    EXPECT_GE(base, 1000LL) << "baseOffset must be gateway-assigned";

    // Verify broker received exactly ONE RecordBatch with K records.
    ASSERT_EQ(h.gw->batches.size(), 1u)
        << "Tier-2 must produce exactly ONE RecordBatch (one append)";
    const auto& b0 = h.gw->batches[0];
    EXPECT_EQ(b0.recCount, kK)
        << "RecordBatch must contain exactly K=" << kK << " records";

    // Contiguous offsets: the gateway's nextOffset before ack was base, after = base+K.
    // The baseOffset returned by future must equal the batch's base.
    EXPECT_EQ(b0.topicId, 20);
    EXPECT_EQ(b0.partition, 0);

    // Records land at base..base+K-1 — gateway assigns base = nextOffset before ack.
    // We already have base = fut.get(); verify gateway advanced by K.
    // (nextOffset was 1000 before this batch; after ack it is 1000+K.)
    EXPECT_EQ(h.gw->nextOffset, base + static_cast<int64_t>(kK));
}

// ===========================================================================
// Test 2: SingleAppendAtomicity
//   K records appear in ONE batch, NOT split across multiple RecordBatches.
// ===========================================================================
TEST(BatchBuilder, SingleAppendAtomicity) {
    static constexpr int32_t kInStr = 3003, kEgStr = 3004;
    static constexpr int kK = 8;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 5'000'000; // large linger — no Tier-1 flush
    pcfg.batchBytes  = 1;         // Tier-1 batchBytes=1: any Tier-1 record auto-flushes
    pcfg.maxInFlight = 16;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(21, 1);
    Key  key(uint64_t{0});

    auto bb = p.batch(topic, key);
    const std::vector<uint8_t> val(20, 0xCC);
    for (int i = 0; i < kK; ++i) {
        bb.add(std::span<const uint8_t>(val));
    }
    auto fut = bb.send();

    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (fut.wait_for(0s) == std::future_status::ready) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(done) << "Future must resolve";

    // The K records must arrive in exactly ONE batch (atomic single append).
    EXPECT_EQ(h.gw->batches.size(), 1u)
        << "Tier-2: all K records must be in ONE RecordBatch, not split";
    ASSERT_GE(h.gw->batches.size(), 1u);
    EXPECT_EQ(h.gw->batches[0].recCount, kK)
        << "RecordBatch must carry all K records";
}

// ===========================================================================
// Test 3: EmptyBatchRejected
//   send() with 0 records → std::invalid_argument.
// ===========================================================================
TEST(BatchBuilder, EmptyBatchRejected) {
    static constexpr int32_t kInStr = 3005, kEgStr = 3006;
    CB3AgentHarness h(kInStr, kEgStr);
    // No connect needed — guard fires before any network call.

    ProducerConfig pcfg;
    pcfg.acks = Acks::Leader;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(22, 1);
    Key  key(uint64_t{0});

    auto bb = p.batch(topic, key);
    // No add() calls → empty batch.

    EXPECT_THROW(bb.send(), std::invalid_argument)
        << "send() with 0 records must throw std::invalid_argument";

    // Confirm no batch was submitted to the broker.
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(h.gw->batchCount.load(std::memory_order_acquire), 0)
        << "Empty batch must not produce any Produce message";
}

// ===========================================================================
// Test 4: AddAfterSendGuard
//   add() after send() → std::logic_error.
// ===========================================================================
TEST(BatchBuilder, AddAfterSendGuard) {
    static constexpr int32_t kInStr = 3007, kEgStr = 3008;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;
    pcfg.batchBytes  = 1;
    pcfg.maxInFlight = 4;
    pcfg.produceTimeoutMs = 1000;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(23, 1);
    Key  key(uint64_t{0});

    auto bb = p.batch(topic, key);
    const std::vector<uint8_t> val = {0xAA};
    bb.add(std::span<const uint8_t>(val));

    // First send() must succeed (future resolves).
    auto fut = bb.send();

    // add() after send() must throw.
    EXPECT_THROW(bb.add(std::span<const uint8_t>(val)), std::logic_error)
        << "add() after send() must throw std::logic_error";

    // Let the future resolve (so no hanging future at end of test).
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (fut.wait_for(0s) == std::future_status::ready) break;
        std::this_thread::sleep_for(kPumpSleep);
    }
    // Don't check fut.get() — just ensure it resolved cleanly.
}

// ===========================================================================
// Test 5: BinaryValues
//   NUL+0xFF values survive round-trip; a larger batch (6 records) works.
// ===========================================================================
TEST(BatchBuilder, BinaryValues) {
    static constexpr int32_t kInStr = 3009, kEgStr = 3010;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 5'000'000;
    pcfg.batchBytes  = 64 * 1024;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(24, 1);
    Key  key(uint64_t{77});

    auto bb = p.batch(topic, key);

    // Binary values: NUL + 0xFF patterns.
    const std::vector<uint8_t> binVal1 = {0x00, 0xFF, 0x00, 0xFF};
    const std::vector<uint8_t> binVal2 = {0xFF, 0x00, 0xFF, 0x00};
    const std::vector<uint8_t> binVal3 = {0x00, 0x00, 0xFF, 0xFF};
    // Larger records.
    const std::vector<uint8_t> bigVal(100, 0xAB);
    const std::vector<uint8_t> bigVal2(200, 0x42);
    // One more binary.
    const std::vector<uint8_t> binVal4 = {0x01, 0x00, 0x02, 0xFF, 0x03};

    bb.add(std::span<const uint8_t>(binVal1));
    bb.add(std::span<const uint8_t>(binVal2));
    bb.add(std::span<const uint8_t>(binVal3));
    bb.add(std::span<const uint8_t>(bigVal));
    bb.add(std::span<const uint8_t>(bigVal2));
    bb.add(std::span<const uint8_t>(binVal4));

    auto fut = bb.send();

    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (fut.wait_for(0s) == std::future_status::ready) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(done) << "Future must resolve";

    ASSERT_GE(h.gw->batches.size(), 1u);
    const auto& b0 = h.gw->batches[0];
    EXPECT_EQ(b0.recCount, 6) << "Must receive all 6 records in one batch";
    ASSERT_EQ(b0.values.size(), 6u);

    // Check binary values round-trip correctly.
    auto checkBytes = [](const std::string& actual, const std::vector<uint8_t>& expected) {
        ASSERT_EQ(actual.size(), expected.size()) << "Value size mismatch";
        for (std::size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(static_cast<uint8_t>(actual[i]), expected[i])
                << "Byte mismatch at index " << i;
        }
    };

    checkBytes(b0.values[0], binVal1);
    checkBytes(b0.values[1], binVal2);
    checkBytes(b0.values[2], binVal3);
    checkBytes(b0.values[3], bigVal);
    checkBytes(b0.values[4], bigVal2);
    checkBytes(b0.values[5], binVal4);
}

// ===========================================================================
// Test 6: Tier1Tier2SamePartitionOrdered
//   Tier-1 send (auto-batch) then Tier-2 batch().send() to the SAME partition.
//   Both succeed with correct ordered offsets: T1 batch before T2 batch.
// ===========================================================================
TEST(BatchBuilder, Tier1Tier2SamePartitionOrdered) {
    static constexpr int32_t kInStr = 3011, kEgStr = 3012;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    // Use acks=Leader so we can track both futures.
    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;        // flush Tier-1 quickly
    pcfg.batchBytes  = 64 * 1024; // large — won't trigger on size
    pcfg.maxInFlight = 8;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(25, 1); // single partition
    Key  key(uint64_t{0});

    // Tier-1 send: fire-and-forget (auto-batch; future for tracking).
    // Use a callback to get Tier-1 baseOffset.
    std::atomic<int64_t> t1Offset{-1};
    const std::vector<uint8_t> t1Val = {0x11, 0x22, 0x33};
    p.send(topic, key, std::span<const uint8_t>(t1Val),
           [&](int64_t off) { t1Offset.store(off, std::memory_order_release); });

    // Tier-2 send: explicit batch.
    auto bb = p.batch(topic, key);
    const std::vector<uint8_t> t2Val = {0xAA, 0xBB};
    bb.add(std::span<const uint8_t>(t2Val));
    bb.add(std::span<const uint8_t>(t2Val));
    auto t2Fut = bb.send();

    // Pump until both resolve.
    bool t1done = false, t2done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (t1Offset.load(std::memory_order_acquire) >= 0) t1done = true;
        if (t2Fut.wait_for(0s) == std::future_status::ready) t2done = true;
        if (t1done && t2done) break;
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(t1done) << "Tier-1 callback must fire";
    ASSERT_TRUE(t2done) << "Tier-2 future must resolve";

    const int64_t t1Base = t1Offset.load();
    const int64_t t2Base = t2Fut.get();

    EXPECT_GE(t1Base, 1000LL) << "T1 base must be gateway-assigned";
    EXPECT_GE(t2Base, 1000LL) << "T2 base must be gateway-assigned";

    // Ordering: T1 was submitted first; must appear before T2 in the log.
    // T1 batch has 1 record → T2 base = T1 base + 1.
    ASSERT_GE(h.gw->batches.size(), 2u) << "Expected at least 2 batches";

    // Identify T1 and T2 batches by corrId.
    const auto& firstBatch  = h.gw->batches[0];
    const auto& secondBatch = h.gw->batches[1];

    // First batch must have T1's record count (1), second must have T2's (2).
    EXPECT_EQ(firstBatch.recCount,  1) << "T1 batch must have 1 record";
    EXPECT_EQ(secondBatch.recCount, 2) << "T2 batch must have 2 records";

    // T2 base = T1 base + T1 recCount.
    EXPECT_EQ(t2Base, t1Base + static_cast<int64_t>(firstBatch.recCount))
        << "T2 must start immediately after T1 (submission order preserved)";
}

// ===========================================================================
// Test 7: IdempotentSeqContinuity
//   baseSequence advances monotonically and gap-free across Tier-1 and Tier-2.
// ===========================================================================
TEST(BatchBuilder, IdempotentSeqContinuity) {
    static constexpr int32_t kInStr = 3013, kEgStr = 3014;
    CB3AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;
    pcfg.batchBytes  = 64 * 1024;
    pcfg.maxInFlight = 16;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(26, 1);
    Key  key(uint64_t{0});

    const std::vector<uint8_t> val(5, 0x55);

    // Send pattern: T1 (1 rec), T2 (2 recs), T1 (1 rec), T2 (3 recs), T1 (1 rec).
    // Expected baseSequences: 0, 1, 3, 4, 7 (gap-free across both tiers).

    // T1-a: 1 record.
    auto fut1a = p.send(topic, key, std::span<const uint8_t>(val), want_future);

    // T2-a: 2 records.
    {
        auto bb = p.batch(topic, key);
        bb.add(std::span<const uint8_t>(val));
        bb.add(std::span<const uint8_t>(val));
        auto fut2a = bb.send();

        // T1-b: 1 record.
        auto fut1b = p.send(topic, key, std::span<const uint8_t>(val), want_future);

        // T2-b: 3 records.
        {
            auto bb2 = p.batch(topic, key);
            bb2.add(std::span<const uint8_t>(val));
            bb2.add(std::span<const uint8_t>(val));
            bb2.add(std::span<const uint8_t>(val));
            auto fut2b = bb2.send();

            // T1-c: 1 record.
            auto fut1c = p.send(topic, key, std::span<const uint8_t>(val), want_future);

            // Wait for all to resolve.
            bool all = false;
            for (int i = 0; i < kPumpMax; ++i) {
                h.agent->doWork(monoNs());
                h.gw->poll();
                if (fut1a.wait_for(0s) == std::future_status::ready &&
                    fut2a.wait_for(0s) == std::future_status::ready &&
                    fut1b.wait_for(0s) == std::future_status::ready &&
                    fut2b.wait_for(0s) == std::future_status::ready &&
                    fut1c.wait_for(0s) == std::future_status::ready) {
                    all = true;
                    break;
                }
                std::this_thread::sleep_for(kPumpSleep);
            }
            ASSERT_TRUE(all) << "All futures must resolve";
        }
    }

    // All 5 batches must have been received.
    ASSERT_GE(h.gw->batches.size(), 5u) << "Expected 5 batches total";

    // Verify monotonically increasing, gap-free baseSequence across all batches.
    int32_t nextExpected = h.gw->batches[0].baseSeq; // whatever seq 0 is
    for (std::size_t i = 0; i < h.gw->batches.size(); ++i) {
        const auto& b = h.gw->batches[i];
        EXPECT_EQ(b.baseSeq, nextExpected)
            << "baseSequence must be gap-free at batch " << i
            << " (recCount=" << b.recCount << ")";
        EXPECT_GT(b.recCount, 0) << "recCount must be > 0 at batch " << i;
        nextExpected = b.baseSeq + b.recCount;
    }
    // Strictly increasing.
    for (std::size_t i = 1; i < h.gw->batches.size(); ++i) {
        EXPECT_GT(h.gw->batches[i].baseSeq, h.gw->batches[i-1].baseSeq)
            << "baseSeq must be strictly increasing";
    }
}

// ===========================================================================
// CB-5 A1: CrossTierOrderingUnderMaxInFlight
//
// Submits a Tier-1 record then a Tier-2 batch to the SAME partition with
// maxInFlight=1 and a slow-ack gateway (holds acks until we release them).
//
// Assertion: the gateway MUST receive the T1 batch before the T2 batch
// (submission order preserved), even under maxInFlight backpressure.
// The T2 batch must NOT arrive before the T1 ack is sent.
//
// This is the exact scenario from the CB-3 review finding: without the A1
// fix, flushBatches() would dispatch T2 while T1 is still in-flight at
// maxInFlight=1, jumping T2 ahead in the log.
// ===========================================================================

// Extend CB3Gateway with slow-ack capability for this test.
struct SlowAckGateway {
    struct BatchRecord {
        int32_t topicId   = 0;
        int32_t partition = 0;
        int64_t corrId    = 0;
        int32_t baseSeq   = -1;
        int32_t recCount  = 0;
    };

    std::vector<BatchRecord> batches;
    std::atomic<int>         batchCount {0};
    int64_t                  nextOffset = 500;

    // Hold all acks when false; flush on release.
    bool holdAcks = true;
    std::vector<std::tuple<int32_t,int32_t,int64_t,int64_t>> pendingAcks_; // (part,cnt,sess,corr)

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;

    uint8_t replyBuf[2048]{};

    explicit SlowAckGateway(const std::string& aeronDir,
                             int32_t ingressStream,
                             int32_t egressStream)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("SlowAckGateway: connect failed");

        int64_t subId = gwAeron->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = gwAeron->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!gwSub) gwSub = gwAeron->findSubscription(subId);
            if (!gwPub) gwPub = gwAeron->findExclusivePublication(pubId);
            if (gwSub && gwPub) break;
            std::this_thread::sleep_for(kPumpSleep);
        }
        if (!gwSub || !gwPub)
            throw std::runtime_error("SlowAckGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~SlowAckGateway() {
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

    void sendAck(int32_t partition, int32_t recCount,
                 int64_t sessionId, int64_t corrId) {
        const int64_t baseOff = nextOffset;
        nextOffset += recCount;
        std::size_t n = rcc::encodeProduceResponse(
            replyBuf, sizeof(replyBuf),
            /*errorCode=*/0, partition, baseOff, sessionId, corrId);
        if (n > 0) gwSend(replyBuf, n);
    }

    /// Release all held acks in order.
    void releaseAcks() {
        for (auto& [part, cnt, sess, corr] : pendingAcks_) {
            sendAck(part, cnt, sess, corr);
        }
        pendingAcks_.clear();
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
            if (n > 0) gwSend(replyBuf, n);
            return;
        }

        if (tid != rcc::TemplateId::Produce) return;

        auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;

        BatchRecord rec;
        rec.topicId   = opt->topicId;
        rec.partition = opt->partition;
        rec.corrId    = opt->correlationId;

        const auto& rb = opt->recordBatch;
        if (rb.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            const uint8_t* d = rb.data();
            rec.baseSeq = static_cast<int32_t>(
                static_cast<uint32_t>(d[53])        |
               (static_cast<uint32_t>(d[54]) <<  8) |
               (static_cast<uint32_t>(d[55]) << 16) |
               (static_cast<uint32_t>(d[56]) << 24));
            rec.recCount = static_cast<int32_t>(
                static_cast<uint32_t>(d[57])        |
               (static_cast<uint32_t>(d[58]) <<  8) |
               (static_cast<uint32_t>(d[59]) << 16) |
               (static_cast<uint32_t>(d[60]) << 24));
        }

        batches.push_back(rec);
        batchCount.fetch_add(1, std::memory_order_release);

        if (opt->correlationId == 0) return; // acks=None

        if (holdAcks) {
            pendingAcks_.emplace_back(opt->partition, rec.recCount,
                                      opt->sessionId, opt->correlationId);
        } else {
            sendAck(opt->partition, rec.recCount, opt->sessionId, opt->correlationId);
        }
    }
};

TEST(BatchBuilder, CrossTierOrderingUnderMaxInFlight) {
    static constexpr int32_t kInStr = 3015, kEgStr = 3016;

    const std::string agentDir = uniqueDir("cb5_a1");
    const std::string aeronDir = agentDir + "/aeron";

    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = kInStr;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = kEgStr;
    cfg.transport.dataDir         = agentDir;
    cfg.session.egressChannel     = "aeron:ipc";
    cfg.session.egressStreamId    = kEgStr;
    cfg.session.keepaliveIntervalNs = 10'000'000'000LL;

    rprod::ProducerHub    hub;

    auto agent = std::make_unique<rca::ClientAgent>(cfg);
    agent->setProducerDrainFn(
        [&](int64_t nowNs, int64_t sessionId,
               rca::ClientAgent::SendFn sendFn) -> int {
            return hub.drainAndFlush(nowNs, sessionId, std::move(sendFn));
        });
    agent->setProduceResponseFn(
        [&](int64_t correlationId, int64_t baseOffset) {
            hub.onProduceResponse(correlationId, baseOffset);
        });
    agent->setEgressObserver([](rcc::TemplateId, const uint8_t*, std::size_t){});
    agent->start();
    std::this_thread::sleep_for(150ms);

    auto gw = std::make_unique<SlowAckGateway>(aeronDir, kInStr, kEgStr);

    // Connect.
    agent->submitConnect();
    for (int i = 0; i < kPumpMax; ++i) {
        agent->doWork(monoNs());
        gw->poll();
        if (agent->sessionState() == rcs::SessionState::Connected) break;
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_EQ(agent->sessionState(), rcs::SessionState::Connected)
        << "Failed to connect";

    // Producer with maxInFlight=1 so only 1 batch can be in-flight at a time.
    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;        // flush fast
    pcfg.batchBytes  = 64*1024;
    pcfg.maxInFlight = 1;        // KEY: only 1 in-flight per partition

    auto impl = std::make_shared<rprod::ProducerImpl>(pcfg);
    hub.add(impl);
    Producer p(impl);

    auto topic = makeTopic(30, 1);
    Key  key(uint64_t{0});

    const std::vector<uint8_t> t1val = {0x11};
    const std::vector<uint8_t> t2val = {0x22, 0x33};

    // Hold all acks — T1 goes in-flight, T2 must wait.
    gw->holdAcks = true;

    // Submit T1 (Tier-1 auto-batch).
    auto fut1 = p.send(topic, key, std::span<const uint8_t>(t1val), want_future);

    // Submit T2 (Tier-2 explicit batch) AFTER T1.
    auto bb = p.batch(topic, key);
    bb.add(std::span<const uint8_t>(t2val));
    bb.add(std::span<const uint8_t>(t2val));
    auto fut2 = bb.send();

    // Pump for a while: only T1 should be sent (maxInFlight=1, acks held).
    for (int i = 0; i < 300; ++i) {
        agent->doWork(monoNs());
        gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    // Assert: gateway received exactly 1 batch (T1); T2 must be blocked.
    const int batchesBefore = gw->batchCount.load(std::memory_order_acquire);
    EXPECT_EQ(batchesBefore, 1)
        << "With maxInFlight=1 and held acks, only T1 should be sent; got "
        << batchesBefore << " batch(es). T2 must NOT jump ahead of T1.";

    // The one batch received must be T1 (1 record, recCount=1).
    ASSERT_GE(gw->batches.size(), 1u);
    EXPECT_EQ(gw->batches[0].recCount, 1)
        << "First batch must be T1 (1 record); got recCount="
        << gw->batches[0].recCount;

    // Now release T1 ack → unblocks the in-flight slot → T2 can flow.
    gw->holdAcks = false;
    gw->releaseAcks();

    // Pump until T2 arrives and both futures resolve.
    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        agent->doWork(monoNs());
        gw->poll();
        if (fut1.wait_for(0s) == std::future_status::ready &&
            fut2.wait_for(0s) == std::future_status::ready) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done) << "Both T1 and T2 futures must resolve after ack release";
    ASSERT_GE(gw->batches.size(), 2u) << "Expected 2 batches total";

    // Submission order: T1 before T2 → T1.baseSeq < T2.baseSeq.
    const auto& b0 = gw->batches[0];
    const auto& b1 = gw->batches[1];

    EXPECT_EQ(b0.recCount, 1) << "Batch 0 must be T1 (1 record)";
    EXPECT_EQ(b1.recCount, 2) << "Batch 1 must be T2 (2 records)";
    EXPECT_LT(b0.baseSeq, b1.baseSeq)
        << "T1 baseSeq must be < T2 baseSeq (submission order preserved)";
    EXPECT_EQ(b1.baseSeq, b0.baseSeq + b0.recCount)
        << "T2 must immediately follow T1 (gap-free)";

    // Cleanup.
    agent->stop();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// CB-5 A3: RingFullTier2FailsFast
//
// A tiny ring (4 KiB) is filled with Tier-1 records so the next Tier-2
// ring write fails.  The Tier-2 future must resolve IMMEDIATELY with an
// exception (not hang until produceTimeoutMs).
// ===========================================================================
TEST(BatchBuilder, RingFullTier2FailsFast) {
    static constexpr int32_t kInStr = 3017, kEgStr = 3018;

    const std::string agentDir = uniqueDir("cb5_a3");
    const std::string aeronDir = agentDir + "/aeron";

    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = kInStr;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = kEgStr;
    cfg.transport.dataDir         = agentDir;
    cfg.session.egressChannel     = "aeron:ipc";
    cfg.session.egressStreamId    = kEgStr;
    cfg.session.keepaliveIntervalNs = 10'000'000'000LL;
    cfg.ringCapacity              = 4096; // 4 KiB ring

    rprod::ProducerHub   hub;

    auto agent = std::make_unique<rca::ClientAgent>(cfg);
    agent->setProducerDrainFn([](int64_t,int64_t,rca::ClientAgent::SendFn){return 0;});
    agent->setProduceResponseFn([](int64_t,int64_t){});
    agent->setEgressObserver([](rcc::TemplateId,const uint8_t*,std::size_t){});
    agent->start();
    std::this_thread::sleep_for(150ms);

    // Use a tiny ProducerImpl ring (4 KiB) to make it easy to fill.
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1'000'000'000LL;
    pcfg.batchBytes       = 64 * 1024;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 30'000; // long timeout — should NOT be reached

    auto impl = std::make_shared<rprod::ProducerImpl>(pcfg, 4 * 1024);
    hub.add(impl);
    Producer p(impl);

    auto topic = makeTopic(31, 1);
    Key  key(uint64_t{0});

    // Fill the ring with Tier-1 records.
    const std::vector<uint8_t> val(32, 0xCC);
    for (int i = 0; i < 200; ++i) {
        impl->submitSendRecord(
            topic.topicId, key.partition(topic.partitionCount),
            key, std::span<const uint8_t>(val),
            /*wantAck=*/false, /*corrId=*/0);
    }
    // The Tier-2 token is only 8 bytes (a Tier2Entry* travels the ring), so it can
    // still fit in the tail the 80-byte records leave; top the ring off with the
    // smallest messages (8-byte flush tokens) so it is truly full.  F8c C0: an
    // app-thread flush() on a full ring now waits for ring room (the request is
    // never dropped); nothing drains here, so this thread poses as the duty
    // thread, whose flush() on a full ring never waits.
    impl->setPumpFn(nullptr, nullptr, std::this_thread::get_id());
    for (int i = 0; i < 1'000; ++i) impl->submitFlush();

    // Now try a Tier-2 batch — ring should be full → fail immediately.
    auto bb = p.batch(topic, key);
    bb.add(std::span<const uint8_t>(val));
    auto fut2 = bb.send();

    // Future must be immediately ready (ring-full fail-fast).
    const auto status = fut2.wait_for(0ms);
    EXPECT_EQ(status, std::future_status::ready)
        << "Tier-2 ring-full must fail the future IMMEDIATELY (not via timeout)";

    if (status == std::future_status::ready) {
        EXPECT_THROW({ fut2.get(); }, std::runtime_error)
            << "Ring-full Tier-2 must throw std::runtime_error";
    }

    // Cleanup.
    agent->stop();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}

// ===========================================================================
// A tiny ring filled with Tier-1 records so the NEXT Tier-1 future send fails
// to enqueue.  The future must resolve IMMEDIATELY with an exception, NOT hang
// forever waiting for an ack that can never come (regression for the stranded-
// future-on-ring-full bug).  Same for the callback overload (callback(-1)).
// ===========================================================================
TEST(BatchBuilder, RingFullTier1FutureFailsFast) {
    static constexpr int32_t kInStr = 3019, kEgStr = 3020;
    const std::string agentDir = uniqueDir("cb5_t1full");

    rca::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = kInStr;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = kEgStr;
    cfg.transport.dataDir         = agentDir;
    cfg.session.egressChannel     = "aeron:ipc";
    cfg.session.egressStreamId    = kEgStr;
    cfg.session.keepaliveIntervalNs = 10'000'000'000LL;
    cfg.ringCapacity              = 4096;

    rprod::ProducerHub   hub;
    auto agent = std::make_unique<rca::ClientAgent>(cfg);
    agent->setProducerDrainFn([](int64_t,int64_t,rca::ClientAgent::SendFn){return 0;});
    agent->setProduceResponseFn([](int64_t,int64_t){});
    agent->setEgressObserver([](rcc::TemplateId,const uint8_t*,std::size_t){});
    agent->start();
    std::this_thread::sleep_for(150ms);

    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1'000'000'000LL;
    pcfg.batchBytes       = 64 * 1024;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 30'000; // long — must NOT be reached
    auto impl = std::make_shared<rprod::ProducerImpl>(pcfg, 4 * 1024);
    hub.add(impl);
    Producer p(impl);

    auto topic = makeTopic(32, 1);
    Key  key(uint64_t{0});
    const std::vector<uint8_t> val(32, 0xCC);

    // Fill the ring with fire-and-forget records.
    for (int i = 0; i < 200; ++i) {
        impl->submitSendRecord(
            topic.topicId, key.partition(topic.partitionCount),
            key, std::span<const uint8_t>(val), /*wantAck=*/false, /*corrId=*/0);
    }

    // Tier-1 future send on a full ring → future must be READY immediately.
    auto fut = p.send(topic, key, std::span<const uint8_t>(val),
                      rune::client::want_future_t{});
    const auto status = fut.wait_for(0ms);
    EXPECT_EQ(status, std::future_status::ready)
        << "Tier-1 ring-full must reject the future IMMEDIATELY, not hang";
    if (status == std::future_status::ready) {
        EXPECT_THROW({ fut.get(); }, std::runtime_error)
            << "Ring-full Tier-1 future must throw std::runtime_error";
    }

    // Callback overload on a full ring → callback must fire with offset=-1.
    std::atomic<int64_t> cbOffset{0};
    std::atomic<bool>    cbFired{false};
    p.send(topic, key, std::span<const uint8_t>(val),
           [&](int64_t off){ cbOffset.store(off); cbFired.store(true); });
    EXPECT_TRUE(cbFired.load()) << "Ring-full callback must fire (not be stranded)";
    EXPECT_EQ(cbOffset.load(), -1) << "Ring-full callback must signal error (-1)";

    agent->stop();
    try { std::filesystem::remove_all(agentDir); } catch (...) {}
}
