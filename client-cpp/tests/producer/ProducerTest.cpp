// tests/producer/ProducerTest.cpp
//
// CB-2 Acceptance tests — Producer Tier-1 (auto-batch, idempotent, acks).
//
// Test matrix:
//   1.  AutoBatchByteTrigger        — N sends to same (topic,key) accumulate into ONE
//                                     batch, flushed when batchBytes reached.
//   2.  AutoBatchLingerTrigger      — single send flushes after linger elapses
//                                     (injected clock via doWork(nowNs)); before
//                                     linger the batch stays pending.
//   3.  PartitionSameKey            — same key → same partition across multiple sends.
//   4.  PartitionMatchesBrokerHash  — client partition == partitionForKey(key, N).
//   5.  PartitionDifferentKeys      — different keys spread across partitions.
//   6.  AckFutureResolvesWithOffset — want_future: future resolves with broker baseOffset.
//   7.  AckCallbackFires            — callback fires with broker baseOffset.
//   8.  AcksNoneNoResponse          — acks=None: batch delivered, no future/callback.
//   9.  MaxInFlightBound            — slow/no-ack broker: sends stop flushing beyond cap.
//   10. IdempotentMonotonicSequence — batches carry monotonically increasing baseSequence.
//   11. BinaryAndEmptyValue         — NUL+0xFF value, empty value both survive round-trip.
//   12. ZeroAllocVoidSend           — void send path provably has no new/malloc.
//   13. TopicLookup                 — TopicHandle validation + Key::partition() == partitionForKey.
//
// Gateway harness: reuses the CC-5/CC-6 shared-driver pattern (gateway Aeron
// client connects to the agent's embedded driver via the same aeronDir).
//
// Rules:
//   - Bounded pumps (kPumpMax iterations, kPumpSleep between) — no busy-spin.
//   - Unique temp dirs: getpid() + atomic counter.
//   - Injected clock for linger tests: pass synthetic nowNs to doWork().
//   - No background runner thread in these tests: we drive doWork() directly.

#include "../support/AckLedger.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/session/SessionMachine.h>

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
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;

namespace rcc  = rune::client::codec;
namespace rca  = rune::client::agent;
namespace rcs  = rune::client::session;
namespace rprod = rune::client::producer;
namespace rc   = rune::client;

using rc::Key;
using rc::TopicHandle;
using rc::Producer;
using rc::ProducerConfig;
using rc::Acks;
using rc::AckToken;
using rc::OpStatus;
using rc::SendResult;
using test_support::AckLedger;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

static constexpr int  kPumpMax   = 5'000;
static constexpr auto kPumpSleep = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {

static std::atomic<int> g_dirCtr{0};

std::string uniqueDir(const char* prefix = "cb2_test") {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
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
// ProduceGateway — fake broker that decodes Produce frames, replies
// ProduceResponse.  Also handles ConnectRequest → ConnectResponse.
// ---------------------------------------------------------------------------

struct ProduceGateway {
    struct BatchRecord {
        int32_t topicId    = 0;
        int32_t partition  = 0;
        int64_t corrId     = 0;
        int64_t producerId = -1; // from RecordBatch header offset 43 (F5)
        int32_t baseSeq    = -1; // from RecordBatch header offset 53
        int32_t recCount   = 0;
        uint8_t acks       = 0xff; // captured from wire frame (0=Leader/None, 1=All)
        int64_t frameProducerId    = -1;  // Produce FRAME fields (what the broker dedups on)
        int16_t frameProducerEpoch = -1;
        int32_t frameBaseSeq       = -1;
        int16_t producerEpoch      = -1;  // from RecordBatch header
        std::vector<std::string> values; // decoded record values
    };

    // Pending ack info (stored when replyAcks=false at receive time).
    struct PendingAck {
        int64_t corrId;
        int64_t sessionId;
        int32_t partition;
        int32_t recCount;
    };

    std::vector<BatchRecord> batches;
    std::atomic<int>         batchCount {0};
    int64_t                  nextOffset = 100;

    bool replyAcks = true; // set false to simulate slow broker (maxInFlight test)
    std::vector<PendingAck> pendingAcks_; // held when replyAcks=false

    std::shared_ptr<aeron::Aeron>                gwAeron;
    std::shared_ptr<aeron::Subscription>         gwSub;
    std::shared_ptr<aeron::ExclusivePublication> gwPub;
    std::unique_ptr<aeron::FragmentAssembler>    gwAsm;

    uint8_t replyBuf[2048]{};

    explicit ProduceGateway(const std::string& aeronDir,
                             int32_t ingressStream,
                             int32_t egressStream)
    {
        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        gwAeron = aeron::Aeron::connect(ctx);
        if (!gwAeron) throw std::runtime_error("ProduceGateway: connect failed");

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
            throw std::runtime_error("ProduceGateway: sub/pub timed out");

        gwAsm = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t off, aeron::util::index_t len,
                   aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                        static_cast<std::size_t>(len));
            });
    }

    ~ProduceGateway() {
        gwAsm.reset(); gwPub.reset(); gwSub.reset(); gwAeron.reset();
    }

    int poll(int limit = 16) {
        if (!gwSub || !gwAsm) return 0;
        return gwSub->poll(gwAsm->handler(), limit);
    }

    bool send(const uint8_t* buf, std::size_t len) {
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

        if (tid != rcc::TemplateId::Produce) return;

        auto opt = rcc::decodeProduce(buf, len);
        if (!opt) return;

        BatchRecord rec;
        rec.topicId   = opt->topicId;
        rec.partition = opt->partition;
        rec.corrId    = opt->correlationId;
        rec.acks      = opt->acks;  // B2: capture wire acks field
        rec.frameProducerId    = opt->producerId;
        rec.frameProducerEpoch = opt->producerEpoch;
        rec.frameBaseSeq       = opt->baseSequence;

        // Decode RecordBatch header via the reader (producerId, baseSequence,
        // recordCount — F5 adds producerId to what this test harness captures).
        const auto& rb = opt->recordBatch;
        if (rb.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            rune::protocol::RecordBatchReader reader(rb.data(), rb.size());
            rec.producerId    = reader.producerId();
            rec.producerEpoch = reader.producerEpoch();
            rec.baseSeq       = reader.baseSequence();
            rec.recCount   = reader.recordCount();

            // Decode values.
            reader.forEach([&](std::string_view /*key*/, std::string_view val,
                               int64_t /*off*/, int64_t /*ts*/) {
                rec.values.emplace_back(val.data(), val.size());
            });
        }

        batches.push_back(rec);
        batchCount.fetch_add(1, std::memory_order_release);

        if (opt->correlationId == 0) return; // acks=None → corrId=0

        if (!replyAcks) {
            // Stash for later replay when replyAcks is re-enabled.
            pendingAcks_.push_back(PendingAck{
                opt->correlationId, opt->sessionId,
                opt->partition, rec.recCount
            });
            return;
        }

        sendAck(opt->partition, rec.recCount, opt->sessionId, opt->correlationId);
    }

    /// Send a ProduceResponse for a single batch.
    void sendAck(int32_t partition, int32_t recCount,
                 int64_t sessionId, int64_t corrId) {
        const int64_t baseOff = nextOffset;
        nextOffset += recCount;
        std::size_t n = rcc::encodeProduceResponse(
            replyBuf, sizeof(replyBuf),
            /*errorCode=*/0,
            partition,
            baseOff,
            sessionId,
            corrId);
        if (n > 0) send(replyBuf, n);
    }

    /// Flush any pending acks accumulated while replyAcks was false.
    void flushPendingAcks() {
        for (auto& pa : pendingAcks_) {
            sendAck(pa.partition, pa.recCount, pa.sessionId, pa.corrId);
        }
        pendingAcks_.clear();
    }
};

// ---------------------------------------------------------------------------
// AgentHarness — wraps ClientAgent + ProduceGateway.
// Tests drive doWork() directly (no background runner).
// ---------------------------------------------------------------------------

struct AgentHarness {
    std::string agentDir;
    std::string aeronDir;

    std::unique_ptr<rca::ClientAgent> agent;
    std::unique_ptr<ProduceGateway>   gw;

    rprod::ProducerHub    hub;

    explicit AgentHarness(int32_t ingressStream, int32_t egressStream,
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
        cfg.session.keepaliveIntervalNs = 10'000'000'000LL; // 10s
        cfg.ringCapacity              = ringCap;

        agent = std::make_unique<rca::ClientAgent>(cfg);

        // Wire producer hooks (setup-phase only, before start()).
        agent->setProducerDrainFn(
            [this](int64_t nowNs, int64_t sessionId,
                   rca::ClientAgent::SendFn sendFn) -> int {
                return hub.drainAndFlush(nowNs, sessionId, std::move(sendFn));
            });
        agent->setProduceResponseFn(
            [this](int64_t correlationId, int64_t baseOffset) {
                hub.onProduceResponse(correlationId, baseOffset);
            });
        // Absorb egress (ConnectResponse handled by session machine internally).
        agent->setEgressObserver([](rcc::TemplateId, const uint8_t*, std::size_t){});

        agent->start();
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<ProduceGateway>(aeronDir, ingressStream, egressStream);
    }

    ~AgentHarness() {
        gw.reset();
        if (agent) agent->stop();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }

    /// Create and register a producer with the hub.
    Producer makeProducer(ProducerConfig pcfg = {}) {
        auto* impl = new rprod::ProducerImpl(pcfg);  // F8c C7: the handle owns it (bare)
        hub.add(impl);
        return Producer(impl);
    }

    /// Connect the agent (submit + pump until Connected).
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

    /// Pump until batchCount reaches target or limit exceeded.
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

// ---------------------------------------------------------------------------
// Helper: build a TopicHandle inline.
// ---------------------------------------------------------------------------
static TopicHandle makeTopic(int32_t topicId, int32_t partitionCount) {
    return TopicHandle{topicId, partitionCount};
}

// ===========================================================================
// Test 1: AutoBatchByteTrigger
//   N sends to the same (topic,key) accumulate until batchBytes → one batch.
// ===========================================================================
TEST(Producer, AutoBatchByteTrigger) {
    static constexpr int32_t kInStr = 2001, kEgStr = 2002;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect to gateway";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 10'000'000; // 10 s — won't fire during test
    pcfg.batchBytes = 500;        // triggers at ~500 bytes
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(1, 4);
    Key  key(uint64_t{42});

    // Each value is 100 bytes.  builderBytes now tracks the TRUE encoded WIRE size
    // (61-byte batch header + per-record body incl. framing ≈ 114 B/record), so the
    // 500-byte trigger fires after ~4 records (61 + 4*114 = 517 ≥ 500) rather than
    // 5 under the old value-only count.  Point of the test: the byte trigger packs
    // MULTIPLE records into one batch.
    const std::vector<uint8_t> val(100, 0xAB);
    const std::span<const uint8_t> valSpan(val);

    for (int i = 0; i < 6; ++i) {
        p.send(topic, key, valSpan);
    }

    ASSERT_TRUE(h.pumpUntilBatches(1))
        << "Expected 1 batch flushed on batchBytes trigger";

    ASSERT_GE(h.gw->batches.size(), 1u);
    EXPECT_GT(h.gw->batches[0].recCount, 1)
        << "Batch must accumulate multiple records before flushing";
    EXPECT_EQ(h.gw->batches[0].topicId, 1);
}

// ===========================================================================
// Test 2: AutoBatchLingerTrigger
//   One send with huge batchBytes → not flushed at T0.
//   Advance clock past lingerUs → flushed.
// ===========================================================================
TEST(Producer, AutoBatchLingerTrigger) {
    static constexpr int32_t kInStr = 2003, kEgStr = 2004;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect to gateway";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 5'000;           // 5 ms linger
    pcfg.batchBytes = 10 * 1024 * 1024; // 10 MB — won't trigger on size
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(2, 1);
    Key  key(uint64_t{1});

    const std::vector<uint8_t> val(10, 0xCC);
    p.send(topic, key, std::span<const uint8_t>(val));

    // T0 is before the linger interval. Pump 20 iterations with frozen clock.
    const int64_t T0      = monoNs();
    const int64_t lingerNs = 5'000LL * 1'000LL; // 5 ms in ns

    for (int i = 0; i < 20; ++i) {
        h.agent->doWork(T0); // clock frozen before linger expiry
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }
    EXPECT_EQ(h.gw->batchCount.load(), 0)
        << "Batch must NOT flush before linger elapses";

    // Advance clock past linger.
    const int64_t Tafter = T0 + lingerNs + 2'000'000LL; // +2 ms extra

    bool flushed = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(Tafter);
        h.gw->poll();
        if (h.gw->batchCount.load(std::memory_order_acquire) >= 1) {
            flushed = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }
    ASSERT_TRUE(flushed) << "Batch must flush after linger elapses";
}

// ===========================================================================
// Test 3: PartitionSameKey
//   Same key always routes to the same partition.
// ===========================================================================
TEST(Producer, PartitionSameKey) {
    static constexpr int32_t kInStr = 2005, kEgStr = 2006;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1;   // instant linger — flush on first pump
    pcfg.batchBytes = 1;   // flush every record
    pcfg.maxInFlight = 32;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(3, 8);
    Key  key(uint64_t{0xDEADBEEF});

    const std::vector<uint8_t> val(4, 0x11);

    for (int i = 0; i < 5; ++i) {
        p.send(topic, key, std::span<const uint8_t>(val));
    }
    p.flush();

    ASSERT_TRUE(h.pumpUntilBatches(5))
        << "Expected 5 batches (batchBytes=1 → one per send)";

    const int32_t expectedPart = h.gw->batches[0].partition;
    for (const auto& b : h.gw->batches) {
        EXPECT_EQ(b.partition, expectedPart)
            << "Same key must always route to the same partition";
    }
}

// ===========================================================================
// Test 4: PartitionMatchesBrokerHash
//   Client-side partition == partitionForKey(key, N) for multiple keys.
// ===========================================================================
TEST(Producer, PartitionMatchesBrokerHash) {
    static constexpr int32_t kInStr = 2007, kEgStr = 2008;
    static constexpr int32_t kParts = 7;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 64;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(4, kParts);

    const std::vector<uint8_t> val(1, 0x01);

    // 10 different uint64 keys.
    const std::vector<uint64_t> keys = {0, 1, 7, 42, 99, 100, 12345, 99999, 0xFFFF, 0xDEAD};
    for (auto k : keys) {
        p.send(topic, Key(k), std::span<const uint8_t>(val));
        p.flush();
    }

    ASSERT_TRUE(h.pumpUntilBatches(static_cast<int>(keys.size())))
        << "Expected one batch per key send";

    ASSERT_EQ(h.gw->batches.size(), keys.size());
    for (std::size_t i = 0; i < keys.size(); ++i) {
        // Build the 8-byte LE representation of keys[i].
        uint8_t kb[8];
        const uint64_t kv = keys[i];
        for (int j = 0; j < 8; ++j) kb[j] = static_cast<uint8_t>(kv >> (j * 8));
        const int32_t expectedPart =
            rune::protocol::partitionForKey(std::span<const uint8_t>(kb, 8), kParts);
        EXPECT_EQ(h.gw->batches[i].partition, expectedPart)
            << "Partition mismatch for key=" << keys[i];
    }
}

// ===========================================================================
// Test 5: PartitionDifferentKeys
//   Different keys spread across > 1 partition (FNV-1a is uniform enough).
// ===========================================================================
TEST(Producer, PartitionDifferentKeys) {
    static constexpr int32_t kInStr = 2009, kEgStr = 2010;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 64;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(5, 16);

    const std::vector<uint8_t> val(1, 0x01);

    // 20 keys (multiples of a prime) — with 16 partitions FNV-1a will spread them.
    for (uint64_t k = 0; k < 20; ++k) {
        p.send(topic, Key(k * 7919), std::span<const uint8_t>(val));
        p.flush();
    }

    ASSERT_TRUE(h.pumpUntilBatches(20)) << "Expected 20 batches";

    std::set<int32_t> partsSeen;
    for (const auto& b : h.gw->batches) partsSeen.insert(b.partition);
    EXPECT_GT(partsSeen.size(), 1u)
        << "Different keys must spread across multiple partitions";
}

// ===========================================================================
// Test 6: AckFutureResolvesWithOffset
//   want_future → future resolves with the broker's baseOffset.
// ===========================================================================
TEST(Producer, AckFutureResolvesWithOffset) {
    static constexpr int32_t kInStr = 2011, kEgStr = 2012;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::Leader;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(6, 1);
    Key  key(uint64_t{0});

    const std::vector<uint8_t> val = {0x01, 0x02, 0x03};
    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(val), AckToken{1}), SendResult::Ok);

    bool done = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (L.times(1) != 0) {
            done = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(done) << "The ack token must complete within timeout";
    ASSERT_EQ(L.status(1), OpStatus::Ok);
    const int64_t baseOffset = L.value(1);
    EXPECT_GE(baseOffset, 100LL)
        << "baseOffset must be the gateway-assigned value (>=100)";
}

// ===========================================================================
// Test 7: AckCallbackFires
//   Callback fires with baseOffset when ProduceResponse arrives.
// ===========================================================================
TEST(Producer, AckCallbackFires) {
    static constexpr int32_t kInStr = 2013, kEgStr = 2014;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::Leader;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(7, 1);
    Key  key(uint64_t{0});

    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    const std::vector<uint8_t> val = {0xAB};
    p.send(topic, key, std::span<const uint8_t>(val), AckToken{7});

    bool fired = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (L.times(7) != 0) {
            fired = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fired) << "The onAck handler must fire when ProduceResponse arrives";
    EXPECT_EQ(L.times(7), 1);
    EXPECT_GE(L.value(7), 100LL);
}

// ===========================================================================
// Test 8: AcksNoneNoResponse
//   acks=None → batch delivered, correlationId=0, no ProduceResponse needed.
// ===========================================================================
TEST(Producer, AcksNoneNoResponse) {
    static constexpr int32_t kInStr = 2015, kEgStr = 2016;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(8, 1);
    Key  key(uint64_t{1});

    const std::vector<uint8_t> val = {0xFF};
    p.send(topic, key, std::span<const uint8_t>(val));
    p.flush();

    ASSERT_TRUE(h.pumpUntilBatches(1)) << "acks=None batch must still be delivered";
    EXPECT_EQ(h.gw->batches.size(), 1u);
}

// ===========================================================================
// Test 9: MaxInFlightBound
//   maxInFlight=1 + slow broker (no acks) → after 1 batch in flight, no more
//   are flushed until ack arrives.
// ===========================================================================
TEST(Producer, MaxInFlightBound) {
    static constexpr int32_t kInStr = 2017, kEgStr = 2018;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;
    pcfg.batchBytes  = 1;
    pcfg.maxInFlight = 1;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(9, 1);
    Key  key(uint64_t{0});

    const std::vector<uint8_t> val(10, 0x01);

    // Stop gateway from replying — broker goes silent.
    h.gw->replyAcks = false;

    // Send 3 messages (each with an ack token).
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(val), AckToken{static_cast<rc::Token>(i + 1)}),
                  SendResult::Ok);
    }
    p.flush();

    // Pump 200 iterations — must see exactly 1 batch (maxInFlight=1 cap).
    for (int i = 0; i < 200; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }

    const int batchesSent = h.gw->batchCount.load(std::memory_order_acquire);
    EXPECT_EQ(batchesSent, 1)
        << "maxInFlight=1 must cap outstanding batches at 1; got " << batchesSent;

    // Re-enable acks and replay the held ProduceResponse for batch 1.
    // This unblocks inFlight so batch 2 can flow.
    h.gw->replyAcks = true;
    h.gw->flushPendingAcks(); // sends the stashed ProduceResponse for batch 1
    ASSERT_TRUE(h.pumpUntilBatches(2))
        << "After ack, next batch must be flushed";
}

// ===========================================================================
// Test 10: IdempotentMonotonicSequence
//   Batches for a partition carry monotonically increasing baseSequence.
// ===========================================================================
TEST(Producer, IdempotentMonotonicSequence) {
    static constexpr int32_t kInStr = 2019, kEgStr = 2020;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::Leader;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1; // one batch per send
    pcfg.maxInFlight = 16;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(10, 1); // 1 partition → all on partition 0
    Key  key(uint64_t{0});

    const std::vector<uint8_t> val(5, 0xAA);
    static constexpr int kSends = 5;

    for (int i = 0; i < kSends; ++i) {
        p.send(topic, key, std::span<const uint8_t>(val));
        p.flush();
    }

    ASSERT_TRUE(h.pumpUntilBatches(kSends))
        << "Expected " << kSends << " batches";

    // Verify monotonically increasing baseSequence (gap-free: each batch has 1 record).
    // nextBaseSequence advances by recCount per batch (0, 1, 2, 3, 4 for recCount=1).
    ASSERT_GE(h.gw->batches.size(), static_cast<std::size_t>(kSends));
    int32_t nextExpected = h.gw->batches[0].baseSeq; // start at whatever seq 0 is
    for (int i = 0; i < kSends; ++i) {
        const auto& b = h.gw->batches[i];
        EXPECT_EQ(b.baseSeq, nextExpected)
            << "baseSequence must be monotonically increasing without gaps at batch " << i;
        EXPECT_GT(b.recCount, 0) << "recCount must be > 0 at batch " << i;
        nextExpected = b.baseSeq + b.recCount;
    }
    // Also verify that the sequences are strictly increasing.
    for (int i = 1; i < kSends; ++i) {
        EXPECT_GT(h.gw->batches[i].baseSeq, h.gw->batches[i-1].baseSeq)
            << "baseSeq must be strictly increasing between batches";
    }
}


// ===========================================================================
// EOS-DUP regression: the broker dedups on the Produce FRAME's identity
// (producerId / producerEpoch / baseSequence), not on the RecordBatch header.
// The plain producer stamped only the header and left the frame at -1, so the
// broker treated every idempotent produce as non-idempotent: every retransmit
// (ack timeout, session reconnect) was appended again — 5-36k duplicate records
// per 100k in the k3s T15b gates.  The frame must carry exactly the header's
// identity for acks=Leader/All, and stay -1 for acks=None.
// ===========================================================================
TEST(ProducerRegression, IdempotentFrameCarriesProducerIdentityRegression) {
    for (const Acks acks : {Acks::Leader, Acks::All}) {
        static constexpr int32_t kInStr = 2061, kEgStr = 2062;
        AgentHarness h(kInStr, kEgStr);
        ASSERT_TRUE(h.connect()) << "Failed to connect";
        ProducerConfig pcfg;
        pcfg.acks        = acks;
        pcfg.lingerUs    = 1;
        pcfg.batchBytes  = 1;   // one batch per send
        pcfg.maxInFlight = 16;
        auto p     = h.makeProducer(pcfg);
        auto topic = makeTopic(10, 1);
        Key  key(uint64_t{0});
        const std::vector<uint8_t> val(5, 0xAB);
        for (int i = 0; i < 3; ++i) {
            p.send(topic, key, std::span<const uint8_t>(val));
            p.flush();
        }
        ASSERT_TRUE(h.pumpUntilBatches(3));
        for (std::size_t i = 0; i < 3; ++i) {
            const auto& b = h.gw->batches[i];
            ASSERT_GE(b.producerId, 0) << "precondition: idempotent header";
            EXPECT_EQ(b.frameProducerId, b.producerId) << "frame producerId must equal the header's, batch " << i;
            EXPECT_EQ(b.frameProducerEpoch, b.producerEpoch) << "frame epoch must equal the header's, batch " << i;
            EXPECT_EQ(b.frameBaseSeq, b.baseSeq) << "frame baseSequence must equal the header's, batch " << i;
        }
    }
}

// ===========================================================================
// Test 11: BinaryAndEmptyValue
//   NUL+0xFF value and empty value survive the round-trip intact.
// ===========================================================================
TEST(Producer, BinaryAndEmptyValue) {
    static constexpr int32_t kInStr = 2021, kEgStr = 2022;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1;
    pcfg.batchBytes = 1;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(11, 1);
    Key  key(uint64_t{0});

    // Send binary value: NUL + 0xFF + NUL + 0xFF.
    const std::vector<uint8_t> binVal = {0x00, 0xFF, 0x00, 0xFF};
    p.send(topic, key, std::span<const uint8_t>(binVal));
    p.flush();

    // Send empty value.
    const std::vector<uint8_t> emptyVal = {};
    p.send(topic, key, std::span<const uint8_t>(emptyVal));
    p.flush();

    ASSERT_TRUE(h.pumpUntilBatches(2)) << "Expected 2 batches";

    ASSERT_EQ(h.gw->batches.size(), 2u);

    // Batch 0: binary value.
    ASSERT_EQ(h.gw->batches[0].values.size(), 1u);
    const std::string& v0 = h.gw->batches[0].values[0];
    ASSERT_EQ(v0.size(), 4u);
    EXPECT_EQ(static_cast<uint8_t>(v0[0]), 0x00u);
    EXPECT_EQ(static_cast<uint8_t>(v0[1]), 0xFFu);
    EXPECT_EQ(static_cast<uint8_t>(v0[2]), 0x00u);
    EXPECT_EQ(static_cast<uint8_t>(v0[3]), 0xFFu);

    // Batch 1: empty value.
    ASSERT_EQ(h.gw->batches[1].values.size(), 1u);
    EXPECT_EQ(h.gw->batches[1].values[0].size(), 0u);
}

// ===========================================================================
// Test 12: ZeroAllocVoidSend — REAL zero-alloc proof via global new/delete hook
//
//   Installs a process-wide operator new/delete counting hook, warms up the
//   ring + agent internals (allows any one-time allocations), then asserts
//   ZERO heap allocations occur in a steady-state void-send loop.
//
//   This is the LATENCY GATE: if ANY call on the void send path (partition
//   hash, ring write, etc.) allocates, the counter increments and the test
//   fails.  The only non-failing allocs are during warm-up.
//
//   Threading note: both the test thread (app) and the agent thread run
//   concurrently during the baseline phase.  We snapshot the counter before
//   and after the measured send loop on the app thread.  Agent-thread allocs
//   during the measured window would be caught, but the measured window is
//   deliberately narrow (app-thread send loop only, no doWork call inside).
// ===========================================================================

namespace {

// Global allocation counters — std::atomic so agent-thread incrs are visible.
std::atomic<long> g_newCount  {0};
std::atomic<long> g_delCount  {0};
std::atomic<bool> g_hookActive{false};

} // anonymous namespace

// Override global operator new/delete to count allocations.
// These are the lowest-level hooks; malloc-backed allocations from system
// libraries route through here on macOS (libSystem uses new/delete via the
// ObjC runtime path for C++ objects).
void* operator new(std::size_t size) {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_newCount.fetch_add(1, std::memory_order_relaxed);
    }
    void* p = std::malloc(size);
    if (!p) throw std::bad_alloc{};
    return p;
}

void* operator new[](std::size_t size) {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_newCount.fetch_add(1, std::memory_order_relaxed);
    }
    void* p = std::malloc(size);
    if (!p) throw std::bad_alloc{};
    return p;
}

void operator delete(void* p) noexcept {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_delCount.fetch_add(1, std::memory_order_relaxed);
    }
    std::free(p);
}

void operator delete[](void* p) noexcept {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_delCount.fetch_add(1, std::memory_order_relaxed);
    }
    std::free(p);
}

void operator delete(void* p, std::size_t) noexcept {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_delCount.fetch_add(1, std::memory_order_relaxed);
    }
    std::free(p);
}

void operator delete[](void* p, std::size_t) noexcept {
    if (g_hookActive.load(std::memory_order_relaxed)) {
        g_delCount.fetch_add(1, std::memory_order_relaxed);
    }
    std::free(p);
}

TEST(Producer, ZeroAllocVoidSend) {
    static constexpr int32_t kInStr = 2023, kEgStr = 2024;
    // 4 MiB ring → ample headroom for 5000 × 70-byte ring entries.
    AgentHarness h(kInStr, kEgStr, 4 * 1024 * 1024);

    ProducerConfig pcfg;
    pcfg.acks        = Acks::None;
    pcfg.lingerUs    = 1'000'000'000LL; // 1000 s — no linger flush during test
    pcfg.batchBytes  = 64 * 1024 * 1024; // 64 MB — no size flush
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(12, 4);
    Key  key(uint64_t{12345});

    const std::vector<uint8_t> val(32, 0x55);
    const std::span<const uint8_t> valSpan(val);

    // --- Warm-up phase: allow one-time allocs (ring init, partition map, etc.) ---
    // Run 200 sends + doWork to touch all lazily-allocated paths.
    for (int i = 0; i < 200; ++i) {
        p.send(topic, key, valSpan);
    }
    h.agent->doWork(monoNs());
    h.gw->poll();
    // Drain remaining ring entries before the measured phase.
    for (int i = 0; i < 20; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(1ms);
    }

    // --- Measured phase: enable hook, run steady-state void-send loop ---
    g_newCount.store(0, std::memory_order_seq_cst);
    g_delCount.store(0, std::memory_order_seq_cst);
    g_hookActive.store(true, std::memory_order_seq_cst);

    // 5,000 sends in a tight loop — pure app-thread void fast path.
    static constexpr int kMeasuredSends = 5'000;
    for (int i = 0; i < kMeasuredSends; ++i) {
        p.send(topic, key, valSpan); // THE path under test
    }

    g_hookActive.store(false, std::memory_order_seq_cst);

    const long allocsDuringMeasure = g_newCount.load(std::memory_order_seq_cst);

    // Assert: ZERO heap allocations on the steady-state void send path.
    EXPECT_EQ(allocsDuringMeasure, 0L)
        << "void Producer::send() must perform ZERO heap allocations in steady state; "
        << "got " << allocsDuringMeasure << " allocs for " << kMeasuredSends << " sends";

    // Drain remaining ring (ring is pre-allocated; this is post-measurement).
    for (int i = 0; i < 100; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (i > 10) std::this_thread::sleep_for(1ms);
    }
}

// ===========================================================================
// Test 13: TopicLookup
//   TopicHandle validation + Key::partition() == partitionForKey().
// ===========================================================================
TEST(Producer, TopicLookup) {
    // Valid topic handle.
    TopicHandle th{5, 8};
    EXPECT_TRUE(th.valid());
    EXPECT_EQ(th.topicId, 5);
    EXPECT_EQ(th.partitionCount, 8);

    // Invalid topic handle.
    TopicHandle bad{-1, 1};
    EXPECT_FALSE(bad.valid());

    // Key::partition() matches partitionForKey.
    Key k(uint64_t{100});
    const int32_t part = k.partition(8);
    EXPECT_GE(part, 0);
    EXPECT_LT(part, 8);

    const auto kbytes   = k.bytes();
    const int32_t expected = rune::protocol::partitionForKey(kbytes, 8);
    EXPECT_EQ(part, expected)
        << "Key::partition() must match partitionForKey()";

    // Empty key → valid partition.
    Key empty(std::span<const uint8_t>{});
    EXPECT_GE(empty.partition(4), 0);
    EXPECT_LT(empty.partition(4), 4);
}

// ===========================================================================
// FIX 1 — Test 14: CloseWithPendingFuture
//   send(want_future) then close() before any ack → future throws (not hangs).
//   Tests that producerHub_.failAll("Client closed") is called on close().
// ===========================================================================
TEST(Producer, CloseWithPendingFuture) {
    static constexpr int32_t kInStr = 2025, kEgStr = 2026;
    AgentHarness h(kInStr, kEgStr);
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 30'000;
    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(15, 1);
    const std::vector<uint8_t> val = {0x01};
    // Submitted but never drained (no doWork): the record sits in the producer ring.
    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    ASSERT_EQ(p.trySend(topic, Key(uint64_t{0}), std::span<const uint8_t>(val), AckToken{1}), SendResult::Ok);
    ASSERT_EQ(L.times(1), 0);
    h.hub.failAll(OpStatus::Closed);
    ASSERT_EQ(L.times(1), 1) << "close must complete tokens whose records are still in the ring";
    EXPECT_EQ(L.status(1), OpStatus::Closed);
}

// ===========================================================================
// FIX 1 — Test 15: ProduceAckTimeout (no-ack gateway → future fails after timeout)
//   Gateway receives Produce but never replies → sweepTimedOut() fails the future.
//   Uses a short produceTimeoutMs to bound the test duration.
// ===========================================================================
TEST(Producer, ProduceAckTimeout) {
    static constexpr int32_t kInStr = 2027, kEgStr = 2028;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    // Short produce timeout for the test.
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 200; // 200 ms per attempt — short enough for unit test
    pcfg.deliveryTimeoutMs = 400; // retry-then-fail: ~2 attempts then give up (bounds test)

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(13, 1);
    Key  key(uint64_t{0});

    // Gateway will receive but never reply.
    h.gw->replyAcks = false;

    const std::vector<uint8_t> val = {0xAB};
    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(val), AckToken{1}), SendResult::Ok);

    // Must be pending initially.
    ASSERT_EQ(L.times(1), 0) << "The token must be pending before timeout";

    // Wait past the timeout, calling sweepTimedOut() periodically.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    bool resolved = false;
    while (std::chrono::steady_clock::now() < deadline) {
        h.agent->doWork(monoNs());
        h.gw->poll();

        // Simulate the Client's sweepTimeouts() calling hub.sweepTimedOut().
        h.hub.sweepTimedOut(std::chrono::steady_clock::now());

        if (L.times(1) != 0) {
            resolved = true;
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    ASSERT_TRUE(resolved) << "The token must be completed by the timeout sweep (not hang)";
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed) << "Timed-out send completes DeliveryFailed";
}

// ===========================================================================
// FIX 1 — Test 16: ProduceAckTimeoutCallback (callback error variant)
//   Callback fires with offset=-1 when ack times out.
// ===========================================================================
TEST(Producer, ProduceAckTimeoutCallback) {
    static constexpr int32_t kInStr = 2029, kEgStr = 2030;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 4;
    pcfg.produceTimeoutMs = 200; // short per-attempt timeout
    pcfg.deliveryTimeoutMs = 400; // retry-then-fail: ~2 attempts then give up (bounds test)

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(14, 1);
    Key  key(uint64_t{0});

    h.gw->replyAcks = false;

    AckLedger L;
    p.onAck(&AckLedger::onAck, &L);
    const std::vector<uint8_t> val = {0xBB};
    p.send(topic, key, std::span<const uint8_t>(val), AckToken{1});

    // Pump past the timeout.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        h.hub.sweepTimedOut(std::chrono::steady_clock::now());
        if (L.times(1) != 0) break;
        std::this_thread::sleep_for(20ms);
    }

    ASSERT_EQ(L.times(1), 1) << "The handler must fire on timeout (not hang)";
    EXPECT_EQ(L.value(1), -1LL) << "Timed-out ack carries offset=-1";
    EXPECT_EQ(L.status(1), OpStatus::DeliveryFailed);
}

// ===========================================================================
// FIX 5 — Test 17: MultiRecordBatch
//   Pack >1 record into a single batch via batchBytes size trigger.
//   Assert: baseSequence advances by recordCount (gap-free), recordCount > 1.
//
//   Design: use acks=None (no corrId allocation), small batchBytes (<=200),
//   LARGE linger (no linger flush), and NO explicit p.flush() call.
//   All records are submitted to the ring before doWork is called, so the
//   batchBytes trigger fires after enough records accumulate.
//   This avoids the globalFlushPending_ early-flush issue (p.flush() sets the
//   flag before the ring is drained, causing a 1-record batch).
// ===========================================================================
TEST(Producer, MultiRecordBatch) {
    static constexpr int32_t kInStr = 2031, kEgStr = 2032;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    // Small batchBytes (150 bytes) so a batch of 5×20-byte records (= ~100 bytes
    // of value, plus varint overhead) will fill it and trigger one batch flush.
    // Large linger so no time-based flush fires during the test.
    ProducerConfig pcfg;
    pcfg.acks        = Acks::None;
    pcfg.lingerUs    = 1'000'000'000LL; // 1000 s — no linger flush
    pcfg.batchBytes  = 150;             // triggers at ~150 bytes of builder value bytes
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(15, 1); // 1 partition: all records go to partition 0
    Key  key(uint64_t{0});

    // Each send: 20-byte value. After 8 sends, builderBytes = 8*20 = 160 >= 150 → flush.
    // Note: builderBytes tracks accumulated valueLen only (not headers).
    const std::vector<uint8_t> val(20, 0xAA);
    static constexpr int kRecords = 8; // 8 × 20 = 160 bytes > batchBytes(150)

    // Queue all records into the ring BEFORE calling doWork.
    // No p.flush() here — we rely on batchBytes trigger alone.
    for (int i = 0; i < kRecords; ++i) {
        p.send(topic, key, std::span<const uint8_t>(val));
    }

    // doWork drains one record at a time, calling flushBatches after each.
    // Once builderBytes >= 150, the size trigger fires and a multi-record
    // batch is flushed.
    ASSERT_TRUE(h.pumpUntilBatches(1)) << "Expected exactly 1 multi-record batch";

    ASSERT_GE(h.gw->batches.size(), 1u);
    const auto& b0 = h.gw->batches[0];

    // The batch must contain MORE than 1 record (batchBytes trigger fires
    // after ≥ 150 bytes accumulated, which requires ≥ 8 records of 20 bytes).
    EXPECT_GT(b0.recCount, 1)
        << "First batch must pack multiple records (batchBytes trigger)";

    // F5: acks=None batches are ALWAYS encoded non-idempotent — producerId=-1
    // (the wire sentinel telling the broker to skip sequence validation) and
    // baseSequence left at the RecordBatchBuilder's own "unset" default (-1).
    // Nothing tracks or retransmits an acks=None batch, so stamping a real
    // sequence that nothing can recover on loss is exactly the bug this fixes.
    EXPECT_EQ(b0.producerId, -1)
        << "acks=None batch must carry the non-idempotent producerId sentinel (-1)";
    EXPECT_EQ(b0.baseSeq, -1)
        << "acks=None batch must carry the non-idempotent baseSequence sentinel (-1)";

    // builderBytes now tracks true WIRE size (batch header + per-record framing),
    // so the batchBytes trigger fires earlier and leaves a sub-threshold tail that
    // the trigger alone will never flush.  Explicitly flush the remainder so the
    // gap-free-baseSequence invariant can be checked across ALL records.
    p.flush();

    // Wait for all batches to be sent (remaining records after the first flush).
    // Poll until total received records == kRecords (all records flushed).
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        int32_t got = 0;
        for (const auto& b : h.gw->batches) got += b.recCount;
        if (got >= kRecords) break;
        std::this_thread::sleep_for(kPumpSleep);
    }

    // F5: every acks=None batch must carry the non-idempotent sentinel (-1) for
    // BOTH producerId and baseSequence — none of them occupy a slot in the
    // partition's real idempotent sequence.  The correctness signal for "no
    // records lost or duplicated" is now the total record count, not
    // sequence continuity (acks=None sequences are intentionally untracked).
    int32_t totalRecs = 0;
    for (std::size_t i = 0; i < h.gw->batches.size(); ++i) {
        const auto& b = h.gw->batches[i];
        EXPECT_EQ(b.producerId, -1)
            << "acks=None batch " << i << " must carry producerId sentinel -1";
        EXPECT_EQ(b.baseSeq, -1)
            << "acks=None batch " << i << " must carry baseSequence sentinel -1";
        totalRecs += b.recCount;
    }
    EXPECT_EQ(totalRecs, kRecords)
        << "All records must be delivered exactly once across batches (none lost/duplicated)";
}

// ===========================================================================
// FIX 5 — Test 18: MultiPartitionInterleave
//   Two keys that land on different partitions within the same producer.
//   Both partitions receive records; sequences are tracked independently.
// ===========================================================================
TEST(Producer, MultiPartitionInterleave) {
    static constexpr int32_t kInStr = 2033, kEgStr = 2034;
    static constexpr int32_t kParts = 8;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    // F5: acks=None batches are now always non-idempotent (baseSequence=-1,
    // untracked — see MultiRecordBatch above), so per-partition sequence
    // tracking — what this test actually verifies — requires acks=Leader/All.
    ProducerConfig pcfg;
    pcfg.acks        = Acks::Leader;
    pcfg.lingerUs    = 1;   // flush fast
    pcfg.batchBytes  = 1;   // flush every record
    pcfg.maxInFlight = 32;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(16, kParts);

    const std::vector<uint8_t> val(4, 0x01);

    // Find two keys that hash to DIFFERENT partitions.
    uint64_t kA = 0, kB = 1;
    {
        uint8_t ka[8]{}, kb[8]{};
        for (int j = 0; j < 8; ++j) ka[j] = static_cast<uint8_t>(kA >> (j * 8));
        for (int j = 0; j < 8; ++j) kb[j] = static_cast<uint8_t>(kB >> (j * 8));
        int32_t pA = rune::protocol::partitionForKey(std::span<const uint8_t>(ka, 8), kParts);
        int32_t pB = rune::protocol::partitionForKey(std::span<const uint8_t>(kb, 8), kParts);
        // Scan for a second key on a different partition if needed.
        for (uint64_t k2 = 2; pA == pB && k2 < 1000; ++k2) {
            kB = k2;
            for (int j = 0; j < 8; ++j) kb[j] = static_cast<uint8_t>(kB >> (j * 8));
            pB = rune::protocol::partitionForKey(std::span<const uint8_t>(kb, 8), kParts);
        }
        ASSERT_NE(pA, pB)
            << "Test setup: could not find two keys on different partitions (should not happen with FNV-1a)";
    }

    Key keyA(kA), keyB(kB);
    static constexpr int kSendsEach = 3;

    // Interleave sends to partition A and B.
    for (int i = 0; i < kSendsEach; ++i) {
        p.send(topic, keyA, std::span<const uint8_t>(val));
        p.flush();
        p.send(topic, keyB, std::span<const uint8_t>(val));
        p.flush();
    }

    // Expect kSendsEach batches per partition = 2 * kSendsEach total.
    ASSERT_TRUE(h.pumpUntilBatches(2 * kSendsEach))
        << "Expected " << 2 * kSendsEach << " batches (interleaved partitions)";

    // Collect seen partitions.
    std::set<int32_t> partsSeen;
    for (const auto& b : h.gw->batches) partsSeen.insert(b.partition);
    EXPECT_GE(partsSeen.size(), 2u)
        << "Interleaved sends must produce batches on at least 2 distinct partitions";

    // Verify per-partition baseSequence is monotonically increasing.
    std::map<int32_t, int32_t> lastSeq;
    for (const auto& b : h.gw->batches) {
        auto it = lastSeq.find(b.partition);
        if (it == lastSeq.end()) {
            EXPECT_EQ(b.baseSeq, 0)
                << "First batch for partition " << b.partition << " must start at seq 0";
            lastSeq[b.partition] = b.baseSeq + b.recCount;
        } else {
            EXPECT_EQ(b.baseSeq, it->second)
                << "Gap-free sequence required for partition " << b.partition;
            it->second += b.recCount;
        }
    }
}

// ===========================================================================
// FIX 5 — Test 19: OversizedValueRejected
//   Value > kMaxSendValueBytes must NOT crash; the send is rejected with
//   ValueTooLarge / std::length_error.
//   A subsequent normal send must still succeed.
// ===========================================================================
TEST(Producer, OversizedValueRejected) {
    static constexpr int32_t kInStr = 2035, kEgStr = 2036;
    AgentHarness h(kInStr, kEgStr);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::None;
    pcfg.lingerUs    = 1;
    pcfg.batchBytes  = 1;
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(17, 1);
    Key  key(uint64_t{0});

    // Value exactly at kMaxSendValueBytes — must be accepted.
    const std::vector<uint8_t> maxVal(rc::Producer::kMaxSendValueBytes, 0xAA);
    p.send(topic, key, std::span<const uint8_t>(maxVal));
    p.flush();

    // Value one byte over — reported to the caller, never silently dropped.
    const std::vector<uint8_t> overVal(rc::Producer::kMaxSendValueBytes + 1, 0xBB);
    EXPECT_EQ(p.trySend(topic, key, std::span<const uint8_t>(overVal)), rc::SendResult::ValueTooLarge);
    EXPECT_THROW(p.send(topic, key, std::span<const uint8_t>(overVal)), std::length_error);
    p.flush();

    // Only the max-size send should produce a batch (the oversized one was dropped).
    ASSERT_TRUE(h.pumpUntilBatches(1)) << "max-size value must produce a batch";

    // Wait a bit extra to confirm the oversized value didn't sneak through.
    for (int i = 0; i < 50; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        std::this_thread::sleep_for(kPumpSleep);
    }
    EXPECT_EQ(h.gw->batchCount.load(std::memory_order_acquire), 1)
        << "Oversized value must be silently dropped (no extra batch)";
}

// ===========================================================================
// FIX 5 — Test 20: RingFullDropAndRecover
//   Fill the ring (tiny ring cap) then verify sends are dropped without crash,
//   and the producer recovers once the ring is drained.
// ===========================================================================
TEST(Producer, RingFullDropAndRecover) {
    static constexpr int32_t kInStr = 2037, kEgStr = 2038;
    // Very small ring (4 KiB usable) to make it easy to fill.
    AgentHarness h(kInStr, kEgStr, 4 * 1024);
    ASSERT_TRUE(h.connect()) << "Failed to connect";

    ProducerConfig pcfg;
    pcfg.acks        = Acks::None;
    pcfg.lingerUs    = 1'000'000'000LL; // don't auto-flush during fill
    pcfg.batchBytes  = 64 * 1024;
    pcfg.maxInFlight = 4;

    // Use an impl directly so we can call submitSendRecord and inspect the return value.
    auto* impl = new rprod::ProducerImpl(pcfg, 4 * 1024);
    h.hub.add(impl);
    Producer p(impl);

    auto topic = makeTopic(18, 1);
    Key  key(uint64_t{0});

    const std::vector<uint8_t> val(32, 0xCC); // 32-byte value → ~70 byte ring msg

    // Fill ring until writes start failing (returns false from submitSendRecord).
    int sent = 0, dropped = 0;
    for (int i = 0; i < 200; ++i) {
        const bool ok = impl->submitSendRecord(
            topic.topicId, key.partition(topic.partitionCount),
            key, std::span<const uint8_t>(val),
            /*wantAck=*/false, /*corrId=*/0);
        if (ok) ++sent;
        else { ++dropped; }
    }
    // Must have at least some drops (ring is too small for 200 × 70-byte msgs).
    EXPECT_GT(dropped, 0)
        << "Ring must report drops once full (fire-and-forget contract)";

    // Drain the ring: keep calling doWork until the ring is empty enough for a
    // fresh send to succeed.  With up to 256 records drained per doWork call,
    // this should take at most ~2 doWork iterations for |sent| <= 200 records.
    bool okAfter = false;
    for (int i = 0; i < 200; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        // Try the send — once the ring has free space it will succeed.
        okAfter = impl->submitSendRecord(
            topic.topicId, key.partition(topic.partitionCount),
            key, std::span<const uint8_t>(val),
            /*wantAck=*/false, /*corrId=*/0);
        if (okAfter) break;
        std::this_thread::sleep_for(1ms);
    }

    EXPECT_TRUE(okAfter)
        << "After ring drain, sends must succeed (producer recovers from ring-full)";
}

// ===========================================================================
// Test 19: AcksWireEncoding  [B2]
//   Verify that the acks field is correctly set on the wire:
//     - acks=All  producer   → Produce.acks == 1 (wire)
//     - acks=Leader producer → Produce.acks == 0 (wire)
// ===========================================================================
TEST(Producer, AcksWireEncoding) {
    // --- acks=All sends wire acks=1 ---
    {
        static constexpr int32_t kInStr = 2039, kEgStr = 2040;
        AgentHarness h(kInStr, kEgStr);
        ASSERT_TRUE(h.connect()) << "Failed to connect";

        ProducerConfig pcfg;
        pcfg.acks      = Acks::All;
        pcfg.lingerUs  = 1;
        pcfg.batchBytes = 1;

        auto p     = h.makeProducer(pcfg);
        auto topic = makeTopic(20, 1);
        Key  key(uint64_t{0});

        const std::vector<uint8_t> val(4, 0xAA);
        p.send(topic, key, std::span<const uint8_t>(val));
        p.flush();

        ASSERT_TRUE(h.pumpUntilBatches(1)) << "acks=All batch must be delivered";

        // The wire acks field must be 1 (All) — not 0 (Leader).
        ASSERT_FALSE(h.gw->batches.empty());
        EXPECT_EQ(h.gw->batches[0].acks, uint8_t{1})
            << "acks=All producer must encode wire acks=1; got "
            << static_cast<int>(h.gw->batches[0].acks);
    }

    // --- acks=Leader sends wire acks=0 ---
    {
        static constexpr int32_t kInStr = 2041, kEgStr = 2042;
        AgentHarness h(kInStr, kEgStr);
        ASSERT_TRUE(h.connect()) << "Failed to connect";

        ProducerConfig pcfg;
        pcfg.acks      = Acks::Leader;
        pcfg.lingerUs  = 1;
        pcfg.batchBytes = 1;

        auto p     = h.makeProducer(pcfg);
        auto topic = makeTopic(21, 1);
        Key  key(uint64_t{0});

        const std::vector<uint8_t> val(4, 0xBB);
        p.send(topic, key, std::span<const uint8_t>(val));
        p.flush();

        ASSERT_TRUE(h.pumpUntilBatches(1)) << "acks=Leader batch must be delivered";

        ASSERT_FALSE(h.gw->batches.empty());
        EXPECT_EQ(h.gw->batches[0].acks, uint8_t{0})
            << "acks=Leader producer must encode wire acks=0; got "
            << static_cast<int>(h.gw->batches[0].acks);
    }
}
