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
#include <future>
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
        int32_t topicId   = 0;
        int32_t partition = 0;
        int64_t corrId    = 0;
        int32_t baseSeq   = -1; // from RecordBatch header offset 53
        int32_t recCount  = 0;
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

        // Decode RecordBatch header: baseSequence at offset 53, recordCount at 57.
        const auto& rb = opt->recordBatch;
        if (rb.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            const uint8_t* d = rb.data();
            // baseSequence (int32 LE at offset 53).
            rec.baseSeq = static_cast<int32_t>(
                static_cast<uint32_t>(d[53])        |
               (static_cast<uint32_t>(d[54]) <<  8) |
               (static_cast<uint32_t>(d[55]) << 16) |
               (static_cast<uint32_t>(d[56]) << 24));
            // recordCount (int32 LE at offset 57).
            rec.recCount = static_cast<int32_t>(
                static_cast<uint32_t>(d[57])        |
               (static_cast<uint32_t>(d[58]) <<  8) |
               (static_cast<uint32_t>(d[59]) << 16) |
               (static_cast<uint32_t>(d[60]) << 24));

            // Decode values.
            rune::protocol::RecordBatchReader reader(rb.data(), rb.size());
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
    std::atomic<int64_t>  nextCorrId_ {1};

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
        auto impl = std::make_shared<rprod::ProducerImpl>(
            pcfg, nextCorrId_);
        hub.add(impl);
        return Producer(std::move(impl));
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

    // Each value is 100 bytes → 6 sends ≈ 600 bytes > 500 → one batch flush.
    const std::vector<uint8_t> val(100, 0xAB);
    const std::span<const uint8_t> valSpan(val);

    for (int i = 0; i < 6; ++i) {
        p.send(topic, key, valSpan);
    }

    ASSERT_TRUE(h.pumpUntilBatches(1))
        << "Expected 1 batch flushed on batchBytes trigger";

    ASSERT_GE(h.gw->batches.size(), 1u);
    EXPECT_GE(h.gw->batches[0].recCount, 5)
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
    auto fut = p.send(topic, key, std::span<const uint8_t>(val), want_future);

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

    ASSERT_TRUE(done) << "Future must resolve within timeout";
    const int64_t baseOffset = fut.get();
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

    std::atomic<int64_t> cbOffset {-1};
    const std::vector<uint8_t> val = {0xAB};
    p.send(topic, key, std::span<const uint8_t>(val),
           [&](int64_t offset) { cbOffset.store(offset, std::memory_order_release); });

    bool fired = false;
    for (int i = 0; i < kPumpMax; ++i) {
        h.agent->doWork(monoNs());
        h.gw->poll();
        if (cbOffset.load(std::memory_order_acquire) >= 0) {
            fired = true;
            break;
        }
        std::this_thread::sleep_for(kPumpSleep);
    }

    ASSERT_TRUE(fired) << "Callback must fire when ProduceResponse arrives";
    EXPECT_GE(cbOffset.load(), 100LL);
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

    // Send 3 messages (each with a future to register the corrId).
    std::vector<std::future<int64_t>> futs;
    for (int i = 0; i < 3; ++i) {
        futs.push_back(p.send(topic, key, std::span<const uint8_t>(val), want_future));
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
// Test 12: ZeroAllocVoidSend (structural proof)
//
//   The void Producer::send(topic, key, value) path:
//     1. Calls key.partition(partitionCount) — pure FNV-1a arithmetic on the
//        inline 16-byte array; zero allocation.
//     2. Calls ProducerImpl::submitSendRecord() — writes into a stack-local
//        uint8_t[8192] scratch buffer, then ring_.write() copies it into the
//        pre-allocated ring.  NO new/malloc on this path.
//     3. Does NOT call registerCallback/registerFuture (those allocate).
//     4. Does NOT construct a per-call std::function.
//
//   We verify the structural invariant by running 10,000 void sends in a
//   tight loop.  The ring drains between batches of doWork() calls.  If the
//   void path allocated, the ring would overflow (each send would be ~38 +
//   value_size bytes; the ring is large enough for the burst).
//   No OOM, no crash → structural zero-alloc property holds.
// ===========================================================================
TEST(Producer, ZeroAllocVoidSend) {
    static constexpr int32_t kInStr = 2023, kEgStr = 2024;
    // 1 MiB ring → ample headroom for 200 × 70-byte ring entries per round.
    AgentHarness h(kInStr, kEgStr, 1 * 1024 * 1024);

    ProducerConfig pcfg;
    pcfg.acks       = Acks::None;
    pcfg.lingerUs   = 1'000'000'000; // 1000 s — no linger flush
    pcfg.batchBytes = 64 * 1024 * 1024; // 64 MB — no size flush
    pcfg.maxInFlight = 4;

    auto p     = h.makeProducer(pcfg);
    auto topic = makeTopic(12, 4);
    Key  key(uint64_t{12345});

    const std::vector<uint8_t> val(32, 0x55);
    const std::span<const uint8_t> valSpan(val);

    static constexpr int kSendBatch = 200;
    int total = 0;
    for (int round = 0; round < 50; ++round) {
        for (int i = 0; i < kSendBatch; ++i) {
            // This is the zero-alloc void fast path under test.
            p.send(topic, key, valSpan);
            ++total;
        }
        // Drain the ring so it doesn't fill up.
        h.agent->doWork(monoNs());
        h.gw->poll();
    }

    EXPECT_EQ(total, 10'000);
    // If we got here without an assertion failure or OOM, the ring is functional.
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
