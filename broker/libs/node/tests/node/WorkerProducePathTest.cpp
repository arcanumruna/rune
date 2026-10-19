// ---------------------------------------------------------------------------
// WorkerProducePathTest.cpp — Worker per-worker ingress produce path (E5.1.2d).
//
// Tests the full produce path: WorkerIngress → handleProduceBatch → append →
// ack via ret_.  All tests use the flag-on path (perWorkerIngressActive_=true).
//
// Tests:
//
//   1. AcksLeader_ImmediateAck
//        Publish one Produce frame (acks=0) to the worker's Aeron stream.
//        doWork → handler runs handleProduceBatch → ack appears on ret_ immediately
//        (same cycle, no HW wait).  Assert: batch appended, ret_ has exactly one
//        ProduceResult, baseOffset correct.
//
//   2. AcksAll_DeferredAckOnHWAdvance
//        Publish one Produce frame (acks=1 = All) to the worker's stream.
//        Worker is single-node leader (minIsr=1, ISR=[self]) so HW advances
//        immediately after onLeaderLogAppended().  Assert: ack fires once HW
//        passes lastOffset (verified by draining ret_ and checking baseOffset).
//
//   3. G_A6_Backpressure_NoDropOnFullRet (G-A6 structural test)
//        Block the ret_ consumer (never drain it) so it fills up.
//        Sustain produce publishes well beyond ret_ capacity.
//        Assert:
//          (a) Worker stops appending once ret_ is full and an ack is owed
//              (the append count stops advancing — no new records enter the log).
//          (b) After draining ret_ the worker resumes appending in the next
//              doWork cycle and every produce gets exactly one ack (no drop).
//
// Fixture: one embedded IPC Aeron driver + one Aeron client.
// Uses disk-backed PartitionReplica (Log in a tmpdir) so replica->log() is valid.
// Mirrors the WorkerIngressFixture pattern from WorkerIngressTest.cpp.
// ---------------------------------------------------------------------------

#include <rune/node/EmbeddedDriver.h>
#include <rune/storage/testing/DemandChannelForTest.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/crypto/Aead.h>
#include <rune/protocol/client/Protocol.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <Subscription.h>
#include <concurrent/AtomicBuffer.h>

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::node {
namespace {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Temp-dir RAII
// ---------------------------------------------------------------------------
static fs::path makeTempDir(std::string_view tag = "") {
    static std::atomic<uint32_t> g_seq{0};
    auto seq  = g_seq.fetch_add(1, std::memory_order_relaxed);
    auto name = std::string("rune_wpp_") + std::string(tag) +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view tag = "") : path(makeTempDir(tag)) {
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// ---------------------------------------------------------------------------
// Minimal NullTransport for PartitionReplica construction.
// ---------------------------------------------------------------------------
class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override { return true; }
    bool sendOffsetsForLeaderEpochRequest(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochReqMsg&) override { return true; }
    bool sendOffsetsForLeaderEpochResponse(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochRespMsg&) override { return true; }
    bool sendReconcileRequest(rune::replication::NodeId,
                              const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendReconcileResponse(rune::replication::NodeId,
                               const rune::replication::FetchResponseMsg&) override { return true; }
};

// ---------------------------------------------------------------------------
// Helper: build a one-record RecordBatch.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(int32_t recordCount = 1) {
    rune::protocol::RecordBatchBuilder b(0, 1000LL);
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// ---------------------------------------------------------------------------
// Helper: encode a full Produce wire frame (as sent by the client).
// ---------------------------------------------------------------------------
static std::vector<uint8_t> encodeProduceFrame(
        int64_t sessionId, int64_t correlationId,
        int32_t topicId, int32_t partition, uint8_t acks,
        const std::vector<uint8_t>& batch)
{
    pc::Produce msg;
    msg.sessionId     = sessionId;
    msg.correlationId = correlationId;
    msg.topicId       = topicId;
    msg.partition     = partition;
    msg.partitionKnown = 1;
    msg.acks          = acks;
    msg.producerId    = -1;     // legacy (no idempotent)
    msg.producerEpoch = 0;
    msg.baseSequence  = -1;
    msg.key           = {};
    msg.recordBatch   = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> buf(msg.encodedSize());
    msg.encode(buf.data());
    return buf;
}

// ---------------------------------------------------------------------------
// WorkerProduceFixture — embedded IPC driver + one Aeron client.
// Mirrors WorkerIngressFixture exactly.
// ---------------------------------------------------------------------------
class WorkerProduceFixture : public ::testing::Test {
public:
    static constexpr int kPollIterMax = 100'000;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("fix_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        workerIngress_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    // -----------------------------------------------------------------------
    // makeIngress — subscribe a WorkerIngress on (channel, stream).
    // -----------------------------------------------------------------------
    std::unique_ptr<WorkerIngress> makeIngress(const std::string& channel,
                                               int32_t stream) {
        return std::make_unique<WorkerIngress>(aeron_, channel, stream);
    }

    // -----------------------------------------------------------------------
    // makeExclusivePub — resolve an ExclusivePublication.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::ExclusivePublication> makeExclusivePub(
            const std::string& channel, int32_t stream) {
        int64_t regId = aeron_->addExclusivePublication(channel, stream);
        std::shared_ptr<aeron::ExclusivePublication> pub;
        waitBounded([&]() {
            pub = aeron_->findExclusivePublication(regId);
            return pub != nullptr;
        }, "makeExclusivePub");
        return pub;
    }

    // -----------------------------------------------------------------------
    // offerBounded — offer raw bytes on a publication, retry until success.
    // -----------------------------------------------------------------------
    static bool offerBounded(aeron::ExclusivePublication& pub,
                             const uint8_t* data, std::size_t len) {
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(data),
            static_cast<aeron::util::index_t>(len));
        for (int i = 0; i < kPollIterMax; ++i) {
            int64_t pos = pub.offer(ab, 0, static_cast<aeron::util::index_t>(len));
            if (pos > 0) return true;
            if (pos == aeron::BACK_PRESSURED || pos == aeron::ADMIN_ACTION) {
                std::this_thread::sleep_for(100us);
                continue;
            }
            return false;
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // waitConnected — wait for a publication to connect.
    // -----------------------------------------------------------------------
    void waitConnected(aeron::ExclusivePublication& pub,
                       WorkerIngress& ingress) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        while (clk::now() < deadline && !pub.isConnected()) {
            ingress.poll(10, [](const uint8_t*, std::size_t, aeron::Header&) {});
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub.isConnected()) << "Publication never connected";
    }

    // -----------------------------------------------------------------------
    // makeLeaderReplica — create a single-node leader PartitionReplica
    // backed by a disk-based Log (so replica->log() is safe to call).
    // -----------------------------------------------------------------------
    static std::unique_ptr<rune::replication::PartitionReplica>
    makeLeaderReplica(int32_t topicId, int32_t partition,
                      rune::storage::Log& log,
                      NullTransport& transport)
    {
        rune::replication::PartitionConfig cfg{};
        cfg.topicId             = topicId;
        cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;

        auto replica = std::make_unique<rune::replication::PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            /*replicas=*/std::vector<rune::replication::NodeId>{1},
            /*isr=*/std::vector<rune::replication::NodeId>{1},
            &log, &transport);
        return replica;
    }

    std::shared_ptr<aeron::Aeron>        aeron_;
    std::unique_ptr<WorkerIngress>       workerIngress_;

private:
    template <typename Cond>
    void waitBounded(Cond&& cond, std::string_view label) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        for (int i = 0; i < kPollIterMax && clk::now() < deadline; ++i) {
            if (cond()) return;
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(cond()) << "waitBounded timed out: " << label;
    }

    void startDriver() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();

        // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
        // readiness pumping — aeron_driver_init already wrote cnc.dat and stamped
        // the heartbeat synchronously before the driver thread even starts.
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir;
        // Use a small term length so back-pressure fills quickly in test 3.
        cfg.termBufferBytes = 64 * 1024;
        driver_ = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
    }

    void stopDriver() {
        driver_.reset();
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    std::unique_ptr<TempDir>   tmpDir_;
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
};

// ===========================================================================
// TEST 1: AcksLeader_ImmediateAck
//
// Publish one Produce frame (acks=0) to the worker's Aeron stream.
// The worker decodes it, calls handleProduceBatch, appends to log, and
// immediately enqueues a ProduceResult ack on ret_.
// ===========================================================================
TEST_F(WorkerProduceFixture, AcksLeader_ImmediateAck)
{
    constexpr int32_t kStream    = 501;
    constexpr int32_t kTopicId   = 10;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    // Disk-backed Log + NullTransport + PartitionReplica.
    TempDir logDir("log1_");
    rune::storage::Log log(logDir.path.string());
    NullTransport      transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    // WorkerIngress subscription.
    WorkerIngress ingress(aeron_, kChannel, kStream);
    ASSERT_TRUE(ingress.resolved()) << "WorkerIngress did not resolve";

    // Worker: small rings for faster test.
    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(/*workerId=*/0, kRingBytes, kRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(&ingress, /*active=*/true);
    // Short ack timeout for tests (500 ms).
    worker.setAckTimeoutNs(500'000'000LL);

    // Publisher.
    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, ingress);

    // Encode and publish one Produce frame (acks=0 = Leader).
    auto batch = makeBatch(1);
    auto frame = encodeProduceFrame(
        /*sessionId=*/7, /*correlationId=*/42,
        kTopicId, kPartition, /*acks=*/0, batch);

    ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()))
        << "offer failed";

    // Drive doWork until the ack appears on ret_.
    std::size_t ackCount = 0;
    int64_t     ackedBase = -1;
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 10s;
    while (clk::now() < deadline && ackCount == 0) {
        worker.doWork(std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count());
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) {
                ++ackCount;
                ackedBase = ret.baseOffset;
                EXPECT_EQ(ret.errorCode, 0)     << "Unexpected error in ack";
                EXPECT_EQ(ret.sessionId, 7)      << "sessionId mismatch";
                EXPECT_EQ(ret.correlationId, 42) << "correlationId mismatch";
                EXPECT_EQ(ret.partition, kPartition);
            }
        });
        std::this_thread::sleep_for(100us);
    }

    EXPECT_EQ(ackCount, 1u)  << "Expected exactly one ack";
    EXPECT_EQ(ackedBase, 0)  << "First batch must start at offset 0";
    EXPECT_GT(log.logEndOffset(), 0) << "Log must have advanced past 0";
}

// ===========================================================================
// TEST 2: AcksAll_DeferredAckOnHWAdvance
//
// Publish one Produce frame with acks=1 (All).  The worker is a single-node
// leader (minIsr=1, ISR=[self]) so onLeaderLogAppended() advances the HW to
// cover the batch immediately.  After draining ret_ the ack should fire in
// the next doWork cycle once flushOwedAcksAll_ sees HW > lastOffset.
// ===========================================================================
TEST_F(WorkerProduceFixture, AcksAll_DeferredAckOnHWAdvance)
{
    constexpr int32_t kStream    = 502;
    constexpr int32_t kTopicId   = 11;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log2_");
    rune::storage::Log log(logDir.path.string());
    NullTransport      transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    WorkerIngress ingress(aeron_, kChannel, kStream);
    ASSERT_TRUE(ingress.resolved());

    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(/*workerId=*/1, kRingBytes, kRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(&ingress, /*active=*/true);
    worker.setAckTimeoutNs(500'000'000LL); // 500ms

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, ingress);

    auto batch = makeBatch(1);
    auto frame = encodeProduceFrame(
        /*sessionId=*/8, /*correlationId=*/99,
        kTopicId, kPartition, /*acks=*/1 /*All*/, batch);

    ASSERT_TRUE(offerBounded(*pub, frame.data(), frame.size()));

    std::size_t ackCount  = 0;
    int16_t     errCode   = -1;
    int64_t     ackedBase = -1;

    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 10s;
    while (clk::now() < deadline && ackCount == 0) {
        const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
        worker.doWork(now);
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) {
                ++ackCount;
                errCode   = ret.errorCode;
                ackedBase = ret.baseOffset;
            }
        });
        std::this_thread::sleep_for(100us);
    }

    EXPECT_EQ(ackCount, 1u)  << "acks=All: exactly one ack expected";
    EXPECT_EQ(errCode,  0)   << "acks=All ack must be OK (single-node leader, HW=1)";
    EXPECT_EQ(ackedBase, 0)  << "First batch starts at offset 0";
    EXPECT_GT(log.logEndOffset(), 0);
}

// ===========================================================================
// TEST 3: G_A6_Backpressure_NoDropOnFullRet
//
// G-A6 structural backpressure test.
//
// The G-A6 invariant is: the worker stops polling WorkerIngress when ret_ does
// NOT have space for another max-size message (hasSpaceForMaxMsg() == false).
// This ensures back-pressure propagates: Aeron term buffer fills → client
// offer() back-pressures.  No ack is ever dropped.
//
// Sizing: ret_ ring = 4 KiB.
//   maxMsgLength() = 4096 / 8 = 512 bytes.
//   hasSpaceForMaxMsg() fires when used > 4096 - 2*512 = 3072 bytes.
//   Each ack frame ≈ sizeof(WorkerReturnWire) + Agrona record header ≈ 144 bytes.
//   Backpressure fires after approx 3072/144 ≈ 21 acks.
//
// Scenario:
//   1. Wire up a worker with a TINY ret_ ring (4 KiB) so it fills quickly.
//   2. Publish kPhase1Count (100) produces WITHOUT draining ret_.  Drive doWork
//      each iteration.  The ring fills after ~21 acks; doWork stops polling
//      WorkerIngress once hasSpaceForMaxMsg() returns false.
//   3. Record LEO = N (acks that fit before ret_ filled, N << 100).
//   4. Run several more doWork cycles without draining; assert LEO stays at N.
//   5. Drain ret_ (exactly N acks, all errorCode=0).
//   6. After draining, the worker resumes polling.  Verify all remaining queued
//      Aeron frames (phase-1 remainder + new phase-3 frames) are processed and
//      each gets exactly one ack (no drops, no duplicates).
//
// ===========================================================================
TEST_F(WorkerProduceFixture, G_A6_Backpressure_NoDropOnFullRet)
{
    constexpr int32_t kStream    = 503;
    constexpr int32_t kTopicId   = 12;
    constexpr int32_t kPartition = 0;
    const std::string kChannel   = "aeron:ipc";

    TempDir logDir("log3_");
    rune::storage::Log log(logDir.path.string());
    NullTransport      transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, log, transport);

    WorkerIngress ingress(aeron_, kChannel, kStream);
    ASSERT_TRUE(ingress.resolved());

    // Tiny ret_ ring (4 KiB): fills after ~21 acks → triggers hasSpaceForMaxMsg fast.
    // Large ingress ring (32 MiB): worker-op ingress is separate from Aeron ingress.
    constexpr std::size_t kRetRingBytes     = 4 * 1024;
    constexpr std::size_t kIngressRingBytes = 32 * 1024 * 1024;
    Worker worker(/*workerId=*/2, kIngressRingBytes, kRetRingBytes);
    worker.setOwnedDirect({replica.get()});
    worker.setWorkerIngress(&ingress, /*active=*/true);
    worker.setAckTimeoutNs(2'000'000'000LL); // 2 s timeout

    auto pub = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub);
    waitConnected(*pub, ingress);

    auto batch     = makeBatch(1);
    int64_t correlId = 1000;

    // --- Phase 1: Publish 100 produces, drive doWork WITHOUT draining ret_.
    // After ~21 acks the ring fills; subsequent doWork calls stop polling ingress.
    constexpr int kPhase1Count = 100;
    for (int i = 0; i < kPhase1Count; ++i) {
        auto frame = encodeProduceFrame(
            /*sessionId=*/9, correlId++, kTopicId, kPartition, /*acks=*/0, batch);
        offerBounded(*pub, frame.data(), frame.size());

        using clk = std::chrono::steady_clock;
        const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
        worker.doWork(now);
    }

    // leoAfterPhase1 = N: appends that happened before hasSpaceForMaxMsg() fired.
    const int64_t leoAfterPhase1 = log.logEndOffset();
    EXPECT_GT(leoAfterPhase1, 0LL)
        << "At least some appends must have happened before ret_ filled";
    EXPECT_LT(leoAfterPhase1, static_cast<int64_t>(kPhase1Count))
        << "ret_ must fill before all 100 batches are processed (G-A6 kicks in)";

    // --- G-A6 hold: pump more doWork cycles without draining → LEO must not advance.
    {
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 200ms;
        while (clk::now() < dl) {
            const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            worker.doWork(now);
            std::this_thread::sleep_for(1ms);
        }
    }
    EXPECT_EQ(log.logEndOffset(), leoAfterPhase1)
        << "G-A6: worker must hold when ret_ is full — no new LEO advance";

    // --- Phase 2: Resume.
    // Send kPhase3Count more produces, then drain ret_ (which will simultaneously
    // flush retOverflow_ and process the remaining queued Aeron frames).
    // The worker resumes naturally once ret_ has space after draining.
    constexpr int kPhase3Count = 10;
    for (int i = 0; i < kPhase3Count; ++i) {
        auto frame3 = encodeProduceFrame(
            /*sessionId=*/9, correlId++, kTopicId, kPartition, /*acks=*/0, batch);
        ASSERT_TRUE(offerBounded(*pub, frame3.data(), frame3.size()))
            << "Phase 3 offer failed at i=" << i;
    }

    // Drive doWork + drainReturn until all kPhase1Count + kPhase3Count acks have
    // been received.  This verifies:
    //   (a) No ack is dropped (exactly kPhase1Count + kPhase3Count acks total).
    //   (b) Each produce gets exactly one ack (no duplicates).
    //   (c) All acks are OK (errorCode=0).
    const std::size_t kTotalExpected =
        static_cast<std::size_t>(kPhase1Count) +
        static_cast<std::size_t>(kPhase3Count);
    std::size_t totalAcks = 0;
    using clk = std::chrono::steady_clock;
    const auto deadline2 = clk::now() + 10s;
    while (clk::now() < deadline2 && totalAcks < kTotalExpected) {
        const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
        worker.doWork(now);
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) {
                EXPECT_EQ(ret.errorCode, 0) << "All acks must be OK (no errors)";
                ++totalAcks;
            }
        });
        std::this_thread::sleep_for(100us);
    }
    EXPECT_EQ(totalAcks, kTotalExpected)
        << "Total acks must equal total produces (no drops, no duplicates)";

    // Final invariant: total acks == total LEO.
    const int64_t totalLeo = log.logEndOffset();
    EXPECT_EQ(static_cast<int64_t>(totalAcks), totalLeo)
        << "Total acks must equal final LEO (exactly-once, no phantom appends)";
}

// ===========================================================================
// F3 round 5 (C2 / I1 / M4): size limits on the per-worker produce path.
// ===========================================================================

// A valid RecordBatch whose encoded size is EXACTLY `target` bytes (value
// length tuned; varint steps are bridged by also tuning the key length).
static std::vector<uint8_t> makeBatchOfExactSize(std::size_t target) {
    auto build = [](std::size_t keyLen, std::size_t valLen) {
        rune::protocol::RecordBatchBuilder b(0, 1000LL);
        b.append(std::string(keyLen, 'k'), std::string(valLen, 'v'), 1000LL);
        return b.build();
    };
    for (std::size_t keyLen = 1; keyLen <= 4; ++keyLen) {
        const std::size_t base = build(keyLen, 0).size();
        if (base > target) break;
        std::size_t val = target - base;
        for (int iter = 0; iter < 8; ++iter) {
            auto b = build(keyLen, val);
            if (b.size() == target) return b;
            if (b.size() > target) {
                const std::size_t over = b.size() - target;
                if (over > val) break;
                val -= over;
            } else {
                val += target - b.size();
            }
        }
    }
    ADD_FAILURE() << "could not build a batch of exactly " << target << " bytes";
    return {};
}

static std::vector<uint8_t> encodeProduceFrameIdem(int64_t correlationId,
                                                   const std::vector<uint8_t>& batch,
                                                   int64_t producerId) {
    pc::Produce msg;
    msg.sessionId      = 1;
    msg.correlationId  = correlationId;
    msg.topicId        = 0;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.acks           = 0;
    msg.producerId     = producerId;
    msg.producerEpoch  = 0;
    msg.baseSequence   = producerId >= 0 ? 0 : -1;
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> buf(msg.encodedSize());
    msg.encode(buf.data());
    return buf;
}

// C2 + M4: the per-worker ingress path (WorkerIngress -> handleWorkerIngressFrame_
// -> handleProduceBatch) had NO size check: a UDP client's own (larger) term
// governs the image frame size, so the broker's term never bounded it.  A
// batch above the effective max must be rejected with MessageTooLarge (never
// appended), exactly the max must be accepted, and the idempotent
// (producerId >= 0) produce must be rejected identically.
TEST(WorkerProduceRegression, IngressBatchAboveEffectiveMaxRejectedRegression) {
    TempDir dir("f3r5_ingress_max_");
    rune::storage::Log log(dir.path.string());
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);

    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    constexpr std::size_t kMax = 4096;
    worker.setMaxBatchBytes(kMax);
    ASSERT_EQ(worker.maxIntakeBatchBytes(), kMax);

    auto runOne = [&](const std::vector<uint8_t>& frame) {
        int16_t code = -999;
        int n = 0;
        (void)worker.handleIngressFrameForTest(frame.data(), frame.size(), /*nowNs=*/1);
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) { code = ret.errorCode; ++n; }
        });
        EXPECT_EQ(n, 1) << "exactly one ProduceResult per frame (never silently dropped)";
        return code;
    };

    const auto atMax  = makeBatchOfExactSize(kMax);
    const auto overBy = makeBatchOfExactSize(kMax + 1);
    ASSERT_EQ(atMax.size(), kMax);
    ASSERT_EQ(overBy.size(), kMax + 1);

    EXPECT_EQ(runOne(encodeProduceFrameIdem(1, overBy, -1)),
              static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(log.logEndOffset(), 0) << "an oversize batch must never be appended";

    EXPECT_EQ(runOne(encodeProduceFrameIdem(2, overBy, /*producerId=*/7)),
              static_cast<int16_t>(pc::ErrorCode::MessageTooLarge))
        << "the idempotent/transactional produce must be size-checked too";
    EXPECT_EQ(log.logEndOffset(), 0);

    EXPECT_EQ(runOne(encodeProduceFrameIdem(3, atMax, -1)), 0)
        << "a batch of exactly the effective max must be accepted";
    EXPECT_EQ(log.logEndOffset(), 1);
}

// I1: tryEnqueueOp must never throw on a claim larger than the worker ingress
// ring's maxMsgLength (agrona's checkMsgLength throws IllegalArgumentException).
// The max tail = ring maxMsgLength - WorkerOp framing (SBE header 8 + block 219
// + tail length prefix 4) — computed from the generated codec constants.
TEST(WorkerProduceRegression, OversizeEnqueueNeverThrowsRegression) {
    constexpr std::size_t kRing = 64 * 1024;
    Worker worker(0, kRing);
    constexpr std::size_t kExpectedTail = kRing / 8 -
        (rune::protocol::internal::sbe::MessageHeader::encodedLength() +
         rune::protocol::internal::sbe::WorkerOp::sbeBlockLength() +
         rune::protocol::internal::sbe::WorkerOp::tailHeaderLength());
    EXPECT_EQ(Worker::maxOpTailBytesFor(kRing), kExpectedTail);
    EXPECT_EQ(worker.maxOpTailBytes(), kExpectedTail);

    auto produceOp = [](std::size_t n) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.recordBatchBytes.assign(n, 0xAB);
        return op;
    };
    bool ok = true;
    EXPECT_NO_THROW(ok = static_cast<bool>(worker.tryEnqueueOp(produceOp(kExpectedTail + 1))));
    EXPECT_FALSE(ok) << "an oversize Produce op must be refused, not thrown";

    WorkerOp apply;
    apply.type = WorkerOp::Type::ApplyReplicated;
    apply.replicatedPayload.records.assign(kExpectedTail + 1, 0xCD);
    ok = true;
    EXPECT_NO_THROW(ok = static_cast<bool>(worker.tryEnqueueOp(std::move(apply))));
    EXPECT_FALSE(ok) << "an oversize ApplyReplicated op must be refused, not thrown";

    ok = false;
    EXPECT_NO_THROW(ok = static_cast<bool>(worker.tryEnqueueOp(produceOp(kExpectedTail))));
    EXPECT_TRUE(ok) << "exactly the max tail must fit the ring";
}

// F3 round 6, item 2: the per-worker oversize-produce reject log is rate-
// limited (first, then every 1024th) — a client spamming oversize batches must
// not flood the log.  Every frame is still rejected with MessageTooLarge.
TEST(WorkerProduceRegression, OversizeRejectLogRateLimitedRegression) {
    TempDir dir("f3r6_ratelimit_");
    rune::storage::Log log(dir.path.string());
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    worker.setMaxBatchBytes(256);
    const auto over  = makeBatchOfExactSize(257);
    const auto frame = encodeProduceFrameIdem(1, over, -1);
    int rejects = 0;
    for (int i = 0; i < 2100; ++i) {
        (void)worker.handleIngressFrameForTest(frame.data(), frame.size(), 1);
        worker.drainReturn([&](const WorkerReturn& r) {
            if (r.errorCode == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) ++rejects;
        });
    }
    EXPECT_EQ(rejects, 2100) << "every oversize frame is rejected";
    EXPECT_EQ(worker.oversizeProduceRejectsForTest(), 2100u);
    EXPECT_EQ(worker.oversizeProduceRejectLogsForTest(), 3u) << "logged at #1, #1025, #2049";
    EXPECT_EQ(log.logEndOffset(), 0);
}

// No-hang wave (R2): a per-worker-ingress Produce whose MessageHeader decodes
// but whose body does not is ANSWERED ProduceResult{InvalidRequest} on its
// correlationId (it used to be dropped with a warn — the client waited out its
// timeout).  A frame with no decodable header stays unanswerable.
TEST(WorkerProduceRegression, UndecodableIngressProduceIsAnsweredInvalidRequestRegression) {
    TempDir dir("nohang_ingress_");
    rune::storage::Log log(dir.path.string());
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});

    auto frame = encodeProduceFrameIdem(4242, makeBatchOfExactSize(128), -1);
    frame[pc::MessageHeader::ENCODED_SIZE + 9] = 2;   // unknown acks byte → Produce::decode rejects
    ASSERT_FALSE(pc::Produce::decode(frame.data(), frame.size()).has_value());
    int n = 0;
    WorkerReturn got;
    (void)worker.handleIngressFrameForTest(frame.data(), frame.size(), /*nowNs=*/1);
    worker.drainReturn([&](const WorkerReturn& ret) {
        if (ret.type == WorkerReturn::Type::ProduceResult) { got = ret; ++n; }
    });
    ASSERT_EQ(n, 1) << "an undecodable ingress Produce must be answered exactly once";
    EXPECT_EQ(got.errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(got.correlationId, pc::MessageHeader::decode(frame.data(), frame.size())->correlationId);
    EXPECT_EQ(log.logEndOffset(), 0) << "never appended";
    EXPECT_EQ(worker.undecodableProduceAnswersForTest(), 1u);

    // Fix round 2: an EMPTY batch is a malformed request too — InvalidRequest, not Internal.
    {
        const auto emptyFrame = encodeProduceFrameIdem(4243, std::vector<uint8_t>{}, -1);
        int m = 0; int16_t code = -1;
        (void)worker.handleIngressFrameForTest(emptyFrame.data(), emptyFrame.size(), 1);
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) { code = ret.errorCode; ++m; }
        });
        ASSERT_EQ(m, 1);
        EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    }

    const std::vector<uint8_t> noHeader(12, 0);   // < MessageHeader: nothing to answer on
    n = 0;
    (void)worker.handleIngressFrameForTest(noHeader.data(), noHeader.size(), 1);
    worker.drainReturn([&](const WorkerReturn& ret) { if (ret.type == WorkerReturn::Type::ProduceResult) ++n; });
    EXPECT_EQ(n, 0);
}

// hotpath-A review m1: the hold through the REAL controlled poll.  A frame
// whose partition's roll is deferred ABORTs its image: it and every later frame
// of the SAME publisher wait (image-wide back-pressure, the documented
// contract), while OTHER images keep flowing; once a ready segment exists the
// held frames are re-delivered in order — none lost, none duplicated.
TEST_F(WorkerProduceFixture, HeldFrameAbortsOnlyItsImageAndIsRedeliveredRegression)
{
    constexpr int32_t kStream = 509;
    const std::string kChannel = "aeron:ipc";
    TempDir dirA("hold_img_a_"), dirB("hold_img_b_");
    rune::storage::Log logA(dirA.path.string(), /*rollBytes=*/256);
    rune::storage::Log logB(dirB.path.string());
    logA.recover();
    logB.recover();
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh1{&Hook::fn, nullptr};
    logA.setProvisioning(&demandCh1);
    NullTransport transport;
    auto repA = makeLeaderReplica(20, 0, logA, transport);
    auto repB = makeLeaderReplica(20, 1, logB, transport);
    WorkerIngress ingress(aeron_, kChannel, kStream);
    ASSERT_TRUE(ingress.resolved());
    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker worker(0, kRingBytes, kRingBytes);
    worker.setOwnedDirect({repA.get(), repB.get()});
    worker.setWorkerIngress(&ingress, /*active=*/true);

    std::vector<std::pair<int64_t, int64_t>> acked;   // (session, correlation), in order
    auto drain = [&] {
        worker.drainReturn([&](const WorkerReturn& r) {
            if (r.type == WorkerReturn::Type::ProduceResult) {
                EXPECT_EQ(r.errorCode, 0);
                acked.emplace_back(r.sessionId, r.correlationId);
            }
        });
    };
    // Fill A until its next append needs a roll (no ready segment).
    int64_t corr = 0;
    while (!logA.rollDeferred()) {
        const auto f = encodeProduceFrame(1, ++corr, 20, 0, 0, makeBatch());
        ASSERT_GE(worker.handleIngressFrameForTest(f.data(), f.size(), 1), 0);
        drain();
    }
    acked.clear();

    auto pub1 = makeExclusivePub(kChannel, kStream);
    auto pub2 = makeExclusivePub(kChannel, kStream);
    ASSERT_TRUE(pub1 && pub2);
    waitConnected(*pub1, ingress);
    waitConnected(*pub2, ingress);
    const auto a1 = encodeProduceFrame(/*session*/11, 101, 20, 0, 0, makeBatch());   // A: held
    const auto b1 = encodeProduceFrame(11, 102, 20, 1, 0, makeBatch());              // B, behind it
    const auto b2 = encodeProduceFrame(/*session*/22, 201, 20, 1, 0, makeBatch());   // other image
    ASSERT_TRUE(offerBounded(*pub1, a1.data(), a1.size()));
    ASSERT_TRUE(offerBounded(*pub1, b1.data(), b1.size()));
    ASSERT_TRUE(offerBounded(*pub2, b2.data(), b2.size()));
    using clk = std::chrono::steady_clock;
    auto pump = [&](auto done, std::chrono::milliseconds budget = 5000ms) {
        const auto deadline = clk::now() + budget;
        while (clk::now() < deadline && !done()) {
            worker.doWork(std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count());
            drain();
            std::this_thread::sleep_for(100us);
        }
    };
    pump([&] { return !acked.empty(); });
    pump([&] { return false; }, 300ms);   // keep polling a while: nothing more may pass
    ASSERT_EQ(acked.size(), 1u) << "only the OTHER image's frame may pass";
    EXPECT_EQ(acked[0], std::make_pair(int64_t{22}, int64_t{201}));
    EXPECT_GE(worker.intakeRollHoldsForTest(), 1u);
    // A ready segment arrives: the held image resumes, in order.
    ASSERT_EQ(logA.provisionReady(1), 1);
    pump([&] { return acked.size() >= 3; });
    ASSERT_EQ(acked.size(), 3u);
    EXPECT_EQ(acked[1], std::make_pair(int64_t{11}, int64_t{101}));
    EXPECT_EQ(acked[2], std::make_pair(int64_t{11}, int64_t{102}));
    (void)logA.finalizeStagingRenames();
    (void)logA.drainStagingRenamesDone();
}

// hotpath-A F2/F3: a produce whose append would need a roll that cannot happen
// without blocking the worker (no pre-provisioned segment ready on a
// provisioned log) is HELD — the frame is not decoded into state, answered or
// appended (the production controlled poll ABORTs it and re-delivers it) — and
// goes through once the provisioner refilled the pool.  It used to append and
// roll inline: an open + ftruncate + mmap (+ fallocate) on the worker.
TEST(WorkerProduceRegression, DeferredRollHoldsIngressFrameRegression) {
    TempDir dir("hotpatha_hold_");
    rune::storage::Log log(dir.path.string(), /*rollBytes=*/256);
    log.recover();
    struct Hook {
        static bool fn(void* c, rune::storage::Log*, rune::storage::Log::ProvisionDemand k) noexcept {
            if (k == rune::storage::Log::ProvisionDemand::Adopted) ++*static_cast<int*>(c);
            return true;
        }
    };
    int demands = 0;
    rune::storage::testing::DemandChannelForTest demandCh2{&Hook::fn, &demands};
    log.setProvisioning(&demandCh2);
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});

    int answers = 0;
    auto drain = [&] {
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) { EXPECT_EQ(ret.errorCode, 0); ++answers; }
        });
    };
    int64_t corr = 0;
    // Fill the active segment through the worker (no roll needed yet).
    while (!log.rollDeferred()) {
        const auto f = encodeProduceFrame(1, ++corr, 0, 0, /*acks=*/0, makeBatch());
        ASSERT_GE(worker.handleIngressFrameForTest(f.data(), f.size(), 1), 0);
        drain();
    }
    ASSERT_EQ(answers, corr);
    const int64_t leo = log.logEndOffset();
    const auto segs = log.segmentBaseOffsets().size();
    const auto held = encodeProduceFrame(1, ++corr, 0, 0, 0, makeBatch());
    EXPECT_LT(worker.handleIngressFrameForTest(held.data(), held.size(), 1), 0) << "the frame must be held";
    drain();
    EXPECT_EQ(answers, corr - 1) << "a held frame is not answered";
    EXPECT_EQ(log.logEndOffset(), leo) << "a held frame is not appended";
    EXPECT_EQ(log.segmentBaseOffsets().size(), segs) << "no inline segment create on the worker";
    EXPECT_EQ(worker.intakeRollHoldsForTest(), 1u);
    // The provisioner refills: the re-delivered frame goes through and rolls.
    ASSERT_EQ(log.provisionReady(1), 1);
    EXPECT_GE(worker.handleIngressFrameForTest(held.data(), held.size(), 1), 0);
    drain();
    EXPECT_EQ(answers, corr);
    EXPECT_EQ(log.logEndOffset(), leo + 1);
    EXPECT_EQ(log.segmentBaseOffsets().size(), segs + 1);
    EXPECT_EQ(demands, 1);
    (void)log.finalizeStagingRenames();
    (void)log.drainStagingRenamesDone();
}

// Lows batch C (D8c R3, found with it): the same hold for an ENCRYPTED session.  The frame was decrypted — its seq
// committed to the worker's anti-replay window — then held (ABORTed); its redelivery failed the replay check and was
// DROPPED (never appended or answered).  The seq is now committed only when the frame is consumed.
TEST(WorkerProduceRegression, EncryptedFrameHeldByADeferredRollIsAppendedOnRedeliveryRegression) {
    TempDir dir("hotpatha_hold_tls_");
    rune::storage::Log log(dir.path.string(), /*rollBytes=*/256);
    log.recover();
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh{&Hook::fn, nullptr};
    log.setProvisioning(&demandCh);
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});

    // An encrypted session (announce with its c2b key, as the gateway's SessionAnnouncer sends it).
    constexpr int64_t kSid = 1;
    rune::crypto::AeadKey key{};
    for (std::size_t i = 0; i < sizeof(key.bytes); ++i) key.bytes[i] = static_cast<uint8_t>(i * 5 + 1);
    const uint8_t prefix[4] = {9, 8, 7, 6};
    WorkerOp ann;
    ann.type = WorkerOp::Type::SessionAnnounce;
    ann.sessionId = kSid;
    ann.egressStreamId = 7;
    ann.egressChannel = "aeron:ipc";
    ann.sessionEncrypted = true;
    ann.senderCipher = static_cast<uint8_t>(rune::crypto::Cipher::Aes256Gcm);
    ann.c2bKeyValid = true;
    std::memcpy(ann.c2bKey, key.bytes, sizeof(ann.c2bKey));
    std::memcpy(ann.c2bKeyPrefix, prefix, sizeof(ann.c2bKeyPrefix));
    ASSERT_TRUE(worker.tryEnqueueOp(std::move(ann)));
    for (int i = 0; i < 4 && worker.sessionCryptoEntriesForTest() == 0; ++i) worker.doWork(1'000 + i);
    ASSERT_EQ(worker.sessionCryptoEntriesForTest(), 1u);
    rune::crypto::AeadCipher client(rune::crypto::Cipher::Aes256Gcm, key, prefix);
    uint64_t seq = 0;
    auto seal = [&](const std::vector<uint8_t>& plain) {
        const uint64_t sq = ++seq;
        std::vector<uint8_t> out(plain.size() + pc::kCryptoOverhead);
        std::memcpy(out.data(), plain.data(), pc::MessageHeader::ENCODED_SIZE);
        pc::CryptoSubHeader sub;
        sub.version = 1;
        sub.seq = sq;
        sub.encode(out.data() + pc::MessageHeader::ENCODED_SIZE);
        const std::size_t n = client.seal(sq, out.data(), pc::kCryptoHeaderBytes,
                                          plain.data() + pc::MessageHeader::ENCODED_SIZE,
                                          plain.size() - pc::MessageHeader::ENCODED_SIZE,
                                          out.data() + pc::kCryptoHeaderBytes);
        out.resize(pc::kCryptoHeaderBytes + n);
        return out;
    };

    int answers = 0;
    auto drain = [&] {
        worker.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult) { EXPECT_EQ(ret.errorCode, 0); ++answers; }
        });
    };
    int64_t corr = 0;
    while (!log.rollDeferred()) {
        const auto f = seal(encodeProduceFrame(kSid, ++corr, 0, 0, /*acks=*/0, makeBatch()));
        ASSERT_GE(worker.handleIngressFrameForTest(f.data(), f.size(), 1), 0);
        drain();
        ASSERT_EQ(answers, corr) << "encrypted produce " << corr << " was not answered (decrypt?)";
    }
    const int64_t leo = log.logEndOffset();
    const auto held = seal(encodeProduceFrame(kSid, ++corr, 0, 0, 0, makeBatch()));
    ASSERT_LT(worker.handleIngressFrameForTest(held.data(), held.size(), 1), 0) << "the frame must be held";
    ASSERT_EQ(log.provisionReady(1), 1);
    EXPECT_GE(worker.handleIngressFrameForTest(held.data(), held.size(), 1), 0);
    drain();
    EXPECT_EQ(answers, corr) << "the redelivered encrypted frame was dropped as a replay";
    EXPECT_EQ(log.logEndOffset(), leo + 1) << "the redelivered encrypted frame was not appended";
    // A replay of the CONSUMED frame is still refused: never appended twice.
    EXPECT_EQ(worker.handleIngressFrameForTest(held.data(), held.size(), 1), 0);
    drain();
    EXPECT_EQ(log.logEndOffset(), leo + 1) << "a replay of a consumed frame must be dropped";
    (void)log.finalizeStagingRenames();
    (void)log.drainStagingRenamesDone();
}

// hotpath-A review m1: a frame that would be REFUSED anyway (this replica is
// not the leader) is answered at once — never held behind a deferred roll.
TEST(WorkerProduceRegression, RefusedFrameIsAnsweredNotHeldWhileRollDeferredRegression) {
    TempDir dir("hotpatha_refuse_");
    rune::storage::Log log(dir.path.string(), /*rollBytes=*/256);
    log.recover();
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh3{&Hook::fn, nullptr};
    log.setProvisioning(&demandCh3);
    int64_t off = 0;
    while (!log.rollDeferred()) {   // fill (as a follower's replicated appends would)
        rune::protocol::RecordBatchBuilder b(off, 1000);
        b.append("k", "v", 1000);
        const auto bb = b.build();
        off = log.appendNoSync(bb.data(), bb.size(), off);
    }
    NullTransport transport;
    rune::replication::PartitionConfig cfg{};
    cfg.topicId = 0; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
    auto follower = std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Follower, 1, std::vector<rune::replication::NodeId>{1, 2},
        std::vector<rune::replication::NodeId>{1, 2}, &log, &transport);
    Worker worker(0);
    worker.setOwnedDirect({follower.get()});
    const auto f = encodeProduceFrame(1, 7, 0, 0, 0, makeBatch());
    EXPECT_GE(worker.handleIngressFrameForTest(f.data(), f.size(), 1), 0) << "a refused frame must not be held";
    int16_t code = -1;
    worker.drainReturn([&](const WorkerReturn& r) {
        if (r.type == WorkerReturn::Type::ProduceResult) code = r.errorCode;
    });
    EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_EQ(worker.intakeRollHoldsForTest(), 0u);
}

// DUR-1 (B1, Kafka model): the worker's produce role gate is
// PartitionReplica::acceptsProduce (shared with the gateway + DR harness) — a
// follower is refused (retriable NotLeader); a leader, user or internal, is
// writable at becomeLeader (HW = min over the ISR: every leader is an ISR member
// holding every committed record — no completeness gate).
TEST(WorkerProduceGateRegression, FollowerRefusedLeaderWritableAtOnce) {
    namespace rr = rune::replication;
    const auto dir = std::filesystem::temp_directory_path() /
                     ("rune_b1_gate_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    {
        rune::storage::Log log(dir.string());
        log.recover();
        rr::NullPartitionTransport t;
        for (const int32_t topicId : {-1, 4}) {
            rr::PartitionConfig cfg;
            cfg.selfId  = 1;
            cfg.topicId = topicId;
            rr::PartitionReplica rep(cfg, rr::Role::Follower, 0, {1, 2, 3}, {1, 2, 3}, &log, &t);
            EXPECT_FALSE(rune::node::Worker::produceRoleGatePassesForTest(&rep)) << "follower";
            rep.becomeLeader(2, {1, 2, 3});          // no follower has fetched yet
            ASSERT_EQ(rep.role(), rr::Role::Leader);
            EXPECT_TRUE(rune::node::Worker::produceRoleGatePassesForTest(&rep))
                << "a leader must be writable at once (topic " << topicId << ")";
        }
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}


// No-hang fix round 4 (item 6): each worker logs its OWN no-hang counters (incl.
// undecodableProduceAnswers) from its own duty pass every kNoHangStatsLogNs when
// one changed — the change detector compares the counters one by one.
TEST(WorkerProduceRegression, WorkerLogsItsNoHangCountersPeriodicallyOnChangeRegression) {
    TempDir dir("nohang_wstats_");
    rune::storage::Log log(dir.path.string());
    NullTransport transport;
    auto replica = WorkerProduceFixture::makeLeaderReplica(0, 0, log, transport);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    EXPECT_FALSE(worker.maybeLogNoHangStatsForTest(1)) << "nothing changed: no line";
    EXPECT_EQ(worker.nextNoHangStatsLogNsForTest(), 1 + Worker::kNoHangStatsLogNs);
    auto frame = encodeProduceFrameIdem(4244, makeBatchOfExactSize(128), -1);
    frame[pc::MessageHeader::ENCODED_SIZE + 9] = 2;   // undecodable body → answered InvalidRequest
    (void)worker.handleIngressFrameForTest(frame.data(), frame.size(), /*nowNs=*/2);
    worker.drainReturn([](const WorkerReturn&) {});
    ASSERT_EQ(worker.undecodableProduceAnswersForTest(), 1u);
    EXPECT_FALSE(worker.maybeLogNoHangStatsForTest(3)) << "logged before its 10 s cadence";
    EXPECT_TRUE(worker.maybeLogNoHangStatsForTest(1 + Worker::kNoHangStatsLogNs)) << "a changed counter was not logged";
    EXPECT_FALSE(worker.maybeLogNoHangStatsForTest(1 + 2 * Worker::kNoHangStatsLogNs)) << "unchanged counters logged again";
    // Field-by-field: changes that would cancel out in an XOR signature are still changes.
    Worker::NoHangStats a{}, b{};
    b.undecodableProduceAnswers = 1; b.acksAllRefusedNoReservation = 1;
    EXPECT_NE(a, b);
    a.undecodableProduceAnswers = 2;
    b = Worker::NoHangStats{};
    b.acksAllRefusedNoReservation = 2;
    EXPECT_NE(a, b);
}


// ===========================================================================
// DR-bugs fix round 1, review B-I6: the controller's AlterIsr answer through the REAL worker relay (the WorkerOp
// AeronPartitionTransport builds — topicId / partition / roleEpoch / rolePartitionEpoch / errorCode — executed by
// Worker::doWork on its owner thread, here the test thread) keeps a stale-answered proposal pending
// (InvalidUpdateVersion) and still clears its own on InvalidRequest at the base.  RED: Worker.h drops
// `r.partitionEpoch = op.rolePartitionEpoch();` (the InvalidRequest clear never matches the base).
// ===========================================================================
TEST(AlterIsrRelayRegression, StaleAnswerThroughTheWorkerRelayKeepsTheProposalAndOwnInvalidRequestClearsRegression) {
    TempDir logDir("relay_");
    rune::storage::Log log(logDir.path.string());
    log.recover();
    NullTransport transport;
    rune::replication::PartitionConfig cfg{};
    cfg.topicId = 12; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
    const std::vector<rune::replication::NodeId> all{1, 2, 3};
    auto replica = std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Follower, 0, all, all, &log, &transport);
    replica->setIsrChannel(&transport);                      // controller-managed (as BrokerNode wires it)
    replica->becomeLeader(1, all, /*partitionEpoch=*/4);
    ASSERT_EQ(replica->role(), rune::replication::Role::Leader);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    int64_t now = 1'000'000'000LL;
    const int32_t live[] = {1, 2};
    replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), now);   // proposes {1,2} at base 4
    ASSERT_TRUE(replica->isrProposalPendingForTest());
    auto answer = [&](int16_t code, int32_t pe) {
        WorkerOp op;   // exactly as AeronPartitionTransport builds it (+ the replica WorkerPool stamps at routing)
        op.type = WorkerOp::Type::AlterIsrResponse;
        op.replica = replica.get();
        op.topicId = 12; op.partition = 0;
        op.roleEpoch = 1; op.rolePartitionEpoch = pe;
        op.replicatedPayload.errorCode = code;
        ASSERT_TRUE(static_cast<bool>(worker.tryEnqueueOp(std::move(op))));
        worker.doWork(now += 1'000'000);
    };
    answer(rune::replication::kAlterIsrInvalidUpdateVersion, 5);
    EXPECT_TRUE(replica->isrProposalPendingForTest()) << "a stale answer through the relay dropped the proposal";
    EXPECT_TRUE(replica->isrProposalAwaitingMetadataForTest());
    const auto& maximal = replica->maximalIsrForTest();
    EXPECT_NE(std::find(maximal.begin(), maximal.end(), 3), maximal.end()) << "the maximal ISR lost node 3";
    // The committed assignment resolves it (same epoch, pe 5): a new committed state {1,2,3}.
    WorkerOp lead;
    lead.type = WorkerOp::Type::BecomeLeader;
    lead.replica = replica.get();
    lead.topicId = 12; lead.partition = 0; lead.roleEpoch = 1; lead.rolePartitionEpoch = 5;
    lead.isr = {1, 2, 3};
    ASSERT_TRUE(static_cast<bool>(worker.tryEnqueueOp(std::move(lead))));
    worker.doWork(now += 1'000'000);
    EXPECT_FALSE(replica->isrProposalPendingForTest()) << "the applied assignment resolves the proposal";
    // A fresh proposal at base 5, answered InvalidRequest at ITS base through the relay: cleared.
    replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), now += 1'000'000);
    ASSERT_TRUE(replica->isrProposalPendingForTest());
    answer(rune::replication::kAlterIsrInvalidRequest, 5);
    EXPECT_FALSE(replica->isrProposalPendingForTest()) << "the relay lost the partitionEpoch: its own InvalidRequest "
                                                          "no longer clears it (ISR changes would freeze)";
    // ISR protocol (Step 4): the proposal id and send ordinal ride through the relay too (op.nextOffset / roleLeaderId).
    now += 100'000'000;   // past the InvalidRequest back-off of the previous proposal
    replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), now);   // a fresh proposal at base 5
    ASSERT_TRUE(replica->isrProposalPendingForTest());
    const int64_t id = replica->pendingProposalIdForTest();
    auto answerId = [&](int16_t code, int32_t pe, int64_t pid, int32_t seq) {
        WorkerOp op; op.type = WorkerOp::Type::AlterIsrResponse; op.replica = replica.get();
        op.topicId = 12; op.partition = 0; op.roleEpoch = 1; op.rolePartitionEpoch = pe;
        op.replicatedPayload.errorCode = code; op.nextOffset = pid; op.roleLeaderId = seq;
        ASSERT_TRUE(static_cast<bool>(worker.tryEnqueueOp(std::move(op)))); worker.doWork(now += 1'000'000);
    };
    answerId(rune::replication::kAlterIsrIneligibleReplica, 5, id + 1, 1);
    EXPECT_TRUE(replica->isrProposalPendingForTest()) << "an answer for another proposal id cleared this one";
    answerId(rune::replication::kAlterIsrIneligibleReplica, 5, id, 1);
    EXPECT_FALSE(replica->isrProposalPendingForTest()) << "its own answer through the relay no longer clears it";
}

// CP1 review I1 (Worker half): the send ordinal rides through the worker relay too.  An answer to an OLDER send of the
// pending proposal must keep it; only the newest send's answer clears.  RED: delete `r.sendSeq = op.roleLeaderId();`
// in Worker.h (the older answer then arrives with seq 0 and clears the proposal).
TEST(AlterIsrRelayRegression, OlderSendAnswerThroughTheWorkerRelayNeverClearsRegression) {
    TempDir logDir("relay_seq_");
    rune::storage::Log log(logDir.path.string());
    log.recover();
    NullTransport transport;
    rune::replication::PartitionConfig cfg{};
    cfg.topicId = 12; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
    const std::vector<rune::replication::NodeId> all{1, 2, 3};
    auto replica = std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Follower, 0, all, all, &log, &transport);
    replica->setIsrChannel(&transport);
    replica->becomeLeader(1, all, /*partitionEpoch=*/4);
    ASSERT_EQ(replica->role(), rune::replication::Role::Leader);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    int64_t now = 1'000'000'000LL;
    const int32_t live[] = {1, 2};
    replica->shrinkIsrToLive(std::span<const int32_t>(live, 2), now);   // proposes {1,2} at base 4 (send #1)
    ASSERT_TRUE(replica->isrProposalPendingForTest());
    const int64_t id = replica->pendingProposalIdForTest();
    replica->doWork(now += 60'000'000);   // the 50 ms resend: send #2
    ASSERT_EQ(replica->pendingLastSeqForTest(), 2);
    auto answerId = [&](int16_t code, int32_t pe, int64_t pid, int32_t seq) {
        WorkerOp op; op.type = WorkerOp::Type::AlterIsrResponse; op.replica = replica.get();
        op.topicId = 12; op.partition = 0; op.roleEpoch = 1; op.rolePartitionEpoch = pe;
        op.replicatedPayload.errorCode = code; op.nextOffset = pid; op.roleLeaderId = seq;
        ASSERT_TRUE(static_cast<bool>(worker.tryEnqueueOp(std::move(op)))); worker.doWork(now += 1'000'000);
    };
    answerId(rune::replication::kAlterIsrIneligibleReplica, 4, id, 1);
    EXPECT_TRUE(replica->isrProposalPendingForTest()) << "an older-send answer cleared the proposal through the relay";
    EXPECT_EQ(replica->olderSendAnswersForTest(), 1u);
    answerId(rune::replication::kAlterIsrIneligibleReplica, 4, id, 2);
    EXPECT_FALSE(replica->isrProposalPendingForTest());
}

// ISR v3 Step 6 (R14, M11): a FetchRequest relayed through a worker carries the follower's storage id in
// WorkerOp::hwBound and the leader slot records it.  RED: omit `req.storageId = op.hwBound();` in Worker.h.
TEST(WorkerFetchRelayRegression, StorageIdAndEpochStartSurviveTheWorkerOpRegression) {
    TempDir logDir("relay_storage_");
    rune::storage::Log log(logDir.path.string());
    log.recover();
    NullTransport transport;
    rune::replication::PartitionConfig cfg{};
    cfg.topicId = 12; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
    const std::vector<rune::replication::NodeId> all{1, 2, 3};
    auto replica = std::make_unique<rune::replication::PartitionReplica>(
        cfg, rune::replication::Role::Follower, 0, all, all, &log, &transport);
    replica->setIsrChannel(&transport);
    replica->becomeLeader(1, all, /*partitionEpoch=*/4);
    ASSERT_EQ(replica->role(), rune::replication::Role::Leader);
    Worker worker(0);
    worker.setOwnedDirect({replica.get()});
    int64_t now = 1'000'000'000LL;
    WorkerOp op; op.type = WorkerOp::Type::FetchRequest; op.replica = replica.get(); op.topicId = 12; op.partition = 0;
    op.fetchRequestPayload.leaderEpoch = 1; op.fetchRequestPayload.partition = 0; op.fetchRequestPayload.followerId = 2;
    op.fetchRequestPayload.fetchOffset = 0; op.fetchRequestPayload.maxBytes = 0; op.fetchRequestPayload.topicId = 12;
    op.fetchRequestPayload.lastFetchedEpoch = -1; op.fetchRequestPayload.incarnation = 1; op.fetchRequestPayload.nowNs = now;
    op.hwBound = 0xABCD; op.nextOffset = -1;
    ASSERT_TRUE(static_cast<bool>(worker.tryEnqueueOp(std::move(op)))); worker.doWork(now += 1'000'000);
    EXPECT_EQ(replica->followerStorageForTest(2), 0xABCD);
}

} // namespace
} // namespace rune::node
