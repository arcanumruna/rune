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
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
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

} // namespace
} // namespace rune::node
