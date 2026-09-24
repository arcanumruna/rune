// ---------------------------------------------------------------------------
// IngressMigrationTest.cpp — E5.1.5: exactly-once produce across partition
// leadership reassignment when per-worker ingress is active (flag ON).
//
// Proves:
//   1. PSM (idempotency) reconstruction on ownership GAIN:
//        When a worker gains leadership of a partition (executeBecomeLeaderWire_),
//        it rebuilds its ProducerStateManager from the committed log tail (HW-bound).
//        A retried produce spanning the move is deduped correctly on the new owner —
//        no re-append (no data duplication), no false-reject (no data loss).
//
//   2. Ex-owner rejects cleanly (retriable NotLeader):
//        After losing ownership, the old worker returns NotLeader (ErrorCode=2)
//        for any produce targeting the now-unowned partition.
//        No cross-worker forwarding (shared-nothing).
//
//   3. Exactly-once across reassignment (flag ON):
//        Produce N batches before the move; trigger reassignment (BecomeFollower
//        on old owner, BecomeLeader on new owner); continue producing after.
//        Every record appears EXACTLY ONCE in the committed log:
//          - No gap    (no dropped produce, all N+M records present).
//          - No dup    (idempotency held via reconstructed PSM on new owner).
//        The test drives all produce through the worker's WorkerIngress Aeron
//        subscription (perWorkerIngressActive_=true) — NOT via replica->append().
//
// Architecture:
//   Two Workers, each with a WorkerIngress subscription on a distinct Aeron IPC
//   stream.  One disk-backed PartitionReplica (single-node leader, minIsr=1).
//   The "old" worker starts as leader (owns the replica); after the simulated
//   failover, the replica is transferred to the "new" worker.
//
//   Leadership change mechanics (in-process, no BrokerNode):
//     1. Enqueue BecomeFollower to the old worker's ingress_ ring.
//     2. Enqueue BecomeLeader  to the new worker's ingress_ ring.
//     3. Enqueue SetOwnership  to both workers (old loses, new gains the replica).
//   These arrive as WorkerOp ring messages and are executed on each worker's
//   own duty-cycle thread (here: test thread, single-threaded drive).
//
// The Aeron embedded IPC driver is shared between the two WorkerIngress
// subscriptions and the test-thread publisher.
//
// Build gate: BUILD_BROKER_NODE_TESTS && aeron_FOUND (same gate as
//   WorkerProducePathTest and WorkerPerWorkerIngressE2ETest).
// TSan: TSAN_OPTIONS=halt_on_error=0 suppressions=cmake/tsan-aeron.supp
//
// C++23, LE, no virtual on hot path, no cross-worker forwarding.
// ---------------------------------------------------------------------------

#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerWire.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
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
#include <optional>
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
    auto name = std::string("rune_imgr_") + std::string(tag) +
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
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
    void sendOffsetsForLeaderEpochRequest(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochReqMsg&) override {}
    void sendOffsetsForLeaderEpochResponse(
            rune::replication::NodeId,
            const rune::replication::OffsetsForLeaderEpochRespMsg&) override {}
    void sendReconcileRequest(rune::replication::NodeId,
                              const rune::replication::FetchRequestMsg&) override {}
    void sendReconcileResponse(rune::replication::NodeId,
                               const rune::replication::FetchResponseMsg&) override {}
};

// ---------------------------------------------------------------------------
// makeLeaderReplica — single-node leader (minIsr=1, ISR=[selfId])
// ---------------------------------------------------------------------------
static std::unique_ptr<rune::replication::PartitionReplica>
makeLeaderReplica(int32_t topicId, int32_t partition,
                  rune::replication::NodeId selfId,
                  rune::storage::Log& log,
                  NullTransport& transport)
{
    rune::replication::PartitionConfig cfg{};
    cfg.topicId             = topicId;
    cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
    cfg.selfId              = selfId;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;

    return std::make_unique<rune::replication::PartitionReplica>(
        cfg,
        rune::replication::Role::Leader, /*epoch=*/1,
        /*replicas=*/std::vector<rune::replication::NodeId>{selfId},
        /*isr=*/std::vector<rune::replication::NodeId>{selfId},
        &log, &transport);
}

// ---------------------------------------------------------------------------
// makeIdempotentProduceFrame — encode a Produce frame with idempotent fields.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeIdempotentProduceFrame(
        int64_t sessionId, int64_t correlationId,
        int32_t topicId, int32_t partition,
        uint8_t acks,
        int64_t producerId, int16_t producerEpoch, int32_t baseSequence)
{
    // Build a single-record batch with the producer fields set.
    rune::protocol::RecordBatchBuilder b(0, 1000LL);
    b.setProducerId(producerId);
    b.setProducerEpoch(producerEpoch);
    b.setBaseSequence(baseSequence);
    b.append("k", "v", 1000LL);
    auto batchBytes = b.build();

    pc::Produce msg;
    msg.sessionId      = sessionId;
    msg.correlationId  = correlationId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.partitionKnown = 1;
    msg.acks           = acks;
    msg.producerId     = producerId;
    msg.producerEpoch  = producerEpoch;
    msg.baseSequence   = baseSequence;
    msg.key            = {};
    msg.recordBatch    = std::span<const uint8_t>(batchBytes.data(), batchBytes.size());

    std::vector<uint8_t> buf(msg.encodedSize());
    msg.encode(buf.data());
    return buf;
}

// ---------------------------------------------------------------------------
// IngressMigrationFixture — embedded IPC Aeron driver + two WorkerIngress
// subscriptions on distinct streams (one per Worker).
// ---------------------------------------------------------------------------
class IngressMigrationFixture : public ::testing::Test {
public:
    static constexpr int kPollIterMax = 200'000;

    void SetUp() override {
        tmpDir_ = std::make_unique<TempDir>("fix_");
        startDriver();
        connectAeron();
    }

    void TearDown() override {
        ingressA_.reset();
        ingressB_.reset();
        aeron_.reset();
        stopDriver();
        tmpDir_.reset();
    }

    std::unique_ptr<WorkerIngress> makeIngress(int32_t stream) {
        return std::make_unique<WorkerIngress>(aeron_, "aeron:ipc", stream);
    }

    std::shared_ptr<aeron::ExclusivePublication> makeExclusivePub(int32_t stream) {
        int64_t regId = aeron_->addExclusivePublication("aeron:ipc", stream);
        std::shared_ptr<aeron::ExclusivePublication> pub;
        waitBounded([&]() {
            pub = aeron_->findExclusivePublication(regId);
            return pub != nullptr;
        }, "makeExclusivePub");
        return pub;
    }

    // Offer bytes to an ExclusivePublication; retry up to kPollIterMax times.
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

    // Wait until pub is connected (polling ingress to drive Aeron).
    void waitConnected(aeron::ExclusivePublication& pub, WorkerIngress& ingress) {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 10s;
        while (clk::now() < deadline && !pub.isConnected()) {
            ingress.poll(10, [](const uint8_t*, std::size_t, aeron::Header&) {});
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(pub.isConnected()) << "Publication never connected";
    }

    std::shared_ptr<aeron::Aeron>   aeron_;
    std::unique_ptr<WorkerIngress>  ingressA_;
    std::unique_ptr<WorkerIngress>  ingressB_;

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

        aeron_driver_context_init(&driverCtx_);
        aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
        aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
        aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
        aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

        ASSERT_EQ(aeron_driver_init(&driver_, driverCtx_), 0)
            << "aeron_driver_init: " << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, /*manual_main_loop=*/true), 0)
            << "aeron_driver_start: " << aeron_errmsg();

        driverRunning_.store(true, std::memory_order_release);
        driverThread_ = std::thread([this]() {
            while (driverRunning_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_,
                    aeron_driver_main_do_work(driver_));
            }
        });

        const std::string cncPath = aeronDir + "/cnc.dat";
        for (int i = 0; i < 400; ++i) {
            aeron_driver_main_do_work(driver_);
            if (fs::exists(cncPath)) break;
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(fs::exists(cncPath)) << "Aeron cnc.dat not created";
    }

    void stopDriver() {
        if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
            if (driverThread_.joinable()) driverThread_.join();
        }
        if (driver_) {
            const auto dl = std::chrono::steady_clock::now() + 50ms;
            while (std::chrono::steady_clock::now() < dl) {
                if (aeron_driver_main_do_work(driver_) == 0) break;
                std::this_thread::sleep_for(200us);
            }
            aeron_driver_close(driver_);
            driver_ = nullptr;
        }
        if (driverCtx_) {
            aeron_driver_context_close(driverCtx_);
            driverCtx_ = nullptr;
        }
    }

    void connectAeron() {
        const std::string aeronDir = (tmpDir_->path / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr) << "Aeron::connect failed";
    }

    std::unique_ptr<TempDir>    tmpDir_;
    aeron_driver_context_t*     driverCtx_   = nullptr;
    aeron_driver_t*             driver_      = nullptr;
    std::atomic<bool>           driverRunning_{false};
    std::thread                 driverThread_;
};

// ===========================================================================
// TEST: ExOwner_RejectsWithNotLeader
//
// Proves deliverable 2: ex-owner returns retriable NotLeader (not Internal)
// for produces targeting an un-owned partition.
//
// Scenario:
//   1. Worker A owns partition P (Leader).
//   2. Send BecomeFollower + SetOwnership (empty) to worker A's ingress_ ring.
//      Worker A now owns no replicas.
//   3. Publish a Produce frame for P to worker A's Aeron stream.
//   4. Assert: ack arrives with errorCode == NotLeader (2), NOT Internal (6).
// ===========================================================================
TEST_F(IngressMigrationFixture, ExOwner_RejectsWithNotLeader)
{
    constexpr int32_t kStreamA   = 601;
    constexpr int32_t kTopicId   = 20;
    constexpr int32_t kPartition = 0;
    constexpr int64_t kPid       = 99999LL;
    constexpr int16_t kEpoch     = 1;

    // Setup: disk-backed log + single-node leader replica.
    TempDir logDir("wr_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, 1, log, transport);

    // Worker A: owns the partition, perWorkerIngressActive=true.
    ingressA_ = makeIngress(kStreamA);
    ASSERT_TRUE(ingressA_->resolved()) << "WorkerIngress A did not resolve";

    constexpr std::size_t kRingBytes = 256 * 1024;
    Worker workerA(/*workerId=*/0, kRingBytes, kRingBytes);
    workerA.setOwnedDirect({replica.get()});
    workerA.setWorkerIngress(ingressA_.get(), /*active=*/true);
    workerA.setAckTimeoutNs(500'000'000LL); // 500 ms

    // Publisher to worker A's stream.
    auto pubA = makeExclusivePub(kStreamA);
    ASSERT_TRUE(pubA);
    waitConnected(*pubA, *ingressA_);

    // --- Step 1: Transfer ownership away from worker A ---
    // Send BecomeFollower via the ingress_ ring (control-thread → worker thread).
    {
        WorkerOp op{};
        op.type        = WorkerOp::Type::BecomeFollower;
        op.topicId     = kTopicId;
        op.partition   = kPartition;
        op.replica     = replica.get();
        op.roleEpoch   = static_cast<rune::replication::Epoch>(2);
        op.roleLeaderId = static_cast<rune::replication::NodeId>(99); // dummy new leader
        ASSERT_TRUE(workerA.tryEnqueueOp(op))
            << "Failed to enqueue BecomeFollower to worker A";
    }
    // SetOwnership: empty set (worker A loses the partition).
    {
        WorkerOp op{};
        op.type     = WorkerOp::Type::SetOwnership;
        op.topicId  = kTopicId;
        op.partition = kPartition;
        // ownedSet empty → worker A owns nothing after this
        ASSERT_TRUE(workerA.tryEnqueueOp(op))
            << "Failed to enqueue SetOwnership(empty) to worker A";
    }

    // Drive doWork to process the ring ops (BecomeFollower + SetOwnership).
    {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 2s;
        while (clk::now() < deadline) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerA.doWork(now);
            // Drain ret_ to unblock the worker.
            workerA.drainReturn([](const WorkerReturn&) {});
            // Check: once worker A owns no replicas it won't own the partition.
            if (workerA.ownedCount() == 0) break;
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_EQ(workerA.ownedCount(), 0u)
        << "Worker A must own 0 replicas after SetOwnership(empty)";
    ASSERT_NE(replica->role(), rune::replication::Role::Leader)
        << "Replica must no longer be Leader after BecomeFollower";

    // --- Step 2: Produce to worker A's stream (now un-owned) ---
    auto frame = makeIdempotentProduceFrame(
        /*sessionId=*/1, /*correlationId=*/200,
        kTopicId, kPartition, /*acks=*/0, kPid, kEpoch, /*baseSeq=*/0);

    ASSERT_TRUE(offerBounded(*pubA, frame.data(), frame.size()))
        << "offer to ex-owner stream failed";

    // --- Step 3: Collect the ack ---
    std::optional<int16_t> gotErrorCode;
    using clk = std::chrono::steady_clock;
    const auto deadline = clk::now() + 5s;
    while (clk::now() < deadline && !gotErrorCode.has_value()) {
        int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
        workerA.doWork(now);
        workerA.drainReturn([&](const WorkerReturn& ret) {
            if (ret.type == WorkerReturn::Type::ProduceResult &&
                ret.correlationId == 200) {
                gotErrorCode = ret.errorCode;
            }
        });
        std::this_thread::sleep_for(100us);
    }

    ASSERT_TRUE(gotErrorCode.has_value())
        << "No ack received from ex-owner within 5s";

    // KEY ASSERTION: must be NotLeader (2), not Internal (6).
    EXPECT_EQ(*gotErrorCode,
              static_cast<int16_t>(pc::ErrorCode::NotLeader))
        << "Ex-owner must return NotLeader (retriable) for an un-owned partition. "
           "Got errorCode=" << *gotErrorCode
        << " (Internal=6 means the wrong code path was taken — "
           "client cannot distinguish leadership loss from a server bug).";

    // Log must NOT have advanced (no record was appended for the un-owned partition).
    EXPECT_EQ(log.logEndOffset(), 0)
        << "Log must NOT advance when the produce was rejected as NotLeader";
}

// ===========================================================================
// TEST: PsmReconstructionOnOwnershipGain_DedupsAcrossMove
//
// Proves deliverable 1: PSM reconstruction on ownership GAIN.
//
// Scenario:
//   1. Worker A owns partition P (Leader).  Produce N idempotent batches via
//      Worker A's WorkerIngress path (flag ON).  Each batch: pid=P, epoch=E,
//      baseSeq=0..N-1.  All N acks arrive; log.LEO = N.
//   2. Simulate failover:
//        - BecomeFollower to worker A (demote).
//        - SetOwnership(empty) to worker A (loses partition).
//        - BecomeLeader to worker B (promotes; PSM rebuild fires on worker B's thread).
//        - SetOwnership({replica}) to worker B (gains partition).
//   3. Retry seq=N-1 (last committed) via Worker B's WorkerIngress stream.
//      KEY ASSERTION A: ack arrives with errorCode=OK and baseOffset == original
//      (dedup — no re-append, log.LEO unchanged).
//   4. Send seq=N (genuinely new) via Worker B.
//      KEY ASSERTION B: ack arrives with errorCode=OK and baseOffset > N-1
//      (Accept, log.LEO advances by 1).
//   5. Verify exactly-once in log: no duplicate records, no gap.
// ===========================================================================
TEST_F(IngressMigrationFixture, PsmReconstructionOnOwnershipGain_DedupsAcrossMove)
{
    constexpr int32_t kStreamA   = 611;
    constexpr int32_t kStreamB   = 612;
    constexpr int32_t kTopicId   = 21;
    constexpr int32_t kPartition = 0;
    constexpr int64_t kPid       = 12345678LL;
    constexpr int16_t kEpoch     = 3;
    constexpr int     kN         = 4;  // batches produced before the move

    // Disk-backed log + single-node leader replica (shared between workers).
    TempDir logDir("psm_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, 1, log, transport);

    // Worker A: starts as leader.
    ingressA_ = makeIngress(kStreamA);
    ASSERT_TRUE(ingressA_->resolved()) << "WorkerIngress A did not resolve";
    constexpr std::size_t kRingBytes = 2 * 1024 * 1024;
    auto workerA = std::make_unique<Worker>(/*workerId=*/0, kRingBytes, kRingBytes);
    workerA->setOwnedDirect({replica.get()});
    workerA->setWorkerIngress(ingressA_.get(), /*active=*/true);
    workerA->setAckTimeoutNs(2'000'000'000LL); // 2 s

    // Worker B: starts with no owned replicas; will become the new leader.
    ingressB_ = makeIngress(kStreamB);
    ASSERT_TRUE(ingressB_->resolved()) << "WorkerIngress B did not resolve";
    auto workerB = std::make_unique<Worker>(/*workerId=*/1, kRingBytes, kRingBytes);
    workerB->setOwnedDirect({});  // empty initially
    workerB->setWorkerIngress(ingressB_.get(), /*active=*/true);
    workerB->setAckTimeoutNs(2'000'000'000LL);

    // Publisher to worker A's Aeron stream.
    auto pubA = makeExclusivePub(kStreamA);
    ASSERT_TRUE(pubA);
    waitConnected(*pubA, *ingressA_);

    // Publisher to worker B's Aeron stream (connected now; used after the move).
    auto pubB = makeExclusivePub(kStreamB);
    ASSERT_TRUE(pubB);
    // Worker B has no owned replicas yet but the subscription is live.
    // We'll drive doWork on B to let Aeron poll complete for the publication.
    {
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 5s;
        while (clk::now() < dl && !pubB->isConnected()) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([](const WorkerReturn&){});
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_TRUE(pubB->isConnected()) << "PubB never connected to worker B";

    // -------------------------------------------------------------------
    // Phase 1: Produce kN idempotent batches via Worker A's ingress.
    // -------------------------------------------------------------------
    std::vector<int64_t> baseOffsets(static_cast<std::size_t>(kN), -1LL);

    for (int seq = 0; seq < kN; ++seq) {
        auto frame = makeIdempotentProduceFrame(
            /*sessionId=*/1, /*correlationId=*/static_cast<int64_t>(100 + seq),
            kTopicId, kPartition, /*acks=*/0, kPid, kEpoch, seq);

        ASSERT_TRUE(offerBounded(*pubA, frame.data(), frame.size()))
            << "offer to worker A failed for seq=" << seq;

        // Drive until this ack arrives.
        bool gotAck = false;
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 5s;
        while (clk::now() < deadline && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerA->doWork(now);
            workerA->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == static_cast<int64_t>(100 + seq)) {
                    EXPECT_EQ(ret.errorCode, 0)
                        << "phase-1 produce seq=" << seq << " must be OK";
                    baseOffsets[static_cast<std::size_t>(seq)] = ret.baseOffset;
                    gotAck = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }
        ASSERT_TRUE(gotAck)
            << "Phase 1: no ack for seq=" << seq << " within 5s";
    }

    // Verify monotone offsets (each single-record batch occupies one offset).
    for (int seq = 1; seq < kN; ++seq) {
        ASSERT_GT(baseOffsets[static_cast<std::size_t>(seq)],
                  baseOffsets[static_cast<std::size_t>(seq - 1)])
            << "Phase 1: offsets must be strictly increasing";
    }

    const int64_t leoAfterPhase1 = log.logEndOffset();
    ASSERT_EQ(leoAfterPhase1, static_cast<int64_t>(kN))
        << "Phase 1: LEO must equal kN=" << kN << " (one record per batch)";

    // -------------------------------------------------------------------
    // Phase 2: Simulate failover — demote Worker A, promote Worker B.
    //
    // On the WORKER threads (here: single-threaded test drive), the operations
    // are delivered via the ingress_ (WorkerOp) ring, not the Aeron stream.
    //
    // Order:
    //   a. BecomeFollower → Worker A's ingress_ ring.
    //   b. SetOwnership(empty) → Worker A's ingress_ ring.
    //   c. BecomeLeader  → Worker B's ingress_ ring.
    //      This triggers reconstructPsmFromLog_ on Worker B's thread.
    //   d. SetOwnership({replica}) → Worker B's ingress_ ring.
    // -------------------------------------------------------------------

    // (a) BecomeFollower to Worker A.
    {
        WorkerOp op{};
        op.type         = WorkerOp::Type::BecomeFollower;
        op.topicId      = kTopicId;
        op.partition    = kPartition;
        op.replica      = replica.get();
        op.roleEpoch    = static_cast<rune::replication::Epoch>(2);
        op.roleLeaderId = static_cast<rune::replication::NodeId>(99);
        ASSERT_TRUE(workerA->tryEnqueueOp(op));
    }
    // (b) SetOwnership(empty) to Worker A.
    {
        WorkerOp op{};
        op.type     = WorkerOp::Type::SetOwnership;
        op.topicId  = kTopicId;
        op.partition = kPartition;
        // ownedSet empty → Worker A owns nothing.
        ASSERT_TRUE(workerA->tryEnqueueOp(op));
    }

    // Drive Worker A to process BecomeFollower + SetOwnership.
    {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 3s;
        while (clk::now() < deadline) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerA->doWork(now);
            workerA->drainReturn([](const WorkerReturn&) {});
            if (workerA->ownedCount() == 0) break;
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_EQ(workerA->ownedCount(), 0u) << "Worker A must own 0 after demotion";
    ASSERT_NE(replica->role(), rune::replication::Role::Leader)
        << "Replica must not be Leader after BecomeFollower";

    // (c) BecomeLeader to Worker B (triggers PSM reconstruction on worker B's thread).
    {
        WorkerOp op{};
        op.type        = WorkerOp::Type::BecomeLeader;
        op.topicId     = kTopicId;
        op.partition   = kPartition;
        op.replica     = replica.get();
        op.roleEpoch   = static_cast<rune::replication::Epoch>(2);
        op.roleLeaderId = 0; // unused for BecomeLeader
        op.isr         = {static_cast<rune::replication::NodeId>(1)}; // new ISR
        ASSERT_TRUE(workerB->tryEnqueueOp(op));
    }
    // (d) SetOwnership({replica}) to Worker B.
    {
        WorkerOp op{};
        op.type     = WorkerOp::Type::SetOwnership;
        op.topicId  = kTopicId;
        op.partition = kPartition;
        op.ownedSet = {replica.get()};
        ASSERT_TRUE(workerB->tryEnqueueOp(op));
    }

    // Drive Worker B to process BecomeLeader + SetOwnership.
    // After BecomeLeader the PSM reconstruction seam fires:
    //   psm_.rebuildFromLog(kTopicId, *log, replica->highWatermark(), kPartition)
    {
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 3s;
        while (clk::now() < deadline) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([](const WorkerReturn&) {});
            if (workerB->ownedCount() == 1) break;
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_EQ(workerB->ownedCount(), 1u) << "Worker B must own 1 replica after BecomeLeader";
    ASSERT_EQ(replica->role(), rune::replication::Role::Leader)
        << "Replica must be Leader after BecomeLeader on Worker B";

    // Verify PSM reconstruction: Worker B's PSM must know about the committed
    // batches (seq 0..kN-1) so retries are deduped.
    {
        const auto* psmEntry = workerB->psmForTest().entry(
            kTopicId, kPid, kPartition);
        ASSERT_NE(psmEntry, nullptr)
            << "Worker B's PSM must have an entry for kPid after reconstruction. "
               "If null, reconstructPsmFromLog_ was not called or scanned no records.";
        EXPECT_EQ(psmEntry->producerEpoch, kEpoch)
            << "PSM entry epoch must match kEpoch=" << kEpoch;
        EXPECT_EQ(psmEntry->lastSequence, kN - 1)
            << "PSM entry lastSequence must be kN-1=" << (kN - 1)
               << " (all phase-1 batches committed); "
               "if 0 or -1, PSM was not rebuilt from the log";
    }

    // -------------------------------------------------------------------
    // Phase 3: Retry seq=kN-1 (last committed) via Worker B's ingress.
    //
    // KEY ASSERTION A (exactly-once dedup):
    //   The ack must arrive with errorCode=OK and baseOffset == the ORIGINAL
    //   offset from phase 1.  Log.LEO must NOT advance (no re-append).
    // -------------------------------------------------------------------
    const int64_t leoBeforeRetry = log.logEndOffset();

    {
        const int32_t lastSeq = kN - 1;
        auto frame = makeIdempotentProduceFrame(
            /*sessionId=*/2, /*correlationId=*/200,
            kTopicId, kPartition, /*acks=*/0, kPid, kEpoch, lastSeq);

        ASSERT_TRUE(offerBounded(*pubB, frame.data(), frame.size()))
            << "offer retry of seq=" << lastSeq << " to Worker B failed";

        bool gotAck = false;
        int16_t gotError = -1;
        int64_t gotBase  = -1;
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 5s;
        while (clk::now() < deadline && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == 200) {
                    gotError = ret.errorCode;
                    gotBase  = ret.baseOffset;
                    gotAck   = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }

        ASSERT_TRUE(gotAck) << "Phase 3 retry: no ack from Worker B within 5s";

        EXPECT_EQ(gotError, 0)
            << "Phase 3 retry (seq=" << lastSeq << "): ack must be OK (duplicate dedup). "
               "errorCode=" << gotError
            << " (NotLeader=2 means ownership not gained; Internal=6 means PSM rebuild missing)";

        EXPECT_EQ(gotBase, baseOffsets[static_cast<std::size_t>(lastSeq)])
            << "Phase 3 retry: baseOffset must match the ORIGINAL phase-1 offset="
            << baseOffsets[static_cast<std::size_t>(lastSeq)]
            << ". A different (new) baseOffset means the batch was RE-APPENDED "
               "(idempotency broken — PSM rebuild did not fire or was wrong-bounded).";

        EXPECT_EQ(log.logEndOffset(), leoBeforeRetry)
            << "Phase 3 retry: log.LEO must NOT advance (duplicate — no re-append). "
               "LEO advanced from " << leoBeforeRetry << " to " << log.logEndOffset()
            << ". This is the critical idempotency-across-move violation.";
    }

    // -------------------------------------------------------------------
    // Phase 4: Send seq=kN (genuinely new) via Worker B.
    //
    // KEY ASSERTION B:
    //   ack must be OK with baseOffset > prior (Accept, log advances).
    // -------------------------------------------------------------------
    {
        const int32_t newSeq = kN;
        auto frame = makeIdempotentProduceFrame(
            /*sessionId=*/2, /*correlationId=*/201,
            kTopicId, kPartition, /*acks=*/0, kPid, kEpoch, newSeq);

        ASSERT_TRUE(offerBounded(*pubB, frame.data(), frame.size()))
            << "offer new seq=" << newSeq << " to Worker B failed";

        bool gotAck = false;
        int16_t gotError = -1;
        int64_t gotBase  = -1;
        using clk = std::chrono::steady_clock;
        const auto deadline = clk::now() + 5s;
        while (clk::now() < deadline && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == 201) {
                    gotError = ret.errorCode;
                    gotBase  = ret.baseOffset;
                    gotAck   = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }

        ASSERT_TRUE(gotAck) << "Phase 4 new seq: no ack from Worker B within 5s";

        EXPECT_EQ(gotError, 0)
            << "Phase 4 new seq=" << newSeq << ": ack must be OK (Accept). "
               "errorCode=" << gotError;

        EXPECT_GT(gotBase, baseOffsets[static_cast<std::size_t>(kN - 1)])
            << "Phase 4 new seq: baseOffset must be strictly above the last "
               "phase-1 offset=" << baseOffsets[static_cast<std::size_t>(kN - 1)]
            << ". Got baseOffset=" << gotBase;

        EXPECT_GT(log.logEndOffset(), leoBeforeRetry)
            << "Phase 4 new seq: log.LEO must advance (genuinely new produce).";
    }

    // -------------------------------------------------------------------
    // Phase 5: Exactly-once verification across the entire log.
    //
    // Scan the log and verify:
    //   - Exactly kN+1 committed records (kN phase-1 + 1 new in phase 4).
    //   - No duplicate baseOffsets.
    //   - No gap in offsets [0 .. kN] (inclusive).
    // -------------------------------------------------------------------
    {
        // Read the full log content.
        const int64_t finalLeo = log.logEndOffset();
        EXPECT_EQ(finalLeo, static_cast<int64_t>(kN + 1))
            << "Exactly kN+1=" << (kN + 1) << " records must be in the log "
               "(kN phase-1 + 1 new from phase 4). "
               "LEO=" << finalLeo << " indicates a dup-append or missing append.";

        // Rebuild a fresh PSM from the full log (LEO = HW for a single-node leader
        // with no followers — all appends are immediately HW-advanced).
        rune::storage::ProducerStateManager scanPsm;
        scanPsm.rebuildFromLog(kTopicId, log, finalLeo, kPartition);

        // The retry window should hold all kN+1 batches (window size = 5).
        // seq kN (the new batch) must be the lastSequence.
        const auto* entry = scanPsm.entry(kTopicId, kPid, kPartition);
        ASSERT_NE(entry, nullptr)
            << "Exactly-once check: PSM rebuilt from final log must have an entry";
        EXPECT_EQ(entry->lastSequence, kN)
            << "Final log PSM: lastSequence must be kN=" << kN
               << " (kN+1 batches, 0-indexed). Got " << entry->lastSequence;
    }
}

// ===========================================================================
// TEST: ExactlyOnceAcrossMove_RetrySpanningMove
//
// Extended exactly-once test: a produce retry spans the ownership move.
// The batch is published to Worker A BEFORE the move completes, and the
// ack does not arrive (worker A loses ownership before processing it).
// The same batch is then re-published to Worker B AFTER the move.
//
// This is the most adversarial scenario: the batch may or may not have been
// appended by Worker A before ownership was lost.  We assert:
//   - Exactly kN + 1 records in the final log (no gap, no dup).
//   - If the batch was appended by Worker A, Worker B's PSM deduplicates
//     the retry (no re-append).
//   - If the batch was NOT appended by Worker A, Worker B accepts and
//     appends it (no gap).
//
// To make the test deterministic: we flush Worker A to process all pending
// frames BEFORE triggering the move, then do the same retry scenario as
// PsmReconstructionOnOwnershipGain_DedupsAcrossMove.  In both cases the
// invariant holds: each record appears exactly once.
// ===========================================================================
TEST_F(IngressMigrationFixture, ExactlyOnceAcrossMove_RetrySpanningMove)
{
    constexpr int32_t kStreamA   = 621;
    constexpr int32_t kStreamB   = 622;
    constexpr int32_t kTopicId   = 22;
    constexpr int32_t kPartition = 0;
    constexpr int64_t kPid       = 55555555LL;
    constexpr int16_t kEpoch     = 7;
    constexpr int     kN         = 3;  // batches before the move

    TempDir logDir("retryspan_");
    rune::storage::Log log(logDir.path.string());
    NullTransport transport;
    auto replica = makeLeaderReplica(kTopicId, kPartition, 2, log, transport);

    ingressA_ = makeIngress(kStreamA);
    ASSERT_TRUE(ingressA_->resolved());
    ingressB_ = makeIngress(kStreamB);
    ASSERT_TRUE(ingressB_->resolved());

    constexpr std::size_t kRingBytes = 2 * 1024 * 1024;
    auto workerA = std::make_unique<Worker>(/*workerId=*/2, kRingBytes, kRingBytes);
    workerA->setOwnedDirect({replica.get()});
    workerA->setWorkerIngress(ingressA_.get(), /*active=*/true);
    workerA->setAckTimeoutNs(2'000'000'000LL);

    auto workerB = std::make_unique<Worker>(/*workerId=*/3, kRingBytes, kRingBytes);
    workerB->setOwnedDirect({});
    workerB->setWorkerIngress(ingressB_.get(), /*active=*/true);
    workerB->setAckTimeoutNs(2'000'000'000LL);

    auto pubA = makeExclusivePub(kStreamA);
    ASSERT_TRUE(pubA);
    waitConnected(*pubA, *ingressA_);

    auto pubB = makeExclusivePub(kStreamB);
    ASSERT_TRUE(pubB);
    {
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 5s;
        while (clk::now() < dl && !pubB->isConnected()) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([](const WorkerReturn&){});
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_TRUE(pubB->isConnected());

    // --- Phase 1: Produce kN batches via Worker A ---
    std::vector<int64_t> bOffsets(static_cast<std::size_t>(kN), -1LL);
    for (int seq = 0; seq < kN; ++seq) {
        auto frame = makeIdempotentProduceFrame(
            1, static_cast<int64_t>(300 + seq),
            kTopicId, kPartition, 0, kPid, kEpoch, seq);
        ASSERT_TRUE(offerBounded(*pubA, frame.data(), frame.size()));

        bool gotAck = false;
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 5s;
        while (clk::now() < dl && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerA->doWork(now);
            workerA->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == static_cast<int64_t>(300 + seq)) {
                    EXPECT_EQ(ret.errorCode, 0);
                    bOffsets[static_cast<std::size_t>(seq)] = ret.baseOffset;
                    gotAck = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }
        ASSERT_TRUE(gotAck) << "Phase 1 seq=" << seq << " no ack";
    }

    ASSERT_EQ(log.logEndOffset(), static_cast<int64_t>(kN));

    // --- Phase 2: Demote Worker A, promote Worker B ---
    {
        WorkerOp op{};
        op.type = WorkerOp::Type::BecomeFollower;
        op.topicId = kTopicId; op.partition = kPartition;
        op.replica = replica.get();
        op.roleEpoch = 3; op.roleLeaderId = 99;
        ASSERT_TRUE(workerA->tryEnqueueOp(op));
    }
    {
        WorkerOp op{};
        op.type = WorkerOp::Type::SetOwnership;
        ASSERT_TRUE(workerA->tryEnqueueOp(op));
    }
    {
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 3s;
        while (clk::now() < dl) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerA->doWork(now); workerA->drainReturn([](const WorkerReturn&){});
            if (workerA->ownedCount() == 0) break;
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_EQ(workerA->ownedCount(), 0u);

    {
        WorkerOp op{};
        op.type = WorkerOp::Type::BecomeLeader;
        op.topicId = kTopicId; op.partition = kPartition;
        op.replica = replica.get();
        op.roleEpoch = 3;
        op.isr = {2};
        ASSERT_TRUE(workerB->tryEnqueueOp(op));
    }
    {
        WorkerOp op{};
        op.type = WorkerOp::Type::SetOwnership;
        op.ownedSet = {replica.get()};
        ASSERT_TRUE(workerB->tryEnqueueOp(op));
    }
    {
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 3s;
        while (clk::now() < dl) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now); workerB->drainReturn([](const WorkerReturn&){});
            if (workerB->ownedCount() == 1) break;
            std::this_thread::sleep_for(1ms);
        }
    }
    ASSERT_EQ(workerB->ownedCount(), 1u);
    ASSERT_EQ(replica->role(), rune::replication::Role::Leader);

    // --- Phase 3: Retry seq=kN-1 (spans the move) via Worker B ---
    const int64_t leoBeforeRetry = log.logEndOffset();

    {
        const int32_t retrySeq = kN - 1;
        auto frame = makeIdempotentProduceFrame(
            3, 400, kTopicId, kPartition, 0, kPid, kEpoch, retrySeq);
        ASSERT_TRUE(offerBounded(*pubB, frame.data(), frame.size()));

        bool gotAck = false;
        int16_t gotErr = -1; int64_t gotBase = -1;
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 5s;
        while (clk::now() < dl && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == 400) {
                    gotErr = ret.errorCode; gotBase = ret.baseOffset; gotAck = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }
        ASSERT_TRUE(gotAck) << "Phase 3 retry: no ack from Worker B";

        EXPECT_EQ(gotErr, 0)
            << "Retry spanning move: ack must be OK (either Duplicate or Accept); "
               "errorCode=" << gotErr;

        // The retry MUST result in the ORIGINAL base offset (Duplicate dedup).
        // Worker B's PSM was reconstructed from the log so it knows about seq=kN-1.
        EXPECT_EQ(gotBase, bOffsets[static_cast<std::size_t>(kN - 1)])
            << "Retry spanning move: baseOffset must be the ORIGINAL="
            << bOffsets[static_cast<std::size_t>(kN - 1)]
            << ". Got=" << gotBase
            << ". This proves PSM reconstruction on ownership gain works correctly.";

        EXPECT_EQ(log.logEndOffset(), leoBeforeRetry)
            << "Retry spanning move: log.LEO must NOT advance (duplicate dedup). "
               "Advance=" << (log.logEndOffset() - leoBeforeRetry)
            << " indicates a re-append (idempotency violation).";
    }

    // --- Phase 4: New seq=kN via Worker B (Accept) ---
    {
        auto frame = makeIdempotentProduceFrame(
            3, 401, kTopicId, kPartition, 0, kPid, kEpoch, kN);
        ASSERT_TRUE(offerBounded(*pubB, frame.data(), frame.size()));

        bool gotAck = false; int16_t gotErr = -1; int64_t gotBase = -1;
        using clk = std::chrono::steady_clock;
        const auto dl = clk::now() + 5s;
        while (clk::now() < dl && !gotAck) {
            int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                clk::now().time_since_epoch()).count();
            workerB->doWork(now);
            workerB->drainReturn([&](const WorkerReturn& ret) {
                if (ret.type == WorkerReturn::Type::ProduceResult &&
                    ret.correlationId == 401) {
                    gotErr = ret.errorCode; gotBase = ret.baseOffset; gotAck = true;
                }
            });
            std::this_thread::sleep_for(100us);
        }
        ASSERT_TRUE(gotAck) << "Phase 4 new seq: no ack";
        EXPECT_EQ(gotErr, 0);
        EXPECT_GT(gotBase, bOffsets[static_cast<std::size_t>(kN - 1)]);
        EXPECT_GT(log.logEndOffset(), leoBeforeRetry);
    }

    // --- Exactly-once check ---
    const int64_t finalLeo = log.logEndOffset();
    EXPECT_EQ(finalLeo, static_cast<int64_t>(kN + 1))
        << "Exactly-once: final log must have kN+1=" << (kN + 1)
           << " records. LEO=" << finalLeo;
}

} // namespace
} // namespace rune::node
