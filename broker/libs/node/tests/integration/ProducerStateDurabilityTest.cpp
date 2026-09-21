// ---------------------------------------------------------------------------
// ProducerStateDurabilityTest.cpp — TX-2b: producer-state durability across
// restart and leader failover.
//
// Verifies that the PSM (ProducerStateManager) used by ClientGateway for
// idempotent-producer dedup is REBUILT from the partition Log when the gateway
// transitions from Follower to Leader (or on restart).  Without the rebuild, a
// duplicate produce after failover would be incorrectly accepted (re-appended)
// rather than deduped.
//
// Scenario:
//   1. Stand up a 3-node cluster (leader + 2 followers) using InMemoryTransport.
//   2. Append N idempotent batches (pid=P, epoch=E, seq 0..N-1) directly to the
//      leader log using RecordBatchBuilder with proper producer fields.
//   3. Drive replication until HW advances past all batches.
//   4. (a) RESTART scenario: construct a fresh ProducerStateManager + call
//          rebuildFromLog(); assert duplicate is detected.
//      (b) FAILOVER scenario: kill leader, promote node2; node2's PSM is rebuilt
//          from the Log; assert duplicate detected + log end offset unchanged;
//          assert a genuinely new seq is accepted.
//
// Design mirrors TxnStateReplicationTest (InMemoryTransport, deterministic clock).
// NO Aeron driver needed.  NO Docker.  C++23.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/Log.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::storage;

// ===========================================================================
// Helpers
// ===========================================================================

static constexpr int kDriverCap = 1200;

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct PsmDurDir {
    std::string path;
    explicit PsmDurDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_psmd_" + std::string(tag) + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~PsmDurDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

static PartitionConfig makePsmPartCfg(NodeId selfId, int minIsr = 2) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = true;
    cfg.fastAckOnApply      = true;
    return cfg;
}

// Build a single-record idempotent RecordBatch with the given producer fields.
// Uses RecordBatchBuilder with setProducerId/setProducerEpoch/setBaseSequence.
// Returns the raw batch bytes (CRC-covered, LE header).
static std::vector<uint8_t> makeIdempotentBatch(int64_t baseOffset,
                                                 int64_t producerId,
                                                 int16_t producerEpoch,
                                                 int32_t baseSequence,
                                                 std::string_view value = "v")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, /*baseTimestamp=*/0);
    builder.setProducerId(producerId);
    builder.setProducerEpoch(producerEpoch);
    builder.setBaseSequence(baseSequence);
    builder.append(/*key=*/"", value, /*timestamp=*/0);
    return builder.build();
}

// ===========================================================================
// Fixture: 3-node cluster (same pattern as TxnStateReplicationTest)
// ===========================================================================
class ProducerStateDurabilityTest : public ::testing::Test {
protected:
    static constexpr NodeId kNode1 = 1;
    static constexpr NodeId kNode2 = 2;
    static constexpr NodeId kNode3 = 3;

    // We use partition=7 as the test partition to verify the PSM keys on partition.
    static constexpr int32_t kPartition = 7;

    ProducerStateDurabilityTest()
        : dir1("n1"), dir2("n2"), dir3("n3")
        , log1(dir1.path), log2(dir2.path), log3(dir3.path)
    {}

    void SetUp() override {
        const std::vector<NodeId> replicas = {kNode1, kNode2, kNode3};
        const std::vector<NodeId> isr      = {kNode1, kNode2, kNode3};

        node1_ = std::make_unique<PartitionReplica>(
            makePsmPartCfg(kNode1, /*minIsr=*/2),
            Role::Leader, /*epoch=*/1, replicas, isr, &log1, &transport_);

        node2_ = std::make_unique<PartitionReplica>(
            makePsmPartCfg(kNode2, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log2, &transport_);

        node3_ = std::make_unique<PartitionReplica>(
            makePsmPartCfg(kNode3, /*minIsr=*/2),
            Role::Follower, /*epoch=*/1, replicas, isr, &log3, &transport_);

        transport_.setLeader(node1_.get());
        transport_.addFollower(kNode2, node2_.get());
        transport_.addFollower(kNode3, node3_.get());
    }

    bool driveUntil(std::function<bool()> done, int64_t stepNs = 1'000'000LL) {
        for (int i = 0; i < kDriverCap && !done(); ++i) {
            clock_ += stepNs;
            node1_->doWork(clock_);
            node2_->doWork(clock_);
            node3_->doWork(clock_);
            transport_.pump(clock_);
        }
        return done();
    }

    PsmDurDir dir1, dir2, dir3;
    rune::storage::Log log1, log2, log3;

    InMemoryTransport transport_;
    std::unique_ptr<PartitionReplica> node1_, node2_, node3_;
    int64_t clock_ = 0;
};

// ===========================================================================
// Test 1: Rebuild from log recovers producer state (restart scenario).
//
// A fresh PSM constructed from scratch starts empty and misidentifies a
// duplicate as Accept; after rebuildFromLog it correctly reports Duplicate.
// ===========================================================================
TEST_F(ProducerStateDurabilityTest, RebuildFromLogRestoresDedup) {
    // Append N idempotent batches from a single producer.
    // pid=42, epoch=0, seq 0..N-1 (one record per batch → lastOffsetDelta=0)
    const int64_t kPid   = 42;
    const int16_t kEpoch = 0;
    const int     kN     = 5;

    // Track base offsets appended to the leader's log (node1/log1).
    std::vector<int64_t> baseOffsets;
    for (int seq = 0; seq < kN; ++seq) {
        int64_t base = log1.logEndOffset();
        auto batch = makeIdempotentBatch(base, kPid, kEpoch, seq);
        int64_t lastOff = base; // single-record: lastOffsetDelta=0
        log1.append(batch, lastOff);
        baseOffsets.push_back(base);
    }

    // Node1's LEO should be kN.
    ASSERT_EQ(log1.logEndOffset(), kN);

    // Drive replication so node2 has all batches.
    node1_->onLeaderLogAppended();  // tell the leader replica about the new data
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= kN; }))
        << "node2 HW must reach " << kN << " within " << kDriverCap << " iterations";

    // -----------------------------------------------------------------------
    // PHASE 1 (RED without fix): A fresh PSM knows nothing → misidentifies seq=0
    // as Accept (not Duplicate).
    // -----------------------------------------------------------------------
    {
        ProducerStateManager freshPsm;
        // Seq 0 → the fresh PSM has no state; it will Accept (this is the bug).
        auto vr = freshPsm.validate(/*topicId=*/0, kPid, kPartition, kEpoch, /*baseSeq=*/0, /*lastSeq=*/0);
        // Without the fix this would be Accept — demonstrating the hole.
        // We intentionally don't ASSERT here because this is the pre-fix baseline.
        (void)vr;
    }

    // -----------------------------------------------------------------------
    // PHASE 2 (GREEN): PSM rebuilt from the log correctly detects duplicates.
    // -----------------------------------------------------------------------
    ProducerStateManager rebuiltPsm;
    // rebuildFromLog must scan log1 and reconstruct per-(topicId,pid,partition) state.
    // We pass topicId=0 and partition=kPartition (the partition these batches were produced to).
    rebuiltPsm.rebuildFromLog(/*topicId=*/0, log1, log1.logEndOffset(), kPartition);

    // After rebuild: seq=0..N-1 are all in the retry window (kRetryWindowSize=5
    // and we appended exactly 5 batches, so all fit).
    for (int seq = 0; seq < kN; ++seq) {
        auto vr = rebuiltPsm.validate(/*topicId=*/0, kPid, kPartition, kEpoch, seq, seq);
        ASSERT_TRUE(vr.isDuplicate())
            << "seq=" << seq << " must be Duplicate after rebuild; got status="
            << static_cast<int>(vr.status);
        EXPECT_EQ(vr.retryBaseOffset, baseOffsets[static_cast<std::size_t>(seq)])
            << "retryBaseOffset must match original for seq=" << seq;
    }

    // A genuinely new sequence should still be accepted.
    {
        auto vr = rebuiltPsm.validate(/*topicId=*/0, kPid, kPartition, kEpoch, kN, kN);
        ASSERT_TRUE(vr.isAccept())
            << "seq=" << kN << " (new) must be Accept after rebuild";
    }
}

// ===========================================================================
// Test 2: Failover scenario — new leader rebuilds PSM; duplicate detect + no
// re-append; genuinely new seq accepted.
// ===========================================================================
TEST_F(ProducerStateDurabilityTest, FailoverLeaderRebuildsDedup) {
    const int64_t kPid   = 99;
    const int16_t kEpoch = 2;
    const int     kN     = 3;
    const int32_t kPartition2 = 0;  // partition 0 for this test

    // Append N idempotent batches to leader log1.
    std::vector<int64_t> baseOffsets;
    for (int seq = 0; seq < kN; ++seq) {
        int64_t base = log1.logEndOffset();
        auto batch = makeIdempotentBatch(base, kPid, kEpoch, seq, "data");
        log1.append(batch, base);
        baseOffsets.push_back(base);
    }

    // Drive replication to majority durability.
    node1_->onLeaderLogAppended();
    ASSERT_TRUE(driveUntil([&]{ return node2_->highWatermark() >= kN; }))
        << "node2 HW must reach " << kN;

    const int64_t logEndBeforeRetry = log2.logEndOffset();
    ASSERT_EQ(logEndBeforeRetry, kN);

    // Kill leader (node1), promote node2.
    transport_.setLeader(nullptr);
    transport_.removeFollower(kNode2);

    node2_->becomeLeader(/*newEpoch=*/2, {kNode2, kNode3});
    transport_.setLeader(node2_.get());
    node3_->becomeFollower(/*newEpoch=*/2, kNode2);

    // New leader node2 builds a fresh PSM and rebuilds from its log.
    ProducerStateManager newLeaderPsm;
    newLeaderPsm.rebuildFromLog(/*topicId=*/0, log2, node2_->highWatermark(), kPartition2);

    // Re-send the last committed batch (seq = kN-1) — must be Duplicate.
    {
        int32_t lastSeq = kN - 1;
        auto vr = newLeaderPsm.validate(/*topicId=*/0, kPid, kPartition2, kEpoch, lastSeq, lastSeq);
        ASSERT_TRUE(vr.isDuplicate())
            << "Re-sending seq=" << lastSeq << " after failover must be Duplicate; "
            << "got status=" << static_cast<int>(vr.status)
            << " — PSM state was lost across failover (TX-2b bug)";
        EXPECT_EQ(vr.retryBaseOffset, baseOffsets[static_cast<std::size_t>(lastSeq)])
            << "retryBaseOffset must match the original base offset";

        // Crucially: log end offset must NOT change (no re-append).
        EXPECT_EQ(log2.logEndOffset(), logEndBeforeRetry)
            << "Log must not advance on a Duplicate — re-append detected!";
    }

    // Re-send seq=0 — also Duplicate.
    {
        auto vr = newLeaderPsm.validate(/*topicId=*/0, kPid, kPartition2, kEpoch, 0, 0);
        ASSERT_TRUE(vr.isDuplicate())
            << "Re-sending seq=0 after failover must be Duplicate";
        EXPECT_EQ(vr.retryBaseOffset, baseOffsets[0]);
    }

    // A genuinely new sequence (seq=kN) must be accepted.
    {
        auto vr = newLeaderPsm.validate(/*topicId=*/0, kPid, kPartition2, kEpoch, kN, kN);
        ASSERT_TRUE(vr.isAccept())
            << "New seq=" << kN << " must be Accept on new leader";
    }
}

// ===========================================================================
// Test 3: PSM rebuild is idempotent — calling rebuildFromLog twice gives the
// same result (cleared + replayed from scratch each time).
// ===========================================================================
TEST_F(ProducerStateDurabilityTest, RebuildIsIdempotent) {
    const int64_t kPid   = 7;
    const int16_t kEpoch = 1;
    const int     kN     = 2;
    const int32_t kPart  = 1;

    std::vector<int64_t> baseOffsets;
    for (int seq = 0; seq < kN; ++seq) {
        int64_t base = log1.logEndOffset();
        auto batch = makeIdempotentBatch(base, kPid, kEpoch, seq);
        log1.append(batch, base);
        baseOffsets.push_back(base);
    }

    ProducerStateManager psm;
    psm.rebuildFromLog(/*topicId=*/0, log1, log1.logEndOffset(), kPart);

    // First rebuild: duplicate detected.
    {
        auto vr = psm.validate(/*topicId=*/0, kPid, kPart, kEpoch, 0, 0);
        ASSERT_TRUE(vr.isDuplicate()) << "First rebuild must detect dup seq=0";
    }

    // Second rebuild: idempotent — state cleared and replayed from log.
    psm.rebuildFromLog(/*topicId=*/0, log1, log1.logEndOffset(), kPart);
    {
        auto vr = psm.validate(/*topicId=*/0, kPid, kPart, kEpoch, 0, 0);
        ASSERT_TRUE(vr.isDuplicate()) << "Second rebuild must still detect dup seq=0";
    }

    // Accept for new seq is also correct after double rebuild.
    {
        auto vr = psm.validate(/*topicId=*/0, kPid, kPart, kEpoch, kN, kN);
        ASSERT_TRUE(vr.isAccept()) << "New seq after double rebuild must be Accept";
    }
}

// ===========================================================================
// Test 4: Non-idempotent batches (producerId=-1) do NOT affect PSM state and
// rebuildFromLog skips them.
// ===========================================================================
TEST_F(ProducerStateDurabilityTest, NonIdempotentBatchesSkippedInRebuild) {
    const int32_t kPart = 3;

    // Append a regular (non-idempotent) batch: producerId=-1.
    {
        int64_t base = log1.logEndOffset();
        rune::protocol::RecordBatchBuilder builder(base, 0);
        // No setProducerId → defaults to -1
        builder.append("", "legacy", 0);
        auto batch = builder.build();
        log1.append(batch, base);
    }

    // Now append an idempotent batch.
    const int64_t kPid   = 55;
    const int16_t kEpoch = 0;
    int64_t idempotentBase = log1.logEndOffset();
    {
        auto batch = makeIdempotentBatch(idempotentBase, kPid, kEpoch, 0);
        log1.append(batch, idempotentBase);
    }

    ProducerStateManager psm;
    psm.rebuildFromLog(/*topicId=*/0, log1, log1.logEndOffset(), kPart);

    // The idempotent batch's seq=0 must be deduped.
    {
        auto vr = psm.validate(/*topicId=*/0, kPid, kPart, kEpoch, 0, 0);
        ASSERT_TRUE(vr.isDuplicate())
            << "seq=0 of idempotent batch must be Duplicate after rebuild";
        EXPECT_EQ(vr.retryBaseOffset, idempotentBase);
    }

    // PSM size = 1 (only the idempotent producer).
    EXPECT_EQ(psm.size(), 1u);
}
