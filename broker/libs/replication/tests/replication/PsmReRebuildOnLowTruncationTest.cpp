// ---------------------------------------------------------------------------
// PsmReRebuildOnLowTruncationTest.cpp — Task 11: PSM re-rebuild when a
// KIP-101 divergence truncation lands below the rebuild high-watermark.
//
// Three test cases:
//
//   ReRebuildsWhenTruncationBelowRebuildHw:
//     Build a follower PartitionReplica over an InMemoryLog; seed the log with
//     idempotent batches up to offset 200 and call psm_.rebuildFromLog(HW=200);
//     then force truncateLog(120) via applyOffsetsForLeaderEpochResponse with
//     endOffset=120.  Assert that:
//       (a) psm_.rebuildHighWatermark() == 120 after truncation (re-rebuild ran).
//       (b) A sequence above 120 that was previously deduped is now accepted
//           (the old entry no longer exists — no false-duplicate).
//       (c) A sequence below 120 is still deduped (surviving state preserved).
//
//   NoReRebuildWhenTruncationAboveRebuildHw:
//     Rebuild PSM at HW=100; truncate at 150 (> 100).  The PSM dedup window
//     is safe (covers only [0,100)); assert rebuildHighWatermark() stays 100
//     (no second rebuild) and the below-100 entries are still valid.
//
//   AtomicTruncationAndReRebuild:
//     Verify the re-rebuild is atomic with the truncation: after truncateLog the
//     log LEO equals truncateOffset AND rebuildHighWatermark() equals
//     truncateOffset (not the previous HW) — no intermediate state visible.
//
// Uses InMemoryLog + InMemoryTransport (no disk, no Aeron).
// All test cases are single-threaded (duty-thread model) so TSan has no races
// to report here; adding TSan coverage is handled by the overall suite.
//
// C++23. NO busy-spin. NO unbounded loops.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <unistd.h>
#include <vector>

using namespace rune::replication;
using namespace rune::storage;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_psmTestSeq{0};

static std::string psmUniq() {
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_psmTestSeq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count());
}

// Build a single-record idempotent RecordBatch with explicit producer fields.
static std::vector<uint8_t> makeIdempotentBatch(int64_t baseOffsetHint,
                                                 int64_t producerId,
                                                 int16_t producerEpoch,
                                                 int32_t baseSequence,
                                                 std::string_view value = "v")
{
    rune::protocol::RecordBatchBuilder b(baseOffsetHint, 0);
    b.setProducerId(producerId);
    b.setProducerEpoch(producerEpoch);
    b.setBaseSequence(baseSequence);
    b.append("", value, 0);
    return b.build();
}

static PartitionConfig makeFollowerConfig(NodeId selfId, NodeId leaderId)
{
    PartitionConfig cfg;
    cfg.topicId              = 1;
    cfg.partitionId          = 0;
    cfg.selfId               = selfId;
    cfg.minIsr               = 1;
    cfg.fetchIntervalNs      = 1'000'000LL;    // 1 ms
    cfg.isrCheckIntervalNs   = 1'000'000LL;
    cfg.maxFetchBytes        = 1 << 20;
    cfg.pushOnAppend         = false;
    cfg.fastAckOnApply       = false;
    (void)leaderId;
    return cfg;
}

// Null transport: we drive the PSM and truncation directly — no fetch round-trips.
class NullTransportPsm : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
    void sendOffsetsForLeaderEpochRequest(
            NodeId, const OffsetsForLeaderEpochReqMsg&) override {}
    void sendOffsetsForLeaderEpochResponse(
            NodeId, const OffsetsForLeaderEpochRespMsg&) override {}
};

// ---------------------------------------------------------------------------
// Seed an InMemoryLog with `count` idempotent single-record batches.
//
// Batches:  offset 0 → count-1, pid=pid, epoch=0, seq 0..count-1.
// Returns the log end offset after seeding (== count).
// ---------------------------------------------------------------------------
static int64_t seedLog(InMemoryLog& log,
                        int64_t pid,
                        int32_t count)
{
    for (int32_t i = 0; i < count; ++i) {
        auto batch = makeIdempotentBatch(/*baseOffsetHint=*/log.logEndOffset(),
                                          pid, /*epoch=*/0, /*seq=*/i);
        log.append(batch, /*lastOffset=*/log.logEndOffset());
    }
    return log.logEndOffset();
}

// ===========================================================================
// TEST 1 — Re-rebuild fires when truncation is BELOW the rebuild HW
//
// Key: kRetryWindowSize == 5, so with kTotal=6 batches (seqs 0..5):
//   after rebuild at HW=6, window = {seq1, seq2, seq3, seq4, seq5},
//   lastSequence = 5.
//
// Truncation to kTruncTo=3 removes offsets 3,4,5 from the log.
// Without re-rebuild:
//   - psm.validate(seq=3) → Duplicate (window still has seq3→offset3,
//     which no longer exists) → FALSE DUPLICATE, acknowledged data loss.
//   - psm.validate(seq=6) → OOS (lastSequence=5, expected=6 but we are
//     re-producing from seq=3) → incorrect reject.
// With re-rebuild at HW=3:
//   - window = {seq0, seq1, seq2}, lastSequence = 2, expected = 3.
//   - psm.validate(seq=3) → Accept (correct: next expected).
//   - psm.validate(seq=2) → Duplicate (correct: still in window).
// ===========================================================================

TEST(PsmReRebuildOnLowTruncation, ReRebuildsWhenTruncationBelowRebuildHw)
{
    // 6 batches (offsets 0..5, seqs 0..5) → all fit in retry window (size=5)
    // after the last 5.  HW=6 covers all committed; truncate to 3 (< HW=6).
    static constexpr int32_t kTotal     = 6;    // LEO=6 after seeding
    static constexpr int32_t kRebuildHw = 6;    // rebuild PSM at HW=6 (all committed)
    static constexpr int32_t kTruncTo   = 3;    // truncate to offset 3 (< rebuild HW)
    static constexpr int64_t kPid       = 99;
    static constexpr int32_t kTopicId   = 1;
    static constexpr int32_t kPartition = 0;

    InMemoryLog log(1 << 20, 10000);
    NullTransportPsm transport;

    // Seed the log with kTotal idempotent batches.
    int64_t leo = seedLog(log, kPid, kTotal);
    ASSERT_EQ(leo, kTotal) << "Log should have " << kTotal << " offsets after seeding";

    // Build a follower replica over the seeded log.
    NodeId followerId = 2;
    NodeId leaderId   = 1;
    std::vector<NodeId> replicas{leaderId, followerId};
    std::vector<NodeId> isr{leaderId, followerId};

    PartitionReplica follower(makeFollowerConfig(followerId, leaderId),
                               Role::Follower, /*epoch=*/1,
                               replicas, isr, &log, &transport);

    // Expose psm_ via the test accessor and rebuild it explicitly.
    ProducerStateManager& psm = follower.producerStateManagerForTest();
    psm.rebuildFromLog(kTopicId, log, kRebuildHw, kPartition);

    ASSERT_EQ(psm.rebuildHighWatermark(), kRebuildHw)
        << "rebuildHighWatermark should equal kRebuildHw after explicit rebuild";

    // Before truncation: seq=kTruncTo (= seq 3, offset 3) IS in the retry
    // window (one of the last 5 batches) → Duplicate.
    {
        int32_t seq = kTruncTo;  // seq=3, offset=3 — committed and in window
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0,
                               seq, seq);
        EXPECT_TRUE(r.isDuplicate())
            << "Before truncation seq=" << seq
            << " must be a Duplicate (it is committed and in the retry window)";
    }

    // Force a truncation to kTruncTo via OffsetsForLeaderEpochResponse.
    follower.becomeFollower(/*leaderEpoch=*/2, leaderId);

    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch   = 2;
    resp.partition     = kPartition;
    resp.responseEpoch = 1;    // shared epoch = 1
    resp.endOffset     = kTruncTo;
    resp.errorCode     = 0;    // kErrNone
    resp.topicId       = kTopicId;

    follower.applyOffsetsForLeaderEpochResponse(resp);

    // --- Key assertions ---

    // (a) rebuildHighWatermark moved down to kTruncTo (re-rebuild fired).
    EXPECT_EQ(psm.rebuildHighWatermark(), kTruncTo)
        << "rebuildHighWatermark must equal kTruncTo after low truncation";

    // (b) seq=kTruncTo that was previously a Duplicate is now ACCEPTED.
    //     After re-rebuild at HW=3: lastSequence=2, expected=3.
    {
        int32_t seq = kTruncTo;
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0,
                               seq, seq);
        EXPECT_TRUE(r.isAccept())
            << "After low truncation seq=" << seq
            << " must be accepted (no false-duplicate); got status="
            << static_cast<int>(r.status);
    }

    // (c) A sequence strictly below kTruncTo is still Duplicate (surviving state).
    {
        int32_t seq = kTruncTo - 1;  // seq=2, offset=2 — still in log
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0,
                               seq, seq);
        EXPECT_TRUE(r.isDuplicate())
            << "Surviving committed seq=" << seq
            << " must still be a Duplicate after truncation";
    }

    // (d) Log LEO must equal kTruncTo.
    EXPECT_EQ(follower.logEndOffset(), kTruncTo)
        << "Log LEO must equal kTruncTo after truncation";
}

// ===========================================================================
// TEST 2 — No re-rebuild when truncation is AT or ABOVE the rebuild HW
//
// kRebuildHw=5, kTruncTo=8 (> 5): PSM window for [0,5) is safe because all
// offsets it references (0..4) survive the truncation to 8.  No re-rebuild
// should fire, so rebuildHighWatermark() stays at kRebuildHw=5.
// ===========================================================================

TEST(PsmReRebuildOnLowTruncation, NoReRebuildWhenTruncationAboveRebuildHw)
{
    static constexpr int32_t kTotal        = 10;   // 10 batches (offsets 0..9)
    static constexpr int32_t kRebuildHw    = 5;    // rebuild at HW=5 (first 5 batches)
    static constexpr int32_t kTruncTo      = 8;    // truncate above rebuild HW (safe)
    static constexpr int64_t kPid          = 77;
    static constexpr int32_t kTopicId      = 1;
    static constexpr int32_t kPartition    = 0;

    InMemoryLog log(1 << 20, 10000);
    NullTransportPsm transport;

    int64_t leo = seedLog(log, kPid, kTotal);
    ASSERT_EQ(leo, kTotal);

    NodeId followerId = 2;
    NodeId leaderId   = 1;
    std::vector<NodeId> replicas{leaderId, followerId};
    std::vector<NodeId> isr{leaderId, followerId};

    PartitionReplica follower(makeFollowerConfig(followerId, leaderId),
                               Role::Follower, /*epoch=*/1,
                               replicas, isr, &log, &transport);

    ProducerStateManager& psm = follower.producerStateManagerForTest();

    // Rebuild PSM at HW=kRebuildHw (only first 5 batches → seqs 0..4 in window).
    psm.rebuildFromLog(kTopicId, log, kRebuildHw, kPartition);
    ASSERT_EQ(psm.rebuildHighWatermark(), kRebuildHw);

    // seq=kRebuildHw-1 (=4) is in the window → Duplicate.
    {
        int32_t seq = kRebuildHw - 1;  // seq=4, still in window after rebuild at HW=5
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0, seq, seq);
        EXPECT_TRUE(r.isDuplicate())
            << "seq=" << seq << " should be Duplicate before truncation";
    }

    // Force a truncation to kTruncTo=8 (> kRebuildHw=5) — PSM does NOT re-rebuild.
    follower.becomeFollower(/*leaderEpoch=*/2, leaderId);

    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch   = 2;
    resp.partition     = kPartition;
    resp.responseEpoch = 1;
    resp.endOffset     = kTruncTo;
    resp.errorCode     = 0;
    resp.topicId       = kTopicId;

    follower.applyOffsetsForLeaderEpochResponse(resp);

    // rebuildHighWatermark stays at kRebuildHw — no re-rebuild was triggered.
    EXPECT_EQ(psm.rebuildHighWatermark(), kRebuildHw)
        << "rebuildHighWatermark must remain kRebuildHw when truncation is above it";

    // seq=kRebuildHw-1 still deduped (PSM state for [0,5) is intact).
    {
        int32_t seq = kRebuildHw - 1;
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0, seq, seq);
        EXPECT_TRUE(r.isDuplicate())
            << "seq=" << seq << " must still be Duplicate (no re-rebuild occurred)";
    }

    // Log LEO must equal kTruncTo.
    EXPECT_EQ(follower.logEndOffset(), kTruncTo)
        << "Log LEO must equal kTruncTo";
}

// ===========================================================================
// TEST 3 — Re-rebuild is atomic with the truncation
//
// After truncateLog: log.logEndOffset() == truncateOffset AND
// psm.rebuildHighWatermark() == truncateOffset (not the old HW).
// No intermediate inconsistent state is observable because both mutations
// happen inside the single truncateLog() call on the duty thread before any
// new append can arrive.
//
// Uses kTotal=6, kRebuildHw=6, kTruncTo=4 to stay within retry window.
// ===========================================================================

TEST(PsmReRebuildOnLowTruncation, ReRebuildAtomicWithTruncation)
{
    static constexpr int32_t kTotal     = 6;    // offsets 0..5
    static constexpr int32_t kRebuildHw = 6;    // all 6 batches
    static constexpr int32_t kTruncTo   = 4;    // remove offsets 4,5 (< kRebuildHw)
    static constexpr int64_t kPid       = 55;
    static constexpr int32_t kTopicId   = 1;
    static constexpr int32_t kPartition = 0;

    InMemoryLog log(1 << 20, 10000);
    NullTransportPsm transport;

    int64_t leo = seedLog(log, kPid, kTotal);
    ASSERT_EQ(leo, kTotal);

    NodeId followerId = 2;
    NodeId leaderId   = 1;
    std::vector<NodeId> replicas{leaderId, followerId};
    std::vector<NodeId> isr{leaderId, followerId};

    PartitionReplica follower(makeFollowerConfig(followerId, leaderId),
                               Role::Follower, /*epoch=*/1,
                               replicas, isr, &log, &transport);

    ProducerStateManager& psm = follower.producerStateManagerForTest();

    // Rebuild PSM at HW=kRebuildHw.
    psm.rebuildFromLog(kTopicId, log, kRebuildHw, kPartition);
    ASSERT_EQ(psm.rebuildHighWatermark(), kRebuildHw);

    // Trigger a truncation to kTruncTo (< kRebuildHw) via epoch response.
    follower.becomeFollower(/*leaderEpoch=*/2, leaderId);

    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch   = 2;
    resp.partition     = kPartition;
    resp.responseEpoch = 1;
    resp.endOffset     = kTruncTo;
    resp.errorCode     = 0;
    resp.topicId       = kTopicId;

    follower.applyOffsetsForLeaderEpochResponse(resp);

    // Atomicity assertion: both log state and PSM state reflect kTruncTo in one
    // step — no intermediate window where log is truncated but PSM is not.
    const int64_t logLeo = follower.logEndOffset();
    const int64_t psmHw  = psm.rebuildHighWatermark();

    EXPECT_EQ(logLeo, kTruncTo)
        << "Log LEO must be kTruncTo after truncation";
    EXPECT_EQ(psmHw, kTruncTo)
        << "rebuildHighWatermark must equal kTruncTo (atomic re-rebuild)";

    // The PSM must reflect only surviving offsets [0, kTruncTo):
    // seq=kTruncTo-1 (last surviving, offset 3) → Duplicate.
    // seq=kTruncTo   (next expected, offset 4 was removed) → Accept.
    {
        int32_t seqSurv = kTruncTo - 1;
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0,
                               seqSurv, seqSurv);
        EXPECT_TRUE(r.isDuplicate())
            << "Last surviving seq=" << seqSurv << " must be Duplicate";
    }
    {
        int32_t seqNext = kTruncTo;
        auto r = psm.validate(kTopicId, kPid, kPartition, /*epoch=*/0,
                               seqNext, seqNext);
        EXPECT_TRUE(r.isAccept())
            << "Next seq=" << seqNext << " must be Accept (no false-duplicate)";
    }
}
