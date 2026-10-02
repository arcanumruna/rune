// ProducePathTest.cpp — Unit tests for rune::storage::handleProduceBatch (E5.1.2b/d).
//
// Drives the shared produce-batch helper DIRECTLY against a real Log +
// ProducerStateManager — no Aeron, no gateway, no worker.
//
// Tests:
//   1. HappyPath_AppendAndOffsetRestamp
//        Single batch → OK, baseOffset correct, LEO advances, restamp verified.
//   2. HappyPath_TwoBatches_OffsetsContinuous
//        Two sequential batches → baseOffsets are contiguous.
//   3. IdempotentDuplicate_NoDuplicateAppend
//        Same (pid, epoch, seq) sent twice → second returns isDuplicate + prior offset,
//        LEO unchanged.
//   4. IdempotentOutOfOrder_Rejected
//        Seq gap → OutOfOrderSequence, log untouched.
//   5. IdempotentFenced_Rejected
//        Lower epoch after higher epoch → InvalidProducerEpoch, log untouched.
//   6. CrcCorrupt_Rejected
//        CRC-corrupted batch → Internal error, log untouched.
//   7. MinIsr_acksAll_Rejected
//        acks=All + isrSize < minIsr → MinIsrViolation.
//   8. MinIsr_acksAll_NoReplica_Passthrough
//        acks=All + isrSize==0 (no replica) → appended OK (passthrough).
//   9. LegacyProducer_NoDedup
//        producerId==-1 → appends twice without dedup.
//  10. BatchTooShort_Rejected
//        batchLen < kMinBatchSize → Internal.
//  11. (E5.1.2d nit Q3) DurabilityTier2_StrictSync
//        durabilityTier==2 on a disk-backed log → syncDone=true in the outcome.

#include <gtest/gtest.h>

#include <rune/storage/ProducePath.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::storage {
namespace {

namespace pc  = rune::protocol::client;
namespace fs  = std::filesystem;

// ---------------------------------------------------------------------------
// Temp-dir RAII helper.
// ---------------------------------------------------------------------------
struct TmpDir {
    fs::path path;
    explicit TmpDir(std::string_view tag = "") {
        static std::atomic<uint32_t> seq{0};
        auto n = seq.fetch_add(1, std::memory_order_relaxed);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto name = std::string("rune_ppt_") + std::string(tag)
                  + std::to_string(static_cast<long>(::getpid()))
                  + "_" + std::to_string(n)
                  + "_" + std::to_string(ts);
        path = fs::temp_directory_path() / name;
        fs::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ---------------------------------------------------------------------------
// Build a RecordBatch with `recordCount` single-char records.
// Optional idempotent producer fields (pid==-1 → legacy).
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(
        int32_t   recordCount   = 1,
        int64_t   producerId    = -1,
        int16_t   producerEpoch = 0,
        int32_t   baseSequence  = -1)
{
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    if (producerId >= 0) {
        b.setProducerId(producerId);
        b.setProducerEpoch(producerEpoch);
        b.setBaseSequence(baseSequence);
    }
    for (int32_t i = 0; i < recordCount; ++i) {
        b.append("k", "v", 1000LL + i);
    }
    return b.build();
}

// ---------------------------------------------------------------------------
// Convenience: call handleProduceBatch with default acks / minIsr / tier.
// ---------------------------------------------------------------------------
static ProduceOutcome callHelper(
        std::vector<uint8_t>&              batch,
        rune::storage::Log&                log,
        rune::storage::ProducerStateManager& psm,
        int32_t topicId    = 1,
        int32_t partition  = 0,
        int64_t producerId = -1,
        int16_t prodEpoch  = 0,
        int32_t baseSeq    = -1,
        bool    acksAll    = false,
        int     isrSize    = 0,
        int     minIsr     = 1,
        uint8_t tier       = 1)
{
    return handleProduceBatch(
        batch.data(), batch.size(),
        topicId, partition,
        producerId, prodEpoch, baseSeq,
        log, psm,
        acksAll, isrSize, minIsr, tier);
}

// ---------------------------------------------------------------------------
// Fixture: each test gets a fresh Log + PSM.
// ---------------------------------------------------------------------------
class ProducePathTest : public ::testing::Test {
protected:
    ProducePathTest() : dir_("fix"), log_(dir_.path.string()) {}

    TmpDir                           dir_;
    rune::storage::Log               log_;
    rune::storage::ProducerStateManager psm_;
};

// ===========================================================================
// TEST 1: HappyPath_AppendAndOffsetRestamp
// ===========================================================================
TEST_F(ProducePathTest, HappyPath_AppendAndOffsetRestamp)
{
    auto batch = makeBatch(3);

    // LEO is 0 before any append.
    ASSERT_EQ(log_.logEndOffset(), 0);

    auto out = callHelper(batch, log_, psm_);

    EXPECT_TRUE(out.isOK());
    EXPECT_FALSE(out.isDuplicate);
    EXPECT_EQ(out.baseOffset, 0);
    EXPECT_EQ(out.lastOffset, 2);   // base=0, delta=2 (3 records → delta=2)
    EXPECT_EQ(out.recordCount, 3);

    // LEO must have advanced to lastOffset+1.
    EXPECT_EQ(log_.logEndOffset(), 3);

    // Verify the restamp: bytes[0..7] of the returned batch must be the assigned
    // base offset (0) in little-endian.
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(batch[i], 0u) << "restamp byte " << i << " wrong";
    }
}

// ===========================================================================
// TEST 2: HappyPath_TwoBatches_OffsetsContinuous
// ===========================================================================
TEST_F(ProducePathTest, HappyPath_TwoBatches_OffsetsContinuous)
{
    auto b1 = makeBatch(2);
    auto b2 = makeBatch(3);

    auto o1 = callHelper(b1, log_, psm_);
    EXPECT_TRUE(o1.isOK());
    EXPECT_EQ(o1.baseOffset, 0);
    EXPECT_EQ(o1.lastOffset, 1);  // 2 records → delta=1

    auto o2 = callHelper(b2, log_, psm_);
    EXPECT_TRUE(o2.isOK());
    EXPECT_EQ(o2.baseOffset, 2);  // immediately after the first batch
    EXPECT_EQ(o2.lastOffset, 4);  // 3 records → delta=2

    EXPECT_EQ(log_.logEndOffset(), 5);
}

// ===========================================================================
// TEST 3: IdempotentDuplicate_NoDuplicateAppend
// ===========================================================================
TEST_F(ProducePathTest, IdempotentDuplicate_NoDuplicateAppend)
{
    constexpr int64_t kPid   = 42;
    constexpr int16_t kEpoch =  1;
    constexpr int32_t kSeq   =  0;

    auto b1 = makeBatch(2, kPid, kEpoch, kSeq);
    auto b2 = makeBatch(2, kPid, kEpoch, kSeq); // exact retry

    // First produce: should succeed.
    auto o1 = callHelper(b1, log_, psm_,
                         /*topicId=*/1, /*partition=*/0,
                         kPid, kEpoch, kSeq);
    ASSERT_TRUE(o1.isOK());
    ASSERT_FALSE(o1.isDuplicate);
    const int64_t priorBase = o1.baseOffset;
    const int64_t leo1      = log_.logEndOffset();

    // Second produce (exact retry): should be deduped.
    auto o2 = callHelper(b2, log_, psm_,
                         /*topicId=*/1, /*partition=*/0,
                         kPid, kEpoch, kSeq);
    EXPECT_TRUE(o2.isOK());
    EXPECT_TRUE(o2.isDuplicate);
    EXPECT_EQ(o2.baseOffset, priorBase);   // prior offset returned
    EXPECT_EQ(log_.logEndOffset(), leo1);  // LEO unchanged (no re-append)
}

// ===========================================================================
// TEST 4: IdempotentOutOfOrder_Rejected
// ===========================================================================
TEST_F(ProducePathTest, IdempotentOutOfOrder_Rejected)
{
    constexpr int64_t kPid   = 7;
    constexpr int16_t kEpoch = 0;

    // Produce seq=0 first.
    auto b1 = makeBatch(1, kPid, kEpoch, /*baseSeq=*/0);
    auto o1 = callHelper(b1, log_, psm_,
                         /*topicId=*/1, /*partition=*/0,
                         kPid, kEpoch, /*baseSeq=*/0);
    ASSERT_TRUE(o1.isOK());
    ASSERT_FALSE(o1.isDuplicate);

    const int64_t leo1 = log_.logEndOffset();

    // Gap: next expected seq=1, but we send seq=10.
    auto b2 = makeBatch(1, kPid, kEpoch, /*baseSeq=*/10);
    auto o2 = callHelper(b2, log_, psm_,
                         /*topicId=*/1, /*partition=*/0,
                         kPid, kEpoch, /*baseSeq=*/10);
    EXPECT_EQ(o2.error, pc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(log_.logEndOffset(), leo1);  // log untouched
}

// ===========================================================================
// TEST 5: IdempotentFenced_Rejected
// ===========================================================================
TEST_F(ProducePathTest, IdempotentFenced_Rejected)
{
    constexpr int64_t kPid = 99;

    // Establish epoch=2.
    auto b1 = makeBatch(1, kPid, /*epoch=*/2, /*seq=*/0);
    auto o1 = callHelper(b1, log_, psm_,
                         1, 0, kPid, /*epoch=*/2, /*seq=*/0);
    ASSERT_TRUE(o1.isOK());

    const int64_t leo1 = log_.logEndOffset();

    // Zombie: epoch=1 (lower than established epoch=2).
    auto b2 = makeBatch(1, kPid, /*epoch=*/1, /*seq=*/1);
    auto o2 = callHelper(b2, log_, psm_,
                         1, 0, kPid, /*epoch=*/1, /*seq=*/1);
    EXPECT_EQ(o2.error, pc::ErrorCode::InvalidProducerEpoch);
    EXPECT_EQ(log_.logEndOffset(), leo1);  // log untouched
}

// ===========================================================================
// TEST 6: CrcCorrupt_Rejected
// ===========================================================================
TEST_F(ProducePathTest, CrcCorrupt_Rejected)
{
    auto batch = makeBatch(1);

    // Corrupt one byte inside the CRC-covered region (past kAttributesOffset=21).
    // kAttributesOffset=21; corrupt byte at offset 22 (inside CRC region).
    ASSERT_GT(batch.size(), 22u);
    batch[22] ^= 0xFF;

    const int64_t leo0 = log_.logEndOffset();

    auto out = callHelper(batch, log_, psm_);
    EXPECT_EQ(out.error, pc::ErrorCode::Internal);
    EXPECT_EQ(log_.logEndOffset(), leo0);  // log untouched
}

// ===========================================================================
// TEST 7: MinIsr_acksAll_Rejected
// ===========================================================================
TEST_F(ProducePathTest, MinIsr_acksAll_Rejected)
{
    auto batch = makeBatch(1);
    const int64_t leo0 = log_.logEndOffset();

    // acksAll=true, isrSize=1, minIsr=2 → MinIsrViolation.
    auto out = callHelper(batch, log_, psm_,
                          /*topicId=*/1, /*partition=*/0,
                          /*producerId=*/-1, /*epoch=*/0, /*seq=*/-1,
                          /*acksAll=*/true, /*isrSize=*/1, /*minIsr=*/2);
    EXPECT_EQ(out.error, pc::ErrorCode::MinIsrViolation);
    EXPECT_EQ(log_.logEndOffset(), leo0);  // log untouched
}

// ===========================================================================
// TEST 8: MinIsr_acksAll_NoReplica_Passthrough
// ===========================================================================
TEST_F(ProducePathTest, MinIsr_acksAll_NoReplica_Passthrough)
{
    auto batch = makeBatch(2);

    // acksAll=true, isrSize=0 (no replica wired) → passthrough (single-node / test).
    auto out = callHelper(batch, log_, psm_,
                          /*topicId=*/1, /*partition=*/0,
                          /*producerId=*/-1, /*epoch=*/0, /*seq=*/-1,
                          /*acksAll=*/true, /*isrSize=*/0, /*minIsr=*/2);
    EXPECT_TRUE(out.isOK());
    EXPECT_FALSE(out.isDuplicate);
    EXPECT_EQ(out.baseOffset, 0);
    EXPECT_EQ(log_.logEndOffset(), 2);
}

// ===========================================================================
// TEST 9: LegacyProducer_NoDedup
// ===========================================================================
TEST_F(ProducePathTest, LegacyProducer_NoDedup)
{
    // producerId==-1 → legacy path; same batch appended twice without dedup.
    auto b1 = makeBatch(2);
    auto b2 = makeBatch(2);  // same content, but legacy = no PSM check

    auto o1 = callHelper(b1, log_, psm_);
    ASSERT_TRUE(o1.isOK());
    EXPECT_EQ(o1.baseOffset, 0);

    auto o2 = callHelper(b2, log_, psm_);
    ASSERT_TRUE(o2.isOK());
    // Must NOT be deduped — legacy produces always append.
    EXPECT_FALSE(o2.isDuplicate);
    EXPECT_EQ(o2.baseOffset, 2);  // LEO advanced past first batch

    EXPECT_EQ(log_.logEndOffset(), 4);
}

// ===========================================================================
// TEST 10: BatchTooShort_Rejected
// ===========================================================================
TEST_F(ProducePathTest, BatchTooShort_Rejected)
{
    // A buffer shorter than kMinBatchSize = kRecordCountOffset + 4 = 61 bytes.
    std::vector<uint8_t> tiny(10, 0xAB);
    const int64_t leo0 = log_.logEndOffset();

    auto out = handleProduceBatch(
        tiny.data(), tiny.size(),
        /*topicId=*/1, /*partition=*/0,
        /*producerId=*/-1, /*producerEpoch=*/0, /*baseSequence=*/-1,
        log_, psm_,
        /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*durabilityTier=*/1);
    EXPECT_EQ(out.error, pc::ErrorCode::Internal);
    EXPECT_EQ(log_.logEndOffset(), leo0);
}

// ===========================================================================
// TEST 11: DurabilityTier2_StrictSync (E5.1.2d nit Q3)
//
// durabilityTier==2 on a disk-backed log → sync() is called inline and
// syncDone == true in the outcome.
// ===========================================================================
TEST_F(ProducePathTest, DurabilityTier2_StrictSync)
{
    auto batch = makeBatch(1);

    // Use tier=2 (Strict).  The fixture's log_ IS disk-backed (real Log on tmpdir),
    // so sync() runs inline and returns syncDone=true.
    auto out = callHelper(batch, log_, psm_,
                          /*topicId=*/1, /*partition=*/0,
                          /*producerId=*/-1, /*prodEpoch=*/0, /*baseSeq=*/-1,
                          /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1,
                          /*tier=*/2);

    EXPECT_TRUE(out.isOK());
    EXPECT_FALSE(out.isDuplicate);
    EXPECT_EQ(out.baseOffset, 0);
    EXPECT_EQ(out.lastOffset, 0);
    EXPECT_TRUE(out.syncDone)
        << "durabilityTier==2 on a disk-backed log must set syncDone=true";
    EXPECT_EQ(log_.logEndOffset(), 1);
}

// ===========================================================================
// EOS-DUP defense (eos-dup-fix-review §3): the Produce frame's producer
// identity (not CRC-covered) must equal the CRC-covered RecordBatch header
// (offsets 43/51/53).  The pre-460dd12 client sent frame pid -1 with a header
// pid >= 0: the broker skipped dedup and silently appended every retransmit.
// A mismatch is now a loud, non-retriable SchemaValidationFailed (the record
// failed broker validation; resending the same bytes always fails again),
// never appended, never seen by the PSM.
// ===========================================================================
TEST_F(ProducePathTest, IdentityRegression_FramePidMinusOneWithIdempotentHeaderRejected)
{
    auto b = makeBatch(1, /*pid=*/42, /*epoch=*/0, /*seq=*/0);
    auto o = callHelper(b, log_, psm_, 1, 0, /*producerId=*/-1, /*epoch=*/0, /*seq=*/-1);
    EXPECT_EQ(o.error, pc::ErrorCode::SchemaValidationFailed);
    EXPECT_TRUE(o.identityMismatch);
    EXPECT_EQ(log_.logEndOffset(), 0) << "never appended";
    EXPECT_EQ(psm_.size(), 0u);
}

TEST_F(ProducePathTest, IdentityRegression_FramePidWithLegacyHeaderRejected)
{
    auto b = makeBatch(1);                                   // header pid -1
    auto o = callHelper(b, log_, psm_, 1, 0, /*producerId=*/42, /*epoch=*/0, /*seq=*/0);
    EXPECT_EQ(o.error, pc::ErrorCode::SchemaValidationFailed);
    EXPECT_TRUE(o.identityMismatch);
    EXPECT_EQ(log_.logEndOffset(), 0);
    EXPECT_EQ(psm_.size(), 0u) << "the PSM never sees a mismatched identity";
}

TEST_F(ProducePathTest, IdentityRegression_EpochOrSequenceMismatchRejected)
{
    auto b = makeBatch(1, /*pid=*/42, /*epoch=*/3, /*seq=*/0);
    auto oe = callHelper(b, log_, psm_, 1, 0, 42, /*epoch=*/2, /*seq=*/0);
    EXPECT_EQ(oe.error, pc::ErrorCode::SchemaValidationFailed);
    EXPECT_TRUE(oe.identityMismatch);
    auto os = callHelper(b, log_, psm_, 1, 0, 42, /*epoch=*/3, /*seq=*/1);
    EXPECT_EQ(os.error, pc::ErrorCode::SchemaValidationFailed);
    EXPECT_TRUE(os.identityMismatch);
    auto op = callHelper(b, log_, psm_, 1, 0, /*producerId=*/43, /*epoch=*/3, /*seq=*/0);
    EXPECT_EQ(op.error, pc::ErrorCode::SchemaValidationFailed) << "both pids >= 0 but different";
    EXPECT_TRUE(op.identityMismatch);
    auto ok = callHelper(b, log_, psm_, 1, 0, 42, /*epoch=*/3, /*seq=*/0);
    EXPECT_TRUE(ok.isOK()) << "the matching frame is accepted";
    EXPECT_FALSE(ok.identityMismatch);
    EXPECT_EQ(log_.logEndOffset(), 1);
}

// Legacy (acks=None / non-idempotent) frames carry epoch 0 against a header
// epoch of -1 (client m1): only producerId is compared when it is < 0.
TEST_F(ProducePathTest, IdentityRegression_LegacyFrameEpochZeroAgainstHeaderMinusOneAccepted)
{
    auto b = makeBatch(1);                                   // header pid/epoch/seq = -1
    auto o = callHelper(b, log_, psm_, 1, 0, /*producerId=*/-1, /*epoch=*/0, /*seq=*/-1);
    EXPECT_TRUE(o.isOK());
    EXPECT_FALSE(o.identityMismatch);
    EXPECT_EQ(log_.logEndOffset(), 1);
}

// EOS-OOS: a retransmit older than the window but entirely <= lastSequence is
// a KNOWN-OLD duplicate: OK, offset unknown (-1), never appended.  lastOffset
// carries the offset its ack condition must cover (the batch holding
// lastSequence), so the caller parks it exactly like an in-window duplicate.
TEST_F(ProducePathTest, IdempotenceWindowRegression_KnownOldDuplicateIsOkUnknownOffsetNoAppend)
{
    constexpr int64_t kPid = 77;
    const int kBatches = rune::protocol::client::kIdempotentMaxInFlightBatches + 8;
    for (int i = 0; i < kBatches; ++i) {
        auto b = makeBatch(2, kPid, 0, 2 * i);
        ASSERT_TRUE(callHelper(b, log_, psm_, 1, 0, kPid, 0, 2 * i).isOK()) << i;
    }
    const int64_t leo = log_.logEndOffset();
    auto old = makeBatch(2, kPid, 0, 0);
    auto o = callHelper(old, log_, psm_, 1, 0, kPid, 0, 0);
    EXPECT_TRUE(o.isOK()) << "never OutOfOrderSequence for a stored batch";
    EXPECT_TRUE(o.isDuplicate);
    EXPECT_EQ(o.baseOffset, -1) << "Kafka DUPLICATE_SEQUENCE_NUMBER: offset unknown";
    EXPECT_EQ(o.lastOffset, leo - 1) << "covers the batch holding lastSequence";
    EXPECT_EQ(log_.logEndOffset(), leo) << "never re-appended";
    // An in-window retransmit still reports its original range.
    auto recent = makeBatch(2, kPid, 0, 2 * (kBatches - 1));
    auto r = callHelper(recent, log_, psm_, 1, 0, kPid, 0, 2 * (kBatches - 1));
    EXPECT_TRUE(r.isDuplicate);
    EXPECT_EQ(r.baseOffset, leo - 2);
    EXPECT_EQ(r.lastOffset, leo - 1);
    // A batch partially overlapping lastSequence stays OutOfOrderSequence.
    auto partial = makeBatch(3, kPid, 0, 2 * kBatches - 1);
    EXPECT_EQ(callHelper(partial, log_, psm_, 1, 0, kPid, 0, 2 * kBatches - 1).error,
              pc::ErrorCode::OutOfOrderSequence);
}

// Review I2: a batch whose sequences would run past INT32_MAX (signed overflow
// in baseSequence + recordCount - 1) is refused OutOfOrderSequence, never
// appended.  (Rune's producer bumps the epoch before overflow; a wrap BETWEEN
// batches — seq 0 after INT32_MAX — is accepted, see the PSM regression.)
TEST_F(ProducePathTest, IdempotenceWindowRegression_BatchCrossingInt32MaxRefused)
{
    constexpr int32_t kMax = std::numeric_limits<int32_t>::max();
    constexpr int64_t kPid = 78;
    psm_.update(1, kPid, 0, 0, 0, kMax - 2, 0);              // lastSequence = MAX - 2
    auto b = makeBatch(3, kPid, 0, kMax - 1);                 // MAX-1, MAX, (overflow)
    auto o = callHelper(b, log_, psm_, 1, 0, kPid, 0, kMax - 1);
    EXPECT_EQ(o.error, pc::ErrorCode::OutOfOrderSequence);
    EXPECT_EQ(log_.logEndOffset(), 0);
}

} // namespace
} // namespace rune::storage
