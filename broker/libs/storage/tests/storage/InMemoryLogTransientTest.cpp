// InMemoryLogTransientTest.cpp — Phase 2b E2: the InMemoryLog surface a
// TRANSIENT topic's data path needs: the zero-copy restamp append (base offset
// + leader epoch patched in the ring copy), the eviction floor (never evict
// uncommitted data — refuse instead), the refusal codes, resetTo (follower
// catch-up below the leader's ring start), and handleProduceBatch over the ring.
#include <gtest/gtest.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <cstdint>
#include <string>
#include <vector>

using rune::storage::InMemoryLog;
namespace pc = rune::protocol::client;

namespace {
std::vector<uint8_t> batchOf(int records, std::size_t valueBytes = 100) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
int32_t deltaOf(const std::vector<uint8_t>& b) {
    return rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
}
} // namespace

TEST(InMemoryLogTransientRegression, RestampAppendPatchesBaseOffsetAndEpochInTheRingCopyOnly) {
    InMemoryLog log(1 << 20, 1024);
    log.setLeaderEpochStamp(3);
    const auto a = batchOf(2), b = batchOf(3);
    int64_t base = -1;
    ASSERT_EQ(log.appendNoSyncRestamp(a.data(), a.size(), deltaOf(a), base), 2);
    EXPECT_EQ(base, 0);
    const std::vector<uint8_t> bBefore = b;
    ASSERT_EQ(log.appendNoSyncRestamp(b.data(), b.size(), deltaOf(b), base), 5);
    EXPECT_EQ(base, 2);
    EXPECT_EQ(b, bBefore) << "the caller's batch is never mutated (it may alias a read-only fragment)";
    const auto v = log.readView(2, 1 << 20);
    ASSERT_FALSE(v.empty());
    rune::protocol::RecordBatchReader r(v.data, v.len);
    EXPECT_EQ(r.baseOffset(), 2);
    EXPECT_EQ(r.partitionLeaderEpoch(), 3) << "the leader stamps its epoch (Kafka parity; KIP-320 lineage)";
    EXPECT_TRUE(r.ensureValid()) << "base offset and leader epoch are outside the CRC";
}

TEST(InMemoryLogTransientRegression, EvictionFloorRefusesInsteadOfEvictingUncommittedData) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);   // room for exactly 4 batches
    log.setEvictionFloor(0);                // nothing committed yet
    int64_t base = -1;
    for (int i = 0; i < 4; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0) << "an uncommitted batch was evicted";
    EXPECT_EQ(log.logEndOffset(), 4);
    EXPECT_EQ(log.fullRefusals(), 1u);
    log.setEvictionFloor(2);                // offsets 0 and 1 are committed (HW = 2)
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 5);
    EXPECT_EQ(log.logStartOffset(), 1) << "exactly one committed batch made room";
    EXPECT_EQ(log.evictedRecords(), 1u);
}

TEST(InMemoryLogTransientRegression, RecordCapEvictionHonoursTheFloorToo) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 20, /*maxRecords=*/2);
    log.setEvictionFloor(0);
    int64_t base = -1;
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0);
}

TEST(InMemoryLogTransient, DefaultFloorKeepsDropOldestBehaviour) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);
    int64_t base = -1;
    for (int i = 0; i < 10; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    EXPECT_EQ(log.logEndOffset(), 10);
    EXPECT_EQ(log.logStartOffset(), 6) << "4 batches retained, oldest evicted (unbounded floor)";
    EXPECT_EQ(log.fullRefusals(), 0u);
    EXPECT_EQ(log.evictedRecords(), 6u);
}

TEST(InMemoryLogTransientRegression, BatchLargerThanTheRingIsRefusedTooLarge) {
    const auto big = batchOf(1, 4096);
    InMemoryLog log(1024, 16);
    int64_t base = -1;
    EXPECT_EQ(log.appendNoSyncRestamp(big.data(), big.size(), 0, base), InMemoryLog::kRefusedTooLarge);
    EXPECT_EQ(log.logEndOffset(), 0);
    EXPECT_EQ(log.appendNoSync(big.data(), big.size(), 0), InMemoryLog::kRefusedTooLarge)
        << "the plain append refuses too (was an assert)";
}

TEST(InMemoryLogTransient, ResetToRestartsTheRingEmptyAtAnOffset) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 16, 64);
    int64_t base = -1;
    for (int i = 0; i < 3; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    log.resetTo(42);
    EXPECT_EQ(log.logStartOffset(), 42);
    EXPECT_EQ(log.logEndOffset(), 42);
    EXPECT_TRUE(log.readView(42, 1 << 16).empty());
    ASSERT_EQ(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 43);
    EXPECT_EQ(base, 42);
    const auto v = log.readView(42, 1 << 16);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(rune::protocol::RecordBatchReader(v.data, v.len).baseOffset(), 42);
}

TEST(InMemoryLogTransientRegression, ProducePathOverTheRingAppendsAndRefusesFullAsRetriable) {
    const auto x = batchOf(1);
    InMemoryLog log(2 * x.size(), 1024);
    log.setEvictionFloor(0);
    rune::storage::ProducerStateManager psm;
    auto produce = [&](InMemoryLog& l) {
        return rune::storage::handleProduceBatch(x.data(), x.size(), /*topicId=*/1, /*partition=*/0,
                                                 /*producerId=*/-1, /*epoch=*/0, /*baseSeq=*/-1, l, psm,
                                                 /*acksAll=*/false, /*isrSize=*/0, /*minIsr=*/1, /*tier=*/1);
    };
    const auto o1 = produce(log);
    ASSERT_TRUE(o1.isOK());
    EXPECT_EQ(o1.baseOffset, 0);
    ASSERT_TRUE(produce(log).isOK());
    const auto full = produce(log);
    EXPECT_EQ(full.error, pc::ErrorCode::TransientLogFull) << "retriable (42, ruling 2): nothing was appended";
    EXPECT_TRUE(full.logFull);
    EXPECT_EQ(log.logEndOffset(), 2);

    InMemoryLog tiny(64, 4);
    const auto tooLarge = produce(tiny);
    EXPECT_EQ(tooLarge.error, pc::ErrorCode::MessageTooLarge) << "a batch larger than the ring can never fit";
    EXPECT_TRUE(tooLarge.logFull);
}

// E2 Task 2 review: a batch shorter than its base-offset field is refused, never
// patched past the copy; nothing is appended.
TEST(InMemoryLogTransientRegression, ShortBatchIsRefusedNotPatchedPastTheCopy) {
    InMemoryLog log(1 << 20, 1024);
    const uint8_t tiny[4] = {1, 2, 3, 4};
    int64_t base = -7;
    EXPECT_EQ(log.appendNoSyncRestamp(tiny, sizeof(tiny), 0, base), InMemoryLog::kRefusedTooLarge);
    EXPECT_EQ(base, -7) << "no base offset is reported for a refused batch";
    EXPECT_EQ(log.logEndOffset(), 0);
}

// ---------------------------------------------------------------------------
// E2 Task 11 (interaction matrix, follower ParityWithDisk cells): a follower
// applies a FETCHED span of several whole batches in ONE appendNoSync.  The ring
// must index it per batch, exactly like the disk Log — one Entry for the whole
// span made a read from a mid-span offset re-deliver the earlier batches, made
// eviction drop committed batches only together with their span-mates, and left
// truncateTo(n) with n inside the span holding the stale bytes past n.
// ---------------------------------------------------------------------------
namespace {
/// A batch as the leader's ring holds it: base offset stamped.
std::vector<uint8_t> batchAt(int64_t base, int records, std::size_t valueBytes = 100) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    const std::string v(valueBytes, 'v');
    for (int i = 0; i < records; ++i) b.append("k", v, 1000 + i);
    return b.build();
}
std::vector<uint8_t> concat(std::initializer_list<const std::vector<uint8_t>*> parts) {
    std::vector<uint8_t> out;
    for (const auto* p : parts) out.insert(out.end(), p->begin(), p->end());
    return out;
}
} // namespace

TEST(InMemoryLogMultiBatchSpanRegression, ReadFromAMidSpanOffsetStartsAtThatBatch) {
    InMemoryLog log(1 << 20, 1024);
    const auto b0 = batchAt(0, 2), b1 = batchAt(2, 2);
    const auto span = concat({&b0, &b1});
    ASSERT_EQ(log.appendNoSync(span.data(), span.size(), 3), 0);
    EXPECT_EQ(log.logEndOffset(), 4);

    const auto v = log.readView(3, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.baseOffset, 2) << "a read from offset 3 re-delivered the batch at 0 (disk Log: batch 2)";
    EXPECT_EQ(v.len, b1.size());
    EXPECT_EQ(log.read(2, 1 << 20), b1) << "the copying read too";
    const auto all = log.readView(0, 1 << 20);
    EXPECT_EQ(all.len, span.size()) << "both batches stay contiguous for a read from the span start";
    EXPECT_EQ(all.nextOffset, 4);
}

TEST(InMemoryLogMultiBatchSpanRegression, TruncateInsideASpanDropsTheBatchesPastIt) {
    InMemoryLog log(1 << 20, 1024);
    const auto b0 = batchAt(0, 2), b1 = batchAt(2, 2);
    const auto span = concat({&b0, &b1});
    ASSERT_EQ(log.appendNoSync(span.data(), span.size(), 3), 0);
    log.truncateTo(2);   // a follower's divergence truncation (KIP-279)
    EXPECT_EQ(log.logEndOffset(), 2);
    EXPECT_TRUE(log.readView(2, 1 << 20).empty()) << "the truncated batch is still readable";
    EXPECT_EQ(log.readView(0, 1 << 20).len, b0.size()) << "the ring still holds the stale bytes past offset 2";

    const auto nb = batchAt(2, 3, 50);   // the new leader's lineage at 2
    ASSERT_EQ(log.appendNoSync(nb.data(), nb.size(), 4), 2);
    EXPECT_EQ(log.read(2, 1 << 20), nb);
    EXPECT_EQ(log.read(0, 1 << 20), concat({&b0, &nb}));
}

TEST(InMemoryLogMultiBatchSpanRegression, EvictionDropsOneBatchOfASpanAtATime) {
    const auto b0 = batchAt(0, 2), b1 = batchAt(2, 2), b2 = batchAt(4, 2);
    const auto span = concat({&b0, &b1});
    InMemoryLog log(span.size() + b2.size() / 2, 1024);   // room for the span, not span + b2
    ASSERT_EQ(log.appendNoSync(span.data(), span.size(), 3), 0);
    ASSERT_EQ(log.appendNoSync(b2.data(), b2.size(), 5), 4);
    EXPECT_EQ(log.logStartOffset(), 2) << "the committed batch at 2 was evicted together with its span-mate";
    EXPECT_EQ(log.evictedRecords(), 2u);
    EXPECT_EQ(log.read(2, 1 << 20), concat({&b1, &b2}));
}

TEST(InMemoryLogMultiBatchSpanRegression, EvictionFloorProtectsOnlyTheBatchesAtOrAboveIt) {
    const auto b0 = batchAt(0, 2), b1 = batchAt(2, 2), b2 = batchAt(4, 2);
    const auto span = concat({&b0, &b1});
    InMemoryLog log(span.size() + b2.size() / 2, 1024);
    ASSERT_EQ(log.appendNoSync(span.data(), span.size(), 3), 0);
    log.setEvictionFloor(2);   // HW = 2: the batch at 0 is committed, the one at 2 is not
    EXPECT_EQ(log.appendNoSync(b2.data(), b2.size(), 5), 4) << "the committed batch at 0 was evictable";
    EXPECT_EQ(log.logStartOffset(), 2);
}

TEST(InMemoryLogMultiBatchSpanRegression, SpanOfMoreBatchesThanTheEntryCapIsRefusedTooLarge) {
    InMemoryLog log(1 << 20, 2);
    const auto b0 = batchAt(0, 1), b1 = batchAt(1, 1), b2 = batchAt(2, 1);
    const auto span = concat({&b0, &b1, &b2});
    EXPECT_EQ(log.appendNoSync(span.data(), span.size(), 2), InMemoryLog::kRefusedTooLarge);
    EXPECT_EQ(log.logEndOffset(), 0) << "nothing appended";
    const auto two = concat({&b0, &b1});
    ASSERT_EQ(log.appendNoSync(two.data(), two.size(), 1), 0);
    const auto b2b = batchAt(2, 1);
    ASSERT_EQ(log.appendNoSync(b2b.data(), b2b.size(), 2), 2) << "the entry cap evicts per batch";
    EXPECT_EQ(log.logStartOffset(), 1);
}
