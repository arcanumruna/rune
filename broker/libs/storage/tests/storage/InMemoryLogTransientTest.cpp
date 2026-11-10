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
#include <limits>
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

// E2 Task 12 (seeded stress, seed 41): an append the floor forces to refuse
// first EVICTED the committed batches below the floor, then refused anyway —
// committed records lost for an append that never happened (readers at those
// offsets were skipped forward).  A refusal must have no side effect: decide
// the fit first, evict only when the append will land.
TEST(InMemoryLogTransientRegression, RefusedAppendEvictsNothingRegression) {
    const auto x = batchOf(1);
    InMemoryLog log(4 * x.size(), 1024);   // room for exactly 4 batches
    log.setEvictionFloor(0);
    int64_t base = -1;
    for (int i = 0; i < 4; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    log.setEvictionFloor(1);                // only offset 0 is committed
    const auto big = batchOf(1, 2 * x.size());   // needs > 1 batch of room: refused
    EXPECT_EQ(log.appendNoSyncRestamp(big.data(), big.size(), 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0) << "a refused append evicted the committed batch at 0";
    EXPECT_EQ(log.evictedRecords(), 0u);
    EXPECT_EQ(log.logEndOffset(), 4);
    const auto v = log.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(rune::protocol::RecordBatchReader(v.data, v.len).baseOffset(), 0) << "the committed batch is still served";
    // ... and the same append fits once enough is committed: it evicts then.
    log.setEvictionFloor(4);
    EXPECT_EQ(log.appendNoSyncRestamp(big.data(), big.size(), 0, base), 5);
    EXPECT_GT(log.logStartOffset(), 0);
}

TEST(InMemoryLogTransientRegression, RefusedAppendByTheRecordCapEvictsNothingRegression) {
    const auto x = batchOf(1);
    InMemoryLog log(1 << 20, /*maxRecords=*/3);
    log.setEvictionFloor(0);
    int64_t base = -1;
    for (int i = 0; i < 3; ++i) ASSERT_GE(log.appendNoSyncRestamp(x.data(), x.size(), 0, base), 0);
    log.setEvictionFloor(1);
    // A 3-batch span needs 3 free slots; only 1 is evictable: refused, nothing evicted.
    std::vector<uint8_t> span;
    for (int64_t b = 0; b < 3; ++b) {
        rune::protocol::RecordBatchBuilder bb(b, 1000);
        bb.append("k", "v", 1000);
        const auto one = bb.build();
        span.insert(span.end(), one.begin(), one.end());
    }
    EXPECT_EQ(log.appendNoSync(span.data(), span.size(), log.logEndOffset() + 2), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0) << "a refused span append evicted a committed batch";
    EXPECT_EQ(log.logEndOffset(), 3);
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

// ---------------------------------------------------------------------------
// Lease topics (spec A.3 / A.4): the produce reserve and bytesFrom().
// ---------------------------------------------------------------------------
#include <random>

namespace {
// A ring of 5 batches of length L, reserve L.  State: used = 3L (+1 for `plusOne`).
struct ReserveRing {
    std::vector<uint8_t> x = batchOf(1);
    std::vector<uint8_t> y = batchOf(1, 101);
    std::size_t L = x.size();
    InMemoryLog log{5 * batchOf(1).size(), 1024};
    int64_t base = -1;
    // free = 2L (reserve + len) when !plusOne, 2L-1 when plusOne.
    explicit ReserveRing(bool plusOne, int64_t floor) {
        EXPECT_EQ(y.size(), L + 1);
        log.setEvictionFloor(floor);
        log.setProduceReserve(L);
        EXPECT_GE(log.appendNoSync(x.data(), x.size(), 0), 0);
        EXPECT_GE(log.appendNoSync(x.data(), x.size(), 1), 0);
        const auto& third = plusOne ? y : x;
        EXPECT_GE(log.appendNoSync(third.data(), third.size(), 2), 0);
    }
};
} // namespace

TEST(InMemoryLogReserveRegression, ReserveBoundariesRegression) {
    // free == reserve + len: lands (floor blocking everything or not).
    for (const int64_t floor : {int64_t{0}, std::numeric_limits<int64_t>::max()}) {
        ReserveRing r(false, floor);
        ASSERT_EQ(r.log.appendNoSyncRestamp(r.x.data(), r.x.size(), 0, r.base), 4) << "floor=" << floor;
        EXPECT_EQ(r.log.logStartOffset(), 0);
    }
    // free == reserve + len - 1, floor blocks: refused, nothing evicted, counters unchanged.
    {
        ReserveRing r(true, 0);
        EXPECT_EQ(r.log.appendNoSyncRestamp(r.x.data(), r.x.size(), 0, r.base), InMemoryLog::kRefusedFull);
        EXPECT_EQ(r.log.logStartOffset(), 0);
        EXPECT_EQ(r.log.logEndOffset(), 3);
        EXPECT_EQ(r.log.evictedRecords(), 0u);
        EXPECT_EQ(r.log.fullRefusals(), 1u);
    }
    // ... the same append, reserve not applied (plain append): lands without evicting.
    {
        ReserveRing r(true, 0);
        ASSERT_EQ(r.log.appendNoSync(r.x.data(), r.x.size(), 3), 3);
        EXPECT_EQ(r.log.logStartOffset(), 0);
        EXPECT_EQ(r.log.evictedRecords(), 0u);
    }
    // free == reserve + len - 1, floor lets one batch go: the reserve evicts it and lands.
    {
        ReserveRing r(true, 1);
        ASSERT_EQ(r.log.appendNoSyncRestamp(r.x.data(), r.x.size(), 0, r.base), 4);
        EXPECT_EQ(r.log.logStartOffset(), 1);
    }
    // ... and with NO floor the reserve evicts as needed.
    {
        ReserveRing r(true, std::numeric_limits<int64_t>::max());
        ASSERT_EQ(r.log.appendNoSyncRestamp(r.x.data(), r.x.size(), 0, r.base), 4);
        EXPECT_EQ(r.log.logStartOffset(), 1);
    }
    // Reserve 0 behaves exactly as before: the same state lands at once.
    {
        ReserveRing r(true, 0);
        r.log.setProduceReserve(0);
        EXPECT_EQ(r.log.produceReserve(), 0u);
        ASSERT_EQ(r.log.appendNoSyncRestamp(r.x.data(), r.x.size(), 0, r.base), 4);
        EXPECT_EQ(r.log.logStartOffset(), 0);
        EXPECT_EQ(r.log.fullRefusals(), 0u);
    }
}

TEST(InMemoryLogReserveRegression, WrapPaddingCountsRegression) {
    const auto x = batchOf(1);
    const std::size_t L = x.size();
    const auto z = batchOf(1, 100 + L / 2);
    const std::size_t Z = z.size();
    ASSERT_GT(Z, L);
    ASSERT_LE(Z, 2 * L);
    InMemoryLog log(5 * L, 1024);
    log.setEvictionFloor(0);
    for (int i = 0; i < 4; ++i) ASSERT_GE(log.appendNoSync(x.data(), x.size(), i), 0);   // tail = 4L, la = L < Z: wraps
    log.setProduceReserve(L);
    int64_t base = -1;
    log.setEvictionFloor(2);   // offsets 0 and 1 may go; 2 may not
    // After evicting 0 and 1: used 2L + (Z + L padding) + reserve L = 4L + Z > 5L.  Without the padding it would fit.
    EXPECT_EQ(log.appendNoSyncRestamp(z.data(), Z, 0, base), InMemoryLog::kRefusedFull);
    EXPECT_EQ(log.logStartOffset(), 0);
    EXPECT_EQ(log.evictedRecords(), 0u) << "a refusal evicted";
    log.setEvictionFloor(3);   // offset 2 may go too
    ASSERT_EQ(log.appendNoSyncRestamp(z.data(), Z, 0, base), 5);
    EXPECT_EQ(log.logStartOffset(), 3);
}

TEST(InMemoryLogBytesFromRegression, MatchesEvictionRegression) {
    InMemoryLog empty(1 << 12, 64);
    EXPECT_EQ(empty.bytesFrom(0), 0u);
    EXPECT_EQ(empty.bytesFrom(-5), 0u);

    constexpr std::size_t kCap = 6000;
    InMemoryLog log(kCap, 64);
    std::mt19937_64 rng(0xC0FFEE);
    struct E { int64_t base; int64_t last; std::size_t len; const uint8_t* ptr; };
    std::vector<E> all;
    const uint8_t* ringBase = nullptr;
    int checked = 0, wrapsSeen = 0;
    for (int step = 0; step < 600; ++step) {
        const auto b = batchOf(1 + static_cast<int>(rng() % 3), 20 + static_cast<std::size_t>(rng() % 700));
        int64_t base = -1;
        const int64_t next = log.appendNoSyncRestamp(b.data(), b.size(), deltaOf(b), base);
        ASSERT_GT(next, 0);
        const auto v = log.readView(base, static_cast<int32_t>(b.size()));
        ASSERT_FALSE(v.empty());
        if (all.empty()) ringBase = v.data;   // the first append lands at ring offset 0
        all.push_back({base, next - 1, b.size(), v.data});
        // The live entries, oldest first.
        std::vector<E> live;
        for (const auto& e : all) if (e.last >= log.logStartOffset()) live.push_back(e);
        ASSERT_FALSE(live.empty());
        // Oracle: bytes of the suffix = sum of lengths + the padding after each pre-wrap batch.
        std::size_t suffix = 0;
        std::vector<std::size_t> expect(live.size());
        for (std::size_t k = live.size(); k-- > 0;) {
            suffix += live[k].len;
            if (k + 1 < live.size() && live[k + 1].ptr < live[k].ptr) {
                suffix += kCap - static_cast<std::size_t>(live[k].ptr - ringBase) - live[k].len;
                ++wrapsSeen;
            }
            expect[k] = suffix;
        }
        for (std::size_t k = 0; k < live.size(); ++k) {
            ASSERT_EQ(log.bytesFrom(live[k].base), expect[k]) << "step " << step << " entry " << k;
            if (live[k].last > live[k].base) ASSERT_EQ(log.bytesFrom(live[k].base + 1), expect[k]) << "mid-batch offset";
            ++checked;
        }
        ASSERT_EQ(log.bytesFrom(log.logStartOffset()), expect[0]);
        ASSERT_EQ(log.bytesFrom(log.logStartOffset() - 1), expect[0]);
        ASSERT_EQ(log.bytesFrom(std::numeric_limits<int64_t>::min()), expect[0]);
        ASSERT_EQ(log.bytesFrom(log.logEndOffset()), 0u);
        ASSERT_EQ(log.bytesFrom(log.logEndOffset() + 1000), 0u);
    }
    EXPECT_GT(checked, 1000);
    EXPECT_GT(wrapsSeen, 100) << "the run must exercise wrap padding";
    EXPECT_GT(log.logStartOffset(), 0) << "the run must exercise eviction";
}
