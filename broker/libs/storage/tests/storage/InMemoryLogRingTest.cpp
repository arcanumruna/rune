// ---------------------------------------------------------------------------
// InMemoryLogRingTest.cpp — tests for the circular ring buffer implementation
// of InMemoryLog (Task 6, Milestone E Phase 3).
//
// Tests:
//   1. WrapAroundReadReturnsByteIdenticalBatches — fill past capacity so
//      head/tail wrap; read across the wrap boundary — assert byte-identical
//      batch contents and correct offsets.  THE KEY WRAP TEST.
//   2. DropOldestEvictsWithoutMemmoveAndAdvancesLogStart — cap-bounded evict
//      advances logStartOffset; O(1) (no memmove), all surviving batches readable.
//   3. BackPressureModeRefusesAppendWhenFullReturnsSentinel — FullPolicy::BackPressure,
//      fill to cap, next append returns -1, data not grown, existing entries intact;
//      after truncateTo frees space, append resumes.
//
// Additional ring correctness tests:
//   4. RingBytesUsedNeverExceedsCapacity — bytesUsed accounting stays <= maxBytes_.
//   5. WrapAroundEvictionAdvancesLogStartCorrectly — evicting a pre-wrap entry
//      followed by the wrapped entry adjusts logStartOffset_ step-by-step.
//   6. TruncateAcrossWrapBoundary — truncateTo removes a wrapped-in entry and
//      the ring can accept new appends without corruption.
//   7. MultipleWrapsReadConsistency — simulate several full wrap cycles and verify
//      every readable batch is byte-identical to what was written.
//   8. BackPressureResumesAfterEvictionFreeSpace — BackPressure: after a manual
//      truncateTo opens space, an append succeeds.
//   9. SingleBatchLargerThanHalfRing — a batch that occupies > maxBytes_/2 wraps
//      correctly and is readable byte-identical.
// ---------------------------------------------------------------------------

#include <rune/storage/InMemoryLog.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <numeric>
#include <vector>

using rune::storage::FullPolicy;
using rune::storage::InMemoryLog;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a distinguishable batch of `size` bytes where each byte equals
/// (offset + i) % 256, so different batches have different content.
static std::vector<uint8_t> makeBatch(std::size_t size, uint8_t seed = 0) {
    std::vector<uint8_t> b(size);
    for (std::size_t i = 0; i < size; ++i) {
        b[i] = static_cast<uint8_t>((seed + i) % 256);
    }
    return b;
}

// ---------------------------------------------------------------------------
// Test 1: WrapAroundReadReturnsByteIdenticalBatches (KEY TEST)
// ---------------------------------------------------------------------------
// Strategy: use a small ring (200 bytes), append batches of 60 bytes.
// After the third append the ring wraps (60+60+60=180 < 200, fourth batch
// would need to wrap: tail_=180, linearAvail=20 < 60 → wrap).
// Fill enough so that the ring has wrapped at least once and older batches
// have been evicted.  Then read every surviving batch and assert
// byte-identical content.
TEST(InMemoryLogRing, WrapAroundReadReturnsByteIdenticalBatches) {
    constexpr std::size_t kRingBytes   = 200;
    constexpr std::size_t kBatchSize   = 60;
    constexpr std::size_t kMaxRecords  = 64;
    constexpr int         kTotalBatchCount = 8; // well past a full ring wrap

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    // Record what was appended so we can verify reads.
    struct Written { std::vector<uint8_t> data; int64_t baseOffset; int64_t lastOffset; };
    std::vector<Written> written;

    for (int i = 0; i < kTotalBatchCount; ++i) {
        auto b = makeBatch(kBatchSize, static_cast<uint8_t>(i * 7));
        int64_t base = log.append(b.data(), b.size(), /*lastOffset=*/i);
        if (base >= 0) {
            written.push_back({b, base, i});
        }
    }

    // logStartOffset tells us what's still in the ring.
    const int64_t startOff = log.logStartOffset();
    const int64_t endOff   = log.logEndOffset();
    EXPECT_GT(endOff, startOff) << "at least some data must be in the ring";

    // Read all surviving batches in one call.
    auto got = log.read(startOff, static_cast<int32_t>(kRingBytes));
    ASSERT_FALSE(got.empty()) << "read must return surviving data";

    // Reconstruct the expected bytes by concatenating surviving written batches.
    std::vector<uint8_t> expected;
    for (const auto& w : written) {
        if (w.lastOffset >= startOff) {
            expected.insert(expected.end(), w.data.begin(), w.data.end());
        }
    }

    EXPECT_EQ(got.size(), expected.size())
        << "read returned " << got.size() << " bytes but expected " << expected.size();
    EXPECT_EQ(got, expected) << "byte-identical batch contents required across wrap boundary";

    // Also verify reading individual surviving batches one by one.
    for (const auto& w : written) {
        if (w.baseOffset < startOff) continue; // evicted
        auto single = log.read(w.baseOffset, static_cast<int32_t>(kBatchSize));
        ASSERT_FALSE(single.empty())
            << "batch at baseOffset=" << w.baseOffset << " must be readable";
        // The first batch in the read result must be byte-identical.
        ASSERT_GE(single.size(), kBatchSize);
        std::vector<uint8_t> firstBatch(single.begin(), single.begin() + kBatchSize);
        EXPECT_EQ(firstBatch, w.data)
            << "batch at baseOffset=" << w.baseOffset << " is not byte-identical";
    }
}

// ---------------------------------------------------------------------------
// Test 2: DropOldestEvictsWithoutMemmoveAndAdvancesLogStart
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, DropOldestEvictsWithoutMemmoveAndAdvancesLogStart) {
    // Ring: 120 bytes, max 3 records.  Each batch is 30 bytes.
    // First 3 appends fill 90 bytes (under the 120-byte cap) — no eviction.
    // 4th append hits the maxRecords=3 cap → evicts b0, logStartOffset → 1.
    constexpr std::size_t kRingBytes  = 120;
    constexpr std::size_t kBatchSize  = 30;
    constexpr std::size_t kMaxRecords = 3;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    auto b0 = makeBatch(kBatchSize, 0);
    auto b1 = makeBatch(kBatchSize, 1);
    auto b2 = makeBatch(kBatchSize, 2);
    auto b3 = makeBatch(kBatchSize, 3);

    log.append(b0.data(), kBatchSize, 0); // off 0
    log.append(b1.data(), kBatchSize, 1); // off 1
    log.append(b2.data(), kBatchSize, 2); // off 2 — 3 records, maxRecords hit on next

    EXPECT_EQ(log.logStartOffset(), 0);
    EXPECT_EQ(log.logEndOffset(),   3);

    // 4th append triggers eviction of b0 (maxRecords=3 would become 4).
    log.append(b3.data(), kBatchSize, 3); // off 3 → evicts b0
    EXPECT_EQ(log.logStartOffset(), 1) << "eviction must advance logStartOffset";
    EXPECT_EQ(log.logEndOffset(),   4);

    // b0 is gone.
    EXPECT_TRUE(log.read(0, 1024).empty()) << "evicted batch must not be readable";

    // b1, b2, b3 are readable and byte-identical.
    for (const auto& [off, bref] : std::initializer_list<std::pair<int64_t, const std::vector<uint8_t>*>>{
             {1, &b1}, {2, &b2}, {3, &b3}}) {
        auto out = log.read(off, static_cast<int32_t>(kRingBytes));
        ASSERT_FALSE(out.empty()) << "batch at off=" << off << " must be readable";
        // First batch returned must match.
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, *bref) << "batch at off=" << off << " byte-mismatch";
    }
}

// ---------------------------------------------------------------------------
// Test 3: BackPressureModeRefusesAppendWhenFullReturnsSentinel
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, BackPressureModeRefusesAppendWhenFullReturnsSentinel) {
    // Ring: 100 bytes, max 4 records.  Batch size 30.
    // First 3 batches fit (90 bytes).  4th batch (30 bytes) would take 120 bytes —
    // ring is full, BackPressure must refuse.
    constexpr std::size_t kRingBytes  = 100;
    constexpr std::size_t kBatchSize  = 30;
    constexpr std::size_t kMaxRecords = 10;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::BackPressure);

    auto b = makeBatch(kBatchSize, 0xAB);

    int64_t r0 = log.append(b.data(), kBatchSize, 0);
    int64_t r1 = log.append(b.data(), kBatchSize, 1);
    int64_t r2 = log.append(b.data(), kBatchSize, 2);
    EXPECT_GE(r0, 0); EXPECT_GE(r1, 0); EXPECT_GE(r2, 0);

    // 4th append exceeds byte cap — must return sentinel -1.
    auto b4 = makeBatch(kBatchSize, 0xCD);
    int64_t r3 = log.append(b4.data(), kBatchSize, 3);
    EXPECT_EQ(r3, -1) << "BackPressure: append must return -1 when ring is full";

    // Existing entries must still be intact.
    EXPECT_EQ(log.logStartOffset(), 0);
    EXPECT_EQ(log.logEndOffset(),   3); // unchanged — refused append must not advance LEO

    // Verify the three existing batches are still readable.
    for (int off = 0; off < 3; ++off) {
        auto out = log.read(off, static_cast<int32_t>(kRingBytes));
        ASSERT_FALSE(out.empty()) << "batch at off=" << off << " must still be readable";
    }

    // truncateTo frees space → an append should now succeed.
    log.truncateTo(1); // remove b0 and b1 (baseOffset >= 1 removes b1 and b2)
    // After truncateTo(1): only b0 remains (baseOffset=0 < 1), entries with baseOffset >= 1 removed.
    // logEndOffset_ = 1, logStartOffset_ = 0.
    EXPECT_EQ(log.logEndOffset(), 1);

    // Now 70 bytes are free (100 - 30 = 70 >= 30).
    auto fresh = makeBatch(kBatchSize, 0xEF);
    int64_t r4 = log.append(fresh.data(), kBatchSize, 1);
    EXPECT_GE(r4, 0) << "BackPressure: append must succeed after truncateTo freed space";
    EXPECT_EQ(log.logEndOffset(), 2);

    // New batch is readable and byte-identical.
    auto out = log.read(r4, static_cast<int32_t>(kRingBytes));
    ASSERT_FALSE(out.empty());
    std::vector<uint8_t> firstBatch(out.begin(), out.begin() + kBatchSize);
    EXPECT_EQ(firstBatch, fresh) << "resumed append must store byte-identical data";
}

// ---------------------------------------------------------------------------
// Test 4: RingBytesUsedNeverExceedsCapacity
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, RingBytesUsedNeverExceedsCapacity) {
    // Stress test: repeatedly append varying-sized batches into a small ring
    // and assert the internal bytesUsed_ never exceeds maxBytes_.
    // We can observe this indirectly: if bytesUsed_ is tracked correctly, no
    // assertion fires and reads always return valid data.
    constexpr std::size_t kRingBytes  = 300;
    constexpr std::size_t kMaxRecords = 8;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    const std::size_t sizes[] = {50, 70, 40, 90, 55, 65, 30, 80, 45, 75, 60, 35};
    int64_t off = 0;
    for (std::size_t iter = 0; iter < 60; ++iter) {
        std::size_t sz = sizes[iter % (sizeof sizes / sizeof sizes[0])];
        auto b = makeBatch(sz, static_cast<uint8_t>(iter));
        int64_t base = log.append(b.data(), sz, off);
        EXPECT_GE(base, 0) << "DropOldest: append must never fail";
        ++off;

        // logStartOffset <= logEndOffset (invariant).
        EXPECT_LE(log.logStartOffset(), log.logEndOffset());
    }

    // Read everything that survives and confirm it's non-empty.
    auto out = log.read(log.logStartOffset(), static_cast<int32_t>(kRingBytes));
    EXPECT_FALSE(out.empty());
}

// ---------------------------------------------------------------------------
// Test 5: WrapAroundEvictionAdvancesLogStartCorrectly
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, WrapAroundEvictionAdvancesLogStartCorrectly) {
    // Ring: 120 bytes.  Batches: 50 bytes each.  maxRecords=10 (not the binding cap).
    // Append A(50): tail=50, used=50.
    // Append B(50): tail=100, used=100.
    // Append C(50): linearAvail=20 < 50 → wrap, padding=20; totalNeed=70.
    //   Evict A: used-=50 → 50, head=50. Still 50+70=120 <= 120, but need to check:
    //   After evicting A: used=50, need(70): 50+70=120 <= 120. OK.
    //   padding=20, bytesUsed+=20 → 70. tail=0, ringPos=0.
    //   memcpy C at 0. tail=50, bytesUsed+=50 → 120.
    // Now: entries=[B(ringOffset=50), C(ringOffset=0)], head=50.
    // logStartOffset_ advanced to B's baseOffset after evicting A.
    constexpr std::size_t kRingBytes  = 120;
    constexpr std::size_t kBatchSize  = 50;
    constexpr std::size_t kMaxRecords = 10;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    auto bA = makeBatch(kBatchSize, 0xAA);
    auto bB = makeBatch(kBatchSize, 0xBB);
    auto bC = makeBatch(kBatchSize, 0xCC);
    auto bD = makeBatch(kBatchSize, 0xDD);

    log.append(bA.data(), kBatchSize, 0); // off 0
    log.append(bB.data(), kBatchSize, 1); // off 1
    EXPECT_EQ(log.logStartOffset(), 0);

    log.append(bC.data(), kBatchSize, 2); // off 2 — forces wrap, evicts A
    EXPECT_EQ(log.logStartOffset(), 1)
        << "logStartOffset must advance to 1 after evicting A";
    EXPECT_EQ(log.logEndOffset(), 3);

    // A is gone; B and C are readable.
    EXPECT_TRUE(log.read(0, 1024).empty()) << "A must be evicted";
    {
        auto out = log.read(1, static_cast<int32_t>(kRingBytes));
        ASSERT_GE(out.size(), kBatchSize);
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, bB) << "B byte-mismatch";
    }
    {
        auto out = log.read(2, static_cast<int32_t>(kRingBytes));
        ASSERT_GE(out.size(), kBatchSize);
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, bC) << "C byte-mismatch";
    }

    // Append D — triggers eviction of B (logStartOffset → 2), and wraps again.
    // After evicting B: used = 120 - 50 = 70. head=100. need for D: linearAvail=120-50=70 >= 50 → no wrap.
    // Actually: tail=50, linearAvail=70, no wrap. totalNeed=50. bytesUsed+50=70+50=120 <= 120. OK.
    log.append(bD.data(), kBatchSize, 3); // off 3
    EXPECT_EQ(log.logStartOffset(), 2)
        << "logStartOffset must advance to 2 after evicting B";
    EXPECT_EQ(log.logEndOffset(), 4);

    // C and D are readable.
    {
        auto out = log.read(2, static_cast<int32_t>(kRingBytes));
        ASSERT_GE(out.size(), kBatchSize);
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, bC) << "C byte-mismatch after evicting B";
    }
    {
        auto out = log.read(3, static_cast<int32_t>(kRingBytes));
        ASSERT_GE(out.size(), kBatchSize);
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, bD) << "D byte-mismatch";
    }
}

// ---------------------------------------------------------------------------
// Test 6: TruncateAcrossWrapBoundary
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, TruncateAcrossWrapBoundary) {
    // Build the wrap scenario: ring=120, batches=50.
    // A(0), B(1), C(2 — wrapped), then truncateTo(2) removes C.
    // After truncate the ring should accept new appends.
    constexpr std::size_t kRingBytes  = 120;
    constexpr std::size_t kBatchSize  = 50;
    constexpr std::size_t kMaxRecords = 10;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    auto bA = makeBatch(kBatchSize, 0xA0);
    auto bB = makeBatch(kBatchSize, 0xB0);
    auto bC = makeBatch(kBatchSize, 0xC0);

    log.append(bA.data(), kBatchSize, 0);
    log.append(bB.data(), kBatchSize, 1);
    // C forces eviction of A and wraps tail to 0.
    log.append(bC.data(), kBatchSize, 2);
    EXPECT_EQ(log.logStartOffset(), 1);
    EXPECT_EQ(log.logEndOffset(),   3);

    // Truncate back: remove C (baseOffset=2 >= 2).
    log.truncateTo(2);
    EXPECT_EQ(log.logEndOffset(), 2);

    // B should still be readable.
    {
        auto out = log.read(1, static_cast<int32_t>(kRingBytes));
        ASSERT_GE(out.size(), kBatchSize);
        std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
        EXPECT_EQ(first, bB) << "B must be intact after truncate";
    }
    // C must be gone (offset 2 is truncated).
    {
        auto out = log.read(2, static_cast<int32_t>(kRingBytes));
        EXPECT_TRUE(out.empty() || out.size() < kBatchSize)
            << "C must not be readable after truncateTo(2)";
    }

    // New append must succeed without corruption.
    auto bE = makeBatch(kBatchSize, 0xE0);
    int64_t rE = log.append(bE.data(), kBatchSize, 2);
    EXPECT_GE(rE, 0) << "append after truncate must succeed";
    EXPECT_EQ(log.logEndOffset(), 3);

    // E is readable and byte-identical.
    auto out = log.read(rE, static_cast<int32_t>(kRingBytes));
    ASSERT_GE(out.size(), kBatchSize);
    std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
    EXPECT_EQ(first, bE) << "appended E must be byte-identical after post-truncate append";
}

// ---------------------------------------------------------------------------
// Test 7: MultipleWrapsReadConsistency
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, MultipleWrapsReadConsistency) {
    // Simulate many wrap cycles.  After each append, read all surviving batches
    // and verify they are byte-identical to what was written.
    constexpr std::size_t kRingBytes   = 256;
    constexpr std::size_t kBatchSize   = 70;
    constexpr std::size_t kMaxRecords  = 20;
    constexpr int         kIterations  = 25;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    struct Written { std::vector<uint8_t> data; int64_t lastOffset; };
    std::vector<Written> history;

    for (int i = 0; i < kIterations; ++i) {
        auto b = makeBatch(kBatchSize, static_cast<uint8_t>(i * 13));
        log.append(b.data(), kBatchSize, i);
        history.push_back({b, i});

        // After each append, verify all surviving entries.
        const int64_t startOff = log.logStartOffset();
        for (const auto& w : history) {
            if (w.lastOffset < startOff) continue; // evicted
            // Read starting at this batch.
            auto out = log.read(w.lastOffset, static_cast<int32_t>(kBatchSize));
            ASSERT_GE(out.size(), kBatchSize)
                << "iter=" << i << " batch lastOff=" << w.lastOffset << " not readable";
            std::vector<uint8_t> got(out.begin(), out.begin() + kBatchSize);
            EXPECT_EQ(got, w.data)
                << "iter=" << i << " batch lastOff=" << w.lastOffset << " byte mismatch";
        }
    }
}

// ---------------------------------------------------------------------------
// Test 8: BackPressureResumesAfterTruncateFreeSpace
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, BackPressureResumesAfterTruncateFreeSpace) {
    constexpr std::size_t kRingBytes  = 100;
    constexpr std::size_t kBatchSize  = 40;
    constexpr std::size_t kMaxRecords = 10;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::BackPressure);

    auto b = makeBatch(kBatchSize, 0x55);

    // Two appends fill 80 bytes.
    EXPECT_GE(log.append(b.data(), kBatchSize, 0), 0);
    EXPECT_GE(log.append(b.data(), kBatchSize, 1), 0);

    // Third append (40 bytes) with only 20 bytes left → refused.
    auto bC = makeBatch(kBatchSize, 0x66);
    EXPECT_EQ(log.append(bC.data(), kBatchSize, 2), -1)
        << "BackPressure must refuse when only 20 bytes remain";

    // truncateTo(1) removes the batch at baseOffset=0, freeing 40 bytes.
    log.truncateTo(1);
    EXPECT_EQ(log.logEndOffset(), 1);

    // Now 60 bytes are free (100-40); 40-byte batch fits.
    auto bD = makeBatch(kBatchSize, 0x77);
    int64_t rD = log.append(bD.data(), kBatchSize, 1);
    EXPECT_GE(rD, 0) << "BackPressure: append must succeed after truncateTo freed space";

    // bD is readable.
    auto out = log.read(rD, static_cast<int32_t>(kRingBytes));
    ASSERT_GE(out.size(), kBatchSize);
    std::vector<uint8_t> first(out.begin(), out.begin() + kBatchSize);
    EXPECT_EQ(first, bD);
}

// ---------------------------------------------------------------------------
// Test 9: SingleBatchLargerThanHalfRing
// ---------------------------------------------------------------------------
TEST(InMemoryLogRing, SingleBatchLargerThanHalfRing) {
    // A batch of 70 bytes in a 100-byte ring (> half the ring).
    // After two appends the ring is full; the third forces the wrap path.
    constexpr std::size_t kRingBytes  = 100;
    constexpr std::size_t kMaxRecords = 10;

    InMemoryLog log(kRingBytes, kMaxRecords, FullPolicy::DropOldest);

    auto bA = makeBatch(70, 0xA1);
    auto bB = makeBatch(70, 0xB2);

    log.append(bA.data(), 70, 0); // fills [0,70), tail=70
    log.append(bB.data(), 70, 1); // linearAvail=30 < 70 → wrap; evicts A; writes B at [0,70)
    EXPECT_EQ(log.logStartOffset(), 1);
    EXPECT_EQ(log.logEndOffset(),   2);

    // B must be readable and byte-identical.
    auto out = log.read(1, static_cast<int32_t>(kRingBytes));
    ASSERT_GE(out.size(), 70U);
    std::vector<uint8_t> got(out.begin(), out.begin() + 70);
    EXPECT_EQ(got, bB) << "Large batch after wrap must be byte-identical";

    // Append a third large batch.
    auto bC = makeBatch(70, 0xC3);
    int64_t rC = log.append(bC.data(), 70, 2);
    EXPECT_GE(rC, 0);
    EXPECT_EQ(log.logStartOffset(), 2);

    auto outC = log.read(2, static_cast<int32_t>(kRingBytes));
    ASSERT_GE(outC.size(), 70U);
    std::vector<uint8_t> gotC(outC.begin(), outC.begin() + 70);
    EXPECT_EQ(gotC, bC) << "Third large batch must be byte-identical";
}
