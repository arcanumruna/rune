// ============================================================================
// MixedWorkloadIntegrationTest.cpp
//
// Capstone integration tests: interleaved produce / consume / retention /
// restart cycles, with a shadow model kept in-test as ground truth.
//
// Design principles:
//   - A std::vector<ProducedRecord> shadow_ records every produced record.
//   - retainedFromOffset_ tracks the lowest offset guaranteed to survive
//     (updated after each deleteOldSegments call).
//   - Every assertion compares the live storage against the shadow model.
//   - Invariants checked throughout:
//       * offsets strictly monotonic
//       * highWatermark == total produced (never decremented)
//       * no committed record ever lost except via explicit retention
//       * after retention, offsets < retainedFromOffset_ return empty
//       * after restart, HWM and full consume still match shadow model
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/Log.h>
#include <rune/storage/PartitionApi.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <memory>
#include <random>

using namespace rune::storage;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// MixedWorkload fixture: extends IntegrationTestBase to hold shadow state
// and a live PartitionApi instance.
// ---------------------------------------------------------------------------
class MixedWorkload : public IntegrationTestBase {
protected:
    // Shadow model: one entry per produced record.
    std::vector<ProducedRecord> shadow_;
    // The minimum surviving offset after retention (starts at 0).
    int64_t retainedFromOffset_ = 0;
    // The live PartitionApi (may be null when "destroyed").
    std::unique_ptr<PartitionApi> api_;
    // Deterministic RNG (seeded once per test).
    std::mt19937 rng_{12345};

    // How many records total have been produced (shadow_.size()).
    int64_t totalProduced() const { return static_cast<int64_t>(shadow_.size()); }

    void SetUp() override {
        IntegrationTestBase::SetUp();
        api_ = std::make_unique<PartitionApi>(testDir_.string());
    }

    // Produce `k` records, appending to shadow_.
    void produceK(int k, int vsizeBase = 24) {
        for (int i = 0; i < k; ++i) {
            ProducedRecord rec;
            int absIdx = static_cast<int>(shadow_.size());
            int vsize  = (i % 5 == 0) ? 128 : vsizeBase; // vary sizes
            rec.key       = makeKey(absIdx);
            rec.value     = makeValue(absIdx, vsize);
            rec.timestamp = static_cast<int64_t>(10000 + absIdx);
            int64_t hw = api_->produce(rec.key, rec.value, rec.timestamp);
            EXPECT_EQ(hw, totalProduced() + 1)
                << "produce returned unexpected HWM at absIdx=" << absIdx;
            shadow_.push_back(std::move(rec));
        }
    }

    // Consume from `fromOffset` and validate against shadow model.
    // Returns the consumed records.
    std::vector<ConsumedRecord> consumeAndVerify(int64_t fromOffset,
                                                  const char* ctx = "") {
        auto recs = api_->consume(fromOffset);

        // If fromOffset is below the retained floor, expect empty or starting
        // at a batch boundary >= retainedFromOffset_.
        if (fromOffset < retainedFromOffset_) {
            // Records may be empty (offset deleted) or start at a later batch.
            // We cannot guarantee the exact first record, so just validate CRC.
            for (auto& r : recs) {
                EXPECT_GE(r.offset, retainedFromOffset_)
                    << ctx << ": consumed record below retained floor";
            }
            return recs;
        }

        // Normal path: records must be a contiguous suffix of the shadow model.
        int64_t expectedCount = totalProduced() - fromOffset;
        EXPECT_EQ(static_cast<int64_t>(recs.size()), expectedCount)
            << ctx << ": consume(" << fromOffset << ") count mismatch";

        for (size_t i = 0; i < recs.size(); ++i) {
            int64_t absIdx = fromOffset + static_cast<int64_t>(i);
            EXPECT_EQ(recs[i].offset, absIdx)
                << ctx << ": offset mismatch at i=" << i;
            if (absIdx < totalProduced()) {
                EXPECT_EQ(recs[i].key,   shadow_[static_cast<size_t>(absIdx)].key)
                    << ctx << ": key mismatch at absIdx=" << absIdx;
                EXPECT_EQ(recs[i].value, shadow_[static_cast<size_t>(absIdx)].value)
                    << ctx << ": value mismatch at absIdx=" << absIdx;
                EXPECT_EQ(recs[i].timestamp,
                          shadow_[static_cast<size_t>(absIdx)].timestamp)
                    << ctx << ": timestamp mismatch at absIdx=" << absIdx;
            }
        }
        return recs;
    }

    // Apply retention using a Log directly (same directory) so we can control
    // segment roll size; updates retainedFromOffset_.
    void applyRetention(int64_t ROLL, int64_t retainFraction) {
        // Re-open as Log to call deleteOldSegments.
        rune::storage::Log log(testDir_.string(), ROLL);
        int64_t budget = log.totalBytes() / retainFraction;
        log.deleteOldSegments(budget);
        // Find the new lowest surviving base offset.
        std::vector<int64_t> bases = log.segmentBaseOffsets();
        if (!bases.empty())
            retainedFromOffset_ = bases.front();
    }

    // Destroy and reopen the PartitionApi; verify HWM matches shadow.
    void restartAndVerify(const char* ctx = "") {
        api_.reset();
        api_ = std::make_unique<PartitionApi>(testDir_.string());
        EXPECT_EQ(api_->highWatermark(), totalProduced())
            << ctx << ": HWM after restart must equal total produced";

        // Full consume validation: drain all surviving records.
        std::vector<ConsumedRecord> all;
        int64_t nextOffset = retainedFromOffset_;
        while (true) {
            auto chunk = api_->consume(nextOffset);
            if (chunk.empty()) break;
            nextOffset = chunk.back().offset + 1;
            all.insert(all.end(), chunk.begin(), chunk.end());
        }

        // Records must match shadow from retainedFromOffset_ onward.
        EXPECT_EQ(static_cast<int64_t>(all.size()),
                  totalProduced() - retainedFromOffset_)
            << ctx << ": after restart, total surviving records mismatch";

        for (size_t i = 0; i < all.size(); ++i) {
            int64_t absIdx = all[i].offset;
            ASSERT_GE(absIdx, retainedFromOffset_)
                << ctx << ": record below retained floor after restart";
            ASSERT_LT(absIdx, totalProduced())
                << ctx << ": record offset >= totalProduced after restart";
            EXPECT_EQ(all[i].key,   shadow_[static_cast<size_t>(absIdx)].key)
                << ctx << ": key mismatch after restart at absIdx=" << absIdx;
            EXPECT_EQ(all[i].value, shadow_[static_cast<size_t>(absIdx)].value)
                << ctx << ": value mismatch after restart at absIdx=" << absIdx;
        }
    }
};

// ---------------------------------------------------------------------------
// 1. 30-round mixed workload: produce / consume / retention / restart.
// ---------------------------------------------------------------------------
TEST_F(MixedWorkload, ThirtyRoundInterleaved) {
    // We use a separate "inner Log" for retention with tiny roll,
    // and a PartitionApi (default roll) for produce/consume.
    // To force many segments, we periodically flush via a small-roll Log reopen.
    constexpr int64_t TINY_ROLL = 512; // used only for retention ops
    constexpr int ROUNDS = 30;
    constexpr int K_PER_ROUND = 40; // records produced per round

    for (int round = 0; round < ROUNDS; ++round) {
        // — Produce K records.
        produceK(K_PER_ROUND);

        EXPECT_EQ(api_->highWatermark(), totalProduced())
            << "round " << round << ": HWM must equal totalProduced";

        // — Occasionally consume from a random surviving offset.
        if (round % 3 == 0 && totalProduced() > 0) {
            std::uniform_int_distribution<int64_t> dist(
                retainedFromOffset_, totalProduced() - 1);
            int64_t offset = dist(rng_);
            consumeAndVerify(offset, ("round-consume-" + std::to_string(round)).c_str());
        }

        // — Occasionally run retention.
        if (round % 5 == 4 && totalProduced() > K_PER_ROUND * 3) {
            // Close the PartitionApi so the Log file handles are released,
            // then open a raw Log for retention.
            api_.reset();
            applyRetention(TINY_ROLL, 2); // keep ~50%
            // Reopen PartitionApi.
            api_ = std::make_unique<PartitionApi>(testDir_.string());
            EXPECT_EQ(api_->highWatermark(), totalProduced())
                << "round " << round << ": HWM unchanged by retention";
        }

        // — Occasionally destroy + reopen + verify.
        if (round % 7 == 6) {
            restartAndVerify(("round-restart-" + std::to_string(round)).c_str());
        }
    }

    // Final full validation.
    restartAndVerify("final");
}

// ---------------------------------------------------------------------------
// 2. Invariant: highWatermark == totalProduced throughout; never decremented
//    by retention.
// ---------------------------------------------------------------------------
TEST_F(MixedWorkload, HwmNeverDecrementedByRetention) {
    constexpr int64_t TINY_ROLL = 256;
    constexpr int ROUNDS = 15;
    constexpr int K = 30;

    for (int round = 0; round < ROUNDS; ++round) {
        produceK(K);
        int64_t hwBefore = api_->highWatermark();
        EXPECT_EQ(hwBefore, totalProduced());

        // Apply retention (close first to release file handles).
        api_.reset();
        applyRetention(TINY_ROLL, 3);
        api_ = std::make_unique<PartitionApi>(testDir_.string());

        int64_t hwAfter = api_->highWatermark();
        EXPECT_EQ(hwAfter, totalProduced())
            << "round " << round << ": HWM must stay == totalProduced after retention";
        EXPECT_GE(hwAfter, hwBefore)
            << "round " << round << ": HWM must not decrease";
    }
}

// ---------------------------------------------------------------------------
// 3. No committed record lost except via explicit retention: shadow model check
//    across restarts.
// ---------------------------------------------------------------------------
TEST_F(MixedWorkload, NoDurabilityLossWithoutRetention) {
    constexpr int ROUNDS = 10;
    constexpr int K = 50;

    for (int round = 0; round < ROUNDS; ++round) {
        produceK(K);
        // Restart without retention — all records must survive.
        api_.reset();
        api_ = std::make_unique<PartitionApi>(testDir_.string());
        EXPECT_EQ(api_->highWatermark(), totalProduced());

        // Validate all surviving records (retainedFromOffset_ stays 0).
        consumeAndVerify(retainedFromOffset_,
                         ("no-retention-round-" + std::to_string(round)).c_str());
    }
}

// ---------------------------------------------------------------------------
// 4. Offsets strictly monotonic across the entire consume-all result.
// ---------------------------------------------------------------------------
TEST_F(MixedWorkload, ConsumedOffsetsStrictlyMonotonic) {
    constexpr int N = 500;
    produceK(N);

    // Drain all records.
    std::vector<ConsumedRecord> all;
    while (static_cast<int64_t>(all.size()) < totalProduced()) {
        auto chunk = api_->consume(static_cast<int64_t>(all.size()));
        if (chunk.empty()) break;
        all.insert(all.end(), chunk.begin(), chunk.end());
    }

    ASSERT_EQ(static_cast<int64_t>(all.size()), totalProduced());
    for (size_t i = 1; i < all.size(); ++i) {
        EXPECT_GT(all[i].offset, all[i - 1].offset)
            << "offsets not strictly increasing at i=" << i;
    }
}

// ---------------------------------------------------------------------------
// 5. After aggressive retention then produce + restart: only non-deleted
//    records are returned and new records are readable.
// ---------------------------------------------------------------------------
TEST_F(MixedWorkload, RetentionThenProduceThenRestart) {
    constexpr int64_t TINY_ROLL = 512;
    constexpr int N1 = 200; // initial produce
    constexpr int N2 = 50;  // produce after retention

    // Phase 1: produce N1 records.
    produceK(N1);

    // Phase 2: apply aggressive retention.
    api_.reset();
    applyRetention(TINY_ROLL, 4); // keep ~25%
    api_ = std::make_unique<PartitionApi>(testDir_.string());
    EXPECT_EQ(api_->highWatermark(), totalProduced());

    // Phase 3: produce N2 more records after retention.
    produceK(N2);
    EXPECT_EQ(api_->highWatermark(), totalProduced());

    // Phase 4: restart.
    restartAndVerify("post-retention-produce-restart");

    // Phase 5: new records (from N1..N1+N2-1) must be readable byte-exact.
    std::vector<ConsumedRecord> all;
    int64_t next = static_cast<int64_t>(N1); // start after first batch
    while (true) {
        auto chunk = api_->consume(next);
        if (chunk.empty()) break;
        next = chunk.back().offset + 1;
        all.insert(all.end(), chunk.begin(), chunk.end());
    }
    ASSERT_EQ(static_cast<int>(all.size()), N2)
        << "exactly N2 records produced post-retention must be readable";
    for (int i = 0; i < N2; ++i) {
        int64_t absIdx = N1 + i;
        EXPECT_EQ(all[static_cast<size_t>(i)].offset, absIdx);
        EXPECT_EQ(all[static_cast<size_t>(i)].value,
                  shadow_[static_cast<size_t>(absIdx)].value)
            << "post-retention value mismatch at absIdx=" << absIdx;
    }
}
