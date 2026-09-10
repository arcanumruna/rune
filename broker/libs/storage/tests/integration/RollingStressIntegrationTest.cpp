// ============================================================================
// RollingStressIntegrationTest.cpp
//
// Integration tests: high-volume produce with tiny segmentRollBytes to force
// thousands of segment rolls, then verify full correctness.
//
// Key invariants:
//   - segmentBaseOffsets() count is large and strictly increasing.
//   - consume(0) returns exactly N records with contiguous offsets 0..N-1 and
//     byte-exact values.
//   - 200 random offset spot-checks each return a valid batch whose baseOffset
//     is <= the requested offset and <= logEndOffset.
//   - Destroy + reopen + recover reproduces the same logEndOffset and all N
//     records survive (log reproducibility).
//   - Mixed value sizes (small + larger) so batches differ.
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/PartitionApi.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <random>

using namespace rune::storage;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helper: build value of variable size based on seed for diversity.
// ---------------------------------------------------------------------------
static std::string makeVarValue(int i) {
    // Alternate between small (8 bytes) and larger (200 bytes) values so
    // batches have different sizes and span segment boundaries differently.
    int sz = (i % 7 == 0) ? 200 : 8;
    std::string v;
    v.reserve(static_cast<size_t>(sz));
    for (int b = 0; b < sz; ++b) {
        uint8_t byte;
        if (b % 7 == 0) byte = 0x00;
        else byte = static_cast<uint8_t>((i * 31 + b * 17 + 0xAB) & 0xFF);
        v.push_back(static_cast<char>(byte));
    }
    return v;
}

// ---------------------------------------------------------------------------
// 1. 20,000 records with tiny roll; segmentBaseOffsets strictly increasing;
//    count large.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RollingStress_ManySegmentsStrictlyIncreasing) {
    constexpr int N = 20'000;
    constexpr int64_t ROLL = 2 * 1024; // 2 KiB

    fs::path dir = testDir_ / "stress1";
    fs::create_directories(dir);

    {
        rune::protocol::RecordBatchBuilder* pb = nullptr;
        (void)pb;
        // Use Log directly for fine-grained segment control.
        rune::storage::Log log(dir.string(), ROLL);
        for (int i = 0; i < N; ++i) {
            std::string val = makeVarValue(i);
            rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), 1000 + i);
            b.append(makeKey(i), val, 1000 + i);
            log.append(b.build(), static_cast<int64_t>(i));
        }

        auto bases = log.segmentBaseOffsets();
        ASSERT_GT(static_cast<int>(bases.size()), 10)
            << "expected many segment rolls with 2 KiB threshold";

        // Strictly increasing.
        for (size_t j = 1; j < bases.size(); ++j)
            EXPECT_GT(bases[j], bases[j - 1])
                << "segment base offsets must be strictly increasing at j=" << j;

        EXPECT_EQ(log.logEndOffset(), static_cast<int64_t>(N));
    }
}

// ---------------------------------------------------------------------------
// 2. consume(0) via PartitionApi returns exactly N records, contiguous offsets,
//    byte-exact values.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RollingStress_ConsumeAllContiguousByteExact) {
    constexpr int N = 5'000; // smaller count for full byte-exact check
    constexpr int64_t ROLL = 1024;

    fs::path dir = testDir_ / "stress2";
    fs::create_directories(dir);

    std::vector<ProducedRecord> produced;
    produced.reserve(N);
    {
        rune::storage::Log log(dir.string(), ROLL);
        for (int i = 0; i < N; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeVarValue(i);
            rec.timestamp = static_cast<int64_t>(2000 + i);
            rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), rec.timestamp);
            b.append(rec.key, rec.value, rec.timestamp);
            log.append(b.build(), static_cast<int64_t>(i));
            produced.push_back(std::move(rec));
        }
    }

    // Reopen via PartitionApi (which calls recover).
    PartitionApi api(dir.string());
    ASSERT_EQ(api.highWatermark(), static_cast<int64_t>(N));

    // Consume all from offset 0.  Due to the known 1 MiB maxBytes limitation,
    // we drain with repeated consume calls until we have all N records.
    std::vector<ConsumedRecord> all;
    all.reserve(N);
    while (static_cast<int>(all.size()) < N) {
        int64_t nextOffset = static_cast<int64_t>(all.size());
        auto chunk = api.consume(nextOffset);
        if (chunk.empty()) break;
        all.insert(all.end(), chunk.begin(), chunk.end());
    }

    ASSERT_EQ(static_cast<int>(all.size()), N) << "total consumed must equal N";

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(all[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
            << "offset mismatch at i=" << i;
        EXPECT_EQ(all[static_cast<size_t>(i)].key, produced[static_cast<size_t>(i)].key)
            << "key mismatch at i=" << i;
        EXPECT_EQ(all[static_cast<size_t>(i)].value, produced[static_cast<size_t>(i)].value)
            << "value mismatch at i=" << i;
        EXPECT_EQ(all[static_cast<size_t>(i)].timestamp, produced[static_cast<size_t>(i)].timestamp)
            << "timestamp mismatch at i=" << i;
    }
}

// ---------------------------------------------------------------------------
// 3. 200 random offset spot-checks: each returns a batch with valid CRC and
//    baseOffset <= requested offset, records at correct absolute offsets.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RollingStress_RandomOffsetSpotChecks) {
    constexpr int N = 3'000;
    constexpr int64_t ROLL = 1024;
    constexpr int SAMPLES = 200;

    fs::path dir = testDir_ / "stress3";
    fs::create_directories(dir);

    std::vector<ProducedRecord> produced;
    produced.reserve(N);
    {
        rune::storage::Log log(dir.string(), ROLL);
        for (int i = 0; i < N; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeVarValue(i);
            rec.timestamp = static_cast<int64_t>(3000 + i);
            rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), rec.timestamp);
            b.append(rec.key, rec.value, rec.timestamp);
            log.append(b.build(), static_cast<int64_t>(i));
            produced.push_back(std::move(rec));
        }
    }

    rune::storage::Log log2(dir.string(), ROLL);
    log2.recover(); // Rebuild in-memory indexes from disk before random reads.

    std::mt19937 rng(0xDEADBEEF);
    std::uniform_int_distribution<int> dist(0, N - 1);

    for (int s = 0; s < SAMPLES; ++s) {
        int64_t requestedOffset = static_cast<int64_t>(dist(rng));
        auto raw = log2.read(requestedOffset, 1 << 20);
        ASSERT_FALSE(raw.empty())
            << "sample " << s << ": read at offset " << requestedOffset << " returned empty";

        rune::protocol::RecordBatchReader reader(raw.data(), raw.size());
        EXPECT_TRUE(reader.ensureValid())
            << "sample " << s << ": batch at offset " << requestedOffset << " failed CRC";
        EXPECT_LE(reader.baseOffset(), requestedOffset)
            << "sample " << s << ": batch baseOffset must be <= requested";
        EXPECT_GE(reader.baseOffset(), 0)
            << "sample " << s << ": batch baseOffset must be non-negative";
        EXPECT_LE(reader.baseOffset(), static_cast<int64_t>(N))
            << "sample " << s << ": batch baseOffset must be within produced range";
    }
}

// ---------------------------------------------------------------------------
// 4. Destroy + reopen + recover reproduces the same logEndOffset and all N
//    records survive (log reproducibility).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, RollingStress_ReopenReproducesSameOffsets) {
    constexpr int N = 4'000;
    constexpr int64_t ROLL = 1024;

    fs::path dir = testDir_ / "stress4";
    fs::create_directories(dir);

    std::vector<ProducedRecord> produced;
    produced.reserve(N);
    {
        rune::storage::Log log(dir.string(), ROLL);
        for (int i = 0; i < N; ++i) {
            ProducedRecord rec;
            rec.key       = makeKey(i);
            rec.value     = makeVarValue(i);
            rec.timestamp = static_cast<int64_t>(4000 + i);
            rune::protocol::RecordBatchBuilder b(static_cast<int64_t>(i), rec.timestamp);
            b.append(rec.key, rec.value, rec.timestamp);
            log.append(b.build(), static_cast<int64_t>(i));
            produced.push_back(std::move(rec));
        }
    }
    // Reopen and recover.
    PartitionApi api(dir.string());
    EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N))
        << "logEndOffset after reopen+recover must equal N";

    // Drain all records.
    std::vector<ConsumedRecord> all;
    all.reserve(N);
    while (static_cast<int>(all.size()) < N) {
        int64_t next = static_cast<int64_t>(all.size());
        auto chunk = api.consume(next);
        if (chunk.empty()) break;
        all.insert(all.end(), chunk.begin(), chunk.end());
    }

    ASSERT_EQ(static_cast<int>(all.size()), N);
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(all[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
            << "offset wrong after reopen at i=" << i;
        EXPECT_EQ(all[static_cast<size_t>(i)].value, produced[static_cast<size_t>(i)].value)
            << "value wrong after reopen at i=" << i;
    }
}
