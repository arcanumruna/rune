// RecordBatch benchmark.
//
// Covers the full RecordBatchBuilder / RecordBatchReader public surface:
//
//  BM_Batch_Build_1Record          — build() overhead for one record
//  BM_Batch_Build_10Records        — 10-record batch; allocation + CRC path
//  BM_Batch_Build_100Records       — 100-record batch; typical broker flush
//  BM_Batch_EnsureValid_1          — CRC validation on a 1-record batch
//  BM_Batch_EnsureValid_100        — CRC validation on a 100-record batch
//  BM_Batch_ForEach_100Records     — iterate + sum payload sizes (no alloc)
//  BM_Batch_StampLeaderEpoch       — in-place epoch stamp (broker hot path)
//  BM_Batch_BuildParseRoundTrip    — 48-B key+value MMORPG message end-to-end

#include <benchmark/benchmark.h>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

#include "rune/protocol/RecordBatch.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
namespace {

// Build a batch with `n` records, each carrying key and value payloads.
static std::vector<uint8_t> makeBatch(int n,
                                       std::string_view key,
                                       std::string_view value) {
    rune::protocol::RecordBatchBuilder builder(0, 1000000LL);
    for (int i = 0; i < n; ++i) {
        builder.append(key, value, 1000000LL + i);
    }
    return builder.build();
}

// MMORPG position-update key: entity_id (8 B) + zone_id (4 B) +
//   msg_type (2 B) + seq (4 B) + padding (2 B) = 20 B packed via alignas(1)
#pragma pack(push, 1)
struct MmoKey {
    uint64_t entity_id{0x00DEADBEEF000001ULL};
    uint32_t zone_id{42};
    uint16_t msg_type{0x0010};  // POSITION_UPDATE
    uint32_t seq{0};
    uint16_t pad{0};
};
#pragma pack(pop)

// MMORPG value: x/y/z (float×3 = 12 B), heading (4 B), vx/vy (float×2 = 8 B),
//   health (2 B), flags (2 B), padding (4 B) = 32 B packed
#pragma pack(push, 1)
struct MmoValue {
    float x{100.5f}, y{200.75f}, z{0.0f};
    float heading{1.5707f};
    float vx{1.0f}, vy{0.0f};
    uint16_t health{1000};
    uint16_t flags{0x0003};
    uint8_t padding[4]{};
};
#pragma pack(pop)

} // namespace

// ---------------------------------------------------------------------------
// BM_Batch_Build_1Record
// ---------------------------------------------------------------------------
static void BM_Batch_Build_1Record(benchmark::State& state) {
    const std::string key(8, 'K');
    const std::string value(32, 'V');

    for (auto _ : state) {
        rune::protocol::RecordBatchBuilder builder(0, 1000000LL);
        builder.append(key, value, 1000000LL);
        auto batch = builder.build();
        benchmark::DoNotOptimize(batch.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * 1);
}
BENCHMARK(BM_Batch_Build_1Record)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Batch_Build_10Records
// ---------------------------------------------------------------------------
static void BM_Batch_Build_10Records(benchmark::State& state) {
    const std::string key(8, 'K');
    const std::string value(32, 'V');

    for (auto _ : state) {
        rune::protocol::RecordBatchBuilder builder(0, 1000000LL);
        for (int i = 0; i < 10; ++i) {
            builder.append(key, value, 1000000LL + i);
        }
        auto batch = builder.build();
        benchmark::DoNotOptimize(batch.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * 10);
}
BENCHMARK(BM_Batch_Build_10Records)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Batch_Build_100Records
// ---------------------------------------------------------------------------
static void BM_Batch_Build_100Records(benchmark::State& state) {
    const std::string key(8, 'K');
    const std::string value(32, 'V');

    for (auto _ : state) {
        rune::protocol::RecordBatchBuilder builder(0, 1000000LL);
        for (int i = 0; i < 100; ++i) {
            builder.append(key, value, 1000000LL + i);
        }
        auto batch = builder.build();
        benchmark::DoNotOptimize(batch.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_Batch_Build_100Records)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Batch_EnsureValid_1
// ---------------------------------------------------------------------------
static void BM_Batch_EnsureValid_1(benchmark::State& state) {
    // Build once outside the loop — only validation is timed.
    const auto batch = makeBatch(1, std::string(8, 'K'), std::string(32, 'V'));

    for (auto _ : state) {
        rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
        bool valid = reader.ensureValid();
        benchmark::DoNotOptimize(valid);
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Batch_EnsureValid_1)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Batch_EnsureValid_100
// ---------------------------------------------------------------------------
static void BM_Batch_EnsureValid_100(benchmark::State& state) {
    const auto batch = makeBatch(100, std::string(8, 'K'), std::string(32, 'V'));

    for (auto _ : state) {
        rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
        bool valid = reader.ensureValid();
        benchmark::DoNotOptimize(valid);
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(batch.size()));
    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_Batch_EnsureValid_100)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Batch_ForEach_100Records — iterate and accumulate payload sizes
// ---------------------------------------------------------------------------
static void BM_Batch_ForEach_100Records(benchmark::State& state) {
    const auto batch = makeBatch(100, std::string(8, 'K'), std::string(32, 'V'));

    for (auto _ : state) {
        rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
        int64_t total = 0;
        reader.forEach([&](std::string_view key,
                           std::string_view val,
                           int64_t offset,
                           int64_t ts) {
            total += static_cast<int64_t>(key.size() + val.size());
            (void)offset; (void)ts;
        });
        benchmark::DoNotOptimize(total);
    }

    state.SetItemsProcessed(state.iterations() * 100);
}
BENCHMARK(BM_Batch_ForEach_100Records)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Batch_StampLeaderEpoch — in-place broker path (no CRC recompute)
// ---------------------------------------------------------------------------
static void BM_Batch_StampLeaderEpoch(benchmark::State& state) {
    auto batch = makeBatch(10, std::string(8, 'K'), std::string(32, 'V'));
    int32_t epoch = 0;

    for (auto _ : state) {
        rune::protocol::RecordBatchReader::stampLeaderEpoch(batch.data(), epoch++);
        benchmark::DoNotOptimize(batch.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Batch_StampLeaderEpoch)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Batch_BuildParseRoundTrip_SmallMessage
//
// MMORPG-shaped: 48-B key + value round-trip (builder → build → reader →
// forEach). One batch per iteration; SetItemsProcessed = records processed.
// ---------------------------------------------------------------------------
static void BM_Batch_BuildParseRoundTrip_SmallMessage(benchmark::State& state) {
    MmoKey   mmoKey{};
    MmoValue mmoVal{};

    std::string key(sizeof(mmoKey), '\0');
    std::string value(sizeof(mmoVal), '\0');
    std::memcpy(key.data(), &mmoKey, sizeof(mmoKey));
    std::memcpy(value.data(), &mmoVal, sizeof(mmoVal));

    for (auto _ : state) {
        // Build
        rune::protocol::RecordBatchBuilder builder(0, 1000000LL);
        builder.append(key, value, 1000000LL);
        auto batch = builder.build();
        benchmark::ClobberMemory();

        // Validate + parse
        rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
        bool valid = reader.ensureValid();
        benchmark::DoNotOptimize(valid);

        int64_t totalBytes = 0;
        reader.forEach([&](std::string_view k,
                           std::string_view v,
                           int64_t offset,
                           int64_t ts) {
            totalBytes += static_cast<int64_t>(k.size() + v.size());
            (void)offset; (void)ts;
        });
        benchmark::DoNotOptimize(totalBytes);
    }

    state.SetItemsProcessed(state.iterations()); // messages/s
}
BENCHMARK(BM_Batch_BuildParseRoundTrip_SmallMessage)->Unit(benchmark::kNanosecond);
