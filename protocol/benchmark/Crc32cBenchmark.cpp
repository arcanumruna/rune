// CRC-32C benchmark.
//
// Three scenarios:
//  1. BM_Crc32c_OneShot      — single crc32c(ptr, len) call over varying sizes
//     (16 B … 1 MiB) to show throughput scaling; SetBytesProcessed enables
//     GB/s reporting in the output.
//
//  2. BM_Crc32c_Incremental  — 1 KiB buffer split into 8 × 128-B chunks fed
//     through the seed overload, simulating the broker stitching frame fields.
//
//  3. BM_Crc32c_SmallMessage — 48-byte MMORPG-shaped payload (position update:
//     entity-id 8 B, x/y/z floats 12 B, heading 4 B, velocity 8 B, seq 4 B,
//     flags 2 B, padding 10 B); SetItemsProcessed reports messages/sec.

#include <benchmark/benchmark.h>
#include <cstdint>
#include <cstring>
#include <vector>

#include "rune/protocol/Crc32c.h"

// ---------------------------------------------------------------------------
// BM_Crc32c_OneShot — throughput across buffer sizes
// ---------------------------------------------------------------------------
static void BM_Crc32c_OneShot(benchmark::State& state) {
    const int64_t size = state.range(0);
    std::vector<uint8_t> buf(static_cast<size_t>(size), 0xAB);

    for (auto _ : state) {
        uint32_t result = rune::protocol::crc32c(buf.data(), buf.size());
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * size);
    state.SetLabel(std::to_string(size) + " B");
}
BENCHMARK(BM_Crc32c_OneShot)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(65536)
    ->Arg(1048576)
    ->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------
// BM_Crc32c_Incremental — compose CRC over 8 equal-sized chunks
// ---------------------------------------------------------------------------
static void BM_Crc32c_Incremental(benchmark::State& state) {
    // 1 KiB split into 8 × 128-byte chunks
    constexpr size_t kTotal  = 1024;
    constexpr size_t kChunks = 8;
    constexpr size_t kChunk  = kTotal / kChunks;

    std::vector<uint8_t> buf(kTotal);
    for (size_t i = 0; i < kTotal; ++i) buf[i] = static_cast<uint8_t>(i & 0xFF);

    for (auto _ : state) {
        uint32_t crc = 0;
        for (size_t c = 0; c < kChunks; ++c) {
            crc = rune::protocol::crc32c(crc, buf.data() + c * kChunk, kChunk);
        }
        benchmark::DoNotOptimize(crc);
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(kTotal));
}
BENCHMARK(BM_Crc32c_Incremental)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Crc32c_SmallMessage — 48-byte MMORPG position-update payload
// ---------------------------------------------------------------------------
static void BM_Crc32c_SmallMessage(benchmark::State& state) {
    // Layout (48 bytes):
    //  [0..7]   entity_id  (uint64)
    //  [8..11]  x          (float)
    //  [12..15] y          (float)
    //  [16..19] z          (float)
    //  [20..23] heading    (float)
    //  [24..31] vx/vy      (float × 2)
    //  [32..35] sequence   (uint32)
    //  [36..37] flags      (uint16)
    //  [38..47] padding
    alignas(8) uint8_t payload[48]{};
    uint64_t entity_id = 0x00DEADBEEF000001ULL;
    std::memcpy(payload, &entity_id, 8);
    // fill some floats so the data isn't all zeros
    float vals[4] = {100.5f, 200.75f, 0.0f, 1.5707f};
    std::memcpy(payload + 8, vals, 16);

    for (auto _ : state) {
        uint32_t result = rune::protocol::crc32c(payload, sizeof(payload));
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations()); // messages/s
}
BENCHMARK(BM_Crc32c_SmallMessage)->Unit(benchmark::kNanosecond);
