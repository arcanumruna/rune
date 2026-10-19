// Varint benchmark.
//
// Exercises writeVarlong/readVarlong/writeVarint/readVarint from Varint.h
// under five scenarios:
//
//  BM_Varint_WriteSmall        — zigzag-encode values 0..127 cycling; each
//                                fits in one byte; measures best-case path.
//  BM_Varint_WriteLarge        — INT64_MAX / INT64_MIN alternating; worst-case
//                                10-byte encodings.
//  BM_Varint_ReadSmall         — decode a pre-encoded single-byte stream.
//  BM_Varint_ReadLarge         — decode a pre-encoded 10-byte stream.
//  BM_Varint_RoundTripStream   — encode + decode 100 mixed values into one
//                                shared buffer; models a real frame build.

#include <benchmark/benchmark.h>
#include <cstdint>
#include <climits>
#include <vector>

#include "rune/protocol/Varint.h"

// ---------------------------------------------------------------------------
// BM_Varint_WriteSmall — single-byte encoded values cycling 0..127
// ---------------------------------------------------------------------------
static void BM_Varint_WriteSmall(benchmark::State& state) {
    uint8_t out[10];
    int64_t v = 0;

    for (auto _ : state) {
        size_t n = rune::protocol::writeVarlong(v, out);
        benchmark::DoNotOptimize(n);
        benchmark::DoNotOptimize(out[0]);
        v = (v + 1) & 0x7F; // cycle 0..127
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Varint_WriteSmall)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Varint_WriteLarge — worst-case 10-byte encodings (INT64_MAX / INT64_MIN)
// ---------------------------------------------------------------------------
static void BM_Varint_WriteLarge(benchmark::State& state) {
    uint8_t out[10];
    bool toggle = false;

    for (auto _ : state) {
        int64_t v = toggle ? INT64_MAX : INT64_MIN;
        size_t n = rune::protocol::writeVarlong(v, out);
        benchmark::DoNotOptimize(n);
        benchmark::DoNotOptimize(out[9]);
        toggle = !toggle;
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Varint_WriteLarge)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Varint_ReadSmall — decode a pre-built single-byte stream
// ---------------------------------------------------------------------------
static void BM_Varint_ReadSmall(benchmark::State& state) {
    // Pre-encode 128 small values (0..127) into a stream
    std::vector<uint8_t> stream;
    stream.reserve(128);
    uint8_t tmp[10];
    for (int64_t i = 0; i < 128; ++i) {
        size_t n = rune::protocol::writeVarlong(i, tmp);
        stream.insert(stream.end(), tmp, tmp + n);
    }
    const size_t streamLen = stream.size();

    for (auto _ : state) {
        size_t pos = 0;
        int64_t sum = 0;
        while (pos < streamLen) {
            sum += rune::protocol::readVarlong(stream.data(), pos);
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * 128);
}
BENCHMARK(BM_Varint_ReadSmall)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Varint_ReadLarge — decode a pre-built 10-byte-per-value stream
// ---------------------------------------------------------------------------
static void BM_Varint_ReadLarge(benchmark::State& state) {
    constexpr int kCount = 64;
    std::vector<uint8_t> stream;
    stream.reserve(kCount * 10);
    uint8_t tmp[10];
    for (int i = 0; i < kCount; ++i) {
        int64_t v = (i % 2 == 0) ? INT64_MAX : INT64_MIN;
        size_t n = rune::protocol::writeVarlong(v, tmp);
        stream.insert(stream.end(), tmp, tmp + n);
    }
    const size_t streamLen = stream.size();

    for (auto _ : state) {
        size_t pos = 0;
        int64_t sum = 0;
        while (pos < streamLen) {
            sum += rune::protocol::readVarlong(stream.data(), pos);
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * kCount);
}
BENCHMARK(BM_Varint_ReadLarge)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Varint_RoundTripStream — encode + decode 100 values in one shared buffer
//
// Simulates a real record: header contains entity-id (large), followed by
// small field deltas (1..5), then a few moderate values (100..1000).
// ---------------------------------------------------------------------------
static void BM_Varint_RoundTripStream(benchmark::State& state) {
    // Build the value sequence once; reused every iteration.
    constexpr int kN = 100;
    std::vector<int64_t> values(kN);
    for (int i = 0; i < kN; ++i) {
        if (i == 0)       values[i] = 0x00DEADBEEF000001LL; // entity-id shaped
        else if (i < 50)  values[i] = static_cast<int64_t>(i % 5 + 1); // small deltas
        else              values[i] = static_cast<int64_t>((i - 50) * 20 + 100); // moderate
    }

    std::vector<uint8_t> buf;
    buf.resize(kN * 10); // worst-case allocation

    for (auto _ : state) {
        // Encode phase
        size_t writePos = 0;
        for (int i = 0; i < kN; ++i) {
            writePos += rune::protocol::writeVarlong(values[i], buf.data() + writePos);
        }
        benchmark::ClobberMemory();

        // Decode phase
        size_t readPos = 0;
        int64_t sum = 0;
        for (int i = 0; i < kN; ++i) {
            sum += rune::protocol::readVarlong(buf.data(), readPos);
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * kN);
}
BENCHMARK(BM_Varint_RoundTripStream)->Unit(benchmark::kNanosecond);
