// benchmark/ViewTableBench.cpp
//
// Task 7 (§7.1 / §14.0) — the ViewTable storage GATE.
//
// A churn workload (60% create / 25% update / 15% delete) against the
// key->row index + chunked-slab ViewTable from Tasks 3-6, at three row
// sizes (64 / 256 / 1024 B), reporting the counters the gate is judged on:
//
//   rehash_tax_us  = ViewTable::worstRehashTaxUs() after the whole churn —
//                    the single worst per-op cost paid by the incremental
//                    index-rehash machinery (maybePrezero/migrateStep/
//                    startGrow). Gate: < 50us for ALL three row sizes, and
//                    FLAT across row sizes (the timed sections only ever
//                    touch the tiny tags_/keys_/rid_ index arrays, never
//                    the row-payload chunks_, so there is no code-level
//                    reason for it to scale with rowLen).
//   delete_p99_ns  = p99 of individual erase() wall-clock latencies.
//                    Gate: flat across row sizes (erase() never moves a
//                    row's payload).
//   find_steady_ns = ns/op over a pure get()-only window on the final live
//                    set (no inserts/deletes in that window). Gate: ~25-30.
//   fallbacks      = ViewTable::rehashFallbacks(): how many startGrow()
//                    calls had to finish the next index buffer's zero-fill
//                    inline because de-amortized pre-provisioning wasn't
//                    ready. Expected 0 under this workload (see ViewTable.h).
//   steady_alloc   = heap allocations (via g_allocCount, gated by
//                    g_allocEnabled) during that SAME pure find() window.
//                    Gate: must be exactly 0 (get() never allocates).
//
// Row sizes are produced via a RowLayout padded with a FixedStr filler
// column (buildRowLayout() from ViewRow.h) so rowLen matches exactly:
// 1 bitmap byte + an 8-byte Int64 key + a 4-byte Int32 payload + filler.
//
// NOTE: this is a single, heavy "one full churn pass per iteration"
// benchmark (15M/15M/8M rows) — ->Iterations(1) so it runs exactly once per
// row size rather than google-benchmark's normal repeat-until-stable loop,
// which would multiply an already multi-second workload.
#include <benchmark/benchmark.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

// Defined once in benchmark/BenchmarkMain.cpp; shared by every benchmark TU
// that needs the zero-alloc assertion (see that file's header comment).
extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

using namespace rune::view;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::codec::FieldValue;

// 3 fields (id, val, filler) -> bitmapBytes = ceil(3/8) = 1, dataOffset = 1.
// rowLen = 1 + 8 (id: Int64) + 4 (val: Int32) + fillerLen.
constexpr uint32_t kFixedOverhead = 1 + 8 + 4;

RowLayout layoutForRowSize(uint32_t targetRowLen) {
    const uint16_t fillerLen = static_cast<uint16_t>(targetRowLen - kFixedOverhead);
    auto desc = DescriptorBuilder("view_table_bench", 1)
        .add("id", PrimType::Int64, 1, /*isKey*/true)
        .add("val", PrimType::Int32)
        .addFixedStr("filler", fillerLen)
        .build();
    RowLayout L = buildRowLayout(desc);
    return L;
}

void setVal(uint8_t* row, const RowLayout& L, int32_t v) {
    FieldValue fv{};
    fv.type = PrimType::Int32;
    fv.present = true;
    fv.i = v;
    writeSlot(row, L, 1, fv);
}

// Rows-at-target scaled down from the brief's 15M/15M/8M so the suite stays
// fast under normal `--benchmark_filter` CI runs; pass a larger value via
// --benchmark_min_time / a custom Arg sweep for the full §7.1 gate run.
uint64_t targetRowsFor(size_t rowSize) {
    return rowSize >= 1024 ? 8'000'000 : 15'000'000;
}

} // namespace

static void BM_ViewTableChurn(benchmark::State& st) {
    const uint32_t rowSize = static_cast<uint32_t>(st.range(0));
    const uint64_t targetRows = targetRowsFor(rowSize);
    const uint64_t totalOps = static_cast<uint64_t>(targetRows / 0.60);

    for (auto _ : st) {
        RowLayout L = layoutForRowSize(rowSize);
        ViewTable t(L);

        std::vector<uint64_t> alive;
        alive.reserve(targetRows + 16);
        std::unordered_map<uint64_t, uint32_t> idx;
        idx.reserve(targetRows * 2);

        std::vector<uint32_t> deleteLatNs;
        deleteLatNs.reserve(static_cast<size_t>(totalOps * 0.16));

        std::mt19937_64 rng(0xD00DF00Dull ^ rowSize);
        std::uniform_int_distribution<int> opDist(0, 99);

        uint64_t nextKey = 1;

        for (uint64_t i = 0; i < totalOps; ++i) {
            const int r = opDist(rng);
            const bool doCreate = (r < 60) || alive.empty();
            const bool doUpdate = !doCreate && (r < 85);

            if (doCreate) {
                const uint64_t key = nextKey++;
                uint8_t* row = t.put(key);
                setVal(row, L, static_cast<int32_t>(key));
                idx[key] = static_cast<uint32_t>(alive.size());
                alive.push_back(key);
            } else if (doUpdate) {
                std::uniform_int_distribution<size_t> pick(0, alive.size() - 1);
                const uint64_t key = alive[pick(rng)];
                uint8_t* row = t.put(key);
                setVal(row, L, static_cast<int32_t>(key ^ i));
            } else {
                std::uniform_int_distribution<size_t> pick(0, alive.size() - 1);
                const size_t pos = pick(rng);
                const uint64_t key = alive[pos];

                const auto t0 = std::chrono::steady_clock::now();
                t.erase(key);
                const auto t1 = std::chrono::steady_clock::now();

                const uint32_t lastIdx = static_cast<uint32_t>(alive.size() - 1);
                const uint64_t lastKey = alive[lastIdx];
                alive[pos] = lastKey;
                idx[lastKey] = static_cast<uint32_t>(pos);
                alive.pop_back();
                idx.erase(key);

                const uint64_t ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
                deleteLatNs.push_back(static_cast<uint32_t>(ns));
            }
        }

        // Steady-state find window: pure get() over the final live set, no
        // insert/delete/harness-vector mutation in-loop, so any heap
        // activity observed here is attributable only to ViewTable::get().
        const size_t findIters = std::min<size_t>(alive.size() * 4, 4'000'000);
        std::vector<uint64_t> probeKeys(findIters);
        {
            std::uniform_int_distribution<size_t> pick(0, alive.size() - 1);
            for (size_t i = 0; i < findIters; ++i) probeKeys[i] = alive[pick(rng)];
        }

        g_allocCount.store(0, std::memory_order_relaxed);
        g_allocEnabled.store(true, std::memory_order_relaxed);
        const auto fs0 = std::chrono::steady_clock::now();
        uint64_t sink = 0;
        for (size_t i = 0; i < findIters; ++i) {
            const uint8_t* row = t.get(probeKeys[i]);
            sink += reinterpret_cast<uintptr_t>(row) & 0xFF;
        }
        const auto fs1 = std::chrono::steady_clock::now();
        g_allocEnabled.store(false, std::memory_order_relaxed);
        const int64_t steadyAlloc = g_allocCount.load(std::memory_order_relaxed);
        benchmark::DoNotOptimize(sink);

        const double findSteadyNs = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(fs1 - fs0).count()) /
            static_cast<double>(findIters);

        std::sort(deleteLatNs.begin(), deleteLatNs.end());
        const double deleteP99Ns = deleteLatNs.empty() ? 0.0
            : static_cast<double>(deleteLatNs[static_cast<size_t>(deleteLatNs.size() * 0.99)]);

        st.counters["rehash_tax_us"] = t.worstRehashTaxUs();
        st.counters["delete_p99_ns"] = deleteP99Ns;
        st.counters["find_steady_ns"] = findSteadyNs;
        st.counters["fallbacks"] = static_cast<double>(t.rehashFallbacks());
        st.counters["steady_alloc"] = static_cast<double>(steadyAlloc);
        st.counters["final_size"] = static_cast<double>(t.size());
        st.counters["chunk_count"] = static_cast<double>(t.chunkCount());
    }
}
BENCHMARK(BM_ViewTableChurn)
    ->Arg(64)->Arg(256)->Arg(1024)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();
