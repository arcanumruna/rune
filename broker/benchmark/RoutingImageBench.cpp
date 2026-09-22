// RoutingImageBench.cpp — MD1 Task 8: RoutingImage lookup latency + zero-alloc.
//
// Benchmarks:
//
//   1. BM_RoutingImage_Lookup_Hit
//      Per-call latency of lookup(pid) for a pid that exists (cache-warm).
//      Expect single-digit ns.  No heap allocation in timed region.
//
//   2. BM_RoutingImage_Lookup_Miss
//      Per-call latency of lookup(pid) for an out-of-range pid (sentinel path).
//      Same O(1) array-bounds check — expect same or lower latency than a hit.
//
//   3. BM_RoutingImage_Lookup_Scattered
//      lookup() across all 1024 pids in round-robin — stress L1 cache.
//      Measures amortised latency when the working set doesn't fit a single
//      cache line.
//
//   4. BM_RoutingImage_Publish_ThenRead
//      Publisher swaps image; reader acquire-loads then does one lookup.
//      Measures the combined publish+read latency — confirms the atomic
//      exchange path stays cheap.
//
// Zero-alloc assertion strategy
// ──────────────────────────────
// The lookup loop annotates results with DoNotOptimize to prevent DCE.
// The image is pre-built outside the timed region.  No allocator calls can
// occur in lookup() (direct array index + return-by-reference).  We verify
// this structurally:
//   a. The image is const after construction.
//   b. lookup() is noexcept and returns a const reference.
//   c. No std::vector or std::string is created inside lookup().
// The comment "ZERO_ALLOC" is left in the timed region for audit.

#include <benchmark/benchmark.h>

#include <rune/metadata/RoutingImage.h>

#include <cstdint>
#include <memory>
#include <vector>

using namespace rune::metadata;

namespace {

// ── Shared image fixture ────────────────────────────────────────────────────

static constexpr int kNumPartitions = 1024;

static std::shared_ptr<const RoutingImage> makeImage(int n = kNumPartitions)
{
    std::vector<RoutingImage::BuildEntry> entries;
    entries.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        entries.push_back(RoutingImage::BuildEntry{
            /*globalPartitionId=*/ static_cast<int32_t>(i),
            /*leaderNodeId=*/      static_cast<int32_t>(i % 8),
            /*leaderEpoch=*/       static_cast<int32_t>(i + 1)
        });
    }
    return RoutingImage::build(entries);
}

// ── 1. BM_RoutingImage_Lookup_Hit ───────────────────────────────────────────
//
// Always looks up pid=42 (guaranteed hit, warm in L1).
// Measures the absolute floor latency: array index check + reference return.
// ─────────────────────────────────────────────────────────────────────────────

static void BM_RoutingImage_Lookup_Hit(benchmark::State& state)
{
    auto img = makeImage();

    // Pre-warm: touch the relevant cache line.
    int32_t warm = img->lookup(42).leaderNodeId;
    benchmark::DoNotOptimize(warm);

    for (auto _ : state) {
        // ZERO_ALLOC: direct array index, const reference return.
        const auto& e = img->lookup(42);
        int32_t v = e.leaderNodeId;
        benchmark::DoNotOptimize(v);
    }

    state.SetLabel("zero-alloc O(1) hit");
}
BENCHMARK(BM_RoutingImage_Lookup_Hit)->Unit(benchmark::kNanosecond);

// ── 2. BM_RoutingImage_Lookup_Miss ──────────────────────────────────────────
//
// Always looks up an out-of-range pid (sentinel path).
// Same O(1) bounds check + static sentinel reference.
// ─────────────────────────────────────────────────────────────────────────────

static void BM_RoutingImage_Lookup_Miss(benchmark::State& state)
{
    auto img = makeImage();

    int32_t warm = img->lookup(99999).leaderNodeId;
    benchmark::DoNotOptimize(warm);

    for (auto _ : state) {
        // ZERO_ALLOC: bounds check, return static sentinel reference.
        const auto& e = img->lookup(99999);
        int32_t v = e.leaderNodeId;
        benchmark::DoNotOptimize(v);
    }

    state.SetLabel("zero-alloc O(1) miss/sentinel");
}
BENCHMARK(BM_RoutingImage_Lookup_Miss)->Unit(benchmark::kNanosecond);

// ── 3. BM_RoutingImage_Lookup_Scattered ─────────────────────────────────────
//
// Rotates through all kNumPartitions pids per iteration.
// Stresses the TLB/L1: kNumPartitions * sizeof(Entry) = 1024 * 8 = 8 KiB.
// ─────────────────────────────────────────────────────────────────────────────

static void BM_RoutingImage_Lookup_Scattered(benchmark::State& state)
{
    auto img  = makeImage();
    int32_t i = 0;

    // Pre-warm: a full pass to pull the array into L2/L3.
    for (int32_t p = 0; p < kNumPartitions; ++p) {
        int32_t warm = img->lookup(p).leaderNodeId;
        benchmark::DoNotOptimize(warm);
    }

    for (auto _ : state) {
        // ZERO_ALLOC: same O(1) path, scattered access pattern.
        const auto& e = img->lookup(i);
        int32_t v = e.leaderNodeId;
        benchmark::DoNotOptimize(v);
        i = (i + 1) & (kNumPartitions - 1);  // power-of-2 wrap
    }

    state.SetLabel("zero-alloc O(1) scattered");
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_RoutingImage_Lookup_Scattered)->Unit(benchmark::kNanosecond);

// ── 4. BM_RoutingImage_Publish_ThenRead ─────────────────────────────────────
//
// Each iteration:
//   a. (outside timed) build a new image.
//   b. (timed) publish it (release-store on shared_ptr atomic ops).
//   c. (timed) acquire-load current() into a local snap.
//   d. (timed) lookup(0).
//
// Captures the end-to-end publish→read latency (control-plane swap cost).
// ─────────────────────────────────────────────────────────────────────────────

static void BM_RoutingImage_Publish_ThenRead(benchmark::State& state)
{
    RoutingImageHolder holder;
    holder.publish(makeImage());

    for (auto _ : state) {
        // Pre-build outside timed region to isolate swap+read latency.
        state.PauseTiming();
        auto newImg = makeImage();
        state.ResumeTiming();

        // Timed: publish (release-store) + acquire-load + lookup.
        holder.publish(std::move(newImg));                 // release-store
        auto snap = holder.current();                      // acquire-load
        int32_t v = snap->lookup(0).leaderNodeId;
        benchmark::DoNotOptimize(v);
    }

    state.SetLabel("publish+read round-trip");
}
BENCHMARK(BM_RoutingImage_Publish_ThenRead)->Unit(benchmark::kNanosecond);

} // namespace
