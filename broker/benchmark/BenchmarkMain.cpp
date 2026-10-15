#include "BenchGate.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>

// ---------------------------------------------------------------------------
// Shared heap-allocation counter for zero-alloc assertions.
//
// operator new/delete are replaceable; only one definition may exist in the
// final binary (ODR).  They live here (BenchmarkMain.cpp) so every benchmark
// translation unit can declare them extern and share the same counters without
// ODR violations.
//
// The gated pattern:
//   extern std::atomic<int64_t> g_allocCount;
//   extern std::atomic<bool>    g_allocEnabled;
//
//   g_allocCount.store(0);
//   g_allocEnabled.store(true, std::memory_order_release);
//   /* ... hot loop ... */
//   g_allocEnabled.store(false, std::memory_order_release);
//   int64_t allocs = g_allocCount.load(std::memory_order_acquire);
//   state.counters["allocs"] = static_cast<double>(allocs);
// ---------------------------------------------------------------------------
std::atomic<int64_t> g_allocCount{0};
std::atomic<bool>    g_allocEnabled{false};

void* operator new(std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed))
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}

void* operator new[](std::size_t sz)
{
    if (g_allocEnabled.load(std::memory_order_relaxed))
        g_allocCount.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz);
    if (!p) throw std::bad_alloc{};
    return p;
}

void operator delete(void* p) noexcept  { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept  { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

// Task 23 fix round 1 (R-I2): BENCHMARK_MAIN() plus the gate verdict — any rune::bench::gateFail() makes the
// process exit 1 (the failures are listed on stderr), so a tripped gate is never recorded as a pass.
int main(int argc, char** argv)
{
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    const auto& failures = rune::bench::gateFailures();
    for (const auto& f : failures)
        std::fprintf(stderr, "BENCH GATE FAILED: %s: %s\n", f.benchmark.c_str(), f.message.c_str());
    return failures.empty() ? 0 : 1;
}
