#pragma once
// TestWorkerCount.h — shared test helper for rune::node integration tests.
//
// Reads the RUNE_WORKERS environment variable so that EXIT-gate runs can force
// N>1 workers through the WorkerPool uniformly across every BrokerNode-based
// suite, without touching production code paths.
//
// Usage in a config builder:
//   cfg.workerCount = rune::node::test::workerCountFromEnv();
//
// At N=4:  RUNE_WORKERS=4 ./rune_<suite>_tests
// At N=1:  ./rune_<suite>_tests   (env unset → default 1)
//
// Test-only: must NOT be included from production headers.

#include <cstdint>
#include <cstdlib>

namespace rune::node::test {

/// Return the worker count for test fixtures:
///   - RUNE_WORKERS env set and >= 1 → that value
///   - otherwise → dflt (default: 1, the pre-E1 single-worker path)
inline int32_t workerCountFromEnv(int32_t dflt = 1) {
    const char* w = std::getenv("RUNE_WORKERS");
    if (!w || !*w) return dflt;
    char* end  = nullptr;
    long  v    = std::strtol(w, &end, 10);
    return (end != w && v >= 1) ? static_cast<int32_t>(v) : dflt;
}

} // namespace rune::node::test
