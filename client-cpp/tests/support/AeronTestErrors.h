// tests/support/AeronTestErrors.h — error handler for test-side Aeron clients.
//
// Aeron's default handler ::exit(-1)s the process.  Test gateways attach their own
// Aeron client to the Client's embedded driver; when a test closes the Client (driver
// stopped) before tearing its gateway down, that client's keepalive check may report
// the dead driver.  Count + log instead of exiting the test binary.
#pragma once

#include <atomic>
#include <cstdio>
#include <exception>

namespace test_support {

/// Asynchronous errors reported by test-side Aeron clients in this process.
inline std::atomic<int> g_aeronAsyncErrors{0};

inline void countAeronError(const std::exception& e) {
    const int n = g_aeronAsyncErrors.fetch_add(1, std::memory_order_relaxed) + 1;
    std::fprintf(stderr, "[test aeron client error #%d] %s\n", n, e.what());
}

} // namespace test_support
