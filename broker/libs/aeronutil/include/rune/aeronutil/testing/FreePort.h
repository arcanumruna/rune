#pragma once
// TEST-ONLY: free UDP port allocation that survives PARALLEL test processes (D8, 2026-10-03).
//
// The old helpers (bind port 0, read it back, close) returned EPHEMERAL ports: the kernel hands a just-freed
// ephemeral port to the next process that asks, so two concurrently running test binaries got the same port
// and a broker failed with "Address already in use" (seen as PerWorkerReplication flakes + a null-gateway
// segfault).  Ports here come from 20000..31999 — below the ephemeral ranges of Linux (32768+) and macOS
// (49152+), so the kernel never hands them out implicitly — walked from a pid-scattered start, each one
// checked bindable on 127.0.0.1 before it is returned.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>

namespace rune::aeronutil::testing {

inline bool udpPortBindable(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return false;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0;
    ::close(fd);
    return ok;
}

inline uint16_t allocFreeUdpPort() {
    constexpr uint32_t kBase = 20000, kSpan = 12000;
    static uint32_t next = (static_cast<uint32_t>(::getpid()) * 7919u) % kSpan;   // per-process scatter
    for (uint32_t tries = 0; tries < kSpan; ++tries) {
        const auto p = static_cast<uint16_t>(kBase + (next++ % kSpan));
        if (udpPortBindable(p)) return p;
    }
    std::fprintf(stderr, "allocFreeUdpPort: no bindable port in 20000..31999\n");   // never hand Aeron port 0
    std::abort();
}

} // namespace rune::aeronutil::testing
