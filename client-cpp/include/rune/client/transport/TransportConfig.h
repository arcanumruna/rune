// include/rune/client/transport/TransportConfig.h
//
// Aeron-free transport configuration.  Every Aeron knob the client relies on is
// explicit here (spec §4.1/§4.3): MTU, socket buffers, term length, driver idle
// strategy, driver mode and client liveness.  validate() mirrors the driver's
// own rules (aeron_driver_context.c:1372-1390 for the MTU) so a bad value fails
// at start() with a clear message instead of an opaque aeron_driver_init error.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

namespace rune::client::transport {

/// Who drives the embedded media driver and the Aeron client conductor.
enum class DriverMode : uint8_t {
    Thread,   ///< default: SHARED driver thread + Aeron client conductor thread (unchanged)
    Invoker,  ///< AERON_THREADING_MODE_INVOKER + useConductorAgentInvoker(true): the
              ///< owning thread pumps both through AeronTransport::dutyCycle()
};

inline constexpr std::size_t kAeronDataHeaderLength    = 32;     // AERON_DATA_HEADER_LENGTH
inline constexpr std::size_t kAeronMaxUdpPayloadLength = 65504;  // AERON_MAX_UDP_PAYLOAD_LENGTH
inline constexpr std::size_t kAeronFrameAlignment      = 32;     // AERON_LOGBUFFER_FRAME_ALIGNMENT
inline constexpr std::size_t kMinTermLength            = 64u * 1024;
inline constexpr std::size_t kMaxTermLength            = 1024u * 1024 * 1024;

struct TransportConfig {
    std::string ingressChannel;        ///< Channel the client publishes to (→ gateway)
    int32_t     ingressStreamId = 0;   ///< Stream id for the default ingress publication
    std::string egressChannel;         ///< Channel the client subscribes on (← gateway)
    int32_t     egressStreamId  = 0;   ///< Stream id for the egress subscription
    std::string dataDir;               ///< Root dir; the driver lives at dataDir/aeron
    /// Term buffer length of every log buffer on this driver: power of two in
    /// [64 KiB, 1 GiB].  Default 4 MiB.
    std::size_t termBufferLength = 4u * 1024 * 1024;
    /// UDP MTU of every publication on this driver (Aeron default 1408).  8 KiB:
    /// measured 2026-09-30, 70% of all datagrams on the bench box were 1.4 KB
    /// client produce fragments; 8 KiB halved datagrams and cut p50 2-3x.
    /// Multiple of 32 in (32, 65504].  Env: RUNE_CLIENT_AERON_MTU.
    std::size_t mtuLength = 8192;
    /// SO_SNDBUF / SO_RCVBUF of the driver's UDP sockets (0 = OS default).
    /// Env: RUNE_AERON_SOCKET_BUFFER_BYTES (sets both, as before).
    std::size_t socketSndBufBytes = 2u * 1024 * 1024;
    std::size_t socketRcvBufBytes = 2u * 1024 * 1024;
    /// Thread-mode driver idle strategy (Aeron name: sleep-ns, backoff, noop, ...).
    /// Env: RUNE_CLIENT_AERON_DRIVER_IDLE.  Unused in Invoker mode.
    std::string driverIdleStrategy = "sleep-ns";
    DriverMode  driverMode = DriverMode::Thread;
    /// Driver client-liveness timeout (Aeron default 10 s).  In Invoker mode the
    /// owning thread must call dutyCycle() at least this often, otherwise the
    /// Aeron ClientConductor inter-service timeout closes the client.
    int64_t clientLivenessTimeoutNs = 10'000'000'000LL;
};

/// Returns an error message if any explicit knob violates Aeron's rules.
[[nodiscard]] inline std::optional<std::string> validate(const TransportConfig& c) {
    if (c.mtuLength <= kAeronDataHeaderLength || c.mtuLength > kAeronMaxUdpPayloadLength) {
        return "mtuLength must be > 32 and <= 65504: " + std::to_string(c.mtuLength);
    }
    if ((c.mtuLength & (kAeronFrameAlignment - 1)) != 0) {
        return "mtuLength must be a multiple of 32: " + std::to_string(c.mtuLength);
    }
    const std::size_t t = c.termBufferLength;
    if (t < kMinTermLength || t > kMaxTermLength || (t & (t - 1)) != 0) {
        return "termBufferLength must be a power of two in [65536, 1073741824]: " +
               std::to_string(t);
    }
    if (c.socketSndBufBytes != 0 && c.socketSndBufBytes < c.mtuLength) {
        return "socketSndBufBytes must be 0 (OS default) or >= mtuLength";
    }
    if (c.socketRcvBufBytes != 0 && c.socketRcvBufBytes < c.mtuLength) {
        return "socketRcvBufBytes must be 0 (OS default) or >= mtuLength";
    }
    if (c.clientLivenessTimeoutNs <= 0) {
        return "clientLivenessTimeoutNs must be > 0";
    }
    return std::nullopt;
}

/// Environment overrides win over code configuration (the historical semantics
/// of RUNE_AERON_SOCKET_BUFFER_BYTES).  Malformed values are ignored.
inline void applyEnvOverrides(TransportConfig& c) {
    auto readSize = [](const char* name, std::size_t& out) {
        const char* v = std::getenv(name);
        if (v == nullptr || *v == '\0') return;
        char* end = nullptr;
        const unsigned long long x = std::strtoull(v, &end, 10);
        if (end != nullptr && *end == '\0') out = static_cast<std::size_t>(x);
    };
    readSize("RUNE_CLIENT_AERON_MTU", c.mtuLength);
    std::size_t sock = static_cast<std::size_t>(-1);
    readSize("RUNE_AERON_SOCKET_BUFFER_BYTES", sock);
    if (sock != static_cast<std::size_t>(-1)) {
        c.socketSndBufBytes = sock;
        c.socketRcvBufBytes = sock;
    }
    if (const char* v = std::getenv("RUNE_CLIENT_AERON_DRIVER_IDLE"); v != nullptr && *v != '\0') {
        c.driverIdleStrategy = v;
    }
}

} // namespace rune::client::transport
