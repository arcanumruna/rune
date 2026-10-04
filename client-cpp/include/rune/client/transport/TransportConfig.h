// include/rune/client/transport/TransportConfig.h
//
// Aeron-free transport configuration.  Every Aeron knob the client relies on is
// explicit here (spec §4.1/§4.3): MTU, socket buffers, term length, driver idle
// strategy, driver mode and client liveness.  validate() mirrors the driver's
// own rules (aeron_driver_context.c:1372-1390 for the MTU) so a bad value fails
// at start() with a clear message instead of an opaque aeron_driver_init error.
#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

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
/// Aeron's default MTU (AERON_MTU_LENGTH_DEFAULT): one datagram fits a standard
/// 1500-byte Ethernet path (IP + UDP headers included) without IP fragmentation.
inline constexpr std::size_t kAeronDefaultMtuLength    = 1408;
/// Opt-in "jumbo" MTU for paths that carry it end to end (loopback, jumbo-frame
/// LANs) — NEVER the default (project rule: jumbo MTU stays opt-in).
inline constexpr std::size_t kJumboMtuLength           = 8192;
/// Aeron's default receiver initial window (AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT,
/// aeron_driver_context.c).  The driver refuses to start when the window exceeds
/// the socket's SO_RCVBUF (aeron_driver.c: "Window length greater than socket
/// SO_RCVBUF").
inline constexpr std::size_t kAeronDefaultRcvWindowLength = 128u * 1024;
inline constexpr std::size_t kMinTermLength            = 64u * 1024;
inline constexpr std::size_t kMaxTermLength            = 1024u * 1024 * 1024;
/// Liveness floor: 2x the Aeron C++ client's fixed 500 ms driver keepalive
/// (ClientConductor.h:47 KEEPALIVE_TIMEOUT_MS).  The client's inter-service
/// timeout is the driver's liveness value (Aeron.cpp:57), so anything near
/// 500 ms times out a healthy, properly pumped client.
inline constexpr int64_t kMinClientLivenessTimeoutNs = 1'000'000'000LL;
/// Liveness cap: 1 hour.  The transport derives publication_unblock_timeout =
/// 1.5x liveness, which the driver conductor casts to int64 (overflow > ~6.1e18).
inline constexpr int64_t kMaxClientLivenessTimeoutNs = 3'600'000'000'000LL;
/// aeron::Context DEFAULT_MEDIA_DRIVER_TIMEOUT_MS (Context.h:133): how long the
/// Aeron C++ client tolerates a stale driver heartbeat before closing itself.
inline constexpr long kAeronDefaultMediaDriverTimeoutMs = 10'000;

struct TransportConfig {
    std::string ingressChannel;        ///< Channel the client publishes to (→ gateway)
    int32_t     ingressStreamId = 0;   ///< Stream id for the default ingress publication
    std::string egressChannel;         ///< Channel the client subscribes on (← gateway)
    int32_t     egressStreamId  = 0;   ///< Stream id for the egress subscription
    std::string dataDir;               ///< Root dir; the driver lives at dataDir/aeron
    /// Term buffer length of every log buffer on this driver: power of two in
    /// [64 KiB, 1 GiB].  Default 4 MiB.
    std::size_t termBufferLength = 4u * 1024 * 1024;
    /// UDP MTU of every publication on this driver.  Default: Aeron's 1408, which
    /// fits a standard 1500-byte path without IP fragmentation.
    /// Opt-in jumbo (kJumboMtuLength = 8 KiB) ONLY where the whole path carries
    /// it (loopback, jumbo-frame LANs): measured 2026-09-30 on the bench box, 70%
    /// of all datagrams were 1.4 KB client produce fragments and 8 KiB halved
    /// datagrams and cut p50 2-3x — but on a 1500-byte path an 8 KiB datagram is
    /// IP-fragmented (one lost fragment loses the datagram).
    /// Multiple of 32 in (32, 65504].  Env: RUNE_CLIENT_AERON_MTU.
    std::size_t mtuLength = kAeronDefaultMtuLength;
    /// Linux UDP GSO (send) / GRO (receive) transport bindings in the embedded driver; auto-off where the
    /// kernel/platform lacks them (stock Aeron).  Wire format unchanged.  Env: RUNE_CLIENT_AERON_UDP_GSO=0|1.
    bool        udpGso             = true;
    /// A7: the embedded driver's transport pollers poll epoll-first (one epoll_wait per duty cycle, recvmmsg
    /// only ready sockets) instead of recvmmsg-ing every socket — Linux only.  Env: RUNE_CLIENT_AERON_EPOLL_POLL=0|1.
    bool        epollPoll          = true;
    /// Aeron network-publication datagrams per send, 1..16; 0 = 16 with GSO active, else Aeron's 2.
    /// Env: RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND.
    uint32_t    maxMessagesPerSend = 0;
    /// SO_SNDBUF / SO_RCVBUF of the driver's UDP sockets (0 = OS default).
    /// Env: RUNE_AERON_SOCKET_BUFFER_BYTES (sets both, as before).
    std::size_t socketSndBufBytes = 2u * 1024 * 1024;
    std::size_t socketRcvBufBytes = 2u * 1024 * 1024;
    /// Receiver initial window (0 = Aeron's default, kAeronDefaultRcvWindowLength
    /// = 128 KiB).  Must be >= mtuLength, and <= socketRcvBufBytes when that is
    /// set (the driver refuses to start otherwise).
    std::size_t rcvInitialWindowLength = 0;
    /// Thread-mode driver idle strategy (Aeron name: sleep-ns, backoff, noop, ...).
    /// Env: RUNE_CLIENT_AERON_DRIVER_IDLE.  Unused in Invoker mode.
    std::string driverIdleStrategy = "sleep-ns";
    DriverMode  driverMode = DriverMode::Thread;
    /// Driver client-liveness timeout (Aeron default 10 s).  In Invoker mode the
    /// owning thread must call dutyCycle() at least this often, otherwise the
    /// Aeron ClientConductor inter-service timeout closes the client.
    int64_t clientLivenessTimeoutNs = 10'000'000'000LL;
};

/// The aeron::Context::mediaDriverTimeout the transport applies (T15 R2).
///   Thread:  Aeron's default (10 s) — a separate driver thread can die alone.
///   Invoker: max(liveness, default).  The driver's only pumper is the duty
///            thread that also runs the client conductor, so the driver cannot
///            be dead while the conductor runs; after a stall the conductor would
///            otherwise see a stale driver heartbeat and close itself (the hidden
///            third timeout).  Shard clients run 1 h liveness, so 1 h.  A plain
///            Invoker Client keeps exactly today's 10 s at the default 10 s
///            liveness; one configured with a liveness ABOVE 10 s now gets that
///            liveness as its driver timeout too (T15a m11: a deliberate,
///            benign change — the same stall budget for both timeouts).
[[nodiscard]] constexpr long mediaDriverTimeoutMsFor(const TransportConfig& c) noexcept {
    if (c.driverMode != DriverMode::Invoker) return kAeronDefaultMediaDriverTimeoutMs;
    const auto livenessMs = static_cast<long>(c.clientLivenessTimeoutNs / 1'000'000);
    return livenessMs > kAeronDefaultMediaDriverTimeoutMs ? livenessMs
                                                          : kAeronDefaultMediaDriverTimeoutMs;
}

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
    // Aeron's max message is term/8: it must carry one maximum record (16 KiB, backlog B6) plus the
    // Produce + RecordBatch + record headers, or such a record is accepted and then never sendable.
    constexpr std::size_t kMaxRecordFrameBytes = 16u * 1024 + 1024;
    if (t / 8 < kMaxRecordFrameBytes) {
        return "termBufferLength " + std::to_string(t) + " gives a max message of " + std::to_string(t / 8) +
               " B (term/8) — too small for a 16 KiB record plus headers; use >= 262144";
    }
    if (c.socketSndBufBytes != 0 && c.socketSndBufBytes < c.mtuLength) {
        return "socketSndBufBytes must be 0 (OS default) or >= mtuLength";
    }
    if (c.socketRcvBufBytes != 0 && c.socketRcvBufBytes < c.mtuLength) {
        return "socketRcvBufBytes must be 0 (OS default) or >= mtuLength";
    }
    const std::size_t window =
        c.rcvInitialWindowLength != 0 ? c.rcvInitialWindowLength : kAeronDefaultRcvWindowLength;
    if (window < c.mtuLength) {
        return "rcvInitialWindowLength (" + std::to_string(window) +
               ") must be >= mtuLength (" + std::to_string(c.mtuLength) + ")";
    }
    if (c.socketRcvBufBytes != 0 && c.socketRcvBufBytes < window) {
        return "socketRcvBufBytes (" + std::to_string(c.socketRcvBufBytes) +
               ") is below the Aeron receive window (" + std::to_string(window) +
               (c.rcvInitialWindowLength != 0 ? ", rcvInitialWindowLength" : ", Aeron default") +
               "): the media driver refuses to start with a window larger than SO_RCVBUF. "
               "Raise socketBufferBytes / RUNE_AERON_SOCKET_BUFFER_BYTES to >= the window, "
               "or 0 for the OS default";
    }
    if (c.clientLivenessTimeoutNs < kMinClientLivenessTimeoutNs) {
        return "clientLivenessTimeoutNs must be >= 1000000000 (1 s = 2x the Aeron client's "
               "500 ms driver keepalive; lower values time out a healthy client): " +
               std::to_string(c.clientLivenessTimeoutNs);
    }
    if (c.clientLivenessTimeoutNs > kMaxClientLivenessTimeoutNs) {
        return "clientLivenessTimeoutNs must be <= 3600000000000 (1 hour): " +
               std::to_string(c.clientLivenessTimeoutNs);
    }
    return std::nullopt;
}

/// Parses a non-negative decimal byte count.  nullopt (and a reason in *why) for
/// anything else: empty, sign ("-1" is NOT wrapped to SIZE_MAX), suffix ("8k"),
/// whitespace, overflow.
[[nodiscard]] inline std::optional<std::size_t> parseEnvSize(const char* v, std::string* why) {
    const char* end = v + std::strlen(v);
    unsigned long long x = 0;
    const auto [ptr, ec] = std::from_chars(v, end, x, 10);  // no sign, no spaces
    if (ec == std::errc::result_out_of_range) {
        if (why) *why = "out of range";
        return std::nullopt;
    }
    if (ec != std::errc{} || ptr != end || v == end) {
        if (why) *why = "not a non-negative decimal integer";
        return std::nullopt;
    }
    return static_cast<std::size_t>(x);
}

/// Environment overrides win over code configuration (the historical semantics
/// of RUNE_AERON_SOCKET_BUFFER_BYTES).  A malformed value is NOT applied — and is
/// never dropped silently: it is returned as one warning line per variable for
/// the caller to log (Client logs them once, at construction — never per cycle).
/// An unset or empty variable is simply absent (no warning).
[[nodiscard]] inline std::vector<std::string> applyEnvOverrides(TransportConfig& c) {
    std::vector<std::string> warnings;
    auto readSize = [&warnings](const char* name) -> std::optional<std::size_t> {
        const char* v = std::getenv(name);
        if (v == nullptr || *v == '\0') return std::nullopt;
        std::string why;
        auto x = parseEnvSize(v, &why);
        if (!x) {
            warnings.push_back(std::string("rune-client: ignoring ") + name + "=\"" + v +
                               "\": " + why + " (keeping the configured value)");
        }
        return x;
    };
    if (const auto mtu = readSize("RUNE_CLIENT_AERON_MTU")) c.mtuLength = *mtu;
    if (const auto g = readSize("RUNE_CLIENT_AERON_UDP_GSO")) c.udpGso = (*g != 0);
    if (const auto e = readSize("RUNE_CLIENT_AERON_EPOLL_POLL")) c.epollPoll = (*e != 0);
    if (const auto m = readSize("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND")) {
        if (*m >= 1 && *m <= 16) c.maxMessagesPerSend = static_cast<uint32_t>(*m);
        else warnings.push_back("rune-client: ignoring RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND=" + std::to_string(*m) +
                                ": must be in [1, 16] (keeping the configured value)");
    }
    if (const auto sock = readSize("RUNE_AERON_SOCKET_BUFFER_BYTES")) {
        c.socketSndBufBytes = *sock;  // 0 = OS default, as before
        c.socketRcvBufBytes = *sock;
    }
    if (const char* v = std::getenv("RUNE_CLIENT_AERON_DRIVER_IDLE"); v != nullptr && *v != '\0') {
        c.driverIdleStrategy = v;
    }
    return warnings;
}

} // namespace rune::client::transport
