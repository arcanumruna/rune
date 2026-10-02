#pragma once
// HostDefaults.h — the measured 110k rec/s configuration as the DEFAULT, derived
// from the host (docs/superpowers/specs/2026-10-02-throughput-latency-findings.md
// §4 + its hard constraints).
//
// Split in two:
//   * PURE derivation functions (deriveAeronTransport, deriveShardBasePorts,
//     deriveWorkerCoreBase, applyHostDerivedDefaults): inputs are plain facts,
//     no syscalls — exhaustively unit-tested (HostDefaultsTest).
//   * ONE impure probe (probeHostNetFacts): reads the OS limits / interface MTU.
//
// Every value a deployment sets explicitly (env or NodeConfig field) wins; only
// fields left at their AUTO sentinel are derived.  Startup-only code (cold path):
// allocation and string building are fine here.
//
// Aeron transport rules (all sizes bytes):
//   socketBuffer = min(16 MiB, OS max SO_RCVBUF/SO_SNDBUF)  — capped → loud WARN
//   rcvWindow    = pow2floor(min(4 MiB, socketBuffer/4, term/2))
//                  (64 KiB datagrams: kernel truesize makes a window near
//                  SO_RCVBUF drop packets — 16 MiB window measured 98 % drops)
//   mtu          = 65504 on loopback, else interface MTU − IP/UDP headers,
//                  aligned down to 32; then clamped ≤ socketBuffer (aeron_driver_init
//                  EINVALs when mtu > SO_SNDBUF — the macOS lesson) and ≤ window/4.
//   Explicit values are kept but CLAMPED (with a WARN) where they would make
//   aeron_driver_init / the receive endpoint fail: mtu ≤ SO_SNDBUF cap and ≤
//   window; window ≤ SO_RCVBUF cap.  A config can never reach the driver EINVAL.
#include <rune/node/NodeConfig.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace rune::node {

enum class HostPlatform : uint8_t { Linux, MacOS, Other };

[[nodiscard]] constexpr HostPlatform currentHostPlatform() noexcept {
#if defined(__linux__)
    return HostPlatform::Linux;
#elif defined(__APPLE__)
    return HostPlatform::MacOS;
#else
    return HostPlatform::Other;
#endif
}

/// Sentinels (defined on NodeConfig): "derive from the host".
inline constexpr int32_t kAeronAuto          = NodeConfig::kAeronAuto;
inline constexpr int32_t kWorkerCoreBaseAuto = NodeConfig::kWorkerCoreBaseAuto;

inline constexpr int32_t kTargetSocketBufferBytes  = 16 * 1024 * 1024;
inline constexpr int32_t kMaxDerivedRcvWindowBytes = 4 * 1024 * 1024;
inline constexpr int32_t kLoopbackMtuBytes         = 65504;   ///< AERON_MAX_UDP_PAYLOAD_LENGTH
inline constexpr int32_t kFallbackMtuBytes         = 1408;    ///< Aeron default (unknown interface MTU)
inline constexpr int32_t kAeronFrameAlignment      = 32;
inline constexpr int32_t kMinMtuBytes              = 64;      ///< > Aeron's 32 B data header
inline constexpr int32_t kIpv4UdpHeaderBytes       = 20 + 8;
inline constexpr int32_t kIpv6UdpHeaderBytes       = 40 + 8;

/// What the host allows.  rmemMax / wmemMax are the OS ceilings for SO_RCVBUF /
/// SO_SNDBUF (Linux: /proc/sys/net/core/{r,w}mem_max; macOS: the probed cap,
/// kern.ipc.maxsockbuf-bound).  defaultSnd/RcvBuf: a fresh UDP socket's values
/// (what Aeron validates against when the socket buffer is left at 0 = OS default).
struct HostNetFacts {
    HostPlatform platform      = HostPlatform::Other;
    bool         loopback      = true;    ///< every Aeron endpoint is a loopback address
    bool         ipv6          = false;   ///< non-loopback endpoints are IPv6
    int32_t      interfaceMtu  = 0;       ///< egress interface MTU (0 = unknown)
    int64_t      rmemMax       = 0;
    int64_t      wmemMax       = 0;
    int64_t      defaultSndBuf = 0;
    int64_t      defaultRcvBuf = 0;
};

struct AeronTransportRequest {
    int32_t mtuBytes          = kAeronAuto;
    int32_t socketBufferBytes = kAeronAuto;   ///< 0 = OS default (no setsockopt)
    int32_t rcvWindowBytes    = kAeronAuto;
    int32_t termBufferBytes   = 16 * 1024 * 1024;
};

struct AeronTransportDerived {
    int32_t     mtuBytes          = 0;
    int32_t     socketBufferBytes = 0;
    int32_t     rcvWindowBytes    = 0;
    bool        socketBufferCapped = false;   ///< derived buffer < 16 MiB: OS limit (WARN)
    std::string warnings;                     ///< "" = none; '; '-separated (startup log)
};

/// PURE.  See the header comment for the rules.
[[nodiscard]] AeronTransportDerived deriveAeronTransport(const HostNetFacts& facts,
                                                         const AeronTransportRequest& req);

/// The exact sysctl lines that lift the socket-buffer cap on this platform.
[[nodiscard]] std::string socketBufferSysctlAdvice(HostPlatform platform);

/// "localhost", 127.0.0.0/8, ::1 (and [::1]).  Wildcards (0.0.0.0, ::, "") are NOT
/// loopback — callers decide what a wildcard bind means.
[[nodiscard]] bool isLoopbackHost(std::string_view host) noexcept;
[[nodiscard]] bool isWildcardHost(std::string_view host) noexcept;

/// Per-node shard base ports when the deployment sets none (shard mode is the
/// default): node n gets the block [41000 + 192n, 41000 + 192n + 191] —
/// ingress = base, repl = base + 64, egress control = base + 128 (64 =
/// NodeConfig::kMaxShardWorkers, so co-located nodes never collide).  nullopt when
/// the node id is outside [0, 126] or workers > 64 (explicit ports required).
struct ShardBasePorts { int32_t ingress = 0; int32_t repl = 0; int32_t egressControl = 0; };
inline constexpr int32_t kDerivedShardPortBase   = 41000;
inline constexpr int32_t kDerivedShardPortStride = 3 * NodeConfig::kMaxShardWorkers;
[[nodiscard]] std::optional<ShardBasePorts> deriveShardBasePorts(int32_t nodeId, int32_t workers) noexcept;

/// Worker affinity (= busy-spin, BrokerNode): the bench layout as a host-derived
/// default.  Linux only (affinity is a no-op elsewhere, and an unpinned busy-spin
/// is never wanted).  Core 0 is left to the client / OS / every other thread.
///   co-located (all endpoints loopback): node at index i of the sorted peer list
///     gets cores 1 + i*W .. 1 + i*W + W-1, if 1 + nodes*W <= cores;
///   otherwise (one node per host): cores 1 .. W, if 1 + W <= cores.
/// Returns -1 (unpinned → backoff idle) when the layout does not fit.
[[nodiscard]] int32_t deriveWorkerCoreBase(HostPlatform platform, bool coLocated, int32_t selfIndex,
                                           int32_t nodes, int32_t workers, int32_t cores) noexcept;

/// Applies every host-derived default to `cfg` (idempotent; explicit values win):
/// Aeron mtu / socket buffer / window, shard base ports (self + peers lacking
/// them), the per-worker flags shard mode implies, worker core base.  PURE given
/// `facts` and `cores`.
struct HostDerivation {
    std::string summary;              ///< the one effective-config startup line
    std::string warnings;             ///< "" = none
    bool        socketBufferCapped = false;
};
HostDerivation applyHostDerivedDefaults(NodeConfig& cfg, const HostNetFacts& facts, int32_t cores);

/// Hosts this node's Aeron endpoints use: every peer host (self included) and
/// the client bind host.  A wildcard client bind (0.0.0.0) follows the peers.
[[nodiscard]] std::vector<std::string> nodeEndpointHosts(const NodeConfig& cfg);

/// IMPURE: OS limits + the egress interface MTU for `endpointHosts` (empty =
/// loopback).  Linux reads /proc/sys/net/core/{r,w}mem_max; macOS probes the
/// SO_SNDBUF/SO_RCVBUF cap.  Never throws: unknown facts stay 0 (derivation then
/// uses safe fallbacks).
[[nodiscard]] HostNetFacts probeHostNetFacts(const std::vector<std::string>& endpointHosts);

/// IMPURE: online CPU count (>= 1).
[[nodiscard]] int32_t hostCpuCount() noexcept;

} // namespace rune::node
