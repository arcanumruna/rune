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
//   rcvWindow    = pow2floor(min(4 MiB, kernelRcvBuf/4, term/4)); kernelRcvBuf =
//                  2 × socketBuffer on Linux (the kernel doubles SO_RCVBUF), 1× else
//                  (64 KiB datagrams: kernel truesize makes a window near the
//                  real SO_RCVBUF drop packets — a 16 MiB window on a 16 MiB
//                  sysctl measured 98 % drops; 4 MiB measured clean)
//   mtu          = 65504 when every endpoint datagrams go to is loopback (the
//                  peers + the advertised client endpoint = the peer host; a
//                  wildcard client BIND adds nothing),
//                  else interface MTU − IP/UDP headers, aligned down to 32; then
//                  clamped ≤ socketBuffer (aeron_driver_init EINVALs when mtu >
//                  SO_SNDBUF — the macOS lesson), ≤ the receiver's effective window
//                  min(window, term/2) and (derived only) ≤ window/4 (≥ 4 datagrams
//                  in flight — a deliberate addition to findings §4: it only binds
//                  on hosts with small socket limits).
//   Explicit values are kept but CLAMPED (with a WARN) where they would make
//   aeron_driver_init / the receive endpoint fail: mtu ≤ SO_SNDBUF cap and ≤
//   min(window, term/2); window ≤ SO_RCVBUF cap; an explicit socket buffer above
//   the OS limit is clamped to it.  A config can never reach the driver EINVAL.
//
// Term buffers are non-sparse (findings §4) only when the Aeron dir is on tmpfs;
// on a disk-backed dir, pre-allocating 3 × 16 MiB per publication would zero-fill
// to disk on the (fused: hot) conductor thread, so they stay sparse there.
//
// Derived ports/cores carry the usual caveat: 41000–65383 lie inside the Linux and
// macOS ephemeral ranges (as do Rune's other default ports), so a rare ephemeral
// socket can win a bind; production deployments pin explicit ports.
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
    /// CPUs this process may run on (Linux sched_getaffinity; empty = unknown →
    /// never auto-pin).  Sorted ascending.
    std::vector<int32_t> allowedCpus;
    /// The Aeron dir (or its nearest existing ancestor) is on tmpfs.
    bool         aeronDirTmpfs = false;
    bool         loopback      = true;    ///< every Aeron endpoint is a loopback address
    bool         ipv6          = false;   ///< non-loopback endpoints are IPv6
    int32_t      interfaceMtu  = 0;       ///< egress interface MTU (0 = unknown)
    int64_t      rmemMax       = 0;
    int64_t      wmemMax       = 0;
    int64_t      defaultSndBuf = 0;
    int64_t      defaultRcvBuf = 0;
    bool         udpGso        = false;   ///< kernel accepts UDP_SEGMENT (rune_gso bindings: GSO send)
    bool         udpGro        = false;   ///< kernel accepts UDP_GRO (rune_gso bindings: GRO receive)
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

/// Worker affinity (cache locality; idle = backoff unless opted in): the bench layout as a host-derived
/// default.  Only where every node is VISIBLE on this host — all peers loopback
/// (a co-located cluster, the bench): node at index i of the sorted peer list gets
/// the allowed CPUs [1 + i*W, 1 + i*W + W) of the process affinity mask (index 0
/// is left to the client / OS / every other thread), if those W CPUs exist and are
/// consecutive.  Linux only (affinity is a no-op elsewhere).  Non-loopback
/// deployments return -1: containers sharing one VM look exactly like dedicated
/// hosts, and auto-pinning them would stack several nodes on one core — pin
/// explicitly (RUNE_WORKER_CORE_BASE).  Returns -1 (unpinned) whenever the layout
/// does not fit.
[[nodiscard]] int32_t deriveWorkerCoreBase(HostPlatform platform, bool allLoopback, int32_t selfIndex,
                                           int32_t nodes, int32_t workers,
                                           const std::vector<int32_t>& allowedCpus) noexcept;

/// Non-sparse terms only on tmpfs (see header).  `explicitSparse` wins.
[[nodiscard]] constexpr bool deriveTermSparse(std::optional<bool> explicitSparse, bool aeronDirTmpfs) noexcept {
    return explicitSparse ? *explicitSparse : !aeronDirTmpfs;
}

/// Applies every host-derived default to `cfg` (idempotent; explicit values win):
/// Aeron mtu / socket buffer / window, shard base ports (self + peers lacking
/// them), the per-worker flags shard mode implies, worker core base.  PURE given
/// `facts` and `cores`.
struct HostDerivation {
    std::string summary;              ///< the one effective-config startup line
    std::string warnings;             ///< "" = none
    bool        socketBufferCapped = false;
};
HostDerivation applyHostDerivedDefaults(NodeConfig& cfg, const HostNetFacts& facts,
                                        std::string_view ioBackend = "auto");

/// Hosts this node's datagrams go to: every peer host (self included — clients
/// are told the peer host) and a concrete client bind host.  A wildcard bind
/// (0.0.0.0) adds nothing: the peers decide.
[[nodiscard]] std::vector<std::string> nodeEndpointHosts(const NodeConfig& cfg);

/// IMPURE: OS limits, the egress interface MTU for `endpointHosts` (empty =
/// loopback; a wildcard host = the default route's interface), the process CPU
/// affinity mask and whether `aeronDir` is on tmpfs.  Linux reads
/// /proc/sys/net/core/{r,w}mem_max; macOS probes the SO_SNDBUF/SO_RCVBUF cap.
/// Never throws: unknown facts stay 0 / empty (derivation then uses safe fallbacks).
[[nodiscard]] HostNetFacts probeHostNetFacts(const std::vector<std::string>& endpointHosts,
                                             const std::string& aeronDir = {});

} // namespace rune::node
