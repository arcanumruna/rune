// include/rune/client/shard/ShardConfig.h — client-shard configuration (T15a;
// design note docs/superpowers/specs/2026-10-01-t15-clientshard-design.md §2, §2a, §6).
//
// A shard is one Client (its own embedded driver in invoker mode, Aeron client,
// sessions and egress subscription) plus its Producer, driven by exactly one
// thread.  This header holds everything that is decided BEFORE any shard driver
// starts, all on the control thread (cold path; allocation is fine here):
//
//   resolveShardCount()        AUTO (default) or explicit N — pure, injected CPUs
//   assignShardCores()         optional pinning onto the allowed cores — pure
//   validateShardGroupConfig() egress port range/collisions, maxFrame <= term/8
//   shardClientConfig()        shard i's ClientConfig (dir, port, D1, R1, I4, I5)
//   shardProducerConfig()      shard producer config (Invoker: end-of-pass linger)
//   shardConfigWarnings()      the warnings the group logs once at construction
//   platformAllowedCpus() / platformQuotaCpus() / platformPinningSupported()
//
// Guidance (§2a):
//   * Dedicated box: leave shardCount = AUTO.  A 16-core box with >= 16 partitions
//     gets 16 shard threads; use reservedCores for cores the app or a co-located
//     process needs (they are the LOWEST cores of the calling thread's mask).
//   * Shared box: set shardCount explicitly.  The 4-core bench box with 3
//     co-located brokers uses shardCount = 1, Invoker mode, core 0, mtu 1408
//     (the default; aligned with the broker's interface-derived MTU).
//   * Agent idle modes: the runner's park also delays its driver (D1).  Balanced
//     parks <= 1 ms; LowLatency <= 100 us (client p99 floor ~100 us); BusySpin on
//     a pinned core is the only Agent mode that fits p99 <= 100 us.  N BusySpin
//     runners peg N cores — BusySpin under AUTO without pinning is warned about.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/shard/ShardMap.h>
#include <rune/client/transport/TransportConfig.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace rune::client::shard {

enum class ShardMode : uint8_t {
    Agent,    ///< default: a runner thread per shard (driver pumped by that runner, D1)
    Invoker,  ///< no threads: each app thread constructs and drives its own shard
};

/// shardCount value meaning "derive N from the CPUs and topics" (§2a).
inline constexpr uint32_t kAutoShards = 0;
/// Shard driver term length default (I5): a plain Client keeps 4 MiB.
inline constexpr std::size_t kDefaultShardTermBufferBytes = 1u * 1024 * 1024;
/// "No cgroup CPU quota" (cpu.max "max", no cgroup, or not Linux).
inline constexpr int32_t kNoCpuQuota = 0;

/// Allowed CPUs, ascending and unique.
using CpuSet = std::vector<int32_t>;

struct ShardGroupConfig {
    /// Shard i derives from this: dataDir/shard-i, egress endpoint port + i.  Its
    /// mtuBytes (1408 default, jumbo opt-in) is used as is.
    ClientConfig base;
    /// kAutoShards (default) = min(|allowed|, cgroup quota) - reservedCores, capped
    /// at max(partitionCount) of base.topics, >= 1; 1 when base.topics is empty.
    /// >= 1 = explicit (required in Invoker mode).
    uint32_t     shardCount = kAutoShards;
    ShardMode    mode       = ShardMode::Agent;
    /// Agent: shard i's runner pinned to firstCore + i (each must be allowed).
    int32_t      firstCore  = -1;
    /// Agent, firstCore < 0: pin shard i to the i-th allowed core after the
    /// reserved ones.  Off by default.
    bool         pin        = false;
    /// The lowest reservedCores cores of the calling thread's mask are left out of
    /// AUTO and auto-pinning (the app's main thread, a co-located broker, ...).
    uint32_t     reservedCores = 0;
    ProducerConfig producer = {};
    /// Invoker: replaces producer.lingerUs (spec §4.2 end-of-pass batching, M6).
    int64_t      invokerLingerUs = 0;
    /// Shard driver term length (I5).  maxFrameBytes must be <= term/8.
    std::size_t  shardTermBufferBytes = kDefaultShardTermBufferBytes;
    /// I4: the client and driver share one duty thread, so the liveness timeout
    /// only measures this thread's own stalls: effectively unbounded (1 h).  A
    /// stall then ends in the ordinary path (session expiry -> reconnect).
    /// Overridable; a lower value is warned about.
    int64_t      aeronClientLivenessTimeoutNs = transport::kMaxClientLivenessTimeoutNs;
    /// Test seam (T15b): called on the tearing-down thread just before ShardGroup
    /// tears shard i down (construction failure, close, destructor) — proves the
    /// reverse order.  nullptr = none.
    void (*testTeardownHook)(void* ctx, uint32_t shard) noexcept = nullptr;
    void* testTeardownCtx = nullptr;
    /// Test seam (T15b M3): replaces Key::partition in ShardGroup's routing
    /// (nullptr = none: one predictable branch per send).  Lets a counting
    /// hasher prove the key is hashed once per send and that the shard and the
    /// producer route by the partition they are given.
    int32_t (*testPartitioner)(const Key& key, int32_t partitionCount) noexcept = nullptr;
};

/// A resolved shard count and the one-line reason the group logs.
struct ShardCount {
    uint32_t    count = 1;
    std::string reason;
};

// ---- pure ------------------------------------------------------------------

/// ceil(quotaUs / periodUs) CPUs; kNoCpuQuota when quotaUs <= 0 (cgroup v1 "-1")
/// or periodUs <= 0.
[[nodiscard]] int32_t quotaCpusFromCfs(int64_t quotaUs, int64_t periodUs) noexcept;

/// Parses cgroup v2 cpu.max ("<quota> <period>" or "max <period>").
/// kNoCpuQuota for "max" or anything malformed.
[[nodiscard]] int32_t parseCgroupV2CpuMax(std::string_view text) noexcept;

/// N, resolved once on the control thread before any driver starts (§2a):
///   explicit shardCount >= 1      -> shardCount
///   AUTO, Invoker mode            -> std::invalid_argument (N = number of app
///                                    threads, which only the app knows)
///   AUTO, no topics               -> 1 ("no topics known at construction; AUTO → 1")
///   AUTO                          -> max(1, min(min(|allowed|, quota) - reserved,
///                                                max(partitionCount)))
[[nodiscard]] ShardCount resolveShardCount(const ShardGroupConfig& g, const CpuSet& allowed,
                                           int32_t quotaCpus);

/// Per-shard runner core (-1 = not pinned), in shard order:
///   pinning unsupported and (pin || firstCore >= 0) -> std::invalid_argument
///   Invoker and (pin || firstCore >= 0)             -> std::invalid_argument (the
///                                                      app pins its own threads)
///   firstCore >= 0  -> firstCore + i, each in `allowed`, else std::invalid_argument
///                      listing the allowed cores
///   pin             -> allowed[reservedCores + i]; count must fit
///   otherwise       -> all -1
[[nodiscard]] std::vector<int32_t> assignShardCores(const ShardGroupConfig& g, uint32_t count,
                                                    const CpuSet& allowed, bool pinningSupported);

/// Checks, before any driver starts, that N shards can be built (throws
/// std::invalid_argument naming the shard and the cause):
///   count >= 1; every derived egress port fits (base + N - 1 <= 65535) and none
///   equals the gateway endpoint (same host, localhost == 127.0.0.1); the
///   transport config AFTER the RUNE_* env overrides passes transport::validate;
///   producer.maxFrameBytes <= term / 8; the receive window <= term / 2.
void validateShardGroupConfig(const ShardGroupConfig& g, uint32_t count);

/// Warnings to log once at construction (empty = none).
[[nodiscard]] std::vector<std::string> shardConfigWarnings(const ShardGroupConfig& g, uint32_t count);

/// Shard `index` of `count`: dataDir/shard-i, egress port + i, runnerMode from
/// mode, runnerCore = core (Agent) / -1 (Invoker), driverOnDutyThread = true (D1),
/// reconnectForeverAfterLive = true (R1), termBufferBytes = shardTermBufferBytes,
/// aeronClientLivenessTimeoutNs from the group (I4).  mtuBytes is base's.
[[nodiscard]] ClientConfig shardClientConfig(const ShardGroupConfig& g, uint32_t index, uint32_t count,
                                             int32_t runnerCore);

/// Convenience for an explicit shardCount: the runner core is
/// assignShardCores(g, shardCount, platformAllowedCpus(), platformPinningSupported())
/// [index] — the same pin / firstCore rules and rejections as a group.
/// AUTO -> std::invalid_argument (resolve first).
[[nodiscard]] ClientConfig shardClientConfig(const ShardGroupConfig& g, uint32_t index);

/// The shard producer's config: Invoker -> lingerUs = invokerLingerUs.
[[nodiscard]] ProducerConfig shardProducerConfig(const ShardGroupConfig& g);

// ---- platform (control thread, before any shard thread exists) -------------

/// Linux: the calling thread's sched_getaffinity mask.  macOS: hw.activecpu,
/// as cores 0 .. n-1 (no affinity API).  Never empty (falls back to {0}).
[[nodiscard]] CpuSet platformAllowedCpus();

/// Linux: cgroup v2 /sys/fs/cgroup/cpu.max, else cgroup v1 cpu.cfs_quota_us /
/// cpu.cfs_period_us.  kNoCpuQuota when unlimited, absent, or not Linux.
[[nodiscard]] int32_t platformQuotaCpus();

/// True where a thread can be pinned (Linux).
[[nodiscard]] constexpr bool platformPinningSupported() noexcept {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

} // namespace rune::client::shard
