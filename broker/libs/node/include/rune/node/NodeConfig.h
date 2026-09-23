#pragma once
// NodeConfig.h — env-driven broker node configuration (BR-1, BR-3b, GB-1, GB-4)
//
// Environment variables:
//   RUNE_NODE_ID           — int32, this node's id. Required. Must equal some peer's id.
//   RUNE_PEERS             — comma-separated id@host:consensusPort[:clientPort[:replPort]] entries.
//                            Required, non-empty. Ids must be unique and include RUNE_NODE_ID.
//                            Example (with all ports): "0@rune-1:40111:40200:40300,1@rune-2:40111:40200:40300,2@rune-3:40111:40200:40300"
//                            Example (without client/repl ports): "0@rune-1:40111,1@rune-2:40111,2@rune-3:40111"
//                            Backward-compatible: omitting :clientPort → clientPort=0; omitting :replPort → replPort=0.
//                            Whitespace around commas and entries is trimmed.
//   RUNE_CLIENT_ENDPOINT   — host:port for the client gateway (Required).
//                            Built into "aeron:udp?endpoint=host:port".
//   RUNE_CLIENT_STREAM_ID  — int32, default 1.
//   RUNE_CONSENSUS_STREAM_ID — int32, default 10.
//   RUNE_REPLICATION_STREAM_ID — int32, default 20. Stream for per-partition replication.
//   RUNE_DATA_DIR          — filesystem path for durable data. Required, non-empty.
//   RUNE_TOPICS            — comma-separated name:partitionCount topic declarations (GB-1).
//                            Optional (absent or empty → no topics; produce/subscribe
//                            will return UnknownTopic).
//                            Example: "inventory:8,entities:4,chat:1"
//                            Names must be unique, non-empty; partitionCount >= 1.
//                            topicId is the 0-based index in declaration order.
//
// Election timing (all optional; defaults are fast-but-safe for co-located clusters):
//   RUNE_ELECTION_TIMEOUT_MS        — int64 ms, default 150. Max wait per election round.
//   RUNE_ELECTION_STATUS_INTERVAL_MS — int64 ms, default 20. Canvass/heartbeat cadence.
//   RUNE_STARTUP_CANVASS_TIMEOUT_MS — int64 ms, default 100. Extended canvass on first boot.
//   RUNE_MIN_ISR                    — int32, minimum ISR size for acks=All gate. Default:
//                                     majority = |peers|/2+1 (e.g. 2 for 3 nodes, 1 for 1 node).
//
// For cross-datacenter deployments set these higher (e.g. 1000/200/2000 ms).
//
// All Aeron channels use the aeron:udp transport (container transport).

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// PersistenceMode — controls the log backend for a topic's partitions.
// ---------------------------------------------------------------------------
enum class PersistenceMode : uint8_t {
    Disk   = 0,   ///< On-disk Log (default). Survives node restarts.
    Memory = 1,   ///< In-memory InMemoryLog. Zero-disk, zero-recovery (data lost on restart).
                  ///  A memory topic that must survive node deaths MUST use a durable
                  ///  mode (majority or all-in-sync) so data is replicated to other nodes.
};

// ---------------------------------------------------------------------------
// TopicSpec — one declared topic (GB-1).
// topicId is the 0-based declaration index assigned by parseNodeConfig.
// ---------------------------------------------------------------------------
struct TopicSpec {
    std::string name;
    int32_t     partitionCount = 1;
    int32_t     topicId        = -1;
    enum class CleanupPolicy : uint8_t { Delete = 0, Compact = 1, CompactDelete = 2 };
    CleanupPolicy cleanupPolicy           = CleanupPolicy::Delete;
    int64_t       retentionMs             = -1;             // -1 = unbounded
    int64_t       retentionBytes          = -1;             // -1 = unbounded
    int64_t       deleteRetentionMs       = 86'400'000;     // tombstone retention (1 day)
    double        minCleanableDirtyRatio  = 0.5;            // dirty-ratio compaction trigger
    // Segment roll size in bytes.  Default 64 MiB matches Kafka's production
    // segment.bytes.  Tests may set a small value to form multiple sealed
    // segments quickly (e.g. to exercise compaction end-to-end).
    int64_t       segmentRollBytes        = 64LL * 1024 * 1024;

    // CP-8: compaction lag gates.
    // minCompactionLagMs: a dirty segment is not eligible while
    //   segment.largestTimestamp > nowMs - minCompactionLagMs.
    //   Default 0 (no minimum lag) — same as Kafka.
    int64_t       minCompactionLagMs      = 0;
    // maxCompactionLagMs: when the oldest dirty segment's age exceeds this,
    //   force needCompactionNow (eligible even below minCleanableDirtyRatio).
    //   Default -1 = disabled — same as Kafka's Long.MAX_VALUE.
    int64_t       maxCompactionLagMs      = -1;

    // CP-8: shared I/O throttler budget (bytes/sec across all cleaner threads).
    //   Default -1 = unlimited — same as Kafka.
    int64_t       ioMaxBytesPerSecond     = -1;

    // CP-9: retention scheduler cadence + async-delete delay.
    // retentionCheckMs: how often the cleaner runs retention (ms).
    //   Default 300000 (5 min) = Kafka default.  Tests set 1.
    int64_t       retentionCheckMs        = 300'000LL;
    // fileDeleteDelayMs: delay (ms) before .deleted files are physically removed.
    //   Default 60000 (1 min) = Kafka default.  Tests set 1.
    int64_t       fileDeleteDelayMs       = 60'000LL;
    // logStartOffset: delete segments entirely below this offset.  0 = disabled.
    int64_t       logStartOffset          = 0;

    // Task M1: co-partition group and partition key for co-located topics.
    // coPartitionGroup: if non-empty, this topic shares its partition assignment with
    //   all topics in the same group (balancer ensures matching partition→node mapping).
    // partitionKey: if non-empty, overrides the hash key used for partition routing.
    std::string   coPartitionGroup;
    std::string   partitionKey;

    // Task M1: persistence and durability modes.
    // persistence: selects the log backend (Disk = default, Memory = InMemoryLog).
    // durability: 0 = Majority (default), 1 = AllInSync.  Forwarded to PartitionConfig.
    //   Memory topics that must survive a node death MUST use majority or all-in-sync
    //   durability so the data is replicated to peer nodes before acknowledgement.
    PersistenceMode persistence = PersistenceMode::Disk;
    uint8_t         durability  = 0;   // 0 = Majority, 1 = AllInSync
};

struct PeerConfig {
    int32_t  id            = -1;
    std::string host;
    uint16_t consensusPort    = 0;
    uint16_t clientPort       = 0;  // 0 = no known client endpoint (GB-4, backward-compatible)
    uint16_t replicationPort  = 0;  // 0 = no replication endpoint (GR-3, backward-compatible)

    /// "aeron:udp?endpoint=<host>:<consensusPort>"
    std::string consensusChannel() const;

    /// "aeron:udp?endpoint=<host>:<clientPort>" — empty string if clientPort == 0.
    std::string clientChannel() const;

    /// "aeron:udp?endpoint=<host>:<replicationPort>" — empty string if replicationPort == 0.
    std::string replicationChannel() const;
};

struct ClientEndpoint {
    std::string channel;   // "aeron:udp?endpoint=<host>:<port>"
    int32_t     streamId = 0;
};

struct NodeConfig {
    int32_t             nodeId          = -1;
    std::vector<PeerConfig> peers;       // includes self; sorted by id
    ClientEndpoint      clientEndpoint;
    std::string         dataDir;
    int32_t             consensusStreamId    = 0;
    int32_t             replicationStreamId  = 20;  // GR-3: stream for per-partition replication

    // Topics declared via RUNE_TOPICS (GB-1).
    // Empty when RUNE_TOPICS is absent or empty — valid state (no topics).
    // topicId == index in this vector.
    std::vector<TopicSpec> topics;

    // Election timing — configurable via env (BR-3b).
    // Defaults are fast-but-safe for co-located / test clusters.
    // Increase for cross-datacenter deployments.
    int64_t electionTimeoutMs        = 150;  // default 150 ms
    int64_t electionStatusIntervalMs =  20;  // default  20 ms
    int64_t startupCanvassTimeoutMs  = 100;  // default 100 ms

    // Replication lag tolerance — GR-5.
    // After a follower hasn't fetched for replicaLagMaxMs, it is shrunk from the ISR.
    // Default 10 000 ms (10 s) matches the PartitionConfig default.
    // Tests may set a lower value (e.g. 500 ms) for faster ISR shrink in kill scenarios.
    int64_t replicaLagMaxMs = 10'000;

    // Minimum ISR size gate for acks=All produces — GR-5 [M1].
    // Default: cluster majority (|peers|/2 + 1), so a 3-node cluster requires 2.
    // Set to 1 for single-node deployments (leader-only ISR is the whole cluster).
    // Overridable at runtime via RUNE_MIN_ISR env var.
    // Also forwarded to PartitionConfig::minIsr in buildReplication().
    int32_t gatewayMinIsr = 1;

    // Milestone E1: number of in-process partition workers. Each owned shard is
    // assigned to worker = hash(shardId) % workerCount (ShardRouter). Default 1
    // (single-owner, identical to the pre-E1 single-agent behaviour). Overridable
    // via RUNE_WORKERS. Floored to 1.
    int32_t workerCount = 1;

    // Milestone E1 / E2: per-worker idle strategy selector.
    // E1 does NOT use this (no per-worker AgentRunner yet — workers are driven
    // cooperatively on the single AgentRunner). E2 will give each worker its own
    // AgentRunner and read this field to select the IdleStrategy.
    //
    // Valid values (parsed from RUNE_WORKER_IDLE env var, case-insensitive):
    //   "backoff"   — BackoffIdleStrategy (default; safe for shared cores).
    //   "busyspin"  — BusySpinIdleStrategy (pegs a core; use only on dedicated CPU).
    //   "yielding"  — YieldingIdleStrategy (yield-loop; moderate CPU usage).
    //   "sleeping"  — SleepingIdleStrategy (OS sleep; lowest CPU, highest latency).
    // Any unrecognised value is floored to "backoff" (must NOT default to busy-spin).
    enum class WorkerIdleStrategy : uint8_t {
        Backoff   = 0,  ///< BackoffIdleStrategy — DEFAULT. Must not peg a core.
        BusySpin  = 1,  ///< BusySpinIdleStrategy — lowest latency; pegs core.
        Yielding  = 2,  ///< YieldingIdleStrategy — moderate CPU.
        Sleeping  = 3,  ///< SleepingIdleStrategy — lowest CPU, highest latency.
    };
    WorkerIdleStrategy workerIdleStrategy = WorkerIdleStrategy::Backoff;

    // Shared-nothing conformance ([[rune-shared-nothing-law]]): all cross-thread
    // communication is via agrona ring buffers; their sizes are tunable per
    // deployment (no hardcoded ring capacities). Powers of two.
    //   - replicationOutboundRingBytes: payload bytes of the transport's outbound
    //     ManyToOne ring (worker sends → control-thread drain). RUNE_REPL_OUTBOUND_RING_BYTES.
    //   - workerRingCapacity: slot count of each per-worker ingress/return SPSC
    //     ring (inc2a). RUNE_WORKER_RING_CAPACITY.
    int32_t replicationOutboundRingBytes = 4 * 1024 * 1024;
    int32_t workerRingCapacity           = 256;

    // CP-8: enable segment coalescing in the LogCleanerAgent.
    // When true (default), consecutive sealed segments that together fit within
    // each topic's segmentRollBytes are merged into a single output segment per
    // compaction pass (Kafka parity).  When false, each segment is compacted
    // individually (prior behavior — useful for diagnostics or rollback).
    // Overridable via RUNE_ENABLE_COALESCING env var ("0" or "false" → off).
    bool enableCompactionCoalescing = true;

    // Segment roll size (bytes) for the internal __consumer_offsets log.
    // Default 64 MiB matches TopicSpec::segmentRollBytes.
    // Tests may set a small value (e.g. 128) to force multiple sealed segments
    // so LogCleanerAgent can run compaction on the offset log.
    int64_t offsetLogSegmentRollBytes = 64LL * 1024 * 1024;

    /// Returns the peer entry whose id == nodeId.
    /// Precondition: parse succeeded (peers non-empty, nodeId in peers).
    const PeerConfig& self() const;

    bool isSingleNode() const { return peers.size() == 1; }
};

struct ConfigResult {
    std::optional<NodeConfig> config;
    std::string               error;
};

/// Parse and validate node configuration from an injected env-lookup function.
/// Use this overload in tests (inject a fake env map) so tests never touch
/// the real process environment.
ConfigResult parseNodeConfig(
    const std::function<const char*(const char*)>& getenv_fn);

/// Convenience wrapper that delegates to ::getenv for production use.
ConfigResult parseNodeConfigFromEnv();

} // namespace rune::node
