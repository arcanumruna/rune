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
//   RUNE_WORKER_INGRESS_BASE_STREAM — int32, default clientStreamId+1. Per-worker ingress stream id base (E5.1).
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
// E5.2: Aeron media-driver threading mode (optional):
//   RUNE_AERON_THREADING_MODE — "shared" (default) or "dedicated" (case-insensitive).
//                               Any unrecognised value floors to "shared".
//   RUNE_AERON_DRIVER_IDLE    — idle strategy string for the driver thread(s);
//                               default "sleep-ns" (identical to today's hard-coded value).
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

    // E3 Task 7: async-durability tier (orthogonal to the replication-quorum `durability`
    // field above).  Controls WHEN the worker fires the produce ack relative to local
    // fsync.  Values match the design-decision §2 tiers:
    //   0 = Leader  — ack immediately after append (today's default behaviour).
    //   1 = Majority — ack after HW advance (today's Majority path, gateway-side gate). [DEFAULT]
    //   2 = Strict  — ack only after local fsync completion AND (if acks=All) HW advance.
    // Default Majority (1) so all existing tests are parity-unchanged.
    uint8_t         durabilityTier = 1;  // 0=Leader, 1=Majority, 2=Strict
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

    // Idle strategy for the RUNTIME agent thread (consensus + ClientGateway on one
    // thread).  Parsed from RUNE_RUNTIME_IDLE (same values as RUNE_WORKER_IDLE).
    // Default Backoff (safe, no core pegging).  Set to "busyspin" for the lowest
    // produce/ack round-trip latency (the gateway thread never parks) — sub-100µs
    // territory — at the cost of pegging that core.  Use on a dedicated CPU.
    WorkerIdleStrategy runtimeIdleStrategy = WorkerIdleStrategy::Backoff;

    // E4: per-worker CPU affinity base core (Linux-only; macOS is a no-op).
    //
    // When >= 0, worker i pins its own AgentRunner thread to core
    // (workerCoreBase + i) on the first doWork call (self-pin on entry).
    //
    // -1 (default) = affinity DISABLED — no pinning, current behaviour preserved.
    //   All existing tests are parity-unchanged at the default.
    //
    // Example: with workerCoreBase=2 and workerCount=4, workers pin to cores
    //   2, 3, 4, 5 — reserving cores 0 and 1 for the control thread, Aeron
    //   media driver, and kernel housekeeping (see AffinityConfig in the E4 spec).
    //
    // Overridable via RUNE_WORKER_CORE_BASE env var.
    //   Absent or empty → -1 (disabled).
    //   Any value < -1   → -1 (clamped to disabled).
    int32_t workerCoreBase = -1;

    // E5.1: per-worker Aeron ingress stream id base.
    //
    // Each partition's ingressStreamId in the MetadataResponse is computed as:
    //   workerIngressBaseStreamId + ShardRouter.workerForPartition(topicId, partition)
    //
    // This allows the client to connect to the owning worker's dedicated ingress
    // stream rather than the gateway's shared stream — the foundation of per-worker
    // ingress routing (Task E5.1.2 wires the actual Aeron subscription).
    //
    // Choice: base stream id defaults to clientEndpoint.streamId + 1 (i.e. 2 when
    // RUNE_CLIENT_STREAM_ID is the default 1).  That keeps the gateway stream (1)
    // and the first worker stream (2) non-overlapping without requiring the deployer
    // to set an env var for single-worker configurations.
    //
    // With W workers the allocated range is [base, base+W-1], so deployers with
    // a custom RUNE_CLIENT_STREAM_ID (e.g. 10) should set
    //   RUNE_WORKER_INGRESS_BASE_STREAM = 11
    // to keep the streams consecutive and non-overlapping.
    //
    // Overridable via RUNE_WORKER_INGRESS_BASE_STREAM (parseInt32).
    // The default (0) signals "use clientEndpoint.streamId + 1"; resolved in
    // parseNodeConfig after clientEndpoint.streamId is known.
    int32_t workerIngressBaseStreamId = 0;  // 0 = unset; resolved to clientStreamId+1

    // read-rework T3/T4: per-worker dynamic-MDC egress publication config.
    //
    // Each worker owns ONE egress ExclusivePublication on
    //   channel  = aeron:udp?control-mode=dynamic|control=<host>:<CTRL_BASE+workerId>|fc=min
    //   streamId = workerEgressBaseStreamId + workerId
    // Consumers self-register by subscribing to that control address (dynamic MDC),
    // so the worker never manages the destination list and the control thread never
    // touches the publication (shared-nothing).
    //
    //   - workerEgressControlHost: host the broker binds/advertises for MDC control.
    //     Default "127.0.0.1" (single-box bench/tests); deployments override via
    //     RUNE_WORKER_EGRESS_CONTROL_HOST with the broker's reachable address.
    //   - workerEgressControlBasePort: base UDP control port; worker W uses
    //     base + W.  RUNE_WORKER_EGRESS_CONTROL_BASE_PORT.  Must not collide with
    //     client/consensus/replication ports.
    //   - workerEgressBaseStreamId: 0 = unset; resolved in buildWorkerPool to
    //     workerIngressBaseStreamId + workerCount (keeps ingress/egress stream
    //     ranges non-overlapping).  RUNE_WORKER_EGRESS_BASE_STREAM.
    std::string workerEgressControlHost     = "127.0.0.1";
    int32_t     workerEgressControlBasePort = 40300;
    int32_t     workerEgressBaseStreamId    = 0;  // 0 = unset; resolved in buildWorkerPool

    // Shared-nothing conformance ([[rune-shared-nothing-law]]): all cross-thread
    // communication is via agrona ring buffers; their sizes are tunable per
    // deployment (no hardcoded ring capacities). Powers of two.
    //   - replicationOutboundRingBytes: payload bytes of the transport's outbound
    //     ManyToOne ring (worker sends → control-thread drain). RUNE_REPL_OUTBOUND_RING_BYTES.
    //   - workerIngressRingBytes: byte capacity of each per-worker ingress SPSC
    //     ring (power of two). RUNE_WORKER_INGRESS_RING_BYTES. Default 32 MiB.
    //   - workerReturnRingBytes: byte capacity of each per-worker return SPSC
    //     ring (power of two). RUNE_WORKER_RETURN_RING_BYTES. Default 32 MiB.
    //   - ioSubmitRingDepth: slot depth of each worker's IoBackend submission ring.
    //     RUNE_IO_SUBMIT_RING_DEPTH. Default 1024 (matches Worker::kIoSubmitDepth).
    //   - ioCompletionRingDepth: slot depth of each worker's IoBackend completion ring.
    //     RUNE_IO_COMPLETION_RING_DEPTH. Default 1024 (matches Worker::kIoCompletionDepth).
    //   - memoryLogRingBytes: byte capacity of each InMemoryLog single-owner ring.
    //     RUNE_MEMORY_LOG_RING_BYTES. Default 8 MiB.
    int32_t replicationOutboundRingBytes = 4 * 1024 * 1024;
    int32_t workerIngressRingBytes       = 32 * 1024 * 1024;
    int32_t workerReturnRingBytes        = 32 * 1024 * 1024;
    int32_t ioSubmitRingDepth            = 1024;
    int32_t ioCompletionRingDepth        = 1024;
    int32_t memoryLogRingBytes           = 8 * 1024 * 1024;
    //   - aeronTermBufferBytes: Aeron term buffer length (bytes) for the embedded
    //     media driver — sizes every publication/subscription log buffer.  Power
    //     of two in [64 KiB, 1 GiB].  Default 4 MiB; deployments raise it (e.g.
    //     32 MiB via RUNE_AERON_TERM_BUFFER_BYTES) for deep in-flight throughput.
    int32_t aeronTermBufferBytes         = 4 * 1024 * 1024;
    //   - aeronMtuBytes: Aeron UDP MTU (bytes) for the embedded media driver.
    //     RUNE_AERON_MTU.  Not required to be a power of two.  Default 1408
    //     (Aeron's own AERON_MTU_LENGTH_DEFAULT — the max that fits the default
    //     socket SO_SNDBUF on all platforms).
    //
    //     WHY NOT jumbo (65504): aeron_driver_init() rejects with EINVAL(22) when
    //     mtu_length > the probed socket SO_SNDBUF max (aeron_driver.c:451).  On
    //     macOS (and any host with the default ~64 KiB SO_SNDBUF cap) a 65504-byte
    //     MTU fails driver startup outright — every real embedded-driver test
    //     (GR-7 / TX-2b failover) crashed at init.  Deployments that raise
    //     SO_SNDBUF (net.inet.udp.maxdgram / net.core.wmem_max) and want jumbo
    //     frames for deep batched produces can set RUNE_AERON_MTU=65504.
    int32_t aeronMtuBytes                = 1408;
    //   - gatewayFragmentLimit: maximum fragments polled per ClientGateway
    //     doWork() cycle (RUNE_GATEWAY_FRAGMENT_LIMIT).  Not required to be a
    //     power of two — parsed with parseInt32.  Default 256 (raised from the
    //     old hard-coded 10 to prevent head-of-line blocking on batched ingress).
    //     Also reused for per-worker ingress poll (Task E5.1.2).
    int32_t gatewayFragmentLimit         = 256;
    //   - aeronRcvWindowBytes: Aeron receiver flow-control initial window (bytes).
    //     RUNE_AERON_RCV_WINDOW.  Must be a power of two.  Default 128 KiB
    //     (128 * 1024); raise to e.g. 4 MiB on high-throughput deployments to
    //     avoid receiver window throttling on large batched produces.
    int32_t aeronRcvWindowBytes          = 128 * 1024;

    //   - aeronSocketBufferBytes: OS UDP socket SO_SNDBUF / SO_RCVBUF (bytes) for
    //     the embedded media driver's sockets.  RUNE_AERON_SOCKET_BUFFER_BYTES.
    //     Larger socket buffers absorb produce/ack bursts on loopback/LAN so the
    //     kernel does not drop datagrams under load (which would otherwise force
    //     Aeron NAK/retransmit or app-level retransmit).  0 = leave Aeron's default.
    //     Default 2 MiB.  (Task 6: transport reliability hardening.)
    int32_t aeronSocketBufferBytes       = 2 * 1024 * 1024;

    // E5.2: Aeron media-driver threading mode.
    //
    // Controls how the embedded Aeron media driver allocates its internal
    // threads for the conductor, sender, and receiver roles.
    //
    // Valid values (parsed from RUNE_AERON_THREADING_MODE env var,
    // case-insensitive):
    //   "shared"    — SHARED (default): conductor + sender + receiver share one
    //                 driver thread.  Lowest overhead; good for co-located
    //                 clusters and test environments.
    //   "dedicated" — DEDICATED: conductor, sender, and receiver each get their
    //                 own thread (3 extra threads total).  Better throughput when
    //                 spare cores are available; the main reason to benchmark.
    // Any unrecognised value is floored to "shared" (must NOT change default).
    //
    // Note: on macOS the driver runs in-process; DEDICATED is fully supported
    // but may oversubscribe if fewer than 4 spare cores are available.
    enum class AeronThreadingMode : uint8_t {
        Shared    = 0,  ///< SHARED — DEFAULT.  Single fused driver thread.
        Dedicated = 1,  ///< DEDICATED — 3 separate driver threads (conductor/sender/receiver).
    };
    AeronThreadingMode aeronThreadingMode = AeronThreadingMode::Shared;

    // E5.2: driver-idle strategy string applied to whichever idle setter(s)
    // the active threading mode uses (RUNE_AERON_DRIVER_IDLE, default "sleep-ns").
    //   Shared    mode: passed to aeron_driver_context_set_shared_idle_strategy.
    //   Dedicated mode: passed to conductor / sender / receiver idle setters.
    // Any unrecognised value is passed through to Aeron (it will use its own
    // default on error — safe degradation).  Default "sleep-ns" is identical
    // to today's hard-coded value so unset env → byte-identical behaviour.
    std::string driverIdleStrategy = "sleep-ns";

    // E5.1.3: per-worker Aeron ingress flag.
    //
    // When true, BrokerNode::buildWorkerPool creates one WorkerIngress
    // subscription per worker on stream (workerIngressBaseStreamId + workerId)
    // and calls worker.setWorkerIngress(ingress, /*active=*/true).
    //
    // The gateway produce fanout (tryEnqueueOp for produce) is gated on
    // !perWorkerIngressEnabled so that produce is NOT double-processed.
    //
    // DEFAULT: false — the gateway fanout remains the live production path.
    // The production flip to true requires the client to publish to per-worker
    // streams (E5.1.4) and must not happen before that.
    //
    // Tests that prove the per-worker path end-to-end set this flag ON via
    // NodeConfig directly (without an env var lookup) to keep the default safe.
    //
    // Overridable via RUNE_PER_WORKER_INGRESS env var ("1" or "true" → true).
    bool perWorkerIngressEnabled = false;

    // read-rework T3/T4: per-worker dynamic-MDC EGRESS flag.
    //
    // When true, BrokerNode::buildWorkerPool creates one dynamic-MDC WorkerEgress
    // per worker and the gateway ROUTES Subscribe/Unsubscribe to the owning worker
    // (via the ingress ring) instead of driving the push itself — each worker reads
    // + encodes + pushes its owned partitions directly (shared-nothing, parallel).
    // The gateway's own pushSubscriptions skips user partitions (topicId >= 0) when
    // this is on; internal partitions (topicId < 0) stay gateway-pushed.
    //
    // DEFAULT: false — the gateway push remains the live path.  The production flip
    // to true REQUIRES the client to subscribe to per-worker MDC control endpoints
    // (I4) and must not happen before that.  Independent of perWorkerIngressEnabled.
    //
    // Overridable via RUNE_PER_WORKER_EGRESS env var ("1" or "true" → true).
    bool perWorkerEgressEnabled = false;

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
