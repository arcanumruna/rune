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
//   RUNE_CATALOG_REPL_STREAM_ID — int32, default 30. Stream of the __catalog replication plane
//                            (distinct from the replication stream and the per-worker range).
//   RUNE_DATA_DIR          — filesystem path for durable data. Required, non-empty.
//   RUNE_STORAGE_FORMAT    — new | join | auto; read ONLY when RUNE_DATA_DIR is empty and not formatted.
//                            Unset / "" = never format: an empty data dir refuses to start (exit 78).
//   RUNE_STORAGE_RECOVERY_VOTE — bool, default off.  Unsafe, disaster recovery only.
//
// Election timing (all optional; defaults are fast-but-safe for co-located clusters):
//   RUNE_ELECTION_TIMEOUT_MS        — int64 ms, default 150. Max wait per election round.
//   RUNE_ELECTION_STATUS_INTERVAL_MS — int64 ms, default 20. Canvass/heartbeat cadence.
//   RUNE_STARTUP_CANVASS_TIMEOUT_MS — int64 ms, default 100. Extended canvass on first boot.
//   RUNE_MIN_ISR                    — int32, minimum ISR size for acks=All gate. Default:
//                                     majority = |peers|/2+1 (e.g. 2 for 3 nodes, 1 for 1 node).
//
//   RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND — int64 > 0, the log cleaner's shared I/O throttle. Default unlimited.
//
// Consumer groups (all optional):
//   RUNE_GROUP_MAX_GROUPS           — int32 > 0, the most consumer groups this broker coordinates at once (#214).
//                                     Default 10000.  A JoinGroup that would create one more group is refused.
//   RUNE_GROUP_MIN_SESSION_TIMEOUT_MS — int32 ms > 0, default 1000 (#499).  A smaller JoinGroup session timeout is
//                                     raised to it.
//   RUNE_GROUP_MAX_SESSION_TIMEOUT_MS — int32 ms > 0, default 1800000 (#213 / #499).  A larger one is lowered to it.
//                                     A min above the max (an unset one is its default) refuses startup.
//
// Namespaces (all optional, namespaces Task 7):
//   RUNE_MAX_NAMESPACES             — int32 > 0, default 1024: the cluster-wide cap on namespaces that exist
//                                     (`default` and DELETING ones count).  A CreateNamespace past it is refused.
//   RUNE_NS_DEFAULT_<FIELD>         — u64 >= 0 (FEATURE_BITS: u32), one per NamespaceLimits field, e.g.
//                                     RUNE_NS_DEFAULT_MAX_TOPICS.  Read once at boot; a limit a CreateNamespace omits
//                                     takes this value at request time and is written into the event.  Default 0.
//
// For cross-datacenter deployments set these higher (e.g. 1000/200/2000 ms).
//
// E5.2: Aeron media-driver threading mode (optional):
//   RUNE_AERON_THREADING_MODE — "shared" (default) or "dedicated" (case-insensitive).
//                               Any unrecognised value floors to "shared".
//   RUNE_AERON_DRIVER_IDLE    — idle strategy string for the node driver thread(s);
//                               unset = "backoff" in shard mode, "sleep-ns" in legacy (D2).
//
// All Aeron channels use the aeron:udp transport (container transport).

#include <rune/node/StorageFormat.h>
#include <rune/node/IdleShape.h>
#include <rune/node/PersistenceMode.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/node/PemFile.h>   // Task 9: loadPemFile() — re-exported here for config callers

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <filesystem>
#include <string>
#include <vector>

namespace rune::peerlink { struct ClusterKeys; }   // validateAuth's out-parameter (review C I4)

namespace rune::node {

// ---------------------------------------------------------------------------
// TopicSpec — the internal description of one topic (what TopicRegistry and the catalog boot build from).
// Topics are created by DDL; topicId is the catalog's topic id.
// ---------------------------------------------------------------------------
struct TopicSpec {
    std::string name;
    int32_t     partitionCount = 1;
    int32_t     topicId        = -1;
    /// Namespaces plan Task 6: the namespace the topic lives in (its dir is topicsRoot(dataDir, nsId)/<name>).
    uint32_t    nsId           = 0;
    /// VLS B.2: a view's sink -- a replica-less, catalog-only namespace (no log, no dir, no replica); its rows live
    /// in the source owner's WorkerViewSet.  Set by the catalog sink effect (live and boot) only.
    bool        viewSink       = false;
    /// Replicas per partition (DDL replicationFactor); 0 = every node (the
    /// default).  Clamped to the cluster size.
    int16_t     replicationFactor = 0;
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

    // Task M1 / Phase 2b E: persistence and durability modes.
    // persistence: selects the log backend (Disk = default, Transient = InMemoryLog).
    // durability: 0 = Majority (default), 1 = AllInSync.  Forwarded to PartitionConfig.
    //   Transient topics that must survive a node death MUST use majority or all-in-sync
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

    // Task 5 (0b): schema validation flag.
    // When true, the AdminWorker validates incoming produce records against the
    // registered schema for this topic before appending.  Default false (no validation)
    // so all existing topics are parity-unchanged until the caller opts in.
    bool            validate = false;
};

struct PeerConfig {
    int32_t  id            = -1;
    std::string host;
    uint16_t consensusPort    = 0;
    uint16_t clientPort       = 0;  // 0 = no known client endpoint (GB-4, backward-compatible)
    uint16_t replicationPort  = 0;  // 0 = no replication endpoint (GR-3, backward-compatible)

    // Task 1 (per-worker replication transport): advertised per-worker replication
    // stream count for this peer.  0 = unknown / feature disabled (default).
    // Self's entry is populated from NodeConfig::workerCount during parseNodeConfig.
    // Remote peers' entries are populated from membership descriptors when received.
    int32_t  workerCount      = 0;  // 0 = unknown

    // Self-contained worker shards (spec §3.3): this peer's explicit per-worker base
    // ports.  Worker w listens on replBasePort + w (replication) and
    // ingressBasePort + w (client produce).  0 = not advertised (legacy peers).
    uint16_t replBasePort    = 0;
    uint16_t ingressBasePort = 0;
    // 2026-10-02: the peer's per-worker dynamic-MDC egress-control base (worker w
    // = base + w) — advertised to clients in MetadataResponse for partitions this
    // peer LEADS.  6th RUNE_PEERS segment; 0 = not advertised (derived at start
    // in shard mode, HostDefaults.h).
    uint16_t egressControlBasePort = 0;

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

/// Authentication mode (authentication plan D31).  A NodeConfig built in code (every in-process test, every embedder)
/// is InProcessUnauthenticated: anonymous sessions with full access and plaintext inter-broker links, byte-identical
/// to the pre-authentication broker.  `parseNodeConfig` yields only Enabled (RUNE_AUTH unset / "enabled") or
/// DevDisabled (RUNE_AUTH=disabled; single node only); `src/main.cpp` refuses InProcessUnauthenticated.
enum class AuthMode : uint8_t { InProcessUnauthenticated = 0, Enabled = 1, DevDisabled = 2 };

/// Client transport security (combined fix program M1, issues #10 / #11).  Only per-frame AEAD binds a frame to its
/// session, so with authentication Enabled every client session is TLS: a plaintext Connect is answered HandshakeFailed
/// (29) and creates no session.
///   TlsOptional       a client may offer TLS (served encrypted when this broker has a certificate) or connect in
///                     plaintext.  Under AuthMode::Enabled it means RequireTls (the parse default; RUNE_REQUIRE_TLS=false
///                     with authentication refuses to start).
///   RequireTls        every client session is TLS (RUNE_REQUIRE_TLS=true); a broker without a certificate refuses to
///                     start.
///   PlaintextForTest  authentication over plaintext sessions, for in-process tests only.  `parseNodeConfig` never
///                     yields it and `src/main.cpp` refuses it.
enum class ClientTransport : uint8_t { TlsOptional = 0, RequireTls = 1, PlaintextForTest = 2 };

/// True when this broker was built with the RUNE_TLS CMake option (the default).  A non-TLS build refuses
/// authentication and every RUNE_TLS_* / RUNE_REQUIRE_TLS variable (issue #11).
[[nodiscard]] bool tlsBuild() noexcept;

struct NodeConfig {
    /// "Derive from the host" sentinel for the Aeron transport sizes (mtu, socket
    /// buffer, receive window) — resolved once at BrokerNode::start() by
    /// applyHostDerivedDefaults (HostDefaults.h).  Any other value is explicit.
    static constexpr int32_t kAeronAuto = -1;
    /// workerCoreBase sentinel: derive the affinity layout from the host (-1 stays
    /// "explicitly disabled").
    static constexpr int32_t kWorkerCoreBaseAuto = -2;

    int32_t             nodeId          = -1;
    std::vector<PeerConfig> peers;       // includes self; sorted by id
    ClientEndpoint      clientEndpoint;
    std::string         dataDir;
    // Aeron directory (CnC + every mmapped term buffer) — RUNE_AERON_DIR.
    // Empty → dataDir/aeron (legacy default).  Aeron's log buffers MUST live on
    // tmpfs (/dev/shm) for performance: on a disk-backed dataDir the default puts
    // every message's term-buffer writes through ext4 page-cache writeback.
    std::string         aeronDir;
    [[nodiscard]] std::string effectiveAeronDir() const {
        return aeronDir.empty() ? (std::filesystem::path{dataDir} / "aeron").string()
                                : aeronDir;
    }
    int32_t             consensusStreamId    = 0;
    int32_t             replicationStreamId  = 20;  // GR-3: stream for per-partition replication

    // Task 1 (per-worker replication transport):
    //
    // perWorkerReplication: when true, each worker opens its own dedicated
    //   replication transport on stream replBaseStreamId + workerId, replacing
    //   the shared control-thread replication path.  Default false.
    //   Overridable via RUNE_PER_WORKER_REPLICATION ("1" or "true" → true).
    //
    // replBaseStreamId: base Aeron stream id for the per-worker replication
    //   streams.  Worker W uses stream replBaseStreamId + W.
    //   The range [replBaseStreamId, replBaseStreamId + workerCount - 1] must
    //   not overlap clientStreamId, workerIngress/egress base ranges, or the
    //   consensus/replication stream ids (enforced by validateStreamRanges()).
    //   Default 2000 — chosen to be disjoint from all existing defaults.
    //   Overridable via RUNE_REPL_BASE_STREAM_ID (parseInt32).
    bool    perWorkerReplication = false;
    int32_t replBaseStreamId     = 2000;

    // Catalog history Task 6: the __catalog partition's replication plane —
    // its own Aeron stream on this node's replication channel (the channel the
    // control-thread transport subscribes on with replicationStreamId and, in
    // legacy per-worker mode, the worker planes with replBaseStreamId + w).
    // Must be distinct from replicationStreamId and outside
    // [replBaseStreamId, replBaseStreamId + workerCount) (validateCatalogStream).
    // Overridable via RUNE_CATALOG_REPL_STREAM_ID (parseInt32).  Every broker of
    // a cluster must use the same value.
    int32_t catalogReplStreamId  = 30;

    // Self-contained worker shards (spec §3.1-§3.3).  DEFAULT (2026-10-02, the
    // measured 110k rec/s config, findings §4): shard mode + fused runtime
    // placement, 1 worker per node — what the bench's RUNE_SHARD=1 set.  The legacy
    // mode stays selectable (RUNE_WORKER_MODE=legacy / RUNE_RUNTIME_PLACEMENT=thread).
    // Unset per-worker base ports are derived per node id (HostDefaults.h
    // deriveShardBasePorts) at BrokerNode::start().
    enum class WorkerMode : uint8_t { Legacy = 0, Shard = 1 };          // RUNE_WORKER_MODE
    enum class RuntimePlacement : uint8_t { Thread = 0, Fused = 1 };    // RUNE_RUNTIME_PLACEMENT
    WorkerMode       workerMode       = WorkerMode::Shard;
    RuntimePlacement runtimePlacement = RuntimePlacement::Fused;
    std::optional<int32_t> workerIngressBasePort;   // RUNE_WORKER_INGRESS_BASE_PORT (nullopt = unset)
    std::optional<int32_t> workerReplBasePort;      // RUNE_WORKER_REPL_BASE_PORT    (nullopt = unset)
    static constexpr int32_t kMaxShardWorkers = 64;   // runtime→worker announce mask width
    /// Client liveness of EVERY embedded Aeron driver (node + shard drivers) and the matching media-driver timeout
    /// of their in-process clients.  Driver and client live in this process, so the check can only detect this
    /// process's own stall (SIGSTOP, VM stall, docker pause) — and Aeron's 10 s default then killed the broker on
    /// resume (DR-10).  Default: effectively unbounded (1 h, = the rune client's in-process shards); tests set a
    /// short value to prove no startup phase starves an invoker driver.  No env var.
    static constexpr int64_t kInProcessLivenessNs = 3'600'000'000'000LL;
    int64_t shardClientLivenessTimeoutNs = kInProcessLivenessNs;
    /// The in-process clients' media-driver timeout (ms) matching shardClientLivenessTimeoutNs (never below
    /// Aeron's 10 s default).
    [[nodiscard]] long inProcessDriverTimeoutMs() const noexcept {
        const long ms = static_cast<long>(shardClientLivenessTimeoutNs / 1'000'000);
        return ms > 10'000 ? ms : 10'000;
    }
    [[nodiscard]] bool shardMode() const noexcept { return workerMode == WorkerMode::Shard; }
    [[nodiscard]] bool fused() const noexcept { return runtimePlacement == RuntimePlacement::Fused; }

    // Election timing — configurable via env (BR-3b).
    // Defaults are fast-but-safe for co-located / test clusters.
    // Increase for cross-datacenter deployments.
    int64_t electionTimeoutMs        = 150;  // default 150 ms
    int64_t electionStatusIntervalMs =  20;  // default  20 ms
    int64_t startupCanvassTimeoutMs  = 100;  // default 100 ms

    // Controlled shutdown (RUNE_CONTROLLED_SHUTDOWN_MS): on a graceful stop
    // signal main() hands this node's partition leadership (and, on the
    // controller, the consensus leadership) off before stopping.  The hand-off
    // is event-driven; this is only the last-resort cap after which the node
    // stops anyway (logged; the rest fails over as on an unclean stop).  Keep
    // it below the orchestrator's kill grace (docker stop: 10 s).  0 = off.
    int64_t controlledShutdownMs = 5'000;

    // Replication lag tolerance — GR-5.
    // After a follower hasn't fetched for replicaLagMaxMs, it is shrunk from the ISR.
    // Default 10 000 ms (10 s) matches the PartitionConfig default.
    // Tests may set a lower value (e.g. 500 ms) for faster ISR shrink in kill scenarios.
    int64_t replicaLagMaxMs = 10'000;

    // TEST-ONLY (E2 C1 residual N1): a controller that becomes ready takes no
    // dynamic leadership decision for a USER partition (failover, leaderless,
    // bump, rebalance) for this long — internal partitions are decided as
    // usual.  Holds a full restart's stale committed leadership in place so a
    // test can probe it.  One-shot: measured from this process's FIRST ready
    // edge, never re-armed by a later re-election.  0 (default): no hold;
    // never set in production.
    int64_t testUserDecisionHoldMs = 0;

    // TEST-ONLY (ISR v3 Step 9, R-I1): hold the consensus candidacy from boot even for a Founder/Legacy storage,
    // until the test releases it (BrokerNode::setTestCandidacyHold).  Makes a fresh-cluster formation
    // deterministic: every node decides its storage before anyone stands.  Never set in production.
    bool testCandidacyHoldAtBoot = false;
    // TEST-ONLY (ISR v3 Step 10): MetadataService::setHoldAllDecisionsForTest (toggled at runtime through
    // BrokerNode::setControllerDecisionHoldForTest) makes the ready controller take NO tick decision for ANY partition and
    // append no storage record while set.  This field arms the seam: a toggle-on lasts at most this long (a safety
    // net - a failing test must not wedge its cluster).  0 (default): the toggle has no effect; never set in production.
    int64_t testHoldAllControllerDecisionsMs = 0;
    // TEST-ONLY (ISR v3 Step 10): the metadata feature level this controller supports (0 = kBase: no storage tracking,
    // no NodeStorageRecord - the old-cluster path).  -1 (default): the binary's own level.
    int16_t testSupportedFeatureLevel = -1;
    // TEST-ONLY (ISR v3 Step 9, B-I8): every replica of this node emits no follower fetch for this long after
    // start (PartitionReplica::holdFetchesUntilForTest) - a wiped member that cannot catch up yet.  0 = off.
    int64_t testHoldFollowerFetchesMs = 0;
    /// Test seam (review C I4): runs on the startup thread right after validateAuth() loaded the cluster key - a test
    /// rewrites the key file here to prove the key validated is the key in use.  Cold; never set in production.
    std::function<void()> testAfterAuthValidation;
    // TEST-ONLY (ISR v3 CP2 C1-R): drop every consensus frame sent by this peer for this long after the first frame seen
    // (an address change / transport reconnect window).  -1 = off.
    int32_t testDropConsensusFromPeer = -1;
    int64_t testDropConsensusForMs    = 0;

    // Follower periodic-fetch backstop cadence (RUNE_FETCH_INTERVAL_US, default
    // 5000µs = 5ms).  GR-10 push + fast-ack is meant to drive replication; this
    // is only the lost-push backstop + ISR keepalive.  Exposed so acks=all
    // round-trip cadence can be tuned/measured on shared hardware.
    int64_t fetchIntervalNs = 5'000'000LL;
    // D8b: a back-pressure-paused (held) client session is reaped only after this long without activity
    // or a worker Progress signal (RUNE_MAX_HELD_IDLE_MS, 1 ms .. 24 h; 0 = gateway default 10 min, Kafka
    // connections.max.idle.ms).
    int64_t maxHeldIdleMs = 0;
    // D8c: shard-mode per-session produce credit budget of the gateway-routed path — the most forwarded
    // produces of one session whose acks no worker has claimed yet (RUNE_GATEWAY_PRODUCE_CREDITS,
    // 1 .. kMaxGatewayProduceCredits; 0 = gateway default 1024).  At the budget the gateway pauses that
    // client (controlled ABORT) before any worker's ack queue can fill.
    static constexpr uint32_t kMaxGatewayProduceCredits = 4096;   // == ClientGateway::kMaxSessionProduceCredits
    uint32_t gatewayProduceCredits = 0;

    // Backoff idle max park (RUNE_IDLE_MAX_PARK_US, default 200µs) for the runtime
    // + worker AgentRunners.  The default Agrona 1ms max park adds up to ~1ms wake
    // latency at EACH cross-thread hop of the acks=majority replication round-trip
    // (worker→control→net→control→worker …); 200µs cuts that 5x without pegging a
    // core (backoff still sleeps → consensus heartbeats are not starved, unlike
    // busyspin which triggers re-elections on shared cores).
    // D2 review R2-L2: shard mode — it caps the shard workers' backoff / wake park and the startup pump; in FUSED
    // placement also the runtime + node driver (they ride worker 0).  In THREAD placement the runtime, admin and
    // node-driver threads use RUNE_CONTROL_IDLE_MAX_PARK_US instead (controlIdleShape); fused: only the admin thread.
    int64_t idleMaxParkUs = 200;
    bool    idleMaxParkUsExplicit = false;   ///< RUNE_IDLE_MAX_PARK_US was set (shard mode warns: R2-L2)

    // D2 round 2 — control-plane idle (shard mode).  The node driver thread, the runtime thread (thread
    // placement) and the admin thread carry almost no traffic in shard mode, yet Aeron's backoff (10 spins,
    // 20 yields, park 1 us doubling) re-armed on every heartbeat / status message: idle, the three nodes'
    // control planes burned ~1 core on the 4-core box (node driver ~14% each: ~17k empty recvmmsg/s; runtime
    // ~10-14% each: ~3k yields + 3.4k parks/s) and starved whatever shared their core.  Shard mode: no yields,
    // park from kControlIdleMinParkUs doubling to controlIdleMaxParkUs (RUNE_CONTROL_IDLE_MAX_PARK_US).  The
    // node driver spins ONCE (each Aeron spin is a whole duty cycle — a recvmmsg per socket, ~60-90 us).
    // Measured idle on k3s-node-01 (node driver, Aeron backoff args): default 11.6% -> 1-0-50us-1ms 9.5%
    // -> 1-0-100us-2ms 7% -> 1-0-250us-4ms 5.5% each; involuntary switches 2.9k/s -> ~1/s (no yields).
    // A control message waits at most one park (<= 2 ms) — consensus timeouts are 100s of ms.  Legacy mode
    // (the data path rides the runtime and the node driver) keeps its shapes.  Never busy-spin.
    static constexpr int64_t kControlIdleMinParkUs        = 100;
    static constexpr int64_t kControlIdleMaxParkUsDefault = 2000;
    int64_t controlIdleMaxParkUs = kControlIdleMaxParkUsDefault;
    /// A backoff idle shape: spin passes, yield passes, then park minParkNs doubling to maxParkNs.
    struct BackoffShape {
        int64_t spins     = 10;
        int64_t yields    = 20;
        int64_t minParkNs = 1'000;
        int64_t maxParkNs = 1'000'000;
        friend bool operator==(const BackoffShape&, const BackoffShape&) = default;
    };
    /// Shard mode: the control-plane park shape (10 spins — a runtime / admin pass touches no syscall).
    [[nodiscard]] BackoffShape controlIdleShape() const noexcept {
        const int64_t maxNs = controlIdleMaxParkUs * 1'000;
        return BackoffShape{10, 0, std::min<int64_t>(kControlIdleMinParkUs * 1'000, maxNs), maxNs};
    }
    /// The runtime thread's idle (thread placement; RUNE_RUNTIME_IDLE=busyspin overrides).
    [[nodiscard]] BackoffShape runtimeIdleShape() const noexcept {
        return shardMode() ? controlIdleShape() : BackoffShape{10, 20, 1'000, idleMaxParkUs * 1'000};
    }
    /// The admin thread's idle.
    [[nodiscard]] BackoffShape adminIdleShape() const noexcept {
        return shardMode() ? controlIdleShape() : BackoffShape{};
    }

    // A7 park-until-work (findings §4.12): shard workers park in ppoll() on their invoker driver's
    // socket-readiness fds and their io_uring completion fd (rune_driver_wake.h; Linux), woken by the next
    // datagram / fsync completion and bounded by the backoff park period (1 µs doubling to idleMaxParkUs —
    // the bound for work that has no fd: cross-thread rings, Aeron timers).  RUNE_IDLE_WAKE=0 → the legacy
    // Aeron backoff (timed park).  The spin / yield phase BEFORE the first park stays the Aeron 10 / 20 in
    // both modes (review M1): fd-less work (a ring message from another thread, a conductor command, a
    // timer) usually lands within microseconds of a busy pass, and a park there costs >= the timer slack.
    bool idleWake = true;
    // Idle passes before parking (RUNE_IDLE_SPINS / RUNE_IDLE_YIELDS, a non-negative integer; anything
    // else refuses startup).  kIdleAuto = the Aeron 10 / 20.
    static constexpr int32_t kIdleAuto = -1;
    static constexpr int32_t kAeronIdleSpins  = 10;
    static constexpr int32_t kAeronIdleYields = 20;
    int32_t idleSpins  = kIdleAuto;
    int32_t idleYields = kIdleAuto;

    // A9 ring doorbells (findings §4.15): with park-until-work, the cross-thread rings a shard worker consumes
    // (runtime → worker ops, session lifecycle, worker → fused-runtime returns, admin → fused-runtime, portable
    // fsync completions) ring an eventfd in its park set, so ring work ends a park like a datagram does.  Every
    // wake source a pass can wait on then has an fd — datagrams, io_uring completions, ring messages — so a park
    // can last idleDoorbellParkUs: what is left without an fd is timers, and every timer on the worker path is
    // >= 1 ms (Aeron: NAK 60 ms, SM 200 ms, heartbeats 100 ms+, timer interval 1 s; Rune: election status
    // 20 ms, fetch backstop 5 ms, log sync 10 ms, ack timeout 30 s).
    // The Aeron spin / yield phase stays in front of the first park (auto 10 / 20, as A7): measured (§4.15)
    // on the 4-core box at 20k rec/s, dropping it (RUNE_IDLE_SPINS=0 RUNE_IDLE_YIELDS=0) cuts worker CPU ~6 pts
    // but costs ~+100 µs p50 — every event then pays a park wake on a saturated box.  With the phase kept the
    // 1 ms doorbell park still cuts idle-cluster worker CPU 30% -> 18% at an unchanged 20k p50.
    // RUNE_IDLE_DOORBELL=0 → A7's shape exactly (no doorbell, Aeron 10 / 20, 1 µs .. idleMaxParkUs).
    bool idleDoorbell = true;
    // The doorbell shape's park: first park (RUNE_IDLE_DOORBELL_MIN_PARK_US) doubling to the timer bound
    // (RUNE_IDLE_DOORBELL_PARK_US).  Positive integers; anything else refuses startup.
    int64_t idleDoorbellMinParkUs = 1'000;
    int64_t idleDoorbellParkUs    = 1'000;

    /// Shard workers park on driver readiness unless RUNE_IDLE_WAKE=0.
    [[nodiscard]] bool shardIdleWantsWake() const noexcept { return idleWake; }
    /// The shard-worker backoff shape.  The Aeron spin / yield phase (auto 10 / 20) in both modes (review M1;
    /// A9 measured).  `doorbell`: the runner parks on a ring doorbell too (A9) — the doorbell park
    /// (idleDoorbellMinParkUs .. idleDoorbellParkUs); otherwise A7's 1 µs .. idleMaxParkUs.
    [[nodiscard]] IdleShape shardIdleShape(bool doorbell = false) const noexcept {
        IdleShape s;
        if (doorbell) {
            s.maxSpins  = idleSpins  >= 0 ? idleSpins  : kAeronIdleSpins;
            s.maxYields = idleYields >= 0 ? idleYields : kAeronIdleYields;
            s.minParkNs = idleDoorbellMinParkUs * 1'000;
            s.maxParkNs = idleDoorbellParkUs * 1'000;
            return s;
        }
        s.maxSpins  = idleSpins  >= 0 ? idleSpins  : kAeronIdleSpins;
        s.maxYields = idleYields >= 0 ? idleYields : kAeronIdleYields;
        s.minParkNs = 1'000;
        s.maxParkNs = idleMaxParkUs * 1'000;
        return s;
    }

    // Minimum ISR size gate for acks=All produces — GR-5 [M1].
    // Default: cluster majority (|peers|/2 + 1), so a 3-node cluster requires 2.
    // Set to 1 for single-node deployments (leader-only ISR is the whole cluster).
    // Overridable at runtime via RUNE_MIN_ISR env var.
    // Also forwarded to PartitionConfig::minIsr in buildReplication().
    int32_t gatewayMinIsr = 1;
    /// RUNE_BROKER_LIVENESS_TIMEOUT_MS (CP3 I4): how long the controller waits without a heartbeat before it treats a node
    /// as dead (fails its partitions over).  0 = the default, 3 x the election's leader-heartbeat timeout.  A deterministic
    /// knob for fault tests (a wiped node must boot and find itself the committed leader before it is fenced out).
    int64_t brokerLivenessTimeoutMs = 0;
    /// RUNE_GROUP_MAX_GROUPS (#214): the most consumer groups coordinated at once.  0 = the default
    /// (GroupCoordinationService::kDefaultMaxGroups).
    int32_t groupMaxGroups = 0;
    /// RUNE_GROUP_MIN_SESSION_TIMEOUT_MS / RUNE_GROUP_MAX_SESSION_TIMEOUT_MS (#499): the bounds a JoinGroup's session
    /// timeout is clamped into.  0 = the GroupCoordinationService default (1 s / 30 min).
    int32_t groupMinSessionTimeoutMs = 0;
    int32_t groupMaxSessionTimeoutMs = 0;

    // Review 2026-10-04 MEDIUM-3 (RUNE_SURVIVOR_LEADER_ELECTION, default false):
    // opt-in position-proven survivor election for a partition whose committed
    // ISR has no live member (Kafka unclean.leader.election.enable analog).
    bool survivorLeaderElection = false;

    // ISR design v3 §3.2 (Step 5): how an EMPTY, not yet formatted data dir is formatted at boot.
    // In-process default (rigs): an empty dir formats as founder; the env parser defaults to Required
    // (RUNE_STORAGE_FORMAT unset / "": an empty dir refuses to start).  Ignored once storage.meta exists.
    /// RUNE_MAX_NAMESPACES (namespaces Task 7): the cluster-wide namespace cap, `default` included.
    int32_t maxNamespaces = 1024;
    /// RUNE_NS_DEFAULT_* (namespaces Task 7): the cluster default limits an omitted CreateNamespace field takes.
    rune::protocol::NamespaceLimits nsDefaultLimits{};
    StorageFormat storageFormat = StorageFormat::TestFounder;
    // RUNE_STORAGE_RECOVERY_VOTE (default false): unsafe, disaster recovery only — lets an unconfirmed
    // (replaced-disk) node vote (read by the storage gate, Step 9).
    bool storageRecoveryVote = false;

    // Re-review 2026-10-04 MEDIUM-B (RUNE_UNCLEAN_CONSENSUS_ELECTION, default
    // false): operator escape hatch for the consensus candidacy gate.  Normally
    // a node votes only for a candidate in the newest durable __cluster_metadata
    // ISR it knows (InternalIsrKnowledge) — if no live node is in it, the
    // consensus stays leaderless until a member returns (Kafka: an offline
    // partition).  ON = the gate is not wired: the election is data-blind and a
    // node missing committed metadata records may win (they are then LOST —
    // the analog of unclean.leader.election.enable for the metadata log).  Set
    // it only to recover a cluster whose metadata ISR members are gone for
    // good, then turn it off again.
    bool uncleanConsensusElection = false;

    // Task 9 (RUNE_TLS — client<->broker TLS/AEAD enablement, spec §11):
    // cert/key FILE PATHS (PEM), loaded once at BrokerNode startup and on
    // every SIGHUP (renewal reload) — the ClientGateway ctor itself still
    // wants PEM TEXT, never a path (Task 7's contract); BrokerNode reads
    // these files and passes the text through.  Both empty (the default) =
    // crypto disabled for this node, byte-identical to pre-Task-7/9
    // behaviour - allowed only without authentication (issue #10: an
    // authenticating broker needs its certificate).  Read from the environment
    // by a RUNE_TLS build (the default); a RUNE_TLS=OFF build refuses to start
    // when any of them is set (issue #11), never silently plaintext.
    //   RUNE_TLS_CERT_FILE — PEM cert file path (RUNE_TLS build only).
    //   RUNE_TLS_KEY_FILE  — PEM private-key file path (RUNE_TLS build only).
    std::string tlsCertFile;
    std::string tlsKeyFile;

    // Task 9 / spec §11 require_tls, combined fix program M1: the client transport (see ClientTransport).  The
    // gateway hard-rejects (ConnectResponse{HandshakeFailed}) any Connect that does not establish an encrypted session
    // whenever requiresClientTls().  RUNE_REQUIRE_TLS=true|false (RUNE_TLS build only; false with authentication
    // refuses to start).
    ClientTransport clientTransport = ClientTransport::TlsOptional;
    /// Every client session must be TLS: RequireTls, or authentication Enabled unless PlaintextForTest (M1).
    [[nodiscard]] bool requiresClientTls() const noexcept {
        return clientTransport == ClientTransport::RequireTls ||
               (authMode == AuthMode::Enabled && clientTransport != ClientTransport::PlaintextForTest);
    }

    // --- Authentication (authentication plan Task 9; spec §8 / §9) ---------------------------------------------
    //   RUNE_AUTH                   — "enabled" (default when unset) | "disabled" (dev mode: single node only; every
    //                                 request runs as 'anonymous' with full access; warned at start and every 5 min).
    //   RUNE_CLUSTER_KEY_FILE       — required when enabled (single node included): the shared cluster key; the file
    //                                 must hold >= 32 bytes after trailing whitespace is trimmed (checked at start by
    //                                 validateAuth, not by parse).
    //   RUNE_CLUSTER_KEY_FILE_NEXT  — optional: the key a rolling restart rotates to (a broker accepts both).
    //   RUNE_AUTH_TIMEOUT_MS        — a session that has not authenticated within this is ended (default 10 000).
    //   RUNE_PEER_AUTH_STREAM_ID    — the inter-broker handshake stream (default 40).
    AuthMode    authMode = AuthMode::InProcessUnauthenticated;
    std::string clusterKeyFile;
    std::string clusterKeyFileNext;
    int64_t     authTimeoutMs    = 10'000;
    int32_t     peerAuthStreamId = 40;
    // Issues #15 / #111 (decision M7): a broker serves logins, `__security` reads and security DDL only while its
    // `__security` state is provably current - caught up with the current leader since boot, a leader answer (a
    // follower) within F and the consensus in contact within F; otherwise it answers the retriable AuthUnavailable (48).
    //   RUNE_SECURITY_MAX_STALENESS_MS      — F (default 1 000); must exceed the broker liveness timeout + 500 ms, so a
    //                                         leadership move does not make a healthy broker stale (nor does a hard
    //                                         leader failure: a current follower in the ISR holds, #460) (an explicit value
    //                                         that does not is refused; the default is raised to it).
    //   RUNE_SECURITY_STALE_SESSION_END_MS  — R (default 10 000; > F): a broker stale this long ends its authenticated
    //                                         sessions with a session-level 48 (clients reconnect elsewhere).
    //   RUNE_SECURITY_GATE_DEADLINE_MS      — issue #120: a security DDL OK held by the D24 gate longer than this
    //                                         (default 5 000) is answered Internal "committed; enforcement not confirmed".
    int64_t     securityMaxStalenessMs     = 1'000;
    int64_t     securityStaleSessionEndMs  = 10'000;
    int64_t     securityGateDeadlineMs     = 5'000;
    /// F and R as the broker applies them: never below liveness + 501 ms / F + 1 ms (a default raised by long election
    /// timings or RUNE_BROKER_LIVENESS_TIMEOUT_MS; an explicit env value that violates it is refused at parse).
    [[nodiscard]] int64_t effectiveSecurityMaxStalenessMs() const noexcept {
        const int64_t floor = effectiveBrokerLivenessMs() + 501;
        return securityMaxStalenessMs > floor ? securityMaxStalenessMs : floor;
    }
    [[nodiscard]] int64_t effectiveSecurityStaleSessionEndMs() const noexcept {
        const int64_t f = effectiveSecurityMaxStalenessMs();
        return securityStaleSessionEndMs > f ? securityStaleSessionEndMs : f + 1;
    }
    /// The broker liveness timeout the controller applies (ms): brokerLivenessTimeoutMs, else 3 x the election's
    /// leader-heartbeat timeout (max(electionTimeout, 3 x statusInterval)) - the same derivation as the runtime's.
    [[nodiscard]] int64_t effectiveBrokerLivenessMs() const noexcept {
        if (brokerLivenessTimeoutMs > 0) return brokerLivenessTimeoutMs;
        const int64_t hb = electionTimeoutMs > 3 * electionStatusIntervalMs ? electionTimeoutMs : 3 * electionStatusIntervalMs;
        return 3 * hb;
    }

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
    // (workerCoreBase + i) on the first doWork call (self-pin on entry).  Pinning is
    // for cache locality only: shard workers idle with BACKOFF unless
    // RUNE_WORKER_IDLE=busyspin (then every shard worker busy-spins, pinned or not;
    // "yielding"/"sleeping" are treated as backoff in shard mode).
    //
    // kWorkerCoreBaseAuto (-2, DEFAULT) = derived at start (HostDefaults.h
    //   deriveWorkerCoreBase): Linux shard mode pins node i of a co-located
    //   cluster to cores 1+i*W.. when the box has the cores (the bench layout),
    //   one-node-per-host to 1..W; otherwise / legacy / non-Linux → -1.
    // -1 = affinity explicitly DISABLED.
    //
    // Example: with workerCoreBase=2 and workerCount=4, workers pin to cores
    //   2, 3, 4, 5 — reserving cores 0 and 1 for the control thread, Aeron
    //   media driver, and kernel housekeeping (see AffinityConfig in the E4 spec).
    //
    // Overridable via RUNE_WORKER_CORE_BASE env var.
    //   Absent or empty → kWorkerCoreBaseAuto (derived).
    //   Any value < -1   → -1 (clamped to disabled).
    int32_t workerCoreBase = kWorkerCoreBaseAuto;
    /// Do the shard workers busy-spin?  Only on the explicit RUNE_WORKER_IDLE=busyspin opt-in — never
    /// by default, pinned or not (measured: no p50/p99 gain over backoff; it pegs cores the box needs).
    [[nodiscard]] bool shardWorkersBusySpin() const noexcept {
        return workerIdleStrategy == WorkerIdleStrategy::BusySpin;
    }

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
    //   - workerEgressControlHost: host the broker binds for MDC control.  Empty (the
    //     default) = DERIVED by effectiveWorkerEgressControlHost(): the address clients
    //     reach it at (they send to <advertised RUNE_PEERS host>:<port>) — the advertised
    //     host itself when numeric, else the wildcard.  RUNE_WORKER_EGRESS_CONTROL_HOST
    //     overrides it; an explicit LOOPBACK host on a node advertised at a non-loopback
    //     host refuses to start (review B-I3: the old "127.0.0.1" default silently turned
    //     per-worker egress off in every multi-host / container deployment).
    //   - workerEgressControlBasePort: base UDP control port; worker W uses
    //     base + W.  RUNE_WORKER_EGRESS_CONTROL_BASE_PORT.  Must not collide with
    //     client/consensus/replication ports.  nullopt = not configured: legacy
    //     mode binds kDefaultWorkerEgressControlBasePort (via
    //     effectiveWorkerEgressControlBasePort()); shard mode refuses to start
    //     (spec §3.3: every per-worker port explicit — a shared default would let
    //     co-located shard nodes silently collide).
    //   - workerEgressBaseStreamId: 0 = unset; resolved in buildWorkerPool to
    //     workerIngressBaseStreamId + workerCount (keeps ingress/egress stream
    //     ranges non-overlapping).  RUNE_WORKER_EGRESS_BASE_STREAM.
    std::string workerEgressControlHost;   // empty = derived (effectiveWorkerEgressControlHost)
    /// The MDC control host actually bound (see workerEgressControlHost).  The ONLY way readers obtain it.
    [[nodiscard]] std::string effectiveWorkerEgressControlHost() const;
    static constexpr int32_t kDefaultWorkerEgressControlBasePort = 40300;
    std::optional<int32_t> workerEgressControlBasePort;
    /// The egress-control base port actually bound: the configured value, else the
    /// legacy default.  The ONLY way readers obtain the port.
    [[nodiscard]] int32_t effectiveWorkerEgressControlBasePort() const noexcept {
        return workerEgressControlBasePort.value_or(kDefaultWorkerEgressControlBasePort);
    }
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
    //     RUNE_TRANSIENT_LOG_RING_BYTES (alias RUNE_MEMORY_LOG_RING_BYTES, deprecated). Default 8 MiB.
    int32_t replicationOutboundRingBytes = 32 * 1024 * 1024;   // findings §4 (was 4 MiB)
    int32_t workerIngressRingBytes       = 32 * 1024 * 1024;
    int32_t workerReturnRingBytes        = 32 * 1024 * 1024;
    int32_t ioSubmitRingDepth            = 1024;
    int32_t ioCompletionRingDepth        = 1024;
    int32_t memoryLogRingBytes           = 8 * 1024 * 1024;
    //   - aeronTermBufferBytes: Aeron term buffer length (bytes) for the embedded
    //     media driver — sizes every publication/subscription log buffer.  Power
    //     of two in [64 KiB, 1 GiB].  Default 16 MiB (findings §4; was 4 MiB).
    //     Every publication/image maps 3 terms (48 MiB at the default, NON-sparse
    //     → really allocated on the aeron dir's filesystem): tests set smaller.
    // 1 MiB (measured 2026-10-03, 3-node acks=all: >= 16 MiB throughput, no 100+ ms term-rotation stalls,
    // 16x less /dev/shm).  Max Aeron message = term/8 = 128 KiB -> the batch limit (batchLimitFormula).
    int32_t aeronTermBufferBytes         = 1024 * 1024;
    /// RUNE_AERON_CONTROL_TERM_BYTES: term of the worker's per-session ACK publications only — small so a
    /// term rotation never cleans 16 MiB inside the worker pass.  NOT the gateway egress publications:
    /// consumer record pushes travel there (max message = term/8 must cover the largest batch).
    /// Power of two, >= max(64 KiB, 2 x MTU) (enforced in HostDefaults).
    int32_t aeronControlTermBufferBytes  = 1024 * 1024;
    /// Periodic fsync of a worker's owned logs (leader side) and of follower-applied data, in ms.
    /// 0 = never (default; Kafka's log.flush.interval.* default): durability via replication + OS
    /// writeback; recovery semantics in DUR-1.  The Strict tier keeps its per-write fsync.
    /// RUNE_WORKER_SYNC_INTERVAL_MS / RUNE_FOLLOWER_FLUSH_INTERVAL_MS.
    int64_t workerSyncIntervalMs         = 0;
    /// RUNE_IOWQ_CORES (cpu list "0,4-5"): CPUs for the io_uring io-wq kernel threads.  Unbound, they may
    /// run on (and preempt) a pinned worker's core.  Applied per worker thread at its first pass (io-wq is
    /// per task; the mask covers io-wq threads created afterwards).  Empty = derived (HostDefaults: only
    /// for the derived co-located layout — the allowed CPUs outside every node's workers).
    std::vector<int32_t> ioWorkerCpus;
    /// RUNE_NONWORKER_CORES (cpu list): CPUs for every NON-worker thread the broker creates (threaded node
    /// driver, Aeron client conductor, runtime, admin, cleaner, flushers, signal + main threads).  Unbound,
    /// they run on (and preempt) the pinned workers' cores (D2).  Empty = derived (HostDefaults: with pinned
    /// workers, the allowed CPUs outside the worker cores — every co-located node's for the conventional
    /// layout, else this node's own); still empty = affinity unchanged.
    std::vector<int32_t> nonWorkerCpus;
    int64_t followerFlushIntervalMs      = 0;
    //   - produce intake limit (F18): the largest produce batch the broker accepts
    //     is DERIVED, never refused at startup — produceIntakeLimit() below.  A
    //     batch above it is answered MessageTooLarge at intake (gateway + worker
    //     ingress), so an accepted batch always fits one replication frame.
    static constexpr int32_t kDefaultMaxProduceBatchBytes = 8 * 1024 * 1024;
    //   - aeronMtuBytes: Aeron UDP MTU (bytes) for the embedded media driver.
    //     RUNE_AERON_MTU.  Not required to be a power of two.  Default kAeronAuto:
    //     DERIVED at start (HostDefaults.h deriveAeronTransport) — 65504 when every
    //     endpoint is loopback, else the egress interface MTU minus IP/UDP headers
    //     aligned to 32; always clamped <= the socket buffer the driver will get.
    //
    //     The EINVAL lesson: aeron_driver_init() rejects mtu_length > the probed
    //     socket SO_SNDBUF (aeron_driver.c:451).  macOS's default UDP SO_SNDBUF is
    //     9216 (net.inet.udp.maxdgram) and its cap ~8 MiB (kern.ipc.maxsockbuf):
    //     a 65504 MTU with the OS-default buffer crashed every embedded-driver
    //     test at init.  The derivation sets the socket buffer first and clamps the
    //     MTU (even an explicit one, with a WARN) so that can never recur.
    int32_t aeronMtuBytes                = kAeronAuto;
    //   - gatewayFragmentLimit: maximum fragments polled per ClientGateway
    //     doWork() cycle (RUNE_GATEWAY_FRAGMENT_LIMIT).  Not required to be a
    //     power of two — parsed with parseInt32.  Default 256 (raised from the
    //     old hard-coded 10 to prevent head-of-line blocking on batched ingress).
    //     Also reused for per-worker ingress poll (Task E5.1.2).
    int32_t gatewayFragmentLimit         = 256;
    //   - maxProduceBatchBytes: F3 round 4 — EXPLICIT override of the gateway's
    //     max accepted produce-batch wire size (RUNE_MAX_PRODUCE_BATCH_BYTES).
    //     std::nullopt (default — NOT set) means "use the derived effective
    //     value" (min of the configured default, the ingress transport's hard
    //     per-message maximum, and the replication transport's hard per-message
    //     maximum minus its response framing overhead — see BrokerNode.cpp's
    //     effectiveMaxProduceBatchBytes()/validateExplicitMaxProduceBatch()).
    //   F3 round 6: the effective max is a CLUSTER-WIDE constraint.  Shard
    //   mode verifies it per replication plane (PlaneHello carries it; a
    //   mismatch is Refused like a worker-count mismatch).  Legacy mode does
    //   NOT verify it: aeronTermBufferBytes / workerIngressRingBytes /
    //   replicationOutboundRingBytes / maxProduceBatchBytes MUST be identical
    //   on every node (logged at startup).
    //     A SET value is a deliberate operator request for a LARGER (or
    //     smaller) ceiling than the derived default would give; BrokerNode::
    //     start() refuses to start (naming the exact numbers) if an explicit
    //     value exceeds what the transport can actually carry — silently
    //     capping an EXPLICIT request would be a silent surprise, unlike
    //     capping the un-set default, which was never a promise to begin with.
    std::optional<int64_t> maxProduceBatchBytes;
    // ── Per-tick drain/poll batch limits (all raised from tiny hard-coded 10/16
    //    defaults that throttled the acks=majority replication pipeline; each is a
    //    KNOB so we always know what bounds the per-cycle batch).  A value of 0 is
    //    coerced to the default.  Env names in NodeConfig.cpp.
    //   - workerIngressDrainLimit: routed replication ops (FetchRequest/ApplyReplicated
    //     — the HW-advancing fast-acks) drained per worker duty cycle from the ingress
    //     ring (RUNE_WORKER_INGRESS_DRAIN_LIMIT; was the OneToOneByteRing default 16).
    int32_t workerIngressDrainLimit      = 1024;
    // ── Phase 2c: subscription filters (broker-side filtering of a typed topic's pushes).  A value out of its bounds
    //    refuses start with a message naming the knob.
    //   - filterMaxSubsPerPartition: filtered subscriptions per (topic, partition) (RUNE_FILTER_MAX_SUBS_PER_PARTITION, [1, 1048576]).
    //   - filterMaxIndexValuesPerPartition: distinct index values per partition (RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION, [1, 1048576]).
    //   - filterNoteIntervalMs / filterNoteRecords: a subscription that skipped records gets a position note after
    //     this many ms / skipped records, whichever is first (RUNE_FILTER_NOTE_INTERVAL_MS [1, 60000],
    //     RUNE_FILTER_NOTE_RECORDS [1, 2^31-1]).
    //   - filterVersionWaitMs: how long a filter waits for a schema version it has not seen (RUNE_FILTER_VERSION_WAIT_MS, [0, 600000]).
    //   - filterMatchScratch: the per-worker match scratch, in matches (RUNE_FILTER_MATCH_SCRATCH, [64, 16777216]).
    uint32_t filterMaxSubsPerPartition        = 4096;
    uint32_t filterMaxIndexValuesPerPartition = 4096;
    int32_t  filterNoteIntervalMs             = 50;
    int64_t  filterNoteRecords                = 65536;
    int32_t  filterVersionWaitMs              = 5000;
    uint32_t filterMatchScratch               = 65536;
    //   - workerReturnDrainLimit: acks drained from a worker's return ring per gateway
    //     cycle (RUNE_WORKER_RETURN_DRAIN_LIMIT; was 16).
    int32_t workerReturnDrainLimit       = 1024;
    //   - replFragmentLimit: control-thread replication transport per-tick cap — BOTH
    //     the inbound controlledPoll (fast-acks/pushes from peers) AND the outbound
    //     drain (pushes/acks to peers) (RUNE_REPL_FRAGMENT_LIMIT; was 10).
    int32_t replFragmentLimit            = 256;
    //   - consensusFragmentLimit: consensus ingress poll/drain per tick
    //     (RUNE_CONSENSUS_FRAGMENT_LIMIT; was 10/16).
    int32_t consensusFragmentLimit       = 64;
    //   - workerAckSlotBytes: expected ProduceResult size used by the worker's
    //     reservation-before-poll to size return-ring ack slots
    //     (RUNE_WORKER_ACK_SLOT_BYTES).  Must be >= the framed ProduceResult size
    //     (~160 B).  Default 256.  The old code reserved 2×maxMsgLength per ack,
    //     capping produce intake at ~4 frames/cycle on a multi-MB return ring.
    int32_t workerAckSlotBytes           = 256;
    //   - workerMaxOwedReturns: F15 S3 static cap on a worker's owed returns
    //     (parked acks=all + Strict acks awaiting fsync + owed facts), the G-A6
    //     reservation (RUNE_WORKER_MAX_OWED_RETURNS, default 4096, valid
    //     [256, 8192]; out of range refuses to start).
    int32_t workerMaxOwedReturns         = 4096;
    //   - aeronRcvWindowBytes: Aeron receiver flow-control initial window (bytes).
    //     RUNE_AERON_RCV_WINDOW.  Must be a power of two.  Default kAeronAuto:
    //     min(4 MiB, socketBuffer/4, term/2) rounded down to a power of two
    //     (window <= socket buffer <= sysctl; with 64 KiB datagrams a window near
    //     SO_RCVBUF drops packets — kernel truesize).  Was 128 KiB (F28).
    int32_t aeronRcvWindowBytes          = kAeronAuto;

    //   - aeronTermBufferSparse: create term buffers as sparse files (true, Aeron's
    //     default here) or fully allocated + zero-filled at creation (false).
    //     RUNE_AERON_TERM_SPARSE=true|false.  Sparse terms page-fault on the first
    //     write to every page (measured ~15% of a loaded shard thread) and turn a
    //     full tmpfs into a mid-run SIGBUS; non-sparse moves both to publication
    //     creation (a full tmpfs then fails the create, loudly).  Default
    //     nullopt = DERIVED at start: non-sparse when the Aeron dir is on tmpfs
    //     (findings §4 SPARSE: 73–79k → 100–105k rec/s), sparse on a disk-backed
    //     dir (pre-allocation would zero-fill 48 MiB per log to disk on the
    //     conductor thread).
    std::optional<bool> aeronTermBufferSparse;

    //   - Aeron sender/receiver batching per syscall (0 = Aeron default 2; 1..16).
    //     RUNE_AERON_MAX_MSGS_PER_SEND  -> network_publication_max_messages_per_send
    //     RUNE_AERON_SENDER_IOV         -> sender_io_vector_capacity (sendmmsg batch)
    //     RUNE_AERON_RECEIVER_IOV       -> receiver_io_vector_capacity (recvmmsg batch)
    //     With UDP GSO active (below) and MAX_MSGS_PER_SEND unset, the driver uses 16 (Aeron's max) so a
    //     burst of MTU-size fragments forms one GSO super-datagram.
    int32_t aeronMaxMessagesPerSend      = 0;
    int32_t aeronSenderIoVectorCapacity  = 0;
    int32_t aeronReceiverIoVectorCapacity = 0;

    //   - aeronUdpGso: install the rune_gso Aeron UDP transport bindings — Linux UDP GSO (UDP_SEGMENT) on
    //     send, UDP GRO on receive; wire format unchanged (peers see <= MTU datagrams), auto-off where the
    //     kernel/platform lacks them (stock Aeron).  RUNE_AERON_UDP_GSO=0|false forces stock Aeron (A/B,
    //     fault isolation).  Spec docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md.
    bool aeronUdpGso = true;

    //   - aeronEpollPoll: A7 (findings §4.12) — the media driver's transport pollers take ONE epoll_wait(0)
    //     per duty cycle and recvmmsg only the ready sockets, for every poller with > 1 transport (Aeron
    //     does that only beyond 5 transports and otherwise recvmmsg-s every socket each cycle, mostly
    //     empty: the top CPU cost of an invoker shard pass).  Delivery unchanged; Linux only (elsewhere
    //     stock).  RUNE_AERON_EPOLL_POLL=0|false = Aeron's stock poller (A/B, fault isolation).
    bool aeronEpollPoll = true;

    //   - aeronSocketBufferBytes: OS UDP socket SO_SNDBUF / SO_RCVBUF (bytes) for
    //     the embedded media driver's sockets.  RUNE_AERON_SOCKET_BUFFER_BYTES.
    //     Larger socket buffers absorb produce/ack bursts on loopback/LAN so the
    //     kernel does not drop datagrams under load (which would otherwise force
    //     Aeron NAK/retransmit or app-level retransmit).  0 = leave the OS default.
    //     Default kAeronAuto: min(16 MiB, net.core.rmem_max/wmem_max) (macOS: the
    //     probed SO_*BUF cap); capped below 16 MiB → loud WARN naming the sysctls.
    int32_t aeronSocketBufferBytes       = kAeronAuto;

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
    // the active threading mode uses (RUNE_AERON_DRIVER_IDLE).
    //   Shared    mode: passed to aeron_driver_context_set_shared_idle_strategy.
    //   Dedicated mode: passed to conductor / sender / receiver idle setters.
    // An unrecognised value (or malformed RUNE_AERON_DRIVER_IDLE_ARGS) refuses the node
    // driver's construction (D2 round 2: Aeron's failed setter left a NULL idle function —
    // the driver thread aborted on its first idle pass).
    // D2: empty (DEFAULT) = derived — effectiveDriverIdleStrategy().
    std::string driverIdleStrategy;
    /// The node driver's idle strategy: RUNE_AERON_DRIVER_IDLE if set; else "backoff" in shard mode (the
    /// node driver carries only control-plane traffic there — Aeron's backoff spins, yields, then parks up
    /// to 1 ms; "sleep-ns" with Aeron's tiny default sleep effectively polls, ~45% CPU per node) and
    /// "sleep-ns" in legacy mode (the data path rides that driver).  The per-worker invoker drivers have
    /// no idle loop (pumped by their worker).
    [[nodiscard]] std::string_view effectiveDriverIdleStrategy() const noexcept {
        if (!driverIdleStrategy.empty()) return driverIdleStrategy;
        return shardMode() ? std::string_view("backoff") : std::string_view("sleep-ns");
    }
    /// RUNE_AERON_DRIVER_IDLE_ARGS: Aeron idle-strategy init args for the node driver (backoff:
    /// "spins-yields-minPark-maxPark", durations in ns or with a ns/us/ms/s suffix).  Empty = derived.
    std::string driverIdleArgs;
    /// D2 review R2-L3: Aeron builds yield / spin / noop without init args (only backoff and sleep-ns /
    /// sleeping take them).
    [[nodiscard]] static bool driverIdleTakesArgs(std::string_view strategy) noexcept {
        return strategy == "backoff" || strategy == "sleep-ns" || strategy == "sleeping";
    }
    /// The node driver's idle args: RUNE_AERON_DRIVER_IDLE_ARGS if set; else, for the DERIVED shard-mode
    /// backoff, the control-plane shape with one spin pass ("1-0-<minParkNs>-<maxParkNs>"); else empty
    /// (Aeron's own: AERON_*_IDLE_STRATEGY_INIT_ARGS or its built-in defaults).
    [[nodiscard]] std::string effectiveDriverIdleArgs() const {
        if (!driverIdleArgs.empty()) return driverIdleArgs;
        if (!driverIdleStrategy.empty() || !shardMode()) return {};
        const BackoffShape c = controlIdleShape();
        return "1-0-" + std::to_string(c.minParkNs) + "-" + std::to_string(c.maxParkNs);
    }

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

    /// RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND — the log cleaner's shared I/O throttle (bytes/s across all cleaner work;
    /// Kafka log.cleaner.io.max.bytes.per.second). Absent/empty = -1 = unlimited.
    int64_t cleanerIoMaxBytesPerSecond = -1;

    // PSM fix round 1 (I1, Kafka producer.id.expiration.ms): compaction keeps the
    // last batch METADATA of every producer active within this window (a
    // record-less batch when its last record is compacted away) so a
    // producer-state rebuild still knows it; older producers are expired.
    // Default 7 days.  Overridable via RUNE_PRODUCER_ID_EXPIRATION_MS (>= 0).
    int64_t producerIdExpirationMs = 7LL * 24 * 3600 * 1000;

    // hotpath-A F2 + review M3: READY (pre-provisioned, fallocated) segments
    // the SegmentProvisionerAgent builds for a disk-backed user log — DEMAND-
    // DRIVEN: nothing is reserved until the log's active segment is
    // segmentProvisionThresholdPct full; then up to `depth` ready segments are
    // built (the next roll adopts one with zero syscalls).  Idle partitions
    // reserve 0 extra blocks / fds / mmaps.  Worst-case disk reservation per
    // busy partition: depth x segmentRollBytes (fallocate KEEP_SIZE), bounded
    // node-wide by segmentProvisionCapBytes.  A roll that arrives before its
    // ready segment is deferred (the intake holds, bounded); a build failure or
    // the cap makes that roll inline (blocking, correct, counted).
    // depth: clamped to [1, Log::kMaxProvisionDepth].  RUNE_SEGMENT_PROVISION_DEPTH.
    int32_t segmentProvisionDepth = 1;
    // Fill level (% of segmentRollBytes, 1..100) at which a log demands its
    // next segment.  RUNE_SEGMENT_PROVISION_THRESHOLD_PCT.
    int32_t segmentProvisionThresholdPct = 50;
    // Node-wide cap on bytes reserved (fallocated) by ready segments: > 0
    // fixed, 0 = no cap, < 0 = AUTO (default): segmentProvisionCapFreePct % of
    // the free space of dataDir (statvfs, re-evaluated off-thread).  Over the
    // cap a ready segment is still built — without fallocate (re-review N3).
    // RUNE_SEGMENT_PROVISION_CAP_BYTES / RUNE_SEGMENT_PROVISION_CAP_FREE_PCT.
    int64_t segmentProvisionCapBytes = -1;
    int32_t segmentProvisionCapFreePct = 10;
    // The provisioner thread's park cap (µs) when idle.  Demands ring its
    // doorbell (review m5), so this bounds only fd-less work (replica-view
    // updates).  RUNE_SEGMENT_PROVISIONER_MAX_PARK_US (>= 1).
    int64_t segmentProvisionerMaxParkUs = 10'000;

    // Segment roll size (bytes) for the internal __consumer_offsets log.
    // Default 64 MiB matches TopicSpec::segmentRollBytes.
    // Tests may set a small value (e.g. 128) to force multiple sealed segments
    // so LogCleanerAgent can run compaction on the offset log.
    int64_t offsetLogSegmentRollBytes = 64LL * 1024 * 1024;

    /// Returns the peer entry whose id == nodeId.
    /// Precondition: parse succeeded (peers non-empty, nodeId in peers).
    const PeerConfig& self() const;

    bool isSingleNode() const { return peers.size() == 1; }

    /// Task 1 (per-worker replication transport): validate that the per-worker
    /// replication stream range [replBaseStreamId, replBaseStreamId+workerCount-1]
    /// does not overlap any other configured stream id or range:
    ///   - clientStreamId (clientEndpoint.streamId)
    ///   - workerIngress range [workerIngressBaseStreamId, +workerCount-1]
    ///   - workerEgress range [workerEgressBaseStreamId, +workerCount-1]  (if set)
    ///   - consensusStreamId
    ///   - replicationStreamId
    ///
    /// Returns std::nullopt when the configuration is valid.
    /// Returns an error string describing the first detected overlap when invalid.
    ///
    /// NOT called during parseNodeConfig.  Callers (e.g. BrokerNode startup)
    /// must invoke this after parsing and refuse to start if it returns an error.
    std::optional<std::string> validateStreamRanges() const;

    /// Catalog history Task 6: catalogReplStreamId must not collide with any
    /// stream on the replication channel — replicationStreamId, or a per-worker
    /// replication stream [replBaseStreamId, replBaseStreamId + workerCount).
    /// nullopt = valid; else the error.  Checked by parseNodeConfig and by
    /// BrokerNode at start (programmatic configs).
    std::optional<std::string> validateCatalogStream() const;
    /// Issues #15 / #111: F > the broker liveness timeout + 500 ms (a leadership move does not make a healthy broker stale) and
    /// R > F; a positive gate deadline.  Checked by the env parser (in-process rigs set the fields directly).
    std::optional<std::string> validateSecurityServing(bool stalenessExplicit = true, bool sessionEndExplicit = true) const;

    /// Authentication plan Task 9 (D31/D32).  nullopt = valid; else the explicit refusal naming the variable:
    ///   * DevDisabled with more than one peer ("RUNE_AUTH=disabled (dev mode) refuses to start as part of a
    ///     multi-broker cluster (RUNE_PEERS lists N brokers); enable authentication");
    ///   * Enabled: RUNE_CLUSTER_KEY_FILE (and RUNE_CLUSTER_KEY_FILE_NEXT when set) must exist, be readable and hold
    ///     >= 32 bytes after trimming (reads the file(s) — cold, startup thread).  A group- or world-accessible key file
    ///     REFUSES to start (M13, X102: launchers mount it 0400).
    /// InProcessUnauthenticated reads nothing.  Checked by BrokerNode::start (programmatic configs too) and main.
    /// Review C I4: with `keysOut` set, the keys read here are handed back (Enabled mode) - the ONE load of the cluster key:
    /// BrokerNode::start keeps them in a member and hands out copies, so the key validated is the key in use.
    std::optional<std::string> validateAuth(std::optional<rune::peerlink::ClusterKeys>* keysOut = nullptr) const;

    /// Combined fix program M4 (X012, X082, X083): with authentication Enabled, nullopt = valid; else the refusal for
    /// per-worker ingress outside shard mode, a multi-broker cluster outside shard mode, > 252 workers, > 16 peers or
    /// a node id > 65535.  Pure config; part of validateAuth().
    std::optional<std::string> validateAuthTopology() const;

    /// Refuses an Aeron dir that would destroy the log: the embedded driver runs
    /// with dir_delete_on_start + delete-on-shutdown and recursively deletes its
    /// directory, so aeronDir must not be a filesystem root, equal to dataDir, or an
    /// ancestor of dataDir.  Paths are compared absolute + weakly-canonical (symlinks
    /// resolved for the existing prefix).  Empty aeronDir (→ dataDir/aeron) is safe.
    std::optional<std::string> validateAeronDir() const;

    /// Shard mode: every port this node binds + co-located peers' advertised
    /// ranges, peer base ports, symmetric worker count, stream ranges.  nullopt =
    /// valid (always nullopt in legacy mode unless placement=fused).  BrokerNode
    /// refuses to start on an error (no silent fallback).
    std::optional<std::string> validateShardConfig() const;

    /// The produce INTAKE limit — a thin wrapper over THE batch-limit formula,
    /// Worker::maxAcceptableBatchBytes(ingress ring, return ring, replication
    /// outbound ring, replication term, egress term, client ingress term)
    /// (F18 ⇄ F15 S3 ⇄ F19 ⇄ F3, integration wave 2), lowered by an explicit
    /// RUNE_MAX_PRODUCE_BATCH_BYTES.  Larger → MessageTooLarge at the gateway and the
    /// worker ingress, before append.  Never a startup refusal (small rings /
    /// terms only lower the limit).  Replication terms apply even single-node.
    [[nodiscard]] int32_t produceIntakeLimit() const;
    /// THE formula alone over this config's raw ring / term sizes (no explicit
    /// override): Worker::maxAcceptableBatchBytes(ingress ring, return ring,
    /// repl outbound ring, term, term, term).  produceIntakeLimit() =
    /// min(this, explicit RUNE_MAX_PRODUCE_BATCH_BYTES if set).
    [[nodiscard]] std::size_t batchLimitFormula() const;

    /// Host part of clientEndpoint.channel (the client-facing bind host).
    std::string clientBindHost() const;
    std::string shardIngressBindChannel(int32_t w) const;  // aeron:udp?endpoint=<clientBindHost>:<ingressBase+w>
    std::string shardReplBindChannel(int32_t w) const;     // aeron:udp?endpoint=<peerBindHost>:<replBase+w>
    /// DR-15 fix round 1 (MEDIUM-1): the host this node BINDS its peer-facing
    /// inbound subscriptions on (consensus, replication, shard planes).  Never
    /// depends on this node's own DNS record (a k8s headless-service record
    /// appears only once the pod is Ready — after it bound): RUNE_PEER_BIND_HOST
    /// when set, else self().host when numeric, else the wildcard 0.0.0.0.
    /// self().host stays the ADVERTISED name peers dial.
    std::string peerBindHost() const;
    std::string selfConsensusBindChannel() const;     // aeron:udp?endpoint=<peerBindHost>:<consensusPort>
    std::string selfReplicationBindChannel() const;   // "" when replicationPort == 0
    std::string peerBindHostOverride;                 // RUNE_PEER_BIND_HOST (empty = derived)
    static std::string shardPeerReplChannel(const PeerConfig& p, int32_t w);  // <p.host>:<p.replBasePort+w>
    static std::string shardIngressEndpoint(const PeerConfig& p, int32_t w);  // "<p.host>:<p.ingressBasePort+w>"
};

struct ConfigResult {
    std::optional<NodeConfig> config;
    std::string               error;
    // Non-fatal, one line each, logged by the caller (main) at startup — e.g. a
    // a deprecated spelling.
    std::vector<std::string>  warnings;
};

/// Parse a Linux cpu list ("0", "0,4-5"); sorted + deduplicated.  nullopt on any malformed token,
/// a reversed range, an empty list or a CPU >= 1024 (CPU_SETSIZE).
[[nodiscard]] std::optional<std::vector<int32_t>> parseCpuList(std::string_view s);

/// Parse and validate node configuration from an injected env-lookup function.
/// Use this overload in tests (inject a fake env map) so tests never touch
/// the real process environment.
ConfigResult parseNodeConfig(
    const std::function<const char*(const char*)>& getenv_fn);

/// Convenience wrapper that delegates to ::getenv for production use.
ConfigResult parseNodeConfigFromEnv();

} // namespace rune::node
