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
