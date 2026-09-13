#pragma once
// NodeConfig.h — env-driven broker node configuration (BR-1, BR-3b, GB-1, GB-4)
//
// Environment variables:
//   RUNE_NODE_ID           — int32, this node's id. Required. Must equal some peer's id.
//   RUNE_PEERS             — comma-separated id@host:consensusPort[:clientPort] entries.
//                            Required, non-empty. Ids must be unique and include RUNE_NODE_ID.
//                            Example (with client ports): "0@rune-1:40111:40200,1@rune-2:40111:40200,2@rune-3:40111:40200"
//                            Example (without client ports): "0@rune-1:40111,1@rune-2:40111,2@rune-3:40111"
//                            Backward-compatible: omitting :clientPort → clientPort=0 (no known client endpoint).
//                            Whitespace around commas and entries is trimmed.
//   RUNE_CLIENT_ENDPOINT   — host:port for the client gateway (Required).
//                            Built into "aeron:udp?endpoint=host:port".
//   RUNE_CLIENT_STREAM_ID  — int32, default 1.
//   RUNE_CONSENSUS_STREAM_ID — int32, default 10.
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
};

struct PeerConfig {
    int32_t  id            = -1;
    std::string host;
    uint16_t consensusPort = 0;
    uint16_t clientPort    = 0;  // 0 = no known client endpoint (GB-4, backward-compatible)

    /// "aeron:udp?endpoint=<host>:<consensusPort>"
    std::string consensusChannel() const;

    /// "aeron:udp?endpoint=<host>:<clientPort>" — empty string if clientPort == 0.
    std::string clientChannel() const;
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
    int32_t             consensusStreamId = 0;

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
