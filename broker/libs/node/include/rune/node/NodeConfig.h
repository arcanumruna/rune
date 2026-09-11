#pragma once
// NodeConfig.h — env-driven broker node configuration (BR-1, BR-3b)
//
// Environment variables:
//   RUNE_NODE_ID           — int32, this node's id. Required. Must equal some peer's id.
//   RUNE_PEERS             — comma-separated id@host:consensusPort entries.
//                            Required, non-empty. Ids must be unique and include RUNE_NODE_ID.
//                            Example: "0@rune-1:40111,1@rune-2:40111,2@rune-3:40111"
//                            Whitespace around commas and entries is trimmed.
//   RUNE_CLIENT_ENDPOINT   — host:port for the client gateway (Required).
//                            Built into "aeron:udp?endpoint=host:port".
//   RUNE_CLIENT_STREAM_ID  — int32, default 1.
//   RUNE_CONSENSUS_STREAM_ID — int32, default 10.
//   RUNE_DATA_DIR          — filesystem path for durable data. Required, non-empty.
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

struct PeerConfig {
    int32_t  id            = -1;
    std::string host;
    uint16_t consensusPort = 0;

    /// "aeron:udp?endpoint=<host>:<consensusPort>"
    std::string consensusChannel() const;
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
