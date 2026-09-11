#include <rune/node/NodeConfig.h>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <sstream>
#include <string_view>
#include <unordered_set>

namespace rune::node {

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

namespace {

/// Trim leading and trailing ASCII whitespace from a string_view.
static std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t' ||
                           s.front() == '\r' || s.front() == '\n'))
        s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' ||
                           s.back() == '\r' || s.back() == '\n'))
        s.remove_suffix(1);
    return s;
}

/// Parse an int32_t from sv using from_chars. Returns false on failure.
static bool parseInt32(std::string_view sv, int32_t& out) {
    sv = trim(sv);
    if (sv.empty()) return false;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

/// Parse a uint16_t port from sv using from_chars. Returns false on failure
/// (also rejects 0 and values > 65535).
static bool parsePort(std::string_view sv, uint16_t& out) {
    sv = trim(sv);
    if (sv.empty()) return false;
    uint32_t tmp{};
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), tmp);
    if (ec != std::errc{} || ptr != sv.data() + sv.size()) return false;
    if (tmp == 0 || tmp > 65535) return false;
    out = static_cast<uint16_t>(tmp);
    return true;
}

/// Parse one peer entry of the form "id@host:port". Returns an error string
/// on failure or an empty string on success.
static std::string parsePeer(std::string_view entry, PeerConfig& out) {
    auto at = entry.find('@');
    if (at == std::string_view::npos)
        return std::string("malformed peer entry (missing '@'): ") + std::string(entry);

    std::string_view idPart   = trim(entry.substr(0, at));
    std::string_view hostPort = trim(entry.substr(at + 1));

    int32_t id{};
    if (!parseInt32(idPart, id))
        return std::string("peer id is not a valid integer in entry: ") + std::string(entry);

    auto colon = hostPort.rfind(':');
    if (colon == std::string_view::npos)
        return std::string("malformed peer entry (missing ':' in host:port): ") + std::string(entry);

    std::string_view host     = trim(hostPort.substr(0, colon));
    std::string_view portPart = trim(hostPort.substr(colon + 1));

    if (host.empty())
        return std::string("peer host is empty in entry: ") + std::string(entry);

    uint16_t port{};
    if (!parsePort(portPart, port))
        return std::string("peer port is invalid or out of range in entry: ") + std::string(entry);

    out.id            = id;
    out.host          = std::string(host);
    out.consensusPort = port;
    return {};
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// PeerConfig
// ---------------------------------------------------------------------------

std::string PeerConfig::consensusChannel() const {
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(consensusPort);
}

// ---------------------------------------------------------------------------
// NodeConfig
// ---------------------------------------------------------------------------

const PeerConfig& NodeConfig::self() const {
    for (auto& p : peers)
        if (p.id == nodeId) return p;
    // Precondition violated — should never happen after successful parse.
    static PeerConfig sentinel{};
    return sentinel;
}

// ---------------------------------------------------------------------------
// parseNodeConfig
// ---------------------------------------------------------------------------

ConfigResult parseNodeConfig(
    const std::function<const char*(const char*)>& getenv_fn)
{
    // --- RUNE_NODE_ID (required) ---
    const char* rawNodeId = getenv_fn("RUNE_NODE_ID");
    if (!rawNodeId || std::string_view(rawNodeId).empty())
        return {{}, "RUNE_NODE_ID is required"};
    int32_t nodeId{};
    if (!parseInt32(rawNodeId, nodeId))
        return {{}, std::string("RUNE_NODE_ID is not a valid integer: ") + rawNodeId};

    // --- RUNE_PEERS (required, non-empty) ---
    const char* rawPeers = getenv_fn("RUNE_PEERS");
    if (!rawPeers || std::string_view(rawPeers).empty())
        return {{}, "RUNE_PEERS is required"};

    std::vector<PeerConfig> peers;
    {
        std::string_view sv(rawPeers);
        // Reject trailing comma: after trimming the outer whitespace, if it
        // ends with ',' we would silently skip the implied empty final entry.
        // Detect and reject it explicitly.
        {
            std::string_view trimmed = trim(sv);
            if (!trimmed.empty() && trimmed.back() == ',')
                return {{}, "RUNE_PEERS contains an empty peer entry (trailing comma)"};
        }
        while (!sv.empty()) {
            auto comma = sv.find(',');
            std::string_view entry = (comma == std::string_view::npos)
                                       ? sv
                                       : sv.substr(0, comma);
            sv.remove_prefix(comma == std::string_view::npos ? sv.size() : comma + 1);

            entry = trim(entry);
            if (entry.empty()) {
                // Double comma or leading comma — treat as malformed entry
                return {{}, "RUNE_PEERS contains an empty peer entry"};
            }

            PeerConfig pc;
            std::string err = parsePeer(entry, pc);
            if (!err.empty()) return {{}, err};
            peers.push_back(std::move(pc));
        }
    }

    if (peers.empty())
        return {{}, "RUNE_PEERS must contain at least one peer entry"};

    // Check duplicate ids
    {
        std::unordered_set<int32_t> seen;
        for (auto& p : peers) {
            if (!seen.insert(p.id).second)
                return {{}, std::string("duplicate peer id ") + std::to_string(p.id) + " in RUNE_PEERS"};
        }
    }

    // Sort by id
    std::sort(peers.begin(), peers.end(),
              [](const PeerConfig& a, const PeerConfig& b){ return a.id < b.id; });

    // Verify nodeId is present
    bool found = std::any_of(peers.begin(), peers.end(),
                             [&](const PeerConfig& p){ return p.id == nodeId; });
    if (!found)
        return {{}, std::string("nodeId ") + std::to_string(nodeId) + " not found in RUNE_PEERS"};

    // --- RUNE_CLIENT_ENDPOINT (required) ---
    const char* rawClient = getenv_fn("RUNE_CLIENT_ENDPOINT");
    if (!rawClient || std::string_view(rawClient).empty())
        return {{}, "RUNE_CLIENT_ENDPOINT is required"};

    // --- RUNE_DATA_DIR (required, non-empty) ---
    const char* rawDataDir = getenv_fn("RUNE_DATA_DIR");
    if (!rawDataDir || std::string_view(rawDataDir).empty())
        return {{}, "RUNE_DATA_DIR is required"};

    // --- RUNE_CLIENT_STREAM_ID (optional, default 1) ---
    int32_t clientStreamId = 1;
    const char* rawClientStream = getenv_fn("RUNE_CLIENT_STREAM_ID");
    if (rawClientStream && !std::string_view(rawClientStream).empty()) {
        if (!parseInt32(rawClientStream, clientStreamId))
            return {{}, std::string("RUNE_CLIENT_STREAM_ID is not a valid integer: ") + rawClientStream};
    }

    // --- RUNE_CONSENSUS_STREAM_ID (optional, default 10) ---
    int32_t consensusStreamId = 10;
    const char* rawConsStream = getenv_fn("RUNE_CONSENSUS_STREAM_ID");
    if (rawConsStream && !std::string_view(rawConsStream).empty()) {
        if (!parseInt32(rawConsStream, consensusStreamId))
            return {{}, std::string("RUNE_CONSENSUS_STREAM_ID is not a valid integer: ") + rawConsStream};
    }

    // --- Build result ---
    NodeConfig cfg;
    cfg.nodeId           = nodeId;
    cfg.peers            = std::move(peers);
    cfg.dataDir          = std::string(rawDataDir);
    cfg.consensusStreamId = consensusStreamId;

    cfg.clientEndpoint.channel  = std::string("aeron:udp?endpoint=") + rawClient;
    cfg.clientEndpoint.streamId = clientStreamId;

    return {std::move(cfg), {}};
}

// ---------------------------------------------------------------------------
// parseNodeConfigFromEnv
// ---------------------------------------------------------------------------

ConfigResult parseNodeConfigFromEnv() {
    return parseNodeConfig([](const char* name) -> const char* {
        return std::getenv(name);
    });
}

} // namespace rune::node
