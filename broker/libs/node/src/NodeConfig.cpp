#include <rune/node/NodeConfig.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <sstream>
#include <string_view>
#include <unordered_set>
#include <vector>

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

/// Parse a positive int64_t from sv using from_chars. Returns false on failure.
static bool parseInt64Positive(std::string_view sv, int64_t& out) {
    sv = trim(sv);
    if (sv.empty()) return false;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    if (ec != std::errc{} || ptr != sv.data() + sv.size()) return false;
    return out > 0;
}

/// Parse an optional int64_t env var (positive); leave 'out' unchanged if var is unset/empty.
static std::string parseOptionalInt64Ms(
    const std::function<const char*(const char*)>& getenv_fn,
    const char* varName, int64_t& out)
{
    const char* raw = getenv_fn(varName);
    if (!raw || std::string_view(raw).empty()) return {}; // use default
    if (!parseInt64Positive(raw, out))
        return std::string(varName) + " must be a positive integer (ms), got: " + raw;
    return {};
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

/// Parse one peer entry of the form "id@host:consensusPort[:clientPort[:replPort]]".
/// Optional segments (backward-compatible):
///   - "1@host:40111"               → consensusPort=40111, clientPort=0, replPort=0
///   - "1@host:40111:40200"         → consensusPort=40111, clientPort=40200, replPort=0
///   - "1@host:40111:40200:40300"   → consensusPort=40111, clientPort=40200, replPort=40300
/// Returns an error string on failure or an empty string on success.
static std::string parsePeer(std::string_view entry, PeerConfig& out) {
    auto at = entry.find('@');
    if (at == std::string_view::npos)
        return std::string("malformed peer entry (missing '@'): ") + std::string(entry);

    std::string_view idPart   = trim(entry.substr(0, at));
    std::string_view hostPort = trim(entry.substr(at + 1));

    int32_t id{};
    if (!parseInt32(idPart, id))
        return std::string("peer id is not a valid integer in entry: ") + std::string(entry);

    // Split on the FIRST colon for host.
    auto firstColon = hostPort.find(':');
    if (firstColon == std::string_view::npos)
        return std::string("malformed peer entry (missing ':' in host:port): ") + std::string(entry);

    std::string_view host        = trim(hostPort.substr(0, firstColon));
    std::string_view remainder   = trim(hostPort.substr(firstColon + 1));

    if (host.empty())
        return std::string("peer host is empty in entry: ") + std::string(entry);

    // remainder may be: "consensusPort" | "consensusPort:clientPort" | "consensusPort:clientPort:replPort"
    auto secondColon = remainder.find(':');

    std::string_view consensusPortPart;
    std::string_view clientPortPart;
    std::string_view replPortPart;
    bool hasClientPort = false;
    bool hasReplPort   = false;

    if (secondColon == std::string_view::npos) {
        // Two-segment: no clientPort.
        consensusPortPart = remainder;
    } else {
        // Three-or-four-segment: consensusPort:clientPort[:replPort].
        consensusPortPart = trim(remainder.substr(0, secondColon));
        std::string_view rest2 = trim(remainder.substr(secondColon + 1));
        auto thirdColon = rest2.find(':');
        if (thirdColon == std::string_view::npos) {
            clientPortPart = rest2;
        } else {
            clientPortPart = trim(rest2.substr(0, thirdColon));
            replPortPart   = trim(rest2.substr(thirdColon + 1));
            hasReplPort    = true;
        }
        hasClientPort = true;
    }

    uint16_t consensusPort{};
    if (!parsePort(consensusPortPart, consensusPort))
        return std::string("peer consensusPort is invalid or out of range in entry: ") + std::string(entry);

    uint16_t clientPort = 0;
    if (hasClientPort) {
        if (!parsePort(clientPortPart, clientPort))
            return std::string("peer clientPort is invalid or out of range in entry: ") + std::string(entry);
    }

    uint16_t replPort = 0;
    if (hasReplPort) {
        if (!parsePort(replPortPart, replPort))
            return std::string("peer replicationPort is invalid or out of range in entry: ") + std::string(entry);
    }

    out.id               = id;
    out.host             = std::string(host);
    out.consensusPort    = consensusPort;
    out.clientPort       = clientPort;
    out.replicationPort  = replPort;
    return {};
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// PeerConfig
// ---------------------------------------------------------------------------

std::string PeerConfig::consensusChannel() const {
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(consensusPort);
}

std::string PeerConfig::clientChannel() const {
    if (clientPort == 0) return {};  // no known client endpoint
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(clientPort);
}

std::string PeerConfig::replicationChannel() const {
    if (replicationPort == 0) return {};  // no replication endpoint
    return "aeron:udp?endpoint=" + host + ":" + std::to_string(replicationPort);
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

    // --- RUNE_REPLICATION_STREAM_ID (optional, default 20) ---
    int32_t replicationStreamId = 20;
    const char* rawReplStream = getenv_fn("RUNE_REPLICATION_STREAM_ID");
    if (rawReplStream && !std::string_view(rawReplStream).empty()) {
        if (!parseInt32(rawReplStream, replicationStreamId))
            return {{}, std::string("RUNE_REPLICATION_STREAM_ID is not a valid integer: ") + rawReplStream};
    }

    // --- Election timing (optional, positive ms, env-overridable) ---
    // Defaults set in NodeConfig field initializers; only override if env var present.
    NodeConfig cfg;
    cfg.nodeId              = nodeId;
    cfg.peers               = std::move(peers);
    cfg.dataDir             = std::string(rawDataDir);
    cfg.consensusStreamId   = consensusStreamId;
    cfg.replicationStreamId = replicationStreamId;

    cfg.clientEndpoint.channel  = std::string("aeron:udp?endpoint=") + rawClient;
    cfg.clientEndpoint.streamId = clientStreamId;

    {
        std::string err;
        err = parseOptionalInt64Ms(getenv_fn, "RUNE_ELECTION_TIMEOUT_MS",
                                   cfg.electionTimeoutMs);
        if (!err.empty()) return {{}, err};

        err = parseOptionalInt64Ms(getenv_fn, "RUNE_ELECTION_STATUS_INTERVAL_MS",
                                   cfg.electionStatusIntervalMs);
        if (!err.empty()) return {{}, err};

        err = parseOptionalInt64Ms(getenv_fn, "RUNE_STARTUP_CANVASS_TIMEOUT_MS",
                                   cfg.startupCanvassTimeoutMs);
        if (!err.empty()) return {{}, err};
    }

    // --- RUNE_MIN_ISR (optional) — M1: minimum ISR size for acks=All gate ---
    // Default: majority = |peers|/2+1 (1 for single-node, 2 for 3-node, etc.)
    {
        const int32_t majority = static_cast<int32_t>(cfg.peers.size()) / 2 + 1;
        cfg.gatewayMinIsr = majority; // may be overridden below
        const char* rawMinIsr = getenv_fn("RUNE_MIN_ISR");
        if (rawMinIsr && !std::string_view(rawMinIsr).empty()) {
            int32_t v{};
            if (!parseInt32(rawMinIsr, v) || v < 1)
                return {{}, std::string("RUNE_MIN_ISR must be a positive integer, got: ") + rawMinIsr};
            cfg.gatewayMinIsr = v;
        }
    }

    // --- RUNE_TOPICS (optional) ---
    // Format: comma-separated entries, each of the form:
    //   name:partitionCount[:policy[:segmentRollBytes[:deleteRetentionMs
    //       [:retentionMs[:retentionBytes[:retentionCheckMs[:fileDeleteDelayMs]]]]]]]
    //
    // Optional fields (all backward-compatible — older format "name:N" still works):
    //   policy          — "delete" (default), "compact", or "compact_delete".
    //   segmentRollBytes — positive int64 bytes; default 64 MiB.  Set small (e.g.
    //                      4096) in tests to force segment sealing quickly so that
    //                      the compaction/retention cleaner can actually run.
    //   deleteRetentionMs — tombstone delete-retention window (ms); default 86400000.
    //                       Set 0 to expire tombstones immediately after compaction.
    //   retentionMs     — time-based retention limit (ms); -1 = unbounded (default).
    //   retentionBytes  — size-based retention limit (bytes); -1 = unbounded (default).
    //   retentionCheckMs — how often the retention pass runs (ms); default 300000.
    //                      Set 1 in tests for near-immediate retention checks.
    //   fileDeleteDelayMs — delay before .deleted files are physically removed (ms);
    //                       default 60000.  Set 1 in tests.
    //
    // Empty/absent → no topics (valid — produce/subscribe returns UnknownTopic).
    {
        const char* rawTopics = getenv_fn("RUNE_TOPICS");
        if (rawTopics && !std::string_view(rawTopics).empty()) {
            std::string_view sv(rawTopics);
            std::unordered_set<std::string> seenNames;
            int32_t topicIdx = 0;
            while (!sv.empty()) {
                auto comma = sv.find(',');
                std::string_view entry = (comma == std::string_view::npos)
                                           ? sv
                                           : sv.substr(0, comma);
                sv.remove_prefix(comma == std::string_view::npos ? sv.size() : comma + 1);

                entry = trim(entry);
                if (entry.empty())
                    return {{}, "RUNE_TOPICS contains an empty entry (leading, trailing, or double comma)"};

                // Split the entry by ':' into up to 8 fields.
                // Field 0: name, Field 1: partitionCount, Field 2: policy,
                // Field 3: segmentRollBytes, Field 4: deleteRetentionMs,
                // Field 5: retentionMs, Field 6: retentionBytes,
                // Field 7: retentionCheckMs, Field 8: fileDeleteDelayMs
                std::vector<std::string_view> fields;
                {
                    std::string_view rem = entry;
                    while (!rem.empty()) {
                        auto c = rem.find(':');
                        fields.push_back(trim(c == std::string_view::npos ? rem : rem.substr(0, c)));
                        rem.remove_prefix(c == std::string_view::npos ? rem.size() : c + 1);
                    }
                }

                if (fields.size() < 2)
                    return {{}, std::string("RUNE_TOPICS entry missing ':' in: ") + std::string(entry)};

                std::string_view namePart  = fields[0];
                std::string_view countPart = fields[1];

                if (namePart.empty())
                    return {{}, std::string("RUNE_TOPICS entry has empty name in: ") + std::string(entry)};

                std::string name(namePart);
                if (!seenNames.insert(name).second)
                    return {{}, "RUNE_TOPICS has duplicate topic name: " + name};

                int32_t partCount{};
                if (!parseInt32(countPart, partCount) || partCount < 1)
                    return {{}, std::string("RUNE_TOPICS partition count must be >= 1, got: ") + std::string(countPart) + " in entry: " + std::string(entry)};

                TopicSpec ts;
                ts.name           = std::move(name);
                ts.partitionCount = partCount;
                ts.topicId        = topicIdx++;

                // Field 2 (optional): policy — "delete", "compact", "compact_delete"
                if (fields.size() >= 3 && !fields[2].empty()) {
                    std::string pol(fields[2]);
                    // Case-insensitive comparison.
                    for (auto& ch : pol) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                    if (pol == "delete") {
                        ts.cleanupPolicy = TopicSpec::CleanupPolicy::Delete;
                    } else if (pol == "compact") {
                        ts.cleanupPolicy = TopicSpec::CleanupPolicy::Compact;
                    } else if (pol == "compact_delete" || pol == "compactdelete") {
                        ts.cleanupPolicy = TopicSpec::CleanupPolicy::CompactDelete;
                    } else {
                        return {{}, std::string("RUNE_TOPICS policy must be 'delete', 'compact', or 'compact_delete', got: ") + pol + " in entry: " + std::string(entry)};
                    }
                }

                // Helper: parse an optional int64 field (positive or -1).
                auto parseOptField = [&](std::size_t idx, const char* fieldName, int64_t& out, bool allowNeg1) -> std::string {
                    if (fields.size() <= idx || fields[idx].empty()) return {};
                    int64_t v{};
                    auto [ptr, ec] = std::from_chars(fields[idx].data(), fields[idx].data() + fields[idx].size(), v);
                    if (ec != std::errc{} || ptr != fields[idx].data() + fields[idx].size())
                        return std::string("RUNE_TOPICS ") + fieldName + " must be an integer in: " + std::string(entry);
                    if (!allowNeg1 && v < 1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be a positive integer in: " + std::string(entry);
                    if (allowNeg1 && v < -1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be >= -1 in: " + std::string(entry);
                    out = v;
                    return {};
                };

                // Field 3 (optional): segmentRollBytes (positive int64)
                {
                    std::string err = parseOptField(3, "segmentRollBytes", ts.segmentRollBytes, false);
                    if (!err.empty()) return {{}, err};
                }

                // Field 4 (optional): deleteRetentionMs (non-negative int64)
                {
                    std::string err = parseOptField(4, "deleteRetentionMs", ts.deleteRetentionMs, false);
                    if (!err.empty()) return {{}, err};
                }

                // Field 5 (optional): retentionMs (-1 = unbounded, or positive ms)
                {
                    std::string err = parseOptField(5, "retentionMs", ts.retentionMs, true);
                    if (!err.empty()) return {{}, err};
                }

                // Field 6 (optional): retentionBytes (-1 = unbounded, or positive bytes)
                {
                    std::string err = parseOptField(6, "retentionBytes", ts.retentionBytes, true);
                    if (!err.empty()) return {{}, err};
                }

                // Field 7 (optional): retentionCheckMs (positive int64)
                {
                    std::string err = parseOptField(7, "retentionCheckMs", ts.retentionCheckMs, false);
                    if (!err.empty()) return {{}, err};
                }

                // Field 8 (optional): fileDeleteDelayMs (positive int64)
                {
                    std::string err = parseOptField(8, "fileDeleteDelayMs", ts.fileDeleteDelayMs, false);
                    if (!err.empty()) return {{}, err};
                }

                cfg.topics.push_back(std::move(ts));
            }
        }
        // Absent or empty RUNE_TOPICS → cfg.topics stays empty (valid).
    }

    // --- RUNE_ENABLE_COALESCING (optional, default true) ---
    // CP-10 Fix 2: parse the env var documented in NodeConfig.h so the documented
    // off-switch actually works.  "0" or "false" (case-insensitive) disables
    // compaction coalescing; any other non-empty value (including "1", "true")
    // keeps the default true.
    {
        const char* rawCoalesce = getenv_fn("RUNE_ENABLE_COALESCING");
        if (rawCoalesce && !std::string_view(rawCoalesce).empty()) {
            std::string_view sv = trim(std::string_view(rawCoalesce));
            // Convert to lower-case for case-insensitive comparison.
            std::string lower(sv);
            for (auto& ch : lower)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (lower == "0" || lower == "false") {
                cfg.enableCompactionCoalescing = false;
            } else {
                cfg.enableCompactionCoalescing = true;
            }
        }
        // Absent or empty → keep the default (true, set in NodeConfig initializers).
    }

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
