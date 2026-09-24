#include <rune/node/NodeConfig.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
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

    // --- RUNE_WORKER_INGRESS_BASE_STREAM (optional, default clientStreamId+1) ---
    // E5.1: base Aeron stream id for per-worker ingress subscriptions.
    // Resolved after clientStreamId is known; 0 = unset → use clientStreamId+1.
    int32_t workerIngressBaseStreamId = 0;  // resolved below after NodeConfig is built
    const char* rawWorkerIngressBase = getenv_fn("RUNE_WORKER_INGRESS_BASE_STREAM");
    if (rawWorkerIngressBase && !std::string_view(rawWorkerIngressBase).empty()) {
        if (!parseInt32(rawWorkerIngressBase, workerIngressBaseStreamId))
            return {{}, std::string("RUNE_WORKER_INGRESS_BASE_STREAM is not a valid integer: ") + rawWorkerIngressBase};
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

    // E5.1: resolve workerIngressBaseStreamId — default to clientStreamId+1 when unset.
    cfg.workerIngressBaseStreamId = (workerIngressBaseStreamId == 0)
                                   ? (clientStreamId + 1)
                                   : workerIngressBaseStreamId;

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
    //       [:retentionMs[:retentionBytes[:retentionCheckMs[:fileDeleteDelayMs
    //       [:minCleanableDirtyRatio[:maxCompactionLagMs]]]]]]]]]
    //
    // Optional fields (all backward-compatible — older format "name:N" still works;
    // any field may be left empty to keep its default, e.g. "n:1:compact:4096::::::0.0"):
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
    //   minCleanableDirtyRatio — Kafka min.cleanable.dirty.ratio, double [0.0,1.0];
    //                       default 0.5.  0.0 = compact to latest-per-key every pass
    //                       (no threshold gating); used by tests asserting full
    //                       convergence and by latency-sensitive compacted topics.
    //   maxCompactionLagMs — Kafka max.compaction.lag.ms; -1 = disabled (default).
    //                       Force compaction of a dirty segment once it ages past this.
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

                // Split the entry by ':'.
                // Field 0: name, Field 1: partitionCount.
                // Remaining fields can be either:
                //   (a) Positional legacy fields (no '=' in token): policy, segmentRollBytes, etc.
                //   (b) Named key=value tokens (contain '='): group=<g>, key=<k>,
                //       persistence=memory|disk, durability=majority|all-in-sync.
                // Both formats are backward-compatible: a token is key=value iff it
                // contains '='; otherwise it is a positional legacy field.
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

                // Separate remaining fields into positional and key=value slots.
                // A token is key=value iff it contains exactly one '='.
                // Positional tokens (legacy) occupy fields[2..N] in order; key=value
                // tokens are routed by their key name.  Mixing is allowed: the format
                // "name:N:compact:group=acct" would set policy=compact + group=acct.
                std::vector<std::string_view> positional; // legacy positional fields[2..]
                for (std::size_t fi = 2; fi < fields.size(); ++fi) {
                    const std::string_view f = fields[fi];
                    if (f.empty()) {
                        // Empty colon-field in positional format: keep a placeholder so
                        // later positional fields land at the right index.
                        positional.push_back(f);
                        continue;
                    }
                    auto eq = f.find('=');
                    if (eq == std::string_view::npos) {
                        // Positional legacy field.
                        positional.push_back(f);
                        continue;
                    }
                    // key=value token.
                    std::string_view kname = trim(f.substr(0, eq));
                    std::string_view kval  = trim(f.substr(eq + 1));

                    // Convert key to lower-case for comparison.
                    std::string klower(kname);
                    for (auto& ch : klower) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));

                    if (klower == "group") {
                        ts.coPartitionGroup = std::string(kval);
                    } else if (klower == "key") {
                        ts.partitionKey = std::string(kval);
                    } else if (klower == "persistence") {
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "memory") {
                            ts.persistence = PersistenceMode::Memory;
                        } else if (vlow == "disk") {
                            ts.persistence = PersistenceMode::Disk;
                        } else {
                            return {{}, std::string("RUNE_TOPICS persistence must be 'disk' or 'memory', got: ") + std::string(kval) + " in entry: " + std::string(entry)};
                        }
                    } else if (klower == "durability") {
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "majority") {
                            ts.durability = 0;
                        } else if (vlow == "all-in-sync") {
                            ts.durability = 1;
                        } else {
                            return {{}, std::string("RUNE_TOPICS durability must be 'majority' or 'all-in-sync', got: ") + std::string(kval) + " in entry: " + std::string(entry)};
                        }
                    } else {
                        return {{}, std::string("RUNE_TOPICS unknown option '") + std::string(kname) + "' in entry: " + std::string(entry)};
                    }
                }

                // Process legacy positional fields (using positional[] vector so index
                // 0 = policy, 1 = segmentRollBytes, etc., equivalent to fields[2..]).
                // Helper: access positional field by 0-based index.
                auto posField = [&](std::size_t idx) -> std::string_view {
                    return idx < positional.size() ? positional[idx] : std::string_view{};
                };

                // Positional[0] (optional): policy — "delete", "compact", "compact_delete"
                if (!posField(0).empty()) {
                    std::string pol(posField(0));
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

                // Helper: parse an optional positional int64 field (positive or -1).
                auto parseOptField = [&](std::size_t idx, const char* fieldName, int64_t& out, bool allowNeg1) -> std::string {
                    const auto sv = posField(idx);
                    if (sv.empty()) return {};
                    int64_t v{};
                    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), v);
                    if (ec != std::errc{} || ptr != sv.data() + sv.size())
                        return std::string("RUNE_TOPICS ") + fieldName + " must be an integer in: " + std::string(entry);
                    if (!allowNeg1 && v < 1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be a positive integer in: " + std::string(entry);
                    if (allowNeg1 && v < -1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be >= -1 in: " + std::string(entry);
                    out = v;
                    return {};
                };

                // Positional[1] (optional): segmentRollBytes (positive int64)
                {
                    std::string err = parseOptField(1, "segmentRollBytes", ts.segmentRollBytes, false);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[2] (optional): deleteRetentionMs (non-negative int64)
                {
                    std::string err = parseOptField(2, "deleteRetentionMs", ts.deleteRetentionMs, false);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[3] (optional): retentionMs (-1 = unbounded, or positive ms)
                {
                    std::string err = parseOptField(3, "retentionMs", ts.retentionMs, true);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[4] (optional): retentionBytes (-1 = unbounded, or positive bytes)
                {
                    std::string err = parseOptField(4, "retentionBytes", ts.retentionBytes, true);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[5] (optional): retentionCheckMs (positive int64)
                {
                    std::string err = parseOptField(5, "retentionCheckMs", ts.retentionCheckMs, false);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[6] (optional): fileDeleteDelayMs (positive int64)
                {
                    std::string err = parseOptField(6, "fileDeleteDelayMs", ts.fileDeleteDelayMs, false);
                    if (!err.empty()) return {{}, err};
                }

                // Positional[7] (optional): minCleanableDirtyRatio (double in [0.0, 1.0]).
                // Kafka min.cleanable.dirty.ratio.  Default 0.5.  Set 0.0 to make the
                // compaction cleaner sweep the whole log to latest-per-key every pass
                // (no threshold gating) — e.g. for tests that assert full convergence.
                if (!posField(7).empty()) {
                    std::string s(posField(7));
                    char* endp = nullptr;
                    errno = 0;
                    double v = std::strtod(s.c_str(), &endp);
                    if (endp != s.c_str() + s.size() || errno != 0)
                        return {{}, std::string("RUNE_TOPICS minCleanableDirtyRatio must be a number in: ") + std::string(entry)};
                    if (v < 0.0 || v > 1.0)
                        return {{}, std::string("RUNE_TOPICS minCleanableDirtyRatio must be in [0.0, 1.0] in: ") + std::string(entry)};
                    ts.minCleanableDirtyRatio = v;
                }

                // Positional[8] (optional): maxCompactionLagMs (-1 = disabled, or positive ms).
                // Kafka max.compaction.lag.ms — force compaction of a dirty segment once
                // it ages past this, even below minCleanableDirtyRatio.
                {
                    std::string err = parseOptField(8, "maxCompactionLagMs", ts.maxCompactionLagMs, true);
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

    // --- RUNE_WORKERS (optional) — Milestone E1: in-process partition worker count (floor 1). ---
    if (const char* w = getenv_fn("RUNE_WORKERS")) {
        char* end = nullptr;
        const long v = std::strtol(w, &end, 10);
        cfg.workerCount = (end != w && v >= 1) ? static_cast<int32_t>(v) : 1;
    } else {
        cfg.workerCount = 1;
    }

    // --- Ring-buffer sizes (shared-nothing conformance) — optional, power-of-two. ---
    auto parsePow2 = [](const char* raw, int32_t& out) {
        if (!raw || std::string_view(raw).empty()) return;   // keep default
        char* end = nullptr;
        const long v = std::strtol(raw, &end, 10);
        // Accept only positive powers of two; else keep the default.
        if (end != raw && v > 0 && (v & (v - 1)) == 0) out = static_cast<int32_t>(v);
    };
    parsePow2(getenv_fn("RUNE_REPL_OUTBOUND_RING_BYTES"), cfg.replicationOutboundRingBytes);
    parsePow2(getenv_fn("RUNE_WORKER_INGRESS_RING_BYTES"), cfg.workerIngressRingBytes);
    parsePow2(getenv_fn("RUNE_WORKER_RETURN_RING_BYTES"),  cfg.workerReturnRingBytes);
    parsePow2(getenv_fn("RUNE_IO_SUBMIT_RING_DEPTH"),     cfg.ioSubmitRingDepth);
    parsePow2(getenv_fn("RUNE_IO_COMPLETION_RING_DEPTH"), cfg.ioCompletionRingDepth);
    parsePow2(getenv_fn("RUNE_MEMORY_LOG_RING_BYTES"),    cfg.memoryLogRingBytes);
    parsePow2(getenv_fn("RUNE_AERON_TERM_BUFFER_BYTES"),  cfg.aeronTermBufferBytes);
    // RUNE_AERON_MTU: MTU is not required to be a power of two — use parseInt32.
    if (const char* raw = getenv_fn("RUNE_AERON_MTU")) {
        if (raw && !std::string_view(raw).empty()) {
            int32_t v{};
            if (parseInt32(raw, v)) {
                cfg.aeronMtuBytes = v;
            }
            // Unparseable → keep default 65504 (safe degradation).
        }
    }
    parsePow2(getenv_fn("RUNE_AERON_RCV_WINDOW"), cfg.aeronRcvWindowBytes);
    // RUNE_GATEWAY_FRAGMENT_LIMIT: not required to be a power of two — use parseInt32.
    if (const char* raw = getenv_fn("RUNE_GATEWAY_FRAGMENT_LIMIT")) {
        if (raw && !std::string_view(raw).empty()) {
            int32_t v{};
            if (parseInt32(raw, v)) {
                cfg.gatewayFragmentLimit = v;
            }
            // Unparseable → keep default 256 (safe degradation).
        }
    }

    // --- RUNE_WORKER_IDLE (optional) — Milestone E1/E2: per-worker idle strategy selector. ---
    // E1: parsed + stored here; no per-worker AgentRunner yet (driven cooperatively).
    // E2: will wire this into each worker's AgentRunner.
    // Unrecognised values are floored to "backoff" (must NOT default to busy-spin).
    if (const char* wi = getenv_fn("RUNE_WORKER_IDLE")) {
        std::string s(wi);
        // Case-insensitive compare via tolower.
        for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (s == "busyspin" || s == "busy_spin" || s == "busy-spin") {
            cfg.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
        } else if (s == "yielding") {
            cfg.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::Yielding;
        } else if (s == "sleeping") {
            cfg.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::Sleeping;
        } else {
            // "backoff" or any unrecognised value → default (safe, no core pegging).
            cfg.workerIdleStrategy = NodeConfig::WorkerIdleStrategy::Backoff;
        }
    }
    // Absent → keep NodeConfig default (Backoff).

    // --- RUNE_WORKER_CORE_BASE (optional) — E4: CPU affinity base core. ---
    // -1 = disabled (no pinning, default).  >= 0 → worker i pins to core base+i.
    // Values < -1 are clamped to -1 (disabled).
    if (const char* wcb = getenv_fn("RUNE_WORKER_CORE_BASE")) {
        if (wcb && !std::string_view(wcb).empty()) {
            int32_t v{};
            if (parseInt32(wcb, v)) {
                cfg.workerCoreBase = (v < -1) ? -1 : v;
            }
            // Unparseable → keep default -1 (disabled; no error — safe degradation).
        }
    }
    // Absent → keep NodeConfig default (-1 = disabled).

    // --- RUNE_AERON_THREADING_MODE (optional, default Shared) — E5.2 ---
    // "shared" (default) or "dedicated" (case-insensitive).
    // Any unrecognised value floors to Shared (must not change default behaviour).
    if (const char* atm = getenv_fn("RUNE_AERON_THREADING_MODE")) {
        if (atm && !std::string_view(atm).empty()) {
            std::string s(atm);
            for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            if (s == "dedicated") {
                cfg.aeronThreadingMode = NodeConfig::AeronThreadingMode::Dedicated;
            } else {
                // "shared" or any unrecognised value → default Shared.
                cfg.aeronThreadingMode = NodeConfig::AeronThreadingMode::Shared;
            }
        }
    }
    // Absent → keep NodeConfig default (Shared).

    // --- RUNE_AERON_DRIVER_IDLE (optional, default "sleep-ns") — E5.2 ---
    // Passed to the Aeron idle-strategy setter(s) for the active threading mode.
    // Default "sleep-ns" is byte-identical to today's hard-coded value.
    if (const char* adi = getenv_fn("RUNE_AERON_DRIVER_IDLE")) {
        if (adi && !std::string_view(adi).empty()) {
            cfg.driverIdleStrategy = std::string(adi);
        }
    }
    // Absent → keep NodeConfig default ("sleep-ns").

    // --- RUNE_PER_WORKER_INGRESS (optional, default false) — E5.1.3 ---
    // When "1" or "true", each worker gets its own Aeron subscription and the
    // gateway produce fanout is gated off (!perWorkerIngressEnabled).
    // Default false: gateway fanout remains the live production path.
    if (const char* pwi = getenv_fn("RUNE_PER_WORKER_INGRESS")) {
        if (pwi && !std::string_view(pwi).empty()) {
            std::string_view sv(pwi);
            cfg.perWorkerIngressEnabled = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }
    // Absent → keep NodeConfig default (false = gateway fanout is live path).

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
