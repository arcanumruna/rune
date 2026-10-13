#include <rune/node/NodeConfig.h>
#include <rune/node/TopicName.h>
#include <rune/consensus/PeerResolution.h>
#include <rune/node/Worker.h>
#include <rune/storage/Log.h>                       // Log::kMaxRollBytes                         // Worker::maxAcceptableBatchBytes (THE batch limit)

#include <stdexcept>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <filesystem>
#include <optional>
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

/// Parse one peer entry of the form
///   "id@host:consensusPort[:clientPort[:replPort[:replBasePort[:ingressBasePort[:egressControlBasePort]]]]]".
/// Optional segments (backward-compatible; absent → 0):
///   - "1@host:40111"                         → consensusPort=40111
///   - "1@host:40111:40200"                   → + clientPort=40200
///   - "1@host:40111:40200:40300"             → + replPort=40300
///   - "1@host:40111:40200:40300:40600:40500" → + replBasePort=40600, ingressBasePort=40500
///     (self-contained worker shards, spec §3.3: worker w uses base + w)
///   - 6th segment: egressControlBasePort (per-worker dynamic-MDC egress control,
///     advertised to clients for partitions the peer leads; 2026-10-02).
/// More than 6 port segments is an error.
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
    auto firstColon = hostPort.find(':');
    if (firstColon == std::string_view::npos)
        return std::string("malformed peer entry (missing ':' in host:port): ") + std::string(entry);
    std::string_view host = trim(hostPort.substr(0, firstColon));
    if (host.empty())
        return std::string("peer host is empty in entry: ") + std::string(entry);

    // consensusPort[:clientPort[:replicationPort[:replBasePort[:ingressBasePort]]]]
    static constexpr const char* kNames[6] = {
        "consensusPort", "clientPort", "replicationPort", "replBasePort", "ingressBasePort",
        "egressControlBasePort"};
    std::array<std::string_view, 6> parts{};
    std::size_t n = 0;
    std::string_view rem = trim(hostPort.substr(firstColon + 1));
    for (;;) {
        if (n == parts.size())
            return std::string("too many ':' port segments (max 6) in peer entry: ") + std::string(entry);
        const auto c = rem.find(':');
        parts[n++] = trim(c == std::string_view::npos ? rem : rem.substr(0, c));
        if (c == std::string_view::npos) break;
        rem = rem.substr(c + 1);
    }
    std::array<uint16_t, 6> ports{};
    for (std::size_t i = 0; i < n; ++i) {
        if (!parsePort(parts[i], ports[i]))
            return std::string("peer ") + kNames[i] + " is invalid or out of range in entry: " +
                   std::string(entry);
    }
    out.id              = id;
    out.host            = std::string(host);
    out.consensusPort   = ports[0];
    out.clientPort      = ports[1];
    out.replicationPort = ports[2];
    out.replBasePort    = ports[3];
    out.ingressBasePort = ports[4];
    out.egressControlBasePort = ports[5];
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
// NodeConfig::validateStreamRanges  (Task 1 — per-worker replication transport)
// ---------------------------------------------------------------------------

std::optional<std::string> NodeConfig::validateStreamRanges() const {
    if (workerCount <= 0) return std::nullopt;  // empty range — nothing to check

    // Helper: does the range [base, base+count-1] contain the single stream id?
    auto rangeContains = [](int32_t base, int32_t count, int32_t id) -> bool {
        return id >= base && id < base + count;
    };

    // Helper: do two ranges [a, a+ac-1] and [b, b+bc-1] overlap?
    auto rangesOverlap = [](int32_t a, int32_t ac, int32_t b, int32_t bc) -> bool {
        return a < b + bc && b < a + ac;
    };

    const int32_t base  = replBaseStreamId;
    const int32_t count = workerCount;

    // Check vs. clientStreamId (single stream).
    if (rangeContains(base, count, clientEndpoint.streamId)) {
        return "RUNE_REPL_BASE_STREAM_ID range [" + std::to_string(base) + ", " +
               std::to_string(base + count - 1) + "] overlaps clientStreamId=" +
               std::to_string(clientEndpoint.streamId);
    }

    // Check vs. consensusStreamId (single stream).
    if (rangeContains(base, count, consensusStreamId)) {
        return "RUNE_REPL_BASE_STREAM_ID range [" + std::to_string(base) + ", " +
               std::to_string(base + count - 1) + "] overlaps consensusStreamId=" +
               std::to_string(consensusStreamId);
    }

    // Check vs. replicationStreamId (single stream).
    if (rangeContains(base, count, replicationStreamId)) {
        return "RUNE_REPL_BASE_STREAM_ID range [" + std::to_string(base) + ", " +
               std::to_string(base + count - 1) + "] overlaps replicationStreamId=" +
               std::to_string(replicationStreamId);
    }

    // Check vs. workerIngress range [workerIngressBaseStreamId, +workerCount-1].
    if (workerIngressBaseStreamId > 0 &&
        rangesOverlap(base, count, workerIngressBaseStreamId, workerCount)) {
        return "RUNE_REPL_BASE_STREAM_ID range [" + std::to_string(base) + ", " +
               std::to_string(base + count - 1) + "] overlaps workerIngress range [" +
               std::to_string(workerIngressBaseStreamId) + ", " +
               std::to_string(workerIngressBaseStreamId + workerCount - 1) + "]";
    }

    // Check vs. workerEgress range [workerEgressBaseStreamId, +workerCount-1]
    // (only when explicitly set — 0 means unresolved).
    if (workerEgressBaseStreamId > 0 &&
        rangesOverlap(base, count, workerEgressBaseStreamId, workerCount)) {
        return "RUNE_REPL_BASE_STREAM_ID range [" + std::to_string(base) + ", " +
               std::to_string(base + count - 1) + "] overlaps workerEgress range [" +
               std::to_string(workerEgressBaseStreamId) + ", " +
               std::to_string(workerEgressBaseStreamId + workerCount - 1) + "]";
    }

    return std::nullopt;  // no overlap detected
}

// ---------------------------------------------------------------------------
// Self-contained worker shards (spec §3.3): per-worker channels + validation
// ---------------------------------------------------------------------------

std::string NodeConfig::clientBindHost() const {
    const std::string_view ch = clientEndpoint.channel;
    const auto ep = ch.find("endpoint=");
    if (ep == std::string_view::npos) return "0.0.0.0";
    std::string_view hp = ch.substr(ep + 9);
    if (const auto bar = hp.find('|'); bar != std::string_view::npos) hp = hp.substr(0, bar);
    const auto colon = hp.rfind(':');
    return std::string(colon == std::string_view::npos ? hp : hp.substr(0, colon));
}
namespace {
// Absolute, symlink-resolved (for the existing prefix), lexically normal, with no
// trailing empty element, so "/a/b/", "/a/./b" and "/a/b/c/.." compare equal.
std::filesystem::path normalizedDir(const std::string& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path abs = fs::absolute(p, ec);
    if (ec) abs = fs::path{p};
    fs::path canon = fs::weakly_canonical(abs, ec);
    fs::path out = (ec ? abs : canon).lexically_normal();
    if (!out.has_filename() && out != out.root_path()) out = out.parent_path();
    return out;
}
} // namespace

std::optional<std::string> NodeConfig::validateAeronDir() const {
    if (aeronDir.empty()) return std::nullopt;   // default dataDir/aeron is always safe
    const auto a = normalizedDir(aeronDir);
    if (a == a.root_path())
        return "RUNE_AERON_DIR must not be a filesystem root (" + aeronDir +
               "): the embedded Aeron driver deletes its directory on start/shutdown";
    if (dataDir.empty()) return std::nullopt;
    const auto d = normalizedDir(dataDir);
    auto ai = a.begin();
    auto di = d.begin();
    for (; ai != a.end() && di != d.end(); ++ai, ++di)
        if (*ai != *di) return std::nullopt;     // diverged → neither contains the other
    if (ai == a.end())                           // aeronDir == dataDir or is its ancestor
        return "RUNE_AERON_DIR (" + aeronDir + ") equals or contains RUNE_DATA_DIR (" +
               dataDir + "): the embedded Aeron driver recursively deletes its directory "
               "on start/shutdown and would wipe the log";
    return std::nullopt;
}

// Shard channel helpers are only meaningful after validateShardConfig() passed
// (shard mode, explicit base ports, w in [0, W)).  Called on anything else they
// refuse loudly (logic_error) instead of dereferencing an unset optional or
// building a port-0 channel.
namespace {
void requireShardWorker(int32_t w, int32_t workerCount, const char* what) {
    const int32_t W = workerCount >= 1 ? workerCount : 1;
    if (w < 0 || w >= W)
        throw std::logic_error(std::string(what) + ": worker " + std::to_string(w) +
                               " out of range [0, " + std::to_string(W) + ")");
}
} // namespace

std::string NodeConfig::shardIngressBindChannel(int32_t w) const {
    if (!workerIngressBasePort)
        throw std::logic_error("shardIngressBindChannel: RUNE_WORKER_INGRESS_BASE_PORT unset "
                               "(only valid after validateShardConfig)");
    requireShardWorker(w, workerCount, "shardIngressBindChannel");
    return "aeron:udp?endpoint=" + clientBindHost() + ":" + std::to_string(*workerIngressBasePort + w);
}
std::string NodeConfig::shardReplBindChannel(int32_t w) const {
    if (!workerReplBasePort)
        throw std::logic_error("shardReplBindChannel: RUNE_WORKER_REPL_BASE_PORT unset "
                               "(only valid after validateShardConfig)");
    requireShardWorker(w, workerCount, "shardReplBindChannel");
    return "aeron:udp?endpoint=" + peerBindHost() + ":" + std::to_string(*workerReplBasePort + w);
}
std::string NodeConfig::peerBindHost() const {
    if (!peerBindHostOverride.empty()) return peerBindHostOverride;
    const std::string& h = self().host;
    return rune::consensus::PeerHostTable::isNumericHost(h) ? h : std::string("0.0.0.0");
}
std::string NodeConfig::selfConsensusBindChannel() const {
    return "aeron:udp?endpoint=" + peerBindHost() + ":" + std::to_string(self().consensusPort);
}
std::string NodeConfig::selfReplicationBindChannel() const {
    if (self().replicationPort == 0) return {};
    return "aeron:udp?endpoint=" + peerBindHost() + ":" + std::to_string(self().replicationPort);
}
std::string NodeConfig::shardPeerReplChannel(const PeerConfig& p, int32_t w) {
    if (p.replBasePort == 0 || w < 0)
        throw std::logic_error("shardPeerReplChannel: peer " + std::to_string(p.id) +
                               " has no replBasePort / bad worker " + std::to_string(w) +
                               " (only valid after validateShardConfig)");
    return "aeron:udp?endpoint=" + p.host + ":" + std::to_string(static_cast<int32_t>(p.replBasePort) + w);
}
std::string NodeConfig::shardIngressEndpoint(const PeerConfig& p, int32_t w) {
    if (p.ingressBasePort == 0 || w < 0)
        throw std::logic_error("shardIngressEndpoint: peer " + std::to_string(p.id) +
                               " has no ingressBasePort / bad worker " + std::to_string(w));
    return p.host + ":" + std::to_string(static_cast<int32_t>(p.ingressBasePort) + w);
}

// F18 ⇄ F15 S3 (integration wave 2): a thin wrapper over the ONE batch-limit
// formula.  Replication terms apply even single-node (the limit must not change
// when a node joins a cluster); every publication — replication and client
// egress — uses the embedded driver's term (no channel overrides term-length).
static_assert(NodeConfig::kDefaultMaxProduceBatchBytes == Worker::kMaxProduceBatchBytes,
              "one 8 MiB ceiling");
std::size_t NodeConfig::batchLimitFormula() const {
    const auto term = static_cast<std::size_t>(aeronTermBufferBytes);
    return Worker::maxAcceptableBatchBytes(
        static_cast<std::size_t>(workerIngressRingBytes),
        static_cast<std::size_t>(workerReturnRingBytes),
        static_cast<std::size_t>(replicationOutboundRingBytes),
        term,    // replication publications
        term,    // client egress publications
        term);   // client Produce frame on the broker's term (F3)
}

int32_t NodeConfig::produceIntakeLimit() const {
    std::size_t limit = batchLimitFormula();
    // F3 round 4: an EXPLICIT RUNE_MAX_PRODUCE_BATCH_BYTES can only LOWER it
    // (BrokerNode refuses to start when it exceeds the formula).
    if (maxProduceBatchBytes && *maxProduceBatchBytes > 0)
        limit = std::min(limit, static_cast<std::size_t>(*maxProduceBatchBytes));
    return static_cast<int32_t>(limit);
}

std::optional<std::string> NodeConfig::validateShardConfig() const {
    if (fused() && !shardMode())
        return std::string("RUNE_RUNTIME_PLACEMENT=fused requires RUNE_WORKER_MODE=shard");
    if (!shardMode()) return std::nullopt;

    const int32_t W = workerCount >= 1 ? workerCount : 1;
    if (W > kMaxShardWorkers)
        return "shard mode supports at most " + std::to_string(kMaxShardWorkers) +
               " workers per node, got RUNE_WORKERS=" + std::to_string(W);
    // Every per-worker base port must be explicit and its whole range [base, base+W-1]
    // must be a valid port range (no silent default in shard mode).
    auto checkBase = [W](const std::optional<int32_t>& base, const char* env) -> std::optional<std::string> {
        if (!base.has_value())
            return std::string("shard mode requires ") + env + " to be set explicitly";
        if (*base < 1 || *base + W - 1 > 65535)
            return std::string("shard mode requires ") + env + " in [1, " + std::to_string(65536 - W) +
                   "], got " + std::to_string(*base);
        return std::nullopt;
    };
    if (auto e = checkBase(workerIngressBasePort, "RUNE_WORKER_INGRESS_BASE_PORT")) return e;
    if (auto e = checkBase(workerReplBasePort, "RUNE_WORKER_REPL_BASE_PORT")) return e;
    if (auto e = checkBase(workerEgressControlBasePort, "RUNE_WORKER_EGRESS_CONTROL_BASE_PORT")) return e;
    const int32_t ingressBase = *workerIngressBasePort;
    const int32_t replBase    = *workerReplBasePort;
    const int32_t egressBase  = effectiveWorkerEgressControlBasePort();

    for (const auto& p : peers) {
        if (p.replBasePort == 0)
            return "shard mode requires replBasePort for peer " + std::to_string(p.id) +
                   " in RUNE_PEERS (id@host:consensus:client:repl:replBase:ingressBase)";
        if (p.ingressBasePort == 0)
            return "shard mode requires ingressBasePort for peer " + std::to_string(p.id) +
                   " in RUNE_PEERS (id@host:consensus:client:repl:replBase:ingressBase)";
        if (p.id != nodeId && p.workerCount != 0 && p.workerCount != W)
            return "shard mode requires a symmetric worker count: peer " + std::to_string(p.id) +
                   " advertises " + std::to_string(p.workerCount) + ", self has " + std::to_string(W);
    }
    const PeerConfig& me = self();
    if (me.replBasePort != replBase)
        return "RUNE_WORKER_REPL_BASE_PORT=" + std::to_string(replBase) +
               " disagrees with self RUNE_PEERS replBasePort=" + std::to_string(me.replBasePort) +
               " (an unadvertised base is the DERIVED one every peer computes — advertise it in RUNE_PEERS)";
    if (me.ingressBasePort != ingressBase)
        return "RUNE_WORKER_INGRESS_BASE_PORT=" + std::to_string(ingressBase) +
               " disagrees with self RUNE_PEERS ingressBasePort=" + std::to_string(me.ingressBasePort) +
               " (an unadvertised base is the DERIVED one every peer computes — advertise it in RUNE_PEERS)";
    if (me.egressControlBasePort != 0 && me.egressControlBasePort != egressBase)
        return "RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=" + std::to_string(egressBase) +
               " disagrees with self RUNE_PEERS egressControlBasePort=" + std::to_string(me.egressControlBasePort) +
               " (peers advertise it to clients; an unadvertised base is the DERIVED one — advertise it as the "
               "6th RUNE_PEERS segment)";

    struct Bind { int32_t port; std::string what; };
    std::vector<Bind> binds;
    binds.push_back({me.consensusPort, "consensusPort"});
    if (me.clientPort != 0)      binds.push_back({me.clientPort, "clientPort"});
    if (me.replicationPort != 0) binds.push_back({me.replicationPort, "replicationPort"});
    for (int32_t w = 0; w < W; ++w) {
        const std::string tag = "[w" + std::to_string(w) + "]";
        binds.push_back({ingressBase + w, "workerIngress" + tag});
        binds.push_back({replBase + w, "workerRepl" + tag});
        binds.push_back({egressBase + w, "workerEgressControl" + tag});
    }
    for (std::size_t i = 0; i < binds.size(); ++i)
        for (std::size_t j = i + 1; j < binds.size(); ++j)
            if (binds[i].port == binds[j].port)
                return "port collision: " + binds[i].what + " and " + binds[j].what +
                       " both bind " + std::to_string(binds[i].port);

    for (const auto& p : peers) {
        if (p.id == nodeId || p.host != me.host) continue;       // only co-located peers can collide
        std::vector<Bind> theirs{{p.consensusPort, "consensusPort"}};
        if (p.clientPort != 0)      theirs.push_back({p.clientPort, "clientPort"});
        if (p.replicationPort != 0) theirs.push_back({p.replicationPort, "replicationPort"});
        for (int32_t w = 0; w < W; ++w) {
            theirs.push_back({p.ingressBasePort + w, "workerIngress[w" + std::to_string(w) + "]"});
            theirs.push_back({p.replBasePort + w, "workerRepl[w" + std::to_string(w) + "]"});
        }
        for (const auto& a : binds)
            for (const auto& b : theirs)
                if (a.port == b.port)
                    return "port collision with co-located peer " + std::to_string(p.id) + " (" +
                           p.host + "): our " + a.what + " and its " + b.what + " both use " +
                           std::to_string(a.port);
    }
    return validateStreamRanges();
}

// ---------------------------------------------------------------------------
// parseNodeConfig
// ---------------------------------------------------------------------------

std::optional<std::vector<int32_t>> parseCpuList(std::string_view s) {
    std::vector<int32_t> out;
    if (s.empty()) return std::nullopt;
    std::size_t pos = 0;
    auto num = [](std::string_view t, int32_t& v) {
        if (t.empty() || t.size() > 4) return false;
        v = 0;
        for (char c : t) { if (c < '0' || c > '9') return false; v = v * 10 + (c - '0'); }
        return v < 1024;
    };
    while (pos <= s.size()) {
        const std::size_t comma = std::min(s.find(',', pos), s.size());
        const std::string_view tok = s.substr(pos, comma - pos);
        const std::size_t dash = tok.find('-');
        int32_t lo = 0, hi = 0;
        if (dash == std::string_view::npos) {
            if (!num(tok, lo)) return std::nullopt;
            hi = lo;
        } else if (!num(tok.substr(0, dash), lo) || !num(tok.substr(dash + 1), hi) || hi < lo) {
            return std::nullopt;
        }
        for (int32_t c = lo; c <= hi; ++c) out.push_back(c);
        if (comma == s.size()) break;
        pos = comma + 1;
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

ConfigResult parseNodeConfig(
    const std::function<const char*(const char*)>& getenv_fn)
{
    std::vector<std::string> warnings;   // non-fatal (deprecated spellings); returned on success
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

    // --- RUNE_REPL_BASE_STREAM_ID (optional, default 2000) — Task 1 ---
    // Base Aeron stream id for per-worker replication streams.
    // Worker W uses stream replBaseStreamId + W.
    // Default 2000 is disjoint from all existing default stream ids/ranges.
    int32_t replBaseStreamId = 2000;
    const char* rawReplBase = getenv_fn("RUNE_REPL_BASE_STREAM_ID");
    if (rawReplBase && !std::string_view(rawReplBase).empty()) {
        if (!parseInt32(rawReplBase, replBaseStreamId))
            return {{}, std::string("RUNE_REPL_BASE_STREAM_ID is not a valid integer: ") + rawReplBase};
    }

    // --- Election timing (optional, positive ms, env-overridable) ---
    // Defaults set in NodeConfig field initializers; only override if env var present.
    NodeConfig cfg;
    cfg.nodeId              = nodeId;
    cfg.peers               = std::move(peers);
    cfg.dataDir             = std::string(rawDataDir);
    cfg.consensusStreamId   = consensusStreamId;
    cfg.replicationStreamId = replicationStreamId;

    // host:port (canonical) or an already-formed Aeron channel (docker/README.md
    // form) — never wrapped twice (regression: shard ingress "unknown host").
    cfg.clientEndpoint.channel  = std::string_view(rawClient).starts_with("aeron:")
                                      ? std::string(rawClient)
                                      : std::string("aeron:udp?endpoint=") + rawClient;
    cfg.clientEndpoint.streamId = clientStreamId;
    cfg.replBaseStreamId        = replBaseStreamId;
    // RUNE_AERON_DIR (optional): Aeron dir override (empty → dataDir/aeron).
    if (const char* ad = getenv_fn("RUNE_AERON_DIR")) cfg.aeronDir = ad;
    if (auto err = cfg.validateAeronDir()) return {{}, *err};

    // E5.1: resolve workerIngressBaseStreamId — default to clientStreamId+1 when unset.
    cfg.workerIngressBaseStreamId = (workerIngressBaseStreamId == 0)
                                   ? (clientStreamId + 1)
                                   : workerIngressBaseStreamId;

    // read-rework T3/T4: per-worker dynamic-MDC egress config (optional overrides;
    // buildWorkerPool resolves workerEgressBaseStreamId=0 → ingressBase+workerCount).
    // DR-15 fix round 1 (MEDIUM-1): peer-facing bind host (default: derived — see peerBindHost()).
    if (const char* bh = getenv_fn("RUNE_PEER_BIND_HOST"); bh && *bh) cfg.peerBindHostOverride = bh;
    {
        const char* rawEgHost = getenv_fn("RUNE_WORKER_EGRESS_CONTROL_HOST");
        if (rawEgHost && !std::string_view(rawEgHost).empty())
            cfg.workerEgressControlHost = rawEgHost;
        const char* rawEgPort = getenv_fn("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT");
        if (rawEgPort && !std::string_view(rawEgPort).empty()) {
            int32_t port{};
            if (!parseInt32(rawEgPort, port))
                return {{}, std::string("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT is not a valid integer: ") + rawEgPort};
            cfg.workerEgressControlBasePort = port;
        }
        const char* rawEgStream = getenv_fn("RUNE_WORKER_EGRESS_BASE_STREAM");
        if (rawEgStream && !std::string_view(rawEgStream).empty()) {
            if (!parseInt32(rawEgStream, cfg.workerEgressBaseStreamId))
                return {{}, std::string("RUNE_WORKER_EGRESS_BASE_STREAM is not a valid integer: ") + rawEgStream};
        }
    }

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

        // Controlled shutdown budget (0 = no hand-off).
        if (const char* raw = getenv_fn("RUNE_CONTROLLED_SHUTDOWN_MS"); raw && !std::string_view(raw).empty()) {
            int64_t v = 0;
            if (std::string_view(raw) == "0") v = 0;
            else if (!parseInt64Positive(raw, v))
                return {{}, std::string("RUNE_CONTROLLED_SHUTDOWN_MS must be a non-negative integer (ms), got: ") + raw};
            cfg.controlledShutdownMs = v;
        }
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

    // --- RUNE_SURVIVOR_LEADER_ELECTION (optional, default OFF) — review
    // 2026-10-04 MEDIUM-3: opt in to the position-proven survivor election (a
    // partition whose committed ISR has no live member elects its most complete
    // reporting live replica).  Kafka keeps such a partition offline unless
    // unclean.leader.election.enable — so does Rune by default.
    {
        const char* raw = getenv_fn("RUNE_SURVIVOR_LEADER_ELECTION");
        if (raw) {
            const std::string_view sv(raw);
            cfg.survivorLeaderElection = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }
    // Re-review 2026-10-04 MEDIUM-B: unclean consensus election (default OFF) —
    // see NodeConfig::uncleanConsensusElection.
    {
        const char* raw = getenv_fn("RUNE_UNCLEAN_CONSENSUS_ELECTION");
        if (raw) {
            const std::string_view sv(raw);
            cfg.uncleanConsensusElection = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }

    // Task 9 (RUNE_TLS toggle, spec §11): cert/key file paths + require_tls.
    // Only consulted in a RUNE_TLS build (RUNE_TLS_ENABLED) — a RUNE_TLS=OFF
    // build never reads these env vars, so it stays byte-identical plaintext
    // no matter what an operator sets (the CMake gate, not just a runtime
    // default, is the safety net here).
#ifdef RUNE_TLS_ENABLED
    if (const char* cf = getenv_fn("RUNE_TLS_CERT_FILE"); cf && *cf) cfg.tlsCertFile = cf;
    if (const char* kf = getenv_fn("RUNE_TLS_KEY_FILE");  kf && *kf) cfg.tlsKeyFile  = kf;
    {
        const char* raw = getenv_fn("RUNE_REQUIRE_TLS");
        if (raw) {
            const std::string_view sv(raw);
            cfg.requireTls = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }
#endif

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
                //       persistence=disk|transient (memory = deprecated alias), durability=majority|all-in-sync.
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
                    return {{}, std::string("RUNE_TOPICS entry missing ':' in: ") + escapeNameForLog(entry)};

                std::string_view namePart  = fields[0];
                std::string_view countPart = fields[1];

                if (namePart.empty())
                    return {{}, std::string("RUNE_TOPICS entry has empty name in: ") + escapeNameForLog(entry)};

                std::string name(namePart);
                // DDL conformance B7 / D2d: a static name obeys the DDL name rule — it is
                // the directory dataDir/topics/<name>/ too.
                if (const TopicNameError ne = checkTopicName(name); ne != TopicNameError::None)
                    return {{}, "RUNE_TOPICS topic name '" + escapeNameForLog(name) + "' is invalid (" +
                                std::string(topicNameErrorText(ne)) + ") in entry: " + escapeNameForLog(entry)};
                if (!seenNames.insert(name).second)
                    return {{}, "RUNE_TOPICS has duplicate topic name: " + escapeNameForLog(name)};
                for (const std::string& seen : seenNames) {
                    if (seen != name && topicNamesFoldEqual(seen, name))
                        return {{}, "RUNE_TOPICS topic names '" + escapeNameForLog(seen) + "' and '" + escapeNameForLog(name) +
                                    "' differ only by letter case (they would share one data "
                                    "directory) in entry: " + escapeNameForLog(entry)};
                }

                int32_t partCount{};
                if (!parseInt32(countPart, partCount) || partCount < 1)
                    return {{}, std::string("RUNE_TOPICS partition count must be >= 1, got: ") + escapeNameForLog(countPart) + " in entry: " + escapeNameForLog(entry)};

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
                        ts.coPartitionGroup = escapeNameForLog(kval);
                    } else if (klower == "key") {
                        ts.partitionKey = escapeNameForLog(kval);
                    } else if (klower == "rf") {
                        const std::string kv(kval);
                        char* endp = nullptr;
                        const long v = std::strtol(kv.c_str(), &endp, 10);
                        if (endp == kv.c_str() || *endp != '\0' || v < 1 || v > 255)
                            return {{}, std::string("RUNE_TOPICS rf must be an integer 1..255, got: ") + escapeNameForLog(kval) + " in entry: " + escapeNameForLog(entry)};
                        // E2 residual round 6 (F2): fail fast, naming the constraint —
                        // never silently clamp (Kafka INVALID_REPLICATION_FACTOR) or
                        // accept a factor no acks=All produce / offset commit can meet.
                        const long nodes = static_cast<long>(std::max<std::size_t>(1, cfg.peers.size()));
                        if (v > nodes)
                            return {{}, "RUNE_TOPICS rf (" + std::to_string(v) + ") must be <= the node count (" +
                                            std::to_string(nodes) + ") in entry: " + escapeNameForLog(entry)};
                        if (v < cfg.gatewayMinIsr)
                            return {{}, "RUNE_TOPICS rf (" + std::to_string(v) + ") must be >= RUNE_MIN_ISR (" +
                                            std::to_string(cfg.gatewayMinIsr) + ") in entry: " +
                                            escapeNameForLog(entry) + " — a smaller factor can never satisfy acks=all"};
                        ts.replicationFactor = static_cast<int16_t>(v);
                    } else if (klower == "persistence") {
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "transient") {
                            ts.persistence = PersistenceMode::Transient;
                        } else if (vlow == "memory") {
                            // Phase 2b E: memory topics were renamed transient topics.
                            // Existing compose files keep working; one line per entry.
                            ts.persistence = PersistenceMode::Transient;
                            warnings.push_back("RUNE_TOPICS persistence=memory is deprecated — use "
                                               "persistence=transient (topic '" + ts.name + "')");
                        } else if (vlow == "disk") {
                            ts.persistence = PersistenceMode::Disk;
                        } else {
                            return {{}, std::string("RUNE_TOPICS persistence must be 'disk' or 'transient', got: ") + escapeNameForLog(kval) + " in entry: " + escapeNameForLog(entry)};
                        }
                    } else if (klower == "durability") {
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "majority") {
                            ts.durability = 0;
                        } else if (vlow == "all-in-sync") {
                            ts.durability = 1;
                        } else {
                            return {{}, std::string("RUNE_TOPICS durability must be 'majority' or 'all-in-sync', got: ") + escapeNameForLog(kval) + " in entry: " + escapeNameForLog(entry)};
                        }
                    } else if (klower == "tier") {
                        // Async-durability tier (see NodeConfig.h): controls WHEN the
                        // produce ack fires relative to local fsync.
                        //   leader=0, majority=1 (default), strict=2 (ack after fdatasync).
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "leader") {
                            ts.durabilityTier = 0;
                        } else if (vlow == "majority") {
                            ts.durabilityTier = 1;
                        } else if (vlow == "strict") {
                            ts.durabilityTier = 2;
                        } else {
                            return {{}, std::string("RUNE_TOPICS tier must be 'leader', 'majority' or 'strict', got: ") + escapeNameForLog(kval) + " in entry: " + escapeNameForLog(entry)};
                        }
                    } else {
                        return {{}, std::string("RUNE_TOPICS unknown option '") + escapeNameForLog(kname) + "' in entry: " + escapeNameForLog(entry)};
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
                        return {{}, std::string("RUNE_TOPICS policy must be 'delete', 'compact', or 'compact_delete', got: ") + escapeNameForLog(pol) + " in entry: " + escapeNameForLog(entry)};
                    }
                }

                // Helper: parse an optional positional int64 field (positive or -1).
                auto parseOptField = [&](std::size_t idx, const char* fieldName, int64_t& out, bool allowNeg1) -> std::string {
                    const auto sv = posField(idx);
                    if (sv.empty()) return {};
                    int64_t v{};
                    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), v);
                    if (ec != std::errc{} || ptr != sv.data() + sv.size())
                        return std::string("RUNE_TOPICS ") + fieldName + " must be an integer in: " + escapeNameForLog(entry);
                    if (!allowNeg1 && v < 1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be a positive integer in: " + escapeNameForLog(entry);
                    if (allowNeg1 && v < -1)
                        return std::string("RUNE_TOPICS ") + fieldName + " must be >= -1 in: " + escapeNameForLog(entry);
                    out = v;
                    return {};
                };

                // Positional[1] (optional): segmentRollBytes (positive int64)
                {
                    std::string err = parseOptField(1, "segmentRollBytes", ts.segmentRollBytes, false);
                    if (!err.empty()) return {{}, err};
                    // D19 L1: segment positions are 32-bit (Log::kMaxRollBytes).
                    if (ts.segmentRollBytes > rune::storage::Log::kMaxRollBytes)
                        return {{}, std::string("RUNE_TOPICS segmentRollBytes must be <= ") +
                                        std::to_string(rune::storage::Log::kMaxRollBytes) + " in: " + escapeNameForLog(entry)};
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
                        return {{}, std::string("RUNE_TOPICS minCleanableDirtyRatio must be a number in: ") + escapeNameForLog(entry)};
                    if (v < 0.0 || v > 1.0)
                        return {{}, std::string("RUNE_TOPICS minCleanableDirtyRatio must be in [0.0, 1.0] in: ") + escapeNameForLog(entry)};
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

    // --- RUNE_PRODUCER_ID_EXPIRATION_MS (optional, >= 0) — PSM fix round 1 (I1). ---
    if (const char* raw = getenv_fn("RUNE_PRODUCER_ID_EXPIRATION_MS"); raw && *raw) {
        char* end = nullptr;
        const long long v = std::strtoll(raw, &end, 10);
        if (end != raw && v >= 0) cfg.producerIdExpirationMs = static_cast<int64_t>(v);
    }

    // --- hotpath-A F2: RUNE_SEGMENT_PROVISION_DEPTH / RUNE_SEGMENT_PROVISIONER_MAX_PARK_US ---
    if (const char* raw = getenv_fn("RUNE_SEGMENT_PROVISION_DEPTH"); raw && *raw) {
        char* end = nullptr;
        const long v = std::strtol(raw, &end, 10);
        if (end != raw) cfg.segmentProvisionDepth = static_cast<int32_t>(std::clamp<long>(v, 1, 8));
    }
    if (const char* raw = getenv_fn("RUNE_SEGMENT_PROVISION_THRESHOLD_PCT"); raw && *raw) {
        char* end = nullptr;
        const long v = std::strtol(raw, &end, 10);
        if (end != raw) cfg.segmentProvisionThresholdPct = static_cast<int32_t>(std::clamp<long>(v, 1, 100));
    }
    if (const char* raw = getenv_fn("RUNE_SEGMENT_PROVISION_CAP_BYTES"); raw && *raw) {
        char* end = nullptr;
        const long long v = std::strtoll(raw, &end, 10);
        if (end != raw) cfg.segmentProvisionCapBytes = static_cast<int64_t>(v < 0 ? -1 : v);
    }
    if (const char* raw = getenv_fn("RUNE_SEGMENT_PROVISION_CAP_FREE_PCT"); raw && *raw) {
        char* end = nullptr;
        const long v = std::strtol(raw, &end, 10);
        if (end != raw) cfg.segmentProvisionCapFreePct = static_cast<int32_t>(std::clamp<long>(v, 1, 100));
    }
    if (const char* raw = getenv_fn("RUNE_SEGMENT_PROVISIONER_MAX_PARK_US"); raw && *raw) {
        char* end = nullptr;
        const long long v = std::strtoll(raw, &end, 10);
        if (end != raw && v >= 1) cfg.segmentProvisionerMaxParkUs = static_cast<int64_t>(v);
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
    {
        // Phase 2b E: RUNE_TRANSIENT_LOG_RING_BYTES is canonical; RUNE_MEMORY_LOG_RING_BYTES
        // is a deprecated alias (one warning whenever it is set; canonical wins if both).
        const char* canon = getenv_fn("RUNE_TRANSIENT_LOG_RING_BYTES");
        const char* alias = getenv_fn("RUNE_MEMORY_LOG_RING_BYTES");
        if (alias)
            warnings.push_back("RUNE_MEMORY_LOG_RING_BYTES is deprecated; use RUNE_TRANSIENT_LOG_RING_BYTES");
        parsePow2(canon ? canon : alias, cfg.memoryLogRingBytes);
    }
    parsePow2(getenv_fn("RUNE_AERON_TERM_BUFFER_BYTES"),  cfg.aeronTermBufferBytes);
    if (const char* raw = getenv_fn("RUNE_IOWQ_CORES"); raw && *raw) {
        auto cpus = parseCpuList(raw);
        if (!cpus) return {{}, std::string("RUNE_IOWQ_CORES must be a cpu list like 0,4-5, got '") + raw + "'"};
        cfg.ioWorkerCpus = std::move(*cpus);
    }
    if (const char* raw = getenv_fn("RUNE_NONWORKER_CORES"); raw && *raw) {
        auto cpus = parseCpuList(raw);
        if (!cpus) return {{}, std::string("RUNE_NONWORKER_CORES must be a cpu list like 0,4-5, got '") + raw + "'"};
        cfg.nonWorkerCpus = std::move(*cpus);
    }
    for (auto [name, field] : {std::pair{"RUNE_WORKER_SYNC_INTERVAL_MS", &cfg.workerSyncIntervalMs},
                               std::pair{"RUNE_FOLLOWER_FLUSH_INTERVAL_MS", &cfg.followerFlushIntervalMs}}) {
        if (const char* raw = getenv_fn(name); raw && *raw) {
            char* end = nullptr;
            const long long v = std::strtoll(raw, &end, 10);
            constexpr long long kMaxMs = 30LL * 24 * 3600 * 1000;   // 30 days: ms->ns never overflows
            if (end == raw || *end != '\0' || v < 0 || v > kMaxMs)
                return {{}, std::string(name) + " must be an integer in [0, 2592000000] ms (0 = never), got '" +
                                raw + "'"};
            *field = v;
        }
    }
    if (const char* raw = getenv_fn("RUNE_AERON_CONTROL_TERM_BYTES"); raw && *raw) {
        int32_t ct = 0;
        parsePow2(raw, ct);
        if (ct < 64 * 1024)   // Aeron's minimum term; not a power of two -> ct stays 0
            return {{}, std::string("RUNE_AERON_CONTROL_TERM_BYTES must be a power of two >= 65536, got '") + raw + "'"};
        cfg.aeronControlTermBufferBytes = ct;
    }
    // RUNE_AERON_MTU: MTU is not required to be a power of two — use parseInt32.
    // Unset → derived (HostDefaults).  An explicit value must be a valid Aeron
    // MTU (> 32-byte data header, <= 65504); 32-byte alignment / socket-buffer fit
    // are enforced (clamped + WARN) at start.  Garbage is refused, never a 64 B MTU.
    if (const char* raw = getenv_fn("RUNE_AERON_MTU"); raw && *raw) {
        int32_t v{};
        if (!parseInt32(raw, v) || v <= 32 || v > 65504)
            return {{}, std::string("RUNE_AERON_MTU must be an integer in [33, 65504], got: ") + raw};
        cfg.aeronMtuBytes = v;
    }
    parsePow2(getenv_fn("RUNE_AERON_RCV_WINDOW"), cfg.aeronRcvWindowBytes);
    for (auto [name, field] : {std::pair{"RUNE_AERON_MAX_MSGS_PER_SEND", &cfg.aeronMaxMessagesPerSend},
                               std::pair{"RUNE_AERON_SENDER_IOV", &cfg.aeronSenderIoVectorCapacity},
                               std::pair{"RUNE_AERON_RECEIVER_IOV", &cfg.aeronReceiverIoVectorCapacity}}) {
        if (const char* raw = getenv_fn(name); raw && *raw) {
            int32_t v{};
            if (!parseInt32(raw, v) || v < 1 || v > 16)
                return {{}, std::string(name) + " must be an integer in [1, 16], got '" + raw + "'"};
            *field = v;
        }
    }
    if (const char* raw = getenv_fn("RUNE_AERON_UDP_GSO"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "1" || v == "true") cfg.aeronUdpGso = true;
        else if (v == "0" || v == "false") cfg.aeronUdpGso = false;
        else return {{}, std::string("RUNE_AERON_UDP_GSO must be 0/1/false/true, got '") + raw + "'"};
    }
    if (const char* raw = getenv_fn("RUNE_AERON_EPOLL_POLL"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "1" || v == "true") cfg.aeronEpollPoll = true;
        else if (v == "0" || v == "false") cfg.aeronEpollPoll = false;
        else return {{}, std::string("RUNE_AERON_EPOLL_POLL must be 0/1/false/true, got '") + raw + "'"};
    }
    if (const char* raw = getenv_fn("RUNE_AERON_TERM_SPARSE"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "false" || v == "0") cfg.aeronTermBufferSparse = false;   // explicit (else derived)
        else if (v == "true" || v == "1") cfg.aeronTermBufferSparse = true;
        else return {{}, std::string("RUNE_AERON_TERM_SPARSE must be true|false, got '") + raw + "'"};
    }
    // RUNE_AERON_SOCKET_BUFFER_BYTES: OS UDP SO_SNDBUF/SO_RCVBUF (Task 6). Not
    // required to be a power of two.  0 leaves Aeron's default.
    if (const char* raw = getenv_fn("RUNE_AERON_SOCKET_BUFFER_BYTES")) {
        if (raw && !std::string_view(raw).empty()) {
            int32_t v = 0;
            if (parseInt32(raw, v) && v >= 0) cfg.aeronSocketBufferBytes = v;
        }
    }
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

    // Per-tick drain/poll batch knobs (0 or unparseable → keep default).
    auto parseLimit = [&](const char* name, int32_t& field) {
        if (const char* raw = getenv_fn(name)) {
            if (raw && !std::string_view(raw).empty()) {
                int32_t v{};
                if (parseInt32(raw, v) && v > 0) field = v;
            }
        }
    };
    parseLimit("RUNE_WORKER_INGRESS_DRAIN_LIMIT", cfg.workerIngressDrainLimit);
    parseLimit("RUNE_WORKER_RETURN_DRAIN_LIMIT",  cfg.workerReturnDrainLimit);
    parseLimit("RUNE_REPL_FRAGMENT_LIMIT",        cfg.replFragmentLimit);
    parseLimit("RUNE_CONSENSUS_FRAGMENT_LIMIT",   cfg.consensusFragmentLimit);
    // F15 S3: owed-return cap.  Strict: any integer is kept as given and the
    // [256, 8192] range is enforced at start() (Worker::validateRingConfig), so
    // 0 / negative refuse loudly; a non-integer is a configuration error here.
    if (const char* raw = getenv_fn("RUNE_WORKER_MAX_OWED_RETURNS"); raw && *raw) {
        int32_t v{};
        if (!parseInt32(raw, v))
            return {{}, std::string("RUNE_WORKER_MAX_OWED_RETURNS is not a valid integer: ") + raw};
        cfg.workerMaxOwedReturns = v;
    }

    // RUNE_WORKER_ACK_SLOT_BYTES: return-ring reservation slot size for ProduceResult.
    if (const char* raw = getenv_fn("RUNE_WORKER_ACK_SLOT_BYTES")) {
        if (raw && !std::string_view(raw).empty()) {
            int32_t v{};
            if (parseInt32(raw, v) && v >= 64) {
                cfg.workerAckSlotBytes = v;
            }
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

    // --- RUNE_RUNTIME_IDLE (optional) — idle strategy for the runtime/gateway
    //     thread.  Same values as RUNE_WORKER_IDLE; "busyspin" gives sub-100µs
    //     produce/ack round-trips (gateway thread never parks). ---
    if (const char* ri = getenv_fn("RUNE_RUNTIME_IDLE")) {
        std::string s(ri);
        for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (s == "busyspin" || s == "busy_spin" || s == "busy-spin") {
            cfg.runtimeIdleStrategy = NodeConfig::WorkerIdleStrategy::BusySpin;
        } else if (s == "yielding") {
            cfg.runtimeIdleStrategy = NodeConfig::WorkerIdleStrategy::Yielding;
        } else if (s == "sleeping") {
            cfg.runtimeIdleStrategy = NodeConfig::WorkerIdleStrategy::Sleeping;
        } else {
            cfg.runtimeIdleStrategy = NodeConfig::WorkerIdleStrategy::Backoff;
        }
    }

    // --- RUNE_WORKER_CORE_BASE (optional) — E4: CPU affinity base core. ---
    // -1 = disabled (no pinning, default).  >= 0 → worker i pins to core base+i.
    // Values < -1 are clamped to -1 (disabled).
    if (const char* wcb = getenv_fn("RUNE_WORKER_CORE_BASE")) {
        if (wcb && !std::string_view(wcb).empty()) {
            int32_t v{};
            // Unparseable → -1 (DISABLED, never the auto layout: garbage must not
            // turn a busy-spin on).  Values < -1 are clamped to -1.
            cfg.workerCoreBase = parseInt32(wcb, v) ? ((v < -1) ? -1 : v) : -1;
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

    // --- RUNE_AERON_DRIVER_IDLE (optional) — E5.2 ---
    // Passed to the Aeron idle-strategy setter(s) for the active threading mode.
    // Unset = derived (D2): backoff in shard mode, sleep-ns in legacy —
    // NodeConfig::effectiveDriverIdleStrategy().
    if (const char* adi = getenv_fn("RUNE_AERON_DRIVER_IDLE")) {
        if (adi && !std::string_view(adi).empty()) {
            cfg.driverIdleStrategy = std::string(adi);
        }
    }
    // Absent → empty (derived).
    // D2 round 2: the node driver's idle init args (Aeron syntax; refused at driver start if malformed).
    if (const char* ada = getenv_fn("RUNE_AERON_DRIVER_IDLE_ARGS"); ada && *ada) cfg.driverIdleArgs = ada;
    // D2 round 2: the shard-mode control-plane park cap (node driver, runtime thread, admin thread).
    if (const char* raw = getenv_fn("RUNE_CONTROL_IDLE_MAX_PARK_US"); raw && *raw) {
        char* end = nullptr;
        const long long v = std::strtoll(raw, &end, 10);
        if (end == raw || *end != '\0' || v < 1 || v > 1'000'000)
            return {{}, std::string("RUNE_CONTROL_IDLE_MAX_PARK_US must be an integer in [1, 1000000] us, got '") +
                            raw + "'"};
        cfg.controlIdleMaxParkUs = v;
    }
    {   // D2 round-2 review R2-Md-1: the park cap must stay well inside the election cadence, or an idle leader's
        // heartbeats slip into endless leadership churn.  Limit = 1/4 of the election status interval: an explicit
        // larger cap is refused; the default is clamped (so lowering only the interval never refuses a default).
        const int64_t limitUs = std::max<int64_t>(1, cfg.electionStatusIntervalMs * 1000 / 4);
        if (cfg.controlIdleMaxParkUs > limitUs) {
            const char* raw = getenv_fn("RUNE_CONTROL_IDLE_MAX_PARK_US");
            if (raw && *raw)
                return {{}, "RUNE_CONTROL_IDLE_MAX_PARK_US=" + std::string(raw) + " us exceeds 1/4 of "
                            "RUNE_ELECTION_STATUS_INTERVAL_MS (" + std::to_string(cfg.electionStatusIntervalMs) +
                            " ms -> max " + std::to_string(limitUs) + " us): heartbeats would miss the election cadence"};
            cfg.controlIdleMaxParkUs = limitUs;
        }
    }

    // --- RUNE_MAX_HELD_IDLE_MS (optional, D8b) --- held-session no-progress reap bound, 1 ms .. 24 h
    // (bounded: BrokerNode converts it to ns — no overflow; re-review LOW-B).
    if (const char* raw = getenv_fn("RUNE_MAX_HELD_IDLE_MS"); raw && *raw) {
        constexpr long long kMaxHeldIdleMsCap = 24LL * 3600 * 1000;
        char* end = nullptr;
        errno = 0;
        const long long v = std::strtoll(raw, &end, 10);
        if (end == raw || *end != '\0' || errno == ERANGE || v < 1 || v > kMaxHeldIdleMsCap)
            return {{}, std::string("RUNE_MAX_HELD_IDLE_MS must be an integer in [1, 86400000] (ms), got '") + raw + "'"};
        cfg.maxHeldIdleMs = v;
    }

    // --- RUNE_GATEWAY_PRODUCE_CREDITS (optional, D8c) --- per-session gateway-routed produce credit budget
    if (const char* raw = getenv_fn("RUNE_GATEWAY_PRODUCE_CREDITS"); raw && *raw) {
        char* end = nullptr;
        errno = 0;
        const long long v = std::strtoll(raw, &end, 10);
        if (end == raw || *end != '\0' || errno == ERANGE || v < 1 ||
            v > static_cast<long long>(NodeConfig::kMaxGatewayProduceCredits))
            return {{}, std::string("RUNE_GATEWAY_PRODUCE_CREDITS must be an integer in [1, ") +
                            std::to_string(NodeConfig::kMaxGatewayProduceCredits) + "], got '" + raw + "'"};
        cfg.gatewayProduceCredits = static_cast<uint32_t>(v);
    }

    // --- RUNE_FETCH_INTERVAL_US (optional, default 5000µs) ---
    // Follower periodic-fetch backstop cadence.  Lower = faster acks=all
    // round-trip when GR-10 push under-delivers; measurement/tuning knob.
    if (const char* fiv = getenv_fn("RUNE_FETCH_INTERVAL_US")) {
        if (fiv && !std::string_view(fiv).empty()) {
            char* end = nullptr;
            long long us = std::strtoll(fiv, &end, 10);
            if (end != fiv && us > 0) cfg.fetchIntervalNs = us * 1000LL;
        }
    }

    // --- RUNE_IDLE_MAX_PARK_US (optional, default 200µs) ---
    // Backoff idle max park for runtime + worker AgentRunners; caps per-hop wake
    // latency on the replication round-trip.  Lower = lower latency, more idle CPU.
    // A7 review L7: it also bounds every fd-less wake of a parked shard worker (ring hops, Aeron timers) — an
    // integer in [1, 1000000] us, anything else refuses startup (garbage was silently ignored, unbounded).
    if (const char* imp = getenv_fn("RUNE_IDLE_MAX_PARK_US"); imp && *imp) {
        char* end = nullptr;
        errno = 0;
        const long long us = std::strtoll(imp, &end, 10);
        if (end == imp || *end != '\0' || errno == ERANGE || us < 1 || us > 1'000'000)
            return {{}, std::string("RUNE_IDLE_MAX_PARK_US must be an integer in [1, 1000000] us, got '") + imp + "'"};
        cfg.idleMaxParkUs = us;
        cfg.idleMaxParkUsExplicit = true;   // above 10 ms: a startup warning (HostDefaults)
    }

    // --- A7: RUNE_IDLE_WAKE (default on) / RUNE_IDLE_SPINS / RUNE_IDLE_YIELDS (default auto) ---
    // Shard-worker park-until-work (NodeConfig::idleWake): 0/1/false/true like RUNE_AERON_UDP_GSO; spins /
    // yields: a non-negative decimal integer (auto = the Aeron 10 / 20, NodeConfig::shardIdleShape).  Anything
    // else refuses startup (review D3: a typo must never silently measure the other arm).
    if (const char* raw = getenv_fn("RUNE_IDLE_WAKE"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "1" || v == "true") cfg.idleWake = true;
        else if (v == "0" || v == "false") cfg.idleWake = false;
        else return {{}, std::string("RUNE_IDLE_WAKE must be 0/1/false/true, got '") + raw + "'"};
    }
    for (auto [name, field] : {std::pair{"RUNE_IDLE_SPINS", &cfg.idleSpins},
                               std::pair{"RUNE_IDLE_YIELDS", &cfg.idleYields}}) {
        if (const char* raw = getenv_fn(name); raw && *raw) {
            int32_t n{};
            if (!parseInt32(raw, n) || n < 0)
                return {{}, std::string(name) + " must be a non-negative integer, got '" + raw + "'"};
            *field = n;
        }
    }
    // --- A9: RUNE_IDLE_DOORBELL (default on) / RUNE_IDLE_DOORBELL_MIN_PARK_US / RUNE_IDLE_DOORBELL_PARK_US ---
    // (NodeConfig::idleDoorbell).  Garbage refuses startup, like the A7 knobs.
    if (const char* raw = getenv_fn("RUNE_IDLE_DOORBELL"); raw && *raw) {
        const std::string_view v(raw);
        if (v == "1" || v == "true") cfg.idleDoorbell = true;
        else if (v == "0" || v == "false") cfg.idleDoorbell = false;
        else return {{}, std::string("RUNE_IDLE_DOORBELL must be 0/1/false/true, got '") + raw + "'"};
    }
    for (auto [name, field] : {std::pair{"RUNE_IDLE_DOORBELL_MIN_PARK_US", &cfg.idleDoorbellMinParkUs},
                               std::pair{"RUNE_IDLE_DOORBELL_PARK_US", &cfg.idleDoorbellParkUs}}) {
        if (const char* raw = getenv_fn(name); raw && *raw) {
            int32_t n{};
            if (!parseInt32(raw, n) || n <= 0 || n > 1'000'000)
                return {{}, std::string(name) + " must be an integer in 1..1000000, got '" + raw + "'"};
            *field = n;
        }
    }
    if (cfg.idleDoorbellMinParkUs > cfg.idleDoorbellParkUs)
        return {{}, "RUNE_IDLE_DOORBELL_MIN_PARK_US (" + std::to_string(cfg.idleDoorbellMinParkUs) +
                    ") must not exceed RUNE_IDLE_DOORBELL_PARK_US (" + std::to_string(cfg.idleDoorbellParkUs) + ")"};

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

    // --- RUNE_PER_WORKER_EGRESS (optional, default false) — read-rework T3/T4 ---
    // When "1"/"true", each worker gets its own dynamic-MDC egress publication and
    // the gateway routes Subscribe/Unsubscribe to the owning worker (worker pushes
    // its owned partitions directly).  Default false: gateway push is the live path.
    if (const char* pwe = getenv_fn("RUNE_PER_WORKER_EGRESS")) {
        if (pwe && !std::string_view(pwe).empty()) {
            std::string_view sv(pwe);
            cfg.perWorkerEgressEnabled = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }

    // --- RUNE_PER_WORKER_REPLICATION (optional, default false) — Task 1 ---
    // When "1" or "true", each worker opens its own dedicated replication transport
    // on stream replBaseStreamId + workerId, replacing the shared control-thread
    // replication path.  Default false: shared path remains live.
    if (const char* pwr = getenv_fn("RUNE_PER_WORKER_REPLICATION")) {
        if (pwr && !std::string_view(pwr).empty()) {
            std::string_view sv(pwr);
            cfg.perWorkerReplication = (sv == "1" || sv == "true" || sv == "TRUE");
        }
    }
    // Absent → keep NodeConfig default (false).

    // --- Self-contained worker shards (spec §3.1-§3.3) ---
    auto lower = [](std::string s) {
        for (auto& ch : s) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return s;
    };
    if (const char* raw = getenv_fn("RUNE_WORKER_MODE"); raw && *raw) {
        const std::string s = lower(std::string(trim(raw)));
        if (s == "legacy")     cfg.workerMode = NodeConfig::WorkerMode::Legacy;
        else if (s == "shard") cfg.workerMode = NodeConfig::WorkerMode::Shard;
        else return {{}, std::string("RUNE_WORKER_MODE must be 'legacy' or 'shard', got: ") + raw};
    }
    if (const char* raw = getenv_fn("RUNE_RUNTIME_PLACEMENT"); raw && *raw) {
        const std::string s = lower(std::string(trim(raw)));
        if (s == "thread")     cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
        else if (s == "fused") cfg.runtimePlacement = NodeConfig::RuntimePlacement::Fused;
        else return {{}, std::string("RUNE_RUNTIME_PLACEMENT must be 'thread' or 'fused', got: ") + raw};
    }
    if (const char* raw = getenv_fn("RUNE_WORKER_INGRESS_BASE_PORT"); raw && *raw) {
        int32_t port{};
        if (!parseInt32(raw, port))
            return {{}, std::string("RUNE_WORKER_INGRESS_BASE_PORT is not a valid integer: ") + raw};
        if (port < 1 || port > 65535)   // rejected in every mode (no silent fallback)
            return {{}, std::string("RUNE_WORKER_INGRESS_BASE_PORT must be a port in [1, 65535], got: ") + raw};
        cfg.workerIngressBasePort = port;
    }
    // F3 round 4: EXPLICIT override of the gateway's max accepted produce-batch
    // size.  Left unset (nullopt) by default — BrokerNode::start() then derives
    // the effective ceiling from the transport instead of refusing startup.
    if (const char* raw = getenv_fn("RUNE_MAX_PRODUCE_BATCH_BYTES"); raw && *raw) {
        int64_t bytes{};
        if (!parseInt64Positive(raw, bytes))
            return {{}, std::string("RUNE_MAX_PRODUCE_BATCH_BYTES must be a positive integer, got: ") + raw};
        cfg.maxProduceBatchBytes = bytes;
    }
    if (const char* raw = getenv_fn("RUNE_WORKER_REPL_BASE_PORT"); raw && *raw) {
        int32_t port{};
        if (!parseInt32(raw, port))
            return {{}, std::string("RUNE_WORKER_REPL_BASE_PORT is not a valid integer: ") + raw};
        if (port < 1 || port > 65535)   // rejected in every mode (no silent fallback)
            return {{}, std::string("RUNE_WORKER_REPL_BASE_PORT must be a port in [1, 65535], got: ") + raw};
        cfg.workerReplBasePort = port;
    }
    if (cfg.shardMode()) {
        // A shard IS per-worker ingress + replication + egress on the worker's own driver.
        cfg.perWorkerIngressEnabled = true;
        cfg.perWorkerReplication    = true;
        cfg.perWorkerEgressEnabled  = true;
    }

    // Task 1: populate self peer's workerCount from cfg.workerCount so that
    // membership descriptors advertise the correct per-worker replication capacity.
    // Only set when workerCount > 1 (explicitly configured for multi-worker mode);
    // leave at 0 (unknown) for the default single-worker configuration so that
    // the default-on field is distinguishable from an explicit deployment choice.
    // Shard mode always stamps (even W=1): symmetric-worker-count validation needs it.
    if (cfg.workerCount > 1 || cfg.shardMode()) {
        for (auto& p : cfg.peers) {
            if (p.id == cfg.nodeId) {
                p.workerCount = cfg.workerCount;
                break;
            }
        }
    }

    return {std::move(cfg), {}, std::move(warnings)};
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
