// rune/client/metadata/MetadataCache.h
//
// MD1 Task 10: Client-side per-partition metadata cache.
//
// Stores per-partition routing state ({leaderNodeId, leaderEpoch}) and per-node
// endpoint strings.  Single-writer: all methods MUST be called from the agent
// thread.  No internal locks — the caller is responsible for threading.
//
// Staleness rule (§11):
//   A partition entry is updated only when newEpoch >= lastSeenLeaderEpoch.
//   Lower-epoch updates are silently discarded to avoid regressing to a stale
//   leader after a flapping or out-of-order MetadataResponse.
//
// Redirect ordering rule (Task 16 M5 + re-review 1 NEW-1):
//   A Redirect frame names only an endpoint, so the client GUESSES the leader
//   with a provisional epoch (confirmed + 1) and asks for metadata at once.
//   While that guess stands, an authoritative answer is accepted when
//     (a) its epoch is ABOVE the last confirmed epoch (a real newer view), or
//     (b) its epoch EQUALS the confirmed epoch and it answers a request sent
//         AFTER the redirect (request correlationId >= provisionalSince):
//         the cluster still names the old leader after the redirect, so the
//         redirect was spurious and the old route is restored (M5: never
//         stranded on the redirect target);
//   and discarded otherwise — an equal-epoch answer requested BEFORE the
//   redirect (in flight, or a lagging re-delivery) can never undo it (NEW-1).
//   Request correlationIds come from one monotonic duty-thread counter, so
//   "requested after" is a plain comparison.  An unsolicited response
//   (correlationId 0) counts as requested before.  Residual: an answer
//   requested after the redirect from a node whose view still lags is
//   indistinguishable from a spurious redirect; it is applied, the next
//   produce meets the redirect again, and the client converges as soon as the
//   metadata carries the new epoch (bounded per batch by kMaxRedirects and
//   the delivery timeout — never a duplicate: the same identity is re-sent).
//
// Usage:
//   1. On MetadataResponse, call updateFromResponse().
//   2. On NotLeader/Redirect with embedded leaderId + leaderEpoch, call
//      updatePartitionLeader() for zero-RTT reroute without a full refresh.
//   3. Call leaderFor(topicId, partition) to get the current known leader node.
//   4. Call endpointFor(nodeId) to get the egress channel for that node.

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::client::metadata {

// ---------------------------------------------------------------------------
// PartitionEntry — matches the wire struct in MetadataMessages.h
// ---------------------------------------------------------------------------
struct PartitionEntry {
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int32_t leaderId       = -1;  // NO_LEADER = -1
    int32_t leaderEpoch    = -1;
    int32_t ingressStreamId = 0;  ///< E5.1: per-worker Aeron ingress stream id (0 = legacy/unknown)
    std::string ingressEndpoint;  ///< Task 16: "host:port" of the owning worker's ingress ("" = node)
    uint8_t ingressReceiverId = 0; ///< issue #116: receiver id of the advertised worker route (0 = the gateway)
    int32_t responseControlPort = 0; ///< v16 (NAT-safe egress): the owning worker's response-control port (0 = none)
};

// ---------------------------------------------------------------------------
// BrokerEntry — matches the wire struct in MetadataMessages.h
// ---------------------------------------------------------------------------
struct BrokerEntry {
    int32_t     nodeId = -1;
    int32_t     port   = 0;
    std::string host;          // owned copy
    int32_t     responseControlPort = 0; ///< v16 (NAT-safe egress): this node's gateway response-control port (0 = none)
};

// ---------------------------------------------------------------------------
// RoutingEntry — one slot in the partition routing table
// ---------------------------------------------------------------------------
struct RoutingEntry {
    int32_t leaderNodeId   = -1;  // -1 = no known leader
    int32_t leaderEpoch    = -1;  // -1 = never seen
    int32_t ingressStreamId = 0;  ///< E5.1: per-worker Aeron ingress stream id (0 = legacy/unknown)
    /// Task 16 fix round 1 (M5): the last epoch a MetadataResponse confirmed.  A
    /// Redirect's provisional leaderEpoch (current+1) never raises it, so the
    /// authoritative answer that follows is accepted even when its epoch did not
    /// move (a spurious redirect) — the route is restored instead of stranded.
    int32_t confirmedEpoch = -1;
    /// Re-review 1 (NEW-1): != 0 while a Redirect's provisional leader stands —
    /// the first MetadataRequest correlationId sent after that redirect.  An
    /// equal-to-confirmed-epoch answer to an earlier request is discarded.
    int64_t provisionalSince = 0;
};

// ---------------------------------------------------------------------------
// MetadataCache
// ---------------------------------------------------------------------------
class MetadataCache {
public:
    static constexpr int32_t kNoLeader = -1;

    MetadataCache()  = default;
    ~MetadataCache() = default;

    // Non-copyable, movable (single-writer ownership transfer).
    MetadataCache(const MetadataCache&)            = delete;
    MetadataCache& operator=(const MetadataCache&) = delete;
    MetadataCache(MetadataCache&&)                 = default;
    MetadataCache& operator=(MetadataCache&&)      = default;

    // -------------------------------------------------------------------------
    // Update API (agent thread only)
    // -------------------------------------------------------------------------

    /// Apply a full MetadataResponse: update all broker endpoints and partition
    /// leaders.  The staleness rule is applied per-partition: an update is
    /// accepted only if newEpoch >= lastSeenLeaderEpoch for that partition.
    ///
    /// `requestCorrId`: the correlationId of the MetadataRequest this answers
    /// (0 = unsolicited; the default = "requested now", for callers without
    /// a redirect in play) — the redirect ordering rule above.
    void updateFromResponse(std::span<const BrokerEntry>    brokers,
                            std::span<const PartitionEntry> partitions,
                            int64_t requestCorrId = INT64_MAX) noexcept
    {
        // Update broker → endpoint mapping.
        for (const auto& b : brokers) {
            if (b.nodeId >= 0 && !b.host.empty()) {
                endpoints_[b.nodeId] = b.host + ":" + std::to_string(b.port);
            }
        }

        // Update per-partition routing (staleness-gated).
        for (const auto& p : partitions) {
            if (p.topicId < 0 || p.partition < 0) continue;
            applyPartitionUpdate(p.topicId, p.partition,
                                 p.leaderId, p.leaderEpoch, p.ingressStreamId, /*authoritative=*/true,
                                 requestCorrId);
        }
    }

    /// Zero-RTT reroute: update a SINGLE partition's leader without a full
    /// refresh.  Called when a NotLeader or Redirect response carries
    /// {leaderId, leaderEpoch}.  Staleness rule applies identically.
    /// ingressStreamId is preserved from the existing entry (caller does not
    /// know the per-worker stream on a bare redirect — the stream stays stable).
    void updatePartitionLeader(int32_t topicId,
                               int32_t partition,
                               int32_t newLeaderId,
                               int32_t newLeaderEpoch) noexcept
    {
        applyPartitionUpdate(topicId, partition, newLeaderId, newLeaderEpoch,
                             existingStreamId_(topicId, partition), /*authoritative=*/true, INT64_MAX);
    }

    /// Task 16 fix round 1 (M5): a GUESSED leader (a Redirect frame carries only
    /// an endpoint; the caller invents epoch current+1 so the staleness rule
    /// accepts it).  Unlike updatePartitionLeader it never raises the confirmed
    /// epoch, so the authoritative MetadataResponse that follows is applied even
    /// when its epoch did not move (a spurious redirect) — provided it answers
    /// a request sent after the redirect: `nextRequestCorrId` is the first
    /// correlationId a MetadataRequest sent from now on can carry (NEW-1).
    void updatePartitionLeaderProvisional(int32_t topicId,
                                          int32_t partition,
                                          int32_t newLeaderId,
                                          int32_t newLeaderEpoch,
                                          int64_t nextRequestCorrId) noexcept
    {
        applyPartitionUpdate(topicId, partition, newLeaderId, newLeaderEpoch,
                             existingStreamId_(topicId, partition), /*authoritative=*/false,
                             nextRequestCorrId);
    }

    /// Update (or add) a broker endpoint without touching partition state.
    void updateBrokerEndpoint(int32_t nodeId, std::string endpoint) noexcept {
        if (nodeId >= 0) endpoints_[nodeId] = std::move(endpoint);
    }

    // -------------------------------------------------------------------------
    // Query API (agent thread only)
    // -------------------------------------------------------------------------

    /// Return the current leader node id for a partition, or kNoLeader.
    [[nodiscard]] int32_t leaderFor(int32_t topicId, int32_t partition) const noexcept {
        const auto it = table_.find(makeKey(topicId, partition));
        if (it == table_.end()) return kNoLeader;
        return it->second.leaderNodeId;
    }

    /// Return the last-seen leaderEpoch for a partition, or -1 if unknown.
    [[nodiscard]] int32_t epochFor(int32_t topicId, int32_t partition) const noexcept {
        const auto it = table_.find(makeKey(topicId, partition));
        if (it == table_.end()) return -1;
        return it->second.leaderEpoch;
    }

    /// Return the ingress channel for a node id, or empty string if unknown.
    [[nodiscard]] const std::string& endpointFor(int32_t nodeId) const noexcept {
        static const std::string kEmpty;
        if (nodeId < 0) return kEmpty;
        const auto it = endpoints_.find(nodeId);
        if (it == endpoints_.end()) return kEmpty;
        return it->second;
    }

    /// True if we have at least one partition entry (i.e. populated).
    [[nodiscard]] bool hasPartitionData() const noexcept {
        return !table_.empty();
    }

    /// True if we know the leader for the given partition.
    [[nodiscard]] bool hasLeader(int32_t topicId, int32_t partition) const noexcept {
        return leaderFor(topicId, partition) != kNoLeader;
    }

    /// True if we have an endpoint for the given node.
    [[nodiscard]] bool hasEndpoint(int32_t nodeId) const noexcept {
        if (nodeId < 0) return false;
        return endpoints_.count(nodeId) > 0;
    }

    /// MD1 review IMPORTANT-1: return the nodeId whose "host:port" endpoint
    /// matches the given string, or kNoLeader if no known broker has it.
    /// Used to resolve a REDIRECT endpoint back to an authoritative nodeId so a
    /// zero-RTT cache update can record {partition → leaderNode} in place.
    [[nodiscard]] int32_t nodeIdForEndpoint(const std::string& endpoint) const noexcept {
        for (const auto& [nodeId, ep] : endpoints_) {
            if (ep == endpoint) return nodeId;
        }
        return kNoLeader;
    }

    /// Return the per-worker ingressStreamId for a partition, or 0 if unknown.
    /// 0 means "not advertised by broker / legacy single-stream mode".
    [[nodiscard]] int32_t ingressStreamIdFor(int32_t topicId, int32_t partition) const noexcept {
        const auto it = table_.find(makeKey(topicId, partition));
        if (it == table_.end()) return 0;
        return it->second.ingressStreamId;
    }

    /// Visit every partition with an advertised per-worker ingress stream:
    /// f(topicId, partition, ingressStreamId).  Cold path (producer registration).
    template <class F>
    void forEachIngressStream(F&& f) const {
        for (const auto& [key, e] : table_) {
            if (e.ingressStreamId == 0) continue;
            f(static_cast<int32_t>(key >> 32), static_cast<int32_t>(static_cast<uint32_t>(key)),
              e.ingressStreamId);
        }
    }

    /// Number of known partitions in the cache.
    [[nodiscard]] std::size_t partitionCount() const noexcept {
        return table_.size();
    }

    /// DR client bug 1: every known broker's (nodeId, "host:port"), in hash order,
    /// not node-id order — callers that need a stable order sort.  Cold.
    template <class F>
    void forEachBrokerEndpoint(F&& f) const {
        for (const auto& [nodeId, ep] : endpoints_) f(nodeId, ep);
    }

    /// Number of known brokers in the cache.
    [[nodiscard]] std::size_t brokerCount() const noexcept {
        return endpoints_.size();
    }

    /// Clear all state.
    void clear() noexcept {
        table_.clear();
        endpoints_.clear();
    }

private:
    [[nodiscard]] int32_t existingStreamId_(int32_t topicId, int32_t partition) const noexcept {
        const auto it = table_.find(makeKey(topicId, partition));
        return it != table_.end() ? it->second.ingressStreamId : 0;
    }

    // Encode (topicId, partition) → a single int64_t key.
    // topicId occupies the high 32 bits; partition the low 32 bits.
    static int64_t makeKey(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<int64_t>(topicId) << 32) |
               static_cast<int64_t>(static_cast<uint32_t>(partition));
    }

    // Staleness-gated update for a single partition.
    // `order`: authoritative — the answered request's correlationId; provisional
    // — the first correlationId a request sent after this redirect can carry.
    void applyPartitionUpdate(int32_t topicId, int32_t partition,
                              int32_t newLeaderId, int32_t newEpoch,
                              int32_t newIngressStreamId, bool authoritative,
                              int64_t order) noexcept
    {
        const int64_t key = makeKey(topicId, partition);
        auto [it, inserted] = table_.emplace(key, RoutingEntry{});
        RoutingEntry& entry = it->second;

        // §11 staleness rule: only accept if newEpoch >= lastSeen.  A MetadataResponse
        // is compared with the last CONFIRMED epoch (a provisional redirect epoch
        // must not make the authoritative answer look stale — M5) ...
        const int32_t floor = authoritative ? entry.confirmedEpoch : entry.leaderEpoch;
        if (!inserted && newEpoch < floor) {
            return;  // stale update — discard
        }
        // ... but while a redirect's guess stands, an equal-epoch answer counts
        // only if it was requested after the redirect (NEW-1, rule above).
        if (!inserted && authoritative && entry.provisionalSince != 0 &&
            newEpoch <= entry.confirmedEpoch && order < entry.provisionalSince) {
            return;  // may predate the redirect — discard
        }

        const int32_t prevLeader = entry.leaderNodeId;
        entry.leaderNodeId    = newLeaderId;
        entry.leaderEpoch     = newEpoch;
        entry.ingressStreamId = newIngressStreamId;
        if (authoritative) {
            entry.confirmedEpoch   = newEpoch;
            entry.provisionalSince = 0;
        } else if (entry.provisionalSince == 0 || newLeaderId != prevLeader) {
            // R2-1: keep the FIRST guess's order while a guess to the SAME node
            // stands — a repeat redirect in a burst must not make the answer to
            // the first redirect's request look "earlier" and get discarded.  A
            // redirect to a DIFFERENT node starts a new guess: an answer
            // requested before it could route back to the older target.
            entry.provisionalSince = order > 0 ? order : 1;
        }
    }

public:
    /// R2-1: true while any partition still holds a redirect's unconfirmed guess
    /// (cold path: called once per MetadataResponse).
    [[nodiscard]] bool anyProvisional() const noexcept {
        for (const auto& kv : table_)
            if (kv.second.provisionalSince != 0) return true;
        return false;
    }

private:
    // ---- Members -----
    // Flat hash map: (topicId<<32 | partition) → RoutingEntry
    std::unordered_map<int64_t, RoutingEntry> table_;

    // nodeId → "host:port" string
    std::unordered_map<int32_t, std::string> endpoints_;
};

// ---------------------------------------------------------------------------
// TopicSchemaEntry — per-topic schema metadata populated by openTopic<T>
// ---------------------------------------------------------------------------
struct TopicSchemaEntry {
    uint16_t    schemaVersion = 0;
    bool        validate      = false;
    std::string keyField;
};

// ---------------------------------------------------------------------------
// TopicSchemaCache — maps topicId → TopicSchemaEntry
// Guarded by Client::mtx_; written by openTopic<T> async continuations (app threads).
// ---------------------------------------------------------------------------
class TopicSchemaCache {
public:
    TopicSchemaCache()  = default;
    ~TopicSchemaCache() = default;

    TopicSchemaCache(const TopicSchemaCache&)            = delete;
    TopicSchemaCache& operator=(const TopicSchemaCache&) = delete;
    TopicSchemaCache(TopicSchemaCache&&)                 = default;
    TopicSchemaCache& operator=(TopicSchemaCache&&)      = default;

    void put(int32_t topicId, TopicSchemaEntry entry) noexcept {
        entries_[topicId] = std::move(entry);
    }

    [[nodiscard]] const TopicSchemaEntry* get(int32_t topicId) const noexcept {
        const auto it = entries_.find(topicId);
        if (it == entries_.end()) return nullptr;
        return &it->second;
    }

    void clear() noexcept { entries_.clear(); }

private:
    std::unordered_map<int32_t, TopicSchemaEntry> entries_;
};

} // namespace rune::client::metadata
