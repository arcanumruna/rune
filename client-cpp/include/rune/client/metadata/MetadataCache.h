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
    int32_t topicId     = 0;
    int32_t partition   = 0;
    int32_t leaderId    = -1;  // NO_LEADER = -1
    int32_t leaderEpoch = -1;
};

// ---------------------------------------------------------------------------
// BrokerEntry — matches the wire struct in MetadataMessages.h
// ---------------------------------------------------------------------------
struct BrokerEntry {
    int32_t     nodeId = -1;
    int32_t     port   = 0;
    std::string host;          // owned copy
};

// ---------------------------------------------------------------------------
// RoutingEntry — one slot in the partition routing table
// ---------------------------------------------------------------------------
struct RoutingEntry {
    int32_t leaderNodeId  = -1;  // -1 = no known leader
    int32_t leaderEpoch   = -1;  // -1 = never seen
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
    void updateFromResponse(std::span<const BrokerEntry>    brokers,
                            std::span<const PartitionEntry> partitions) noexcept
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
            applyPartitionUpdate(p.topicId, p.partition, p.leaderId, p.leaderEpoch);
        }
    }

    /// Zero-RTT reroute: update a SINGLE partition's leader without a full
    /// refresh.  Called when a NotLeader or Redirect response carries
    /// {leaderId, leaderEpoch}.  Staleness rule applies identically.
    void updatePartitionLeader(int32_t topicId,
                               int32_t partition,
                               int32_t newLeaderId,
                               int32_t newLeaderEpoch) noexcept
    {
        applyPartitionUpdate(topicId, partition, newLeaderId, newLeaderEpoch);
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

    /// Number of known partitions in the cache.
    [[nodiscard]] std::size_t partitionCount() const noexcept {
        return table_.size();
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
    // Encode (topicId, partition) → a single int64_t key.
    // topicId occupies the high 32 bits; partition the low 32 bits.
    static int64_t makeKey(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<int64_t>(topicId) << 32) |
               static_cast<int64_t>(static_cast<uint32_t>(partition));
    }

    // Staleness-gated update for a single partition.
    void applyPartitionUpdate(int32_t topicId, int32_t partition,
                              int32_t newLeaderId, int32_t newEpoch) noexcept
    {
        const int64_t key = makeKey(topicId, partition);
        auto [it, inserted] = table_.emplace(key, RoutingEntry{});
        RoutingEntry& entry = it->second;

        // §11 staleness rule: only accept if newEpoch >= lastSeen.
        if (!inserted && newEpoch < entry.leaderEpoch) {
            return;  // stale update — discard
        }

        entry.leaderNodeId = newLeaderId;
        entry.leaderEpoch  = newEpoch;
    }

    // ---- Members -----
    // Flat hash map: (topicId<<32 | partition) → RoutingEntry
    std::unordered_map<int64_t, RoutingEntry> table_;

    // nodeId → "host:port" string
    std::unordered_map<int32_t, std::string> endpoints_;
};

} // namespace rune::client::metadata
