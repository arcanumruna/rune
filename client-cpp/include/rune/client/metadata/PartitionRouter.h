// rune/client/metadata/PartitionRouter.h
//
// MD1 Task 10: Per-partition routing — key → partition → leader node → endpoint.
//
// Usage:
//   1. After a MetadataResponse updates MetadataCache, call
//      setTopicPartitionCount(topicId, count) for each topic.
//   2. Call routeKey(topicId, key) to get the channel string for the leader.
//   3. On NotLeader/Redirect, caller updates MetadataCache (zero-RTT) and
//      calls routeKey() again — no full refresh needed.
//
// Single-writer: all methods are agent-thread-only.

#pragma once

#include <rune/client/Key.h>
#include <rune/client/metadata/MetadataCache.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

namespace rune::client::metadata {

// ---------------------------------------------------------------------------
// RouteResult — resolved routing info for a single produce call
// ---------------------------------------------------------------------------
struct RouteResult {
    int32_t     partition   = 0;
    int32_t     leaderNodeId = MetadataCache::kNoLeader;
    std::string channel;    // ingress channel for the leader node (may be empty)

    [[nodiscard]] bool valid() const noexcept {
        return leaderNodeId != MetadataCache::kNoLeader && !channel.empty();
    }
};

// ---------------------------------------------------------------------------
// PartitionRouter
// ---------------------------------------------------------------------------
class PartitionRouter {
public:
    explicit PartitionRouter(const MetadataCache& cache) : cache_(cache) {}

    ~PartitionRouter() = default;

    // Non-copyable (borrows cache by const-ref).
    PartitionRouter(const PartitionRouter&)            = delete;
    PartitionRouter& operator=(const PartitionRouter&) = delete;
    PartitionRouter(PartitionRouter&&)                 = default;
    PartitionRouter& operator=(PartitionRouter&&)      = delete;  // ref member

    // -------------------------------------------------------------------------
    // Configuration (agent thread only)
    // -------------------------------------------------------------------------

    /// Register or update the partition count for a topic.
    /// Called when a MetadataResponse is processed.
    void setTopicPartitionCount(int32_t topicId, int32_t count) noexcept {
        if (topicId >= 0 && count > 0) partitionCounts_[topicId] = count;
    }

    /// Return the partition count for a topic, or 0 if unknown.
    [[nodiscard]] int32_t partitionCountFor(int32_t topicId) const noexcept {
        const auto it = partitionCounts_.find(topicId);
        if (it == partitionCounts_.end()) return 0;
        return it->second;
    }

    // -------------------------------------------------------------------------
    // Routing (agent thread only)
    // -------------------------------------------------------------------------

    /// Route key → partition → leader node → channel.
    /// Returns an invalid RouteResult if the topic is unknown or the partition
    /// has no known leader.
    [[nodiscard]] RouteResult routeKey(int32_t topicId,
                                       const rune::client::Key& key) const noexcept
    {
        RouteResult r;

        const int32_t count = partitionCountFor(topicId);
        if (count <= 0) return r;  // unknown topic

        r.partition    = key.partition(count);
        r.leaderNodeId = cache_.leaderFor(topicId, r.partition);

        if (r.leaderNodeId == MetadataCache::kNoLeader) return r;

        r.channel = cache_.endpointFor(r.leaderNodeId);
        return r;
    }

    /// Route to an explicit partition (bypasses key hashing).
    [[nodiscard]] RouteResult routePartition(int32_t topicId,
                                              int32_t partition) const noexcept
    {
        RouteResult r;
        r.partition    = partition;
        r.leaderNodeId = cache_.leaderFor(topicId, partition);

        if (r.leaderNodeId == MetadataCache::kNoLeader) return r;

        r.channel = cache_.endpointFor(r.leaderNodeId);
        return r;
    }

private:
    const MetadataCache& cache_;  // NOT owned — must outlive this object

    // topicId → number of partitions
    std::unordered_map<int32_t, int32_t> partitionCounts_;
};

} // namespace rune::client::metadata
