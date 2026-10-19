// ShardRouter.h — pure, deterministic shard identity + shard→worker mapping.
//
// Two-level assignment (Milestone E, Core principle 3):
//   MSM assigns shard→NODE (Milestone D; not here).
//   Each node maps shard→WORKER = hash(shardId) % workerCount (here).
//
// Ownership unit = the co-partition shard, NOT the individual partition:
//   shardId = (groupKey, partitionIndex).
//   Grouped topics share a groupKey (caller assigns; see BrokerNode builder).
//   Ungrouped topics fall back to their own topicId as the groupKey (singleton).
//
// Pure: no allocation on the hot path, no virtual calls, deterministic.
#pragma once

#include <rune/metadata/Gpid.h>

#include <cassert>
#include <cstdint>
#include <string_view>
#include <unordered_map>

namespace rune::metadata {

struct ShardId {
    int32_t groupKey;        ///< co-partition group key (or topicId for singletons)
    int32_t partitionIndex;  ///< partition ordinal within the shard

    bool operator==(const ShardId& o) const noexcept {
        return groupKey == o.groupKey && partitionIndex == o.partitionIndex;
    }
};

[[nodiscard]]
inline ShardId shardIdOf(int32_t groupKey, int32_t partition) noexcept {
    return ShardId{groupKey, partition};
}

/// Deterministic shard→worker map. hash(shardId) % workerCount.
/// Uses Fibonacci hashing (Knuth multiplicative) over a 64-bit packed key
/// so the distribution is well-spread and stable across runs.
[[nodiscard]]
inline int32_t workerIdForShard(const ShardId& s, int32_t workerCount) noexcept {
    assert(workerCount >= 1 && "workerCount must be >= 1");
    // Pack (groupKey, partitionIndex) into a 64-bit key, then Fibonacci hash.
    const uint64_t k =
        (static_cast<uint64_t>(static_cast<uint32_t>(s.groupKey)) << 32) |
         static_cast<uint64_t>(static_cast<uint32_t>(s.partitionIndex));
    const uint64_t h = k * UINT64_C(11400714819323198485);
    return static_cast<int32_t>((h >> 32) % static_cast<uint64_t>(workerCount));
}

/// Catalog history Task 17 (C1): a co-partition group's shard key, a PURE function of the group NAME
/// (FNV-1a 32 over its bytes).  Every broker derives the same key for the same group regardless of which topics
/// it booted with or the order groups were created -- so worker w on every broker owns the same shards, which the
/// per-worker replication planes (worker w <-> peer worker w) require.  Range [-2^30, -1]: never a user topicId
/// (>= 0).  Two names that hash alike share workers (placement only, never correctness).
/// Changing this function changes the placement of every grouped topic on every broker; a mixed-build cluster would
/// mis-route -- never change it without a cluster-wide upgrade rule (golden values: ShardRouterTest).
[[nodiscard]] constexpr int32_t coPartitionGroupKey(std::string_view group) noexcept {
    uint32_t h = 2166136261u;
    for (const char ch : group) { h ^= static_cast<uint8_t>(ch); h *= 16777619u; }
    return -1 - static_cast<int32_t>(h & 0x3FFF'FFFFu);
}

class ShardRouter {
public:
    explicit ShardRouter(int32_t workerCount) noexcept
        : workerCount_(workerCount) {
        assert(workerCount_ >= 1 && "workerCount must be >= 1");
    }

    void mapTopicToGroupKey(int32_t topicId, int32_t groupKey) noexcept {
        topicGroup_[topicId] = groupKey;
    }

    /// Catalog history Task 8 (M5): a dropped topic's group entry (its id is
    /// never reused — this only frees the entry).
    void unmapTopic(int32_t topicId) noexcept { topicGroup_.erase(topicId); }

    [[nodiscard]] int32_t groupKeyForTopic(int32_t topicId) const noexcept {
        const auto it = topicGroup_.find(topicId);
        return (it != topicGroup_.end()) ? it->second : topicId;
    }

    [[nodiscard]] ShardId shardForPartition(int32_t topicId,
                                            int32_t partition) const noexcept {
        return shardIdOf(groupKeyForTopic(topicId), partition);
    }

    [[nodiscard]] int32_t workerForPartition(int32_t topicId,
                                             int32_t partition) const noexcept {
        return workerIdForShard(shardForPartition(topicId, partition), workerCount_);
    }

    [[nodiscard]] int32_t workerCount() const noexcept { return workerCount_; }

private:
    int32_t workerCount_;
    // topicId -> groupKey. Not on the append hot path (built at assignment time);
    // lookups here are during ownership derivation, not per-produce.
    std::unordered_map<int32_t, int32_t> topicGroup_;
};

} // namespace rune::metadata
