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

class ShardRouter {
public:
    explicit ShardRouter(int32_t workerCount) noexcept
        : workerCount_(workerCount) {
        assert(workerCount_ >= 1 && "workerCount must be >= 1");
    }

    void mapTopicToGroupKey(int32_t topicId, int32_t groupKey) noexcept {
        topicGroup_[topicId] = groupKey;
    }

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
