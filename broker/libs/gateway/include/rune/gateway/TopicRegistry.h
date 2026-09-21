#pragma once
// TopicRegistry.h — Per-partition Log registry for broker topics (GB-1).
//
// Maps topicId → {TopicSpec, per-partition Log} over the Phase-1 storage engine.
// Constructed from NodeConfig::topics + a dataDir root; each Log lives under:
//   dataDir/topics/<name>/<partition>/
//
// Lifecycle:
//   TopicRegistry ctor creates directories + constructs + recover()s each Log.
//   Restart safety: recover() advances logEndOffset to the true LEO so appends
//   after a restart resume at the correct position (no data loss or offset gap).
//
// Threading:
//   SINGLE-WRITER: all Log appends and reads happen exclusively on the gateway
//   duty-cycle thread (the AgentRunner thread).  No internal locks are taken.
//   Callers MUST NOT share a TopicRegistry across threads without external
//   synchronisation.
//
// API notes:
//   - topicIdForName / topic / log / partitionForKey are bounds-checked.
//   - Unknown topicId or out-of-range partition → nullptr / empty optional.
//   - partitionForKey uses the shared rune::protocol::partitionForKey (FNV-1a 64)
//     so broker and client always agree on the mapping.

#include <rune/node/NodeConfig.h>
#include <rune/storage/Log.h>
#include <rune/protocol/PartitionHash.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::gateway {

class TopicRegistry {
public:
    // Construct the registry from a topic list + data root directory.
    // For each topic, creates directories and opens + recover()s each partition Log.
    // Throws std::runtime_error if a directory cannot be created.
    TopicRegistry(const std::vector<rune::node::TopicSpec>& topics,
                  const std::string& dataDir);

    // Non-copyable, non-movable (owns Log instances).
    TopicRegistry(const TopicRegistry&)            = delete;
    TopicRegistry& operator=(const TopicRegistry&) = delete;

    // -----------------------------------------------------------------------
    // topicCount — total number of registered topics.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t topicCount() const noexcept {
        return static_cast<int32_t>(topics_.size());
    }

    // -----------------------------------------------------------------------
    // topicIdForName — look up a topic by name.
    // Returns empty optional if the name is not registered.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::optional<int32_t>
    topicIdForName(std::string_view name) const;

    // -----------------------------------------------------------------------
    // topic — return the TopicSpec for a given topicId.
    // Returns nullptr if topicId is out of range.
    // -----------------------------------------------------------------------
    [[nodiscard]] const rune::node::TopicSpec*
    topic(int32_t topicId) const noexcept;

    // -----------------------------------------------------------------------
    // log — return the Log for a given (topicId, partition).
    // Returns nullptr if topicId or partition is out of range.
    // SINGLE-WRITER: must only be called on the gateway duty-cycle thread.
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::storage::Log*
    log(int32_t topicId, int32_t partition) noexcept;

    // -----------------------------------------------------------------------
    // partitionForKey — map an opaque key to a partition index.
    // Uses rune::protocol::partitionForKey (FNV-1a 64 % partitionCount).
    // Returns -1 if topicId is out of range (caller treats as UnknownTopic).
    // Result is in [0, topic.partitionCount) when topicId is valid.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t
    partitionForKey(int32_t topicId, std::span<const uint8_t> key) const noexcept;

    // -----------------------------------------------------------------------
    // CP-1: per-topic retention + compaction config accessors.
    // All return a default/safe value when topicId is out of range.
    // -----------------------------------------------------------------------

    /// Return the cleanup policy for topicId.
    /// Returns CleanupPolicy::Delete if topicId is out of range.
    [[nodiscard]] rune::node::TopicSpec::CleanupPolicy
    cleanupPolicyOf(int32_t topicId) const noexcept;

    /// Return the minCleanableDirtyRatio threshold for topicId.
    /// Returns 0.5 (default) if topicId is out of range.
    [[nodiscard]] double
    dirtyRatioThresholdOf(int32_t topicId) const noexcept;

    /// Return the deleteRetentionMs (tombstone retention) for topicId.
    /// Returns 86_400_000 (1 day default) if topicId is out of range.
    [[nodiscard]] int64_t
    deleteRetentionMsOf(int32_t topicId) const noexcept;

private:
    // Per-partition entry: the Log and its directory path.
    struct PartitionEntry {
        std::unique_ptr<rune::storage::Log> log;
    };

    // Per-topic entry: spec + flattened partition array.
    struct TopicEntry {
        rune::node::TopicSpec             spec;
        std::vector<PartitionEntry>       partitions; // size == spec.partitionCount
    };

    std::vector<TopicEntry>               topics_;        // indexed by topicId
    std::unordered_map<std::string, int32_t> nameIndex_;  // name → topicId
};

} // namespace rune::gateway
