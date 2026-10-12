#include <rune/gateway/TopicRegistry.h>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace rune::gateway {

// ---------------------------------------------------------------------------
// TopicRegistry::TopicRegistry
// ---------------------------------------------------------------------------
TopicRegistry::TopicRegistry(
    const std::vector<rune::node::TopicSpec>& topics,
    const std::string& dataDir)
    : dataDir_(dataDir)
{
    topics_.reserve(topics.size());
    nameIndex_.reserve(topics.size());

    for (const auto& spec : topics) {
        createTopic(spec);
    }

    spdlog::info("TopicRegistry: {} topic(s) initialised", topics_.size());
}

// ---------------------------------------------------------------------------
// TopicRegistry::createTopic
// ---------------------------------------------------------------------------
int32_t
TopicRegistry::createTopic(const rune::node::TopicSpec& spec)
{
    // Idempotent guard: if a topic with this name already exists, return its id.
    {
        auto it = nameIndex_.find(spec.name);
        if (it != nameIndex_.end()) {
            spdlog::debug(
                "TopicRegistry::createTopic: topic='{}' already registered with id={}",
                spec.name, it->second);
            return it->second;
        }
    }

    const fs::path root{dataDir_};

    // Assign the next sequential topicId (the registry owns id assignment so that
    // topics_[topicId] lookups stay valid — spec.topicId is the caller's hint but
    // is intentionally ignored for placement).
    const int32_t assignedId = static_cast<int32_t>(topics_.size());

    TopicEntry entry;
    entry.spec           = spec;
    entry.spec.topicId   = assignedId;   // store the assigned (sequential) id
    entry.partitions.reserve(static_cast<std::size_t>(spec.partitionCount));

    for (int32_t p = 0; p < spec.partitionCount; ++p) {
        PartitionEntry pe;

        // Transient topics (PersistenceMode::Transient) do NOT create disk
        // directories or Log objects.  The PartitionEntry is left with log=nullptr.
        // BrokerNode::buildReplication() detects this and substitutes an InMemoryLog.
        if (spec.persistence == rune::node::PersistenceMode::Transient) {
            spdlog::debug(
                "TopicRegistry: topic='{}' partition={} [transient, no disk log]",
                spec.name, p);
            entry.partitions.push_back(std::move(pe));
            continue;
        }

        // dataDir/topics/<name>/<partition>/
        fs::path partDir = root / "topics" / spec.name / std::to_string(p);

        std::error_code ec;
        fs::create_directories(partDir, ec);
        if (ec) {
            throw std::runtime_error(
                "TopicRegistry: cannot create partition dir '" +
                partDir.string() + "': " + ec.message());
        }

        // Honour the topic's configured segment roll size (default 64 MiB;
        // tests may set a small value to form multiple sealed segments so
        // compaction can be exercised end-to-end).
        auto logPtr = std::make_unique<rune::storage::Log>(
            partDir.string(), spec.segmentRollBytes);
        logPtr->recover();

        spdlog::debug(
            "TopicRegistry: topic='{}' partition={} dir='{}' LEO={}",
            spec.name, p, partDir.string(), logPtr->logEndOffset());

        pe.log = std::move(logPtr);
        entry.partitions.push_back(std::move(pe));
    }

    nameIndex_.emplace(spec.name, assignedId);
    topics_.push_back(std::move(entry));

    spdlog::info(
        "TopicRegistry::createTopic: topic='{}' id={} partitions={}",
        spec.name, assignedId, spec.partitionCount);

    return assignedId;
}

// ---------------------------------------------------------------------------
// TopicRegistry::topicIdForName
// ---------------------------------------------------------------------------
std::optional<int32_t>
TopicRegistry::topicIdForName(std::string_view name) const
{
    auto it = nameIndex_.find(std::string(name));
    if (it == nameIndex_.end()) return std::nullopt;
    return it->second;
}

// ---------------------------------------------------------------------------
// TopicRegistry::topic
// ---------------------------------------------------------------------------
const rune::node::TopicSpec*
TopicRegistry::topic(int32_t topicId) const noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return nullptr;
    return &topics_[static_cast<std::size_t>(topicId)].spec;
}

// ---------------------------------------------------------------------------
// TopicRegistry::log
// ---------------------------------------------------------------------------
rune::storage::Log*
TopicRegistry::log(int32_t topicId, int32_t partition) noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return nullptr;
    const auto& entry = topics_[static_cast<std::size_t>(topicId)];
    if (partition < 0 || static_cast<std::size_t>(partition) >= entry.partitions.size())
        return nullptr;
    return entry.partitions[static_cast<std::size_t>(partition)].log.get();
}

// ---------------------------------------------------------------------------
// TopicRegistry::partitionForKey
// ---------------------------------------------------------------------------
int32_t
TopicRegistry::partitionForKey(int32_t topicId,
                                std::span<const uint8_t> key) const noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return -1;
    const int32_t pc = topics_[static_cast<std::size_t>(topicId)].spec.partitionCount;
    return rune::protocol::partitionForKey(key, pc);
}

// ---------------------------------------------------------------------------
// TopicRegistry::cleanupPolicyOf (CP-1)
// ---------------------------------------------------------------------------
rune::node::TopicSpec::CleanupPolicy
TopicRegistry::cleanupPolicyOf(int32_t topicId) const noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return rune::node::TopicSpec::CleanupPolicy::Delete;
    return topics_[static_cast<std::size_t>(topicId)].spec.cleanupPolicy;
}

// ---------------------------------------------------------------------------
// TopicRegistry::dirtyRatioThresholdOf (CP-1)
// ---------------------------------------------------------------------------
double
TopicRegistry::dirtyRatioThresholdOf(int32_t topicId) const noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return 0.5;
    return topics_[static_cast<std::size_t>(topicId)].spec.minCleanableDirtyRatio;
}

// ---------------------------------------------------------------------------
// TopicRegistry::deleteRetentionMsOf (CP-1)
// ---------------------------------------------------------------------------
int64_t
TopicRegistry::deleteRetentionMsOf(int32_t topicId) const noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return 86'400'000LL;
    return topics_[static_cast<std::size_t>(topicId)].spec.deleteRetentionMs;
}

} // namespace rune::gateway
