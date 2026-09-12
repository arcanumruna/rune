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
{
    topics_.reserve(topics.size());
    nameIndex_.reserve(topics.size());

    const fs::path root{dataDir};

    for (const auto& spec : topics) {
        TopicEntry entry;
        entry.spec = spec;
        entry.partitions.reserve(static_cast<std::size_t>(spec.partitionCount));

        for (int32_t p = 0; p < spec.partitionCount; ++p) {
            // dataDir/topics/<name>/<partition>/
            fs::path partDir = root / "topics" / spec.name / std::to_string(p);

            std::error_code ec;
            fs::create_directories(partDir, ec);
            if (ec) {
                throw std::runtime_error(
                    "TopicRegistry: cannot create partition dir '" +
                    partDir.string() + "': " + ec.message());
            }

            auto logPtr = std::make_unique<rune::storage::Log>(partDir.string());
            logPtr->recover();

            spdlog::debug(
                "TopicRegistry: topic='{}' partition={} dir='{}' LEO={}",
                spec.name, p, partDir.string(), logPtr->logEndOffset());

            PartitionEntry pe;
            pe.log = std::move(logPtr);
            entry.partitions.push_back(std::move(pe));
        }

        nameIndex_.emplace(spec.name, spec.topicId);
        topics_.push_back(std::move(entry));
    }

    spdlog::info("TopicRegistry: {} topic(s) initialised", topics_.size());
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

} // namespace rune::gateway
