#include <rune/gateway/TopicRegistry.h>
#include <rune/catalog/TopicName.h>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace rune::gateway {

// ---------------------------------------------------------------------------
// TopicRegistry::TopicRegistry — the static topics, at ids [0, n) in list order.
// ---------------------------------------------------------------------------
TopicRegistry::TopicRegistry(
    const std::vector<rune::node::TopicSpec>& topics,
    const std::string& dataDir)
    : dataDir_(dataDir)
{
    topics_.reserve(topics.size());
    nameIndex_.reserve(topics.size());

    for (const auto& spec : topics) {
        // Idempotent by name: a repeated static name is one topic.
        if (nameIndex_.contains(spec.name)) {
            spdlog::debug("TopicRegistry: static topic='{}' listed twice — one topic", spec.name);
            continue;
        }
        const auto id = static_cast<int32_t>(topics_.size());
        Live live = buildLive_(spec, id);
        topics_.emplace_back(std::in_place_type<Static>, Static{std::move(live)});
        nameIndex_.emplace(spec.name, id);
        ++catalogGeneration_;
    }

    spdlog::info("TopicRegistry: {} topic(s) initialised", topics_.size());
}

TopicRegistry::Live
TopicRegistry::buildLive_(const rune::node::TopicSpec& spec, int32_t id)
{
    const fs::path root{dataDir_};

    Live live;
    live.spec         = spec;
    live.spec.topicId = id;
    live.partitions.reserve(static_cast<std::size_t>(spec.partitionCount));

    for (int32_t p = 0; p < spec.partitionCount; ++p) {
        PartitionEntry pe;

        // Transient topics (PersistenceMode::Transient) do NOT create disk
        // directories or Log objects.  The PartitionEntry is left with log=nullptr.
        // BrokerNode::buildReplication() detects this and substitutes an InMemoryLog.
        if (spec.persistence == rune::node::PersistenceMode::Transient) {
            spdlog::debug(
                "TopicRegistry: topic='{}' partition={} [transient, no disk log]",
                spec.name, p);
            live.partitions.push_back(std::move(pe));
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
        live.partitions.push_back(std::move(pe));
    }
    return live;
}

// ---------------------------------------------------------------------------
// Catalog history Task 8: createTopicAt / reserveTopicAt / dropTopic / releaseDroppedLogs
// ---------------------------------------------------------------------------
void
TopicRegistry::fillGapsBelow_(std::size_t slot)
{
    while (topics_.size() < slot) topics_.emplace_back(std::in_place_type<Gap>);
}

int32_t
TopicRegistry::createTopicAt(int32_t topicId, const rune::node::TopicSpec& spec)
{
    if (topicId < 0 || spec.name.empty()) {
        spdlog::error("TopicRegistry::createTopicAt: refused id={} name='{}' (invalid)", topicId, spec.name);
        return -1;
    }
    const auto slot = static_cast<std::size_t>(topicId);
    if (slot < topics_.size()) {
        const Slot& e = topics_[slot];
        if (std::holds_alternative<Dropped>(e)) {
            spdlog::error("TopicRegistry::createTopicAt: id={} ('{}') is a dropped catalog id — never re-created",
                          topicId, spec.name);
            return -1;
        }
        if (const auto* p = std::get_if<Pending>(&e); p != nullptr && p->name != spec.name) {
            spdlog::error("TopicRegistry::createTopicAt: id={} is reserved for '{}', refused for '{}'",
                          topicId, p->name, spec.name);
            return -1;
        }
        if (const Live* l = live_(e)) {
            if (l->spec.name == spec.name && std::holds_alternative<Serving>(e)) return topicId;   // the same effect again
            spdlog::error("TopicRegistry::createTopicAt: id={} already holds {} '{}', refused for '{}'", topicId,
                          std::holds_alternative<Static>(e) ? "the static topic" : "", l->spec.name, spec.name);
            return -1;
        }
        // Gap, or the Pending slot of this very create: filled below.
    }
    if (const auto it = nameIndex_.find(spec.name); it != nameIndex_.end()) {
        spdlog::error("TopicRegistry::createTopicAt: name '{}' is held by live id={}, refused for id={}",
                      spec.name, it->second, topicId);
        return -1;
    }

    Live live = buildLive_(spec, topicId);   // may throw: nothing changed yet
    fillGapsBelow_(slot);                    // ids this broker has not seen (yet): Gap, fillable
    if (slot < topics_.size()) topics_[slot].emplace<Serving>(Serving{std::move(live)});
    else                       topics_.emplace_back(std::in_place_type<Serving>, Serving{std::move(live)});
    nameIndex_.emplace(spec.name, topicId);
    ++catalogGeneration_;
    spdlog::info("TopicRegistry::createTopicAt: topic='{}' id={} partitions={} (catalog)",
                 spec.name, topicId, spec.partitionCount);
    return topicId;
}

bool
TopicRegistry::reserveTopicAt(int32_t topicId, const std::string& name)
{
    if (topicId < 0 || name.empty()) return false;
    const auto slot = static_cast<std::size_t>(topicId);
    if (slot < topics_.size()) {
        Slot& e = topics_[slot];
        if (const auto* p = std::get_if<Pending>(&e); p != nullptr && p->name == name) return true;   // idempotent
        if (!std::holds_alternative<Gap>(e)) {
            spdlog::error("TopicRegistry::reserveTopicAt: id={} cannot be reserved for '{}' (taken)", topicId, name);
            return false;
        }
        e.emplace<Pending>(Pending{name});
        ++catalogGeneration_;
        return true;
    }
    fillGapsBelow_(slot);
    topics_.emplace_back(std::in_place_type<Pending>, Pending{name});
    ++catalogGeneration_;
    return true;
}

bool
TopicRegistry::dropTopic(int32_t topicId)
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return false;
    Slot& e = topics_[static_cast<std::size_t>(topicId)];
    if (std::holds_alternative<Dropped>(e)) return false;
    Live* l = live_(e);
    if (l == nullptr) {   // Pending / Gap: nothing indexed, no Logs — seen now, dropped
        rune::node::TopicSpec spec;
        spec.topicId = topicId;
        if (const auto* p = std::get_if<Pending>(&e)) spec.name = p->name;
        e.emplace<Dropped>(Dropped{std::move(spec), {}});
        ++catalogGeneration_;
        return true;
    }
    if (const auto it = nameIndex_.find(l->spec.name); it != nameIndex_.end() && it->second == topicId)
        nameIndex_.erase(it);
    const std::string name = l->spec.name;
    Dropped d{std::move(l->spec), std::move(l->partitions)};
    e.emplace<Dropped>(std::move(d));
    ++catalogGeneration_;
    spdlog::info("TopicRegistry::dropTopic: topic='{}' id={} dropped (serves nothing; logs held until released)",
                 name, topicId);
    return true;
}

bool
TopicRegistry::releaseDroppedLogs(int32_t topicId)
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return false;
    auto* d = std::get_if<Dropped>(&topics_[static_cast<std::size_t>(topicId)]);
    if (d == nullptr || d->partitions.empty()) return false;
    d->partitions.clear();   // closes every Log (unmaps its segments)
    d->partitions.shrink_to_fit();
    return true;
}

rune::storage::Log*
TopicRegistry::servableLog(int32_t topicId, int32_t partition) noexcept
{
    return log(topicId, partition);
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
// TopicRegistry::topicIdForFoldedName (DDL conformance B3)
// ---------------------------------------------------------------------------
std::optional<int32_t>
TopicRegistry::topicIdForFoldedName(std::string_view name) const
{
    for (const auto& [n, id] : nameIndex_) {
        if (n != name && rune::catalog::topicNamesFoldEqual(n, name)) return id;
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// TopicRegistry::topic — a serving slot's spec; nullptr for any other.
// ---------------------------------------------------------------------------
const rune::node::TopicSpec*
TopicRegistry::topic(int32_t topicId) const noexcept
{
    const Slot* sl = slot_(topicId);
    if (sl == nullptr) return nullptr;
    const Live* l = live_(*sl);
    return l != nullptr ? &l->spec : nullptr;
}

// ---------------------------------------------------------------------------
// TopicRegistry::log — a serving slot's partition Log; nullptr for any other.
// ---------------------------------------------------------------------------
rune::storage::Log*
TopicRegistry::log(int32_t topicId, int32_t partition) noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return nullptr;
    Live* l = live_(topics_[static_cast<std::size_t>(topicId)]);
    if (l == nullptr) [[unlikely]] return nullptr;   // dropped / gap / pending
    if (partition < 0 || static_cast<std::size_t>(partition) >= l->partitions.size()) return nullptr;
    return l->partitions[static_cast<std::size_t>(partition)].log.get();
}

// ---------------------------------------------------------------------------
// TopicRegistry::partitionForKey
// ---------------------------------------------------------------------------
int32_t
TopicRegistry::partitionForKey(int32_t topicId,
                                std::span<const uint8_t> key) const noexcept
{
    const rune::node::TopicSpec* spec = topic(topicId);
    if (spec == nullptr) return -1;   // not served here
    return rune::protocol::partitionForKey(key, spec->partitionCount);
}

// ---------------------------------------------------------------------------
// CP-1 config accessors — any slot with a spec (a Dropped one too); defaults otherwise.
// ---------------------------------------------------------------------------
rune::node::TopicSpec::CleanupPolicy
TopicRegistry::cleanupPolicyOf(int32_t topicId) const noexcept
{
    const rune::node::TopicSpec* spec = specOf_(topicId);
    return spec != nullptr ? spec->cleanupPolicy : rune::node::TopicSpec::CleanupPolicy::Delete;
}

double
TopicRegistry::dirtyRatioThresholdOf(int32_t topicId) const noexcept
{
    const rune::node::TopicSpec* spec = specOf_(topicId);
    return spec != nullptr ? spec->minCleanableDirtyRatio : 0.5;
}

int64_t
TopicRegistry::deleteRetentionMsOf(int32_t topicId) const noexcept
{
    const rune::node::TopicSpec* spec = specOf_(topicId);
    return spec != nullptr ? spec->deleteRetentionMs : 86'400'000LL;
}

} // namespace rune::gateway
