#include <rune/gateway/TopicRegistry.h>
#include <rune/node/TopicName.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <unistd.h>

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
        createTopicImpl(spec, CreateOrigin::Boot);
    }

    spdlog::info("TopicRegistry: {} topic(s) initialised", topics_.size());
}

// ---------------------------------------------------------------------------
// TopicRegistry::createTopic
// ---------------------------------------------------------------------------
int32_t
TopicRegistry::createTopic(const rune::node::TopicSpec& spec)
{
    return createTopicImpl(spec, CreateOrigin::Runtime);
}

int32_t
TopicRegistry::createTopicImpl(const rune::node::TopicSpec& spec, CreateOrigin origin)
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

    // B10 / D4 (review M1): the marker is only ever written into a topic dir THIS
    // create made — never stamped onto a pre-existing dir (e.g. one a case-variant
    // name shares on a case-insensitive filesystem, or a surviving old dir).
    const fs::path topicDir = root / "topics" / spec.name;
    std::error_code existEc;
    const bool topicDirPreexisted = fs::exists(fs::symlink_status(topicDir, existEc));

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

    // B10 / D4: the two-phase topic-id marker ties this directory to its id, so
    // boot can tell a never-committed orphan's dir (pending: deleted) from any other
    // unclaimed dir (quarantined).  Runtime creates only — see kTopicIdMarkerFile.
    // Fix round 2 (N2): a runtime create that REUSES a pre-existing dir (a surviving
    // orphan's dir) clears any stale marker there, so an old "pending <otherId>"
    // can never arm a delete of this topic's data.
    if (origin == CreateOrigin::Runtime && topicDirPreexisted &&
        spec.persistence != rune::node::PersistenceMode::Transient && spec.partitionCount > 0) {
        if (clearTopicIdMarker(topicDir.string()))
            spdlog::warn("TopicRegistry: topic='{}' reuses an existing dir — removed its stale "
                         "topic-id marker", spec.name);
    }
    if (origin == CreateOrigin::Runtime && !topicDirPreexisted &&
        spec.persistence != rune::node::PersistenceMode::Transient && spec.partitionCount > 0) {
        if (writeTopicIdMarker(topicDir.string(), kTopicIdMarkerPending, assignedId))
            entry.marker = MarkerState::Pending;
    }

    // C1: a runtime create is not servable until its catalog record commits.
    entry.committed = (origin == CreateOrigin::Boot);

    nameIndex_.emplace(spec.name, assignedId);
    topics_.push_back(std::move(entry));

    spdlog::info(
        "TopicRegistry::createTopic: topic='{}' id={} partitions={}",
        spec.name, assignedId, spec.partitionCount);

    return assignedId;
}

// ---------------------------------------------------------------------------
// writeTopicIdMarker / TopicRegistry::markTopicCommitted (B10 / D4, fix round 1)
// ---------------------------------------------------------------------------
namespace {
// Ensures `dir` is a real directory (never a symlink).
bool isPlainDir(const fs::path& dir) {
    std::error_code ec;
    const fs::file_status st = fs::symlink_status(dir, ec);
    return !ec && fs::is_directory(st) && !fs::is_symlink(st);
}
} // namespace

bool
writeTopicIdMarker(const std::string& topicDir, std::string_view state, int32_t id)
{
    const fs::path dir{topicDir};
    const fs::path tmp    = dir / kTopicIdMarkerTmpFile;
    const fs::path marker = dir / kTopicIdMarkerFile;
    if (!isPlainDir(dir)) {
        spdlog::warn("TopicRegistry: not writing topic-id marker — '{}' is not a plain directory",
                     dir.string());
        return false;
    }
    {   // the final path must be absent or a regular file — never a symlink
        std::error_code ec;
        const fs::file_status mst = fs::symlink_status(marker, ec);
        if (fs::exists(mst) && !fs::is_regular_file(mst)) {
            spdlog::warn("TopicRegistry: not writing topic-id marker — '{}' is not a regular file",
                         marker.string());
            return false;
        }
    }
    std::error_code rmEc;
    fs::remove(tmp, rmEc);   // unlinks a leftover/planted entry itself, never a symlink's target

    char buf[48];
    const int len = std::snprintf(buf, sizeof buf, "%.*s %d\n",
                                  static_cast<int>(state.size()), state.data(), id);
    if (len <= 0 || static_cast<std::size_t>(len) >= sizeof buf) return false;

    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0) {
        spdlog::warn("TopicRegistry: cannot create topic-id marker temp '{}': {}", tmp.string(),
                     std::strerror(errno));
        return false;
    }
    bool ok = true;
    std::size_t off = 0;
    while (off < static_cast<std::size_t>(len)) {
        const ssize_t w = ::write(fd, buf + off, static_cast<std::size_t>(len) - off);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { ok = false; break; }
        off += static_cast<std::size_t>(w);
    }
    if (::close(fd) != 0) ok = false;
    if (!ok) {
        spdlog::warn("TopicRegistry: cannot write topic-id marker temp '{}'", tmp.string());
        std::error_code rec;
        fs::remove(tmp, rec);
        return false;
    }
    std::error_code ec;
    fs::rename(tmp, marker, ec);   // atomic replace: a reader sees the old or the new marker
    if (ec) {
        spdlog::warn("TopicRegistry: cannot install topic-id marker '{}': {}", marker.string(),
                     ec.message());
        std::error_code rec;
        fs::remove(tmp, rec);
        return false;
    }
    return true;
}

bool
clearTopicIdMarker(const std::string& topicDir)
{
    const fs::path dir{topicDir};
    if (!isPlainDir(dir)) return false;
    bool removed = false;
    std::error_code ec;
    if (fs::remove(dir / kTopicIdMarkerFile, ec)) removed = true;   // unlink only
    ec.clear();
    if (fs::remove(dir / kTopicIdMarkerTmpFile, ec)) removed = true;
    return removed;
}

bool
TopicRegistry::markTopicCommitted(int32_t topicId)
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return false;
    TopicEntry& e = topics_[static_cast<std::size_t>(topicId)];
    if (e.marker != MarkerState::Pending) return false;
    const fs::path topicDir = fs::path(dataDir_) / "topics" / e.spec.name;
    if (!writeTopicIdMarker(topicDir.string(), kTopicIdMarkerCommitted, topicId)) return false;
    e.marker = MarkerState::Committed;
    return true;
}

bool
TopicRegistry::commitTopic(int32_t topicId)
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return false;
    TopicEntry& e = topics_[static_cast<std::size_t>(topicId)];
    // The marker rewrite is best-effort (logged on failure); servability never
    // waits on it — the record IS committed.
    (void)markTopicCommitted(topicId);
    if (e.committed) return false;
    e.committed = true;
    return true;
}

rune::storage::Log*
TopicRegistry::servableLog(int32_t topicId, int32_t partition) noexcept
{
    if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size())
        return nullptr;
    const auto& entry = topics_[static_cast<std::size_t>(topicId)];
    if (!entry.committed) [[unlikely]] return nullptr;
    if (partition < 0 || static_cast<std::size_t>(partition) >= entry.partitions.size())
        return nullptr;
    return entry.partitions[static_cast<std::size_t>(partition)].log.get();
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
        if (n != name && rune::node::topicNamesFoldEqual(n, name)) return id;
    }
    return std::nullopt;
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
