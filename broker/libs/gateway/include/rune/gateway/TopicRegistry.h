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

/// The two-phase topic-id marker in dataDir/topics/<name>/ (B10 / D4, fix round 1).
/// One line, written atomically (temp file + rename):
///   "pending <id>\n"   — a runtime create made this dir; its catalog record has
///                        not been confirmed committed yet;
///   "committed <id>\n" — the admin thread confirmed the record committed
///                        (kA2cTopicCommitted on the admin→control ring; the
///                        control thread, which owns this registry, rewrites it).
/// Boot (rune::node::reconcileTopicDirs) DELETES an unclaimed dir only when its
/// marker is "pending" AND recovery created a placeholder for that id.  A
/// "committed" marker whose id has no catalog record means the CATALOG was damaged
/// (CRC truncation, a missing catalog dir): that dir is quarantined, never deleted.
///
/// Written ONLY for a topic created at RUNTIME (DefineTopic / CreateView sink, i.e.
/// createTopic after construction) into a topic dir THIS call created — never for
/// the topics the constructor builds (static RUNE_TOPICS + recovered).  A static
/// topic's id is its position in RUNE_TOPICS, so removing the last static topic
/// while runtime topics exist turns its id into a recovery gap (an __orphan.<id>
/// placeholder); were static dirs marked, boot would DELETE that removed static
/// topic's data instead of quarantining it (D4: only a never-committed definition
/// is ever deleted).
inline constexpr const char* kTopicIdMarkerFile    = "topic-id";
/// The temp file the marker is staged in before the atomic rename.
inline constexpr const char* kTopicIdMarkerTmpFile = "topic-id.tmp";
inline constexpr std::string_view kTopicIdMarkerPending   = "pending";
inline constexpr std::string_view kTopicIdMarkerCommitted = "committed";

/// Atomically (kTopicIdMarkerTmpFile + rename) writes "<state> <id>\n" as
/// topicDir/kTopicIdMarkerFile.  Returns false (logged, temp removed) on failure.
/// Used by TopicRegistry (create / markTopicCommitted) and by boot's
/// reconcileTopicDirs (promotion of a claimed dir's pending marker).
/// Never follows a symlink (fix round 2, N1): refuses when topicDir or the final
/// marker path is a symlink (or not a dir / not a regular file); unlinks any
/// existing temp entry (a planted symlink is unlinked itself, never its target)
/// and creates the temp with O_CREAT|O_EXCL|O_NOFOLLOW, so a symlink re-planted
/// in between makes the open FAIL rather than write through it.
bool writeTopicIdMarker(const std::string& topicDir, std::string_view state, int32_t id);

/// Removes topicDir/topic-id and topicDir/topic-id.tmp (unlink only — a symlink
/// is removed itself, never followed).  Refuses (false) when topicDir is a symlink
/// or not a directory.  Removing a marker can only turn a future delete into a
/// quarantine (fix round 2, N2).  Returns true iff a marker entry was removed.
bool clearTopicIdMarker(const std::string& topicDir);

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
    // topicIdForFoldedName — a topic whose name equals `name` ignoring ASCII case
    // but is NOT exactly `name` (DDL conformance B3: the two would share
    // dataDir/topics/<name>/ on a case-insensitive filesystem).  Linear scan —
    // DDL (cold path) only.  Empty optional when there is none.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::optional<int32_t>
    topicIdForFoldedName(std::string_view name) const;

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
    // createTopic — runtime topic creation (post-boot).
    //
    // Creates partition directories + Log objects for the given spec, runs
    // Log::recover() on each partition, and registers the topic in the
    // internal name/id indexes.
    //
    // The assigned topicId is the next sequential index (topics_.size() at
    // the time of insertion) and is stored back into the entry's spec.
    // The spec.topicId field is IGNORED for placement; the registry owns
    // the assignment so that topics_[id] lookups remain valid.
    //
    // Idempotent: if a topic with the same name already exists, returns
    // its existing topicId without recreating any directories or logs.
    // Writes dataDir/topics/<name>/topic-id = "pending <id>" for a disk topic whose
    // dir this call created (B10 / D4 — see kTopicIdMarkerFile; the constructor's
    // topics get none).
    //
    // Returns the assigned topicId (>= 0).
    // Throws std::runtime_error if a directory cannot be created.
    // SINGLE-WRITER: must only be called on the gateway duty-cycle thread.
    // -----------------------------------------------------------------------
    int32_t createTopic(const rune::node::TopicSpec& spec);

    // -----------------------------------------------------------------------
    // markTopicCommitted — B10 / D4 fix round 1: the topic's catalog record
    // (DefineTopic schema / CreateView record) has committed.  Rewrites a
    // "pending <id>" marker as "committed <id>" (atomic temp + rename).  A topic
    // without a pending marker (boot-built, transient, an existing name) is a
    // no-op.  Returns true iff the marker moved Pending -> Committed.
    // SINGLE-WRITER: control thread only (driven by kA2cTopicCommitted).
    // -----------------------------------------------------------------------
    bool markTopicCommitted(int32_t topicId);

    // -----------------------------------------------------------------------
    // Servability (DDL conformance final review C1).  A topic created at RUNTIME
    // (DefineTopic / CreateView sink) is AWAITING COMMIT until its catalog record
    // commits: it is neither writable nor readable (produce / subscribe answer the
    // retriable NotLeader, metadata hides it), so nothing can be acknowledged into a
    // topic a later boot may reclaim as an orphan.  Boot-built topics (static
    // RUNE_TOPICS + recovered runtime topics) are committed from construction.
    //
    // commitTopic — the record committed (kA2cTopicCommitted on the admin→control
    // ring): the topic becomes servable and its dir marker moves pending ->
    // committed (markTopicCommitted).  Returns true iff the topic was awaiting
    // commit (the caller then builds its replicas + assignment).  Idempotent.
    // SINGLE-WRITER: control thread only.
    // -----------------------------------------------------------------------
    bool commitTopic(int32_t topicId);

    /// The topic exists and is still awaiting its commit (not servable).
    [[nodiscard]] bool awaitingCommit(int32_t topicId) const noexcept {
        return topicId >= 0 && static_cast<std::size_t>(topicId) < topics_.size() &&
               !topics_[static_cast<std::size_t>(topicId)].committed;
    }

    /// The data-plane accessor: log() for a SERVABLE topic, nullptr while the topic
    /// is awaiting commit (callers tell the two apart with awaitingCommit(), off the
    /// hot path).  One extra byte compare on the entry log() already reads.
    /// SINGLE-WRITER: gateway duty-cycle (control) thread only.
    [[nodiscard]] rune::storage::Log*
    servableLog(int32_t topicId, int32_t partition) noexcept;

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
    // Which caller built the topic: the constructor (static + recovered topics) or
    // a runtime DDL create.  Only a runtime create writes the topic-id marker.
    enum class CreateOrigin : uint8_t { Boot, Runtime };
    int32_t createTopicImpl(const rune::node::TopicSpec& spec, CreateOrigin origin);

    // Per-partition entry: the Log and its directory path.
    struct PartitionEntry {
        std::unique_ptr<rune::storage::Log> log;
    };

    // The topic-id marker's state for this topic (see kTopicIdMarkerFile).
    // None = no marker (boot-built, transient, or a dir this create did not make).
    enum class MarkerState : uint8_t { None, Pending, Committed };

    // Per-topic entry: spec + flattened partition array.
    struct TopicEntry {
        rune::node::TopicSpec             spec;
        std::vector<PartitionEntry>       partitions; // size == spec.partitionCount
        MarkerState                       marker = MarkerState::None;
        bool                              committed = true;   // false: runtime create awaiting its record (C1)
    };

    std::string                            dataDir_;       // root data directory (for createTopic)
    std::vector<TopicEntry>               topics_;        // indexed by topicId
    std::unordered_map<std::string, int32_t> nameIndex_;  // name → topicId
};

} // namespace rune::gateway
