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

#include <rune/gateway/NsNameKey.h>
#include <rune/node/NodeConfig.h>
#include <rune/storage/Log.h>
#include <rune/protocol/PartitionHash.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
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
    topicIdForName(std::string_view name) const { return topicIdForName(0, name); }
    // Namespaces plan Task 6: names are unique per namespace -- the (nsId, name) lookup.  The name-only form above
    // is the default namespace (ns 0).
    [[nodiscard]] std::optional<int32_t>
    topicIdForName(uint32_t nsId, std::string_view name) const;

    // -----------------------------------------------------------------------
    // topicIdForFoldedName — a topic whose name equals `name` ignoring ASCII case
    // but is NOT exactly `name` (DDL conformance B3: the two would share
    // topicsRoot(dataDir, nsId)/<name>/ on a case-insensitive filesystem).  Linear scan —
    // DDL (cold path) only.  Empty optional when there is none.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::optional<int32_t>
    topicIdForFoldedName(std::string_view name) const { return topicIdForFoldedName(0, name); }
    [[nodiscard]] std::optional<int32_t>
    topicIdForFoldedName(uint32_t nsId, std::string_view name) const;

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
    // Catalog history Task 8: topics created and dropped BY THE CATALOG.
    //
    // A catalog id's slot is one of (rule 3, one state per slot):
    //   Gap     — an id below a created one that this broker has not seen
    //             (created-and-dropped before its checkpoint, OR not applied
    //             yet): no partitions, never serves, FILLABLE by a later create;
    //   Pending — a create this broker applied that waits behind the drop of its
    //             name (reserveTopicAt): no partitions, never serves (answered
    //             the retriable NotLeader), filled by createTopicAt with the
    //             same id + name;
    //   Serving — created (createTopicAt): serves once its placement is committed
    //             too (placementGated(), spec §5.4);
    //   Dropped — dropped: never serves, NEVER refilled (catalog ids are never
    //             reused).
    //
    // createTopicAt — creates the entry at EXACTLY `topicId` (spec.topicId is
    // ignored), filling a gap below it with Gap slots.  Creates
    // topicsRoot(dataDir, spec.nsId)/<name>/<p>/ for a disk topic and opens + recovers each Log
    // (an existing dir is recovered, never wiped: a restart re-runs the effect).
    // A disk topic's dir carries the topic-id marker (TopicDirMarker.h): written before any partition dir
    // exists; a dir marked with another id, or markerless with record bytes, is never opened (returns -1).
    // Idempotent for the same id + name (returns topicId, nothing rebuilt).
    // Returns -1 (logged) when the id is Dropped, holds another name, or the
    // name is held by another live id.
    // Throws std::runtime_error if a directory cannot be created.
    // SINGLE-WRITER: control thread only.
    int32_t createTopicAt(int32_t topicId, const rune::node::TopicSpec& spec);

    // reserveTopicAt — the create of (topicId, name) was applied but waits
    // behind the drop of its name: the slot becomes Pending (a Gap below it is
    // filled with Gap slots).  Idempotent; false (logged) when the slot is
    // Serving or Dropped, or Pending under another name.
    bool reserveTopicAt(int32_t topicId, const std::string& name);

    // dropTopic — the entry becomes Dropped: it serves nothing from now on
    // (topic() / log() / servableLog() answer nullptr) and its name is freed
    // from the name index.  The Logs stay owned until releaseDroppedLogs(): a
    // retired replica may still reference them until every holder acked.  A
    // Pending or Gap slot is dropped too (seen: dropped), and so is an id beyond
    // the registry (this broker never built it: Gap slots below, a Dropped slot at
    // the id — Task 16, B3).  Returns true iff the entry moved to Dropped
    // (idempotent: false again).
    bool dropTopic(int32_t topicId);

    /// Test-only: Logs still owned by the entry (a Dropped one: 0 once released).
    [[nodiscard]] std::size_t heldLogCountForTest(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        if (sl == nullptr) return 0;
        const std::vector<PartitionEntry>* parts = nullptr;
        if (const auto* d = std::get_if<Dropped>(sl)) parts = &d->partitions;
        else if (const Live* l = live_(*sl)) parts = &l->partitions;
        std::size_t n = 0;
        if (parts != nullptr) for (const auto& pe : *parts) n += pe.log ? 1 : 0;
        return n;
    }

    // releaseDroppedLogs — closes (destroys) a Dropped entry's Logs.  The
    // caller guarantees nothing references them any more.  Returns true iff
    // any Log was released (idempotent).
    bool releaseDroppedLogs(int32_t topicId);

    // The catalog view of an id.  Serving = a static or catalog topic that
    // serves; Pending = applied, waiting behind a drop; Dropped = dropped here;
    // Unknown = never registered here, or a Gap.
    ///   ViewSink = a view's replica-less catalog-only sink (VLS B.2): no logs, no dirs, never a data-plane target.
    enum class TopicState : uint8_t { Unknown, Serving, Pending, Dropped, ViewSink };
    [[nodiscard]] TopicState state(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        if (sl == nullptr) return TopicState::Unknown;
        switch (sl->index()) {
            case kStaticIdx:
            case kServingIdx: return TopicState::Serving;
            case kPendingIdx: return TopicState::Pending;
            case kDroppedIdx: return TopicState::Dropped;
            case kViewSinkIdx: return TopicState::ViewSink;
            default:          return TopicState::Unknown;   // Gap
        }
    }

    /// VLS B.3: is the id a view sink?  O(1); the gateway front's refusal test for every data-plane op.
    [[nodiscard]] bool isViewSink(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        return sl != nullptr && sl->index() == kViewSinkIdx;
    }
    /// A view sink's spec (partitions, key field, name); nullptr for any other slot.
    [[nodiscard]] const rune::node::TopicSpec* viewSinkSpec(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        const auto* v = sl != nullptr ? std::get_if<ViewSink>(sl) : nullptr;
        return v != nullptr ? &v->spec : nullptr;
    }

    /// A catalog-applied topic (createTopicAt) that serves only once its
    /// placement is committed too (C1 generalized, spec §5.4).  Cold probe.
    [[nodiscard]] bool placementGated(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        return sl != nullptr && std::holds_alternative<Serving>(*sl);
    }

    /// E2 final fix wave (M6): bumped on every catalog change (a topic created,
    /// reserved or dropped) — holders of a cached per-partition classification
    /// (the gateway's transient subscriptions) refresh it when it moves.
    [[nodiscard]] uint64_t catalogGeneration() const noexcept { return catalogGeneration_; }

    /// The data-plane accessor: log() of a serving topic, nullptr for any other
    /// slot (callers tell them apart with state(), off the hot path).
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
    uint64_t catalogGeneration_ = 0;   ///< M6: see catalogGeneration()

    struct PartitionEntry {
        std::unique_ptr<rune::storage::Log> log;   // nullptr for a transient partition
    };
    /// What a serving slot holds: its spec and one entry per partition.
    struct Live {
        rune::node::TopicSpec       spec;
        std::vector<PartitionEntry> partitions;   // size == spec.partitionCount
    };
    // One state per slot (hard rule 3), each holding exactly what it needs:
    //   Static  — a topic handed to the constructor (library tests; the broker builds none: topics are created by DDL);
    //   Serving — a catalog topic (createTopicAt): serves once its placement commits;
    //   Pending — applied, waiting behind the drop of its name: the name only;
    //   Gap     — an id this broker has not seen: nothing;
    //   Dropped — dropped: its spec (the config accessors still answer) and the
    //             Logs, held until releaseDroppedLogs.
    struct Static  : Live {};
    struct Serving : Live {};
    struct Pending { std::string name; };
    struct Gap {};
    struct Dropped {
        rune::node::TopicSpec       spec;
        std::vector<PartitionEntry> partitions;
    };
    struct ViewSink { rune::node::TopicSpec spec; };   // VLS B.2: id, name, partitions, key field -- nothing else
    using Slot = std::variant<Static, Serving, Pending, Gap, Dropped, ViewSink>;
    static constexpr std::size_t kViewSinkIdx = 5;
    static constexpr std::size_t kStaticIdx  = 0;
    static constexpr std::size_t kServingIdx = 1;
    static constexpr std::size_t kPendingIdx = 2;
    static constexpr std::size_t kDroppedIdx = 4;

    [[nodiscard]] const Slot* slot_(int32_t topicId) const noexcept {
        if (topicId < 0 || static_cast<std::size_t>(topicId) >= topics_.size()) return nullptr;
        return &topics_[static_cast<std::size_t>(topicId)];
    }
    /// The serving part of a Static / Serving slot; nullptr for any other.
    [[nodiscard]] static const Live* live_(const Slot& s) noexcept {
        if (s.index() == kServingIdx) return &std::get<Serving>(s);
        if (s.index() == kStaticIdx)  return &std::get<Static>(s);
        return nullptr;
    }
    [[nodiscard]] static Live* live_(Slot& s) noexcept { return const_cast<Live*>(live_(std::as_const(s))); }
    /// The spec of any slot that has one (Static / Serving / Dropped).
    [[nodiscard]] const rune::node::TopicSpec* specOf_(int32_t topicId) const noexcept {
        const Slot* sl = slot_(topicId);
        if (sl == nullptr) return nullptr;
        if (const Live* l = live_(*sl)) return &l->spec;
        if (const auto* d = std::get_if<Dropped>(sl)) return &d->spec;
        return nullptr;
    }
    void fillGapsBelow_(std::size_t slot);
    // Builds dirs + Logs for `spec` at `id` (the constructor and createTopicAt).
    Live buildLive_(const rune::node::TopicSpec& spec, int32_t id);

    std::string                              dataDir_;     // root data directory
    std::vector<Slot>                        topics_;      // indexed by topicId
    // live (nsId, name) → topicId (namespaces plan Task 6: a name is unique per namespace)
    std::unordered_map<NsName, int32_t, NsNameHash, NsNameEq> nameIndex_;
};

} // namespace rune::gateway
