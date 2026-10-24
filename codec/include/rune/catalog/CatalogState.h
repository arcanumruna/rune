#pragma once
// rune/catalog/CatalogState.h — the catalog as a pure function of the applied
// __catalog prefix (spec §3: "A broker's in-memory catalog is purely a function of
// the __catalog prefix it has applied").  Single-threaded; owned by whoever replays
// (the broker's admin thread, a client, the CLI).  Mutated ONLY by apply().
#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/AlterOps.h>

#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::catalog {

struct SchemaState {
    std::string             keyField;
    bool                    validate      = false;
    std::vector<uint8_t>    evolvingBlob;
    uint16_t                schemaVersion = 1;
    uint16_t                nextTag       = 0;
    rune::codec::CompatMode compat        = rune::codec::CompatMode::Backward;
    bool operator==(const SchemaState&) const = default;
};
struct TopicObject {
    uint64_t                   objectId          = 0;
    std::string                name;
    uint32_t                   version           = 0;
    int32_t                    topicId           = -1;
    int32_t                    partitions        = 1;
    int16_t                    replicationFactor = 0;
    Persistence                persistence       = Persistence::Disk;
    TopicConfig                config;
    std::optional<SchemaState> schema;
    bool operator==(const TopicObject&) const = default;
};
struct ViewObject {
    uint64_t    objectId = 0;
    std::string name;
    uint32_t    version  = 0;
    ViewDef     def;
    bool operator==(const ViewObject&) const = default;
};
struct Tombstone {
    uint64_t    objectId     = 0;
    std::string name;
    uint32_t    finalVersion = 0;
    bool        wasView      = false;
    bool operator==(const Tombstone&) const = default;
};
struct Applied {
    EventKind kind           = EventKind::CreateTopic;
    uint64_t  objectId       = 0;
    uint32_t  newVersion     = 0;
    uint64_t  fingerprint    = 0;
    int32_t   createdTopicId = -1;   // CreateTopic / CreateView / AlterView: the new (sink) topic id
    int32_t   droppedTopicId = -1;   // DropTopic / DropView / AlterView: the retired (sink) topic id
};
/// What an applied DDL answered — replayed to a retry with the same requestId (ruling 9).
struct RequestRecord {
    int64_t   offset             = -1;
    EventKind kind               = EventKind::CreateTopic;
    uint64_t  objectId           = 0;
    uint32_t  newVersion         = 0;
    int32_t   topicId            = -1;   // the topic / view sink id the answer names (a drop: the retired one)
    uint16_t  schemaVersion      = 0;    // topic kinds with a schema: the resulting schema version; else 0
    uint64_t  requestFingerprint = 0;    // F14: a retry must match it; a different statement is InvalidRequest
};
struct ApplyError {
    /// UnsupportedFormat: no apply rules for e.formatVer.  Precondition: a rule of the
    /// table refused the event against this state.  FingerprintMismatch: the resulting
    /// object hashes differently from e.fingerprint.  BadOps: the event's own content is
    /// malformed (kind/ops disagree, an enum out of range, a blob that does not
    /// deserialize, an ALTER the evolution engine refuses).
    enum class Code : uint8_t { UnsupportedFormat, Precondition, FingerprintMismatch, BadOps };
    int64_t     offset   = -1;
    Code        code     = Code::Precondition;
    uint64_t    expected = 0;    // FingerprintMismatch: the event's fingerprint
    uint64_t    actual   = 0;    // FingerprintMismatch: the recomputed one
    std::string detail;
};

/// The largest segment roll size a topic may set: segment positions are 32-bit, less one
/// 64 MiB batch of headroom (the broker's rune::storage::Log::kMaxRollBytes, D19 L1).
inline constexpr int64_t kMaxSegmentRollBytes = int64_t{INT32_MAX} - (int64_t{64} << 20);

/// THE formatVer-1 TopicConfig rule (ONE validator: CreateTopic's config and the config an
/// AlterTopicConfig results in), exported so a DDL front end refuses exactly what apply
/// refuses.  nullopt = valid; otherwise "<key> <value> <rule>" (e.g. "retention.ms -2 < -1"):
///   retention.ms, retention.bytes, max.compaction.lag.ms >= -1 (-1 = unbounded / disabled);
///   delete.retention.ms, min.compaction.lag.ms >= 0; min.cleanable.dirty.ratio in
///   [0, 1'000'000] ppm; segment.bytes in [1, kMaxSegmentRollBytes]; retention.check.ms,
///   file.delete.delay.ms >= 1; cleanup.policy <= 2, durability <= 1, durability.tier <= 2.
[[nodiscard]] std::optional<std::string> topicConfigError(const TopicConfig& c);
/// AlterTopicConfig's resulting config: `o`'s changes written over `c` (the rule the apply runs; issue #296 previews
/// an ALTER's open-file need with it).  Validate the result with topicConfigError.
void applyConfigChanges(TopicConfig& c, const AlterTopicConfigOps& o) noexcept;

struct ApplyV1;   // src/catalog/ApplyV1.h — the frozen formatVer-1 rules

class CatalogState {
public:
    [[nodiscard]] const TopicObject* topicByName(std::string_view n) const;
    [[nodiscard]] const TopicObject* topicByObjectId(uint64_t id) const;
    [[nodiscard]] const TopicObject* topicByTopicId(int32_t topicId) const;
    [[nodiscard]] const ViewObject*  viewByName(std::string_view n) const;
    [[nodiscard]] const ViewObject*  viewByObjectId(uint64_t id) const;
    [[nodiscard]] const ViewObject*  viewByViewTopicId(int32_t topicId) const;
    /// A live topic or view owns this name.
    [[nodiscard]] bool nameTaken(std::string_view n) const { return topicByName(n) || viewByName(n); }
    /// A live topic or view name that differs from `n` only in ASCII letter case (two such
    /// names would share one directory on a case-insensitive filesystem); nullptr: none.
    [[nodiscard]] const std::string* liveNameFoldingOnto(std::string_view n) const;
    /// A live view whose sources include `topicName` (nullptr: none).
    [[nodiscard]] const ViewObject* viewReading(std::string_view topicName) const;
    /// Live objects in objectId order.
    [[nodiscard]] std::vector<const TopicObject*> topics() const;
    [[nodiscard]] std::vector<const ViewObject*>  views() const;
    /// Dropped objects, in apply order.
    [[nodiscard]] const std::vector<Tombstone>&  tombstones() const noexcept { return tombstones_; }

    [[nodiscard]] uint64_t nextObjectId() const noexcept { return nextObjectId_; }
    [[nodiscard]] int32_t  nextTopicId()  const noexcept { return nextTopicId_; }
    [[nodiscard]] int64_t  appliedOffset() const noexcept { return appliedOffset_; }   // -1 = nothing applied
    /// The applied event carrying `requestId` (ruling 9, F14) — what its DDL answered.  Cold.
    [[nodiscard]] std::optional<RequestRecord> request(const RequestId& requestId) const;
    /// FNV-1a-64 over exactly: appliedOffset, nextObjectId, nextTopicId, then each live
    /// object's fingerprint (CatalogFingerprint.h) in objectId order, then each tombstone's
    /// in apply order.  NOT hashed: which objectId / topicId / viewTopicId each object holds,
    /// a view's compiledPlanBlob (fingerprints are id-free, ruling 11) and the request
    /// records.  So equal digests at equal offsets mean equal definitions, counters and
    /// drop history — the replicas-agree check — not byte-identical CatalogState values.
    [[nodiscard]] uint64_t digest() const noexcept;

private:
    friend struct ApplyV1;   // src/catalog/ApplyV1.cpp — the only mutator (via apply())
    std::map<uint64_t, TopicObject>               topics_;        // objectId → object (ordered: digest order)
    std::map<uint64_t, ViewObject>                views_;
    std::map<std::string, uint64_t, std::less<>>  topicNames_;    // live topic name → objectId
    std::map<std::string, uint64_t, std::less<>>  viewNames_;     // live view name → objectId
    std::unordered_map<int32_t, uint64_t>         topicIds_;      // topicId → topic objectId
    std::unordered_map<int32_t, uint64_t>         viewTopicIds_;  // sink topicId → view objectId
    std::unordered_map<RequestId, RequestRecord, RequestIdHash> requests_;   // {0,0} never stored
    std::vector<Tombstone>                        tombstones_;
    uint64_t nextObjectId_  = 1;
    int32_t  nextTopicId_   = 0;
    int64_t  appliedOffset_ = -1;
};

/// Apply `e` (at log `offset`) under the rules of e.formatVer, verify its fingerprint,
/// and commit it.  Atomic: on any error the state is unchanged.  `offset` must be
/// greater than appliedOffset() (the state is a function of an ordered prefix).
[[nodiscard]] std::expected<Applied, ApplyError> apply(CatalogState& s, const CatalogEvent& e, int64_t offset);
/// The fingerprint `e` WOULD produce on `s` (the leader stamps it).  Never mutates `s`;
/// fails exactly where apply() would (except FingerprintMismatch and the offset order).
[[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const CatalogState& s, const CatalogEvent& e);
[[nodiscard]] std::string_view applyErrorName(ApplyError::Code c) noexcept;
/// F20: name-based ALTER ops → durable tags (the leader), and back (apply; `rune catalog replay`).
/// resolve: an ADD gets nextTag + (ADDs before it); a DROP / RENAME names the LIVE field's tag
/// (a later op sees an earlier RENAME / ADD / DROP in the same list).  An unknown name, a
/// name collision or tag exhaustion is an error naming it.
[[nodiscard]] std::expected<std::vector<ResolvedAlterOp>, std::string>
    resolveAlterOps(const SchemaState& schema, std::span<const rune::codec::AlterOp> ops);
/// The inverse on any state with the same definition: tag → the live field's current name.
/// An ADD's tag must equal the running nextTag; a tag that is not live, a non-canonical op
/// (a field set that its type does not use) or a FixedStr without a width is an error.
[[nodiscard]] std::expected<std::vector<rune::codec::AlterOp>, std::string>
    unresolveAlterOps(const SchemaState& schema, std::span<const ResolvedAlterOp> ops);

} // namespace rune::catalog
