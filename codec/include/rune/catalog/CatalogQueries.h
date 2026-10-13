#pragma once
// rune/catalog/CatalogQueries.h — history / as-of / diff over a recorded __catalog log
// (spec §7: SHOW HISTORY, AS OF, DIFF).  Pure functions of the log; every state they
// produce is replayed through the verifying CatalogReplayer, so a query never shows a
// state the log does not verifiably produce.  Cold path (CLI / client queries).
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>

#include <climits>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::catalog {

/// A schema is an aspect of its topic object (ruling 2): Schema selects the schema kinds.
enum class ObjectType : uint8_t { Topic, View, Schema };
[[nodiscard]] std::string_view objectTypeName(ObjectType t) noexcept;   // "topic", "view", "schema"

/// type: Topic = CreateTopic, DropTopic, AlterTopicConfig and the schema kinds of a topic;
///       Schema = CreateSchema, AlterSchema, SetCompatibility and a CreateTopic that carried a
///       schema; View = CreateView, AlterView, DropView; nullopt = every kind.
/// name: matches objectName — every object EVER named so (a drop-and-reuse shows both,
///       told apart by objectId); empty = every name.
/// sinceMs: timestampMs >= sinceMs.
struct HistoryFilter {
    std::optional<ObjectType> type;
    std::string               name;
    int64_t                   sinceMs = INT64_MIN;
};
/// The matching events, in log (offset) order; pointers into `log`.
[[nodiscard]] std::vector<const RecordedEvent*> history(std::span<const RecordedEvent> log, const HistoryFilter& f);

/// Offset o: the state after every event at offset <= o.
/// TimeMs t: events with timestampMs <= t, in offset order, STOPPING at the first later one
///           (time is informational; the offset is the order).
/// Version n: up to and including the event that produced version n of the LATEST object
///           named `name` of `type` — object version for Topic / View; schema version for
///           Schema, where the latest object is the latest topic named `name` that carried a
///           schema.  Not found → ReplayError{Code::Apply, detail "no version n of <type> '<name>'"}.
struct AsOf {
    enum class By : uint8_t { Offset, TimeMs, Version };
    By          by    = By::Offset;
    int64_t     value = 0;
    ObjectType  type  = ObjectType::Topic;   // Version only
    std::string name;                        // Version only
};
/// Replays `log` (which must start at offset 0, gap-free) through a fresh CatalogReplayer
/// up to the as-of point — so an as-of point past a bad event surfaces that event's error.
[[nodiscard]] std::expected<CatalogState, ReplayError> stateAsOf(std::span<const RecordedEvent> log, const AsOf& at);

/// One object that differs between two states, paired by (type, name) — type Topic or View.
/// Added: in b only (fingerprintA = 0).  Removed: in a only (fingerprintB = 0).
/// Changed: both, fingerprints differ; `fields` names every differing definition field.
struct DiffEntry {
    enum class Change : uint8_t { Added, Removed, Changed };
    Change                   change = Change::Changed;
    ObjectType               type   = ObjectType::Topic;
    std::string              name;
    uint64_t                 fingerprintA = 0;
    uint64_t                 fingerprintB = 0;
    std::vector<std::string> fields;
};
/// Ordered by (type, name).  Empty when every live definition is equal (ids never count,
/// ruling 11: a dropped-and-recreated identical object is no change).
[[nodiscard]] std::vector<DiffEntry> diff(const CatalogState& a, const CatalogState& b);

/// The fingerprinted fields that differ, by diff name, in fingerprint order (CatalogFingerprint.h).
/// Topic: version, partitions, replicationFactor, persistence, config.<field> (TopicConfig member
/// names), schema (present vs absent), schema.keyField, schema.validate, schema.fields (the live
/// field set or nextTag), schema.version, schema.compat, schema.descriptor (any other change of
/// the evolving descriptor).  View: version, view.sinkPartitions, view.persistence,
/// view.keyField, view.sources, view.plan, view.output.  Ids are never fields.
[[nodiscard]] std::vector<std::string> changedFields(const TopicObject& a, const TopicObject& b);
[[nodiscard]] std::vector<std::string> changedFields(const ViewObject& a, const ViewObject& b);

} // namespace rune::catalog
