// src/catalog/ApplyV1.cpp — the FROZEN formatVer-1 apply rules (spec §5.3, §6).
//
// One rule per kind; every rule refuses each invalid precondition explicitly, naming it
// (ApplyError::detail) — nothing is silently accepted, clamped or defaulted.  `run`
// builds a staged change without touching the state; `commit` installs it.  The state
// is therefore a pure function of the applied prefix, and apply is atomic.
//
//   CreateTopic       name valid (TopicName.h: 1..249 bytes of [A-Za-z0-9._-], not "." / "..",
//                     no "__" prefix), free, not case-folding onto a different live name;
//                     objectId == nextObjectId; ops.topicId == nextTopicId; versions 0 -> 1;
//                     partitions >= 1; replicationFactor >= 0; config (below);
//                     schema (if any): descriptor deserializes, nextTag past every tag,
//                     keyField (if any) a live field
//   DropTopic         live topic: objectId, name, topicId agree; versions chain; no live
//                     view reads it
//   AlterTopicConfig  live topic; versions chain; changes non-empty, no key twice, the ratio
//                     fits 32 bits; the RESULTING config passes topicConfigError
//   config            (CreateTopic's and AlterTopicConfig's result — ONE validator,
//                     topicConfigError): retention.ms, retention.bytes, max.compaction.lag.ms
//                     >= -1; delete.retention.ms, min.compaction.lag.ms >= 0;
//                     min.cleanable.dirty.ratio in [0, 1'000'000] ppm; segment.bytes in
//                     [1, kMaxSegmentRollBytes]; retention.check.ms, file.delete.delay.ms >= 1;
//                     cleanup.policy <= 2, durability <= 1, durability.tier <= 2
//   CreateSchema      live UNTYPED topic; versions chain; schema as for CreateTopic
//   AlterSchema       live typed topic; versions chain; schema version not exhausted; ops
//                     non-empty and not all SET COMPATIBILITY (that is SetCompatibility,
//                     ruling 3); unresolveAlterOps succeeds (F20); the compatibility gate
//                     (checkCompatibility under the effective mode) and applyOps succeed
//   SetCompatibility  live typed topic; versions chain; schema version not exhausted;
//                     applyOps({SET COMPATIBILITY mode}) succeeds (F9: today's bump)
//   CreateView        name as for CreateTopic; objectId == nextObjectId; viewTopicId ==
//                     nextTopicId; versions 0 -> 1; the definition (below)
//   AlterView         live view: objectId, name agree; versions chain; viewTopicId ==
//                     nextTopicId (a fresh sink, ruling 4); the definition
//   DropView          live view: objectId, name, viewTopicId agree; versions chain
//   view definition   sinkPartitions >= 1; sources non-empty, each a live TOPIC; sinkPartitions ==
//                     the FIRST source's partitions (#143: the sink mirrors it);
//                     normalizedPlanBlob non-empty; outputDescriptorBlob deserializes;
//                     keyField (if any) a live field of the output
//   every kind        a requestId other than {0,0} not already applied (the leader never
//                     writes one twice); the event's own values valid (the encode/decode
//                     validation: kind/ops agreement, enum ranges)
#include "ApplyV1.h"
#include "Wire.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <exception>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace rune::catalog {
namespace {

namespace cd = rune::codec;
using Code = ApplyError::Code;

constexpr uint16_t kLiveForever      = 0xFFFF;   // FieldDescriptor::untilVersion sentinel
constexpr uint16_t kMaxSchemaVersion = 0xFFFE;   // the next one must stay below the sentinel

std::unexpected<ApplyError> fail(int64_t offset, Code code, std::string detail) {
    return std::unexpected(ApplyError{offset, code, 0, 0, std::move(detail)});
}
/// Names in a detail are client-supplied: escaped so a rejected one cannot forge log lines.
std::string quoted(std::string_view s) { return "'" + escapeNameForLog(s) + "'"; }
std::string prefix(const CatalogEvent& e) { return std::string(kindName(e.kind)) + ": "; }

/// An existing object's version chain: prev == current, new == current + 1.
std::optional<std::string> chainError(const CatalogEvent& e, uint32_t current) {
    if (current == std::numeric_limits<uint32_t>::max())
        return prefix(e) + "object version " + std::to_string(current) + " is exhausted";
    if (e.prevVersion != current || e.newVersion != current + 1)
        return prefix(e) + "version chain " + std::to_string(e.prevVersion) + " -> " + std::to_string(e.newVersion) +
               ", object at " + std::to_string(current);
    return std::nullopt;
}

/// A new object: a free non-empty name, the next objectId, the next topic id, versions 0 -> 1.
std::optional<std::string> createError(const CatalogState& s, const CatalogEvent& e, int32_t topicId,
                                       std::string_view topicIdName) {
    if (const TopicNameError ne = checkTopicName(e.objectName); ne != TopicNameError::None)
        return prefix(e) + "name " + quoted(e.objectName) + " is invalid (" + std::string(topicNameErrorText(ne)) + ")";
    if (s.nameTaken(e.objectName)) return prefix(e) + "name " + quoted(e.objectName) + " is taken";
    if (const std::string* other = s.liveNameFoldingOnto(e.objectName))
        return prefix(e) + "name " + quoted(e.objectName) + " case-folds onto the live name " + quoted(*other);
    if (s.nextObjectId() == std::numeric_limits<uint64_t>::max()) return prefix(e) + "object ids are exhausted";
    if (e.objectId != s.nextObjectId())
        return prefix(e) + "objectId " + std::to_string(e.objectId) + ", next is " + std::to_string(s.nextObjectId());
    if (s.nextTopicId() == std::numeric_limits<int32_t>::max()) return prefix(e) + "topic ids are exhausted";
    if (topicId != s.nextTopicId())
        return prefix(e) + std::string(topicIdName) + " " + std::to_string(topicId) + ", next is " +
               std::to_string(s.nextTopicId());
    if (e.prevVersion != 0 || e.newVersion != 1)
        return prefix(e) + "version chain " + std::to_string(e.prevVersion) + " -> " + std::to_string(e.newVersion) +
               ", a new object is 0 -> 1";
    return std::nullopt;
}

bool hasLiveField(const cd::Descriptor& d, std::string_view name) {
    for (const auto& f : d.fields)
        if (f.untilVersion == kLiveForever && f.name == name) return true;
    return false;
}

/// Parse a blob that must be a descriptor; a throw is BadOps naming `what`.
std::expected<cd::Descriptor, ApplyError> parseDescriptor(const CatalogEvent& e, int64_t offset,
                                                          const std::vector<uint8_t>& blob, std::string_view what) {
    try {
        return cd::deserialize(std::span<const uint8_t>(blob.data(), blob.size()));
    } catch (const std::exception& x) {
        return fail(offset, Code::BadOps, prefix(e) + std::string(what) + " does not deserialize: " + x.what());
    }
}

/// A SchemaDef (CreateTopic / CreateSchema) → its initial state: schemaVersion 1, Backward.
std::expected<SchemaState, ApplyError> schemaFromDef(const CatalogEvent& e, int64_t offset, const SchemaDef& def) {
    if (def.descriptorBlob.empty())
        return fail(offset, Code::Precondition, prefix(e) + "schema with an empty descriptor (an untyped topic has none)");
    auto d = parseDescriptor(e, offset, def.descriptorBlob, "the schema descriptor");
    if (!d) return std::unexpected(std::move(d.error()));
    uint32_t pastEveryTag = 0;
    for (const auto& f : d->fields) pastEveryTag = std::max<uint32_t>(pastEveryTag, uint32_t{f.id} + 1);
    if (def.nextTag < pastEveryTag)
        return fail(offset, Code::Precondition, prefix(e) + "nextTag " + std::to_string(def.nextTag) +
                                                    " does not exceed every field tag (needs >= " +
                                                    std::to_string(pastEveryTag) + ")");
    if (!def.keyField.empty() && !hasLiveField(*d, def.keyField))
        return fail(offset, Code::Precondition, prefix(e) + "key field " + quoted(def.keyField) + " is not a live field");
    return SchemaState{.keyField = def.keyField, .validate = def.validate, .evolvingBlob = def.descriptorBlob,
                       .schemaVersion = 1, .nextTag = def.nextTag, .compat = cd::CompatMode::Backward};
}

/// The live topic `e` names: objectId and name agree.
std::expected<const TopicObject*, ApplyError> liveTopic(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    const TopicObject* t = s.topicByObjectId(e.objectId);
    if (t == nullptr)
        return fail(offset, Code::Precondition, prefix(e) + "no live topic with objectId " + std::to_string(e.objectId));
    if (t->name != e.objectName)
        return fail(offset, Code::Precondition, prefix(e) + "objectId " + std::to_string(e.objectId) + " is topic " +
                                                    quoted(t->name) + ", not " + quoted(e.objectName));
    if (auto c = chainError(e, t->version)) return fail(offset, Code::Precondition, std::move(*c));
    return t;
}
std::expected<const ViewObject*, ApplyError> liveView(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    const ViewObject* v = s.viewByObjectId(e.objectId);
    if (v == nullptr)
        return fail(offset, Code::Precondition, prefix(e) + "no live view with objectId " + std::to_string(e.objectId));
    if (v->name != e.objectName)
        return fail(offset, Code::Precondition, prefix(e) + "objectId " + std::to_string(e.objectId) + " is view " +
                                                    quoted(v->name) + ", not " + quoted(e.objectName));
    if (auto c = chainError(e, v->version)) return fail(offset, Code::Precondition, std::move(*c));
    return v;
}

/// A typed topic's next schema version stays below the untilVersion sentinel.
std::optional<ApplyError> schemaVersionError(const CatalogEvent& e, int64_t offset, const TopicObject& t) {
    if (!t.schema)
        return ApplyError{offset, Code::Precondition, 0, 0, prefix(e) + "topic " + quoted(t.name) + " is untyped"};
    if (t.schema->schemaVersion >= kMaxSchemaVersion)
        return ApplyError{offset, Code::Precondition, 0, 0,
                          prefix(e) + "schema version " + std::to_string(t.schema->schemaVersion) + " is exhausted"};
    return std::nullopt;
}

/// applyOps on the evolving descriptor (the engine today's ALTER runs), at schemaVersion + 1.
/// Any EvolveError or throw is BadOps; the result must read back (it is the next evolving blob).
std::expected<std::vector<uint8_t>, ApplyError> evolve(const CatalogEvent& e, int64_t offset, const SchemaState& schema,
                                                       const std::vector<cd::AlterOp>& ops, uint16_t& nextTag) {
    try {
        const cd::Descriptor cur = cd::deserialize(std::span<const uint8_t>(schema.evolvingBlob.data(),
                                                                            schema.evolvingBlob.size()));
        uint16_t tag = nextTag;
        auto next = cd::applyOps(cur, ops, tag, static_cast<uint16_t>(schema.schemaVersion + 1));
        if (!next) return fail(offset, Code::BadOps, prefix(e) + next.error().message);
        std::vector<uint8_t> blob = cd::serialize(*next);
        (void)cd::deserialize(std::span<const uint8_t>(blob.data(), blob.size()));   // reads back, or throws
        nextTag = tag;
        return blob;
    } catch (const std::exception& x) {
        return fail(offset, Code::BadOps, prefix(e) + "schema evolution failed: " + x.what());
    }
}

std::optional<std::string> viewDefError(const CatalogState& s, const CatalogEvent& e, int64_t offset,
                                        const ViewDef& v, std::optional<ApplyError>& badOps) {
    if (v.sinkPartitions < 1) return prefix(e) + "sinkPartitions " + std::to_string(v.sinkPartitions) + " < 1";
    if (v.sourceNames.empty()) return prefix(e) + "a view reads at least one source";
    for (const auto& src : v.sourceNames)
        if (s.topicByName(src) == nullptr) return prefix(e) + "source " + quoted(src) + " is not a live topic";
    // #143: the sink is co-partitioned with its FIRST source (AdminWorker writes exactly that, and partition counts
    // are immutable): a mismatch is a crafted / foreign catalog, refused here so every applier halts on it.
    if (const TopicObject* first = s.topicByName(v.sourceNames.front()); v.sinkPartitions != first->partitions)
        return prefix(e) + "view " + quoted(e.objectName) + " sink partitions " + std::to_string(v.sinkPartitions) +
               " but its first source " + quoted(first->name) + " has " + std::to_string(first->partitions);
    if (v.normalizedPlanBlob.empty()) return prefix(e) + "the normalized plan is empty";
    auto out = parseDescriptor(e, offset, v.outputDescriptorBlob, "the output descriptor");
    if (!out) { badOps = std::move(out.error()); return std::nullopt; }
    if (!v.keyField.empty() && !hasLiveField(*out, v.keyField))
        return prefix(e) + "key field " + quoted(v.keyField) + " is not a field of the output";
    return std::nullopt;
}
std::optional<ApplyError> checkViewDef(const CatalogState& s, const CatalogEvent& e, int64_t offset, const ViewDef& v) {
    std::optional<ApplyError> badOps;
    if (auto d = viewDefError(s, e, offset, v, badOps)) return ApplyError{offset, Code::Precondition, 0, 0, std::move(*d)};
    return badOps;
}

/// AlterTopicConfig's own shape: at least one change, no key twice, and a ratio that fits its
/// i32 field (checked before the narrowing store, so no value wraps into range).  The
/// RANGES are topicConfigError's, run over the resulting config.
std::optional<std::string> configChangesError(const CatalogEvent& e, const AlterTopicConfigOps& o) {
    if (o.changes.empty()) return prefix(e) + "no changes";
    uint32_t seen = 0;
    for (const ConfigChange& c : o.changes) {
        const auto k = static_cast<uint8_t>(c.key);
        if (seen & (1u << k)) return prefix(e) + "key " + std::to_string(k) + " set twice";
        seen |= 1u << k;
        if (c.key == ConfigKey::MinCleanableDirtyRatioPpm &&
            (c.value < std::numeric_limits<int32_t>::min() || c.value > std::numeric_limits<int32_t>::max()))
            return prefix(e) + "min.cleanable.dirty.ratio " + std::to_string(c.value) + " does not fit 32 bits";
    }
    return std::nullopt;
}
}  // namespace

// Public (CatalogState.h, issue #296): the broker previews an ALTER's resulting config with the same rule.
void applyConfigChanges(TopicConfig& c, const AlterTopicConfigOps& o) noexcept {
    for (const ConfigChange& ch : o.changes) {
        switch (ch.key) {
        case ConfigKey::RetentionMs:               c.retentionMs = ch.value; break;
        case ConfigKey::RetentionBytes:            c.retentionBytes = ch.value; break;
        case ConfigKey::DeleteRetentionMs:         c.deleteRetentionMs = ch.value; break;
        case ConfigKey::MinCleanableDirtyRatioPpm: c.minCleanableDirtyRatioPpm = static_cast<int32_t>(ch.value); break;
        case ConfigKey::MinCompactionLagMs:        c.minCompactionLagMs = ch.value; break;
        case ConfigKey::MaxCompactionLagMs:        c.maxCompactionLagMs = ch.value; break;
        }
    }
}

namespace {

// ── one rule per kind ─────────────────────────────────────────────────────────
using Result = std::expected<Staged, ApplyError>;

Result createTopic(const CatalogState& s, const CatalogEvent& e, int64_t off, const CreateTopicOps& o) {
    if (auto c = createError(s, e, o.topicId, "topicId")) return fail(off, Code::Precondition, std::move(*c));
    if (o.partitions < 1) return fail(off, Code::Precondition, prefix(e) + "partitions " + std::to_string(o.partitions) + " < 1");
    if (o.replicationFactor < 0)
        return fail(off, Code::Precondition, prefix(e) + "replicationFactor " + std::to_string(o.replicationFactor) + " < 0");
    if (auto c = topicConfigError(o.config)) return fail(off, Code::Precondition, prefix(e) + "config " + *c);
    TopicObject t{.objectId = e.objectId, .name = e.objectName, .version = 1, .topicId = o.topicId,
                  .partitions = o.partitions, .replicationFactor = o.replicationFactor,
                  .persistence = o.persistence, .config = o.config, .schema = std::nullopt};
    if (o.schema) {
        auto sch = schemaFromDef(e, off, *o.schema);
        if (!sch) return std::unexpected(std::move(sch.error()));
        t.schema = std::move(*sch);
    }
    return StagedCreate{std::move(t)};
}

Result dropTopic(const CatalogState& s, const CatalogEvent& e, int64_t off, const DropTopicOps& o) {
    auto t = liveTopic(s, e, off);
    if (!t) return std::unexpected(std::move(t.error()));
    if (o.topicId != (*t)->topicId)
        return fail(off, Code::Precondition, prefix(e) + "topicId " + std::to_string(o.topicId) + ", topic " +
                                                 quoted(e.objectName) + " is " + std::to_string((*t)->topicId));
    if (const ViewObject* v = s.viewReading(e.objectName))
        return fail(off, Code::Precondition, prefix(e) + "topic " + quoted(e.objectName) + " is read by view " + quoted(v->name));
    return StagedRemove{e.objectId, Tombstone{e.objectId, e.objectName, e.newVersion, /*wasView=*/false}, (*t)->topicId};
}

Result alterTopicConfig(const CatalogState& s, const CatalogEvent& e, int64_t off, const AlterTopicConfigOps& o) {
    auto t = liveTopic(s, e, off);
    if (!t) return std::unexpected(std::move(t.error()));
    if (auto c = configChangesError(e, o)) return fail(off, Code::Precondition, std::move(*c));
    TopicObject next = **t;
    applyConfigChanges(next.config, o);
    if (auto c = topicConfigError(next.config)) return fail(off, Code::Precondition, prefix(e) + "config " + *c);
    next.version = e.newVersion;
    return StagedUpdate{std::move(next)};
}

Result createSchema(const CatalogState& s, const CatalogEvent& e, int64_t off, const CreateSchemaOps& o) {
    auto t = liveTopic(s, e, off);
    if (!t) return std::unexpected(std::move(t.error()));
    if ((*t)->schema) return fail(off, Code::Precondition, prefix(e) + "topic " + quoted(e.objectName) + " already has a schema");
    auto sch = schemaFromDef(e, off, o.schema);
    if (!sch) return std::unexpected(std::move(sch.error()));
    TopicObject next = **t;
    next.schema = std::move(*sch);
    next.version = e.newVersion;
    return StagedUpdate{std::move(next)};
}

Result alterSchema(const CatalogState& s, const CatalogEvent& e, int64_t off, const AlterSchemaOps& o) {
    auto t = liveTopic(s, e, off);
    if (!t) return std::unexpected(std::move(t.error()));
    if (auto err = schemaVersionError(e, off, **t)) return std::unexpected(std::move(*err));
    if (o.ops.empty()) return fail(off, Code::Precondition, prefix(e) + "no operations");
    bool onlySetCompat = true;
    for (const auto& op : o.ops) onlySetCompat = onlySetCompat && op.type == cd::AlterOpType::SetCompatibility;
    if (onlySetCompat)
        return fail(off, Code::Precondition, prefix(e) + "every operation is SET COMPATIBILITY (a SetCompatibility event)");

    const SchemaState& cur = *(*t)->schema;
    auto ops = unresolveAlterOps(cur, std::span<const ResolvedAlterOp>(o.ops));   // F20: against THIS state
    if (!ops) return fail(off, Code::Precondition, prefix(e) + ops.error());

    // Today's ALTER (AdminWorker::startAlter_ d/e): a SET COMPATIBILITY inside sets both the
    // mode the gate checks and the mode the new state carries.
    cd::CompatMode mode = cur.compat;
    for (const auto& op : *ops)
        if (op.type == cd::AlterOpType::SetCompatibility) mode = op.mode;
    if (auto gate = cd::checkCompatibility(*ops, mode); !gate)
        return fail(off, Code::BadOps, prefix(e) + gate.error().message);

    uint16_t nextTag = cur.nextTag;
    auto blob = evolve(e, off, cur, *ops, nextTag);
    if (!blob) return std::unexpected(std::move(blob.error()));

    // DDL conformance B8 / D3a: a RENAME of the key field moves keyField with it (in order,
    // so a chain id -> a, a -> b ends at b).
    std::string keyField = cur.keyField;
    for (const auto& op : *ops)
        if (op.type == cd::AlterOpType::Rename && !keyField.empty() && op.name == keyField) keyField = op.newName;

    TopicObject next = **t;
    next.schema = SchemaState{.keyField = std::move(keyField), .validate = cur.validate, .evolvingBlob = std::move(*blob),
                              .schemaVersion = static_cast<uint16_t>(cur.schemaVersion + 1), .nextTag = nextTag,
                              .compat = mode};
    next.version = e.newVersion;
    return StagedUpdate{std::move(next)};
}

Result setCompatibility(const CatalogState& s, const CatalogEvent& e, int64_t off, const SetCompatibilityOps& o) {
    auto t = liveTopic(s, e, off);
    if (!t) return std::unexpected(std::move(t.error()));
    if (auto err = schemaVersionError(e, off, **t)) return std::unexpected(std::move(*err));
    const SchemaState& cur = *(*t)->schema;
    cd::AlterOp op{};
    op.type = cd::AlterOpType::SetCompatibility;
    op.mode = o.mode;
    uint16_t nextTag = cur.nextTag;
    auto blob = evolve(e, off, cur, std::vector<cd::AlterOp>{op}, nextTag);   // F9: today's version bump
    if (!blob) return std::unexpected(std::move(blob.error()));
    TopicObject next = **t;
    next.schema->evolvingBlob = std::move(*blob);
    next.schema->schemaVersion = static_cast<uint16_t>(cur.schemaVersion + 1);
    next.schema->nextTag = nextTag;
    next.schema->compat = o.mode;
    next.version = e.newVersion;
    return StagedUpdate{std::move(next)};
}

Result createView(const CatalogState& s, const CatalogEvent& e, int64_t off, const CreateViewOps& o) {
    if (auto c = createError(s, e, o.view.viewTopicId, "viewTopicId")) return fail(off, Code::Precondition, std::move(*c));
    if (auto err = checkViewDef(s, e, off, o.view)) return std::unexpected(std::move(*err));
    return StagedCreate{ViewObject{.objectId = e.objectId, .name = e.objectName, .version = 1, .def = o.view}};
}

Result alterView(const CatalogState& s, const CatalogEvent& e, int64_t off, const AlterViewOps& o) {
    auto v = liveView(s, e, off);
    if (!v) return std::unexpected(std::move(v.error()));
    if (s.nextTopicId() == std::numeric_limits<int32_t>::max())
        return fail(off, Code::Precondition, prefix(e) + "topic ids are exhausted");
    if (o.view.viewTopicId != s.nextTopicId())
        return fail(off, Code::Precondition, prefix(e) + "viewTopicId " + std::to_string(o.view.viewTopicId) +
                                                 " is not a fresh sink, next is " + std::to_string(s.nextTopicId()));
    if (auto err = checkViewDef(s, e, off, o.view)) return std::unexpected(std::move(*err));
    ViewObject next = **v;
    next.def = o.view;
    next.version = e.newVersion;
    return StagedUpdate{std::move(next), (*v)->def.viewTopicId};
}

Result dropView(const CatalogState& s, const CatalogEvent& e, int64_t off, const DropViewOps& o) {
    auto v = liveView(s, e, off);
    if (!v) return std::unexpected(std::move(v.error()));
    if (o.viewTopicId != (*v)->def.viewTopicId)
        return fail(off, Code::Precondition, prefix(e) + "viewTopicId " + std::to_string(o.viewTopicId) + ", view " +
                                                 quoted(e.objectName) + " sinks to " + std::to_string((*v)->def.viewTopicId));
    return StagedRemove{e.objectId, Tombstone{e.objectId, e.objectName, e.newVersion, /*wasView=*/true},
                        (*v)->def.viewTopicId};
}

int32_t topicIdOf(const StagedObject& o) noexcept {
    if (const auto* t = std::get_if<TopicObject>(&o)) return t->topicId;
    return std::get<ViewObject>(o).def.viewTopicId;
}
uint16_t schemaVersionOf(const StagedObject& o) noexcept {
    const auto* t = std::get_if<TopicObject>(&o);
    return (t != nullptr && t->schema) ? t->schema->schemaVersion : uint16_t{0};
}

} // namespace

std::optional<std::string> topicConfigError(const TopicConfig& c) {
    auto atLeast = [](std::string_view key, int64_t v, int64_t min) -> std::optional<std::string> {
        if (v < min) return std::string(key) + " " + std::to_string(v) + " < " + std::to_string(min);
        return std::nullopt;
    };
    auto byte = [](std::string_view key, uint8_t v, uint8_t max) -> std::optional<std::string> {
        if (v > max) return std::string(key) + " " + std::to_string(v) + " > " + std::to_string(max);
        return std::nullopt;
    };
    if (auto r = byte("cleanup.policy", c.cleanupPolicy, wire::kMaxCleanupPolicy)) return r;
    if (auto r = atLeast("retention.ms", c.retentionMs, -1)) return r;
    if (auto r = atLeast("retention.bytes", c.retentionBytes, -1)) return r;
    if (auto r = atLeast("delete.retention.ms", c.deleteRetentionMs, 0)) return r;
    if (c.minCleanableDirtyRatioPpm < 0 || c.minCleanableDirtyRatioPpm > 1'000'000)
        return "min.cleanable.dirty.ratio " + std::to_string(c.minCleanableDirtyRatioPpm) + " outside [0, 1000000] ppm";
    if (auto r = atLeast("segment.bytes", c.segmentRollBytes, 1)) return r;
    if (c.segmentRollBytes > kMaxSegmentRollBytes)
        return "segment.bytes " + std::to_string(c.segmentRollBytes) + " > " + std::to_string(kMaxSegmentRollBytes);
    if (auto r = atLeast("min.compaction.lag.ms", c.minCompactionLagMs, 0)) return r;
    if (auto r = atLeast("max.compaction.lag.ms", c.maxCompactionLagMs, -1)) return r;
    if (auto r = atLeast("retention.check.ms", c.retentionCheckMs, 1)) return r;
    if (auto r = atLeast("file.delete.delay.ms", c.fileDeleteDelayMs, 1)) return r;
    if (auto r = byte("durability", c.durability, wire::kMaxDurability)) return r;
    if (auto r = byte("durability.tier", c.durabilityTier, wire::kMaxDurabilityTier)) return r;
    return std::nullopt;   // coPartitionGroup, partitionKey: free-form (bounded by the wire's str16)
}

std::expected<Staged, ApplyError> ApplyV1::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (auto v = wire::validateEvent(e))
        return fail(offset, Code::BadOps, prefix(e) + "invalid event: " + std::string(encodeErrorName(*v)));
    if (!e.principal.requestId.none() && s.request(e.principal.requestId))
        return fail(offset, Code::Precondition, prefix(e) + "requestId " + std::to_string(e.principal.requestId.hi) + ":" +
                                                    std::to_string(e.principal.requestId.lo) + " is already applied");
    return std::visit([&](const auto& o) -> Result {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, CreateTopicOps>)           return createTopic(s, e, offset, o);
        else if constexpr (std::is_same_v<T, DropTopicOps>)        return dropTopic(s, e, offset, o);
        else if constexpr (std::is_same_v<T, AlterTopicConfigOps>) return alterTopicConfig(s, e, offset, o);
        else if constexpr (std::is_same_v<T, CreateSchemaOps>)     return createSchema(s, e, offset, o);
        else if constexpr (std::is_same_v<T, AlterSchemaOps>)      return alterSchema(s, e, offset, o);
        else if constexpr (std::is_same_v<T, SetCompatibilityOps>) return setCompatibility(s, e, offset, o);
        else if constexpr (std::is_same_v<T, CreateViewOps>)       return createView(s, e, offset, o);
        else if constexpr (std::is_same_v<T, AlterViewOps>)        return alterView(s, e, offset, o);
        else if constexpr (std::is_same_v<T, DropViewOps>)         return dropView(s, e, offset, o);
        else   // a namespace kind (formatVer 2): not a rule of this table; ApplyV2 owns them
            return fail(offset, Code::Precondition, prefix(e) + "namespace kinds have no formatVer-1 rule");
    }, e.ops);
}

uint64_t ApplyV1::fingerprintOfStaged(const Staged& st) noexcept {
    return std::visit([](const auto& x) -> uint64_t {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, StagedRemove>) return fingerprintOf(x.tomb);
        else return std::visit([](const auto& obj) { return fingerprintOf(obj); }, x.object);
    }, st);
}

Applied ApplyV1::commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, Staged&& st) {
    Applied a{.kind = e.kind, .objectId = e.objectId, .newVersion = e.newVersion, .fingerprint = fingerprint,
              .createdTopicId = -1, .droppedTopicId = -1};
    RequestRecord r{.offset = offset, .kind = e.kind, .objectId = e.objectId, .newVersion = e.newVersion,
                    .topicId = -1, .schemaVersion = 0, .requestFingerprint = e.principal.requestFingerprint};

    std::visit([&](auto&& x) {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, StagedCreate>) {
            a.createdTopicId = topicIdOf(x.object);
            r.topicId = a.createdTopicId;
            r.schemaVersion = schemaVersionOf(x.object);
            if (auto* t = std::get_if<TopicObject>(&x.object)) {
                s.topicNames_.emplace(t->name, t->objectId);
                s.topicIds_.emplace(t->topicId, t->objectId);
                s.topics_.emplace(t->objectId, std::move(*t));
            } else {
                auto& v = std::get<ViewObject>(x.object);
                s.viewNames_.emplace(v.name, v.objectId);
                s.viewTopicIds_.emplace(v.def.viewTopicId, v.objectId);
                s.views_.emplace(v.objectId, std::move(v));
            }
            ++s.nextObjectId_;
            ++s.nextTopicId_;
        } else if constexpr (std::is_same_v<T, StagedUpdate>) {
            r.topicId = topicIdOf(x.object);
            r.schemaVersion = schemaVersionOf(x.object);
            if (auto* t = std::get_if<TopicObject>(&x.object)) {
                s.topics_.at(t->objectId) = std::move(*t);   // same name, same topicId: indexes unchanged
            } else {
                auto& v = std::get<ViewObject>(x.object);
                if (x.retiredSinkTopicId >= 0) {   // AlterView: a fresh sink replaces the old one (ruling 4)
                    a.droppedTopicId = x.retiredSinkTopicId;
                    a.createdTopicId = v.def.viewTopicId;
                    s.viewTopicIds_.erase(x.retiredSinkTopicId);
                    s.viewTopicIds_.emplace(v.def.viewTopicId, v.objectId);
                    ++s.nextTopicId_;
                }
                s.views_.at(v.objectId) = std::move(v);
            }
        } else {   // StagedRemove
            a.droppedTopicId = x.droppedTopicId;
            r.topicId = x.droppedTopicId;
            if (x.tomb.wasView) {
                s.viewNames_.erase(x.tomb.name);
                s.viewTopicIds_.erase(x.droppedTopicId);
                s.views_.erase(x.objectId);
            } else {
                s.topicNames_.erase(x.tomb.name);
                s.topicIds_.erase(x.droppedTopicId);
                s.topics_.erase(x.objectId);
            }
            s.tombstones_.push_back(std::move(x.tomb));
        }
    }, std::move(st));

    s.appliedOffset_ = offset;
    if (!e.principal.requestId.none()) s.requests_.emplace(e.principal.requestId, r);
    return a;
}

} // namespace rune::catalog
