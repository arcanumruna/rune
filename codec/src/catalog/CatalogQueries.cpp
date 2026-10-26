// src/catalog/CatalogQueries.cpp — see CatalogQueries.h.
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <exception>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <utility>

namespace rune::catalog {

namespace {
namespace cd = rune::codec;

bool isTopicKind(EventKind k) noexcept {
    switch (k) {
    case EventKind::CreateTopic: case EventKind::DropTopic: case EventKind::AlterTopicConfig:
    case EventKind::CreateSchema: case EventKind::AlterSchema: case EventKind::SetCompatibility:
        return true;
    default:
        return false;
    }
}
bool isViewKind(EventKind k) noexcept {
    return k == EventKind::CreateView || k == EventKind::AlterView || k == EventKind::DropView;
}
/// A schema kind, or a CreateTopic that carried a schema.
bool isSchemaEvent(const CatalogEvent& e) noexcept {
    switch (e.kind) {
    case EventKind::CreateSchema: case EventKind::AlterSchema: case EventKind::SetCompatibility:
        return true;
    case EventKind::CreateTopic: {
        const auto* o = std::get_if<CreateTopicOps>(&e.ops);
        return o != nullptr && o->schema.has_value();
    }
    default:
        return false;
    }
}
bool matchesType(const CatalogEvent& e, ObjectType t) noexcept {
    switch (t) {
    case ObjectType::Topic:  return isTopicKind(e.kind);
    case ObjectType::View:   return isViewKind(e.kind);
    case ObjectType::Schema: return isSchemaEvent(e);
    }
    return false;
}

ReplayError noVersion(const AsOf& at) {
    return ReplayError{.offset = -1, .code = ReplayError::Code::Apply, .decode = std::nullopt, .apply = std::nullopt,
                       .detail = "no version " + std::to_string(at.value) + " of " +
                                 std::string(objectTypeName(at.type)) + " '" + escapeNameForLog(at.name) + "'"};
}

/// Replay `log` in order until `stopBefore(rec)` (that event is not applied) or
/// `stopAfter(rec, state)` (that event is the last applied) holds, or the log ends.
/// `reached` reports whether stopAfter fired.
template <class StopBefore, class StopAfter>
std::expected<CatalogState, ReplayError> replayUntil(std::span<const RecordedEvent> log, StopBefore stopBefore,
                                                     StopAfter stopAfter, bool& reached) {
    reached = false;
    CatalogReplayer r;
    for (const RecordedEvent& rec : log) {
        if (stopBefore(rec)) break;
        if (auto a = r.feedDecoded(rec.offset, rec.event); !a) return std::unexpected(std::move(a.error()));
        if (stopAfter(rec, r.state())) { reached = true; break; }
    }
    return r.state();
}

/// The objectId of the LATEST object named at.name of at.type (Topic / View: its create;
/// Schema: the topic of the last schema event under that name).  nullopt: none.
std::optional<uint64_t> latestObject(std::span<const RecordedEvent> log, const AsOf& at) {
    std::optional<uint64_t> id;
    for (const RecordedEvent& rec : log) {
        const CatalogEvent& e = rec.event;
        if (e.objectName != at.name || e.nsId != at.nsId) continue;
        const bool hit = at.type == ObjectType::Topic  ? e.kind == EventKind::CreateTopic
                       : at.type == ObjectType::View   ? e.kind == EventKind::CreateView
                                                       : isSchemaEvent(e);
        if (hit) id = e.objectId;
    }
    return id;
}

std::expected<CatalogState, ReplayError> asOfVersion(std::span<const RecordedEvent> log, const AsOf& at) {
    const auto target = latestObject(log, at);
    if (!target) return std::unexpected(noVersion(at));
    bool reached = false;
    std::expected<CatalogState, ReplayError> s;
    if (at.type == ObjectType::Schema) {
        // The schema version is part of the RESULTING state (ruling 2): read it after each
        // schema event of the target, as the v1 rules produced it.
        if (at.value < 1 || at.value > std::numeric_limits<uint16_t>::max()) return std::unexpected(noVersion(at));
        s = replayUntil(
            log, [](const RecordedEvent&) { return false; },
            [&](const RecordedEvent& rec, const CatalogState& st) {
                if (rec.event.objectId != *target || !isSchemaEvent(rec.event)) return false;
                const TopicObject* t = st.topicByObjectId(*target);
                return t != nullptr && t->schema && t->schema->schemaVersion == at.value;
            },
            reached);
    } else {
        if (at.value < 1 || at.value > std::numeric_limits<uint32_t>::max()) return std::unexpected(noVersion(at));
        const bool topic = at.type == ObjectType::Topic;
        s = replayUntil(
            log, [](const RecordedEvent&) { return false; },
            [&](const RecordedEvent& rec, const CatalogState&) {
                return rec.event.objectId == *target && rec.event.newVersion == at.value &&
                       (topic ? isTopicKind(rec.event.kind) : isViewKind(rec.event.kind));
            },
            reached);
    }
    if (!s) return s;
    if (!reached) return std::unexpected(noVersion(at));
    return s;
}

// ── field-level diff ──────────────────────────────────────────────────────────
bool sameField(const cd::FieldDescriptor& a, const cd::FieldDescriptor& b) noexcept {
    return a.name == b.name && a.id == b.id && a.type == b.type && a.sinceVersion == b.sinceVersion &&
           a.untilVersion == b.untilVersion && a.isKey == b.isKey && a.fixedLen == b.fixedLen &&
           a.enumValues == b.enumValues;
}
bool sameFields(const cd::Descriptor& a, const cd::Descriptor& b) noexcept {
    if (a.fields.size() != b.fields.size()) return false;
    for (std::size_t i = 0; i < a.fields.size(); ++i)   // declaration order is part of the layout
        if (!sameField(a.fields[i], b.fields[i])) return false;
    return true;
}
std::optional<cd::Descriptor> parse(const std::vector<uint8_t>& blob) {
    try {
        return cd::deserialize(std::span<const uint8_t>(blob.data(), blob.size()));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

void schemaFields(const SchemaState& a, const SchemaState& b, std::vector<std::string>& out) {
    if (a.keyField != b.keyField) out.emplace_back("schema.keyField");
    if (a.validate != b.validate) out.emplace_back("schema.validate");
    bool fieldsDiffer = a.nextTag != b.nextTag;
    bool descriptorOther = false;
    if (a.evolvingBlob != b.evolvingBlob) {
        const auto da = parse(a.evolvingBlob);
        const auto db = parse(b.evolvingBlob);
        if (!da || !db) {
            descriptorOther = true;
        } else {
            const bool fd = !sameFields(*da, *db);
            const bool vd = da->version != db->version;
            fieldsDiffer = fieldsDiffer || fd;
            // The evolving descriptor's version follows the schema version; any other
            // difference (name, a version moving alone, bytes that change nothing above)
            // is reported as the descriptor itself, so no change goes unnamed.
            descriptorOther = da->name != db->name || (vd && a.schemaVersion == b.schemaVersion) || (!fd && !vd);
        }
    }
    if (fieldsDiffer) out.emplace_back("schema.fields");
    if (a.schemaVersion != b.schemaVersion) out.emplace_back("schema.version");
    if (a.compat != b.compat) out.emplace_back("schema.compat");
    if (descriptorOther) out.emplace_back("schema.descriptor");
}

/// The view output as the fingerprint sees it: the descriptor definition, its version and
/// its field declaration order (CatalogFingerprint.h).
bool sameOutput(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    if (a == b) return true;
    const auto da = parse(a);
    const auto db = parse(b);
    if (!da || !db) return false;
    if (cd::fingerprint(*da) != cd::fingerprint(*db) || da->version != db->version ||
        da->fields.size() != db->fields.size())
        return false;
    for (std::size_t i = 0; i < da->fields.size(); ++i)
        if (da->fields[i].id != db->fields[i].id) return false;
    return true;
}

template <class Obj>
void addPairs(std::map<NsKey, std::pair<const Obj*, const Obj*>>& m, const std::vector<const Obj*>& objs, bool isA) {
    for (const Obj* o : objs) {
        auto& p = m[NsKey{o->nsId, o->name}];
        (isA ? p.first : p.second) = o;
    }
}
template <class Obj>
void diffType(ObjectType type, const std::vector<const Obj*>& a, const std::vector<const Obj*>& b,
              std::vector<DiffEntry>& out) {
    std::map<NsKey, std::pair<const Obj*, const Obj*>> byName;   // (type, nsId, name) order
    addPairs(byName, a, true);
    addPairs(byName, b, false);
    for (const auto& [key, p] : byName) {
        const uint64_t fa = p.first ? fingerprintOf(*p.first) : 0;
        const uint64_t fb = p.second ? fingerprintOf(*p.second) : 0;
        if (p.first && p.second && fa == fb) continue;
        DiffEntry d{.change = !p.first ? DiffEntry::Change::Added
                            : !p.second ? DiffEntry::Change::Removed
                                        : DiffEntry::Change::Changed,
                    .type = type, .name = key.name, .fingerprintA = fa, .fingerprintB = fb, .fields = {},
                    .nsId = key.nsId};
        if (d.change == DiffEntry::Change::Changed) d.fields = changedFields(*p.first, *p.second);
        out.push_back(std::move(d));
    }
}
} // namespace

std::string_view objectTypeName(ObjectType t) noexcept {
    switch (t) {
    case ObjectType::Topic:  return "topic";
    case ObjectType::View:   return "view";
    case ObjectType::Schema: return "schema";
    }
    return "?";
}

std::vector<const RecordedEvent*> history(std::span<const RecordedEvent> log, const HistoryFilter& f) {
    std::vector<const RecordedEvent*> out;
    for (const RecordedEvent& rec : log) {
        const CatalogEvent& e = rec.event;
        if (f.type && !matchesType(e, *f.type)) continue;
        if (!f.name.empty() && e.objectName != f.name) continue;
        if (f.nsId && e.nsId != *f.nsId) continue;
        if (e.timestampMs < f.sinceMs) continue;
        out.push_back(&rec);
    }
    return out;
}

std::expected<CatalogState, ReplayError> stateAsOf(std::span<const RecordedEvent> log, const AsOf& at) {
    bool reached = false;
    const auto never = [](const RecordedEvent&, const CatalogState&) { return false; };
    switch (at.by) {
    case AsOf::By::Offset:
        return replayUntil(log, [&](const RecordedEvent& r) { return r.offset > at.value; }, never, reached);
    case AsOf::By::TimeMs:
        return replayUntil(log, [&](const RecordedEvent& r) { return r.event.timestampMs > at.value; }, never, reached);
    case AsOf::By::Version:
        return asOfVersion(log, at);
    }
    return std::unexpected(ReplayError{.offset = -1, .code = ReplayError::Code::Apply, .decode = std::nullopt,
                                       .apply = std::nullopt,
                                       .detail = "unknown AS OF selector " + std::to_string(static_cast<int>(at.by))});
}

std::vector<std::string> changedFields(const TopicObject& a, const TopicObject& b) {
    std::vector<std::string> out;
    if (a.version != b.version) out.emplace_back("version");
    if (a.partitions != b.partitions) out.emplace_back("partitions");
    if (a.replicationFactor != b.replicationFactor) out.emplace_back("replicationFactor");
    if (a.persistence != b.persistence) out.emplace_back("persistence");
    const TopicConfig& x = a.config;
    const TopicConfig& y = b.config;
    if (x.cleanupPolicy != y.cleanupPolicy) out.emplace_back("config.cleanupPolicy");
    if (x.retentionMs != y.retentionMs) out.emplace_back("config.retentionMs");
    if (x.retentionBytes != y.retentionBytes) out.emplace_back("config.retentionBytes");
    if (x.deleteRetentionMs != y.deleteRetentionMs) out.emplace_back("config.deleteRetentionMs");
    if (x.minCleanableDirtyRatioPpm != y.minCleanableDirtyRatioPpm) out.emplace_back("config.minCleanableDirtyRatioPpm");
    if (x.segmentRollBytes != y.segmentRollBytes) out.emplace_back("config.segmentRollBytes");
    if (x.minCompactionLagMs != y.minCompactionLagMs) out.emplace_back("config.minCompactionLagMs");
    if (x.maxCompactionLagMs != y.maxCompactionLagMs) out.emplace_back("config.maxCompactionLagMs");
    if (x.retentionCheckMs != y.retentionCheckMs) out.emplace_back("config.retentionCheckMs");
    if (x.fileDeleteDelayMs != y.fileDeleteDelayMs) out.emplace_back("config.fileDeleteDelayMs");
    if (x.durability != y.durability) out.emplace_back("config.durability");
    if (x.durabilityTier != y.durabilityTier) out.emplace_back("config.durabilityTier");
    if (x.coPartitionGroup != y.coPartitionGroup) out.emplace_back("config.coPartitionGroup");
    if (x.partitionKey != y.partitionKey) out.emplace_back("config.partitionKey");
    if (a.schema.has_value() != b.schema.has_value()) out.emplace_back("schema");
    else if (a.schema) schemaFields(*a.schema, *b.schema, out);
    return out;
}

std::vector<std::string> changedFields(const ViewObject& a, const ViewObject& b) {
    std::vector<std::string> out;
    if (a.version != b.version) out.emplace_back("version");
    if (a.def.sinkPartitions != b.def.sinkPartitions) out.emplace_back("view.sinkPartitions");
    if (a.def.persistence != b.def.persistence) out.emplace_back("view.persistence");
    if (a.def.keyField != b.def.keyField) out.emplace_back("view.keyField");
    if (a.def.sourceNames != b.def.sourceNames) out.emplace_back("view.sources");
    if (a.def.normalizedPlanBlob != b.def.normalizedPlanBlob) out.emplace_back("view.plan");
    if (!sameOutput(a.def.outputDescriptorBlob, b.def.outputDescriptorBlob)) out.emplace_back("view.output");
    return out;
}

std::vector<DiffEntry> diff(const CatalogState& a, const CatalogState& b) {
    std::vector<DiffEntry> out;
    diffType(ObjectType::Topic, a.topics(), b.topics(), out);
    diffType(ObjectType::View, a.views(), b.views(), out);
    return out;
}

} // namespace rune::catalog
