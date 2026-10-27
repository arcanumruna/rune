// Catalog history (spec §4, §6; ruling 11): the object fingerprints cover EVERY definition
// field and NO id.  Table-driven: one row per struct field, and each struct's field count is
// pinned by a structured binding — adding a field to TopicObject / TopicConfig / SchemaState /
// ViewObject / ViewDef / Tombstone breaks the build here until the field gets its row (and
// its place in CatalogFingerprint.cpp), so no field is ever forgotten silently.
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <functional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rc = rune::catalog;
namespace cd = rune::codec;

namespace {

// ── field-count pins: a structured binding must name EVERY member ──────────────────
constexpr std::size_t kTopicObjectFields = 10;   // + nsId (namespaces): not in the frozen v1 fingerprint, mixed in by CatalogFingerprintV2
constexpr std::size_t kTopicConfigFields = 14;
constexpr std::size_t kSchemaStateFields = 6;
constexpr std::size_t kViewObjectFields  = 5;
constexpr std::size_t kViewDefFields     = 8;
constexpr std::size_t kTombstoneFields   = 5;
[[maybe_unused]] void pinFieldCounts(const rc::TopicObject& t, const rc::TopicConfig& c, const rc::SchemaState& s,
                                     const rc::ViewObject& v, const rc::ViewDef& d, const rc::Tombstone& x) {
    [[maybe_unused]] const auto& [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10] = t;                      // 10
    [[maybe_unused]] const auto& [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14] = c;     // 14
    [[maybe_unused]] const auto& [s1, s2, s3, s4, s5, s6] = s;                                          // 6
    [[maybe_unused]] const auto& [v1, v2, v3, v4, v5] = v;                                           // 5
    [[maybe_unused]] const auto& [d1, d2, d3, d4, d5, d6, d7, d8] = d;                                  // 8
    [[maybe_unused]] const auto& [x1, x2, x3, x4, x5] = x;                                           // 5
}

/// Definition: the field is part of what the object IS — changing it changes the fingerprint.
/// Id: cluster-local identity (ruling 11) — changing it never does.
enum class Role { Definition, Id };
template <class T>
struct Row {
    const char*            field;
    Role                   role;
    std::function<void(T&)> mutate;
};

/// Every row mutates a copy of `base` (and must really change it); a Definition row must
/// change `fp`, an Id row must not.  One row per field, each field once.
template <class T, class Fp>
void expectEveryField(const T& base, const std::vector<Row<T>>& rows, std::size_t fieldCount, Fp fp) {
    EXPECT_EQ(rows.size(), fieldCount) << "one row per struct field";
    std::set<std::string> seen;
    const uint64_t before = fp(base);
    for (const auto& r : rows) {
        EXPECT_TRUE(seen.insert(r.field).second) << r.field << " has two rows";
        T m = base;
        r.mutate(m);
        EXPECT_FALSE(m == base) << r.field << ": the row's mutation does not change the field";
        if (r.role == Role::Definition) EXPECT_NE(fp(m), before) << r.field << " is not fingerprinted";
        else EXPECT_EQ(fp(m), before) << r.field << " is an id and must not be fingerprinted";
    }
}

std::vector<uint8_t> descriptorBlob() {   // every field attribute represented once
    cd::DescriptorBuilder b("players");
    b.add("id", cd::PrimType::Int64, 1, true);
    b.add("class", cd::PrimType::Enum, 1, false, {1, 2, 3});
    b.addFixedStr("tag", 8);
    b.addTagged("old", 3, cd::PrimType::Int32, 1, 2, false, {});   // retired at version 2
    b.add("bio", cd::PrimType::String);
    return cd::serialize(b.build());
}

rc::TopicObject baseTopic() {
    rc::TopicObject t;
    t.objectId = 5; t.name = "players"; t.version = 3; t.topicId = 7; t.partitions = 4; t.replicationFactor = 3;
    t.persistence = rc::Persistence::Disk;
    t.schema = rc::SchemaState{.keyField = "id", .validate = true, .evolvingBlob = descriptorBlob(), .schemaVersion = 2,
                               .nextTag = 5, .compat = cd::CompatMode::Backward};
    return t;
}
rc::ViewObject baseView() {
    rc::ViewObject v;
    v.objectId = 9; v.name = "totals"; v.version = 2;
    v.def.viewTopicId = 11; v.def.sinkPartitions = 2; v.def.persistence = rc::Persistence::Disk; v.def.keyField = "id";
    v.def.sourceNames = {"players", "guilds"}; v.def.normalizedPlanBlob = {1, 2, 3}; v.def.compiledPlanBlob = {4, 5};
    v.def.outputDescriptorBlob = descriptorBlob();
    return v;
}

/// Re-serialize the descriptor in `blob` with one attribute changed.
std::vector<uint8_t> withDescriptor(const std::vector<uint8_t>& blob, const std::function<void(cd::Descriptor&)>& f) {
    cd::Descriptor d = cd::deserialize(blob);
    f(d);
    return cd::serialize(d);
}
cd::FieldDescriptor& field(cd::Descriptor& d, const char* name) {
    for (auto& f : d.fields) if (f.name == name) return f;
    throw std::logic_error(name);
}
/// Every attribute of a descriptor that is part of its definition.  The FIELD ORDER is too:
/// it fixes the block layout (offsets) a record is written in.
std::vector<std::pair<const char*, std::function<void(cd::Descriptor&)>>> descriptorAttributeMutations() {
    return {
        {"descriptor name", [](cd::Descriptor& d) { d.name = "pc"; }},
        {"descriptor version", [](cd::Descriptor& d) { d.version = 4; }},
        {"field order", [](cd::Descriptor& d) { std::swap(d.fields[0], d.fields[1]); }},
        {"field name", [](cd::Descriptor& d) { field(d, "id").name = "pid"; }},
        {"field tag", [](cd::Descriptor& d) { field(d, "bio").id = 9; }},
        {"field type", [](cd::Descriptor& d) { field(d, "id").type = cd::PrimType::UInt64; }},
        {"field sinceVersion", [](cd::Descriptor& d) { field(d, "bio").sinceVersion = 2; }},
        {"field untilVersion", [](cd::Descriptor& d) { field(d, "old").untilVersion = 3; }},
        {"field isKey", [](cd::Descriptor& d) { field(d, "class").isKey = true; }},
        {"field fixedLen", [](cd::Descriptor& d) { field(d, "tag").fixedLen = 16; }},
        {"field enumValues", [](cd::Descriptor& d) { field(d, "class").enumValues.push_back(4); }},
        {"a field added", [](cd::Descriptor& d) { cd::FieldDescriptor f; f.name = "x"; f.id = 7; d.fields.push_back(f); }},
        {"a field removed", [](cd::Descriptor& d) { d.fields.pop_back(); }},
    };
}

} // namespace

TEST(CatalogFingerprintRegression, TopicObjectEveryFieldIsCoveredAndIdsAreNot) {
    const std::vector<Row<rc::TopicObject>> rows = {
        {"objectId", Role::Id, [](rc::TopicObject& t) { t.objectId = 99; }},
        {"name", Role::Definition, [](rc::TopicObject& t) { t.name = "npcs"; }},
        {"version", Role::Definition, [](rc::TopicObject& t) { t.version = 4; }},
        {"topicId", Role::Id, [](rc::TopicObject& t) { t.topicId = 42; }},
        {"nsId", Role::Id, [](rc::TopicObject& t) { t.nsId = 7; }},   // the v1 fingerprint is frozen; v2 mixes the nsId in
        {"partitions", Role::Definition, [](rc::TopicObject& t) { t.partitions = 5; }},
        {"replicationFactor", Role::Definition, [](rc::TopicObject& t) { t.replicationFactor = 1; }},
        {"persistence", Role::Definition, [](rc::TopicObject& t) { t.persistence = rc::Persistence::Transient; }},
        {"config", Role::Definition, [](rc::TopicObject& t) { t.config.retentionMs = 1; }},   // per field: below
        {"schema", Role::Definition, [](rc::TopicObject& t) { t.schema.reset(); }},           // per field: below
    };
    expectEveryField(baseTopic(), rows, kTopicObjectFields, [](const rc::TopicObject& t) { return rc::fingerprintOf(t); });
}

TEST(CatalogFingerprintRegression, TopicConfigEveryFieldIsCovered) {
    using C = rc::TopicConfig;
    const std::vector<Row<C>> rows = {
        {"cleanupPolicy", Role::Definition, [](C& c) { c.cleanupPolicy = 1; }},
        {"retentionMs", Role::Definition, [](C& c) { c.retentionMs = 5; }},
        {"retentionBytes", Role::Definition, [](C& c) { c.retentionBytes = 6; }},
        {"deleteRetentionMs", Role::Definition, [](C& c) { c.deleteRetentionMs = 7; }},
        {"minCleanableDirtyRatioPpm", Role::Definition, [](C& c) { c.minCleanableDirtyRatioPpm = 8; }},
        {"segmentRollBytes", Role::Definition, [](C& c) { c.segmentRollBytes = 9; }},
        {"minCompactionLagMs", Role::Definition, [](C& c) { c.minCompactionLagMs = 10; }},
        {"maxCompactionLagMs", Role::Definition, [](C& c) { c.maxCompactionLagMs = 11; }},
        {"retentionCheckMs", Role::Definition, [](C& c) { c.retentionCheckMs = 12; }},
        {"fileDeleteDelayMs", Role::Definition, [](C& c) { c.fileDeleteDelayMs = 13; }},
        {"durability", Role::Definition, [](C& c) { c.durability = 1; }},
        {"durabilityTier", Role::Definition, [](C& c) { c.durabilityTier = 0; }},
        {"coPartitionGroup", Role::Definition, [](C& c) { c.coPartitionGroup = "g"; }},
        {"partitionKey", Role::Definition, [](C& c) { c.partitionKey = "k"; }},
    };
    const rc::TopicObject base = baseTopic();
    expectEveryField(base.config, rows, kTopicConfigFields, [&](const C& c) {
        rc::TopicObject t = base; t.config = c; return rc::fingerprintOf(t);
    });
}

TEST(CatalogFingerprintRegression, SchemaStateEveryFieldIsCovered) {
    using S = rc::SchemaState;
    const std::vector<Row<S>> rows = {
        {"keyField", Role::Definition, [](S& s) { s.keyField = "class"; }},
        {"validate", Role::Definition, [](S& s) { s.validate = false; }},
        {"evolvingBlob", Role::Definition, [](S& s) { s.evolvingBlob.push_back(0); }},   // per attribute: below
        {"schemaVersion", Role::Definition, [](S& s) { s.schemaVersion = 3; }},
        {"nextTag", Role::Definition, [](S& s) { s.nextTag = 6; }},
        {"compat", Role::Definition, [](S& s) { s.compat = cd::CompatMode::Forward; }},
    };
    const rc::TopicObject base = baseTopic();
    expectEveryField(*base.schema, rows, kSchemaStateFields, [&](const S& s) {
        rc::TopicObject t = base; t.schema = s; return rc::fingerprintOf(t);
    });
}

TEST(CatalogFingerprintRegression, EveryAttributeOfTheEvolvingDescriptorIsCovered) {
    const rc::TopicObject base = baseTopic();
    const uint64_t before = rc::fingerprintOf(base);
    for (const auto& [what, mutate] : descriptorAttributeMutations()) {
        rc::TopicObject t = base;
        t.schema->evolvingBlob = withDescriptor(base.schema->evolvingBlob, mutate);
        EXPECT_NE(rc::fingerprintOf(t), before) << what;
    }
}

TEST(CatalogFingerprintRegression, ViewObjectEveryFieldIsCoveredAndIdsAreNot) {
    const std::vector<Row<rc::ViewObject>> rows = {
        {"objectId", Role::Id, [](rc::ViewObject& v) { v.objectId = 99; }},
        {"nsId", Role::Id, [](rc::ViewObject& v) { v.nsId = 7; }},
        {"name", Role::Definition, [](rc::ViewObject& v) { v.name = "sums"; }},
        {"version", Role::Definition, [](rc::ViewObject& v) { v.version = 3; }},
        {"def", Role::Definition, [](rc::ViewObject& v) { v.def.sinkPartitions = 7; }},   // per field: below
    };
    expectEveryField(baseView(), rows, kViewObjectFields, [](const rc::ViewObject& v) { return rc::fingerprintOf(v); });
}

TEST(CatalogFingerprintRegression, ViewDefEveryFieldIsCoveredAndIdsAreNot) {
    using D = rc::ViewDef;
    const std::vector<Row<D>> rows = {
        {"viewTopicId", Role::Id, [](D& d) { d.viewTopicId = 42; }},
        {"sinkPartitions", Role::Definition, [](D& d) { d.sinkPartitions = 3; }},
        {"persistence", Role::Definition, [](D& d) { d.persistence = rc::Persistence::Transient; }},
        {"keyField", Role::Definition, [](D& d) { d.keyField = "class"; }},
        {"sourceNames", Role::Definition, [](D& d) { d.sourceNames = {"guilds", "players"}; }},   // order matters
        {"normalizedPlanBlob", Role::Definition, [](D& d) { d.normalizedPlanBlob = {1, 2, 4}; }},
        // Embeds cluster-local topic ids (ruling 11): never fingerprinted.
        {"compiledPlanBlob", Role::Id, [](D& d) { d.compiledPlanBlob = {9, 9, 9}; }},
        {"outputDescriptorBlob", Role::Definition, [](D& d) {   // per attribute: below
             d.outputDescriptorBlob = withDescriptor(d.outputDescriptorBlob, [](cd::Descriptor& x) { x.name = "o"; }); }},
    };
    const rc::ViewObject base = baseView();
    expectEveryField(base.def, rows, kViewDefFields, [&](const D& d) {
        rc::ViewObject v = base; v.def = d; return rc::fingerprintOf(v);
    });
}

TEST(CatalogFingerprintRegression, EveryAttributeOfTheViewOutputDescriptorIsCovered) {
    const rc::ViewObject base = baseView();
    const uint64_t before = rc::fingerprintOf(base);
    for (const auto& [what, mutate] : descriptorAttributeMutations()) {
        rc::ViewObject v = base;
        v.def.outputDescriptorBlob = withDescriptor(base.def.outputDescriptorBlob, mutate);
        EXPECT_NE(rc::fingerprintOf(v), before) << what;
    }
    // The output is hashed as a DEFINITION, not as bytes: the same descriptor re-serialized
    // (e.g. read from a legacy RCD1 blob) hashes equal.
    rc::ViewObject same = base;
    same.def.outputDescriptorBlob = cd::serialize(cd::deserialize(base.def.outputDescriptorBlob));
    EXPECT_EQ(rc::fingerprintOf(same), before);
}

TEST(CatalogFingerprintRegression, TombstoneEveryFieldIsCoveredAndIdsAreNot) {
    const std::vector<Row<rc::Tombstone>> rows = {
        {"objectId", Role::Id, [](rc::Tombstone& t) { t.objectId = 99; }},
        {"nsId", Role::Id, [](rc::Tombstone& t) { t.nsId = 7; }},
        {"name", Role::Definition, [](rc::Tombstone& t) { t.name = "y"; }},
        {"finalVersion", Role::Definition, [](rc::Tombstone& t) { t.finalVersion = 4; }},
        {"wasView", Role::Definition, [](rc::Tombstone& t) { t.wasView = true; }},
    };
    expectEveryField(rc::Tombstone{.objectId = 1, .name = "x", .finalVersion = 3, .wasView = false}, rows,
                     kTombstoneFields, [](const rc::Tombstone& t) { return rc::fingerprintOf(t); });
}

TEST(CatalogFingerprintRegression, KindsAndLengthPrefixesSeparateLookalikes) {
    // A topic, a view and a tombstone of the same name never collide (the 'T' / 'V' / 'D' tags).
    rc::TopicObject t; t.name = "x"; t.version = 1;
    rc::ViewObject v; v.name = "x"; v.version = 1;
    const rc::Tombstone d{.objectId = 0, .name = "x", .finalVersion = 1, .wasView = false};
    EXPECT_NE(rc::fingerprintOf(t), rc::fingerprintOf(v));
    EXPECT_NE(rc::fingerprintOf(t), rc::fingerprintOf(d));
    // A byte moved across a length-prefixed boundary changes the hash.
    rc::TopicObject a = baseTopic(), b = baseTopic();
    a.config.coPartitionGroup = "ab"; a.config.partitionKey = "c";
    b.config.coPartitionGroup = "a";  b.config.partitionKey = "bc";
    EXPECT_NE(rc::fingerprintOf(a), rc::fingerprintOf(b));
}
