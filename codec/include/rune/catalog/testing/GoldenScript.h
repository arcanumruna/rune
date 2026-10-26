#pragma once
// rune/catalog/testing/GoldenScript.h — the deterministic formatVer-1 event script the
// golden generator (tools/make_catalog_golden.cpp) and the golden / replayer / query tests
// share.  Header-only.  Built on a growing CatalogState exactly as the leader builds events:
// fill the event, stamp it with previewFingerprint, apply it.  Every AlterSchema is built
// with resolveAlterOps on the growing state, so its ops carry durable tags (F20).
//
// Deterministic: timestamps 1'760'000'000'000 + 1000*i, fixed principals, request ids
// RequestId{0x601D, i+1} with request fingerprints 0xF00D'0000 + i (F14).
//
// FROZEN with formatVer 1: tests/catalog/golden/v1/basic.rcat was generated from this script
// and CatalogGoldenRegression.TheGoldenScriptStillProducesTheCheckedInBytes pins it.  Never
// edit an event; a new formatVer gets a new script beside this one.  (One deliberate exception, #143:
// the view sinks were made to mirror their first source's partition count when that became an
// apply rule; basic.rcat and CatalogGoldenPins.h were regenerated with tools/make_catalog_golden.)
//
// Offsets (objectId / topicId in brackets):
//   0 CreateTopic players     typed {id i64 key, hp i32, guild str}, disk, 8 partitions, rf 3,
//                             compacted, non-default config                        [1 / 0]
//   1 CreateTopic events      untyped, 4 partitions, rf 2, non-default config       [2 / 1]
//   2 CreateTopic presence    untyped, transient, 2 partitions, rf 1                [3 / 2]
//   3 CreateSchema events     {kind enum[1,2,3], payload bytes, at u64}, no key
//   4 AlterSchema players     ADD mana i32                         (replayedFrom set)
//   5 AlterSchema players     RENAME hp -> health                  -> schema version 3
//   6 SetCompatibility players FORWARD
//   7 AlterTopicConfig events retention.ms 60000
//   8 AlterTopicConfig events retention.bytes 1 GiB, min.cleanable.dirty.ratio 0.25
//   9 CreateView top_players  over players, sink 3, 8 partitions                    [4 / 3]
//  10 AlterView top_players   fresh sink 4 (old 3), transient, 8 partitions, wider output
//  11 CreateView ev_view      over events, sink 5, 4 partitions                     [5 / 5]
//  12 DropView ev_view
//  13 DropTopic presence
//  14 CreateTopic presence    reuse: new objectId, transient, untyped               [6 / 6]
//  15 DropView top_players
//  16 DropTopic players
//  17 CreateTopic players     reuse: new objectId, untyped                          [7 / 7]
//  18 CreateSchema presence   {player i64 key, zone fixedstr(8)}  (replayedFrom set)
//  19 AlterSchema presence    ADD status fixedstr(4), DROP zone, SET COMPATIBILITY BACKWARD
//  20 SetCompatibility presence FORWARD
//  21 CreateView ev_view      reuse of a VIEW name: new objectId, over events, sink 8, 4 partitions [8 / 8]
//  22 AlterView ev_view       fresh sink 9 (old 8), two sources (events, presence), 4 partitions
//                             (replayedFrom set)
// Every kind appears at least twice; replayedFrom is present on 4, 18, 22 and absent elsewhere.
// Every view sink has its FIRST source's partition count (#143: the apply rule refuses anything else).
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rune::catalog::testing {

inline constexpr int64_t  kGoldenBaseTimestampMs       = 1'760'000'000'000;
inline constexpr uint64_t kGoldenRequestIdHi           = 0x601D;
inline constexpr uint64_t kGoldenRequestFingerprintBase = 0xF00D'0000;

namespace golden_detail {

namespace cd = rune::codec;

/// Builds events on a growing state; throws std::logic_error naming the step if any event
/// does not preview or apply (the script is broken — never silently emitted).
class ScriptBuilder {
public:
    const CatalogState& state() const noexcept { return s_; }

    void emit(Ops ops, const std::string& name, uint64_t objectId, uint32_t prev, std::string statement,
              std::optional<ReplayedFrom> replayedFrom = std::nullopt, std::string authenticated = {}) {
        const auto i = static_cast<uint64_t>(events_.size());
        CatalogEvent e;
        e.formatVer   = kFormatV1;   // FROZEN: the v1 script stays a formatVer-1 log (kCurrentFormatVer is 2)
        e.kind        = kindOf(ops);
        e.ops         = std::move(ops);
        e.objectId    = objectId;
        e.objectName  = name;
        e.prevVersion = prev;
        e.newVersion  = prev + 1;
        e.statement   = std::move(statement);
        e.principal   = Principal{.sessionId = 0x5E55'1000 + static_cast<int64_t>(i % 3),
                                  .clientName = "golden-cli",
                                  .sourceAddress = "aeron:udp?endpoint=10.0.0.7:40123",
                                  .authenticated = std::move(authenticated),
                                  .requestId = RequestId{kGoldenRequestIdHi, i + 1},
                                  .requestFingerprint = kGoldenRequestFingerprintBase + i};
        e.replayedFrom = std::move(replayedFrom);
        e.timestampMs  = kGoldenBaseTimestampMs + 1000 * static_cast<int64_t>(i);
        auto fp = previewFingerprint(s_, e);
        if (!fp) throw std::logic_error("golden script step " + std::to_string(i) + ": " + fp.error().detail);
        e.fingerprint = *fp;
        if (auto a = apply(s_, e, static_cast<int64_t>(i)); !a)
            throw std::logic_error("golden script step " + std::to_string(i) + ": " + a.error().detail);
        events_.push_back(std::move(e));
    }

    // ── one helper per kind, naming objects by their CURRENT live name ──────────
    void createTopic(const std::string& name, CreateTopicOps o, std::string stmt) {
        o.topicId = s_.nextTopicId();
        emit(std::move(o), name, s_.nextObjectId(), 0, std::move(stmt));
    }
    void dropTopic(const std::string& name, std::string stmt) {
        const TopicObject& t = topic(name);
        emit(DropTopicOps{t.topicId}, name, t.objectId, t.version, std::move(stmt));
    }
    void alterConfig(const std::string& name, std::vector<ConfigChange> changes, std::string stmt) {
        const TopicObject& t = topic(name);
        emit(AlterTopicConfigOps{std::move(changes)}, name, t.objectId, t.version, std::move(stmt));
    }
    void createSchema(const std::string& name, SchemaDef def, std::string stmt,
                      std::optional<ReplayedFrom> rf = std::nullopt) {
        const TopicObject& t = topic(name);
        emit(CreateSchemaOps{std::move(def)}, name, t.objectId, t.version, std::move(stmt), std::move(rf));
    }
    void alterSchema(const std::string& name, const std::vector<cd::AlterOp>& ops, std::string stmt,
                     std::optional<ReplayedFrom> rf = std::nullopt, std::string authenticated = {}) {
        const TopicObject& t = topic(name);
        auto resolved = resolveAlterOps(*t.schema, ops);
        if (!resolved) throw std::logic_error("golden script resolve on '" + name + "': " + resolved.error());
        emit(AlterSchemaOps{std::move(*resolved)}, name, t.objectId, t.version, std::move(stmt), std::move(rf),
             std::move(authenticated));
    }
    void setCompat(const std::string& name, cd::CompatMode mode, std::string stmt) {
        const TopicObject& t = topic(name);
        emit(SetCompatibilityOps{mode}, name, t.objectId, t.version, std::move(stmt));
    }
    void createView(const std::string& name, ViewDef v, std::string stmt) {
        v.viewTopicId = s_.nextTopicId();
        emit(CreateViewOps{std::move(v)}, name, s_.nextObjectId(), 0, std::move(stmt));
    }
    void alterView(const std::string& name, ViewDef v, std::string stmt, std::optional<ReplayedFrom> rf = std::nullopt) {
        const ViewObject& cur = view(name);
        v.viewTopicId = s_.nextTopicId();   // a fresh sink (ruling 4)
        emit(AlterViewOps{std::move(v)}, name, cur.objectId, cur.version, std::move(stmt), std::move(rf));
    }
    void dropView(const std::string& name, std::string stmt) {
        const ViewObject& v = view(name);
        emit(DropViewOps{v.def.viewTopicId}, name, v.objectId, v.version, std::move(stmt));
    }

    std::vector<CatalogEvent> take() && { return std::move(events_); }

private:
    const TopicObject& topic(const std::string& n) const {
        const TopicObject* t = s_.topicByName(n);
        if (t == nullptr) throw std::logic_error("golden script: no live topic '" + n + "'");
        return *t;
    }
    const ViewObject& view(const std::string& n) const {
        const ViewObject* v = s_.viewByName(n);
        if (v == nullptr) throw std::logic_error("golden script: no live view '" + n + "'");
        return *v;
    }
    CatalogState              s_;
    std::vector<CatalogEvent> events_;
};

inline cd::AlterOp addOp(std::string name, cd::PrimType t, uint16_t fixedLen = 0) {
    cd::AlterOp op{};
    op.type = cd::AlterOpType::Add; op.name = std::move(name); op.primType = t; op.fixedLen = fixedLen;
    op.mode = cd::CompatMode::Backward;
    return op;
}
inline cd::AlterOp dropOp(std::string name) {
    cd::AlterOp op{};
    op.type = cd::AlterOpType::Drop; op.name = std::move(name); op.primType = cd::PrimType::Int32; op.fixedLen = 0;
    op.mode = cd::CompatMode::Backward;
    return op;
}
inline cd::AlterOp renameOp(std::string from, std::string to) {
    cd::AlterOp op{};
    op.type = cd::AlterOpType::Rename; op.name = std::move(from); op.newName = std::move(to);
    op.primType = cd::PrimType::Int32; op.fixedLen = 0; op.mode = cd::CompatMode::Backward;
    return op;
}
inline cd::AlterOp setCompatOp(cd::CompatMode m) {
    cd::AlterOp op{};
    op.type = cd::AlterOpType::SetCompatibility; op.primType = cd::PrimType::Int32; op.fixedLen = 0; op.mode = m;
    return op;
}

} // namespace golden_detail

/// The 23 formatVer-1 golden events, offsets 0..22 (see the table above).
[[nodiscard]] inline std::vector<CatalogEvent> goldenScriptV1() {
    namespace cd = rune::codec;
    using golden_detail::addOp;
    using golden_detail::dropOp;
    using golden_detail::renameOp;
    using golden_detail::setCompatOp;
    golden_detail::ScriptBuilder b;

    // 0 — players: typed, disk, rf 3, compacted, non-default config.
    {
        cd::DescriptorBuilder d("players");
        d.add("id", cd::PrimType::Int64, 1, /*isKey=*/true).add("hp", cd::PrimType::Int32).add("guild", cd::PrimType::String);
        CreateTopicOps o;
        o.partitions = 8; o.replicationFactor = 3; o.persistence = Persistence::Disk;
        o.config.cleanupPolicy = 1;                      // Compact
        o.config.deleteRetentionMs = 3'600'000;
        o.config.minCompactionLagMs = 5'000;
        o.config.maxCompactionLagMs = 600'000;
        o.config.coPartitionGroup = "world";
        o.config.partitionKey = "id";
        o.schema = SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = cd::serialize(d.build()), .nextTag = 3};
        b.createTopic("players", std::move(o),
                      "CREATE TOPIC players (id INT64 KEY, hp INT32, guild STRING) WITH (partitions=8, replicas=3, "
                      "cleanup.policy=compact)");
    }
    // 1 — events: untyped, 4 partitions.
    {
        CreateTopicOps o;
        o.partitions = 4; o.replicationFactor = 2;
        o.config.cleanupPolicy = 2;                      // CompactDelete
        o.config.retentionMs = 7LL * 24 * 3'600'000;
        o.config.segmentRollBytes = 16LL << 20;
        o.config.retentionCheckMs = 10'000;
        o.config.fileDeleteDelayMs = 1'000;
        o.config.durability = 1;                         // AllInSync
        o.config.durabilityTier = 2;                     // Strict
        b.createTopic("events", std::move(o), "CREATE TOPIC events WITH (partitions=4, replicas=2)");
    }
    // 2 — presence: untyped, transient.
    {
        CreateTopicOps o;
        o.partitions = 2; o.replicationFactor = 1; o.persistence = Persistence::Transient;
        o.config.durabilityTier = 0;                     // Leader
        b.createTopic("presence", std::move(o), "CREATE TOPIC presence WITH (partitions=2, persistence=transient)");
    }
    // 3 — CreateSchema on events (schema-less adoption).
    {
        cd::DescriptorBuilder d("events");
        d.add("kind", cd::PrimType::Enum, 1, false, {1, 2, 3}).add("payload", cd::PrimType::Bytes).add("at", cd::PrimType::UInt64);
        b.createSchema("events", SchemaDef{.keyField = "", .validate = false, .descriptorBlob = cd::serialize(d.build()), .nextTag = 3},
                       "DEFINE TOPIC events (kind ENUM(1,2,3), payload BYTES, at UINT64)");
    }
    // 4, 5 — AlterSchema players.
    b.alterSchema("players", {addOp("mana", cd::PrimType::Int32)}, "ALTER TOPIC players ADD mana INT32",
                  ReplayedFrom{.clusterId = "eu-west-staging", .offset = 41, .principal = "ops@replay"});
    b.alterSchema("players", {renameOp("hp", "health")}, "ALTER TOPIC players RENAME hp TO health", std::nullopt,
                  /*authenticated=*/"alice");
    // 6 — SetCompatibility players.
    b.setCompat("players", cd::CompatMode::Forward, "ALTER TOPIC players SET COMPATIBILITY FORWARD");
    // 7, 8 — AlterTopicConfig events.
    b.alterConfig("events", {{ConfigKey::RetentionMs, 60'000}}, "ALTER TOPIC events SET (retention.ms=60000)");
    b.alterConfig("events", {{ConfigKey::RetentionBytes, int64_t{1} << 30}, {ConfigKey::MinCleanableDirtyRatioPpm, 250'000}},
                  "ALTER TOPIC events SET (retention.bytes=1073741824, min.cleanable.dirty.ratio=0.25)");
    // 9 — CreateView top_players over players.
    {
        cd::DescriptorBuilder out("top_players");
        out.add("id", cd::PrimType::Int64, 1, true).add("health", cd::PrimType::Int32);
        ViewDef v;
        v.sinkPartitions = 8; v.persistence = Persistence::Disk; v.keyField = "id"; v.sourceNames = {"players"};
        v.normalizedPlanBlob = {0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x20};
        v.compiledPlanBlob = {0xC0, 0x00, 0x00, 0x00, 0x00, 0x10, 0x20};
        v.outputDescriptorBlob = cd::serialize(out.build());
        b.createView("top_players", std::move(v), "CREATE VIEW top_players AS SELECT id, health FROM players");
    }
    // 10 — AlterView top_players: a fresh sink (new 4, old 3 — F2), a different plan and output.
    {
        cd::DescriptorBuilder out("top_players");
        out.add("id", cd::PrimType::Int64, 1, true).add("health", cd::PrimType::Int32).add("mana", cd::PrimType::Int32);
        ViewDef v;
        v.sinkPartitions = 8; v.persistence = Persistence::Transient; v.keyField = "id"; v.sourceNames = {"players"};
        v.normalizedPlanBlob = {0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x20, 0x30};
        v.compiledPlanBlob = {0xC1, 0x00, 0x00, 0x00, 0x00, 0x10, 0x20, 0x30};
        v.outputDescriptorBlob = cd::serialize(out.build());
        b.alterView("top_players", std::move(v), "ALTER VIEW top_players AS SELECT id, health, mana FROM players");
    }
    // 11, 12 — CreateView ev_view over events, then DropView.
    {
        cd::DescriptorBuilder out("ev_view");
        out.add("kind", cd::PrimType::Enum, 1, false, {1, 2, 3}).add("n", cd::PrimType::UInt64);
        ViewDef v;
        v.sinkPartitions = 4; v.sourceNames = {"events"};
        v.normalizedPlanBlob = {0x02, 0x00, 0x00, 0x00, 0x00};
        v.compiledPlanBlob = {0xC2, 0x01, 0x00, 0x00, 0x00};
        v.outputDescriptorBlob = cd::serialize(out.build());
        b.createView("ev_view", std::move(v), "CREATE VIEW ev_view AS SELECT kind, COUNT(*) AS n FROM events GROUP BY kind");
    }
    b.dropView("ev_view", "DROP VIEW ev_view");
    // 13, 14 — drop and reuse presence (a new objectId; transient again).
    b.dropTopic("presence", "DROP TOPIC presence");
    {
        CreateTopicOps o;
        o.partitions = 2; o.replicationFactor = 1; o.persistence = Persistence::Transient;
        o.config.durabilityTier = 0;
        b.createTopic("presence", std::move(o), "CREATE TOPIC presence WITH (partitions=2, persistence=transient)");
    }
    // 15, 16, 17 — retire top_players and players, then reuse the name untyped.
    b.dropView("top_players", "DROP VIEW top_players");
    b.dropTopic("players", "DROP TOPIC players");
    b.createTopic("players", CreateTopicOps{}, "CREATE TOPIC players");
    // 18 — CreateSchema on the reused presence (a FixedStr field and a key).
    {
        cd::DescriptorBuilder d("presence");
        d.add("player", cd::PrimType::Int64, 1, true).addFixedStr("zone", 8);
        b.createSchema("presence", SchemaDef{.keyField = "player", .validate = true, .descriptorBlob = cd::serialize(d.build()),
                                             .nextTag = 2},
                       "DEFINE TOPIC presence (player INT64 KEY, zone FIXEDSTR(8))",
                       ReplayedFrom{.clusterId = "eu-west-staging", .offset = 77, .principal = ""});
    }
    // 19 — AlterSchema with a FixedStr ADD, a DROP and an inline SET COMPATIBILITY.
    b.alterSchema("presence",
                  {addOp("status", cd::PrimType::FixedStr, 4), dropOp("zone"), setCompatOp(cd::CompatMode::Backward)},
                  "ALTER TOPIC presence ADD status FIXEDSTR(4), DROP zone, SET COMPATIBILITY BACKWARD");
    // 20 — SetCompatibility presence.
    b.setCompat("presence", cd::CompatMode::Forward, "ALTER TOPIC presence SET COMPATIBILITY FORWARD");
    // 21, 22 — reuse the VIEW name ev_view, then alter it onto two sources.
    {
        cd::DescriptorBuilder out("ev_view");
        out.add("at", cd::PrimType::UInt64, 1, true);
        ViewDef v;
        v.sinkPartitions = 4; v.keyField = "at"; v.sourceNames = {"events"};
        v.normalizedPlanBlob = {0x03, 0x00, 0x00, 0x00, 0x00};
        v.compiledPlanBlob = {0xC3, 0x01, 0x00, 0x00, 0x00};
        v.outputDescriptorBlob = cd::serialize(out.build());
        b.createView("ev_view", std::move(v), "CREATE VIEW ev_view AS SELECT at FROM events");
    }
    {
        cd::DescriptorBuilder out("ev_view");
        out.add("at", cd::PrimType::UInt64, 1, true).add("player", cd::PrimType::Int64);
        ViewDef v;
        v.sinkPartitions = 4; v.keyField = "at"; v.sourceNames = {"events", "presence"};
        v.normalizedPlanBlob = {0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00};
        v.compiledPlanBlob = {0xC4, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00};
        v.outputDescriptorBlob = cd::serialize(out.build());
        b.alterView("ev_view", std::move(v), "ALTER VIEW ev_view AS SELECT e.at, p.player FROM events e JOIN presence p",
                    ReplayedFrom{.clusterId = "", .offset = 0, .principal = "bob"});
    }
    return std::move(b).take();
}

} // namespace rune::catalog::testing
