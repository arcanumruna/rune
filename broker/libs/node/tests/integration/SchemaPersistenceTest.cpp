// ---------------------------------------------------------------------------
// SchemaPersistenceTest.cpp — a schema survives restart through __catalog
// (catalog history Task 9 port of the Task 6 __topic_schemas suite).
//
// Every DDL is ONE __catalog event (CatalogDdlRig: a real disk Log + leader
// replica + the AdminWorker's CatalogApplier).  A restart is a fresh AdminWorker
// over the same catalog dir whose one boot replay (bootCatalog) re-applies the
// checkpointed prefix — the SchemaRegistry is its projection.  The scenarios of
// the __topic_schemas suite map one to one (same names where the behaviour is
// unchanged):
//   * the schema record lands in the log (now: one CreateTopic event);
//   * the registry is rebuilt after a restart (now: the catalog boot replay);
//   * a duplicate define writes nothing; validators follow the schema;
//   * the topic id is the catalog's (RULING-1: never a client's or a guess);
//   * every version + nextTag replays; records decode per writer version;
//   * replay is idempotent (two restarts, identical registries);
//   * an event past the HW is never applied;
//   * every version survives: __catalog is NEVER compacted (the composite
//     compaction key of __topic_schemas is moot — no compaction to survive);
//   * a topic is claimed under its OWN name at boot (catalogLiveTopicSpecs).
// Retired with __topic_schemas' boot replay (Task 16 deletes the log): its
// corrupt / truncated-record boot aborts — the catalog's are CatalogApplier's
// (Task 7: decode / fingerprint / digest halts, checkpoint-beyond-log refusal).
// TopicSchema's own codec (still in the tree until Task 16) keeps its tests.
// ---------------------------------------------------------------------------

#include <unistd.h>
#include <gtest/gtest.h>

#include "support/CatalogDdlRig.h"

#include <rune/node/AdminWorker.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Record.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

using namespace rune::node;
using namespace rune::codec;
using namespace rune::replication;
using rune::node::test::CatalogDdlRig;
namespace rc = rune::catalog;
namespace fs = std::filesystem;

namespace {

std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build());
}

AlterOp addOp(const std::string& name, PrimType t) {
    AlterOp op{}; op.type = AlterOpType::Add; op.name = name; op.primType = t; return op;
}
AlterOp setCompatOp(CompatMode m) {
    AlterOp op{}; op.type = AlterOpType::SetCompatibility; op.mode = m; return op;
}
AlterOp dropOp(const std::string& name) {
    AlterOp op{}; op.type = AlterOpType::Drop; op.name = name; return op;
}
AlterOp renameOp(const std::string& oldName, const std::string& newName) {
    AlterOp op{}; op.type = AlterOpType::Rename; op.name = oldName; op.newName = newName; return op;
}

CatalogDdlRig::Answer define(CatalogDdlRig& rig, const std::string& name, const std::string& keyField,
                             int32_t partitions, bool validate, std::vector<uint8_t> blob) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.keyField = keyField;
    m.partitionCount = partitions; m.validate = validate; m.blob = std::move(blob);
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

// [blockLength:u16][version:u16][fixed block] — the record wire shape readField()
// expects.  `fields` is (byteOffsetFromBlockStart, byteWidth, value).
std::vector<uint8_t> encodeFixedRecord(uint16_t version, uint32_t blockLength,
                                       const std::vector<std::tuple<uint32_t, uint32_t, uint64_t>>& fields) {
    std::vector<uint8_t> b(4 + blockLength, 0);
    b[0] = static_cast<uint8_t>(blockLength); b[1] = static_cast<uint8_t>(blockLength >> 8);
    b[2] = static_cast<uint8_t>(version);     b[3] = static_cast<uint8_t>(version >> 8);
    for (const auto& [offset, width, value] : fields)
        for (uint32_t i = 0; i < width; ++i) b[4 + offset + i] = static_cast<uint8_t>(value >> (8 * i));
    return b;
}

// Every field LIVE at entry.version gets version*1000 + tag at its OWN materialized offset.
std::vector<uint8_t> encodeRecordForEntry(const RegistryEntry& entry) {
    std::vector<std::tuple<uint32_t, uint32_t, uint64_t>> fields;
    for (const auto& f : entry.descriptor.fields)
        fields.emplace_back(f.offset, 8u, static_cast<uint64_t>(entry.version) * 1000ULL + f.id);
    return encodeFixedRecord(entry.version, entry.descriptor.blockLength, fields);
}

// Byte-identical registry state for every version 1..maxVersion of `topicId`.
void expectRegistriesByteIdenticalForAllVersions(const SchemaRegistry& a, const SchemaRegistry& b,
                                                 int32_t topicId, uint16_t maxVersion) {
    for (uint16_t v = 1; v <= maxVersion; ++v) {
        const RegistryEntry* ea = a.byIdVersion(topicId, v);
        const RegistryEntry* eb = b.byIdVersion(topicId, v);
        ASSERT_NE(ea, nullptr) << "registry A missing version " << v;
        ASSERT_NE(eb, nullptr) << "registry B missing version " << v;
        EXPECT_EQ(ea->version, eb->version);
        EXPECT_EQ(ea->nextTag, eb->nextTag) << "nextTag mismatch at v" << v;
        EXPECT_EQ(static_cast<int>(ea->compatMode), static_cast<int>(eb->compatMode)) << "at v" << v;
        EXPECT_EQ(ea->fingerprint, eb->fingerprint) << "fingerprint mismatch at v" << v;
        EXPECT_EQ(ea->keyField, eb->keyField) << "at v" << v;
        EXPECT_EQ(ea->partitionCount, eb->partitionCount) << "at v" << v;
        EXPECT_EQ(ea->descriptorBlob, eb->descriptorBlob) << "descriptorBlob mismatch at v" << v;
        ASSERT_EQ(ea->descriptor.fields.size(), eb->descriptor.fields.size()) << "at v" << v;
        for (std::size_t i = 0; i < ea->descriptor.fields.size(); ++i) {
            EXPECT_EQ(ea->descriptor.fields[i].name, eb->descriptor.fields[i].name);
            EXPECT_EQ(ea->descriptor.fields[i].id, eb->descriptor.fields[i].id);
            EXPECT_EQ(ea->descriptor.fields[i].offset, eb->descriptor.fields[i].offset);
        }
    }
    ASSERT_NE(a.latest(topicId), nullptr);
    ASSERT_NE(b.latest(topicId), nullptr);
    EXPECT_EQ(a.latest(topicId)->version, b.latest(topicId)->version);
}

} // namespace

TEST(SchemaPersistenceTest, SchemaRecord_LandsInLog) {
    CatalogDdlRig rig("sp_lands", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", "userId", 4, false, charsBlob());
    EXPECT_EQ(a.code, 0u) << "DefineTopic must succeed (OK=0)";
    EXPECT_EQ(a.topicId, 0) << "RULING-1: the topic id is the catalog's";
    ASSERT_NE(rig.schemas().byName("characters"), nullptr) << "the projection holds it after commit + apply";
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u) << "exactly one __catalog event";
    EXPECT_EQ(log[0].event.kind, rc::EventKind::CreateTopic);
    EXPECT_TRUE(std::get<rc::CreateTopicOps>(log[0].event.ops).schema.has_value()) << "the schema rides the event";
}

TEST(SchemaPersistenceTest, RebuildFromLog_RecoveryAfterRestart) {
    CatalogDdlRig rig("sp_restart", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", "userId", 8, false, charsBlob());
    ASSERT_EQ(a.code, 0u);
    rig.restart();   // a fresh AdminWorker over the same catalog dir: its boot replay
    const RegistryEntry* entry = rig.schemas().byName("characters");
    ASSERT_NE(entry, nullptr) << "the schema survives the restart through __catalog";
    EXPECT_EQ(entry->topicId, a.topicId);
    EXPECT_EQ(entry->partitionCount, 8);
    EXPECT_EQ(entry->version, 1u);
    EXPECT_EQ(entry->keyField, "userId");
    EXPECT_EQ(entry->descriptor.name, "characters");
}

TEST(SchemaPersistenceTest, DuplicateDefineTopic_IdempotentOnLog) {
    CatalogDdlRig rig("sp_dup", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", "userId", 4, false, charsBlob()).code, 0u);
    const int64_t leoAfterFirst = rig.replica().logEndOffset();
    EXPECT_EQ(define(rig, "characters", "userId", 4, false, charsBlob()).code, 25u);
    EXPECT_EQ(rig.replica().logEndOffset(), leoAfterFirst) << "TopicExists appends nothing";
}

TEST(SchemaPersistenceTest, ValidateTrue_PushValidatorsCalled) {
    CatalogDdlRig rig("sp_validate", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "orders", "", 2, /*validate=*/true, charsBlob());
    ASSERT_EQ(a.code, 0u);
    const auto n = std::count(rig.effects().begin(), rig.effects().end(),
                              "schemaChanged:" + std::to_string(a.topicId) + "@v1");
    EXPECT_EQ(n, 1) << "the validators are fanned out exactly once, for the assigned topic id";
}

TEST(SchemaPersistenceTest, Regression_RULING1_TopicIdIsTheCatalogId) {
    CatalogDdlRig rig("sp_ruling1", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "items", "", 1, false, charsBlob());
    const auto b = define(rig, "items2", "", 1, false, charsBlob());
    EXPECT_EQ(a.topicId, 0);
    EXPECT_EQ(b.topicId, 1) << "the next catalog id";
    const RegistryEntry* e = rig.schemas().byName("items");   // the TOPIC name (the blob says "characters")
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->topicId, a.topicId);
}

TEST(SchemaPersistenceTest, ReplayReconstructsAllVersionsAndNextTag) {
    CatalogDdlRig rig("sp_versions", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", "userId", 4, false, charsBlob());
    ASSERT_EQ(a.code, 0u);
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).code, 0);
    ASSERT_EQ(rig.alter("characters", {setCompatOp(CompatMode::Forward)}).code, 0);
    rig.restart();
    for (uint16_t v = 1; v <= 3; ++v)
        ASSERT_NE(rig.schemas().byIdVersion(a.topicId, v), nullptr) << "version " << v << " replays";
    EXPECT_EQ(rig.schemas().latest(a.topicId)->version, 3u);
    EXPECT_EQ(rig.schemas().nextTagFor(a.topicId), 4) << "nextTag past the ALTER's tag";
    EXPECT_EQ(rig.schemas().latest(a.topicId)->compatMode, CompatMode::Forward);
}

// G9-14: an event past the HW (appended, never committed) is never applied —
// neither live nor by the boot replay (whose bound is the checkpoint, ruling 8).
TEST(SchemaPersistenceHwGating, RecordPastHighWatermarkExcludedFromReplayRegression) {
    CatalogDdlRig rig("sp_hw", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", "userId", 4, false, charsBlob()).code, 0u);
    rig.becomeServingLeader({1, 2});   // follower 2 silent
    rune::codec::AlterOp op = addOp("score", PrimType::Int64);
    AlterTopicMsg m; m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.topicName = "characters";
    m.opsBlob = rune::codec::encodeAlterOps({op}); m.meta.requestId = rig.freshRequestId();
    (void)rig.submitRawNoWait(DdlRequest{m});
    for (int i = 0; i < 32; ++i) rig.pump();
    EXPECT_EQ(rig.recordedLog().size(), 2u) << "the ALTER's event is in the log";
    EXPECT_EQ(rig.schemas().byName("characters")->version, 1u) << "but never applied past the HW";
    rig.restart();
    EXPECT_EQ(rig.schemas().byName("characters")->version, 1u) << "nor by the boot replay";
}

// G27-32: data records written under every schema version decode per their OWN
// writer version after a restart (the versions come back from __catalog).
TEST(SchemaPersistenceTest, MixedDataAndSchemaReplay_RecordsDecodePerWriterVersionRegression) {
    CatalogDdlRig rig("sp_mixed", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "events", "id", 1, false, serialize(DescriptorBuilder("events", 1)
        .add("id",     PrimType::Int64, 1, true)
        .add("amount", PrimType::Int64)
        .build()));
    ASSERT_EQ(a.code, 0u);
    const int32_t topicId = a.topicId;
    ASSERT_EQ(rig.alter("events", {addOp("bonus", PrimType::Int64)}).version, 2u);
    ASSERT_EQ(rig.alter("events", {dropOp("amount")}).version, 3u);

    constexpr uint16_t kIdTag = 0, kAmountTag = 1, kBonusTag = 2;
    const std::vector<uint8_t> r1 = encodeFixedRecord(1, 16, {{0, 8, 111}, {8, 8, 222}});
    const std::vector<uint8_t> r2 = encodeFixedRecord(2, 24, {{0, 8, 333}, {8, 8, 444}, {16, 8, 555}});
    const std::vector<uint8_t> r3 = encodeFixedRecord(3, 16, {{0, 8, 777}, {8, 8, 888}});

    rig.restart();
    const RegistryEntry* e1 = rig.schemas().byIdVersion(topicId, 1);
    const RegistryEntry* e2 = rig.schemas().byIdVersion(topicId, 2);
    const RegistryEntry* e3 = rig.schemas().byIdVersion(topicId, 3);
    ASSERT_NE(e1, nullptr); ASSERT_NE(e2, nullptr); ASSERT_NE(e3, nullptr);

    auto dec = [](const RegistryEntry* e, const std::vector<uint8_t>& r, uint16_t tag) {
        return readField(e->descriptor, std::span<const uint8_t>(r.data(), r.size()), tag);
    };
    EXPECT_EQ(dec(e1, r1, kIdTag).i, 111);
    EXPECT_EQ(dec(e1, r1, kAmountTag).i, 222);
    EXPECT_FALSE(dec(e1, r1, kBonusTag).present) << "bonus does not exist yet at v1";
    EXPECT_EQ(dec(e2, r2, kIdTag).i, 333);
    EXPECT_EQ(dec(e2, r2, kAmountTag).i, 444);
    EXPECT_EQ(dec(e2, r2, kBonusTag).i, 555);
    EXPECT_EQ(dec(e3, r3, kIdTag).i, 777);
    EXPECT_FALSE(dec(e3, r3, kAmountTag).present) << "amount was dropped at v3 — absent, never garbage";
    EXPECT_EQ(dec(e3, r3, kBonusTag).i, 888);
    // The contrast: the WRONG (v2) descriptor misreads r3 — decode must use the writer's version.
    const auto wrong = dec(e2, r3, kAmountTag);
    ASSERT_TRUE(wrong.present);
    EXPECT_EQ(wrong.i, 888);
}

// G51-56: replay idempotence — two restarts replay to byte-identical registries.
TEST(SchemaPersistenceTest, ReplayTwiceYieldsByteIdenticalRegistryStateRegression) {
    CatalogDdlRig rig("sp_twice", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", "userId", 4, false, charsBlob());
    ASSERT_EQ(a.code, 0u);
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).version, 2u);
    ASSERT_EQ(rig.alter("characters", {setCompatOp(CompatMode::Forward)}).version, 3u);
    const SchemaRegistry live = rig.schemas();
    rig.restart();
    const SchemaRegistry replayA = rig.schemas();
    rig.restart();
    expectRegistriesByteIdenticalForAllVersions(replayA, rig.schemas(), a.topicId, 3);
    expectRegistriesByteIdenticalForAllVersions(live, rig.schemas(), a.topicId, 3);
}

// GC5 / the composite-key regression, catalog form: __catalog is NEVER compacted
// or retention-deleted (spec §3), so every version of a long ADD / DROP / RENAME
// lineage survives a restart, resolves by its fingerprint, and decodes its own
// writer version's records.
TEST(SchemaPersistenceCompaction, CatalogIsNeverCompactedSoEveryVersionSurvivesRegression) {
    NodeConfig nc;
    const auto cfg = makeCatalogPartitionConfig(nc, 3);
    EXPECT_EQ(cfg.cleanupPolicy, CleanupPolicy::Delete);
    EXPECT_EQ(cfg.retentionMs, -1);
    EXPECT_EQ(cfg.retentionBytes, -1);

    CatalogDdlRig rig("sp_lineage", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "lineage", "id", 1, false, serialize(DescriptorBuilder("lineage", 1)
        .add("id", PrimType::Int64, 1, true).add("amount", PrimType::Int64).add("label", PrimType::Int64).build()));
    ASSERT_EQ(a.code, 0u);
    const std::vector<std::vector<AlterOp>> steps = {
        {addOp("score", PrimType::Int64)}, {addOp("bonus", PrimType::Int64)}, {dropOp("amount")},
        {renameOp("label", "title")},      {addOp("flag", PrimType::Int64)},  {dropOp("score")},
        {renameOp("title", "caption")},    {addOp("extra", PrimType::Int64)}, {dropOp("bonus")},
        {renameOp("caption", "headline")}, {addOp("final", PrimType::Int64)}};
    for (const auto& ops : steps) ASSERT_EQ(rig.alter("lineage", ops).code, 0);
    std::vector<uint64_t> fps;
    for (uint16_t v = 1; v <= 12; ++v) fps.push_back(rig.schemas().byIdVersion(a.topicId, v)->fingerprint);

    rig.restart();
    for (uint16_t v = 1; v <= 12; ++v) {
        const RegistryEntry* e = rig.schemas().byIdVersion(a.topicId, v);
        ASSERT_NE(e, nullptr) << "version " << v << " survived";
        EXPECT_EQ(e->fingerprint, fps[v - 1]);
        EXPECT_EQ(rig.schemas().byFingerprint(a.topicId, fps[v - 1]), e) << "v" << v << " resolves by fingerprint";
        const std::vector<uint8_t> rec = encodeRecordForEntry(*e);
        for (const auto& f : e->descriptor.fields)
            EXPECT_EQ(readField(e->descriptor, std::span<const uint8_t>(rec.data(), rec.size()), f.id).i,
                      static_cast<int64_t>(v) * 1000 + f.id) << "v" << v << " tag " << f.id;
    }
    EXPECT_EQ(rig.recordedLog().size(), 12u) << "one event per version, none lost or duplicated";
}

// Final review I2, catalog form: boot claims a topic's dir under the topic's OWN
// name (never its descriptor's) — and a view sink under the view's name; a
// dropped topic is not claimed (its dir is quarantined: a reused name starts empty).
TEST(SchemaPersistenceTest, DefineTopicRecordRecoversTopicUnderItsOwnNameRegression) {
    CatalogDdlRig rig("sp_ownname", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "typed_route", "userId", 3, false, charsBlob()).code, 0u);   // descriptor "characters"
    ASSERT_EQ(rig.define({.name = "gone", .partitions = 1}).code, 0);
    ASSERT_EQ(rig.define({.name = "src", .partitions = 1, .typed = true}).code, 0);
    ASSERT_EQ(rig.createView("v_src", "src").code, 0);
    ASSERT_EQ(rig.dropTopic("gone").code, 0);
    const std::string dataDir = fs::path(rig.catalogDir()).parent_path().string();
    const auto specs = catalogLiveTopicSpecs(dataDir);
    std::vector<std::string> names;
    for (const auto& s : specs) names.push_back(s.name);
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"src", "typed_route", "v_src"}));
    for (const auto& s : specs)
        if (s.name == "typed_route") { EXPECT_EQ(s.partitionCount, 3); EXPECT_EQ(s.topicId, 0); }
}

// ---------------------------------------------------------------------------
// Final-review Regression (Finding 2): CompatMode2RejectedRegression
//
// TopicSchema::deserialize's trailing (nextTag, compatMode) tail previously
// static_cast the raw compatMode byte straight into rune::codec::CompatMode
// with no range check — a byte outside {0=Backward, 1=Forward} was silently
// accepted as a well-formed (if semantically bogus) CompatMode. This test
// proves a byte of 2 (neither enumerator) is now rejected, same as every
// other bounds/shape check in deserialize().
// ---------------------------------------------------------------------------
TEST(TopicSchemaDeserializeRegression, CompatMode2RejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 1;
    ts.version        = 1;
    ts.partitionCount = 1;
    ts.validate       = false;
    ts.keyField       = "id";
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    // Deliberately malformed: neither CompatMode::Backward(0) nor
    // CompatMode::Forward(1) — bypass the enum's type safety the same way a
    // corrupt/garbled on-disk byte would.
    ts.compatMode     = static_cast<CompatMode>(2);

    const std::vector<uint8_t> blob = ts.serialize();
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(blob.data(), blob.size()));

    EXPECT_FALSE(decoded.has_value())
        << "Regression: compatMode byte=2 (neither Backward(0) nor Forward(1)) must be "
           "REJECTED by deserialize(), not silently accepted as a valid CompatMode";
}

// ---------------------------------------------------------------------------
// GC4 (crash-recovery matrix): truncation points -- unit level.
//
// CompatMode2RejectedRegression above proves a MALFORMED trailing byte is
// rejected; these three prove deserialize() rejects a record TRUNCATED at
// each of the three distinct variable-length regions of the wire layout
// (see the header comment on TopicSchema in SchemaRegistry.h) -- inside the
// keyField bytes, inside the descriptorBlob bytes, and inside the trailing
// (nextTag, compatMode) tail -- not just the gross few-byte garbage payload
// the integration-level CorruptRecordAbortsRebuildRegression/
// CorruptRecordAbortsBootReplayRegression tests use (that garbage is
// truncated inside the very FIRST field, topicId). Each case must recover
// NOTHING (nullopt), never a partial/garbled TopicSchema.
// ---------------------------------------------------------------------------
TEST(TopicSchemaDeserializeRegression, TruncatedInsideKeyFieldRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";  // 6 bytes
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    ts.compatMode     = CompatMode::Backward;

    const std::vector<uint8_t> full = ts.serialize();

    // Fixed header (topicId+version+partitionCount+validate) = 11 bytes,
    // + u16 keyLen prefix = 13 -- keyField bytes start there and run for
    // keyField.size() (6) bytes. Truncate to 3 of the 6 keyField bytes: the
    // keyLen prefix promises 6 bytes but only 3 are actually present.
    constexpr size_t kKeyFieldStart = 13;
    const size_t truncateLen = kKeyFieldStart + 3;
    ASSERT_LT(truncateLen, full.size()) << "vacuousness guard: must actually truncate";

    const std::vector<uint8_t> truncated(full.begin(), full.begin() + truncateLen);
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(truncated.data(), truncated.size()));
    EXPECT_FALSE(decoded.has_value())
        << "Regression: a record truncated MID-keyField (keyLen prefix promises more "
           "bytes than are present) must be rejected, never partially decoded";
}

TEST(TopicSchemaDeserializeRegression, TruncatedInsideDescriptorBlobRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";
    ts.descriptorBlob = serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build());
    ts.nextTag        = 3;
    ts.compatMode     = CompatMode::Backward;

    const std::vector<uint8_t> full = ts.serialize();

    constexpr size_t kKeyFieldStart = 13;
    const size_t descLenPos    = kKeyFieldStart + ts.keyField.size();      // 19
    const size_t descBlobStart = descLenPos + 4;                          // 23
    ASSERT_GT(ts.descriptorBlob.size(), 4u)
        << "vacuousness guard: need a descriptorBlob big enough to truncate mid-blob";
    const size_t truncateLen = descBlobStart + (ts.descriptorBlob.size() / 2);
    ASSERT_LT(truncateLen, full.size()) << "vacuousness guard: must actually truncate";

    const std::vector<uint8_t> truncated(full.begin(), full.begin() + truncateLen);
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(truncated.data(), truncated.size()));
    EXPECT_FALSE(decoded.has_value())
        << "Regression: a record truncated MID-descriptorBlob (descLen prefix promises "
           "more bytes than are present) must be rejected, never partially decoded";
}

TEST(TopicSchemaDeserializeRegression, TruncatedInsideTrailingTailRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    ts.compatMode     = CompatMode::Forward;  // non-default, so a wrong decode would be visible

    const std::vector<uint8_t> full = ts.serialize();
    ASSERT_GE(full.size(), 3u);
    const size_t tailStart = full.size() - 3;  // nextTag(2) + compatMode(1)

    // remaining == 1: only nextTag's first byte present.
    {
        const std::vector<uint8_t> truncated(full.begin(), full.begin() + tailStart + 1);
        const auto decoded = TopicSchema::deserialize(
            std::span<const uint8_t>(truncated.data(), truncated.size()));
        EXPECT_FALSE(decoded.has_value())
            << "Regression: a tail truncated to 1 leftover byte (mid-nextTag) must be rejected";
    }
    // remaining == 2: nextTag fully present, compatMode byte missing.
    {
        const std::vector<uint8_t> truncated(full.begin(), full.begin() + tailStart + 2);
        const auto decoded = TopicSchema::deserialize(
            std::span<const uint8_t>(truncated.data(), truncated.size()));
        EXPECT_FALSE(decoded.has_value())
            << "Regression: a tail truncated to 2 leftover bytes (nextTag present, "
               "compatMode byte missing) must be rejected";
    }
}
