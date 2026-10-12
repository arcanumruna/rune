// libs/node/tests/node/RuntimeTopicRecoveryTest.cpp
//
// Boot-time recovery of RUNTIME-defined topics (DefineTopic + CreateView sinks)
// from the durable admin catalogs (__topic_schemas, __views).  Deterministic,
// in-process: real rune::storage::Log files in a temp dir, written exactly the
// way the AdminWorker writes them (one keyed record per batch), then read back
// by recoverRuntimeTopicSpecs.
//
// Why: the live docker e2e showed a restarted broker re-creates ONLY the static
// config topics — every DefineTopic topic and view sink vanished as a runtime
// replica (read_committed rebuilt from 0 topics), so views could never re-fold
// their source after a restart.
#include <gtest/gtest.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/TopicName.h>
#include <rune/node/ViewRegistry.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using rune::node::TopicSchema;
using rune::node::TopicSpec;
using rune::node::ViewRecord;

namespace {

// A fresh, empty data dir per test (removed on destruction).
struct TempDataDir {
    fs::path path;
    explicit TempDataDir(const char* name)
        : path(fs::temp_directory_path() / ("rune-rtr-" + std::string(name))) {
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDataDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Append each value as its own single-record keyed batch, the way the
// AdminWorker appends DDL records (the key is ignored by recovery).
void appendRecords(const fs::path& dir, const std::vector<std::vector<uint8_t>>& values) {
    fs::create_directories(dir);
    rune::storage::Log log(dir.string());
    log.recover();
    int64_t off = log.logEndOffset();
    for (const auto& v : values) {
        rune::protocol::RecordBatchBuilder b(off, /*baseTimestamp*/ 0);
        const std::string key = "k";
        b.append(std::string_view(key),
                 std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
        log.append(b.build(), off);
        ++off;
    }
}

rune::codec::Descriptor charDescriptor(const char* name) {
    return rune::codec::DescriptorBuilder(name, 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

// A DefineTopic schema record.  topicName may differ from the descriptor name.
std::vector<uint8_t> schemaRecord(int32_t topicId, uint16_t version, int32_t partitions,
                                  const char* topicName, const char* descriptorName,
                                  rune::node::PersistenceMode persistence = rune::node::PersistenceMode::Disk) {
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = version;
    ts.partitionCount = partitions;
    ts.keyField       = "characterId";
    ts.descriptorBlob = rune::codec::serialize(charDescriptor(descriptorName));
    ts.nextTag        = 2;
    ts.topicName      = topicName;
    ts.persistence    = persistence;
    return ts.serialize();
}

// A CreateView catalog record for a {characterId, hp} Replace view over sourceTopicId.
std::vector<uint8_t> viewRecord(int32_t viewTopicId, const char* viewName, int32_t sourceTopicId) {
    const auto src = charDescriptor("character");
    const rune::codec::Descriptor* srcs[] = {&src};
    rune::view::ViewPlan v;
    v.viewName    = viewName;
    v.key         = "characterId";
    v.sources     = {{sourceTopicId, "characterId"}};
    v.output      = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId",
                      rune::view::MergeRule::Replace},
                     {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 0;
    auto comp = rune::view::compileView(v, srcs, viewTopicId).value();

    ViewRecord rec;
    rec.viewTopicId          = viewTopicId;
    rec.viewName             = viewName;
    rec.compiledPlanBlob     = rune::view::serialize(comp.plan);
    rec.persistence          = 0;
    rec.outputDescriptorBlob = rune::codec::serialize(comp.outputDescriptor);
    return rec.serialize();
}

} // namespace

TEST(RuntimeTopicRecoveryRegression, NoCatalogsRecoversNothingAndCreatesNothing) {
    TempDataDir d("empty");
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), /*firstRuntimeId*/ 0);
    EXPECT_TRUE(specs.empty());
    EXPECT_FALSE(fs::exists(d.path / "__topic_schemas")) << "recovery must not create catalog dirs";
    EXPECT_FALSE(fs::exists(d.path / "__views"));
}

// The e2e shape: DefineTopic 'character' (id 0) + a view sink (id 1) over it.
// The topic is recreated under its DURABLE topic name, not the descriptor name,
// and the sink is a keyed Compact table with the source's partition count —
// exactly the TopicSpecs the original createTopic calls used.
TEST(RuntimeTopicRecoveryRegression, RecoversDefinedTopicAndViewSinkInIdOrder) {
    TempDataDir d("defined-and-view");
    appendRecords(d.path / "__views", {viewRecord(1, "char_hp", /*source*/ 0)});
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, /*partitions*/ 3, "character", "character_descriptor")});

    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 2u);

    EXPECT_EQ(specs[0].topicId, 0);
    EXPECT_EQ(specs[0].name, "character");
    EXPECT_EQ(specs[0].partitionCount, 3);
    EXPECT_EQ(specs[0].cleanupPolicy, TopicSpec::CleanupPolicy::Delete);
    EXPECT_TRUE(specs[0].partitionKey.empty()) << "DefineTopic creates with no partition key";

    EXPECT_EQ(specs[1].topicId, 1);
    EXPECT_EQ(specs[1].name, "char_hp");
    EXPECT_EQ(specs[1].partitionCount, 3) << "a view sink mirrors its source's partitions";
    EXPECT_EQ(specs[1].cleanupPolicy, TopicSpec::CleanupPolicy::Compact);
    EXPECT_EQ(specs[1].partitionKey, "characterId");
}

// An ALTERed topic has several schema versions in the catalog; it is still ONE topic.
TEST(RuntimeTopicRecoveryRegression, AlteredTopicYieldsOneSpec) {
    TempDataDir d("altered");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "evolving", "desc_v1"),
                   schemaRecord(0, 2, 1, "evolving", "desc_v2")});

    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].name, "evolving");
}

// Ids below firstRuntimeId belong to STATIC config topics (DefineTopic on a static
// name resolves to the static id); those are already booted from config and must
// not be duplicated.
TEST(RuntimeTopicRecoveryRegression, StaticTopicIdsAreSkipped) {
    TempDataDir d("static-skip");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(1, 1, 1, "static_one", "d1"),
                   schemaRecord(2, 1, 1, "runtime_two", "d2")});

    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), /*firstRuntimeId*/ 2);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].topicId, 2);
    EXPECT_EQ(specs[0].name, "runtime_two");
}

// B5 (DDL conformance, user decision D1): an id with no catalog record is a topic
// whose DefineTopic/CreateView never committed (an orphan: a failed CREATE VIEW's
// sink, a create whose reply was lost).  Refusing to boot made a failed CREATE VIEW
// an outage.  The gap is filled with a hidden, inert placeholder: ZERO partitions
// (no replica, no log, no metadata, no data path), a reserved "__" name.  Every
// recorded topic keeps its id and the orphan's name is free again.
TEST(RuntimeTopicRecoveryRegression, IdGapIsFilledWithInertPlaceholderRegression) {
    TempDataDir d("gap");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(3, 1, 2, "three", "d3")});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 4u);
    EXPECT_EQ(specs[0].name, "zero");
    for (int32_t id : {1, 2}) {
        SCOPED_TRACE(id);
        EXPECT_EQ(specs[id].topicId, id);
        EXPECT_EQ(specs[id].name, rune::node::orphanPlaceholderName(id));
        EXPECT_EQ(specs[id].partitionCount, 0) << "no replica, no log, no metadata entry";
        EXPECT_EQ(specs[id].persistence, rune::node::PersistenceMode::Transient) << "never a disk dir";
    }
    EXPECT_EQ(specs[3].topicId, 3);
    EXPECT_EQ(specs[3].name, "three");
    EXPECT_EQ(specs[3].partitionCount, 2);
}

// A gap that starts at firstRuntimeId (the first runtime topic was the orphan).
TEST(RuntimeTopicRecoveryRegression, GapAtFirstRuntimeIdIsFilledRegression) {
    TempDataDir d("gap-first");
    appendRecords(d.path / "__views", {viewRecord(3, "char_hp", /*source*/ 0)});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), /*firstRuntimeId*/ 2);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_EQ(specs[0].topicId, 2);
    EXPECT_EQ(specs[0].name, rune::node::orphanPlaceholderName(2));
    EXPECT_EQ(specs[0].partitionCount, 0);
    EXPECT_EQ(specs[1].topicId, 3);
    EXPECT_EQ(specs[1].name, "char_hp");
}

// The placeholder name is reserved: no DDL can ever create or address it.
TEST(RuntimeTopicRecoveryRegression, PlaceholderNameIsReservedRegression) {
    EXPECT_EQ(rune::node::orphanPlaceholderName(7), "__orphan.7");
    EXPECT_FALSE(rune::node::isValidTopicName(rune::node::orphanPlaceholderName(7)));
}

// D1: placeholders are logged ONCE per boot (one line naming every id), not per id.
TEST(RuntimeTopicRecoveryRegression, PlaceholdersAreLoggedOnceRegression) {
    TempDataDir d("gap-log");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(4, 1, 1, "four", "d4")});
    auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    const std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    spdlog::set_default_logger(std::make_shared<spdlog::logger>("rtr-capture", sink));
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    spdlog::set_default_logger(previous);
    ASSERT_EQ(specs.size(), 5u);
    int lines = 0;
    std::string theLine;
    for (const std::string& l : sink->last_formatted()) {
        if (l.find("__orphan.") != std::string::npos) { ++lines; theLine = l; }
    }
    EXPECT_EQ(lines, 1) << "one warn line per boot";
    for (const char* id : {"__orphan.1", "__orphan.2", "__orphan.3"})
        EXPECT_NE(theLine.find(id), std::string::npos) << id << " named in: " << theLine;
}

// D1 keeps the loud failure for a catalog DISAGREEMENT: an id claimed by both
// __topic_schemas and __views is not a gap and must still refuse to boot.
TEST(RuntimeTopicRecoveryRegression, IdClaimedByBothCatalogsStillFailsLoudRegression) {
    TempDataDir d("both-catalogs");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(1, 1, 1, "one", "d1")});
    appendRecords(d.path / "__views", {viewRecord(1, "clash", /*source*/ 0)});
    EXPECT_THROW((void)rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0), std::runtime_error);
}

// A schema record written before the topicName trailer existed recovers under
// the descriptor name (the only name it carries).
TEST(RuntimeTopicRecoveryRegression, LegacyRecordFallsBackToDescriptorName) {
    TempDataDir d("legacy");
    appendRecords(d.path / "__topic_schemas", {schemaRecord(0, 1, 1, /*topicName*/ "", "legacy_desc")});

    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].name, "legacy_desc");
}

// A record that does not parse is a damaged catalog: refuse to boot on a guess.
TEST(RuntimeTopicRecoveryRegression, CorruptCatalogRecordFailsLoud) {
    TempDataDir d("corrupt");
    appendRecords(d.path / "__topic_schemas", {std::vector<uint8_t>{0x01, 0x02, 0x03}});
    EXPECT_THROW((void)rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0), std::runtime_error);
}

// Phase 2b E: a transient runtime topic is recreated TRANSIENT (metadata
// survives, data does not — the TopicRegistry/InMemoryLog path starts it empty).
TEST(RuntimeTopicRecoveryRegression, TransientTopicIsRecoveredTransient) {
    TempDataDir d("transient");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 2, "presence", "presence_desc",
                                rune::node::PersistenceMode::Transient),
                   schemaRecord(1, 1, 1, "orders", "orders_desc")});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_EQ(specs[0].name, "presence");
    EXPECT_EQ(specs[0].persistence, rune::node::PersistenceMode::Transient);
    EXPECT_EQ(specs[1].persistence, rune::node::PersistenceMode::Disk);
}

TEST(RuntimeTopicRecoveryRegression, AlteredTransientTopicStaysTransient) {
    TempDataDir d("transient-altered");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "presence", "d1", rune::node::PersistenceMode::Transient),
                   schemaRecord(0, 2, 1, "presence", "d2", rune::node::PersistenceMode::Transient)});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].persistence, rune::node::PersistenceMode::Transient);
}
