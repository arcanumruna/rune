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

#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/node/SchemaRegistry.h>
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
                                  const char* topicName, const char* descriptorName) {
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = version;
    ts.partitionCount = partitions;
    ts.keyField       = "characterId";
    ts.descriptorBlob = rune::codec::serialize(charDescriptor(descriptorName));
    ts.nextTag        = 2;
    ts.topicName      = topicName;
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

// TopicRegistry assigns ids sequentially, so a hole would shift every later topic
// onto another topic's id (wrong log, wrong assignments).  Fail loud instead.
TEST(RuntimeTopicRecoveryRegression, IdGapFailsLoud) {
    TempDataDir d("gap");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "zero", "d0"), schemaRecord(2, 1, 1, "two", "d2")});
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
