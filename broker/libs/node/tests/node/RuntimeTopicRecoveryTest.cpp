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

#include <rune/gateway/TopicRegistry.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewPlan.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <utility>
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

// ── B10 / user decision D4 (amended): boot deletes ONLY orphan-placeholder topic
//    dirs; every other unclaimed topic dir is QUARANTINED (moved, never deleted). ──
namespace {
void touchFile(const fs::path& p, std::string_view content = "x") {
    fs::create_directories(p.parent_path());
    std::ofstream(p) << content;
}
TopicSpec spec(const std::string& name, int32_t id, int32_t partitions = 1) {
    TopicSpec t; t.name = name; t.topicId = id; t.partitionCount = partitions; return t;
}
TopicSpec placeholder(int32_t id) {
    TopicSpec t = spec(rune::node::orphanPlaceholderName(id), id, 0);
    t.persistence = rune::node::PersistenceMode::Transient;
    return t;
}
constexpr int64_t kNow = 1'760'000'000;   // a fixed unix timestamp
std::vector<uint8_t> oneRecordBatch(std::string_view value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, value, 0);
    return b.build();
}
int32_t readMarker(const fs::path& topicDir) {
    std::ifstream in(topicDir / rune::gateway::kTopicIdMarkerFile);
    int32_t id = -1;
    in >> id;
    return id;
}
} // namespace

// The marker is written by a RUNTIME create (DefineTopic / CreateView sink) of a disk
// topic only.  Boot-built topics (static RUNE_TOPICS + recovered) get none: a
// removed static topic's id can become a placeholder gap, and its data must then be
// quarantined, never deleted (see RemovedLastStaticTopicWithRuntimeTopics...).
TEST(RuntimeTopicRecoveryRegression, TopicRegistryWritesTheTopicIdMarkerRegression) {
    TempDataDir d("marker");
    {
        rune::gateway::TopicRegistry reg({spec("events", 0, 2)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("runtime_disk", -1, 2)), 1);
        TopicSpec t = spec("later", -1); t.persistence = rune::node::PersistenceMode::Transient;
        (void)reg.createTopic(t);   // transient: no dir, no marker
        (void)reg.createTopic(spec("runtime_disk", -1, 2));   // idempotent: marker unchanged
    }
    EXPECT_EQ(readMarker(d.path / "topics" / "runtime_disk"), 1);
    EXPECT_FALSE(fs::exists(d.path / "topics" / "events" / rune::gateway::kTopicIdMarkerFile))
        << "a boot-built (static/recovered) topic is never marked";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "events" / "1"));
    EXPECT_FALSE(fs::exists(d.path / "topics" / "later"));
}

TEST(RuntimeTopicRecoveryRegression, OrphanPlaceholderDirIsDeletedRegression) {
    TempDataDir d("orphan-dir");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "00000000000000000000.log");
    touchFile(d.path / "topics" / "live" / "topic-id", "0\n");
    touchFile(d.path / "topics" / "live" / "0" / "00000000000000000000.log");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(), {spec("live", 0), spec("other", 1), placeholder(2), spec("later", 3)}, kNow);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0], fs::canonical(d.path / "topics") / "orphan_v");
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v"));
    EXPECT_FALSE(fs::exists(d.path / "quarantine")) << "nothing quarantined, nothing created";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "live" / "0" / "00000000000000000000.log"));
}

// The whole B5 + B10 flow on real files: a runtime create whose record never
// committed (the orphan) and a recorded one; the next boot recovers a placeholder
// for the orphan's id, deletes ONLY the orphan's dir, and boots the registry.
TEST(RuntimeTopicRecoveryRegression, FailedCreateOrphanDirIsDeletedEndToEndRegression) {
    TempDataDir d("orphan-e2e");
    {   // boot 1: static "events" (id 0); runtime "orphan_v" (id 1, never recorded), "kept" (id 2)
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("orphan_v", -1)), 1);
        ASSERT_EQ(reg.createTopic(spec("kept", -1)), 2);
        reg.log(1, 0)->append(oneRecordBatch("orphan-data"), 0);
        reg.log(2, 0)->append(oneRecordBatch("kept-data"), 0);
    }
    appendRecords(d.path / "__topic_schemas", {schemaRecord(2, 1, 1, "kept", "kept_desc")});
    // boot 2
    std::vector<TopicSpec> all{spec("events", 0)};
    for (auto& s : rune::node::recoverRuntimeTopicSpecs(d.path.string(), 1)) all.push_back(std::move(s));
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[1].name, rune::node::orphanPlaceholderName(1));
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), all, kNow);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0].filename(), "orphan_v");
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v"));
    rune::gateway::TopicRegistry reg(all, d.path.string());
    EXPECT_EQ(reg.topicIdForName("kept"), 2) << "later topics keep their ids";
    EXPECT_EQ(reg.log(2, 0)->logEndOffset(), 1) << "the recorded topic's data is intact";
    EXPECT_EQ(reg.log(1, 0), nullptr) << "the placeholder has no partition";
    EXPECT_FALSE(fs::exists(d.path / "topics" / rune::node::orphanPlaceholderName(1)));
}

// D4 hazard guard: removing the LAST static topic while runtime topics exist turns its
// id into a recovery gap (an __orphan.<id> placeholder).  Its data must be
// QUARANTINED, never deleted — boot-built topics carry no marker for exactly this.
TEST(RuntimeTopicRecoveryRegression, RemovedLastStaticTopicWithRuntimeTopicsIsQuarantinedNotDeletedRegression) {
    TempDataDir d("static-gap");
    {   // boot 1: RUNE_TOPICS=a,b (ids 0,1); runtime "c" (id 2, recorded)
        rune::gateway::TopicRegistry reg({spec("a", 0), spec("b", 1)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("c", -1)), 2);
        reg.log(1, 0)->append(oneRecordBatch("static-b-data"), 0);
    }
    appendRecords(d.path / "__topic_schemas", {schemaRecord(2, 1, 1, "c", "c_desc")});
    // boot 2: RUNE_TOPICS=a -> firstRuntimeId 1, id 1 is now a gap
    std::vector<TopicSpec> all{spec("a", 0)};
    for (auto& s : rune::node::recoverRuntimeTopicSpecs(d.path.string(), 1)) all.push_back(std::move(s));
    ASSERT_EQ(all.size(), 3u);
    ASSERT_EQ(all[1].name, rune::node::orphanPlaceholderName(1));
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), all, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "a removed STATIC topic is never deleted";
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("b-" + std::to_string(kNow)) / "0"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "c" / "0"));
}

TEST(RuntimeTopicRecoveryRegression, RemovedStaticTopicIsQuarantinedIntactAndNotAutoRestoredRegression) {
    TempDataDir d("quarantine");
    {   // boot 1: RUNE_TOPICS=events:1 — one record written
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        rune::storage::Log* log = reg.log(0, 0);
        ASSERT_NE(log, nullptr);
        log->append(oneRecordBatch("kept-record"), 0);
    }
    // boot 2: events removed from RUNE_TOPICS -> quarantined, NOT deleted
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "a removed static topic is never deleted";
    ASSERT_EQ(r.quarantined.size(), 1u);
    const fs::path q = d.path / "quarantine" / ("events-" + std::to_string(kNow));
    EXPECT_EQ(fs::canonical(r.quarantined[0]), fs::canonical(q));
    EXPECT_FALSE(fs::exists(d.path / "topics" / "events"));
    {   // the data is intact and readable where it was moved
        rune::storage::Log moved((q / "0").string());
        moved.recover();
        EXPECT_EQ(moved.logEndOffset(), 1);
        const std::vector<uint8_t> raw = moved.read(0, 1 << 20);
        const std::string bytes(raw.begin(), raw.end());
        EXPECT_NE(bytes.find("kept-record"), std::string::npos);
    }
    {   // boot 3: events re-added — a FRESH empty topic; the quarantine is untouched
        const auto again = rune::node::reconcileTopicDirs(d.path.string(), {spec("events", 0)}, kNow + 60);
        EXPECT_TRUE(again.deleted.empty());
        EXPECT_TRUE(again.quarantined.empty());
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        EXPECT_EQ(reg.log(0, 0)->logEndOffset(), 0) << "re-adding a topic does NOT auto-restore it";
        EXPECT_TRUE(fs::exists(q / "0")) << "the quarantined copy stays until an operator restores it";
    }
}

TEST(RuntimeTopicRecoveryRegression, UnmarkedOrForeignIdDirIsQuarantinedNotDeletedRegression) {
    TempDataDir d("foreign");
    touchFile(d.path / "topics" / "typo_name" / "0" / "seg.log");               // no marker (old build)
    touchFile(d.path / "topics" / "other_build" / "topic-id", "7\n");           // id 7 is not a placeholder
    touchFile(d.path / "topics" / "other_build" / "0" / "seg.log");
    touchFile(d.path / "topics" / "garbage_id" / "topic-id", "not-a-number");
    touchFile(d.path / "topics" / "garbage_id" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("x", 7)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_EQ(r.quarantined.size(), 3u);
    for (const char* n : {"typo_name", "other_build", "garbage_id"}) {
        EXPECT_FALSE(fs::exists(d.path / "topics" / n)) << n;
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(n) + "-" + std::to_string(kNow)) / "0" / "seg.log"))
            << n;
    }
}

// A delete is never decided on a guess: a marker that is not EXACTLY "<digits>[\n]"
// is no marker, even when its leading number names a placeholder.
TEST(RuntimeTopicRecoveryRegression, MalformedMarkerNeverDeletesRegression) {
    TempDataDir d("bad-marker");
    const std::pair<const char*, const char*> cases[] = {
        {"trailing_junk", "2abc"}, {"leading_space", " 2"}, {"two_lines", "2\n2\n"},
        {"negative", "-2"}, {"overflow", "99999999999"}, {"empty_marker", ""}};
    for (const auto& [name, content] : cases) {
        touchFile(d.path / "topics" / name / "topic-id", content);
        touchFile(d.path / "topics" / name / "0" / "seg.log");
    }
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_EQ(r.quarantined.size(), std::size(cases));
    for (const auto& [name, content] : cases)
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(name) + "-" + std::to_string(kNow)) / "0" / "seg.log"))
            << name;
}

TEST(RuntimeTopicRecoveryRegression, QuarantineNameCollisionGetsASuffixRegression) {
    TempDataDir d("q-collide");
    fs::create_directories(d.path / "quarantine" / ("gone-" + std::to_string(kNow)));
    touchFile(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-1"));   // a FILE also collides
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-2") / "0" / "seg.log"));
    EXPECT_TRUE(fs::is_empty(d.path / "quarantine" / ("gone-" + std::to_string(kNow))))
        << "an existing quarantine entry is never overwritten";
}

TEST(RuntimeTopicRecoveryRegression, QuarantinePathThatIsAFileMovesNothingRegression) {
    TempDataDir d("q-file");
    touchFile(d.path / "quarantine", "not a dir");
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log"));
    EXPECT_TRUE(fs::is_regular_file(d.path / "quarantine"));
}

TEST(RuntimeTopicRecoveryRegression, NonTopicEntriesInTopicsDirAreLeftAloneRegression) {
    TempDataDir d("non-topic");
    touchFile(d.path / "topics" / "README");                        // a plain file
    touchFile(d.path / "topics" / "notes" / "todo.txt");            // files directly under <name>/
    touchFile(d.path / "topics" / "weird" / "abc" / "seg.log");     // non-numeric partition dir
    touchFile(d.path / "topics" / "nested" / "0" / "deeper" / "x"); // a sub-dir inside a partition
    fs::create_directories(d.path / "topics" / "empty");            // no partition dir at all
    touchFile(d.path / "elsewhere" / "0" / "seg.log");              // outside topics/: never scanned
    touchFile(d.path / "elsewhere" / "topic-id", "2\n");            // ...even if it looks like an orphan
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    for (const char* p : {"topics/README", "topics/notes/todo.txt", "topics/weird/abc/seg.log",
                          "topics/nested/0/deeper/x", "topics/empty", "elsewhere/0/seg.log"})
        EXPECT_TRUE(fs::exists(d.path / p)) << p;
}

// Nothing outside dataDir/topics is touched: a placeholder-marked dir that sits next
// to topics/ (e.g. an internal catalog) and the dataDir's other children survive
// a reconcile that deletes AND quarantines.
TEST(RuntimeTopicRecoveryRegression, NothingOutsideTopicsDirIsTouchedRegression) {
    TempDataDir d("outside");
    touchFile(d.path / "__views" / "topic-id", "2\n");
    touchFile(d.path / "__views" / "0" / "seg.log");
    touchFile(d.path / "__topic_schemas" / "00000000000000000000.log");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "seg.log");
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "__views" / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(d.path / "__topic_schemas" / "00000000000000000000.log"));
    std::set<std::string> top;
    for (const auto& e : fs::directory_iterator(d.path)) top.insert(e.path().filename().string());
    EXPECT_EQ(top, (std::set<std::string>{"__topic_schemas", "__views", "quarantine", "topics"}));
}

TEST(RuntimeTopicRecoveryRegression, SymlinkOutsideDataDirIsNeverFollowedRegression) {
    TempDataDir d("symlink");
    TempDataDir outside("symlink-outside");
    touchFile(outside.path / "victim" / "topic-id", "2\n");                  // looks like an orphan...
    touchFile(outside.path / "victim" / "0" / "00000000000000000000.log");   // ...OUTSIDE dataDir
    fs::create_directories(d.path / "topics");
    fs::create_directory_symlink(outside.path / "victim", d.path / "topics" / "linked");
    touchFile(d.path / "topics" / "mixed" / "0" / "seg.log");
    fs::create_symlink(outside.path / "victim" / "0" / "00000000000000000000.log",
                       d.path / "topics" / "mixed" / "0" / "link.log");      // a symlink inside a partition
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "a symlinked dir is never deleted";
    EXPECT_TRUE(r.quarantined.empty()) << "nor moved; a dir holding a symlink is not a topic layout";
    EXPECT_TRUE(fs::exists(outside.path / "victim" / "0" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::is_symlink(d.path / "topics" / "linked"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "mixed" / "0" / "seg.log"));
}

// A symlinked PARTITION dir or a symlinked topic-id marker makes the dir "not ours".
TEST(RuntimeTopicRecoveryRegression, SymlinkedPartitionDirOrMarkerIsNeverFollowedRegression) {
    TempDataDir d("symlink-part");
    TempDataDir outside("symlink-part-outside");
    touchFile(outside.path / "part" / "00000000000000000000.log");
    touchFile(outside.path / "marker", "2\n");
    touchFile(d.path / "topics" / "linked_part" / "topic-id", "2\n");
    fs::create_directory_symlink(outside.path / "part", d.path / "topics" / "linked_part" / "0");
    touchFile(d.path / "topics" / "linked_marker" / "0" / "seg.log");
    fs::create_symlink(outside.path / "marker", d.path / "topics" / "linked_marker" / "topic-id");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(outside.path / "part" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::exists(outside.path / "marker"));
    EXPECT_TRUE(fs::is_symlink(d.path / "topics" / "linked_part" / "0"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "linked_marker" / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, SymlinkedTopicsOrQuarantineRootIsNeverFollowedRegression) {
    {
        TempDataDir d("symlink-root");
        TempDataDir outside("symlink-root-outside");
        touchFile(outside.path / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "topics");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
        EXPECT_TRUE(r.deleted.empty());
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_TRUE(fs::exists(outside.path / "gone" / "0" / "seg.log"));
    }
    {
        TempDataDir d("symlink-q");
        TempDataDir outside("symlink-q-outside");
        touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "quarantine");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
        EXPECT_TRUE(r.quarantined.empty()) << "never moves data through a symlinked quarantine";
        EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log"));
        EXPECT_TRUE(fs::is_empty(outside.path));
    }
}

TEST(RuntimeTopicRecoveryRegression, CaseVariantOfAClaimedNameIsKeptRegression) {
    TempDataDir d("case-claim");
    touchFile(d.path / "topics" / "Orders" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty()) << "on a case-insensitive filesystem it may be 'orders' own dir";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
}

// A claim (exact or case-folded) beats a placeholder marker: never delete a dir a
// live topic may own, even if its marker names an orphan id.
TEST(RuntimeTopicRecoveryRegression, ClaimedNameBeatsAPlaceholderMarkerRegression) {
    TempDataDir d("claim-beats-marker");
    touchFile(d.path / "topics" / "Orders" / "topic-id", "2\n");
    touchFile(d.path / "topics" / "Orders" / "0" / "seg.log");
    touchFile(d.path / "topics" / "exact" / "topic-id", "2\n");
    touchFile(d.path / "topics" / "exact" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(), {spec("orders", 0), spec("exact", 1), placeholder(2)}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "exact" / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, MissingTopicsDirIsANoOpRegression) {
    TempDataDir d("no-topics");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics")) << "nothing is created";
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
}
