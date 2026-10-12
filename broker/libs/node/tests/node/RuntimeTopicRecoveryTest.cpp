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

#include <unistd.h>
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
        : path(fs::temp_directory_path() / ("rune-rtr-" + std::string(name) + "-" + std::to_string(::getpid()))) {
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
std::string readMarker(const fs::path& topicDir) {
    std::ifstream in(topicDir / rune::gateway::kTopicIdMarkerFile, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}
// Boot 2 of an end-to-end scenario: static topics + recovery, exactly as BrokerNode.
struct Boot {
    std::vector<TopicSpec> all;
    std::vector<int32_t>   placeholderIds;
    std::string            catalogDamage;   // fix round 2b
};
Boot recoverBoot(const fs::path& dataDir, std::vector<TopicSpec> statics) {
    Boot b{std::move(statics), {}};
    auto rec = rune::node::recoverRuntimeTopics(dataDir.string(), static_cast<int32_t>(b.all.size()));
    for (auto& t : rec.specs) b.all.push_back(std::move(t));
    b.placeholderIds = std::move(rec.placeholderIds);
    b.catalogDamage  = std::move(rec.catalogDamage);
    return b;
}
// The single segment file of a one-segment catalog log dir.
fs::path onlySegment(const fs::path& logDir) {
    for (const auto& e : fs::directory_iterator(logDir))
        if (e.path().extension() == ".log") return e.path();
    return {};
}
std::string readAll(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}
} // namespace

// The marker is written by a RUNTIME create (DefineTopic / CreateView sink) of a disk
// topic only, as "pending <id>", and becomes "committed <id>" when the control
// thread is told the catalog record committed (fix round 1, I1).  Boot-built topics
// (static RUNE_TOPICS + recovered) get none: a removed static topic's id can become
// a placeholder gap, and its data must then be quarantined, never deleted.
TEST(RuntimeTopicRecoveryRegression, TopicRegistryWritesTheTopicIdMarkerRegression) {
    TempDataDir d("marker");
    {
        rune::gateway::TopicRegistry reg({spec("events", 0, 2)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("runtime_disk", -1, 2)), 1);
        EXPECT_EQ(readMarker(d.path / "topics" / "runtime_disk"), "pending 1\n");
        TopicSpec t = spec("later", -1); t.persistence = rune::node::PersistenceMode::Transient;
        EXPECT_EQ(reg.createTopic(t), 2);   // transient: no dir, no marker
        (void)reg.createTopic(spec("runtime_disk", -1, 2));   // idempotent: marker unchanged
        EXPECT_EQ(readMarker(d.path / "topics" / "runtime_disk"), "pending 1\n");

        EXPECT_TRUE(reg.markTopicCommitted(1));
        EXPECT_EQ(readMarker(d.path / "topics" / "runtime_disk"), "committed 1\n");
        EXPECT_FALSE(reg.markTopicCommitted(1)) << "Committed is terminal";
        EXPECT_FALSE(reg.markTopicCommitted(0)) << "a boot-built topic has no marker to commit";
        EXPECT_FALSE(reg.markTopicCommitted(2)) << "a transient topic has no marker";
        EXPECT_FALSE(reg.markTopicCommitted(99)) << "unknown id";
    }
    EXPECT_FALSE(fs::exists(d.path / "topics" / "runtime_disk" / rune::gateway::kTopicIdMarkerTmpFile))
        << "the atomic write leaves no temp file behind";
    EXPECT_FALSE(fs::exists(d.path / "topics" / "events" / rune::gateway::kTopicIdMarkerFile))
        << "a boot-built (static/recovered) topic is never marked";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "events" / "1"));
    EXPECT_FALSE(fs::exists(d.path / "topics" / "later"));
}

// Review M1: the marker is never stamped onto a topic dir this create did not make
// (a pre-existing dir — e.g. a case-variant name sharing it on a case-insensitive
// filesystem, or an old dir that survived) — so a failed create can never arm a
// delete of someone else's data.
TEST(RuntimeTopicRecoveryRegression, MarkerIsNeverWrittenIntoAPreexistingDirRegression) {
    TempDataDir d("marker-preexisting");
    touchFile(d.path / "topics" / "old_dir" / "0" / "00000000000000000000.log", "");
    touchFile(d.path / "topics" / "old_dir" / "topic-id", "pending 7\n");       // a stale orphan's
    touchFile(d.path / "topics" / "old_dir" / "topic-id.tmp", "committed 7\n");
    {
        rune::gateway::TopicRegistry reg({}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("old_dir", -1)), 0);
        EXPECT_FALSE(reg.markTopicCommitted(0));
    }
    EXPECT_FALSE(fs::exists(d.path / "topics" / "old_dir" / rune::gateway::kTopicIdMarkerFile))
        << "fix round 2 (N2): a reused dir's stale marker is removed, never kept";
    EXPECT_FALSE(fs::exists(d.path / "topics" / "old_dir" / rune::gateway::kTopicIdMarkerTmpFile));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "old_dir" / "0" / "00000000000000000000.log"));
}

TEST(RuntimeTopicRecoveryRegression, OrphanPlaceholderDirIsDeletedRegression) {
    TempDataDir d("orphan-dir");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "00000000000000000000.log");
    touchFile(d.path / "topics" / "live" / "topic-id", "committed 0\n");
    touchFile(d.path / "topics" / "live" / "0" / "00000000000000000000.log");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(), {spec("live", 0), spec("other", 1), placeholder(2), spec("later", 3)}, {2}, kNow);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0], fs::canonical(d.path / "topics") / "orphan_v");
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v"));
    EXPECT_FALSE(fs::exists(d.path / "quarantine")) << "nothing quarantined, nothing created";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "live" / "0" / "00000000000000000000.log"));
}

// The whole B5 + B10 flow on real files: a runtime create whose record never
// committed (the orphan: marker stays "pending") and a committed one; the next boot
// recovers a placeholder for the orphan's id, deletes ONLY the orphan's dir, and
// boots the registry.  (Fix round 1: "pending orphan: deleted".)
TEST(RuntimeTopicRecoveryRegression, FailedCreateOrphanDirIsDeletedEndToEndRegression) {
    TempDataDir d("orphan-e2e");
    {   // boot 1: static "events" (id 0); runtime "orphan_v" (id 1, never recorded), "kept" (id 2)
        rune::gateway::TopicRegistry reg({spec("events", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("orphan_v", -1)), 1);
        ASSERT_EQ(reg.createTopic(spec("kept", -1)), 2);
        reg.log(1, 0)->append(oneRecordBatch("orphan-data"), 0);
        reg.log(2, 0)->append(oneRecordBatch("kept-data"), 0);
        appendRecords(d.path / "__topic_schemas", {schemaRecord(2, 1, 1, "kept", "kept_desc")});
        fs::create_directories(d.path / "__views");   // a real broker always has both catalogs
        ASSERT_TRUE(reg.markTopicCommitted(2));   // the admin thread's kA2cTopicCommitted
    }
    // boot 2
    const Boot b = recoverBoot(d.path, {spec("events", 0)});
    ASSERT_EQ(b.all.size(), 3u);
    EXPECT_EQ(b.all[1].name, rune::node::orphanPlaceholderName(1));
    EXPECT_EQ(b.placeholderIds, (std::vector<int32_t>{1}));
    EXPECT_EQ(b.catalogDamage, "") << "fix round 2b: a healthy catalog still deletes";
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0].filename(), "orphan_v");
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v"));
    rune::gateway::TopicRegistry reg(b.all, d.path.string());
    EXPECT_EQ(reg.topicIdForName("kept"), 2) << "later topics keep their ids";
    EXPECT_EQ(reg.log(2, 0)->logEndOffset(), 1) << "the recorded topic's data is intact";
    EXPECT_EQ(reg.log(1, 0), nullptr) << "the placeholder has no partition";
    EXPECT_FALSE(fs::exists(d.path / "topics" / rune::node::orphanPlaceholderName(1)));
}

// Fix round 1, I1: a gap is NOT always a never-committed create.  Log::recover()
// truncates a catalog at the first CRC mismatch, so a committed DefineTopic topic can
// lose its record while a later view survives in __views -> its id becomes a
// placeholder.  Its dir carries a "committed" marker: QUARANTINED with its data
// intact, never deleted.
TEST(RuntimeTopicRecoveryRegression, CrcTruncatedSchemaCatalogQuarantinesCommittedTopicRegression) {
    TempDataDir d("crc-trunc");
    {   // boot 1: static "src" (0); runtime DefineTopic "t1" (1, committed); view sink "v2" (2, committed)
        rune::gateway::TopicRegistry reg({spec("src", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("t1", -1)), 1);
        ASSERT_EQ(reg.createTopic(spec("v2", -1)), 2);
        reg.log(1, 0)->append(oneRecordBatch("acked-t1-data"), 0);
        appendRecords(d.path / "__topic_schemas", {schemaRecord(1, 1, 1, "t1", "t1_desc")});
        appendRecords(d.path / "__views", {viewRecord(2, "v2", /*source*/ 0)});
        ASSERT_TRUE(reg.markTopicCommitted(1));
        ASSERT_TRUE(reg.markTopicCommitted(2));
    }
    {   // damage __topic_schemas: flip a byte inside its only record -> CRC mismatch
        const fs::path seg = onlySegment(d.path / "__topic_schemas");
        ASSERT_FALSE(seg.empty());
        std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(0, std::ios::end);
        const auto size = static_cast<std::streamoff>(f.tellg());
        ASSERT_GT(size, 8);
        f.seekg(size - 8);
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(c ^ 0x5a);
        f.seekp(size - 8);
        f.write(&c, 1);
    }
    // boot 2: recovery truncates the damaged record -> id 1 is a gap
    const Boot b = recoverBoot(d.path, {spec("src", 0)});
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1})) << "precondition: the CRC damage made a gap";
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, {} /* isolate the two-phase marker guard; 2b damage guard tested separately */);
    EXPECT_TRUE(r.deleted.empty()) << "a COMMITTED topic is never deleted, whatever the catalog says";
    ASSERT_EQ(r.quarantined.size(), 1u);
    const fs::path q = d.path / "quarantine" / ("t1-" + std::to_string(kNow));
    EXPECT_EQ(fs::canonical(r.quarantined[0]), fs::canonical(q));
    EXPECT_EQ(readMarker(q), "committed 1\n");
    rune::storage::Log moved((q / "0").string());
    moved.recover();
    EXPECT_EQ(moved.logEndOffset(), 1) << "the quarantined data is intact";
    const std::vector<uint8_t> raw = moved.read(0, 1 << 20);
    EXPECT_NE(std::string(raw.begin(), raw.end()).find("acked-t1-data"), std::string::npos);
    EXPECT_TRUE(fs::exists(d.path / "topics" / "v2" / "0")) << "the surviving view's sink is untouched";
}

// Fix round 1, I1: a MISSING __topic_schemas dir (a partial restore) reads as an
// empty catalog while __views survives -> every DefineTopic id below the view is a
// gap.  Committed dirs are quarantined; NOTHING is deleted.
TEST(RuntimeTopicRecoveryRegression, MissingSchemaCatalogWithSurvivingViewsDeletesNothingRegression) {
    TempDataDir d("missing-schemas");
    {
        rune::gateway::TopicRegistry reg({spec("src", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("t1", -1)), 1);
        ASSERT_EQ(reg.createTopic(spec("t2", -1)), 2);
        ASSERT_EQ(reg.createTopic(spec("v3", -1)), 3);
        reg.log(1, 0)->append(oneRecordBatch("acked-t1"), 0);
        reg.log(2, 0)->append(oneRecordBatch("acked-t2"), 0);
        appendRecords(d.path / "__topic_schemas",
                      {schemaRecord(1, 1, 1, "t1", "d1"), schemaRecord(2, 1, 1, "t2", "d2")});
        appendRecords(d.path / "__views", {viewRecord(3, "v3", /*source*/ 0)});
        for (int32_t id : {1, 2, 3}) ASSERT_TRUE(reg.markTopicCommitted(id));
    }
    fs::remove_all(d.path / "__topic_schemas");
    const Boot b = recoverBoot(d.path, {spec("src", 0)});
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1, 2}));
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, {} /* isolate the two-phase marker guard; 2b damage guard tested separately */);
    EXPECT_TRUE(r.deleted.empty()) << "nothing is deleted on a missing catalog";
    EXPECT_EQ(r.quarantined.size(), 2u);
    for (const char* n : {"t1", "t2"}) {
        const fs::path q = d.path / "quarantine" / (std::string(n) + "-" + std::to_string(kNow));
        EXPECT_TRUE(fs::exists(q / "0")) << n;
        EXPECT_FALSE(fs::exists(d.path / "topics" / n)) << n;
    }
    EXPECT_TRUE(fs::exists(d.path / "topics" / "v3" / "0"));
}

// Fix round 1b: a crash between the record's commit and the control thread's marker
// rewrite leaves "pending" on a CLAIMED topic.  Boot promotes it to "committed"
// before any delete decision, so a later loss of that record quarantines the dir
// instead of deleting it.
TEST(RuntimeTopicRecoveryRegression, BootPromotesAClaimedPendingMarkerSoALaterCatalogLossQuarantinesRegression) {
    TempDataDir d("promote");
    {   // boot 1: static "src" (0); runtime "t1" (1) whose record commits, but the
        // broker crashes before kA2cTopicCommitted is applied (marker stays pending);
        // view sink "v2" (2) committed normally.
        rune::gateway::TopicRegistry reg({spec("src", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("t1", -1)), 1);
        ASSERT_EQ(reg.createTopic(spec("v2", -1)), 2);
        reg.log(1, 0)->append(oneRecordBatch("acked-t1"), 0);
        appendRecords(d.path / "__topic_schemas", {schemaRecord(1, 1, 1, "t1", "d1")});
        appendRecords(d.path / "__views", {viewRecord(2, "v2", /*source*/ 0)});
        ASSERT_TRUE(reg.markTopicCommitted(2));
    }
    ASSERT_EQ(readMarker(d.path / "topics" / "t1"), "pending 1\n");
    {   // boot 2: the catalog claims t1 -> its marker is promoted; nothing else moves
        const Boot b = recoverBoot(d.path, {spec("src", 0)});
        ASSERT_TRUE(b.placeholderIds.empty());
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
        ASSERT_EQ(r.promoted.size(), 1u);
        EXPECT_EQ(r.promoted[0], fs::canonical(d.path / "topics" / "t1"));
        EXPECT_TRUE(r.deleted.empty());
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_EQ(readMarker(d.path / "topics" / "t1"), "committed 1\n");
        EXPECT_EQ(readMarker(d.path / "topics" / "v2"), "committed 2\n") << "already committed: untouched";
        EXPECT_FALSE(fs::exists(d.path / "topics" / "t1" / rune::gateway::kTopicIdMarkerTmpFile));
    }
    fs::remove_all(d.path / "__topic_schemas");   // the record is later lost
    {   // boot 3: id 1 is a placeholder; t1's dir is QUARANTINED, never deleted
        const Boot b = recoverBoot(d.path, {spec("src", 0)});
        ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1}));
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow + 1, {} /* isolate the promotion guard; 2b tested separately */);
        EXPECT_TRUE(r.deleted.empty()) << "a promoted (committed) topic is never deleted";
        ASSERT_EQ(r.quarantined.size(), 1u);
        const fs::path q = d.path / "quarantine" / ("t1-" + std::to_string(kNow + 1));
        EXPECT_TRUE(fs::exists(q / "0"));
        EXPECT_EQ(readMarker(q), "committed 1\n");
    }
}

// Promotion guards: only an EXACT claimed name, a regular (non-symlinked) marker,
// a non-symlinked dir, and a marker id equal to the claiming topic's id.
TEST(RuntimeTopicRecoveryRegression, PromotionNeverTouchesAMismatchedOrSymlinkedMarkerRegression) {
    TempDataDir d("promote-guards");
    TempDataDir outside("promote-guards-outside");
    touchFile(d.path / "topics" / "wrong_id" / "topic-id", "pending 9\n");     // claimed as id 1
    touchFile(d.path / "topics" / "wrong_id" / "0" / "seg.log");
    touchFile(d.path / "topics" / "Folded" / "topic-id", "pending 2\n");       // claimed only by fold
    touchFile(d.path / "topics" / "Folded" / "0" / "seg.log");
    touchFile(outside.path / "marker", "pending 3\n");
    touchFile(d.path / "topics" / "linked_marker" / "0" / "seg.log");
    fs::create_symlink(outside.path / "marker", d.path / "topics" / "linked_marker" / "topic-id");
    touchFile(outside.path / "victim" / "topic-id", "pending 4\n");
    touchFile(outside.path / "victim" / "0" / "seg.log");
    fs::create_directory_symlink(outside.path / "victim", d.path / "topics" / "linked_dir");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(),
        {spec("wrong_id", 1), spec("folded", 2), spec("linked_marker", 3), spec("linked_dir", 4)}, {}, kNow);
    EXPECT_TRUE(r.promoted.empty());
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    // Fix round 2 (N2): the exact-claimed dir's marker names ANOTHER id -> removed
    // (never promoted, never left to arm a later delete).
    ASSERT_EQ(r.markersCleared.size(), 1u);
    EXPECT_EQ(r.markersCleared[0].filename(), "wrong_id");
    EXPECT_FALSE(fs::exists(fs::symlink_status(d.path / "topics" / "wrong_id" / "topic-id")));
    EXPECT_EQ(readMarker(d.path / "topics" / "Folded"), "pending 2\n");
    EXPECT_EQ(readAll(outside.path / "marker"), "pending 3\n") << "never written through a symlink";
    EXPECT_EQ(readAll(outside.path / "victim" / "topic-id"), "pending 4\n");
}

// ── Fix round 2 ──────────────────────────────────────────────────────────────

// N1: the marker writer never follows a symlink planted at topic-id.tmp — on the
// runtime path (markTopicCommitted) and the boot path (promotion).  The planted
// link is unlinked itself; the victim outside dataDir keeps its bytes; the marker
// is a regular file with the right content.
TEST(RuntimeTopicRecoveryRegression, MarkerWriterNeverFollowsASymlinkedTmpRegression) {
    TempDataDir d("tmp-symlink");
    TempDataDir outside("tmp-symlink-outside");
    touchFile(outside.path / "victim", "precious");
    {   // runtime: markTopicCommitted
        rune::gateway::TopicRegistry reg({}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("t", -1)), 0);
        fs::create_symlink(outside.path / "victim", d.path / "topics" / "t" / "topic-id.tmp");
        EXPECT_TRUE(reg.markTopicCommitted(0));
        EXPECT_EQ(readAll(outside.path / "victim"), "precious") << "never written through the link";
        EXPECT_TRUE(fs::is_regular_file(fs::symlink_status(d.path / "topics" / "t" / "topic-id")));
        EXPECT_EQ(readMarker(d.path / "topics" / "t"), "committed 0\n");
        EXPECT_FALSE(fs::exists(fs::symlink_status(d.path / "topics" / "t" / "topic-id.tmp")));
    }
    {   // boot: promotion of a claimed pending dir
        touchFile(d.path / "topics" / "u" / "topic-id", "pending 1\n");
        touchFile(d.path / "topics" / "u" / "0" / "seg.log");
        fs::create_symlink(outside.path / "victim", d.path / "topics" / "u" / "topic-id.tmp");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("t", 0), spec("u", 1)}, {}, kNow);
        EXPECT_EQ(r.promoted.size(), 1u);
        EXPECT_EQ(readAll(outside.path / "victim"), "precious");
        EXPECT_TRUE(fs::is_regular_file(fs::symlink_status(d.path / "topics" / "u" / "topic-id")));
        EXPECT_EQ(readMarker(d.path / "topics" / "u"), "committed 1\n");
    }
}

// N1: a symlink AT topic-id (or a symlinked topic dir) makes the writer refuse —
// runtime (markTopicCommitted) and boot (promotion) — and the target is untouched.
TEST(RuntimeTopicRecoveryRegression, MarkerWriterRefusesASymlinkedMarkerOrDirRegression) {
    TempDataDir d("marker-symlink");
    TempDataDir outside("marker-symlink-outside");
    touchFile(outside.path / "victim", "pending 0\n");
    touchFile(outside.path / "dir" / "topic-id", "pending 1\n");
    rune::gateway::TopicRegistry reg({}, d.path.string());
    ASSERT_EQ(reg.createTopic(spec("m", -1)), 0);
    ASSERT_EQ(reg.createTopic(spec("dirlink", -1)), 1);
    // topic-id replaced by a symlink to a file outside dataDir
    fs::remove(d.path / "topics" / "m" / "topic-id");
    fs::create_symlink(outside.path / "victim", d.path / "topics" / "m" / "topic-id");
    EXPECT_FALSE(reg.markTopicCommitted(0)) << "refused: the final marker path is a symlink";
    EXPECT_EQ(readAll(outside.path / "victim"), "pending 0\n");
    EXPECT_TRUE(fs::is_symlink(d.path / "topics" / "m" / "topic-id"));
    EXPECT_FALSE(fs::exists(fs::symlink_status(d.path / "topics" / "m" / "topic-id.tmp")));
    // the whole topic dir replaced by a symlink to a dir outside dataDir
    fs::rename(d.path / "topics" / "dirlink", d.path / "moved_away");
    fs::create_directory_symlink(outside.path / "dir", d.path / "topics" / "dirlink");
    EXPECT_FALSE(reg.markTopicCommitted(1)) << "refused: the topic dir is a symlink";
    EXPECT_EQ(readAll(outside.path / "dir" / "topic-id"), "pending 1\n");
    EXPECT_FALSE(fs::exists(outside.path / "dir" / "topic-id.tmp"));
    // boot promotion: neither is touched
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("m", 0), spec("dirlink", 1)}, {}, kNow);
    EXPECT_TRUE(r.promoted.empty());
    EXPECT_TRUE(r.markersCleared.empty());
    EXPECT_EQ(readAll(outside.path / "victim"), "pending 0\n");
    EXPECT_EQ(readAll(outside.path / "dir" / "topic-id"), "pending 1\n");
    // and the direct writer refuses both shapes
    EXPECT_FALSE(rune::gateway::writeTopicIdMarker((d.path / "topics" / "m").string(), "committed", 0));
    EXPECT_FALSE(rune::gateway::writeTopicIdMarker((d.path / "topics" / "dirlink").string(), "committed", 1));
    EXPECT_EQ(readAll(outside.path / "victim"), "pending 0\n");
}

// N2: a stale "pending <X>" on an exact-claimed (reused) dir is removed at boot, so
// a later boot where the owner's record is lost and X is a placeholder QUARANTINES
// the owner's data instead of deleting it.
TEST(RuntimeTopicRecoveryRegression, StaleOtherIdMarkerOnAClaimedDirNeverArmsADeleteRegression) {
    TempDataDir d("stale-marker");
    touchFile(d.path / "topics" / "reused" / "topic-id", "pending 2\n");   // an old orphan's id
    touchFile(d.path / "topics" / "reused" / "0" / "seg.log", "owner-data");
    const auto r1 = rune::node::reconcileTopicDirs(d.path.string(), {spec("reused", 5)}, {}, kNow);
    ASSERT_EQ(r1.markersCleared.size(), 1u);
    EXPECT_FALSE(fs::exists(fs::symlink_status(d.path / "topics" / "reused" / "topic-id")));
    // a later boot: the owner's record is gone and id 2 is a placeholder
    const auto r2 = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow + 1);
    EXPECT_TRUE(r2.deleted.empty());
    ASSERT_EQ(r2.quarantined.size(), 1u);
    EXPECT_EQ(readAll(d.path / "quarantine" / ("reused-" + std::to_string(kNow + 1)) / "0" / "seg.log"),
              "owner-data");
}

// N3: a pending orphan that also holds topic-id.tmp (an interrupted rewrite to
// "committed") is evidence the record may have committed: quarantined, never deleted.
TEST(RuntimeTopicRecoveryRegression, PendingOrphanWithAnInterruptedRewriteIsQuarantinedRegression) {
    TempDataDir d("pending-plus-tmp");
    touchFile(d.path / "topics" / "o" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "o" / "topic-id.tmp", "committed 2\n");
    touchFile(d.path / "topics" / "o" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("o-" + std::to_string(kNow)) / "0" / "seg.log"));
}

// M2 / M3: a delete that fails part-way (a partition dir the broker cannot empty)
// never leaves the orphan's dir behind for a same-name create to reuse: what
// remains is QUARANTINED.
TEST(RuntimeTopicRecoveryRegression, FailedOrphanDeleteQuarantinesWhatRemainsRegression) {
    if (::geteuid() == 0) GTEST_SKIP() << "root ignores directory permissions";
    TempDataDir d("delete-fails");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "seg.log");
    const fs::path part = d.path / "topics" / "orphan_v" / "0";
    fs::permissions(part, fs::perms::owner_read | fs::perms::owner_exec);   // seg.log cannot be unlinked
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
    const fs::path q = d.path / "quarantine" / ("orphan_v-" + std::to_string(kNow));
    std::error_code ec;
    fs::permissions(q / "0", fs::perms::owner_all, ec);    // restore for cleanup
    fs::permissions(part, fs::perms::owner_all, ec);
    EXPECT_TRUE(r.deleted.empty()) << "the delete failed";
    ASSERT_EQ(r.quarantined.size(), 1u) << "what remains is quarantined";
    EXPECT_FALSE(fs::exists(d.path / "topics" / "orphan_v")) << "the name's dir is gone";
    EXPECT_TRUE(fs::exists(q / "0" / "seg.log"));
}

// M3: a quarantine that cannot be created (read-only dataDir) moves nothing and
// deletes nothing: the error is logged and the data stays where it is.
TEST(RuntimeTopicRecoveryRegression, UncreatableQuarantineLeavesDataInPlaceRegression) {
    if (::geteuid() == 0) GTEST_SKIP() << "root ignores directory permissions";
    TempDataDir d("q-readonly");
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log", "kept");
    fs::permissions(d.path, fs::perms::owner_read | fs::perms::owner_exec);
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
    std::error_code ec;
    fs::permissions(d.path, fs::perms::owner_all, ec);   // restore for cleanup
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
    EXPECT_EQ(readAll(d.path / "topics" / "gone" / "0" / "seg.log"), "kept");
}

// ── Fix round 2b: a damaged catalog disables every delete ────────────────────
namespace {
// boot 1 of the 2b scenarios: static "src" (0); runtime "orphan_o" (1, PENDING —
// its record never committed); "t2" (2, committed); view sink "v3" (3, committed).
void bootOneWithAnOrphan(const fs::path& dataDir) {
    rune::gateway::TopicRegistry reg({spec("src", 0)}, dataDir.string());
    ASSERT_EQ(reg.createTopic(spec("orphan_o", -1)), 1);
    ASSERT_EQ(reg.createTopic(spec("t2", -1)), 2);
    ASSERT_EQ(reg.createTopic(spec("v3", -1)), 3);
    reg.log(1, 0)->append(oneRecordBatch("orphan-bytes"), 0);
    reg.log(2, 0)->append(oneRecordBatch("acked-t2"), 0);
    appendRecords(dataDir / "__topic_schemas", {schemaRecord(2, 1, 1, "t2", "d2")});
    appendRecords(dataDir / "__views", {viewRecord(3, "v3", /*source*/ 0)});
    ASSERT_TRUE(reg.markTopicCommitted(2));
    ASSERT_TRUE(reg.markTopicCommitted(3));
}
void flipByteNearEnd(const fs::path& seg) {
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    f.seekg(0, std::ios::end);
    const auto size = static_cast<std::streamoff>(f.tellg());
    f.seekg(size - 8);
    char c = 0;
    f.read(&c, 1);
    c = static_cast<char>(c ^ 0x5a);
    f.seekp(size - 8);
    f.write(&c, 1);
}
} // namespace

// Healthy catalogs: the pending orphan is deleted exactly as before (2b changes nothing).
TEST(RuntimeTopicRecoveryRegression, HealthyCatalogStillDeletesThePendingOrphanRegression) {
    TempDataDir d("2b-healthy");
    bootOneWithAnOrphan(d.path);
    const Boot b = recoverBoot(d.path, {spec("src", 0)});
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1}));
    EXPECT_EQ(b.catalogDamage, "");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
    ASSERT_EQ(r.deleted.size(), 1u);
    EXPECT_EQ(r.deleted[0].filename(), "orphan_o");
    EXPECT_TRUE(r.quarantined.empty());
}

// A catalog cut by recovery (CRC mismatch): even the genuinely PENDING orphan is
// quarantined, not deleted — and so is the committed topic whose record was cut.
TEST(RuntimeTopicRecoveryRegression, TruncatedCatalogDeletesNothingRegression) {
    TempDataDir d("2b-truncated");
    bootOneWithAnOrphan(d.path);
    flipByteNearEnd(onlySegment(d.path / "__topic_schemas"));
    const Boot b = recoverBoot(d.path, {spec("src", 0)});
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1, 2}));
    EXPECT_NE(b.catalogDamage.find("__topic_schemas truncated by recovery"), std::string::npos)
        << b.catalogDamage;
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
    EXPECT_TRUE(r.deleted.empty()) << "no deletes on a damaged catalog";
    EXPECT_EQ(r.quarantined.size(), 2u);
    for (const char* n : {"orphan_o", "t2"})
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(n) + "-" + std::to_string(kNow)) / "0")) << n;
    EXPECT_TRUE(fs::exists(d.path / "topics" / "v3" / "0"));
}

// One catalog MISSING while the other exists (a partial restore): same outcome.
TEST(RuntimeTopicRecoveryRegression, MissingCatalogDeletesNothingRegression) {
    TempDataDir d("2b-missing");
    bootOneWithAnOrphan(d.path);
    fs::remove_all(d.path / "__topic_schemas");
    const Boot b = recoverBoot(d.path, {spec("src", 0)});
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1, 2}));
    EXPECT_NE(b.catalogDamage.find("__topic_schemas missing"), std::string::npos) << b.catalogDamage;
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_EQ(r.quarantined.size(), 2u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("orphan_o-" + std::to_string(kNow)) / "0"));
}

// The damage flag also covers __views (cut, or missing while __topic_schemas exists),
// and both catalogs missing (a fresh data dir) is NOT damage.
TEST(RuntimeTopicRecoveryRegression, CatalogDamageCoversBothCatalogsRegression) {
    {
        TempDataDir d("2b-views-cut");
        bootOneWithAnOrphan(d.path);
        flipByteNearEnd(onlySegment(d.path / "__views"));
        EXPECT_NE(recoverBoot(d.path, {spec("src", 0)}).catalogDamage.find("__views truncated by recovery"),
                  std::string::npos);
    }
    {
        TempDataDir d("2b-views-missing");
        bootOneWithAnOrphan(d.path);
        fs::remove_all(d.path / "__views");
        EXPECT_NE(recoverBoot(d.path, {spec("src", 0)}).catalogDamage.find("__views missing"),
                  std::string::npos);
    }
    {
        TempDataDir d("2b-fresh");
        EXPECT_EQ(recoverBoot(d.path, {spec("src", 0)}).catalogDamage, "");
    }
}

// Fix round 1, I2: a placeholder is what RECOVERY created, never a name.  A STATIC
// topic literally named "__orphan.1" (names are unvalidated until Task 5a) next to a
// runtime topic with marker id 1 that is no longer claimed: nothing is deleted —
// neither the committed dir nor even a pending one, since recovery made no
// placeholder for id 1.
TEST(RuntimeTopicRecoveryRegression, StaticTopicNamedLikeAPlaceholderNeverArmsADeleteRegression) {
    TempDataDir d("spoofed-placeholder");
    {   // boot 1: RUNE_TOPICS=a; runtime "x" (id 1) commits
        rune::gateway::TopicRegistry reg({spec("a", 0)}, d.path.string());
        ASSERT_EQ(reg.createTopic(spec("x", -1)), 1);
        reg.log(1, 0)->append(oneRecordBatch("x-data"), 0);
        appendRecords(d.path / "__topic_schemas", {schemaRecord(1, 1, 1, "x", "x_desc")});
        fs::create_directories(d.path / "__views");   // healthy: both catalogs present
        ASSERT_TRUE(reg.markTopicCommitted(1));
    }
    // a second unclaimed dir whose marker is still PENDING id 1 (the stronger case)
    touchFile(d.path / "topics" / "y" / "topic-id", "pending 1\n");
    touchFile(d.path / "topics" / "y" / "0" / "seg.log");
    // boot 2: RUNE_TOPICS=a,__orphan.1 -> firstRuntimeId 2: x's record is skipped
    const Boot b = recoverBoot(d.path, {spec("a", 0), spec(rune::node::orphanPlaceholderName(1), 1)});
    ASSERT_TRUE(b.placeholderIds.empty()) << "recovery created no placeholder";
    ASSERT_EQ(b.catalogDamage, "") << "precondition: a healthy catalog (deletes enabled)";
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
    EXPECT_TRUE(r.deleted.empty()) << "a static name spelling __orphan.<id> never arms a delete";
    EXPECT_EQ(r.quarantined.size(), 2u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("x-" + std::to_string(kNow)) / "0"));
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("y-" + std::to_string(kNow)) / "0" / "seg.log"));
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
        appendRecords(d.path / "__topic_schemas", {schemaRecord(2, 1, 1, "c", "c_desc")});
        fs::create_directories(d.path / "__views");   // healthy: both catalogs present
        ASSERT_TRUE(reg.markTopicCommitted(2));
    }
    // boot 2: RUNE_TOPICS=a -> firstRuntimeId 1, id 1 is now a gap
    const Boot b = recoverBoot(d.path, {spec("a", 0)});
    ASSERT_EQ(b.all.size(), 3u);
    ASSERT_EQ(b.placeholderIds, (std::vector<int32_t>{1}));
    ASSERT_EQ(b.catalogDamage, "") << "precondition: a healthy catalog (deletes enabled)";
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), b.all, b.placeholderIds, kNow, b.catalogDamage);
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
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
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
        const auto again = rune::node::reconcileTopicDirs(d.path.string(), {spec("events", 0)}, {}, kNow + 60);
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
    touchFile(d.path / "topics" / "other_build" / "topic-id", "pending 7\n");   // id 7 is not a placeholder
    touchFile(d.path / "topics" / "other_build" / "0" / "seg.log");
    touchFile(d.path / "topics" / "garbage_id" / "topic-id", "not-a-number");
    touchFile(d.path / "topics" / "garbage_id" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("x", 7)}, {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_EQ(r.quarantined.size(), 3u);
    for (const char* n : {"typo_name", "other_build", "garbage_id"}) {
        EXPECT_FALSE(fs::exists(d.path / "topics" / n)) << n;
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(n) + "-" + std::to_string(kNow)) / "0" / "seg.log"))
            << n;
    }
}

// A delete is never decided on a guess: only EXACTLY "pending <digits>[\n]" naming a
// placeholder deletes.  A committed marker on a placeholder id, a bare number (the
// pre-fix-round-1 format), and every malformed variant is quarantined.  The two
// positive cases pin the other side of the parser (with and without '\n').
TEST(RuntimeTopicRecoveryRegression, MalformedMarkerNeverDeletesRegression) {
    TempDataDir d("bad-marker");
    const std::pair<const char*, const char*> cases[] = {
        {"committed_on_placeholder", "committed 2\n"}, {"bare_number", "2\n"},
        {"trailing_junk", "pending 2abc"}, {"leading_space", " pending 2"},
        {"double_space", "pending  2"}, {"two_lines", "pending 2\npending 2\n"},
        {"upper_case", "PENDING 2"}, {"negative", "pending -2"},
        {"overflow", "pending 99999999999"}, {"no_id", "pending "}, {"empty_marker", ""},
        {"crlf", "pending 2\r\n"}, {"leading_zero", "pending 02"}};
    for (const auto& [name, content] : cases) {
        touchFile(d.path / "topics" / name / "topic-id", content);
        touchFile(d.path / "topics" / name / "0" / "seg.log");
    }
    touchFile(d.path / "topics" / "ok_newline" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "ok_newline" / "0" / "seg.log");
    touchFile(d.path / "topics" / "ok_no_newline" / "topic-id", "pending 3");
    touchFile(d.path / "topics" / "ok_no_newline" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2), placeholder(3)},
                                                  {2, 3}, kNow);
    EXPECT_EQ(r.deleted.size(), 2u);
    EXPECT_FALSE(fs::exists(d.path / "topics" / "ok_newline"));
    EXPECT_FALSE(fs::exists(d.path / "topics" / "ok_no_newline"));
    EXPECT_EQ(r.quarantined.size(), std::size(cases));
    for (const auto& [name, content] : cases)
        EXPECT_TRUE(fs::exists(d.path / "quarantine" / (std::string(name) + "-" + std::to_string(kNow)) / "0" / "seg.log"))
            << name;
}

// An interrupted marker write (temp file present, rename never happened) still
// leaves a recognised layout: the dir is quarantined, not left behind forever.
TEST(RuntimeTopicRecoveryRegression, InterruptedMarkerWriteIsQuarantinedRegression) {
    TempDataDir d("tmp-marker");
    touchFile(d.path / "topics" / "half" / "topic-id.tmp", "pending 2\n");
    touchFile(d.path / "topics" / "half" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
    EXPECT_TRUE(r.deleted.empty()) << "the temp file is never read as a marker";
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("half-" + std::to_string(kNow)) / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, QuarantineNameCollisionGetsASuffixRegression) {
    TempDataDir d("q-collide");
    fs::create_directories(d.path / "quarantine" / ("gone-" + std::to_string(kNow)));
    touchFile(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-1"));   // a FILE also collides
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-2") / "0" / "seg.log"));
    EXPECT_TRUE(fs::is_empty(d.path / "quarantine" / ("gone-" + std::to_string(kNow))))
        << "an existing quarantine entry is never overwritten";
}

TEST(RuntimeTopicRecoveryRegression, QuarantinePathThatIsAFileMovesNothingRegression) {
    TempDataDir d("q-file");
    touchFile(d.path / "quarantine", "not a dir");
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
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
    touchFile(d.path / "elsewhere" / "topic-id", "pending 2\n");            // ...even if it looks like an orphan
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
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
    touchFile(d.path / "__views" / "topic-id", "pending 2\n");
    touchFile(d.path / "__views" / "0" / "seg.log");
    touchFile(d.path / "__topic_schemas" / "00000000000000000000.log");
    touchFile(d.path / "topics" / "orphan_v" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "orphan_v" / "0" / "seg.log");
    touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
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
    touchFile(outside.path / "victim" / "topic-id", "pending 2\n");                  // looks like an orphan...
    touchFile(outside.path / "victim" / "0" / "00000000000000000000.log");   // ...OUTSIDE dataDir
    fs::create_directories(d.path / "topics");
    fs::create_directory_symlink(outside.path / "victim", d.path / "topics" / "linked");
    touchFile(d.path / "topics" / "mixed" / "0" / "seg.log");
    fs::create_symlink(outside.path / "victim" / "0" / "00000000000000000000.log",
                       d.path / "topics" / "mixed" / "0" / "link.log");      // a symlink inside a partition
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
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
    touchFile(outside.path / "marker", "pending 2\n");
    touchFile(d.path / "topics" / "linked_part" / "topic-id", "pending 2\n");
    fs::create_directory_symlink(outside.path / "part", d.path / "topics" / "linked_part" / "0");
    touchFile(d.path / "topics" / "linked_marker" / "0" / "seg.log");
    fs::create_symlink(outside.path / "marker", d.path / "topics" / "linked_marker" / "topic-id");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {placeholder(2)}, {2}, kNow);
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
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
        EXPECT_TRUE(r.deleted.empty());
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_TRUE(fs::exists(outside.path / "gone" / "0" / "seg.log"));
    }
    {
        TempDataDir d("symlink-q");
        TempDataDir outside("symlink-q-outside");
        touchFile(d.path / "topics" / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "quarantine");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
        EXPECT_TRUE(r.quarantined.empty()) << "never moves data through a symlinked quarantine";
        EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log"));
        EXPECT_TRUE(fs::is_empty(outside.path));
    }
}

TEST(RuntimeTopicRecoveryRegression, CaseVariantOfAClaimedNameIsKeptRegression) {
    TempDataDir d("case-claim");
    touchFile(d.path / "topics" / "Orders" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0)}, {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty()) << "on a case-insensitive filesystem it may be 'orders' own dir";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
}

// A claim (exact or case-folded) beats a placeholder marker: never delete a dir a
// live topic may own, even if its marker names an orphan id.
TEST(RuntimeTopicRecoveryRegression, ClaimedNameBeatsAPlaceholderMarkerRegression) {
    TempDataDir d("claim-beats-marker");
    touchFile(d.path / "topics" / "Orders" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "Orders" / "0" / "seg.log");
    touchFile(d.path / "topics" / "exact" / "topic-id", "pending 2\n");
    touchFile(d.path / "topics" / "exact" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(
        d.path.string(), {spec("orders", 0), spec("exact", 1), placeholder(2)}, {2}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "exact" / "0" / "seg.log"));
}

TEST(RuntimeTopicRecoveryRegression, MissingTopicsDirIsANoOpRegression) {
    TempDataDir d("no-topics");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, {}, kNow);
    EXPECT_TRUE(r.deleted.empty());
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics")) << "nothing is created";
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
}
