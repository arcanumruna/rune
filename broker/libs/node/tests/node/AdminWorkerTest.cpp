// AdminWorkerTest.cpp — the AdminWorker's DDL state machine over the catalog
// write path (catalog history Task 9 port).
//
// Every scenario of the pre-catalog suite whose behaviour is unchanged keeps its
// name and assertions, now driven through CatalogDdlRig (a real __catalog leader
// replica, the CatalogApplier and recording deps).  A scenario that only existed
// for the deleted create-topic round-trip (the control thread's reply, its
// `existed` / persistence bytes, kA2cTopicCommitted) is its catalog-state
// equivalent, named the same with `Catalog` prefixed.
#include "support/CatalogDdlRig.h"
#include "support/SecurityRig.h"

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/BrokerNode.h>   // AdminWorkerAgent (the production submit)
#include <rune/node/FdBudget.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/SchemaRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace rune::node;
using rune::node::test::CatalogDdlRig;
using rune::codec::AlterOp;
using rune::codec::AlterOpType;
using rune::codec::CompatMode;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
namespace rc = rune::catalog;

namespace {

// "characters": id (Int64 key), userId (Int64), name (String) — 3 fields.
std::vector<uint8_t> charsBlob() {
    return rune::codec::serialize(DescriptorBuilder("characters", 1)
                                      .add("id", PrimType::Int64, 1, /*isKey=*/true)
                                      .add("userId", PrimType::Int64)
                                      .add("name", PrimType::String)
                                      .build());
}

AlterOp addOp(const std::string& name, PrimType t) {
    AlterOp op{}; op.type = AlterOpType::Add; op.name = name; op.primType = t; return op;
}
AlterOp dropOp(const std::string& name) {
    AlterOp op{}; op.type = AlterOpType::Drop; op.name = name; return op;
}
AlterOp setCompatOp(CompatMode m) {
    AlterOp op{}; op.type = AlterOpType::SetCompatibility; op.mode = m; return op;
}

/// A DefineTopic as the pre-catalog suite sent it (the "characters" descriptor).
CatalogDdlRig::Answer define(CatalogDdlRig& rig, const std::string& name, int32_t partitions, bool validate,
                             std::vector<uint8_t> blob, uint8_t persistence = 0, const std::string& keyField = "userId") {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.partitionCount = partitions;
    m.keyField = keyField; m.validate = validate; m.blob = std::move(blob); m.persistence = persistence;
    m.meta.sessionId = rig.sessionId();
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

std::size_t countEffects(const CatalogDdlRig& rig, const std::string& prefix) {
    return static_cast<std::size_t>(std::count_if(rig.effects().begin(), rig.effects().end(),
                                                  [&](const std::string& e) { return e.rfind(prefix, 0) == 0; }));
}

} // namespace

TEST(AdminWorker, DefineTopicHappyPath) {
    CatalogDdlRig rig("aw_happy", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", 16, /*validate=*/true, charsBlob());
    EXPECT_EQ(a.code, 0);
    EXPECT_EQ(a.topicId, 0) << "the first catalog id";
    ASSERT_NE(rig.schemas().byName("characters"), nullptr);
    EXPECT_EQ(countEffects(rig, "topicApplied:0"), 1u);
    EXPECT_EQ(countEffects(rig, "schemaChanged:0@v1"), 1u) << "a typed topic's validators follow it";
}

TEST(AdminWorker, RejectsMalformedDescriptor) {
    CatalogDdlRig rig("aw_malformed", 1);
    rig.becomeServingLeader();
    EXPECT_EQ(define(rig, "bad", 1, false, {0, 1, 2}).code, 23);   // InvalidSchema
    EXPECT_TRUE(rig.recordedLog().empty()) << "nothing appended";
}

TEST(AdminWorker, DuplicateNameReturnsTopicExists) {
    CatalogDdlRig rig("aw_dup", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    const auto again = define(rig, "characters", 16, false, charsBlob());
    EXPECT_EQ(again.code, 25);   // TopicExists
    EXPECT_EQ(again.topicId, 0);
    EXPECT_EQ(again.version, 1u);
    EXPECT_EQ(rig.recordedLog().size(), 1u) << "no second event";
}

TEST(AdminWorker, GetSchemaUnknownTopic) {
    CatalogDdlRig rig("aw_get_unknown", 1);
    EXPECT_EQ(rig.getSchema("nope").code, 7);   // UnknownTopic
}

TEST(AdminWorker, GetSchemaKnownTopic) {
    CatalogDdlRig rig("aw_get_known", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    EXPECT_EQ(rig.getSchema("characters").code, 0);
}

// The deleted CreateTopicFailureRespondsInternal (a failed control-thread create):
// the catalog's Internal is a HALTED applier — the DDL is never answered OK.
TEST(AdminWorker, CatalogHaltedApplyRespondsInternal) {
    CatalogDdlRig rig("aw_halted", 1);
    rig.becomeServingLeader();
    rig.admin().applierForTest().tamperNextFingerprintForTest();
    const auto a = define(rig, "characters", 16, false, charsBlob());
    EXPECT_EQ(a.code, 6);   // Internal
    EXPECT_TRUE(rig.admin().applier().halted());
    EXPECT_EQ(rig.schemas().byName("characters"), nullptr) << "no projection of an unapplied event";
    EXPECT_EQ(define(rig, "orders", 1, false, charsBlob()).code, 6) << "a halted broker accepts no catalog change";
}

// RULING-2 D: an answer only once the replica HW covers the appended event.
TEST(AdminWorkerRegression, CommitGatedOnHighWatermark) {
    CatalogDdlRig rig("aw_hw", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});   // follower 2 silent
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = "characters"; m.partitionCount = 1;
    m.blob = charsBlob(); m.keyField = "userId";
    m.meta.requestId = rig.freshRequestId();
    const int64_t corr = rig.submitRawNoWait(DdlRequest{std::move(m)});
    for (int i = 0; i < 64; ++i) rig.pump();
    EXPECT_FALSE(rig.answerFor(corr).has_value()) << "answered before the HW covered the event";
    EXPECT_EQ(rig.replica().highWatermark(), 0);
    rig.resumeFollower(2);
    for (int i = 0; i < 64 && !rig.answerFor(corr); ++i) rig.pump();
    ASSERT_TRUE(rig.answerFor(corr).has_value());
    EXPECT_EQ(rig.answerFor(corr)->code, 0);
    EXPECT_GE(rig.replica().highWatermark(), 1);
}

// B10 / D4 → the catalog: a topic is built (topicApplied) exactly once, after its
// event committed and applied; a refused / duplicate / ALTER DDL builds nothing.
TEST(AdminWorkerRegression, CatalogTopicAppliedPostedOnceAfterTheEventCommitsRegression) {
    CatalogDdlRig rig("aw_applied_once", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    EXPECT_EQ(countEffects(rig, "topicApplied:"), 1u);
    (void)define(rig, "characters", 16, false, charsBlob());   // TopicExists
    ASSERT_EQ(rig.alter("characters", {addOp("level", PrimType::Int32)}).code, 0);
    EXPECT_EQ(countEffects(rig, "topicApplied:"), 1u) << "duplicate / ALTER never re-build the topic";
}

TEST(AdminWorkerRegression, CatalogNoTopicAppliedWhenTheDdlIsRefusedRegression) {
    {
        CatalogDdlRig rig("aw_refused_follower", 3);   // never leads
        EXPECT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 2);
        EXPECT_TRUE(rig.effects().empty()) << "a follower builds nothing";
    }
    {
        CatalogDdlRig rig("aw_refused_bad", 1);
        rig.becomeServingLeader();
        EXPECT_EQ(define(rig, "bad", 1, false, {0, 1, 2}).code, 23);
        EXPECT_TRUE(rig.effects().empty());
    }
}

// RULING-2 D, catalog form: DDL is leader-only — a follower answers the
// retriable NotLeader (spec §5.2), never Internal, and appends nothing.
TEST(AdminWorkerRegression, FollowerRejectsDefine) {
    CatalogDdlRig rig("aw_follower", 3);
    rig.becomeFollower(/*leaderId=*/2);
    EXPECT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 2);
    EXPECT_TRUE(rig.recordedLog().empty());
}

// Task 10: a fresh topic is schema version 1 and nextTag starts past every field
// it declares, so the first ALTER ADD allocates a fresh tag.
TEST(AdminWorkerRegression, DefineTopicVersionOneAndNextTagEqualsFieldCount) {
    CatalogDdlRig rig("aw_nexttag", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "characters", 16, false, charsBlob());
    ASSERT_EQ(a.code, 0);
    EXPECT_EQ(a.topicId, 0);
    const RegistryEntry* e = rig.schemas().byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 1);
    EXPECT_EQ(rig.schemas().nextTagFor(e->topicId), 3);
}

// ── Task 11: ALTER TOPIC ─────────────────────────────────────────────────────

TEST(AdminWorker, AlterAddColumnBumpsVersionAndAllocatesTag) {
    CatalogDdlRig rig("aw_alter_add", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    const int32_t tid = rig.schemas().byName("characters")->topicId;
    const auto a = rig.alter("characters", {addOp("score", PrimType::Int64)});
    EXPECT_EQ(a.code, 0);
    EXPECT_EQ(a.version, 2u);
    const RegistryEntry* e = rig.schemas().byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 2);
    EXPECT_EQ(rig.schemas().nextTagFor(tid), 4);
    const auto& fields = e->descriptor.fields;
    auto it = std::find_if(fields.begin(), fields.end(), [](const auto& f) { return f.name == "score"; });
    ASSERT_NE(it, fields.end());
    EXPECT_EQ(it->id, 3);
    // F20: the event stores the op RESOLVED to its durable tag.
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.back().event.kind, rc::EventKind::AlterSchema);
    const auto& ops = std::get<rc::AlterSchemaOps>(log.back().event.ops).ops;
    ASSERT_EQ(ops.size(), 1u);
    EXPECT_EQ(ops[0].tag, 3);
}

TEST(AdminWorkerRegression, AlterDropUnderForwardRejected) {
    CatalogDdlRig rig("aw_alter_fwd", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    ASSERT_EQ(rig.alter("characters", {setCompatOp(CompatMode::Forward)}).code, 0);
    ASSERT_EQ(rig.schemas().byName("characters")->version, 2);
    EXPECT_EQ(rig.recordedLog().back().event.kind, rc::EventKind::SetCompatibility) << "ruling 3";
    EXPECT_EQ(rig.alter("characters", {dropOp("name")}).code, 30);   // SchemaIncompatible
    EXPECT_EQ(rig.schemas().byName("characters")->version, 2);
}

TEST(AdminWorkerRegression, AlterWithStaleExpectedVersionConflicts) {
    CatalogDdlRig rig("aw_alter_cas", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).code, 0);
    const auto stale = rig.alter("characters", {addOp("extra", PrimType::Int64)}, {}, /*expectedVersion=*/1);
    EXPECT_EQ(stale.code, 31);   // SchemaConflict
    EXPECT_EQ(stale.version, 2u);
    EXPECT_EQ(rig.schemas().byName("characters")->version, 2);
}

TEST(AdminWorkerRegression, MultiOpAlterIsAtomicOnFailure) {
    CatalogDdlRig rig("aw_alter_atomic", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, false, charsBlob()).code, 0);
    const int32_t tid = rig.schemas().byName("characters")->topicId;
    EXPECT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64), dropOp("ghost")}).code, 23);
    EXPECT_EQ(rig.schemas().byName("characters")->version, 1);
    EXPECT_EQ(rig.schemas().nextTagFor(tid), 3);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
}

// Task 12: every committed ALTER fans out its version's schema (the validators).
TEST(AdminWorkerRegression, AlterPushesValidatorForNewVersionWhenValidateOnRegression) {
    CatalogDdlRig rig("aw_alter_validator", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, /*validate=*/true, charsBlob()).code, 0);
    EXPECT_EQ(countEffects(rig, "schemaChanged:0@v1"), 1u);
    const auto a = rig.alter("characters", {addOp("score", PrimType::Int64)});
    EXPECT_EQ(a.code, 0);
    EXPECT_EQ(a.version, 2u);
    EXPECT_EQ(countEffects(rig, "schemaChanged:0@v2"), 1u) << "the newly committed version's validator";
}

// Task 12: the production sink pushes a validator frame only for a validate=true
// topic (AdminWorkerAgent::postCatalogValidator_ — CatalogApplierTest pins the
// frames); the schema projection still moves.
TEST(AdminWorkerRegression, AlterDoesNotPushValidatorWhenValidateOffRegression) {
    CatalogDdlRig rig("aw_alter_novalidator", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 16, /*validate=*/false, charsBlob()).code, 0);
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).code, 0);
    const auto* t = rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "characters");
    ASSERT_NE(t, nullptr);
    ASSERT_TRUE(t->schema.has_value());
    EXPECT_FALSE(t->schema->validate) << "validate=false is recorded: the sink posts no validator frame";
}

TEST(AdminWorkerRegression, GetSchemaReportsRegistryPartitionCount) {
    CatalogDdlRig rig("aw_get_parts", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 8, false, charsBlob()).code, 0);
    const auto g = rig.getSchema("characters");
    EXPECT_EQ(g.code, 0);
    EXPECT_EQ(g.partitionCount, 8);
}

TEST(AdminWorkerRegression, GetSchemaByFingerprintReportsPartitionCount) {
    CatalogDdlRig rig("aw_get_fp", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 8, false, charsBlob()).code, 0);
    const uint64_t v1Fp = rig.schemas().byName("characters")->fingerprint;
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).code, 0);
    const auto g = rig.getSchema("characters", v1Fp);
    EXPECT_EQ(g.code, 0) << "v1's fingerprint must resolve to the v1 descriptor";
    EXPECT_EQ(g.version, 1);
    EXPECT_EQ(g.partitionCount, 8);
}

TEST(AdminWorkerRegression, GetSchemaUnknownTopicReportsZeroPartitions) {
    CatalogDdlRig rig("aw_get_zero", 1);
    const auto g = rig.getSchema("nope");
    EXPECT_EQ(g.code, 7);
    EXPECT_EQ(g.partitionCount, 0);
}

TEST(AdminWorkerRegression, GetSchemaByTopicNameWhenItDiffersFromDescriptorName) {
    CatalogDdlRig rig("aw_get_name", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "typed_route", 8, false, charsBlob()).code, 0);
    const auto g = rig.getSchema("typed_route");
    EXPECT_EQ(g.code, 0) << "GetTopicSchema must resolve the DEFINED topic name";
    EXPECT_EQ(g.partitionCount, 8);
    EXPECT_EQ(rig.getSchema("characters").code, 7) << "the descriptor name was never defined as a topic";
}

TEST(AdminWorkerRegression, TwoTopicsSharingADescriptorAreDistinctTopics) {
    CatalogDdlRig rig("aw_shared_desc", 1);
    rig.becomeServingLeader();
    const auto a = define(rig, "chars_a", 1, false, charsBlob());
    ASSERT_EQ(a.code, 0);
    const auto b = define(rig, "chars_b", 4, false, charsBlob());
    ASSERT_EQ(b.code, 0) << "a second topic with the same descriptor is not TopicExists";
    EXPECT_NE(a.topicId, b.topicId);
    EXPECT_EQ(rig.schemas().byName("chars_a")->topicId, a.topicId);
    EXPECT_EQ(rig.schemas().byName("chars_b")->topicId, b.topicId);
    EXPECT_EQ(rig.schemas().byName("chars_b")->partitionCount, 4);
    EXPECT_EQ(define(rig, "chars_a", 1, false, charsBlob()).code, 25);
}

TEST(AdminWorkerRegression, AlterByTopicNameWhenItDiffersFromDescriptorName) {
    CatalogDdlRig rig("aw_alter_name", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "typed_route", 8, false, charsBlob()).code, 0);
    const auto a = rig.alter("typed_route", {addOp("score", PrimType::Int64)});
    EXPECT_EQ(a.code, 0);
    EXPECT_EQ(a.version, 2u);
    EXPECT_EQ(rig.schemas().byName("typed_route")->version, 2);
    EXPECT_EQ(rig.schemas().byName("typed_route")->partitionCount, 8);
}

// ---------------------------------------------------------------------------
// Phase 2b E: transient topics through the DDL state machine.
// ---------------------------------------------------------------------------
TEST(AdminWorkerRegression, TransientDefineRequestsTransientTopicAndRecordsMode) {
    CatalogDdlRig rig("aw_transient", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 4, false, charsBlob(), /*persistence=*/1).code, 0);
    const RegistryEntry* e = rig.schemas().byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->persistence, PersistenceMode::Transient);
    // The DURABLE record (the event every broker applies) carries the mode.
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_EQ(std::get<rc::CreateTopicOps>(log[0].event.ops).persistence, rc::Persistence::Transient);
}

TEST(AdminWorkerRegression, DiskDefineRequestsDiskTopic) {
    CatalogDdlRig rig("aw_disk", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 4, false, charsBlob()).code, 0);
    EXPECT_EQ(rig.schemas().byName("characters")->persistence, PersistenceMode::Disk);
    EXPECT_EQ(std::get<rc::CreateTopicOps>(rig.recordedLog()[0].event.ops).persistence, rc::Persistence::Disk);
}

TEST(AdminWorkerRegression, AlterCarriesTransientPersistenceForward) {
    CatalogDdlRig rig("aw_transient_alter", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 4, false, charsBlob(), /*persistence=*/1).code, 0);
    const auto a = rig.alter("characters", {addOp("email", PrimType::String)}, {}, /*expectedVersion=*/1);
    ASSERT_EQ(a.code, 0);
    ASSERT_EQ(a.version, 2u);
    const RegistryEntry* v2 = rig.schemas().byName("characters");
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(v2->version, 2u);
    EXPECT_EQ(v2->persistence, PersistenceMode::Transient);
}

TEST(AdminWorkerRegression, RedefineExistingTopicWithOtherPersistenceIsTopicExists) {
    CatalogDdlRig rig("aw_redefine_mode", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 4, false, charsBlob(), /*persistence=*/0).code, 0);
    EXPECT_EQ(define(rig, "characters", 4, false, charsBlob(), /*persistence=*/1).code, 25);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
    EXPECT_EQ(rig.schemas().byName("characters")->persistence, PersistenceMode::Disk);
}

// Every DDL request survives the gateway → admin ring exactly (its meta too); an
// unknown persistence byte / a truncated frame is MALFORMED — never cast, never a
// default applied as a real request, and session / corr survive to answer it.
TEST(AdminWorkerRegression, CatalogDdlRingFramesRoundTripEveryRequestRegression) {
    DdlMeta meta;
    meta.statement = "topic p {...};"; meta.clientName = "cli"; meta.sourceAddress = "aeron:udp?endpoint=h:1";
    meta.sessionId = 77; meta.requestId = {5, 6};
    meta.replayedFrom = rune::catalog::ReplayedFrom{"east", 41, "ops"};

    OneToOneByteRing ring{kAdminRequestRingBytes};
    DefineTopicMsg d; d.session = 7; d.corr = 8; d.name = "p"; d.blob = {1, 2}; d.persistence = 1;
    d.replicationFactor = 3; d.config.retentionMs = 99; d.config.partitionKey = "k"; d.meta = meta;
    d.keyFeatures.keyIndex = true; d.keyFeatures.autoIncrement = true; d.keyFeatures.partitioner = rune::protocol::Partitioner::Modulo;
    d.keyFeatures.autoIncrementMax = 1000; d.keyFeatures.autoIncrementReserve = 9;
    AlterTopicConfigMsg c; c.session = 7; c.corr = 9; c.name = "p"; c.expectedVersion = 4;
    c.changes = {{rc::ConfigKey::RetentionBytes, 10}, {rc::ConfigKey::MaxCompactionLagMs, -1}}; c.meta = meta;
    AlterViewMsg v; v.session = 7; v.corr = 10; v.name = "v"; v.keyField = "id"; v.persistence = 1;
    v.expectedVersion = 2; v.planBlob = {9, 9}; v.meta = meta;
    DropTopicMsg dt; dt.session = 7; dt.corr = 11; dt.name = "p"; dt.meta = meta;
    ASSERT_TRUE(encodeDdl(ring, d));
    ASSERT_TRUE(encodeDdl(ring, c));
    ASSERT_TRUE(encodeDdl(ring, v));
    ASSERT_TRUE(encodeDdl(ring, dt));
    std::vector<int32_t> types;
    ring.drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        types.push_back(tid);
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        const auto n = static_cast<std::size_t>(len);
        if (tid == kMsgDefineTopic) {
            const DdlRequest r = decodeDefineTopic(p, n);
            ASSERT_TRUE(std::holds_alternative<DefineTopicMsg>(r));
            const auto& got = std::get<DefineTopicMsg>(r);
            EXPECT_EQ(got.blob, d.blob); EXPECT_EQ(got.persistence, 1u); EXPECT_EQ(got.replicationFactor, 3);
            EXPECT_EQ(got.config, d.config); EXPECT_EQ(got.meta, meta);
            EXPECT_EQ(got.keyFeatures, d.keyFeatures);
            // The 14-byte key features block is followed by the hasLease byte (no lease here): an unknown flag bit or
            // partitioner is malformed, never cast.
            for (const auto& [at, value] : {std::pair{n - 15, uint8_t{0x04}}, std::pair{n - 14, uint8_t{2}}}) {
                std::vector<uint8_t> badFeatures(p, p + n);
                badFeatures[at] = value;
                const DdlRequest r2 = decodeDefineTopic(badFeatures.data(), badFeatures.size());
                const auto* m2 = std::get_if<MalformedDdl>(&r2);
                ASSERT_NE(m2, nullptr);
                EXPECT_EQ(m2->reason, "an unknown key feature bit or partitioner");
            }
            // Truncated: malformed, never a default request; session / corr survive.
            const DdlRequest cutR = decodeDefineTopic(p, n - 1);
            const auto* cut = std::get_if<MalformedDdl>(&cutR);
            ASSERT_NE(cut, nullptr);
            EXPECT_EQ(cut->corr, 8); EXPECT_EQ(cut->session, 7); EXPECT_EQ(cut->templateId, kTidDefineTopic);
            EXPECT_FALSE(cut->reason.empty());
            // Unknown persistence byte: malformed, never cast.
            std::vector<uint8_t> bad(p, p + n);
            bad[8 + 8 + 4 + 1 + 2 + 1 + 2 + 0 + 4 + 2] = 2;   // the persistence byte (name "p", empty key, 2-byte blob)
            const DdlRequest badR = decodeDefineTopic(bad.data(), bad.size());
            const auto* badM = std::get_if<MalformedDdl>(&badR);
            ASSERT_NE(badM, nullptr);
            EXPECT_EQ(badM->reason, "an unknown persistence byte");
        } else if (tid == kMsgAlterTopicConfig) {
            const DdlRequest r = decodeAlterTopicConfig(p, n);
            ASSERT_TRUE(std::holds_alternative<AlterTopicConfigMsg>(r));
            const auto& got = std::get<AlterTopicConfigMsg>(r);
            EXPECT_EQ(got.changes, c.changes); EXPECT_EQ(got.expectedVersion, 4u); EXPECT_EQ(got.meta, meta);
        } else if (tid == kMsgAlterView) {
            const DdlRequest r = decodeAlterView(p, n);
            ASSERT_TRUE(std::holds_alternative<AlterViewMsg>(r));
            const auto& got = std::get<AlterViewMsg>(r);
            EXPECT_EQ(got.planBlob, v.planBlob); EXPECT_EQ(got.persistence, 1u); EXPECT_EQ(got.expectedVersion, 2u);
        } else if (tid == kMsgDropTopic) {
            const DdlRequest r = decodeDropTopic(p, n);
            ASSERT_TRUE(std::holds_alternative<DropTopicMsg>(r));
            const auto& got = std::get<DropTopicMsg>(r);
            EXPECT_EQ(got.name, "p"); EXPECT_EQ(got.meta, meta);
        }
    }, 8);
    EXPECT_EQ(types, (std::vector<int32_t>{kMsgDefineTopic, kMsgAlterTopicConfig, kMsgAlterView, kMsgDropTopic}));
}

// A lease reaches the admin ring through the production submit (AdminWorkerAgent::submitDefineTopic) and decodes equal;
// no lease stays no lease.
TEST(AdminWorkerRegression, LeaseDefineSubmitReachesTheRingRegression) {
    rune::protocol::LeaseSpec spec;
    spec.ttlMs = 10'000; spec.minTtlMs = 1'000; spec.maxTtlMs = 60'000; spec.maxKeys = 9; spec.expiryBatch = 64;
    spec.checkpoint = 1; spec.checkpointIntervalMs = 2'000; spec.checkpointRate = 3;
    rune::node::SchemaRegistry schemas;
    AdminWorkerAgent agent(schemas, /*selfId=*/1, AdminDeps{});
    const rune::protocol::TopicConfigView cfg{};
    const rune::gateway::DdlMetaView meta{};
    const rune::protocol::TopicKeyFeatures kf{};
    const uint8_t blob[2] = {1, 2};
    int seen = 0;
    for (const bool withLease : {true, false}) {
        const std::optional<rune::protocol::LeaseSpec> lease = withLease ? std::optional(spec) : std::nullopt;
        ASSERT_EQ(agent.submitDefineTopic(7, 8, "p", 1, "id", false, blob, 2, 0, 0, cfg, meta, "", kf, lease),
                  rune::gateway::AdminSubmit::Accepted);
        agent.requestRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                                      aeron::util::index_t len) {
            ASSERT_EQ(tid, kMsgDefineTopic);
            const DdlRequest r = decodeDefineTopic(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            const auto* got = std::get_if<DefineTopicMsg>(&r);
            ASSERT_NE(got, nullptr);
            EXPECT_EQ(got->lease.has_value(), withLease);
            if (withLease) EXPECT_EQ(*got->lease, spec);
            ++seen;
        }, 8);
    }
    EXPECT_EQ(seen, 2);
}

// Task 9 fix round 1 (I1, hard rule 3): a frame that does not decode is its own
// variant alternative — MalformedDdl{templateId, session, corr, reason} — never a
// flag beside fields that mean nothing.  Every one of the seven DDL decoders: a
// whole frame is its message, a truncated one is MalformedDdl naming why, and a
// frame with trailing bytes is MalformedDdl naming that.
namespace {
template <typename M>
std::vector<uint8_t> ringFrame(const M& m) {
    OneToOneByteRing ring{kAdminRequestRingBytes};
    EXPECT_TRUE(encodeDdl(ring, m));
    std::vector<uint8_t> raw;
    ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        raw.assign(p, p + len);
    }, 1);
    return raw;
}
template <typename M>
void expectMalformedAlternative(const M& m, DdlRequest (*decode)(const uint8_t*, std::size_t), uint16_t tid) {
    SCOPED_TRACE(tid);
    std::vector<uint8_t> raw = ringFrame(m);
    ASSERT_FALSE(raw.empty());
    EXPECT_TRUE(std::holds_alternative<M>(decode(raw.data(), raw.size())));

    const DdlRequest cut = decode(raw.data(), raw.size() - 1);
    const auto* c = std::get_if<MalformedDdl>(&cut);
    ASSERT_NE(c, nullptr) << "a truncated frame is the Malformed alternative";
    EXPECT_EQ(c->templateId, tid);
    EXPECT_EQ(c->session, m.session);
    EXPECT_EQ(c->corr, m.corr);
    EXPECT_EQ(c->reason, "the frame is shorter than its fields");

    raw.push_back(0);
    const DdlRequest longer = decode(raw.data(), raw.size());
    const auto* l = std::get_if<MalformedDdl>(&longer);
    ASSERT_NE(l, nullptr) << "trailing bytes are the Malformed alternative";
    EXPECT_EQ(l->templateId, tid);
    EXPECT_EQ(l->reason, "trailing bytes after the last field");
}
} // namespace

TEST(AdminWorkerRegression, MalformedFrameIsTheMalformedAlternativeWithItsReasonRegression) {
    DefineTopicMsg d; d.session = 3; d.corr = 31; d.name = "p"; d.blob = {1};
    AlterTopicMsg a; a.session = 3; a.corr = 32; a.topicName = "p"; a.opsBlob = {1, 2};
    CreateViewMsg cv; cv.session = 3; cv.corr = 33; cv.viewName = "v"; cv.keyField = "id"; cv.planBlob = {7};
    DropTopicMsg dt; dt.session = 3; dt.corr = 34; dt.name = "p";
    DropViewMsg dv; dv.session = 3; dv.corr = 35; dv.name = "v";
    AlterViewMsg av; av.session = 3; av.corr = 36; av.name = "v"; av.keyField = "id"; av.planBlob = {8};
    AlterTopicConfigMsg ac; ac.session = 3; ac.corr = 37; ac.name = "p";
    ac.changes = {{rc::ConfigKey::RetentionBytes, 10}};
    expectMalformedAlternative(d, &decodeDefineTopic, kTidDefineTopic);
    expectMalformedAlternative(a, &decodeAlterTopic, kTidAlterTopic);
    expectMalformedAlternative(cv, &decodeCreateView, kTidCreateView);
    expectMalformedAlternative(dt, &decodeDropTopic, kTidDropTopic);
    expectMalformedAlternative(dv, &decodeDropView, kTidDropView);
    expectMalformedAlternative(av, &decodeAlterView, kTidAlterView);
    expectMalformedAlternative(ac, &decodeAlterTopicConfig, kTidAlterTopicConfig);
}

// A malformed request is answered (InvalidSchema / InvalidRequest), never dropped.
TEST(AdminWorkerRegression, MalformedRingRequestIsAnsweredNeverDroppedRegression) {
    CatalogDdlRig rig("aw_malformed_ring", 1);
    rig.becomeServingLeader();
    EXPECT_EQ(rig.submitRaw(DdlRequest{MalformedDdl{kTidDefineTopic, rig.sessionId(), rig.nextCorr(), "x"}}).code, 23);
    EXPECT_EQ(rig.submitRaw(DdlRequest{MalformedDdl{kTidDropTopic, rig.sessionId(), rig.nextCorr(), "x"}}).code, 39);
    EXPECT_TRUE(rig.recordedLog().empty());
}

// Issues #218 / #67: the DDL meta's security stamp crosses the admin ring bit-exact, and a stamp flag other than
// 0 / 1 is malformed (never cast).
TEST(AdminWorkerRegression, TheSecurityStampCrossesTheAdminRingAndARefusedFlagIsMalformedRegression) {
    OneToOneByteRing ring{kAdminRequestRingBytes};
    for (const bool stamped : {false, true}) {
        DropTopicMsg dt; dt.session = 7; dt.corr = 11; dt.name = "p";
        dt.meta.statement = "DROP TOPIC p;"; dt.meta.requestId = {5, 6};
        dt.meta.authenticated = "ns/bob"; dt.meta.callerNsId = 7; dt.meta.authenticatedLocalAt = 3;
        dt.meta.securityStamped = stamped; dt.meta.securityOffset = stamped ? 41 : -1;
        ASSERT_TRUE(encodeDdl(ring, dt));
        int seen = 0;
        ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                       aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto n = static_cast<std::size_t>(len);
            const DdlRequest r = decodeDropTopic(p, n);
            ASSERT_TRUE(std::holds_alternative<DropTopicMsg>(r));
            const auto& got = std::get<DropTopicMsg>(r);
            EXPECT_EQ(got.meta.securityStamped, stamped);
            EXPECT_EQ(got.meta.securityOffset, stamped ? 41 : -1);
            EXPECT_EQ(got.meta, dt.meta);
            EXPECT_EQ(got.meta.callerNsId, 7u);
            EXPECT_EQ(got.meta.authenticatedLocalAt, 3u);
            // The stamp (flag 1 + offset 8) then callerNsId 4 + authenticatedLocalAt 2 + describeMask 1 end the meta,
            // followed by the 4-byte nsId.
            std::vector<uint8_t> bad(p, p + n);
            bad[n - 4 - 16] = 2;
            const DdlRequest badR = decodeDropTopic(bad.data(), bad.size());
            const auto* badM = std::get_if<MalformedDdl>(&badR);
            ASSERT_NE(badM, nullptr);
            EXPECT_EQ(badM->reason, "a securityStamped flag other than 0/1");
            // An authenticatedLocalAt beyond the name is malformed too (the u16 sits before the describeMask byte
            // and the nsId).
            std::vector<uint8_t> badAt(p, p + n);
            badAt[n - 4 - 3] = 0; badAt[n - 4 - 2] = 0x7f;
            const DdlRequest badAtR = decodeDropTopic(badAt.data(), badAt.size());
            const auto* badAtM = std::get_if<MalformedDdl>(&badAtR);
            ASSERT_NE(badAtM, nullptr);
            EXPECT_EQ(badAtM->reason, "an authenticatedLocalAt beyond the authenticated name");
            ++seen;
        }, 8);
        EXPECT_EQ(seen, 1);
    }
}

// Issue #217: the describe mask crosses the admin ring bit-exact for every mask, a message built without it carries
// 0xFF (everything), and a meta that gains the byte keeps the fixed-bytes constant honest.
TEST(AdminWorkerRegression, TheDescribeMaskCrossesTheAdminRingAndDefaultsToEverythingRegression) {
    {
        DropTopicMsg dt;   // aggregate default: the full answers
        EXPECT_EQ(dt.meta.describeMask, 0xFF);
        EXPECT_EQ(DdlMeta{}.describeMask, 0xFF);
    }
    OneToOneByteRing ring{kAdminRequestRingBytes};
    for (const uint8_t mask : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{4}, uint8_t{7}, uint8_t{0xFF}}) {
        DropTopicMsg dt; dt.session = 7; dt.corr = 11; dt.name = "p";
        dt.meta.statement = "DROP TOPIC p;"; dt.meta.requestId = {5, 6};
        dt.meta.describeMask = mask;
        ASSERT_TRUE(encodeDdl(ring, dt));
        int seen = 0;
        ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                       aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const DdlRequest r = decodeDropTopic(p, static_cast<std::size_t>(len));
            ASSERT_TRUE(std::holds_alternative<DropTopicMsg>(r));
            EXPECT_EQ(std::get<DropTopicMsg>(r).meta.describeMask, mask);
            EXPECT_EQ(std::get<DropTopicMsg>(r).meta, dt.meta);
            // A frame one byte short (the mask missing) is malformed, never read as 0xFF: the meta is not the frame's
            // last field, so there is no "old-length frame" - the ring never leaves the process.
            const DdlRequest shortR = decodeDropTopic(p, static_cast<std::size_t>(len) - 1);
            EXPECT_TRUE(std::holds_alternative<MalformedDdl>(shortR));
            ++seen;
        }, 8);
        EXPECT_EQ(seen, 1);
    }
}

namespace {
// A catalog leader with `events` golden __security events applied by its own admin thread (the stamp's reference).
struct StampRig {
    CatalogDdlRig               cat;
    rune::node::test::SecurityRig sec;
    explicit StampRig(const char* tag, std::size_t events)
        : cat(tag, 1), sec(tag, /*goLive=*/false) {
        cat.becomeServingLeader();
        sec.appendEvents(0, events);
        (void)sec.applier.replayLocal(sec.log(), sec.rec.bootEffects());
        cat.admin().attachSecurity(std::move(sec.applier), &sec.replica(), sec.rec.liveEffects());
        for (int i = 0; i < 64; ++i) cat.pump();
    }
    [[nodiscard]] int64_t applied() { return cat.admin().securityApplier()->state().appliedOffset(); }
    /// One more security event reaches the admin thread's applier (the gateway's mirror has not seen it yet).
    void applyOneMoreEvent(std::size_t index) {
        sec.appendEvents(index, index + 1);
        for (int i = 0; i < 64; ++i) cat.pump();
    }
};
}

TEST(AdminWorkerRegression, AStaleAuthorizationStampIsRetriedNotAppendedRegression) {
    StampRig rig("aw_stale_stamp", 4);
    ASSERT_EQ(rig.applied(), 3);
    auto& cat = rig.cat;
    // stamp == applied: proceeds.
    cat.stampSecurity(rig.applied());
    ASSERT_EQ(cat.define({.name = "orders"}).code, 0);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 0u);
    // One security event (a REVOKE of the requester's right) applies after the stamp.
    const int64_t stampS = rig.applied();
    rig.applyOneMoreEvent(4);
    ASSERT_EQ(rig.applied(), stampS + 1);
    const std::size_t logged = cat.recordedLog().size();
    const auto rid = cat.freshRequestId();
    const auto stale = cat.define({.name = "late", .requestId = rid});
    EXPECT_EQ(stale.code, 2) << stale.detail;   // NotLeader
    EXPECT_EQ(cat.recordedLog().size(), logged) << "nothing was appended for the stale request";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 1u);
    // The retry with the same request id and the current stamp (authorized afresh) applies once.
    cat.stampSecurity(rig.applied());
    EXPECT_EQ(cat.define({.name = "late", .requestId = rid}).code, 0);
    EXPECT_EQ(cat.recordedLog().size(), logged + 1);
    // A stamp beyond the applied offset (impossible: the mirror trails the applier) is stale too.
    cat.stampSecurity(rig.applied() + 1);
    EXPECT_EQ(cat.define({.name = "ahead"}).code, 2);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 2u);
    const auto drop = cat.dropTopic("orders");   // a DDL answer that carries a detail
    EXPECT_EQ(drop.code, 2);
    EXPECT_EQ(drop.detail, "the security state changed while this request was in flight; retry");
    EXPECT_EQ(cat.recordedLog().size(), logged + 1) << "the stale drop appended nothing";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 3u);
    // Unstamped (authorization off / in-process): proceeds whatever the applied offset.
    cat.unstampSecurity();
    EXPECT_EQ(cat.define({.name = "unstamped"}).code, 0);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 3u);
}

TEST(AdminWorkerRegression, ARecordedRequestIsAnsweredBeforeTheStampIsCheckedRegression) {
    StampRig rig("aw_stale_dedupe", 4);
    auto& cat = rig.cat;
    cat.stampSecurity(rig.applied());
    const auto rid = cat.freshRequestId();
    const auto first = cat.define({.name = "orders", .requestId = rid});
    ASSERT_EQ(first.code, 0) << first.detail;
    rig.applyOneMoreEvent(4);   // the stamp is now stale
    const std::size_t logged = cat.recordedLog().size();
    const auto again = cat.define({.name = "orders", .requestId = rid});
    EXPECT_EQ(again.code, 0) << "the ORIGINAL result, not a retry: " << again.detail;
    EXPECT_EQ(again.topicId, first.topicId);
    EXPECT_EQ(cat.recordedLog().size(), logged);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 0u);
}

// #67: a CREATE VIEW authorized (READ on its source) before a REVOKE READ applied is not created.
TEST(AdminWorkerRegression, ACreateViewAuthorizedBeforeARevokeIsRetriedNotCreatedRegression) {
    StampRig rig("aw_stale_view", 4);
    auto& cat = rig.cat;
    ASSERT_EQ(cat.define({.name = "orders", .typed = true}).code, 0);   // unstamped set-up
    cat.stampSecurity(rig.applied());
    rig.applyOneMoreEvent(4);   // REVOKE READ ON TOPIC orders lands before the admin thread starts the view
    const std::size_t logged = cat.recordedLog().size();
    const auto stale = cat.createView("orders_v", "orders");
    EXPECT_EQ(stale.code, 2) << stale.detail;
    EXPECT_EQ(cat.recordedLog().size(), logged) << "the view was not created";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 1u);
}

// Per principal: an event about ANOTHER user does not stale a catalog DDL; a role-side change stales its members
// and an event about the requester stales it.  The requester "admin" is the bootstrap user; golden events 0..7 are
// applied, 8 is about ws1, 9 revokes a role's grant, 10 is about admin.
TEST(AdminWorkerRegression, AnEventAboutAnotherUserDoesNotStaleACatalogDdlRegression) {
    StampRig rig("aw_stale_principal", 8);
    auto& cat = rig.cat;
    ASSERT_EQ(rig.applied(), 7);
    cat.stampSecurity(rig.applied());
    rig.applyOneMoreEvent(8);   // ALTER USER ws1: another user
    ASSERT_EQ(rig.applied(), 8);
    EXPECT_EQ(cat.define({.name = "orders", .authenticated = "admin"}).code, 0) << "no retry for a pipelined DDL";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 0u);
    cat.stampSecurity(rig.applied());
    rig.applyOneMoreEvent(9);   // REVOKE from a ROLE: stale only for its members (ws1 is one, admin is not)
    EXPECT_EQ(cat.define({.name = "late", .authenticated = "admin"}).code, 0) << "a non-member proceeds";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 0u);
    EXPECT_EQ(cat.define({.name = "by_member", .authenticated = "ws1"}).code, 2) << "a member is stale";
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 1u);
    cat.stampSecurity(rig.applied());
    rig.applyOneMoreEvent(10);   // ALTER USER admin: the requester itself
    EXPECT_EQ(cat.define({.name = "later", .authenticated = "admin"}).code, 2);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 2u);
}

// A security event appended but not yet applied is classified with the same rule: it cannot slip between the stamp and
// the catalog append.
TEST(AdminWorkerRegression, ASecurityEventInFlightAboutTheRequesterStalesACatalogDdlRegression) {
    StampRig rig("aw_inflight", 8);
    auto& cat = rig.cat;
    AdminRoleMsg role;
    role.leader = true; role.epoch = 1; role.leaderId = 1; role.partitionEpoch = 0; role.isrCount = 2;
    role.isr[0] = 1; role.isr[1] = 2;
    cat.admin().onSecurityRole(role);   // follower 2 never fetches: an appended event stays uncommitted
    for (int i = 0; i < 64 && cat.admin().securityAdmin()->roleName() != "LeaderServing"; ++i) cat.pump();
    ASSERT_EQ(cat.admin().securityAdmin()->roleName(), "LeaderServing");
    SecurityDdlMsg m;
    m.session = cat.sessionId(); m.corr = cat.nextCorr();
    m.kind = static_cast<uint8_t>(rune::protocol::client::SecurityOpKind::AlterUser);
    m.name = "ws1"; m.change = static_cast<uint8_t>(rune::protocol::client::UserChangeKind::Disable);
    m.meta.clientName = "t"; m.meta.statement = "ALTER USER ws1 DISABLE;"; m.meta.authenticated = "admin";
    m.meta.requestId = cat.freshRequestId(); m.meta.sessionId = cat.sessionId();
    cat.admin().submitSecurity(SecurityRequest{std::move(m)});
    for (int i = 0; i < 16 && !cat.admin().securityAdmin()->hasInFlight(); ++i) cat.pump();
    ASSERT_TRUE(cat.admin().securityAdmin()->hasInFlight());
    const int64_t stamp = rig.applied();   // the in-flight event is not applied yet
    cat.stampSecurity(stamp);
    const std::size_t logged = cat.recordedLog().size();
    // A DDL from ws1 while an event about ws1 is in flight: retry, nothing appended.
    EXPECT_EQ(cat.define({.name = "by_ws1", .authenticated = "ws1"}).code, 2);
    EXPECT_EQ(cat.recordedLog().size(), logged);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 1u);
    // The same while the event is about another user: proceeds.
    EXPECT_EQ(cat.define({.name = "by_admin", .authenticated = "admin"}).code, 0);
    EXPECT_EQ(cat.admin().ddlStaleAuthorization(), 1u);
}

// No-hang wave (R1): an unknown persistence byte is never defaulted to Disk.
TEST(AdminWorkerRegression, DirectDefineWithUnknownPersistenceIsInvalidSchemaRegression) {
    CatalogDdlRig rig("aw_bad_mode", 1);
    rig.becomeServingLeader();
    EXPECT_EQ(define(rig, "characters", 4, false, charsBlob(), /*persistence=*/2).code, 23);
    EXPECT_TRUE(rig.recordedLog().empty()) << "nothing appended";
    EXPECT_EQ(rig.schemas().byName("characters"), nullptr);
}

// No-hang fix round 3: the DDL queue is bounded — a request beyond kMaxPendingDdl
// is answered Internal at once instead of queued forever.
TEST(AdminWorkerRegression, DdlQueueIsBoundedAndOverflowIsAnsweredInternalRegression) {
    CatalogDdlRig rig("aw_queue", 1);
    for (std::size_t i = 0; i < AdminWorker::kMaxPendingDdl; ++i) {
        DefineTopicMsg m; m.session = 1; m.corr = static_cast<int64_t>(i); m.name = "t" + std::to_string(i);
        m.partitionCount = 1;
        rig.admin().submit(DdlRequest{std::move(m)});
    }
    ASSERT_EQ(rig.admin().pendingCount(), AdminWorker::kMaxPendingDdl);
    DefineTopicMsg over; over.session = 1; over.corr = 999; over.name = "over"; over.partitionCount = 1;
    rig.admin().submit(DdlRequest{std::move(over)});
    ASSERT_TRUE(rig.answerFor(999).has_value()) << "a DDL beyond the bound was queued (or dropped)";
    EXPECT_EQ(rig.answerFor(999)->code, 6);
    EXPECT_EQ(rig.admin().pendingCount(), AdminWorker::kMaxPendingDdl);
    EXPECT_EQ(rig.admin().pendingDdlRefused(), 1u);
    AlterTopicMsg a; a.session = 1; a.corr = 1000; a.topicName = "t0";
    rig.admin().submit(DdlRequest{std::move(a)});
    ASSERT_TRUE(rig.answerFor(1000).has_value());
    EXPECT_EQ(rig.answerFor(1000)->code, 6);
}

// #418: GetTopicSchema with kSchemaLineageFingerprint answers the topic's EVOLVING descriptor (every field ever
// declared, the retired ones with their lifetimes) at the latest version, so a typed reader can place a record of
// any version; a plain request still answers the materialized latest version.
TEST(AdminWorkerRegression, GetSchemaLineageAnswersTheEvolvingDescriptorRegression) {
    CatalogDdlRig rig("aw_lineage", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 4, false, charsBlob()).code, 0);
    ASSERT_EQ(rig.alter("characters", {dropOp("userId")}).code, 0);                // v2: a mid-block DROP
    ASSERT_EQ(rig.alter("characters", {addOp("level", PrimType::Int32)}).code, 0);  // v3
    const auto lin = rig.getSchema("characters", rune::protocol::client::kSchemaLineageFingerprint);
    ASSERT_EQ(lin.code, 0);
    EXPECT_EQ(lin.version, 3u);
    EXPECT_EQ(lin.partitionCount, 4);
    const auto ev = rune::codec::deserialize(lin.blob);
    ASSERT_NE(ev.find("userId"), nullptr) << "the retired field stays in the lineage";
    EXPECT_EQ(ev.find("userId")->untilVersion, 2u);
    ASSERT_NE(ev.find("level"), nullptr);
    EXPECT_EQ(ev.find("level")->sinceVersion, 3u);
    // The rows a v1 reader needs: userId at 8 in v1, absent from v2 on; level absent before v3.
    EXPECT_EQ(rune::codec::materializeAsOf(ev, 1).find("userId")->offset, 8u);
    EXPECT_EQ(rune::codec::materializeAsOf(ev, 2).find("userId"), nullptr);

    const auto latest = rig.getSchema("characters");
    ASSERT_EQ(latest.code, 0);
    EXPECT_EQ(rune::codec::deserialize(latest.blob).find("userId"), nullptr) << "the plain answer is the materialized v3";
    EXPECT_EQ(rig.getSchema("nope", rune::protocol::client::kSchemaLineageFingerprint).code, 7) << "UnknownTopic";
}

// ── Issue #296: DDL admission against this broker's open-file limit ─────────────────────────────────────────────────
namespace {
CatalogDdlRig::Answer defineWith(CatalogDdlRig& rig, const std::string& name, int32_t partitions,
                                 int64_t segmentBytes = 64LL << 20, int64_t retentionBytes = -1) {
    CatalogDdlRig::DefineArgs a;
    a.name = name; a.partitions = partitions;
    a.config.segmentRollBytes = segmentBytes;
    a.config.retentionBytes = retentionBytes;
    return rig.define(std::move(a));
}
}  // namespace

TEST(AdminWorkerRegression, ADdlThatExceedsTheFdBudgetIsRefusedBeforeItIsAppendedRegression) {
    CatalogDdlRig rig("aw_fd_create", 1, 1, 0, /*fdLimit=*/fdNeedBase() + 100);
    rig.becomeServingLeader();
    const std::size_t events = rig.recordedLog().size();
    const auto big = defineWith(rig, "huge", 1000);
    EXPECT_EQ(big.code, 39) << "InvalidRequest";
    EXPECT_NE(big.detail.find("CREATE TOPIC would raise this broker's estimated open-file need to"), std::string::npos)
        << big.detail;
    EXPECT_NE(big.detail.find("open-file limit " + std::to_string(fdNeedBase() + 100)), std::string::npos) << big.detail;
    EXPECT_EQ(rig.recordedLog().size(), events) << "nothing appended";
    EXPECT_EQ(rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "huge"), nullptr);
    EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 1u);
    EXPECT_EQ(defineWith(rig, "small", 1).code, 0) << "a topic within the budget is admitted";
    EXPECT_EQ(rig.recordedLog().size(), events + 1);
}

TEST(AdminWorkerRegression, AnAlterThatShrinksSegmentsPastTheFdBudgetIsRefusedRegression) {
    // segment.bytes is fixed at CREATE; ALTER changes retention.bytes - with small segments a large retention opens
    // many segments per partition.
    CatalogDdlRig rig("aw_fd_alter", 1, 1, 0, /*fdLimit=*/fdNeedBase() + 100);
    rig.becomeServingLeader();
    ASSERT_EQ(defineWith(rig, "t", 1, /*segmentBytes=*/1LL << 20).code, 0);
    const auto* before = rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "t");
    ASSERT_NE(before, nullptr);
    const uint32_t version = before->version;
    const std::size_t events = rig.recordedLog().size();
    const auto a = rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 1LL << 30}});
    EXPECT_EQ(a.code, 39);
    EXPECT_NE(a.detail.find("ALTER TOPIC would raise"), std::string::npos) << a.detail;
    EXPECT_EQ(rig.recordedLog().size(), events) << "nothing appended";
    const auto* after = rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "t");
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->version, version) << "config unchanged";
    EXPECT_EQ(after->config.retentionBytes, -1);
    EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 10LL << 20}}).code, 0) << "within the budget";
}

TEST(AdminWorkerRegression, ADdlThatLowersTheNeedIsAdmittedWhileOverBudgetRegression) {
    CatalogDdlRig rig("aw_fd_lower", 1);   // no limit while the topic is made
    rig.becomeServingLeader();
    ASSERT_EQ(defineWith(rig, "t", 100, 1LL << 20, 100LL << 20).code, 0);   // 100 x (101 x 2 + 2) fds
    rig.admin().setFdLimitForTest(fdNeedBase() + 100);   // restarted with a lower limit: over budget now
    EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 50LL << 20}}).code, 0)
        << "a smaller retention lowers the need: admitted although still above the limit";
    EXPECT_EQ(rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "t")->config.retentionBytes, 50LL << 20);
    EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 60LL << 20}}).code, 39) << "a raise is refused";
    EXPECT_EQ(defineWith(rig, "more", 1).code, 39) << "a new topic only raises the need";
    EXPECT_EQ(rig.dropTopic("t").code, 0) << "a drop only lowers the need";
    EXPECT_EQ(rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "t"), nullptr);
    EXPECT_EQ(defineWith(rig, "more", 1).code, 0) << "back within the budget";
}

TEST(AdminWorkerRegression, AnUnknownFdLimitNeverRefusesRegression) {
    CatalogDdlRig rig("aw_fd_unknown", 1);   // fdLimit 0: getrlimit failed / not wired
    rig.becomeServingLeader();
    EXPECT_EQ(defineWith(rig, "huge", 1000).code, 0);
    EXPECT_EQ(rig.alterConfig("huge", {{rc::ConfigKey::RetentionBytes, 1LL << 40}}).code, 0);
    EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 0u);
}

// #612: the budget is the smallest open-file limit in the cluster, not only this broker's own.  The control thread
// tells the admin thread the smallest limit any broker advertised and whose it is (setClusterFdLimit).
TEST(AdminWorkerRegression, ADdlAboveAPeersFdBudgetIsRefusedBeforeItIsAppendedRegression) {
    const uint64_t small = fdNeedBase() + 100;
    const uint64_t large = 10'000'000;
    {   // own limit large, a peer's small: refused, naming the peer
        CatalogDdlRig rig("aw_fd_peer_create", 1, 1, 0, /*fdLimit=*/large);
        rig.becomeServingLeader();
        rig.admin().setClusterFdLimit(small, 3);
        EXPECT_EQ(rig.admin().clusterFdLimit(), small);
        EXPECT_EQ(rig.admin().clusterFdLimitNode(), 3);
        const std::size_t events = rig.recordedLog().size();
        const auto big = defineWith(rig, "big", 1000, 1LL << 20, 10LL << 20);
        EXPECT_EQ(big.code, 39) << "InvalidRequest: " << big.detail;
        const std::string want = "above the open-file limit " + std::to_string(small) +
                                 " of broker 3 (the smallest in the cluster)";
        EXPECT_NE(big.detail.find(want), std::string::npos) << big.detail;
        EXPECT_EQ(rig.recordedLog().size(), events) << "nothing appended";
        EXPECT_EQ(rig.admin().applier().state().topicByName(rune::catalog::kDefaultNsId, "big"), nullptr);
        EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 1u);
        EXPECT_EQ(defineWith(rig, "small", 1).code, 0) << "a topic within the smallest budget is admitted";
    }
    {   // the cluster limit above the own one: the own limit rules, with the unchanged wording
        CatalogDdlRig rig("aw_fd_peer_above", 1, 1, 0, /*fdLimit=*/small);
        rig.becomeServingLeader();
        rig.admin().setClusterFdLimit(large, 3);
        const auto big = defineWith(rig, "big", 1000);
        EXPECT_EQ(big.code, 39) << big.detail;
        EXPECT_NE(big.detail.find("above its open-file limit " + std::to_string(small)), std::string::npos)
            << big.detail;
        EXPECT_EQ(big.detail.find("of broker"), std::string::npos) << big.detail;
        EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 1u);
    }
    {   // an unknown own limit does not hide a known peer limit
        CatalogDdlRig rig("aw_fd_peer_ownzero", 1, 1, 0, /*fdLimit=*/0);
        rig.becomeServingLeader();
        rig.admin().setClusterFdLimit(small, 2);
        const auto big = defineWith(rig, "big", 1000);
        EXPECT_EQ(big.code, 39) << big.detail;
        EXPECT_NE(big.detail.find("of broker 2"), std::string::npos) << big.detail;
    }
    {   // both unknown: admitted
        CatalogDdlRig rig("aw_fd_peer_bothzero", 1, 1, 0, /*fdLimit=*/0);
        rig.becomeServingLeader();
        EXPECT_EQ(defineWith(rig, "big", 1000).code, 0);
        EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 0u);
    }
    {   // an ALTER that grows the need: the same table
        CatalogDdlRig rig("aw_fd_peer_alter", 1, 1, 0, /*fdLimit=*/large);
        rig.becomeServingLeader();
        ASSERT_EQ(defineWith(rig, "t", 1, 1LL << 20).code, 0);
        rig.admin().setClusterFdLimit(small, 3);
        const std::size_t events = rig.recordedLog().size();
        const auto a = rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 1LL << 30}});
        EXPECT_EQ(a.code, 39) << a.detail;
        EXPECT_NE(a.detail.find("of broker 3 (the smallest in the cluster)"), std::string::npos) << a.detail;
        EXPECT_EQ(rig.recordedLog().size(), events);
        EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 10LL << 20}}).code, 0) << "within the budget";
        rig.admin().setClusterFdLimit(0, -1);   // the peer is not known any more
        EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 1LL << 30}}).code, 0) << "own limit only";
    }
}

// ── PR #611 review, adversarial pass (issue #296: DDL admission against the open-file budget) ──────────────────────
namespace {
uint64_t needOfOne(int32_t partitions, int64_t segmentBytes, int64_t retentionBytes) {
    TopicSpec t;
    t.partitionCount = partitions;
    t.segmentRollBytes = segmentBytes;
    t.retentionBytes = retentionBytes;
    return fdNeedOfTopic(t, 0);
}
}  // namespace

// Input bounds: a need of exactly the limit is admitted, one fd more is refused (no off-by-one in either direction).
TEST(AdminWorkerRegression, TheFdBudgetAdmitsANeedOfExactlyTheLimitAdversarialRegression) {
    const uint64_t one = needOfOne(3, 64LL << 20, -1);
    {
        CatalogDdlRig rig("aw_fd_adv_eq", 1, 1, 0, /*fdLimit=*/fdNeedBase() + one);
        rig.becomeServingLeader();
        EXPECT_EQ(defineWith(rig, "t", 3).code, 0) << "need == limit is within the budget";
    }
    {
        CatalogDdlRig rig("aw_fd_adv_gt", 1, 1, 0, /*fdLimit=*/fdNeedBase() + one - 1);
        rig.becomeServingLeader();
        EXPECT_EQ(defineWith(rig, "t", 3).code, 39) << "need == limit + 1 is refused";
        EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 1u);
    }
}

// Input bounds / wrap: retention.bytes may be anything up to INT64_MAX and segment.bytes as small as 1, so the per-topic
// term overflows uint64.  A wrapped (small) estimate must never admit a topic whose true need is astronomically above the
// limit; the estimate saturates instead.
TEST(AdminWorkerRegression, AnFdNeedThatWouldOverflowIsRefusedNotWrappedAdversarialRegression) {
    CatalogDdlRig rig("aw_fd_adv_wrap", 1, 1, 0, /*fdLimit=*/1'000'000);
    rig.becomeServingLeader();
    const std::size_t events = rig.recordedLog().size();
    for (const int32_t parts : {1, 2, 4, 8, 1000}) {
        const auto a = defineWith(rig, "wrap" + std::to_string(parts), parts, /*segmentBytes=*/1,
                                  /*retentionBytes=*/int64_t{1} << 62);
        EXPECT_EQ(a.code, 39) << parts << " partitions: " << a.detail;
    }
    const auto max = defineWith(rig, "wrapmax", 7, 1, INT64_MAX);
    EXPECT_EQ(max.code, 39) << max.detail;
    EXPECT_EQ(rig.recordedLog().size(), events) << "nothing appended";
    TopicSpec t;
    t.partitionCount = 4; t.segmentRollBytes = 1; t.retentionBytes = int64_t{1} << 62;
    EXPECT_EQ(fdNeedOfTopic(t, 0), UINT64_MAX) << "the term saturates";
    EXPECT_EQ(estimateFdNeed(std::span<const TopicSpec>(&t, 1), 0), UINT64_MAX) << "and so does the sum";
}

// Protocol state (replay): a retry of a COMMITTED CREATE (same requestId) gets its original OK even after this broker's
// limit dropped below the need - dedupe runs before the budget, never a 39 for a topic that exists.
TEST(AdminWorkerRegression, ARetryOfACommittedCreateKeepsItsOkAfterTheLimitDropsAdversarialRegression) {
    CatalogDdlRig rig("aw_fd_adv_retry", 1);
    rig.becomeServingLeader();
    CatalogDdlRig::DefineArgs a;
    a.name = "t"; a.partitions = 100; a.requestId = rune::protocol::RequestId{0x5151, 7};
    a.config.segmentRollBytes = 1LL << 20; a.config.retentionBytes = 100LL << 20;
    const auto first = rig.define(a);
    ASSERT_EQ(first.code, 0);
    rig.admin().setFdLimitForTest(fdNeedBase() + 1);
    const std::size_t events = rig.recordedLog().size();
    const auto again = rig.define(a);
    EXPECT_EQ(again.code, 0) << again.detail;
    EXPECT_EQ(again.topicId, first.topicId);
    EXPECT_EQ(rig.recordedLog().size(), events) << "a retry appends nothing";
    EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 0u);
}

// Escalation through a sibling op: over budget, an ALTER of a key the estimate does not read (retention.ms) leaves the
// need unchanged and is admitted; an ALTER that raises retention.bytes on ANOTHER topic is refused (the sum counts
// every topic, not only the altered one).
TEST(AdminWorkerRegression, OverBudgetANeutralAlterIsAdmittedAndARaiseOnAnyTopicRefusedAdversarialRegression) {
    CatalogDdlRig rig("aw_fd_adv_sib", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(defineWith(rig, "big", 100, 1LL << 20, 100LL << 20).code, 0);
    ASSERT_EQ(defineWith(rig, "small", 1, 1LL << 20, -1).code, 0);
    rig.admin().setFdLimitForTest(fdNeedBase() + 100);
    EXPECT_EQ(rig.alterConfig("big", {{rc::ConfigKey::RetentionMs, 60'000}}).code, 0) << "need unchanged";
    EXPECT_EQ(rig.alterConfig("small", {{rc::ConfigKey::RetentionBytes, 100LL << 20}}).code, 39)
        << "small alone fits, but the broker's total is above the limit and grows";
    EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 1u);
}

// #204: a security DDL answer that cannot be posted (it does not fit one egress message) halts the broker in every
// build type; it used to be dropped silently (gated) or caught by an assert only in debug builds (ungated).
TEST(PostSecurityResponseDeathRegression, AnAnswerThatCannotBePostedHaltsRegression) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    for (const int64_t gateOffset : {int64_t{-1}, int64_t{5}}) {   // ungated, gated
        EXPECT_DEATH({
            // spdlog's default sink is stdout: send the critical line to stderr, where a death test reads it.
            spdlog::set_default_logger(spdlog::create<spdlog::sinks::stderr_sink_mt>("death"));
            rune::node::SchemaRegistry schemas;
            // The smallest ring (256 B, 32 B per message) is below one bare DdlResponse.
            AdminWorkerAgent agent(schemas, /*selfId=*/1, AdminDeps{}, nullptr, nullptr, nullptr,
                                   /*egressRingBytes=*/256);
            agent.postSecurityResponse(/*session=*/7, /*corr=*/9, /*code=*/0, 0, 0, gateOffset, "ok");
        }, "halting rather than dropping the answer") << "gateOffset " << gateOffset;
    }
}

// ── schema retire (#419): ALTER TOPIC <t> RETIRE VERSION < N ─────────────────────────────────────────────────────────────
namespace {
AlterOp retireOp(uint16_t below) {
    AlterOp op{}; op.type = AlterOpType::RetireBelow; op.version = below; return op;
}
/// A typed "characters" topic with `versions` schema versions, at the retire feature level.
int32_t retireFixture(CatalogDdlRig& rig, uint16_t versions) {
    rig.admin().setClusterFeatureLevel(rune::replication::MetadataFeature::kSchemaRetire);
    rig.becomeServingLeader();
    const auto d = define(rig, "characters", 2, /*validate=*/true, charsBlob());
    EXPECT_EQ(d.code, 0);
    for (uint16_t v = 2; v <= versions; ++v)
        EXPECT_EQ(rig.alter("characters", {addOp("f" + std::to_string(v), PrimType::Int64)}).code, 0);
    return d.topicId;
}
}  // namespace

TEST(AdminWorkerRegression, RetireIsAcceptedAsAFormatFiveEventAndOnlyTheWriteSpecFollowsRegression) {
    CatalogDdlRig rig("aw_retire_ok", 1);
    const int32_t tid = retireFixture(rig, 3);
    const std::size_t schemaEffects = countEffects(rig, "schemaChanged:");
    const std::size_t logSize = rig.recordedLog().size();
    const auto a = rig.alter("characters", {retireOp(3)});
    ASSERT_EQ(a.code, 0) << a.detail;
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), logSize + 1);
    const auto& ev = log.back().event;
    EXPECT_EQ(ev.kind, rc::EventKind::RetireSchemaVersions);
    EXPECT_EQ(ev.formatVer, rc::kFormatV5);
    EXPECT_EQ(std::get<rc::RetireSchemaVersionsOps>(ev.ops).below, 3);
    const RegistryEntry* e = rig.schemas().byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 3) << "the current schema version does not move";
    EXPECT_EQ(e->retiredBelow, 3);
    EXPECT_EQ(rig.admin().applier().state().topicByTopicId(tid)->schema->retiredBelow, 3);
    // the effect is the write spec ONLY: no validator re-post, no filter schema (schemaChanged)
    EXPECT_EQ(countEffects(rig, "schemaChanged:"), schemaEffects);
    ASSERT_FALSE(rig.writeSpecEffects().empty());
    EXPECT_EQ(rig.writeSpecEffects().back(), (CatalogDdlRig::FloorPost{tid, 3}));
    // a later ALTER keeps the floor in the registry and in the write spec the workers get
    ASSERT_EQ(rig.alter("characters", {addOp("f4", PrimType::Int64)}).code, 0);
    EXPECT_EQ(rig.schemas().byName("characters")->retiredBelow, 3);
    EXPECT_EQ(rig.writeSpecEffects().back(), (CatalogDdlRig::FloorPost{tid, 3}));
    // two retires in sequence
    EXPECT_EQ(rig.alter("characters", {retireOp(4)}).code, 0);
    EXPECT_EQ(rig.schemas().byName("characters")->retiredBelow, 4);
}

TEST(AdminWorkerRegression, RetireRefusalsAnswerWithTheApplyRulesTextAndAppendNothingRegression) {
    CatalogDdlRig rig("aw_retire_refuse", 1);
    retireFixture(rig, 3);
    ASSERT_EQ(rig.alter("characters", {retireOp(2)}).code, 0);
    const std::size_t logSize = rig.recordedLog().size();
    struct Case { const char* name; uint16_t below; const char* why; };
    const Case cases[] = {
        {"the same floor again", 2, "already retired"},
        {"a lower floor", 1, "already retired"},
        {"zero", 0, "already retired"},
        {"past the current version", 4, "above the current version 3"},
        {"far past", 60000, "above the current version 3"},
    };
    for (const Case& c : cases) {
        const auto r = rig.alter("characters", {retireOp(c.below)});
        EXPECT_EQ(r.code, 23) << c.name;   // InvalidSchema, as every schema DDL refusal
        EXPECT_NE(r.detail.find(c.why), std::string::npos) << c.name << ": " << r.detail;
    }
    // combined with another operation: refused whole, text names it
    const auto combined = rig.alter("characters", {addOp("zzz", PrimType::Int64), retireOp(3)});
    EXPECT_EQ(combined.code, 23);
    EXPECT_NE(combined.detail.find("RETIRE VERSION cannot be combined"), std::string::npos) << combined.detail;
    const auto twice = rig.alter("characters", {retireOp(3), retireOp(3)});
    EXPECT_EQ(twice.code, 23);
    EXPECT_NE(twice.detail.find("RETIRE VERSION cannot be combined"), std::string::npos) << twice.detail;
    // an unknown topic and an untyped one
    EXPECT_EQ(rig.alter("nope", {retireOp(2)}).code, 7);
    DefineTopicMsg raw;
    raw.session = rig.sessionId(); raw.corr = rig.nextCorr(); raw.name = "plain"; raw.partitionCount = 1;
    raw.meta.sessionId = rig.sessionId(); raw.meta.requestId = rig.freshRequestId();
    ASSERT_EQ(rig.submitRaw(DdlRequest{std::move(raw)}).code, 0);
    EXPECT_EQ(rig.alter("plain", {retireOp(2)}).code, 7) << "an untyped topic has no schema";
    EXPECT_EQ(rig.recordedLog().size(), logSize + 1) << "only the untyped topic's create was appended";
    EXPECT_EQ(rig.schemas().byName("characters")->retiredBelow, 2) << "no refusal moved the floor";
}

TEST(AdminWorkerRegression, RetireIsRefusedBelowTheClusterFeatureLevelRegression) {
    CatalogDdlRig rig("aw_retire_level", 1);
    rig.admin().setClusterFeatureLevel(rune::replication::MetadataFeature::kLeases);   // one below
    rig.becomeServingLeader();
    ASSERT_EQ(define(rig, "characters", 1, true, charsBlob()).code, 0);
    ASSERT_EQ(rig.alter("characters", {addOp("score", PrimType::Int64)}).code, 0);
    const std::size_t logSize = rig.recordedLog().size();
    const auto r = rig.alter("characters", {retireOp(2)});
    EXPECT_EQ(r.code, 39);   // InvalidRequest
    EXPECT_NE(r.detail.find("needs every broker upgraded"), std::string::npos) << r.detail;
    EXPECT_EQ(rig.recordedLog().size(), logSize) << "no event is written";
    rig.admin().setClusterFeatureLevel(rune::replication::MetadataFeature::kSchemaRetire);
    EXPECT_EQ(rig.alter("characters", {retireOp(2)}).code, 0) << "the same statement is fine once the level is reached";
}

TEST(AdminWorkerRegression, ATypedOpenOfARetiredVersionIsRefusedAndTheCurrentOneIsServedRegression) {
    CatalogDdlRig rig("aw_retire_open", 1);
    const int32_t tid = retireFixture(rig, 3);
    const uint64_t fp1 = rig.schemas().byIdVersion(tid, 1)->fingerprint;
    const uint64_t fp2 = rig.schemas().byIdVersion(tid, 2)->fingerprint;
    const uint64_t fp3 = rig.schemas().byIdVersion(tid, 3)->fingerprint;
    ASSERT_NE(fp1, fp2);
    ASSERT_NE(fp2, fp3);
    // before the retire every version opens
    EXPECT_EQ(rig.getSchema("characters", fp1).code, 0);
    ASSERT_EQ(rig.alter("characters", {retireOp(3)}).code, 0);
    EXPECT_EQ(rig.getSchema("characters", fp1).code, 67) << "SchemaVersionRetired";
    EXPECT_EQ(rig.getSchema("characters", fp2).code, 67);
    const auto cur = rig.getSchema("characters", fp3);
    EXPECT_EQ(cur.code, 0);
    EXPECT_EQ(cur.version, 3);
    EXPECT_EQ(rig.getSchema("characters").code, 0) << "the latest (fingerprint 0) is served as today";
    EXPECT_EQ(rig.getSchema("characters", rune::protocol::client::kSchemaLineageFingerprint).code, 0)
        << "the lineage (every version's fields, for decoding stored records) is still served";
    // the same answers across a restart (the registry is rebuilt from the log)
    rig.restart();
    rig.admin().setClusterFeatureLevel(rune::replication::MetadataFeature::kSchemaRetire);
    rig.becomeServingLeader();
    EXPECT_EQ(rig.getSchema("characters", fp1).code, 67);
    EXPECT_EQ(rig.getSchema("characters", fp3).code, 0);
}

// Applier: the write spec (the floor) is posted live on apply and again at boot replay.
TEST(AdminWorkerRegression, TheRetireFloorIsPostedToTheWorkersOnApplyAtBootAndAfterALaterAlterRegression) {
    CatalogDdlRig rig("aw_retire_boot", 1);
    const int32_t tid = retireFixture(rig, 3);
    ASSERT_EQ(rig.alter("characters", {retireOp(2)}).code, 0);
    EXPECT_EQ(rig.writeSpecEffects().back(), (CatalogDdlRig::FloorPost{tid, 2})) << "on apply";
    rig.restart();
    ASSERT_FALSE(rig.bootWriteSpecEffects().empty()) << "the boot replay posts the write spec of every typed topic";
    EXPECT_EQ(rig.bootWriteSpecEffects().back(), (CatalogDdlRig::FloorPost{tid, 2})) << "at boot replay: the floor is restored";
    EXPECT_EQ(rig.schemas().byName("characters")->retiredBelow, 2) << "and in the registry";
}
