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

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/FdBudget.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/SchemaRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

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
    const auto* t = rig.admin().applier().state().topicByName("characters");
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
    EXPECT_EQ(rig.admin().applier().state().topicByName("huge"), nullptr);
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
    const auto* before = rig.admin().applier().state().topicByName("t");
    ASSERT_NE(before, nullptr);
    const uint32_t version = before->version;
    const std::size_t events = rig.recordedLog().size();
    const auto a = rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 1LL << 30}});
    EXPECT_EQ(a.code, 39);
    EXPECT_NE(a.detail.find("ALTER TOPIC would raise"), std::string::npos) << a.detail;
    EXPECT_EQ(rig.recordedLog().size(), events) << "nothing appended";
    const auto* after = rig.admin().applier().state().topicByName("t");
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
    EXPECT_EQ(rig.admin().applier().state().topicByName("t")->config.retentionBytes, 50LL << 20);
    EXPECT_EQ(rig.alterConfig("t", {{rc::ConfigKey::RetentionBytes, 60LL << 20}}).code, 39) << "a raise is refused";
    EXPECT_EQ(defineWith(rig, "more", 1).code, 39) << "a new topic only raises the need";
    EXPECT_EQ(rig.dropTopic("t").code, 0) << "a drop only lowers the need";
    EXPECT_EQ(rig.admin().applier().state().topicByName("t"), nullptr);
    EXPECT_EQ(defineWith(rig, "more", 1).code, 0) << "back within the budget";
}

TEST(AdminWorkerRegression, AnUnknownFdLimitNeverRefusesRegression) {
    CatalogDdlRig rig("aw_fd_unknown", 1);   // fdLimit 0: getrlimit failed / not wired
    rig.becomeServingLeader();
    EXPECT_EQ(defineWith(rig, "huge", 1000).code, 0);
    EXPECT_EQ(rig.alterConfig("huge", {{rc::ConfigKey::RetentionBytes, 1LL << 40}}).code, 0);
    EXPECT_EQ(rig.admin().ddlRefusedFdBudget(), 0u);
}
