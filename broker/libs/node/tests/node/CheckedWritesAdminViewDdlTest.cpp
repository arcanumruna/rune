// CheckedWritesAdminViewDdlTest.cpp — checked writes Part 07 Task 12: CreateView / AlterView accept view plan v2 — the
// feature-level gate, the compile error kinds, viewObjectId and the one-partition rule for keyed views, against the REAL
// AdminWorker (CatalogDdlRig).
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/view/ViewPlan.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
using rune::codec::PrimType;
namespace cat = rune::catalog;
namespace rv = rune::view;
using rune::protocol::client::ErrorCode;
using rune::replication::MetadataFeature::kCheckedWrites;

namespace {

constexpr uint16_t kOk = 0;

uint16_t codeOf(ErrorCode c) { return static_cast<uint16_t>(c); }

rune::codec::Descriptor characterDescriptor() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::UInt64).add("userId", PrimType::UInt64)
        .addFixedStr("name", 16).addFixedStr("tag", 80).add("provider", PrimType::UInt16)
        .addFixedStr("subject", 64).add("gender", PrimType::UInt16).add("lastPlayed", PrimType::Int64).build();
}

rune::codec::Descriptor deviceDescriptor() {
    return rune::codec::DescriptorBuilder("device", 1)
        .add("userId", PrimType::UInt64).addFixedStr("deviceFp", 32).add("platform", PrimType::Enum)
        .add("lastSeen", PrimType::Int64).build();
}

rune::codec::Descriptor sessionDescriptor() {
    return rune::codec::DescriptorBuilder("session", 1).addFixedStr("name", 16).add("score", PrimType::Int64).build();
}

CatalogDdlRig::Answer define(CatalogDdlRig& rig, const std::string& name, const std::string& keyField,
                             const rune::codec::Descriptor& d, int32_t partitions = 1) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.keyField = keyField;
    m.blob = rune::codec::serialize(d);
    m.partitionCount = partitions;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

void atLevel(CatalogDdlRig& rig, int16_t level = kCheckedWrites) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(level);
}

int32_t topicIdOf(CatalogDdlRig& rig, const std::string& name) {
    return rig.admin().applier().state().topicByName(0, name)->topicId;
}

rv::AuthoredField replaceOf(const rune::codec::Descriptor& d, const std::string& name) {
    return {name, d.find(name)->type, 0, name, rv::MergeRule::Replace};
}

rv::ViewPlan projection(CatalogDdlRig& rig, const std::string& source, const rune::codec::Descriptor& d, const std::string& sourceKey,
                        std::vector<rv::KeyComponent> key, std::initializer_list<const char*> select, const char* view = "account_characters") {
    rv::ViewPlan v;
    v.viewName = view;
    v.sources = {{topicIdOf(rig, source), sourceKey}};
    v.keyComponents = std::move(key);
    for (const char* s : select) v.output.push_back(replaceOf(d, s));
    return v;
}

rv::ViewPlan accountCharacters(CatalogDdlRig& rig) {
    return projection(rig, "character", characterDescriptor(), "characterId", {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}},
                      {"userId", "characterId", "name", "gender", "lastPlayed"});
}

// The pre-v2 shape: key = the source key, no key components.
rv::ViewPlan legacyView(CatalogDdlRig& rig) {
    auto v = projection(rig, "character", characterDescriptor(), "characterId", {}, {"characterId", "userId", "name"}, "char_all");
    v.key = "characterId";
    return v;
}

CatalogDdlRig::Answer createView(CatalogDdlRig& rig, const rv::ViewPlan& p, const std::string& keyField = "userId") {
    CreateViewMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.viewName = p.viewName; m.keyField = keyField;
    m.planBlob = rv::serializeAuthored(p);
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

const cat::ViewDef& viewDefOf(const cat::CatalogEvent& e) {
    if (const auto* c = std::get_if<cat::CreateViewOps>(&e.ops)) return c->view;
    return std::get<cat::AlterViewOps>(e.ops).view;
}

rv::CompiledViewPlan compiledOf(const cat::CatalogEvent& e) {
    auto p = rv::deserializeCompiledViewPlan(viewDefOf(e).compiledPlanBlob);
    EXPECT_TRUE(p.has_value());
    return p ? std::move(*p) : rv::CompiledViewPlan{};
}

rv::PredicateNode column(const char* c) { return {rv::PredicateOp::Column, 0, 0, c}; }

// Bytes captured from the code before the plan v2 change (a v1 plan keeps its exact bytes).
const std::vector<uint8_t> kLegacyNormalizedGolden = {
    0x01, 0x08, 0x00, 0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x5f, 0x61, 0x6c, 0x6c, 0x0b, 0x00, 0x00, 0x00, 0x63, 0x68,
    0x61, 0x72, 0x61, 0x63, 0x74, 0x65, 0x72, 0x49, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00,
    0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x61, 0x63, 0x74, 0x65, 0x72, 0x49, 0x64, 0x03, 0x00, 0x00, 0x00, 0x0b, 0x00,
    0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x61, 0x63, 0x74, 0x65, 0x72, 0x49, 0x64, 0x04, 0x00, 0x00, 0x0b, 0x00, 0x00,
    0x00, 0x63, 0x68, 0x61, 0x72, 0x61, 0x63, 0x74, 0x65, 0x72, 0x49, 0x64, 0x00, 0x06, 0x00, 0x00, 0x00, 0x75, 0x73,
    0x65, 0x72, 0x49, 0x64, 0x04, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x49, 0x64, 0x00, 0x04,
    0x00, 0x00, 0x00, 0x6e, 0x61, 0x6d, 0x65, 0x0a, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x6e, 0x61, 0x6d, 0x65, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
const std::vector<uint8_t> kLegacyCompiledGolden = {
    0x01, 0x08, 0x00, 0x00, 0x00, 0x63, 0x68, 0x61, 0x72, 0x5f, 0x61, 0x6c, 0x6c, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x79, 0xcb, 0xf1, 0x34, 0xb7, 0xfb,
    0x45, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x02, 0x00, 0x0a, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

}  // namespace

TEST(CheckedWritesAdminViewDdl, CompositeViewIsCreatedWithItsObjectId) {
    CatalogDdlRig rig("cwavc");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    const auto a = createView(rig, accountCharacters(rig));
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::CatalogEvent e = rig.recordedLog().back().event;
    const rv::CompiledViewPlan plan = compiledOf(e);
    EXPECT_EQ(plan.keyComponents.size(), 2u);
    EXPECT_EQ(plan.viewObjectId, e.objectId);
    EXPECT_NE(e.objectId, 0u);
}

TEST(CheckedWritesAdminViewDdl, LegacyPlanBytesAreUnchanged) {
    CatalogDdlRig rig("cwavl");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    const auto a = createView(rig, legacyView(rig), "characterId");
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::CatalogEvent e = rig.recordedLog().back().event;
    const cat::ViewDef& def = viewDefOf(e);
    EXPECT_EQ(def.normalizedPlanBlob, kLegacyNormalizedGolden);
    EXPECT_EQ(def.compiledPlanBlob, kLegacyCompiledGolden);
}

TEST(CheckedWritesAdminViewDdl, BelowTheLevelPlanV2IsInvalidRequest) {
    CatalogDdlRig rig("cwavb");
    atLevel(rig, kCheckedWrites - 1);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    const auto a = createView(rig, accountCharacters(rig));
    EXPECT_EQ(a.code, codeOf(ErrorCode::InvalidRequest));
    EXPECT_EQ(a.detail, "plan v2 needs the checked-writes feature level");
    const auto b = createView(rig, legacyView(rig), "characterId");
    EXPECT_EQ(b.code, kOk) << b.detail;
}

TEST(CheckedWritesAdminViewDdl, UniqueAndCheckAreNotAvailableYet) {
    CatalogDdlRig rig("cwavu");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    auto u = accountCharacters(rig);
    u.unique = true;
    const auto a = createView(rig, u);
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewUniqueInvalid));
    EXPECT_EQ(a.detail, "unique views are not available yet");
    auto c = accountCharacters(rig);
    c.constraints = {{"genderSet", {column("gender")}}};
    const auto b = createView(rig, c);
    EXPECT_EQ(b.code, codeOf(ErrorCode::ViewConstraintInvalid));
    EXPECT_EQ(b.detail, "check constraints are not available yet");
}

TEST(CheckedWritesAdminViewDdl, CompileKindsMapToTheirCodes) {
    CatalogDdlRig rig("cwavk");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    ASSERT_EQ(define(rig, "device", "userId", deviceDescriptor()).code, kOk);
    ASSERT_EQ(define(rig, "session", "name", sessionDescriptor()).code, kOk);
    const auto chars = characterDescriptor();

    // K = 144 > 128.
    const auto wide = createView(rig, projection(rig, "character", chars, "characterId", {{"tag", 0, "tag"}, {"subject", 0, "subject"}, {"characterId", 0, "characterId"}},
                                                 {"tag", "subject", "characterId"}));
    EXPECT_EQ(wide.code, codeOf(ErrorCode::ViewKeyExpressionInvalid)) << wide.detail;

    // The key does not identify the source record.
    const auto identity = createView(rig, projection(rig, "character", chars, "characterId", {{"userId", 0, "userId"}}, {"userId", "name"}));
    EXPECT_EQ(identity.code, codeOf(ErrorCode::ViewKeyExpressionInvalid)) << identity.detail;

    // lower() over an integer column.
    auto lowered = projection(rig, "character", chars, "characterId", {{"gender", rune::codec::kKeyFunctionLower, "genderKey"}, {"characterId", 0, "characterId"}},
                              {"characterId", "name"});
    lowered.unicodeVersion = rune::codec::kUnicodeVersion;
    const auto lower = createView(rig, lowered);
    EXPECT_EQ(lower.code, codeOf(ErrorCode::ViewKeyExpressionInvalid)) << lower.detail;

    // An unknown key column.
    auto unknown = accountCharacters(rig);
    unknown.keyComponents[0].column = "noSuchColumn";
    const auto missing = createView(rig, unknown);
    EXPECT_EQ(missing.code, codeOf(ErrorCode::ViewFieldUnresolved)) << missing.detail;

    // A groupBy that differs from the key.
    rv::ViewPlan grouped;
    grouped.viewName = "device_history";
    grouped.sources = {{topicIdOf(rig, "device"), "userId"}};
    grouped.keyComponents = {{"userId", 0, "userId"}};
    grouped.groupBy = {{"platform", 0, "platform"}};
    grouped.output = {{"uses", PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    const auto group = createView(rig, grouped);
    EXPECT_EQ(group.code, codeOf(ErrorCode::ViewKeyInvalid)) << group.detail;
    EXPECT_NE(group.detail.find("group by"), std::string::npos) << group.detail;

    // A keyed aggregate over a string(16)-keyed source.
    rv::ViewPlan keyedAggregate;
    keyedAggregate.viewName = "session_scores";
    keyedAggregate.sources = {{topicIdOf(rig, "session"), "name"}};
    keyedAggregate.keyComponents = {{"name", 0, "name"}};
    keyedAggregate.groupBy = {{"name", 0, "name"}};
    keyedAggregate.output = {{"scores", PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    const auto agg = createView(rig, keyedAggregate);
    EXPECT_EQ(agg.code, codeOf(ErrorCode::ViewKeyExpressionInvalid)) << agg.detail;
}

TEST(CheckedWritesAdminViewDdl, UnknownUnicodeVersionIsViewKeyExpressionInvalid) {
    CatalogDdlRig rig("cwavq");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    auto v = projection(rig, "character", characterDescriptor(), "characterId", {{"name", rune::codec::kKeyFunctionLower, "nameKey"}, {"characterId", 0, "characterId"}},
                        {"characterId", "name"});
    v.unicodeVersion = rune::codec::packUnicodeVersion(17, 0, 0);
    const auto a = createView(rig, v);
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewKeyExpressionInvalid));
    EXPECT_NE(a.detail.find("lower() is pinned to Unicode 17.0.0; this broker has 16.0.0"), std::string::npos) << a.detail;
}

TEST(CheckedWritesAdminViewDdl, KeyedViewsNeedOnePartition) {
    CatalogDdlRig rig("cwavp");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor(), 3).code, kOk);
    const auto composite = createView(rig, accountCharacters(rig));
    EXPECT_EQ(composite.code, codeOf(ErrorCode::ViewNotCoPartitioned));
    EXPECT_EQ(composite.detail, "a view keyed on anything but the source key needs a source with one partition");

    auto sourceKeyed = projection(rig, "character", characterDescriptor(), "characterId", {{"characterId", 0, "characterId"}},
                                  {"characterId", "provider"}, "provider_by_character");
    const auto ok = createView(rig, sourceKeyed, "characterId");
    EXPECT_EQ(ok.code, kOk) << ok.detail;
}

TEST(CheckedWritesAdminViewDdlRegression, KeylessSourceIsRefusedBeforeTheCompileRegression) {
    CatalogDdlRig rig("cwavr");
    atLevel(rig);
    ASSERT_EQ(define(rig, "events", "", characterDescriptor()).code, kOk);
    const auto chars = characterDescriptor();
    const auto composite = createView(rig, projection(rig, "events", chars, "", {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}}, {"userId", "characterId"}, "composite"));
    EXPECT_EQ(composite.code, codeOf(ErrorCode::ViewKeyInvalid)) << composite.detail;
    EXPECT_EQ(composite.detail, "view source needs a key");
    const auto single = createView(rig, projection(rig, "events", chars, "", {{"userId", 0, "userId"}}, {"userId", "name"}, "single"));
    EXPECT_EQ(single.code, codeOf(ErrorCode::ViewKeyInvalid)) << single.detail;
    EXPECT_EQ(single.detail, "view source needs a key");
}

TEST(CheckedWritesAdminViewDdl, AlterViewKeepsTheObjectId) {
    CatalogDdlRig rig("cwava");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    ASSERT_EQ(createView(rig, accountCharacters(rig)).code, kOk);
    const uint64_t objectId = rig.recordedLog().back().event.objectId;

    AlterViewMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = "account_characters"; m.keyField = "userId";
    auto v = accountCharacters(rig);
    v.output.push_back(replaceOf(characterDescriptor(), "provider"));
    m.planBlob = rv::serializeAuthored(v);
    m.meta.requestId = rig.freshRequestId();
    const auto a = rig.submitRaw(DdlRequest{std::move(m)});
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::CatalogEvent e = rig.recordedLog().back().event;
    ASSERT_TRUE(std::holds_alternative<cat::AlterViewOps>(e.ops));
    EXPECT_EQ(compiledOf(e).viewObjectId, objectId);
    EXPECT_EQ(e.objectId, objectId);
}
