// CheckedWritesAdminViewDdlTest.cpp — checked writes Part 07 Task 12: CreateView / AlterView accept view plan v2 — the
// feature-level gate, the compile error kinds, viewObjectId and the one-partition rule for keyed views, against the REAL
// AdminWorker (CatalogDdlRig).
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/view/PlanCompiler.h>
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

    const auto tenant = rune::codec::DescriptorBuilder("tenant", 1)
        .add("tenantId", PrimType::UInt16).add("region", PrimType::UInt16).addFixedStr("name", 16).build();
    ASSERT_EQ(define(rig, "tenant", "tenantId", tenant, 3).code, kOk);
    const auto sourceKeyed = createView(rig, projection(rig, "tenant", tenant, "tenantId", {{"tenantId", 0, "tenantId"}},
                                                        {"tenantId", "region"}, "region_by_tenant"), "tenantId");
    ASSERT_EQ(sourceKeyed.code, kOk) << sourceKeyed.detail;
    EXPECT_EQ(rv::keyModeOf(compiledOf(rig.recordedLog().back().event)), rv::ViewKeyMode::LegacyU64);

    const auto otherKey = createView(rig, projection(rig, "tenant", tenant, "tenantId", {{"region", 0, "region"}, {"tenantId", 0, "tenantId"}},
                                                     {"region", "tenantId", "name"}, "tenant_by_region"), "region");
    EXPECT_EQ(otherKey.code, codeOf(ErrorCode::ViewNotCoPartitioned)) << otherKey.detail;
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

TEST(CheckedWritesAdminViewDdlRegression, CompiledPlanNeedingTheLevelIsGatedRegression) {
    CatalogDdlRig rig("cwavg");
    atLevel(rig, kCheckedWrites - 1);
    ASSERT_EQ(define(rig, "session", "name", sessionDescriptor()).code, kOk);
    const auto metric = rune::codec::DescriptorBuilder("metric", 1).add("id", PrimType::UInt64).add("level", PrimType::Int32).build();
    ASSERT_EQ(define(rig, "metric", "id", metric).code, kOk);
    const auto tick = rune::codec::DescriptorBuilder("tick", 1).add("tickId", PrimType::UInt32).add("userId", PrimType::UInt64).build();
    ASSERT_EQ(define(rig, "tick", "tickId", tick).code, kOk);

    // v1 shapes that compile Keyed: a string source key, and an aggregate grouped by an Int32 column.
    rv::ViewPlan byName;
    byName.viewName = "by_name";
    byName.key = "name";
    byName.sources = {{topicIdOf(rig, "session"), "name"}};
    byName.output = {replaceOf(sessionDescriptor(), "name"), replaceOf(sessionDescriptor(), "score")};
    rv::ViewPlan byLevel;
    byLevel.viewName = "by_level";
    byLevel.key = "level";
    byLevel.sources = {{topicIdOf(rig, "metric"), "level"}};
    byLevel.output = {replaceOf(metric, "level"), {"n", PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    // A v1 aggregate over a 4-byte source key: an old broker would drop every record.
    rv::ViewPlan perUser;
    perUser.viewName = "per_user";
    perUser.key = "userId";
    perUser.sources = {{topicIdOf(rig, "tick"), "userId"}};
    perUser.output = {replaceOf(tick, "userId"), {"n", PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    for (const rv::ViewPlan* p : {&byName, &byLevel, &perUser}) {
        const auto a = createView(rig, *p, p->key);
        EXPECT_EQ(a.code, codeOf(ErrorCode::InvalidRequest)) << p->viewName << ": " << a.detail;
        EXPECT_EQ(a.detail, "plan v2 needs the checked-writes feature level") << p->viewName;
    }

    atLevel(rig);
    const auto ok = createView(rig, byName, "name");
    EXPECT_EQ(ok.code, kOk) << ok.detail;
}

TEST(CheckedWritesAdminViewDdlRegression, SourceKeyFieldMustBeTheTopicKeyRegression) {
    CatalogDdlRig rig("cwavw");
    atLevel(rig);
    ASSERT_EQ(define(rig, "character", "characterId", characterDescriptor()).code, kOk);
    const auto keyedDevice = rune::codec::DescriptorBuilder("device", 1)
        .add("userId", PrimType::UInt64, 1, true).addFixedStr("deviceFp", 32).add("platform", PrimType::Enum).add("lastSeen", PrimType::Int64).build();
    ASSERT_EQ(define(rig, "device", "userId", keyedDevice).code, kOk);

    // A composite view that names another column as the source key.
    auto composite = accountCharacters(rig);
    composite.sources[0].keyField = "userId";
    const auto a = createView(rig, composite);
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewKeyInvalid)) << a.detail;
    EXPECT_NE(a.detail.find("characterId"), std::string::npos) << a.detail;

    // A keyed aggregate.
    rv::ViewPlan grouped;
    grouped.viewName = "by_platform";
    grouped.sources = {{topicIdOf(rig, "device"), "platform"}};
    grouped.keyComponents = grouped.groupBy = {{"platform", 0, "platform"}};
    grouped.output = {{"n", PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    const auto c = createView(rig, grouped, "platform");
    EXPECT_EQ(c.code, codeOf(ErrorCode::ViewKeyInvalid)) << c.detail;
    grouped.sources[0].keyField = "userId";
    const auto d = createView(rig, grouped, "platform");
    EXPECT_EQ(d.code, kOk) << d.detail;

    // A plan that compiles to the legacy shape keeps the answer it always had.
    auto legacy = projection(rig, "character", characterDescriptor(), "userId", {}, {"userId", "name"}, "legacy_user");
    legacy.key = "userId";
    const auto e = createView(rig, legacy, "userId");
    EXPECT_EQ(e.code, kOk) << e.detail;
}

namespace {

rune::codec::Descriptor siteDescriptor() {
    return rune::codec::DescriptorBuilder("src", 1)
        .add("id", PrimType::UInt64, 1, true).add("userId", PrimType::UInt64).add("n", PrimType::Int64)
        .add("bio", PrimType::String).add("apiKey", PrimType::String).addFixedStr("label", 16).build();
}

rv::ViewPlan sitePlan(CatalogDdlRig& rig, const char* view, std::initializer_list<rv::AuthoredField> output, const char* key = "id",
                      const char* sourceKey = "id") {
    rv::ViewPlan v;
    v.viewName = view;
    v.key = key;
    v.sources = {{topicIdOf(rig, "src"), sourceKey}};
    v.output = output;
    return v;
}

}  // namespace

TEST(CheckedWritesAdminViewDdlRegression, NamesContainingKeyKeepTheirCodeRegression) {
    CatalogDdlRig rig("cwavm");
    atLevel(rig);
    ASSERT_EQ(define(rig, "src", "id", siteDescriptor()).code, kOk);
    const rv::AuthoredField id{"id", PrimType::UInt64, 0, "id", rv::MergeRule::Replace};

    // The view's name has "key" in it; the refusal is about a field, 34.
    const auto monkey = createView(rig, sitePlan(rig, "monkey", {id, {"n", PrimType::Int64, 0, "nope", rv::MergeRule::Replace}}), "id");
    EXPECT_EQ(monkey.code, codeOf(ErrorCode::ViewFieldUnresolved));
    EXPECT_EQ(monkey.detail, "view 'monkey': output field 'n' references unknown source field 'nope'");

    // So does a column called apiKey.
    const auto api = createView(rig, sitePlan(rig, "v", {id, {"x", PrimType::String, 0, "apiKey", rv::MergeRule::Replace}}), "id");
    EXPECT_EQ(api.code, codeOf(ErrorCode::ViewFieldUnresolved));
    EXPECT_EQ(api.detail, "view 'v': output field 'x' source field 'apiKey' is var-len; Phase 1 views support fixed-width columns only");

    // A var-len source key field is about the key, 35.
    const auto key = createView(rig, sitePlan(rig, "v", {id}, "id", "bio"), "id");
    EXPECT_EQ(key.code, codeOf(ErrorCode::ViewKeyInvalid));
    EXPECT_EQ(key.detail, "view 'v': source 0 key field 'bio' is var-len; keys must be fixed-width");
}

TEST(CheckedWritesAdminViewDdlRegression, EveryCompileRefusalCarriesTheCompilerMessageRegression) {
    CatalogDdlRig rig("cwavs");
    atLevel(rig);
    const auto d = siteDescriptor();
    ASSERT_EQ(define(rig, "src", "id", d).code, kOk);
    const rv::AuthoredField id{"id", PrimType::UInt64, 0, "id", rv::MergeRule::Replace};
    const auto withId = [&](rv::AuthoredField f) { return sitePlan(rig, "v", {id, std::move(f)}); };

    struct Site { const char* label; rv::ViewPlan plan; ErrorCode code; };
    std::vector<Site> sites;
    sites.push_back({"two sources", sitePlan(rig, "v", {id}), ErrorCode::ViewFieldUnresolved});
    sites.back().plan.sources.push_back(sites.back().plan.sources[0]);
    sites.push_back({"no output", sitePlan(rig, "v", {}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"count(*) with a field", withId({"x", PrimType::Int64, 0, "n", rv::MergeRule::CountAll}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"count(*) type", withId({"x", PrimType::Int32, 0, "", rv::MergeRule::CountAll}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"sourceIdx", withId({"x", PrimType::Int64, 1, "n", rv::MergeRule::Replace}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"unknown field", withId({"x", PrimType::Int64, 0, "nope", rv::MergeRule::Replace}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"var-len field", withId({"x", PrimType::String, 0, "bio", rv::MergeRule::Replace}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"not numeric", withId({"x", PrimType::Int64, 0, "label", rv::MergeRule::Sum}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"aggregate type", withId({"x", PrimType::Int32, 0, "n", rv::MergeRule::Max}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"replace type", withId({"x", PrimType::Int32, 0, "n", rv::MergeRule::Replace}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"duplicate output", withId({"id", PrimType::Int64, 0, "n", rv::MergeRule::Replace}), ErrorCode::ViewFieldUnresolved});
    sites.push_back({"key not found", sitePlan(rig, "v", {id}, "id", "nope"), ErrorCode::ViewKeyInvalid});
    sites.push_back({"key var-len", sitePlan(rig, "v", {id}, "id", "bio"), ErrorCode::ViewKeyInvalid});
    sites.push_back({"non-aggregate column", sitePlan(rig, "v", {{"userId", PrimType::UInt64, 0, "userId", rv::MergeRule::Replace},
                                                                  {"n", PrimType::Int64, 0, "n", rv::MergeRule::Replace},
                                                                  {"c", PrimType::Int64, 0, "", rv::MergeRule::CountAll}}, "userId", "userId"),
                     ErrorCode::ViewFieldUnresolved});
    sites.push_back({"key not in output", sitePlan(rig, "v", {{"c", PrimType::Int64, 0, "", rv::MergeRule::CountAll}}, "userId", "userId"),
                     ErrorCode::ViewKeyInvalid});
    sites.push_back({"group-by column rule", sitePlan(rig, "v", {{"userId", PrimType::UInt64, 0, "userId", rv::MergeRule::Max},
                                                                  {"c", PrimType::Int64, 0, "", rv::MergeRule::CountAll}}, "userId", "userId"),
                     ErrorCode::ViewFieldUnresolved});

    for (const Site& s : sites) {
        const rune::codec::Descriptor* srcs[] = {&d, &d};
        const auto direct = rv::compileView(s.plan, std::span(srcs, s.plan.sources.size()), 0);
        ASSERT_FALSE(direct.has_value()) << s.label;
        const auto a = createView(rig, s.plan, "id");
        EXPECT_EQ(a.code, codeOf(s.code)) << s.label << ": " << a.detail;
        EXPECT_EQ(a.detail, direct.error().message) << s.label;
        EXPECT_FALSE(a.detail.empty()) << s.label;
    }
}
