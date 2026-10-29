// CheckedWritesDdlTest.cpp — checked writes Part 02 Task 6: the feature rule table (M3), the topic key types refused at
// define, view source kinds, and the GetTopicSchema kind probe, against the REAL AdminWorker (CatalogDdlRig).
#include <rune/node/TopicFeatureRules.h>
#include <rune/codec/FilterWire.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/client/SchemaMessages.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

#include <tuple>

using namespace rune::node;
using namespace rune::node::test;
using rune::codec::PrimType;
namespace rp = rune::protocol;

namespace {

constexpr uint16_t kOk = 0, kUnknownTopic = 7, kInvalidSchema = 23, kTopicExists = 25, kViewSourceNotFound = 33, kViewKeyInvalid = 35, kLimit = 52;

// ---- M3: featureRefusal, 5 features x 6 shapes ------------------------------------------------------------------

const TopicShape kShapes[6] = {
    {false, false, false}, {false, false, true}, {true, false, false}, {true, false, true}, {true, true, false}, {true, true, true}};
const char* const kShapeNames[6] = {"UntypedDisk", "UntypedTransient", "KeylessDisk", "KeylessTransient", "KeyedDisk", "KeyedTransient"};
const char* const kFeatureNames[5] = {"View", "Filter", "KeyIndex", "AutoIncrement", "Constraint"};

// expected[feature][shape]; nullptr = allowed
const char* const kExpected[5][6] = {
    {"view source must be a typed topic", "view source must be a typed topic", "view source needs a key", "view source needs a key", nullptr, nullptr},
    {"filter needs a typed topic", "filter needs a typed topic", nullptr, nullptr, nullptr, nullptr},
    {"key.index needs a typed topic", "key.index needs a typed topic", "key.index needs a key", "key.index needs a key", nullptr, "key.index needs a persisted topic"},
    {"autoincrement needs a typed topic", "autoincrement needs a typed topic", "autoincrement needs a key", "autoincrement needs a key", nullptr, "autoincrement needs a persisted topic"},
    {"constraints need a typed topic", "constraints need a typed topic", "constraints need a key", "constraints need a key", nullptr, "constraints need a persisted topic"},
};

}  // namespace

class CheckedWritesDdlRule : public ::testing::TestWithParam<std::tuple<int, int>> {};

TEST_P(CheckedWritesDdlRule, FeatureRuleTable) {
    const auto [f, s] = GetParam();
    const auto got = featureRefusal(static_cast<DbFeature>(f), kShapes[s]);
    if (kExpected[f][s] == nullptr) EXPECT_FALSE(got.has_value()) << *got;
    else {
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, kExpected[f][s]);
    }
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesDdl, CheckedWritesDdlRule, ::testing::Combine(::testing::Range(0, 5), ::testing::Range(0, 6)),
    [](const ::testing::TestParamInfo<std::tuple<int, int>>& i) {
        return std::string(kFeatureNames[std::get<0>(i.param)]) + "_" + kShapeNames[std::get<1>(i.param)];
    });

TEST(CheckedWritesDdl, FilterMessageMatchesTheGateway) {
    const auto got = featureRefusal(DbFeature::Filter, TopicShape{});
    ASSERT_TRUE(got);
    EXPECT_EQ(std::string(*got), rune::codec::describe({rune::codec::FilterErrorKind::UntypedTopic}));
}

// ---- key types at define ------------------------------------------------------------------------------------------

namespace {

std::vector<uint8_t> keyDescriptor(PrimType t, uint16_t width) {
    rune::codec::DescriptorBuilder b("t", 1);
    if (t == PrimType::FixedStr) b.addFixedStr("k", width);
    else if (t == PrimType::Enum) b.add("k", t, 1, false, {1, 2});
    else b.add("k", t);
    return rune::codec::serialize(b.add("v", PrimType::Int64).build());
}

CatalogDdlRig::Answer defineWith(CatalogDdlRig& rig, const std::string& name, const std::vector<uint8_t>& blob, const std::string& keyField,
                                 uint32_t partitions = 1) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.partitionCount = partitions;
    m.keyField = keyField; m.blob = blob;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

struct KeyRow { const char* name; PrimType type; uint16_t width; bool ok; const char* spelling; };
const KeyRow kKeyRows[] = {
    {"uint16", PrimType::UInt16, 0, true, ""},   {"int32", PrimType::Int32, 0, true, ""},   {"uint32", PrimType::UInt32, 0, true, ""},
    {"int64", PrimType::Int64, 0, true, ""},     {"uint64", PrimType::UInt64, 0, true, ""}, {"string1", PrimType::FixedStr, 1, true, ""},
    {"string16", PrimType::FixedStr, 16, true, ""},
    {"string17", PrimType::FixedStr, 17, false, "string(17)"}, {"string", PrimType::String, 0, false, "string"},
    {"bytes", PrimType::Bytes, 0, false, "bytes"},   {"float64", PrimType::Float64, 0, false, "float64"},
    {"bool", PrimType::Bool, 0, false, "bool"},      {"enum", PrimType::Enum, 0, false, "enum"},
};

}  // namespace

class CheckedWritesDdlKeyType : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesDdlKeyType, KeyFieldTypesAtDefine) {
    const KeyRow& row = kKeyRows[GetParam()];
    CatalogDdlRig rig("cwkey");
    rig.becomeServingLeader();
    const auto a = defineWith(rig, "t", keyDescriptor(row.type, row.width), "k");
    if (row.ok) {
        EXPECT_EQ(a.code, kOk) << a.detail;
    } else {
        EXPECT_EQ(a.code, kInvalidSchema);
        EXPECT_EQ(a.detail, std::string("key field 'k' is ") + row.spelling + ": a topic key is an integer or string(N) with N <= 16");
    }
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesDdl, CheckedWritesDdlKeyType, ::testing::Range(0, static_cast<int>(sizeof kKeyRows / sizeof kKeyRows[0])),
    [](const ::testing::TestParamInfo<int>& i) { return std::string(kKeyRows[i.param].name); });

TEST(CheckedWritesDdlRegression, AdoptionAppliesTheKeyRuleRegression) {
    CatalogDdlRig rig("cwadopt");
    rig.becomeServingLeader();
    CatalogDdlRig::DefineArgs plain;
    plain.name = "a";
    ASSERT_EQ(rig.define(plain).code, kOk);
    const auto bad = defineWith(rig, "a", keyDescriptor(PrimType::String, 0), "k");
    EXPECT_EQ(bad.code, kInvalidSchema);
    const auto probe = rig.getSchema("a", rp::client::kSchemaKindFingerprint);
    EXPECT_EQ(probe.code, kOk);
    EXPECT_EQ(probe.version, 0) << "the topic stays untyped";
    EXPECT_TRUE(probe.blob.empty());
    EXPECT_EQ(defineWith(rig, "a", keyDescriptor(PrimType::UInt64, 0), "k").code, kOk) << "a topic key adopts";
}

// P4 keeps existing topics working: re-running the DDL of a typed topic answers TopicExists with its id (the idempotent
// "already there" an application's startup relies on), whatever key its re-define names - the topic key rule judges
// only a new definition (a create or an adoption).  RED: run the key rule before the existing-topic answer - 23.
TEST(CheckedWritesDdlRegression, RedefineOfATypedTopicAnswersTopicExistsBeforeTheKeyRuleRegression) {
    CatalogDdlRig rig("cwredefine");
    rig.becomeServingLeader();
    const auto created = defineWith(rig, "p", keyDescriptor(PrimType::UInt64, 0), "k");
    ASSERT_EQ(created.code, kOk);
    const auto again = defineWith(rig, "p", keyDescriptor(PrimType::String, 0), "k");
    EXPECT_EQ(again.code, kTopicExists);
    EXPECT_EQ(again.topicId, created.topicId);
}

TEST(CheckedWritesDdl, ViewSourceKinds) {
    CatalogDdlRig rig("cwview");
    rig.becomeServingLeader();
    CatalogDdlRig::DefineArgs raw;   raw.name = "raw";
    CatalogDdlRig::DefineArgs keyless; keyless.name = "keyless"; keyless.typed = true; keyless.keyField = "";
    CatalogDdlRig::DefineArgs keyed; keyed.name = "keyed"; keyed.typed = true;
    ASSERT_EQ(rig.define(raw).code, kOk);
    ASSERT_EQ(rig.define(keyless).code, kOk);
    ASSERT_EQ(rig.define(keyed).code, kOk);

    auto a = rig.createView("v1", "raw");
    EXPECT_EQ(a.code, kViewSourceNotFound);
    EXPECT_EQ(a.detail, "view source must be a typed topic");
    a = rig.createView("v2", "keyless");
    EXPECT_EQ(a.code, kViewKeyInvalid);
    EXPECT_EQ(a.detail, "view source needs a key");
    a = rig.createView("v3", "missing");
    EXPECT_EQ(a.code, kViewSourceNotFound);
    EXPECT_TRUE(a.detail.empty());
    a = rig.createView("v4", "keyed");
    EXPECT_EQ(a.code, kOk) << a.detail;
}

TEST(CheckedWritesDdlRegression, ViewsOffInTheNamespaceOutranksTheSourceKindRegression) {
    CatalogDdlRig rig("cwviewns");
    rig.becomeServingLeader();
    uint8_t b[rp::kNamespaceLimitsBytes] = {};
    rp::detail::putLimitU32(b + 28 * 8, rp::kNsFeatureCompaction);   // featureBits: compaction only, no views
    rp::NamespaceLimits limits;
    ASSERT_TRUE(rp::decodeLimits(b, sizeof b, limits));
    std::array<uint8_t, 4> mask{};
    mask[28 >> 3] = static_cast<uint8_t>(1u << (28 & 7));
    const auto ns = rig.createNamespace("n", mask, limits);
    ASSERT_EQ(ns.code, kOk) << ns.detail;
    const auto nsId = static_cast<uint32_t>(ns.objectId);

    CatalogDdlRig::DefineArgs raw; raw.name = "raw"; raw.nsId = nsId;
    ASSERT_EQ(rig.define(raw).code, kOk);
    const auto a = rig.createView("v", "raw", false, 0, nsId);
    EXPECT_EQ(a.code, kLimit) << "the namespace's refusal, not the source kind's (33)";
}

TEST(CheckedWritesDdl, KindProbe) {
    CatalogDdlRig rig("cwprobe");
    rig.becomeServingLeader();
    CatalogDdlRig::DefineArgs typed; typed.name = "typed"; typed.typed = true;
    CatalogDdlRig::DefineArgs raw;   raw.name = "raw"; raw.partitions = 3;
    ASSERT_EQ(rig.define(typed).code, kOk);
    ASSERT_EQ(rig.define(raw).code, kOk);

    const auto t = rig.getSchema("typed", rp::client::kSchemaKindFingerprint);
    EXPECT_EQ(t.code, kOk);
    EXPECT_EQ(t.version, 1);
    EXPECT_EQ(t.blob, rig.getSchema("typed", 0).blob);
    EXPECT_FALSE(t.blob.empty());

    const auto r = rig.getSchema("raw", rp::client::kSchemaKindFingerprint);
    EXPECT_EQ(r.code, kOk);
    EXPECT_EQ(r.version, 0);
    EXPECT_TRUE(r.blob.empty());
    EXPECT_EQ(r.partitionCount, 3);
    EXPECT_GE(r.topicId, 0);

    EXPECT_EQ(rig.getSchema("nope", rp::client::kSchemaKindFingerprint).code, kUnknownTopic);
}

TEST(CheckedWritesDdl, KindProbeIsScopedToTheNamespace) {
    CatalogDdlRig rig("cwprobens");
    rig.becomeServingLeader();
    const auto ns = rig.createNamespace("n");
    ASSERT_EQ(ns.code, kOk) << ns.detail;
    const auto nsId = static_cast<uint32_t>(ns.objectId);
    CatalogDdlRig::DefineArgs raw; raw.name = "raw"; raw.nsId = nsId;
    ASSERT_EQ(rig.define(raw).code, kOk);
    EXPECT_EQ(rig.getSchema("raw", rp::client::kSchemaKindFingerprint, nsId).code, kOk);
    EXPECT_EQ(rig.getSchema("raw", rp::client::kSchemaKindFingerprint).code, kUnknownTopic) << "the default namespace has no such topic";
}

TEST(CheckedWritesDdlRegression, ExistingRequestsAnswerAsBeforeRegression) {
    CatalogDdlRig rig("cwprobeold");
    rig.becomeServingLeader();
    CatalogDdlRig::DefineArgs typed; typed.name = "typed"; typed.typed = true;
    CatalogDdlRig::DefineArgs raw;   raw.name = "raw";
    ASSERT_EQ(rig.define(typed).code, kOk);
    ASSERT_EQ(rig.define(raw).code, kOk);
    EXPECT_EQ(rig.getSchema("raw", 0).code, kUnknownTopic);
    const auto lineage = rig.getSchema("typed", rp::client::kSchemaLineageFingerprint);
    EXPECT_EQ(lineage.code, kOk);
    EXPECT_FALSE(lineage.blob.empty());
}
