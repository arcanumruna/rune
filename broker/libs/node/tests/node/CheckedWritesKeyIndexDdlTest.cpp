// CheckedWritesKeyIndexDdlTest.cpp — checked writes Part 05 Task 5: the key.index DDL rules, the catalog format by feature
// level, the features in TopicSchemaResponse and the write spec's keyIndex bit, against the REAL AdminWorker (CatalogDdlRig).
#include <rune/node/WriteState.h>
#include <rune/codec/Serialize.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
using rune::codec::PrimType;
namespace rp = rune::protocol;
namespace cat = rune::catalog;

namespace {

constexpr uint16_t kOk = 0, kInvalidRequest = 39;

std::vector<uint8_t> keyedBlob(PrimType t, uint16_t width = 0) {
    rune::codec::DescriptorBuilder b("t", 1);
    if (t == PrimType::FixedStr) b.addFixedStr("id", width);
    else b.add("id", t);
    return rune::codec::serialize(b.add("name", PrimType::Int64).build());
}

rp::TopicKeyFeatures keyIndex() {
    rp::TopicKeyFeatures f;
    f.keyIndex = true;
    return f;
}

CatalogDdlRig::Answer defineKeyed(CatalogDdlRig& rig, const std::string& name, const std::vector<uint8_t>& blob,
                                  const rp::TopicKeyFeatures& f, uint8_t persistence = 0, const std::string& keyField = "id") {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = name; m.keyField = keyField; m.blob = blob;
    m.persistence = persistence; m.keyFeatures = f;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

const cat::TopicObject* topicOf(CatalogDdlRig& rig, const std::string& name) {
    return rig.admin().applier().state().topicByName(0, name);
}

cat::CatalogEvent lastEvent(const std::vector<cat::RecordedEvent>& log) { return log.back().event; }

void atLevel5(CatalogDdlRig& rig) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(5);
}

}  // namespace

TEST(CheckedWritesKeyIndexDdl, TypedKeyedDiskTopicGetsAKeyIndex) {
    CatalogDdlRig rig("cwki");
    atLevel5(rig);
    const auto a = defineKeyed(rig, "ki", keyedBlob(PrimType::UInt64), keyIndex());
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::CatalogEvent e = lastEvent(rig.recordedLog());
    EXPECT_EQ(e.formatVer, cat::kFormatV3);
    const auto* op = std::get_if<cat::CreateTopicOps>(&e.ops);
    ASSERT_NE(op, nullptr);
    ASSERT_TRUE(op->schema);
    EXPECT_TRUE(op->schema->keyFeatures.keyIndex);
    const cat::TopicObject* t = topicOf(rig, "ki");
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(t->schema->keyFeatures.keyIndex);
    const auto w = writeSpecWireOf(*t);
    ASSERT_TRUE(w);
    EXPECT_TRUE(w->keyIndex);
}

namespace {

struct RefusalRow {
    const char* name;
    int16_t     level;
    bool        typed;
    bool        keyed;
    uint8_t     persistence;
    PrimType    keyType;
    uint16_t    width;
    bool        autoIncrement;
    rp::Partitioner partitioner;
    uint32_t    reserve;
    const char* detail;
};
constexpr uint32_t kReserve = rp::kAutoIncrementReserveDefault;
const RefusalRow kRefusals[] = {
    {"Level4", 4, true, true, 0, PrimType::UInt64, 0, false, rp::Partitioner::Fnv1a, kReserve, "key.index needs every broker at feature level 5"},
    {"Untyped", 5, false, false, 0, PrimType::UInt64, 0, false, rp::Partitioner::Fnv1a, kReserve, "key.index needs a typed topic"},
    {"Keyless", 5, true, false, 0, PrimType::UInt64, 0, false, rp::Partitioner::Fnv1a, kReserve, "key.index needs a key"},
    {"Transient", 5, true, true, 1, PrimType::UInt64, 0, false, rp::Partitioner::Fnv1a, kReserve, "key.index needs a persisted topic"},
};

}  // namespace

class CheckedWritesKeyIndexDdlRefusal : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesKeyIndexDdlRefusal, Refusals) {
    const RefusalRow& row = kRefusals[GetParam()];
    CatalogDdlRig rig("cwkiref");
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(row.level);
    rp::TopicKeyFeatures f = keyIndex();
    f.autoIncrement        = row.autoIncrement;
    f.partitioner          = row.partitioner;
    f.autoIncrementReserve = row.reserve;
    const auto a = defineKeyed(rig, "ki", row.typed ? keyedBlob(row.keyType, row.width) : std::vector<uint8_t>{}, f, row.persistence,
                               row.keyed ? "id" : "");
    EXPECT_EQ(a.code, kInvalidRequest);
    EXPECT_EQ(a.detail, row.detail);
    EXPECT_EQ(rig.admin().applier().state().topicByName(0, "ki"), nullptr);
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesKeyIndexDdl, CheckedWritesKeyIndexDdlRefusal,
                         ::testing::Range(0, static_cast<int>(sizeof kRefusals / sizeof kRefusals[0])),
                         [](const ::testing::TestParamInfo<int>& i) { return std::string(kRefusals[i.param].name); });

namespace {
struct WidthRow { const char* name; PrimType type; uint16_t width; };
const WidthRow kWidths[] = {{"uint16", PrimType::UInt16, 0}, {"int32", PrimType::Int32, 0},  {"uint32", PrimType::UInt32, 0},
                            {"int64", PrimType::Int64, 0},   {"uint64", PrimType::UInt64, 0}, {"string1", PrimType::FixedStr, 1},
                            {"string8", PrimType::FixedStr, 8},
                            {"string9", PrimType::FixedStr, 9},  {"string16", PrimType::FixedStr, 16}};
}  // namespace

class CheckedWritesKeyIndexDdlWidth : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesKeyIndexDdlWidth, EveryKeyWidthUpToSixteenIsAccepted) {
    const WidthRow& row = kWidths[GetParam()];
    CatalogDdlRig rig("cwkiw");
    atLevel5(rig);
    const auto a = defineKeyed(rig, "ki", keyedBlob(row.type, row.width), keyIndex());
    EXPECT_EQ(a.code, kOk) << a.detail;
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesKeyIndexDdl, CheckedWritesKeyIndexDdlWidth,
                         ::testing::Range(0, static_cast<int>(sizeof kWidths / sizeof kWidths[0])),
                         [](const ::testing::TestParamInfo<int>& i) { return std::string(kWidths[i.param].name); });

TEST(CheckedWritesKeyIndexDdl, AdoptionCanSetTheKeyIndex) {
    CatalogDdlRig rig("cwkiadopt");
    atLevel5(rig);
    CatalogDdlRig::DefineArgs raw; raw.name = "a";
    ASSERT_EQ(rig.define(raw).code, kOk);
    const auto a = defineKeyed(rig, "a", keyedBlob(PrimType::UInt64), keyIndex());
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::CatalogEvent adopted = lastEvent(rig.recordedLog());
    const auto* op = std::get_if<cat::CreateSchemaOps>(&adopted.ops);
    ASSERT_NE(op, nullptr);
    EXPECT_TRUE(op->schema.keyFeatures.keyIndex);
    ASSERT_NE(topicOf(rig, "a"), nullptr);
    EXPECT_TRUE(topicOf(rig, "a")->schema->keyFeatures.keyIndex);

    CatalogDdlRig::DefineArgs transient; transient.name = "tr"; transient.persistence = 1;
    ASSERT_EQ(rig.define(transient).code, kOk);
    const auto b = defineKeyed(rig, "tr", keyedBlob(PrimType::UInt64), keyIndex(), /*persistence=*/1);
    EXPECT_EQ(b.code, kInvalidRequest);
    EXPECT_EQ(b.detail, "key.index needs a persisted topic");
}

TEST(CheckedWritesKeyIndexDdl, BelowCheckedWritesWritesV2AndKeepsTheTopicFingerprint) {
    CatalogDdlRig low("cwkilow");
    low.becomeServingLeader();
    low.admin().setClusterFeatureLevel(4);
    ASSERT_EQ(defineKeyed(low, "p", keyedBlob(PrimType::UInt64), {}).code, kOk);
    const cat::CatalogEvent lowTopic = lastEvent(low.recordedLog());
    EXPECT_EQ(lowTopic.formatVer, cat::kCurrentFormatVer);
    ASSERT_EQ(low.createNamespace("n").code, kOk);
    EXPECT_EQ(lastEvent(low.recordedLog()).formatVer, cat::kCurrentFormatVer);

    CatalogDdlRig high("cwkihigh");
    atLevel5(high);
    ASSERT_EQ(defineKeyed(high, "p", keyedBlob(PrimType::UInt64), {}).code, kOk);
    const cat::CatalogEvent highTopic = lastEvent(high.recordedLog());
    EXPECT_EQ(highTopic.formatVer, cat::kFormatV3);
    EXPECT_EQ(highTopic.fingerprint, lowTopic.fingerprint);
    ASSERT_NE(topicOf(high, "p"), nullptr);
    EXPECT_EQ(topicOf(high, "p")->schema->keyFeatures, rp::TopicKeyFeatures{});
    ASSERT_EQ(high.createNamespace("n").code, kOk);
    EXPECT_EQ(lastEvent(high.recordedLog()).formatVer, cat::kFormatV3);
}

TEST(CheckedWritesKeyIndexDdl, SchemaResponseCarriesTheFeatures) {
    CatalogDdlRig rig("cwkiget");
    atLevel5(rig);
    ASSERT_EQ(defineKeyed(rig, "ki", keyedBlob(PrimType::UInt64), keyIndex()).code, kOk);
    ASSERT_EQ(defineKeyed(rig, "plain", keyedBlob(PrimType::UInt64), {}).code, kOk);
    EXPECT_EQ(rig.getSchema("ki", 0).keyFeatures, keyIndex());
    EXPECT_EQ(rig.getSchema("plain", 0).keyFeatures, rp::TopicKeyFeatures{});
    EXPECT_EQ(rig.getSchema("ki", rp::client::kSchemaLineageFingerprint).keyFeatures, keyIndex());
}

TEST(CheckedWritesKeyIndexDdlRegression, AlterKeepsTheKeyIndexRegression) {
    CatalogDdlRig rig("cwkialter");
    atLevel5(rig);
    ASSERT_EQ(defineKeyed(rig, "ki", keyedBlob(PrimType::UInt64), keyIndex()).code, kOk);
    const auto a = rig.alterAdd("ki", "extra");
    ASSERT_EQ(a.code, kOk) << a.detail;
    const cat::TopicObject* t = topicOf(rig, "ki");
    ASSERT_NE(t, nullptr);
    EXPECT_TRUE(t->schema->keyFeatures.keyIndex);
    const auto w = writeSpecWireOf(*t);
    ASSERT_TRUE(w);
    EXPECT_TRUE(w->keyIndex);
}

TEST(CheckedWritesKeyIndexDdl, AdoptionRefusals) {
    CatalogDdlRig::DefineArgs raw; raw.name = "a";
    CatalogDdlRig low("cwkiadlow");
    low.becomeServingLeader();
    low.admin().setClusterFeatureLevel(4);
    ASSERT_EQ(low.define(raw).code, kOk);
    auto a = defineKeyed(low, "a", keyedBlob(PrimType::UInt64), keyIndex());
    EXPECT_EQ(a.code, kInvalidRequest);
    EXPECT_EQ(a.detail, "key.index needs every broker at feature level 5");

    CatalogDdlRig rig("cwkiadwide");
    atLevel5(rig);
    ASSERT_EQ(rig.define(raw).code, kOk);
    a = defineKeyed(rig, "a", keyedBlob(PrimType::FixedStr, 9), keyIndex());
    EXPECT_EQ(a.code, kOk) << a.detail;
}
