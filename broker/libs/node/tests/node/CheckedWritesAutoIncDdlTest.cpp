// CheckedWritesAutoIncDdlTest.cpp — checked writes Part 06 Task 4: the autoincrement DDL rules (P6-5), their
// normalization (P6-1) and the write spec's autoincrement fields, against the REAL AdminWorker (CatalogDdlRig).
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
constexpr uint64_t kMax53 = rp::kAutoIncrementMaxDefault;
const char* const kRetentionDetail = "autoincrement topics keep every record: retention.ms and retention.bytes must be -1";

std::vector<uint8_t> blobOf(const std::string& keyField, PrimType keyType, uint16_t width = 0) {
    rune::codec::DescriptorBuilder b("t", 1);
    if (keyType == PrimType::FixedStr) b.addFixedStr(keyField, width);
    else b.add(keyField, keyType);
    return rune::codec::serialize(b.add("createdAt", PrimType::Int64).build());
}

struct Def {
    std::string     name        = "user";
    std::string     keyField    = "userId";
    bool            typed       = true;
    PrimType        keyType     = PrimType::UInt64;
    uint16_t        width       = 0;
    int32_t         partitions  = 1;
    uint8_t         persistence = 0;
    uint8_t         cleanup     = 1;
    int64_t         retentionMs = -1;
    int64_t         retentionBytes = -1;
    rp::TopicKeyFeatures features{.autoIncrement = true};
};

CatalogDdlRig::Answer define(CatalogDdlRig& rig, const Def& d) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = d.name; m.keyField = d.keyField;
    if (d.typed) m.blob = blobOf(d.keyField.empty() ? "id" : d.keyField, d.keyType, d.width);
    m.partitionCount = d.partitions; m.persistence = d.persistence; m.keyFeatures = d.features;
    m.config.cleanupPolicy = d.cleanup; m.config.retentionMs = d.retentionMs; m.config.retentionBytes = d.retentionBytes;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

const cat::TopicObject* topicOf(CatalogDdlRig& rig, const std::string& name) {
    return rig.admin().applier().state().topicByName(0, name);
}

void atLevel5(CatalogDdlRig& rig) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(5);
}

Def userDef() { return Def{}; }
Def characterDef() {
    Def d;
    d.name = "character"; d.keyField = "characterId"; d.features.autoIncrementReserve = 128;
    return d;
}

}  // namespace

TEST(CheckedWritesAutoIncDdl, RegressorsPathTopicsAreAccepted) {
    CatalogDdlRig rig("cwaiok");
    atLevel5(rig);
    for (const Def& d : {userDef(), characterDef()}) {
        const auto a = define(rig, d);
        ASSERT_EQ(a.code, kOk) << d.name << ": " << a.detail;
        const cat::CatalogEvent e = rig.recordedLog().back().event;
        EXPECT_EQ(e.formatVer, cat::kFormatV3);
        const auto* op = std::get_if<cat::CreateTopicOps>(&e.ops);
        ASSERT_NE(op, nullptr);
        const rp::TopicKeyFeatures& f = op->schema->keyFeatures;
        EXPECT_TRUE(f.keyIndex);
        EXPECT_TRUE(f.autoIncrement);
        EXPECT_EQ(f.partitioner, rp::Partitioner::Modulo);
        EXPECT_EQ(f.autoIncrementMax, kMax53);
        EXPECT_EQ(f.autoIncrementReserve, d.features.autoIncrementReserve == rp::kAutoIncrementReserveDefault ? 4096u : 128u);

        const cat::TopicObject* t = topicOf(rig, d.name);
        ASSERT_NE(t, nullptr);
        const auto w = writeSpecWireOf(*t);
        ASSERT_TRUE(w);
        ASSERT_EQ(topicWriteSpecWireLen(*w), 26u);
        std::vector<uint8_t> bytes(26);
        ASSERT_EQ(encodeTopicWriteSpec(*w, bytes), 26u);
        const auto back = decodeTopicWriteSpec(bytes);
        ASSERT_TRUE(back);
        EXPECT_TRUE(back->autoIncrement);
        EXPECT_EQ(back->partitioner, rp::Partitioner::Modulo);
        EXPECT_EQ(back->autoIncrementMax, kMax53);
        EXPECT_EQ(back->autoIncrementReserve, f.autoIncrementReserve);
        EXPECT_EQ(back->partitionCount, 1);

        const auto s = rig.getSchema(d.name, 0);
        EXPECT_EQ(s.keyFeatures, f);
        EXPECT_EQ(s.keyFeatures.partitioner, rp::Partitioner::Modulo);
        EXPECT_EQ(s.partitionCount, 1);
    }
}

TEST(CheckedWritesAutoIncDdl, Uint32DefaultMaxIsTheTypeMax) {
    CatalogDdlRig rig("cwai32");
    atLevel5(rig);
    Def d; d.name = "ai32"; d.keyField = "id"; d.keyType = PrimType::UInt32; d.partitions = 3;
    const auto a = define(rig, d);
    ASSERT_EQ(a.code, kOk) << a.detail;
    EXPECT_EQ(topicOf(rig, "ai32")->schema->keyFeatures.autoIncrementMax, 4294967295u);
}

namespace {

struct RefusalRow {
    const char* name;
    int16_t     level;
    Def         def;
    bool        adopt;
    const char* detail;
};
Def with(auto&& edit) { Def d; edit(d); return d; }
const RefusalRow kRefusals[] = {
    {"Level4", 4, Def{}, false, "autoincrement needs every broker at feature level 5"},
    {"Untyped", 5, with([](Def& d) { d.typed = false; d.keyField = ""; }), false, "autoincrement needs a typed topic"},
    {"Keyless", 5, with([](Def& d) { d.keyField = ""; }), false, "autoincrement needs a key"},
    {"Transient", 5, with([](Def& d) { d.persistence = 1; }), false, "autoincrement needs a persisted topic"},
    {"Adoption", 5, Def{}, true, "autoincrement needs a new topic"},
    {"KeyUint16", 5, with([](Def& d) { d.keyType = PrimType::UInt16; }), false, "autoincrement needs a uint64, int64 or uint32 key"},
    {"KeyInt32", 5, with([](Def& d) { d.keyType = PrimType::Int32; }), false, "autoincrement needs a uint64, int64 or uint32 key"},
    {"KeyString8", 5, with([](Def& d) { d.keyType = PrimType::FixedStr; d.width = 8; }), false, "autoincrement needs a uint64, int64 or uint32 key"},
    {"PolicyDelete", 5, with([](Def& d) { d.cleanup = 0; }), false, "autoincrement needs cleanup.policy = compact"},
    {"PolicyCompactDelete", 5, with([](Def& d) { d.cleanup = 2; }), false, "autoincrement needs cleanup.policy = compact"},
    {"RetentionMs", 5, with([](Def& d) { d.retentionMs = 1000; }), false, kRetentionDetail},
    {"RetentionBytes", 5, with([](Def& d) { d.retentionBytes = 1; }), false, kRetentionDetail},
    {"ReserveZero", 5, with([](Def& d) { d.features.autoIncrementReserve = 0; }), false, "autoincrement.reserve must be between 1 and 1048576"},
    {"ReserveTooBig", 5, with([](Def& d) { d.features.autoIncrementReserve = 1'048'577; }), false, "autoincrement.reserve must be between 1 and 1048576"},
    {"MaxBelowLowerBound", 5, with([](Def& d) { d.partitions = 4; d.features.autoIncrementMax = 6; }), false,
     "autoincrement.max must be between 7 and 9007199254740991"},
    {"Uint32MaxAboveCap", 5, with([](Def& d) { d.keyType = PrimType::UInt32; d.features.autoIncrementMax = uint64_t{1} << 32; }), false,
     "autoincrement.max must be between 1 and 4294967295"},
    {"ModuloWithoutAutoIncrement", 5, with([](Def& d) { d.features = {.keyIndex = true, .partitioner = rp::Partitioner::Modulo}; }), false,
     "the modulo partitioner needs autoincrement"},
    {"ReserveWithoutAutoIncrement", 5, with([](Def& d) { d.features = {.keyIndex = true, .autoIncrementReserve = 8}; }), false,
     "autoincrement.max and autoincrement.reserve need autoincrement"},
};

}  // namespace

class CheckedWritesAutoIncDdlRefusal : public ::testing::TestWithParam<int> {};

TEST_P(CheckedWritesAutoIncDdlRefusal, Refusals) {
    const RefusalRow& row = kRefusals[GetParam()];
    CatalogDdlRig rig("cwairef");
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(row.level);
    if (row.adopt) {
        CatalogDdlRig::DefineArgs raw; raw.name = row.def.name;
        ASSERT_EQ(rig.define(raw).code, kOk);
    }
    const auto a = define(rig, row.def);
    EXPECT_EQ(a.code, kInvalidRequest) << a.detail;
    EXPECT_EQ(a.detail, row.detail);
    const cat::TopicObject* t = topicOf(rig, row.def.name);
    EXPECT_TRUE(t == nullptr || !t->schema) << "nothing committed";
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesAutoIncDdl, CheckedWritesAutoIncDdlRefusal,
                         ::testing::Range(0, static_cast<int>(sizeof kRefusals / sizeof kRefusals[0])),
                         [](const ::testing::TestParamInfo<int>& i) { return std::string(kRefusals[i.param].name); });

TEST(CheckedWritesAutoIncDdl, MaxAtTheLowerBoundIsAccepted) {
    CatalogDdlRig rig("cwailow");
    atLevel5(rig);
    Def d; d.partitions = 4; d.features.autoIncrementMax = 7;
    const auto a = define(rig, d);
    EXPECT_EQ(a.code, kOk) << a.detail;
}

TEST(CheckedWritesAutoIncDdl, RequestedPartitionerAndKeyIndexAreNormalized) {
    CatalogDdlRig rig("cwainorm");
    atLevel5(rig);
    Def d; d.features = {.keyIndex = false, .autoIncrement = true, .partitioner = rp::Partitioner::Fnv1a};
    const auto a = define(rig, d);
    ASSERT_EQ(a.code, kOk) << a.detail;
    EXPECT_TRUE(topicOf(rig, "user")->schema->keyFeatures.keyIndex);
    EXPECT_EQ(topicOf(rig, "user")->schema->keyFeatures.partitioner, rp::Partitioner::Modulo);
}

TEST(CheckedWritesAutoIncDdlRegression, AlterConfigCannotAddRetentionRegression) {
    CatalogDdlRig rig("cwairet");
    atLevel5(rig);
    ASSERT_EQ(define(rig, userDef()).code, kOk);
    const auto bad = rig.alterConfig("user", {{cat::ConfigKey::RetentionMs, 60'000}});
    EXPECT_EQ(bad.code, kInvalidRequest);
    EXPECT_EQ(bad.detail, kRetentionDetail);
    EXPECT_EQ(topicOf(rig, "user")->config.retentionMs, -1);
    const auto good = rig.alterConfig("user", {{cat::ConfigKey::DeleteRetentionMs, 0}});
    EXPECT_EQ(good.code, kOk) << good.detail;
}

TEST(CheckedWritesAutoIncDdlRegression, AlterSchemaKeepsAutoIncrementRegression) {
    CatalogDdlRig rig("cwaialter");
    atLevel5(rig);
    ASSERT_EQ(define(rig, characterDef()).code, kOk);
    const rp::TopicKeyFeatures before = topicOf(rig, "character")->schema->keyFeatures;
    const auto wireBefore = writeSpecWireOf(*topicOf(rig, "character"));
    const auto a = rig.alterAdd("character", "extra");
    ASSERT_EQ(a.code, kOk) << a.detail;
    EXPECT_EQ(topicOf(rig, "character")->schema->keyFeatures, before);
    EXPECT_TRUE(before.autoIncrement);
    EXPECT_EQ(writeSpecWireOf(*topicOf(rig, "character")), wireBefore);
}
