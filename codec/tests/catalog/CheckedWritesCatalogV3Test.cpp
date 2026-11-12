// Checked writes Task 2: catalog formatVer 3 — the TopicKeyFeatures block in every SchemaDef body, ApplyV3 over
// ApplyV2 (carry + the key.index rules), the fingerprint layer and the v3 golden log.
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/CatalogTypes.h>
#include <gtest/gtest.h>

#include <fstream>
#include <functional>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "CatalogGoldenPins.h"

namespace rc = rune::catalog;
namespace rp = rune::protocol;
namespace cd = rune::codec;

namespace {

std::vector<uint8_t> keyedBlob() {
    cd::DescriptorBuilder b("t");
    b.add("id", cd::PrimType::Int64, 1, /*isKey=*/true).add("v", cd::PrimType::Int32);
    return cd::serialize(b.build());
}
rp::TopicKeyFeatures keyIndexed() {
    rp::TopicKeyFeatures f;
    f.keyIndex = true;
    return f;
}
rc::SchemaDef schemaDef(const rp::TopicKeyFeatures& f, std::string keyField = "id") {
    return rc::SchemaDef{.keyField = std::move(keyField), .validate = true, .descriptorBlob = keyedBlob(), .nextTag = 2, .keyFeatures = f};
}
rc::CreateTopicOps createOps(int32_t topicId, const rp::TopicKeyFeatures& f, rc::Persistence p = rc::Persistence::Disk) {
    rc::CreateTopicOps o;
    o.topicId = topicId; o.partitions = 2; o.persistence = p;
    o.schema = schemaDef(f);
    return o;
}
rc::CatalogEvent event(uint16_t formatVer, rc::Ops ops, std::string name, uint64_t objectId, uint32_t prev, uint32_t nsId = 0) {
    rc::CatalogEvent e;
    e.formatVer = formatVer; e.nsId = nsId;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = prev + 1;
    return e;
}
/// The leader's way: preview the fingerprint, stamp it, apply at the next offset.
std::expected<rc::Applied, rc::ApplyError> put(rc::CatalogState& s, rc::CatalogEvent e) {
    auto fp = rc::previewFingerprint(s, e);
    if (!fp) return std::unexpected(fp.error());
    e.fingerprint = *fp;
    return rc::apply(s, e, s.appliedOffset() + 1);
}
rc::AlterSchemaOps addField() {
    rc::ResolvedAlterOp op;
    op.type = cd::AlterOpType::Add; op.tag = 2; op.name = "w"; op.primType = cd::PrimType::Int64;
    return rc::AlterSchemaOps{{op}};
}
std::vector<uint8_t> blockBytes(const rp::TopicKeyFeatures& f) {
    std::vector<uint8_t> b(rp::kTopicKeyFeaturesWireLen);
    rp::putTopicKeyFeatures(b.data(), f);
    return b;
}
std::vector<uint8_t> tail(const std::vector<uint8_t>& b) {
    return {b.end() - static_cast<std::ptrdiff_t>(rp::kTopicKeyFeaturesWireLen), b.end()};
}

rp::TopicKeyFeatures autoInc(uint32_t reserve = 4096, uint64_t max = rp::kAutoIncrementMaxDefault) {
    rp::TopicKeyFeatures f;
    f.keyIndex = true; f.autoIncrement = true; f.partitioner = rp::Partitioner::Modulo;
    f.autoIncrementMax = max; f.autoIncrementReserve = reserve;
    return f;
}
rc::SchemaDef typedSchema(cd::PrimType keyType, const rp::TopicKeyFeatures& f) {
    cd::DescriptorBuilder b("t");
    b.add("id", keyType, 1, /*isKey=*/true).add("v", cd::PrimType::Int32);
    return rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = cd::serialize(b.build()), .nextTag = 2, .keyFeatures = f};
}
rc::CreateTopicOps autoIncOps(cd::PrimType keyType, const rp::TopicKeyFeatures& f) {
    rc::CreateTopicOps o;
    o.topicId = 0; o.partitions = 2; o.config.cleanupPolicy = 1;
    o.schema = typedSchema(keyType, f);
    return o;
}

} // namespace

TEST(CheckedWritesCatalogV3, SchemaDefBlockRoundTripsInV3) {
    rp::TopicKeyFeatures f = keyIndexed();
    f.partitioner = rp::Partitioner::Modulo; f.autoIncrementMax = 99; f.autoIncrementReserve = 7;
    const auto e = event(rc::kFormatV3, createOps(0, f), "t", 1, 0);
    const auto bytes = rc::encode(e);
    ASSERT_TRUE(bytes.has_value());
    EXPECT_EQ(tail(*bytes), blockBytes(f));
    const auto d = rc::decode(*bytes);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, e);

    const auto c = event(rc::kFormatV3, rc::CreateSchemaOps{schemaDef(f)}, "t", 1, 1);
    const auto cb = rc::encode(c);
    ASSERT_TRUE(cb.has_value());
    EXPECT_EQ(tail(*cb), blockBytes(f));
    EXPECT_EQ(rc::decode(*cb).value(), c);
}

TEST(CheckedWritesCatalogV3, DefaultFeaturesStillEncodeTheBlockInV3) {
    const auto v3 = rc::encode(event(rc::kFormatV3, createOps(0, {}), "t", 1, 0)).value();
    const auto v2 = rc::encode(event(rc::kFormatV2, createOps(0, {}), "t", 1, 0)).value();
    EXPECT_EQ(v3.size(), v2.size() + rp::kTopicKeyFeaturesWireLen);
    EXPECT_EQ(tail(v3), blockBytes({}));
    const auto d = rc::decode(v3);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(std::get<rc::CreateTopicOps>(d->ops).schema->keyFeatures, rp::TopicKeyFeatures{});
}

TEST(CheckedWritesCatalogV3Regression, V1AndV2EventsWithFeaturesAreRefusedRegression) {
    for (const uint16_t v : {rc::kFormatV1, rc::kFormatV2}) {
        const auto refused = rc::encode(event(v, createOps(0, keyIndexed()), "t", 1, 0));
        ASSERT_FALSE(refused.has_value()) << v;
        EXPECT_EQ(refused.error(), rc::EncodeError::NeedsFormatV3) << v;
        const auto refusedSchema = rc::encode(event(v, rc::CreateSchemaOps{schemaDef(keyIndexed())}, "t", 1, 1));
        ASSERT_FALSE(refusedSchema.has_value()) << v;
        EXPECT_EQ(refusedSchema.error(), rc::EncodeError::NeedsFormatV3) << v;

        const auto plain = event(v, createOps(0, {}), "t", 1, 0);   // default features: no block, the old bytes
        const auto bytes = rc::encode(plain);
        ASSERT_TRUE(bytes.has_value()) << v;
        EXPECT_EQ(rc::decode(*bytes).value(), plain) << v;
        const std::size_t nsIdBytes = v == rc::kFormatV1 ? sizeof(uint32_t) : 0;   // formatVer 1 has no nsId field
        EXPECT_EQ(rc::encode(event(rc::kFormatV3, createOps(0, {}), "t", 1, 0)).value().size(),
                  bytes->size() + nsIdBytes + rp::kTopicKeyFeaturesWireLen) << v;
    }
}

TEST(CheckedWritesCatalogV3, BadFlagBitsAreBadEnum) {
    const auto good = rc::encode(event(rc::kFormatV3, createOps(0, keyIndexed()), "t", 1, 0)).value();
    const std::size_t at = good.size() - rp::kTopicKeyFeaturesWireLen;
    auto flags = good;  flags[at] = 0x04;
    EXPECT_EQ(rc::decode(flags).error(), rc::DecodeError::BadEnum);
    auto partitioner = good;  partitioner[at + 1] = 2;
    EXPECT_EQ(rc::decode(partitioner).error(), rc::DecodeError::BadEnum);
    auto cut = good;  cut.pop_back();
    EXPECT_FALSE(rc::decode(cut).has_value());
}

TEST(CheckedWritesCatalogV3Regression, EncodeRefusesAPartitionerTheDecoderRefusesRegression) {
    rp::TopicKeyFeatures f;
    f.partitioner = static_cast<rp::Partitioner>(2);
    const auto topic = rc::encode(event(rc::kFormatV3, createOps(0, f), "t", 1, 0));
    ASSERT_FALSE(topic.has_value());
    EXPECT_EQ(topic.error(), rc::EncodeError::BadEnum);
    const auto schema = rc::encode(event(rc::kFormatV3, rc::CreateSchemaOps{schemaDef(f)}, "t", 1, 1));
    ASSERT_FALSE(schema.has_value());
    EXPECT_EQ(schema.error(), rc::EncodeError::BadEnum);
}

TEST(CheckedWritesCatalogV3, V3KeepsNamespaces) {
    const auto t = event(rc::kFormatV3, createOps(0, keyIndexed()), "t", 1, 0, /*nsId=*/7);
    EXPECT_EQ(rc::decode(rc::encode(t).value()).value().nsId, 7u);
    const auto ns = event(rc::kFormatV3, rc::NamespaceCreatedOps{"ns1", {}}, "ns1", 0, 0, 1);
    const auto d = rc::decode(rc::encode(ns).value());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, ns);
}

TEST(CheckedWritesCatalogV3, ApplyCarriesFeaturesAcrossAlterSchema) {
    for (const uint16_t alterFormat : {rc::kFormatV3, rc::kFormatV2}) {
        rc::CatalogState s;
        ASSERT_TRUE(put(s, event(rc::kFormatV3, createOps(0, keyIndexed()), "ki", 1, 0)).has_value());
        const auto before = s.topicByName(rc::kDefaultNsId, "ki")->schema->keyFeatures;
        EXPECT_TRUE(before.keyIndex);
        auto r = put(s, event(alterFormat, addField(), "ki", 1, 1));
        ASSERT_TRUE(r.has_value()) << r.error().detail;
        const auto* t = s.topicByName(rc::kDefaultNsId, "ki");
        EXPECT_EQ(t->schema->schemaVersion, 2) << alterFormat;
        EXPECT_TRUE(t->schema->keyFeatures.keyIndex) << alterFormat;
    }
}

TEST(CheckedWritesCatalogV3, ApplyAdoptsFeaturesOnCreateSchema) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, event(rc::kFormatV3, rc::CreateTopicOps{.topicId = 0}, "raw", 1, 0)).has_value());
    auto r = put(s, event(rc::kFormatV3, rc::CreateSchemaOps{schemaDef(keyIndexed())}, "raw", 1, 1));
    ASSERT_TRUE(r.has_value()) << r.error().detail;
    EXPECT_TRUE(s.topicByName(rc::kDefaultNsId, "raw")->schema->keyFeatures.keyIndex);
}

TEST(CheckedWritesCatalogV3, ApplyRefusesKeyIndexWithoutAKeyOrOnTransient) {
    rc::CatalogState s;
    auto noKey = createOps(0, keyIndexed());
    noKey.schema->keyField.clear();
    auto a = put(s, event(rc::kFormatV3, noKey, "nk", 1, 0));
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_NE(a.error().detail.find("key.index"), std::string::npos) << a.error().detail;

    auto b = put(s, event(rc::kFormatV3, createOps(0, keyIndexed(), rc::Persistence::Transient), "tr", 1, 0));
    ASSERT_FALSE(b.has_value());
    EXPECT_EQ(b.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_NE(b.error().detail.find("key.index"), std::string::npos) << b.error().detail;
    EXPECT_EQ(s.topics().size(), 0u) << "a refused event changes nothing";
}

TEST(CheckedWritesCatalogV3, FingerprintIgnoresDefaultFeatures) {
    rc::CatalogState s2, s3, sk;
    const auto e2 = event(rc::kFormatV2, createOps(0, {}), "t", 1, 0);
    const auto e3 = event(rc::kFormatV3, createOps(0, {}), "t", 1, 0);
    const auto ek = event(rc::kFormatV3, createOps(0, keyIndexed()), "t", 1, 0);
    EXPECT_EQ(rc::previewFingerprint(s2, e2).value(), rc::previewFingerprint(s3, e3).value());
    EXPECT_NE(rc::previewFingerprint(s3, e3).value(), rc::previewFingerprint(sk, ek).value());
    ASSERT_TRUE(put(s2, e2).has_value());
    ASSERT_TRUE(put(s3, e3).has_value());
    ASSERT_TRUE(put(sk, ek).has_value());
    EXPECT_EQ(s2.digest(), s3.digest());
    EXPECT_NE(s3.digest(), sk.digest());
}

TEST(CheckedWritesCatalogV3, FormatSixHasNoRules) {
    rc::CatalogState s;
    auto e = event(6, rc::CreateTopicOps{.topicId = 0}, "t", 1, 0);
    EXPECT_EQ(rc::apply(s, e, 0).error().code, rc::ApplyError::Code::UnsupportedFormat);
    EXPECT_EQ(rc::previewFingerprint(s, e).error().code, rc::ApplyError::Code::UnsupportedFormat);
}

TEST(CatalogV3GoldenRegression, LogReplaysToPinnedFingerprints) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/basic.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rc::golden::kBasicV3Digests));
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), rc::golden::kBasicV3Digests[i]) << "offset " << i;
    }
    const auto& s = r.state();
    EXPECT_TRUE(s.topicByName(rc::kDefaultNsId, "ki")->schema->keyFeatures.keyIndex) << "kept across the AlterSchema";
    EXPECT_TRUE(s.topicByName(rc::kDefaultNsId, "raw")->schema->keyFeatures.keyIndex) << "adopted by CreateSchema";
    EXPECT_EQ(s.topicByName(rc::kDefaultNsId, "plain"), nullptr);
    EXPECT_NE(s.namespaces().byName("ns1"), nullptr);
}

TEST(CatalogV3GoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytesRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/basic.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV3();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i)
        EXPECT_EQ((*records)[i].second, rc::encode(script[i]).value()) << "offset " << i;
}

TEST(CatalogV3GoldenRegression, TheGoldenLogDecodesToTheScriptEventByEvent) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/basic.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV3();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        auto e = rc::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << "offset " << i;
        EXPECT_EQ(*e, script[i]) << "offset " << i;
        EXPECT_EQ(e->formatVer, rc::kFormatV3);
    }
}

struct AutoIncRuleCase { const char* name; std::function<rc::CatalogEvent()> make; };

class CheckedWritesCatalogV3AutoIncRules : public ::testing::TestWithParam<AutoIncRuleCase> {};

TEST_P(CheckedWritesCatalogV3AutoIncRules, AutoIncrementApplyRules) {
    rc::CatalogState s;
    const auto e = GetParam().make();
    if (e.kind == rc::EventKind::CreateSchema) ASSERT_TRUE(put(s, event(rc::kFormatV3, rc::CreateTopicOps{.topicId = 0}, "raw", 1, 0)).has_value());
    const auto r = rc::previewFingerprint(s, e);
    ASSERT_FALSE(r.has_value()) << GetParam().name;
    EXPECT_EQ(r.error().code, rc::ApplyError::Code::Precondition) << r.error().detail;
}

INSTANTIATE_TEST_SUITE_P(Rules, CheckedWritesCatalogV3AutoIncRules, ::testing::Values(
    AutoIncRuleCase{"NoKeyIndex", [] { auto f = autoInc(); f.keyIndex = false; return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, f), "t", 1, 0); }},
    AutoIncRuleCase{"Fnv1a", [] { auto f = autoInc(); f.partitioner = rp::Partitioner::Fnv1a; return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, f), "t", 1, 0); }},
    AutoIncRuleCase{"PolicyDelete", [] { auto o = autoIncOps(cd::PrimType::UInt64, autoInc()); o.config.cleanupPolicy = 0; return event(rc::kFormatV3, o, "t", 1, 0); }},
    AutoIncRuleCase{"Retention", [] { auto o = autoIncOps(cd::PrimType::UInt64, autoInc()); o.config.retentionMs = 1; return event(rc::kFormatV3, o, "t", 1, 0); }},
    AutoIncRuleCase{"RetentionBytes", [] { auto o = autoIncOps(cd::PrimType::UInt64, autoInc()); o.config.retentionBytes = 1; return event(rc::kFormatV3, o, "t", 1, 0); }},
    AutoIncRuleCase{"Uint16Key", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt16, autoInc()), "t", 1, 0); }},
    AutoIncRuleCase{"Transient", [] { auto o = autoIncOps(cd::PrimType::UInt64, autoInc()); o.persistence = rc::Persistence::Transient; return event(rc::kFormatV3, o, "t", 1, 0); }},
    AutoIncRuleCase{"ReserveZero", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc(0)), "t", 1, 0); }},
    AutoIncRuleCase{"ReserveTooBig", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc(1'048'577)), "t", 1, 0); }},
    AutoIncRuleCase{"MaxBelowTwoPMinusOne", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc(4096, 2)), "t", 1, 0); }},
    AutoIncRuleCase{"MaxAboveCap", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc(4096, rp::kAutoIncrementMaxDefault + 1)), "t", 1, 0); }},
    AutoIncRuleCase{"Uint32MaxAboveCap", [] { return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt32, autoInc(4096, uint64_t{1} << 32)), "t", 1, 0); }},
    AutoIncRuleCase{"CreateSchemaAdoption", [] { return event(rc::kFormatV3, rc::CreateSchemaOps{typedSchema(cd::PrimType::UInt64, autoInc())}, "raw", 1, 1); }},
    AutoIncRuleCase{"ModuloWithoutAutoIncrement", [] { rp::TopicKeyFeatures f; f.keyIndex = true; f.partitioner = rp::Partitioner::Modulo; return event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, f), "t", 1, 0); }}),
    [](const auto& i) { return std::string(i.param.name); });

TEST(CheckedWritesCatalogV3, AutoIncrementTopicAppliesAndFingerprints) {
    rc::CatalogState s, plain;
    const auto e = event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc()), "user", 1, 0);
    ASSERT_TRUE(put(s, e).has_value());
    ASSERT_TRUE(put(plain, event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, {}), "user", 1, 0)).has_value());
    EXPECT_TRUE(s.topicByName(rc::kDefaultNsId, "user")->schema->keyFeatures.autoIncrement);
    EXPECT_NE(s.digest(), plain.digest());
}

TEST(CheckedWritesCatalogV3, AlterTopicConfigCannotAddRetentionToAnAutoIncrementTopic) {
    rc::CatalogState s;
    ASSERT_TRUE(put(s, event(rc::kFormatV3, autoIncOps(cd::PrimType::UInt64, autoInc()), "user", 1, 0)).has_value());
    const auto bad = event(rc::kFormatV3, rc::AlterTopicConfigOps{{{rc::ConfigKey::RetentionMs, 60'000}}}, "user", 1, 1);
    const auto r = rc::previewFingerprint(s, bad);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, rc::ApplyError::Code::Precondition);
    const auto ok = event(rc::kFormatV3, rc::AlterTopicConfigOps{{{rc::ConfigKey::DeleteRetentionMs, 0}}}, "user", 1, 1);
    EXPECT_TRUE(rc::previewFingerprint(s, ok).has_value());
}

TEST(CatalogV3AutoIncGoldenRegression, LogReplaysToPinnedFingerprints) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/autoinc.rcat", std::ios::binary);
    ASSERT_TRUE(in) << "missing golden log";
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value()) << records.error();
    ASSERT_EQ(records->size(), std::size(rc::golden::kAutoIncV3Digests));
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < records->size(); ++i) {
        auto a = r.feed((*records)[i].first, (*records)[i].second);
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(r.state().digest(), rc::golden::kAutoIncV3Digests[i]) << "offset " << i;
    }
    const auto* user = r.state().topicByName(rc::kDefaultNsId, "user");
    const auto* ch   = r.state().topicByName(rc::kDefaultNsId, "character");
    ASSERT_NE(user, nullptr);
    ASSERT_NE(ch, nullptr);
    EXPECT_TRUE(user->schema->keyFeatures.autoIncrement);
    EXPECT_EQ(ch->schema->keyFeatures.autoIncrementReserve, 128u) << "kept across the AlterSchema";
    EXPECT_EQ(r.state().topicByName(rc::kDefaultNsId, "ai32")->schema->keyFeatures.autoIncrementMax, 4'294'967'295u);
}

TEST(CatalogV3AutoIncGoldenRegression, TheGoldenScriptStillProducesTheCheckedInBytesRegression) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/autoinc.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV3AutoInc();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i)
        EXPECT_EQ((*records)[i].second, rc::encode(script[i]).value()) << "offset " << i;
}

TEST(CatalogV3AutoIncGoldenRegression, TheGoldenLogDecodesToTheScriptEventByEvent) {
    std::ifstream in(RUNE_CATALOG_GOLDEN_DIR "/v3/autoinc.rcat", std::ios::binary);
    auto records = rc::readRcat(in);
    ASSERT_TRUE(records.has_value());
    const auto script = rc::testing::goldenScriptV3AutoInc();
    ASSERT_EQ(records->size(), script.size());
    for (std::size_t i = 0; i < script.size(); ++i) {
        auto e = rc::decode((*records)[i].second);
        ASSERT_TRUE(e.has_value()) << "offset " << i;
        EXPECT_EQ(*e, script[i]) << "offset " << i;
        EXPECT_EQ(e->formatVer, rc::kFormatV3);
    }
}
