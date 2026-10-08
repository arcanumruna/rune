#include <gtest/gtest.h>
#include "rune/node/SchemaRegistry.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::node;
using namespace rune::codec;

static TopicSchema makeSchema(int32_t id, const char* name, bool validate) {
    Descriptor d = DescriptorBuilder(name, 1)
        .add("id", PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("strength", PrimType::Int32)
        .build();
    TopicSchema ts; ts.topicId = id; ts.version = 1; ts.keyField = "userId";
    ts.validate = validate; ts.descriptorBlob = serialize(d); return ts;
}

TEST(SchemaRegistry, TopicSchemaRecordRoundTrip) {
    TopicSchema ts = makeSchema(0x40000000, "characters", true);
    auto bytes = ts.serialize();
    auto back = TopicSchema::deserialize(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->topicId, 0x40000000);
    EXPECT_EQ(back->keyField, "userId");
    EXPECT_TRUE(back->validate);
    EXPECT_EQ(back->descriptorBlob, ts.descriptorBlob);
}

TEST(SchemaRegistry, ApplyBuildsValidatorIffValidate) {
    SchemaRegistry reg;
    reg.applyRecord(makeSchema(0x40000000, "chars", true));
    reg.applyRecord(makeSchema(0x40000001, "pos", false));
    const auto* a = reg.byName("chars");
    ASSERT_NE(a, nullptr);
    EXPECT_TRUE(a->validator.has_value());
    EXPECT_EQ(a->fingerprint, fingerprint(a->descriptor));
    const auto* b = reg.byName("pos");
    ASSERT_NE(b, nullptr);
    EXPECT_FALSE(b->validator.has_value());
    EXPECT_EQ(reg.byId(0x40000000), a);
}

TEST(SchemaRegistry, RuntimeIdHighWaterAdvancesAndRecovers) {
    SchemaRegistry reg;
    EXPECT_EQ(reg.allocateRuntimeId(), static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE));
    EXPECT_EQ(reg.allocateRuntimeId(), static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE) + 1);
    // Rebuild from records with a higher id -> high-water jumps past it.
    std::vector<TopicSchema> recs{ makeSchema(0x40000005, "z", false) };
    SchemaRegistry reg2; reg2.rebuildFromRecords(recs);
    EXPECT_EQ(reg2.allocateRuntimeId(), 0x40000006);
}

// Regression: a negative internal id (e.g. kSchemaTopicId = -4) must NOT advance
// the runtime-id high-water. Prior to the M1 fix, uint32_t(-4) = 0xFFFFFFFC would
// satisfy the >= RUNTIME_TOPIC_ID_BASE guard and corrupt the water mark.
TEST(SchemaRegistryRegression, NegativeInternalIdDoesNotAdvanceHighWater) {
    SchemaRegistry reg;
    reg.applyRecord(makeSchema(kSchemaTopicId, "__topic_schemas", false));
    EXPECT_EQ(reg.runtimeIdHighWater(), RUNTIME_TOPIC_ID_BASE);
    EXPECT_EQ(reg.allocateRuntimeId(), static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE));
    // The internal record is still indexed and retrievable by its negative id.
    const auto* e = reg.byId(kSchemaTopicId);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->topicId, kSchemaTopicId);
}

// ---------------------------------------------------------------------------
// Task 9: multi-version registry + per-version fingerprint lookup
// ---------------------------------------------------------------------------

// The cumulative "evolving" descriptor as it stood right after the v1
// commit: 3 fields, all live since v1, none retired yet.
static Descriptor evolvingV1() {
    return DescriptorBuilder("chars_evo", 1)
        .addTagged("id",       0, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("userId",   1, PrimType::Int64, 1, 0xFFFF, true,  {})
        .addTagged("strength", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .build();
}

// The cumulative "evolving" descriptor as it stood right after the v2
// commit (an ADD): same 3 original fields, plus a 4th ("level") that only
// became live at v2.
static Descriptor evolvingV2() {
    return DescriptorBuilder("chars_evo", 2)
        .addTagged("id",       0, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("userId",   1, PrimType::Int64, 1, 0xFFFF, true,  {})
        .addTagged("strength", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .addTagged("level",    3, PrimType::Int32, 2, 0xFFFF, false, {})
        .build();
}

static TopicSchema makeTopicSchema(int32_t topicId, uint16_t version, const Descriptor& evolving) {
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = version;
    ts.keyField        = "userId";
    ts.validate        = false;
    ts.descriptorBlob  = serialize(evolving);
    ts.nextTag         = static_cast<uint16_t>(evolving.fields.size());
    ts.compatMode      = 0;
    return ts;
}

TEST(SchemaRegistry, KeepsMultipleVersionsAndLooksUpByFingerprint) {
    rune::node::SchemaRegistry reg;
    reg.applyRecord(makeTopicSchema(/*topicId=*/1, /*version=*/1, evolvingV1()));
    reg.applyRecord(makeTopicSchema(1, 2, evolvingV2()));

    ASSERT_NE(reg.byIdVersion(1, 1), nullptr);
    ASSERT_NE(reg.byIdVersion(1, 2), nullptr);
    EXPECT_EQ(reg.latest(1)->version, 2);

    // v1's materialized view has 3 live fields; v2's has 4 -> distinct fingerprints.
    EXPECT_EQ(reg.byIdVersion(1, 1)->descriptor.fields.size(), 3u);
    EXPECT_EQ(reg.byIdVersion(1, 2)->descriptor.fields.size(), 4u);

    auto fp1 = reg.byIdVersion(1, 1)->fingerprint;
    EXPECT_EQ(reg.byFingerprint(1, fp1)->version, 1);   // old fingerprint resolves to v1

    auto fp2 = reg.byIdVersion(1, 2)->fingerprint;
    EXPECT_EQ(reg.byFingerprint(1, fp2)->version, 2);
    EXPECT_NE(fp1, fp2);

    EXPECT_EQ(reg.nextTagFor(1), 4);
}

// Regression: a legacy TopicSchema record (serialized before nextTag/compatMode
// existed) must still deserialize cleanly, defaulting compatMode=BACKWARD and
// nextTag to the embedded descriptor's field count.
TEST(SchemaRegistryRegression, LegacyRecordWithoutTrailingFieldsDeserializesWithDefaults) {
    TopicSchema ts = makeSchema(0x40000002, "legacy", false);
    auto bytes = ts.serialize();
    // Strip the trailing (nextTag:u16, compatMode:u8) bytes this build's
    // serialize() appended, to simulate a pre-Task-9 record on disk.
    ASSERT_GE(bytes.size(), 3u);
    bytes.resize(bytes.size() - 3);

    auto back = TopicSchema::deserialize(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->compatMode, 0);
    EXPECT_EQ(back->nextTag, 3u);  // makeSchema() builds a 3-field descriptor
}
