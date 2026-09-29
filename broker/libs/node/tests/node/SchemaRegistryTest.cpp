#include <gtest/gtest.h>
#include "rune/node/SchemaRegistry.h"
#include <rune/codec/Descriptor.h>
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
