#include <rune/node/WorkerReplicationValidator.h>

#include <gtest/gtest.h>

#include <rune/node/SchemaRegistry.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>

#include <cstdint>
#include <tuple>
#include <vector>

using rune::node::validateSymmetricWorkerCount;

TEST(WorkerReplicationValidator, AcceptsEqualCounts) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,4}});
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidator, ToleratesUnknownPeerCount) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,0}});   // peer 2 not yet advertised
    EXPECT_TRUE(r.ok);
}
TEST(WorkerReplicationValidatorRegression, RejectsMismatchNamingNode) {
    auto r = validateSymmetricWorkerCount(4, {{1,4},{2,2}});
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("node 2"), std::string::npos);
    EXPECT_NE(r.error.find("2"), std::string::npos);   // the offending count
}
TEST(WorkerReplicationValidatorRegression, RejectsOnMembershipChange) {
    // simulate a peer rejoining with a new count (same function, new peer list)
    auto r = validateSymmetricWorkerCount(4, {{1,4},{3,8}});
    EXPECT_FALSE(r.ok);
}

// ---------------------------------------------------------------------------
// GC3: version-keyed validator / registry mixed-version read.
//
// The registry materializes exactly one Validator per (topicId, version) --
// RegistryEntry::validator, built from RegistryEntry::descriptor, which is
// already materializeAsOf(evolving, entry.version) (see SchemaRegistry.cpp).
// This is the SAME descriptor executeSchemaUpdateWire_ receives over the
// wire and installs into a worker's validators_ map keyed by (topicId,
// version) (Worker.h). These tests exercise that per-version Validator
// directly, pulled via byIdVersion (never byId/latest), across a lineage
// that includes a DROP -- confirming each version's validator accepts only
// ITS OWN wire layout and rejects every other version's layout, even though
// all are installed for the same topic.
// ---------------------------------------------------------------------------
namespace {

using rune::codec::AlterOp;
using rune::codec::AlterOpType;
using rune::codec::CompatMode;
using rune::codec::Descriptor;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::codec::Validator;
using rune::node::RegistryEntry;
using rune::node::SchemaRegistry;
using rune::node::TopicSchema;

AlterOp validatorAlterAdd(const std::string& name, PrimType t) {
    AlterOp op{};
    op.type     = AlterOpType::Add;
    op.name     = name;
    op.primType = t;
    op.fixedLen = 0;
    return op;
}
AlterOp validatorAlterDrop(const std::string& name) {
    AlterOp op{};
    op.type = AlterOpType::Drop;
    op.name = name;
    return op;
}

// Same production ALTER-commit pattern as AdminWorker::startAlter_ /
// SchemaRegistryTest.cpp's nextVersionViaApplyOps: decode the latest
// evolving descriptor, applyOps(), persist the new evolving descriptor as
// the next version via applyRecord(). `validate=true` throughout so every
// version gets a materialized Validator (RegistryEntry::validator).
TopicSchema nextValidatedVersion(const RegistryEntry& latest, const std::vector<AlterOp>& ops) {
    Descriptor evolving = rune::codec::deserialize(std::span<const uint8_t>(
        latest.descriptorBlob.data(), latest.descriptorBlob.size()));
    uint16_t nextTag = latest.nextTag;
    const uint16_t newVersion = static_cast<uint16_t>(latest.version + 1);
    auto res = rune::codec::applyOps(evolving, ops, nextTag, newVersion);
    if (!res) {
        ADD_FAILURE() << "applyOps failed: " << res.error().message;
    }
    TopicSchema ts;
    ts.topicId        = latest.topicId;
    ts.version         = newVersion;
    ts.keyField        = latest.keyField;
    ts.validate         = true;
    ts.descriptorBlob  = rune::codec::serialize(*res);
    ts.nextTag          = nextTag;
    ts.compatMode       = latest.compatMode;
    return ts;
}

// Encode a raw [blockLength:u16][version:u16][fixed block] record -- the
// exact wire shape Validator::validate() and the worker's schemaRejects_
// both read. `fields` is (byteOffsetFromBlockStart, byteWidth, value).
std::vector<uint8_t> encodeFixedRecord(uint16_t version, uint32_t blockLength,
                                       const std::vector<std::tuple<uint32_t, uint32_t, uint64_t>>& fields) {
    std::vector<uint8_t> b(4 + blockLength, 0);
    b[0] = static_cast<uint8_t>(blockLength); b[1] = static_cast<uint8_t>(blockLength >> 8);
    b[2] = static_cast<uint8_t>(version);     b[3] = static_cast<uint8_t>(version >> 8);
    for (const auto& [offset, width, value] : fields) {
        for (uint32_t i = 0; i < width; ++i)
            b[4 + offset + i] = static_cast<uint8_t>(value >> (8 * i));
    }
    return b;
}

} // namespace

TEST(SchemaRegistryValidator, VersionKeyedValidatorsEachAcceptOnlyTheirOwnLayoutRegression) {
    SchemaRegistry reg;
    const int32_t topicId = 0x40002000;

    // v1: id (key), age.
    Descriptor v1 = DescriptorBuilder("lineage_val", /*version=*/1)
        .addTagged("id",  0, PrimType::Int64, 1, 0xFFFF, /*isKey=*/true,  {})
        .addTagged("age", 1, PrimType::Int32, 1, 0xFFFF, /*isKey=*/false, {})
        .build();
    TopicSchema ts1;
    ts1.topicId        = topicId;
    ts1.version         = 1;
    ts1.keyField        = "id";
    ts1.validate         = true;
    ts1.descriptorBlob  = rune::codec::serialize(v1);
    ts1.nextTag          = 2;
    ts1.compatMode       = CompatMode::Backward;
    reg.applyRecord(ts1);

    // v2: ADD "score".     live = {id, age, score}      blockLength = 20
    reg.applyRecord(nextValidatedVersion(*reg.latest(topicId), {validatorAlterAdd("score", PrimType::Int64)}));
    // v3: DROP "age".      live = {id, score}            blockLength = 16
    reg.applyRecord(nextValidatedVersion(*reg.latest(topicId), {validatorAlterDrop("age")}));
    // v4: ADD "level".     live = {id, score, level}      blockLength = 20
    reg.applyRecord(nextValidatedVersion(*reg.latest(topicId), {validatorAlterAdd("level", PrimType::Int32)}));
    ASSERT_EQ(reg.latest(topicId)->version, 4);

    // Pull the per-version validator exactly the way executeSchemaUpdateWire_'s
    // fan-out source does: byIdVersion (NEVER byId/latest) -- one Validator
    // per writer version, all four installed simultaneously for one topic.
    const RegistryEntry* e1 = reg.byIdVersion(topicId, 1);
    const RegistryEntry* e2 = reg.byIdVersion(topicId, 2);
    const RegistryEntry* e3 = reg.byIdVersion(topicId, 3);
    const RegistryEntry* e4 = reg.byIdVersion(topicId, 4);
    ASSERT_TRUE(e1 && e2 && e3 && e4);
    ASSERT_TRUE(e1->validator.has_value());
    ASSERT_TRUE(e2->validator.has_value());
    ASSERT_TRUE(e3->validator.has_value());
    ASSERT_TRUE(e4->validator.has_value());

    // --- SEQUENCE: each record, stamped with a DIFFERENT writer version,
    //     validates against its OWN version's validator. ---
    EXPECT_TRUE(e1->validator->validate(encodeFixedRecord(1, 12, {{0, 8, 7}, {8, 4, 30}})).ok)
        << "v1 record (id+age) must validate against the v1 validator";
    EXPECT_TRUE(e2->validator->validate(encodeFixedRecord(2, 20, {{0, 8, 7}, {8, 4, 30}, {12, 8, 99}})).ok)
        << "v2 record (id+age+score) must validate against the v2 validator";
    EXPECT_TRUE(e3->validator->validate(encodeFixedRecord(3, 16, {{0, 8, 7}, {8, 8, 99}})).ok)
        << "v3 record (id+score, age dropped) must validate against the v3 validator";
    EXPECT_TRUE(e4->validator->validate(encodeFixedRecord(4, 20, {{0, 8, 7}, {8, 8, 99}, {16, 4, 5}})).ok)
        << "v4 record (id+score+level) must validate against the v4 validator";

    // --- A record stamped vK carrying a DIFFERENT version's layout is
    //     REJECTED by vK's own validator (no cross-version false accept). ---
    // Stamped v3 (blockLength should be 16) but carrying the wider v2 layout
    // (20 bytes, still has the retired "age" field at its v2 offset).
    EXPECT_FALSE(e3->validator->validate(encodeFixedRecord(3, 20, {{0, 8, 7}, {8, 4, 30}, {12, 8, 99}})).ok)
        << "a v3-stamped record with the v2 (pre-drop) layout must be rejected by the v3 validator";
    // Stamped v2 (blockLength should be 20) but carrying the narrower v3
    // layout (16 bytes, age already gone).
    EXPECT_FALSE(e2->validator->validate(encodeFixedRecord(2, 16, {{0, 8, 7}, {8, 8, 99}})).ok)
        << "a v2-stamped record with the v3 (post-drop) layout must be rejected by the v2 validator";
    // Stamped v1 (blockLength should be 12) but carrying the v4 layout (20 bytes).
    EXPECT_FALSE(e1->validator->validate(encodeFixedRecord(1, 20, {{0, 8, 7}, {8, 8, 99}, {16, 4, 5}})).ok)
        << "a v1-stamped record with the v4 layout must be rejected by the v1 validator";
}
