#include <gtest/gtest.h>
#include "rune/node/SchemaRegistry.h"
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
#include <algorithm>
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
    ts.compatMode      = CompatMode::Backward;
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
    EXPECT_EQ(back->compatMode, CompatMode::Backward);
    EXPECT_EQ(back->nextTag, 3u);  // makeSchema() builds a 3-field descriptor
}

// Regression: a trailing tail that is PRESENT but truncated (1 or 2 of the 3
// bytes) must be rejected as malformed, not silently treated as a legacy
// (zero-trailing-byte) record.
TEST(SchemaRegistryRegression, PartiallyTruncatedTrailingTailIsRejected) {
    TopicSchema ts = makeSchema(0x40000003, "partial_tail", false);
    ts.nextTag = 7;
    ts.compatMode = CompatMode::Forward;
    auto bytes = ts.serialize();
    ASSERT_GE(bytes.size(), 3u);

    // Drop 2 of the 3 trailing bytes -> exactly 1 leftover byte.
    auto oneLeftover = bytes;
    oneLeftover.resize(oneLeftover.size() - 2);
    EXPECT_FALSE(TopicSchema::deserialize(oneLeftover).has_value());

    // Drop 1 of the 3 trailing bytes -> exactly 2 leftover bytes.
    auto twoLeftover = bytes;
    twoLeftover.resize(twoLeftover.size() - 1);
    EXPECT_FALSE(TopicSchema::deserialize(twoLeftover).has_value());

    // Sanity: the untruncated record still deserializes fine.
    auto whole = TopicSchema::deserialize(bytes);
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(whole->nextTag, 7);
    EXPECT_EQ(whole->compatMode, CompatMode::Forward);
}

// Regression: a `const RegistryEntry*` taken from latest() before a new
// version is applied must remain valid afterward -- byId_'s per-topic
// container is a deque specifically so growing it (Task 11's ALTER flow:
// read latest(), applyRecord() the new version, read back) never
// reallocates and invalidates a previously-returned pointer.
TEST(SchemaRegistryRegression, LatestPointerSurvivesSubsequentApplyRecord) {
    SchemaRegistry reg;
    reg.applyRecord(makeTopicSchema(1, 1, evolvingV1()));

    const RegistryEntry* p1 = reg.latest(1);
    ASSERT_NE(p1, nullptr);
    const uint64_t fp1Before = p1->fingerprint;
    const uint16_t version1Before = p1->version;

    // Drive enough additional applyRecord() calls (on other topics too, to
    // also exercise growth of unrelated map buckets) that a vector-backed
    // implementation would very likely have reallocated by now.
    for (int32_t t = 2; t < 20; ++t) {
        reg.applyRecord(makeTopicSchema(t, 1, evolvingV1()));
    }
    reg.applyRecord(makeTopicSchema(1, 2, evolvingV2()));

    // p1 must still point at the same, unmodified v1 entry.
    EXPECT_EQ(p1->version, version1Before);
    EXPECT_EQ(p1->fingerprint, fp1Before);
    EXPECT_EQ(p1->topicId, 1);

    // And the registry now also reports v2 as latest, independently.
    EXPECT_EQ(reg.latest(1)->version, 2);
}

// ---------------------------------------------------------------------------
// GC3: long-lineage (>=4 versions) mixed-version read -- byIdVersion /
// byFingerprint must resolve EVERY historical version after a lineage that
// includes an ADD, a DROP, and a RENAME, not just the two trivial
// (identical-op) versions KeepsMultipleVersionsAndLooksUpByFingerprint covers.
// ---------------------------------------------------------------------------

static AlterOp alterAdd(const std::string& name, PrimType t) {
    AlterOp op{};
    op.type     = AlterOpType::Add;
    op.name     = name;
    op.primType = t;
    op.fixedLen = 0;
    return op;
}
static AlterOp alterDrop(const std::string& name) {
    AlterOp op{};
    op.type = AlterOpType::Drop;
    op.name = name;
    return op;
}
static AlterOp alterRename(const std::string& oldName, const std::string& newName) {
    AlterOp op{};
    op.type    = AlterOpType::Rename;
    op.name    = oldName;
    op.newName = newName;
    return op;
}

// Mirror AdminWorker::startAlter_'s production ALTER-commit flow exactly:
// decode the latest's evolving descriptor, applyOps() (advancing nextTag),
// serialize the resulting evolving descriptor into a new TopicSchema at
// version+1, and applyRecord() it. This drives the registry through a
// lineage the same way the real ALTER path does, instead of hand-building
// each version's materialized descriptor.
static TopicSchema nextVersionViaApplyOps(const RegistryEntry& latest,
                                          const std::vector<AlterOp>& ops) {
    Descriptor evolving = deserialize(std::span<const uint8_t>(
        latest.descriptorBlob.data(), latest.descriptorBlob.size()));
    uint16_t nextTag = latest.nextTag;
    const uint16_t newVersion = static_cast<uint16_t>(latest.version + 1);
    auto res = applyOps(evolving, ops, nextTag, newVersion);
    if (!res) {
        ADD_FAILURE() << "applyOps failed: " << res.error().message;
    }
    TopicSchema ts;
    ts.topicId       = latest.topicId;
    ts.version        = newVersion;
    ts.keyField       = latest.keyField;
    ts.validate       = latest.validator.has_value();
    ts.descriptorBlob = serialize(*res);
    ts.nextTag        = nextTag;
    ts.compatMode     = latest.compatMode;
    return ts;
}

TEST(SchemaRegistry, LongLineageWithDropAndRenameResolvesEveryVersionByIdAndFingerprintRegression) {
    SchemaRegistry reg;
    const int32_t topicId = 0x40001000;

    // v1: id (key, never retired), age -- hand-built base, exactly like
    // evolvingV1()/makeTopicSchema() above.
    Descriptor v1 = DescriptorBuilder("lineage", /*version=*/1)
        .addTagged("id",  0, PrimType::Int64, 1, 0xFFFF, /*isKey=*/true,  {})
        .addTagged("age", 1, PrimType::Int32, 1, 0xFFFF, /*isKey=*/false, {})
        .build();
    TopicSchema ts1;
    ts1.topicId        = topicId;
    ts1.version         = 1;
    ts1.keyField        = "id";
    ts1.validate         = false;
    ts1.descriptorBlob  = serialize(v1);
    ts1.nextTag          = 2;   // id=0, age=1 already allocated
    ts1.compatMode       = CompatMode::Backward;
    reg.applyRecord(ts1);

    // v2: ADD "score".
    reg.applyRecord(nextVersionViaApplyOps(*reg.latest(topicId), {alterAdd("score", PrimType::Int64)}));
    // v3: DROP "age".
    reg.applyRecord(nextVersionViaApplyOps(*reg.latest(topicId), {alterDrop("age")}));
    // v4: RENAME "score" -> "rating".
    reg.applyRecord(nextVersionViaApplyOps(*reg.latest(topicId), {alterRename("score", "rating")}));
    // v5: ADD "level" -- a 5th version so the lineage is strictly >=4.
    reg.applyRecord(nextVersionViaApplyOps(*reg.latest(topicId), {alterAdd("level", PrimType::Int32)}));

    ASSERT_EQ(reg.latest(topicId)->version, 5);

    // byIdVersion AND byFingerprint must resolve every one of the 5 versions.
    std::vector<uint64_t> fps;
    for (uint16_t v = 1; v <= 5; ++v) {
        const RegistryEntry* byVer = reg.byIdVersion(topicId, v);
        ASSERT_NE(byVer, nullptr) << "byIdVersion missing version " << v;
        EXPECT_EQ(byVer->version, v);
        fps.push_back(byVer->fingerprint);

        const RegistryEntry* byFp = reg.byFingerprint(topicId, byVer->fingerprint);
        ASSERT_NE(byFp, nullptr) << "byFingerprint missing version " << v;
        EXPECT_EQ(byFp->version, v);
        EXPECT_EQ(byFp->fingerprint, byVer->fingerprint);
    }

    // Every version's fingerprint is distinct (ADD/DROP/RENAME each change
    // the live field set or name, so no two versions collide).
    std::vector<uint64_t> sortedFps = fps;
    std::sort(sortedFps.begin(), sortedFps.end());
    EXPECT_EQ(std::adjacent_find(sortedFps.begin(), sortedFps.end()), sortedFps.end())
        << "two distinct versions in the lineage produced the same fingerprint";

    // Sanity on the live field set actually materialized per version.
    EXPECT_EQ(reg.byIdVersion(topicId, 1)->descriptor.fields.size(), 2u);   // id, age
    EXPECT_EQ(reg.byIdVersion(topicId, 2)->descriptor.fields.size(), 3u);   // id, age, score
    EXPECT_EQ(reg.byIdVersion(topicId, 3)->descriptor.fields.size(), 2u);   // id, score (age dropped)
    EXPECT_NE(reg.byIdVersion(topicId, 4)->descriptor.find("rating"), nullptr);
    EXPECT_EQ(reg.byIdVersion(topicId, 4)->descriptor.find("score"), nullptr)
        << "the pre-rename name must no longer resolve in the live set";
    EXPECT_EQ(reg.byIdVersion(topicId, 5)->descriptor.fields.size(), 3u);   // id, rating, level
}

// ---------------------------------------------------------------------------
// REGRESSION (user-topic restart recovery). A DefineTopic topic's NAME is
// independent of its descriptor's name — Client::defineTopic takes both — so the
// broker must persist it in the schema record; boot recovery recreates the topic
// under exactly this name (a wrong name would open the wrong on-disk log).
// ---------------------------------------------------------------------------
TEST(SchemaRegistryRegression, TopicNameTrailerRoundTrips) {
    TopicSchema ts = makeSchema(0x40000010, "descriptor_name", false);
    ts.nextTag    = 5;
    ts.compatMode = CompatMode::Forward;
    ts.topicName  = "orders_topic";

    auto back = TopicSchema::deserialize(ts.serialize());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->topicName, "orders_topic");
    EXPECT_EQ(back->nextTag, 5u);
    EXPECT_EQ(back->compatMode, CompatMode::Forward);
}

// A record written before the name trailer existed (exact 3-byte tail) still
// parses, with an empty topicName — recovery then falls back to the descriptor name.
TEST(SchemaRegistryRegression, PreNameRecordParsesWithEmptyTopicName) {
    TopicSchema ts = makeSchema(0x40000011, "pre_name", false);
    ts.nextTag = 9;
    const auto bytes = ts.serialize();   // no topicName -> byte-identical pre-name layout

    auto back = TopicSchema::deserialize(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_TRUE(back->topicName.empty());
    EXPECT_EQ(back->nextTag, 9u);
}

// A present-but-truncated name trailer is malformed and must be rejected, never
// silently read as a shorter name or as a pre-name record.
TEST(SchemaRegistryRegression, TruncatedTopicNameTrailerIsRejected) {
    TopicSchema ts = makeSchema(0x40000012, "trunc", false);
    ts.topicName = "abcdef";
    const auto bytes = ts.serialize();

    auto minusOne = bytes;              // name bytes short by one
    minusOne.resize(minusOne.size() - 1);
    EXPECT_FALSE(TopicSchema::deserialize(minusOne).has_value());

    auto halfLen = bytes;               // only 1 of the 2 length-prefix bytes present
    halfLen.resize(halfLen.size() - ts.topicName.size() - 1);
    EXPECT_FALSE(TopicSchema::deserialize(halfLen).has_value());

    auto extra = bytes;                 // an unknown byte after the name
    extra.push_back(0x7F);
    EXPECT_FALSE(TopicSchema::deserialize(extra).has_value());
}

TEST(SchemaRegistryRegression, ApplyRecordCarriesTopicNameToEntry) {
    SchemaRegistry reg;
    TopicSchema ts = makeSchema(0x40000013, "desc", false);
    ts.topicName = "the_topic";
    reg.applyRecord(ts);
    ASSERT_NE(reg.byId(0x40000013), nullptr);
    EXPECT_EQ(reg.byId(0x40000013)->topicName, "the_topic");
}
