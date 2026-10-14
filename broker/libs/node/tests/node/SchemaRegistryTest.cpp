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

TEST(SchemaRegistryRegression, ApplyRecordCarriesTopicNameToEntry) {
    SchemaRegistry reg;
    TopicSchema ts = makeSchema(0x40000013, "desc", false);
    ts.topicName = "the_topic";
    reg.applyRecord(ts);
    ASSERT_NE(reg.byId(0x40000013), nullptr);
    EXPECT_EQ(reg.byId(0x40000013)->topicName, "the_topic");
}

// ---------------------------------------------------------------------------
// REGRESSION (Phase 2b C real-broker e2e): byName() is looked up with the TOPIC
// name (GetTopicSchema, DefineTopic's TopicExists check, AlterTopic), but the
// registry indexed the DESCRIPTOR name.  A topic defined as "typed_route" with
// the "characters" descriptor answered GetTopicSchema("typed_route") with
// UnknownTopic, and two topics sharing one descriptor collided on one name.
// The index key is the durable topic name; a legacy record without one falls
// back to the descriptor name (which is what the client named it pre-trailer).
// ---------------------------------------------------------------------------
TEST(SchemaRegistryRegression, ByNameResolvesTheTopicNameNotTheDescriptorName) {
    SchemaRegistry reg;
    TopicSchema ts = makeSchema(0x40000020, "characters", false);
    ts.topicName = "typed_route";
    reg.applyRecord(ts);
    const RegistryEntry* e = reg.byName("typed_route");
    ASSERT_NE(e, nullptr) << "the topic must resolve by the name it was defined under";
    EXPECT_EQ(e->topicId, 0x40000020);
    EXPECT_EQ(reg.byName("characters"), nullptr)
        << "the descriptor name is not a topic name";
}

TEST(SchemaRegistryRegression, TwoTopicsSharingADescriptorStayDistinct) {
    SchemaRegistry reg;
    TopicSchema a = makeSchema(0x40000021, "characters", false);
    a.topicName = "chars_a";
    TopicSchema b = makeSchema(0x40000022, "characters", false);
    b.topicName = "chars_b";
    reg.applyRecord(a);
    reg.applyRecord(b);
    ASSERT_NE(reg.byName("chars_a"), nullptr);
    ASSERT_NE(reg.byName("chars_b"), nullptr);
    EXPECT_EQ(reg.byName("chars_a")->topicId, 0x40000021);
    EXPECT_EQ(reg.byName("chars_b")->topicId, 0x40000022);
}

TEST(SchemaRegistryRegression, PreNameRecordResolvesByDescriptorName) {
    SchemaRegistry reg;
    reg.applyRecord(makeSchema(0x40000023, "legacy_chars", false));   // empty topicName
    ASSERT_NE(reg.byName("legacy_chars"), nullptr);
    EXPECT_EQ(reg.byName("legacy_chars")->topicId, 0x40000023);
}

TEST(SchemaRegistryRegression, ApplyRecordCarriesPersistenceToEntry) {
    SchemaRegistry reg;
    TopicSchema ts = makeSchema(0x40000025, "desc", false);
    ts.persistence = PersistenceMode::Transient;
    reg.applyRecord(ts);
    ASSERT_NE(reg.byId(0x40000025), nullptr);
    EXPECT_EQ(reg.byId(0x40000025)->persistence, PersistenceMode::Transient);
}
