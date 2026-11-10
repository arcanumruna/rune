// tests/client/CheckedWritesConstraintNameTest.cpp - checked writes Part 09: resolving a constraintId to its view and constraint
// from a replayed catalog, for live, replaced and dropped views, and for each entry of a RecordsRejected completion.
#include <rune/catalog/CatalogReplayer.h>
#include <rune/client/Completion.h>
#include <rune/client/ConstraintNames.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/ViewPlanAuthored.h>

#include <gtest/gtest.h>

#include <vector>

namespace rc  = rune::client;
namespace cat = rune::catalog;
using rune::codec::PrimType;

namespace {

constexpr uint64_t kTopicId = 1, kCharacterName = 2, kAccountStats = 3;   // object ids

rune::codec::Descriptor sourceDescriptor() {
    rune::codec::DescriptorBuilder b("character");
    b.add("characterId", PrimType::UInt64, 1, true);
    b.add("userId", PrimType::UInt64);
    return b.build();
}

rune::view::AuthoredConstraint constraint(const char* name) { return {name, {}}; }

std::vector<uint8_t> planBlob(const char* view, bool unique, std::vector<const char*> constraints) {
    rune::view::ViewPlan plan;
    plan.viewName = view;
    plan.key      = "userId";
    plan.sources.push_back({-1, "userId"});
    plan.output.push_back({"userId", PrimType::UInt64, 0, "userId", rune::view::MergeRule::Replace});
    plan.unique = unique;
    for (const char* c : constraints) plan.constraints.push_back(constraint(c));
    return rune::view::serializeAuthored(plan);
}

struct Log {
    cat::CatalogReplayer replayer;
    std::vector<cat::RecordedEvent> events;
    int32_t nextTopicId = 1;
    uint64_t nextObjectId = 2;

    void append(cat::Ops ops, const std::string& name, uint64_t objectId, uint32_t prev, uint32_t next) {
        cat::CatalogEvent e{.formatVer = cat::kFormatV1};
        e.kind = cat::kindOf(ops); e.ops = std::move(ops); e.objectName = name; e.objectId = objectId; e.prevVersion = prev; e.newVersion = next;
        const auto fp = cat::previewFingerprint(replayer.state(), e);
        ASSERT_TRUE(fp.has_value()) << fp.error().detail;
        e.fingerprint = *fp;
        const int64_t offset = replayer.nextOffset();
        const auto applied = replayer.feedDecoded(offset, e);
        ASSERT_TRUE(applied.has_value()) << cat::describe(applied.error());
        events.push_back({offset, e});
    }
    void createSource() {
        cat::CreateTopicOps t;
        t.topicId = 0; t.partitions = 1; t.replicationFactor = 1;
        t.schema = cat::SchemaDef{.keyField = "characterId", .validate = true, .descriptorBlob = rune::codec::serialize(sourceDescriptor()), .nextTag = 2};
        append(t, "character", kTopicId, 0, 1);
    }
    static cat::ViewDef viewDef(int32_t viewTopicId, std::vector<uint8_t> blob) {
        cat::ViewDef d;
        d.viewTopicId = viewTopicId; d.sinkPartitions = 1; d.keyField = "userId"; d.sourceNames = {"character"};
        d.normalizedPlanBlob = std::move(blob);
        d.outputDescriptorBlob = rune::codec::serialize(sourceDescriptor());
        return d;
    }
    // Returns the view's object id.
    uint64_t createView(const char* name, std::vector<uint8_t> blob) {
        const uint64_t id = nextObjectId++;
        append(cat::CreateViewOps{viewDef(nextTopicId++, std::move(blob))}, name, id, 0, 1);
        return id;
    }
    void alterView(const char* name, uint64_t id, uint32_t version, std::vector<uint8_t> blob) {
        append(cat::AlterViewOps{viewDef(nextTopicId++, std::move(blob))}, name, id, version, version + 1);
    }
    void dropView(const char* name, uint64_t id, int32_t viewTopicId, uint32_t version) {
        append(cat::DropViewOps{viewTopicId}, name, id, version, version + 1);
    }
    std::optional<rc::ConstraintName> name(uint32_t id) const { return rc::resolveConstraintName(replayer.state(), events, id); }
};

uint32_t idOf(uint64_t objectId, uint16_t ordinal) { return static_cast<uint32_t>(objectId << 16) | ordinal; }

Log twoViews() {
    Log log;
    log.createSource();
    EXPECT_EQ(log.createView("character_name", planBlob("character_name", true, {})), kCharacterName);
    EXPECT_EQ(log.createView("account_stats", planBlob("account_stats", false, {"max_characters", "max_level"})), kAccountStats);
    return log;
}

}  // namespace

TEST(CheckedWritesConstraintName, ResolvesUniqueAndCheckOrdinals) {
    const Log log = twoViews();
    EXPECT_EQ(log.name(idOf(kCharacterName, 0)), (rc::ConstraintName{"character_name", "unique"}));
    EXPECT_EQ(log.name(idOf(kAccountStats, 1)), (rc::ConstraintName{"account_stats", "max_characters"}));
    EXPECT_EQ(log.name(idOf(kAccountStats, 2)), (rc::ConstraintName{"account_stats", "max_level"}));
}

TEST(CheckedWritesConstraintName, UnknownIdsAreNone) {
    const Log log = twoViews();
    EXPECT_EQ(log.name(0), std::nullopt) << "the key index";
    EXPECT_EQ(log.name(idOf(kTopicId, 0)), std::nullopt) << "a topic";
    EXPECT_EQ(log.name(idOf(kAccountStats, 3)), std::nullopt);
    EXPECT_EQ(log.name(idOf(kAccountStats, 0)), std::nullopt) << "ordinal 0 of a non-unique view";
}

TEST(CheckedWritesConstraintNameRegression, ReplayedIdResolvesAfterReplaceAndDropRegression) {
    Log log = twoViews();
    log.alterView("account_stats", kAccountStats, 1, planBlob("account_stats", false, {"max_level"}));
    log.dropView("character_name", kCharacterName, 1, 1);
    EXPECT_EQ(log.name(idOf(kAccountStats, 1)), (rc::ConstraintName{"account_stats", "max_level"})) << "the live definition";
    EXPECT_EQ(log.name(idOf(kAccountStats, 2)), (rc::ConstraintName{"account_stats", "max_level"})) << "the earlier definition";
    EXPECT_EQ(log.name(idOf(kCharacterName, 0)), (rc::ConstraintName{"character_name", "unique"})) << "the dropped view";
}

TEST(CheckedWritesConstraintNameRegression, DroppedThenRecreatedNameResolvesTheOldIdToTheOldViewRegression) {
    Log log = twoViews();
    log.dropView("account_stats", kAccountStats, 2, 1);
    const uint64_t again = log.createView("account_stats", planBlob("account_stats", true, {"max_level"}));
    ASSERT_NE(again, kAccountStats);
    EXPECT_EQ(log.name(idOf(kAccountStats, 1)), (rc::ConstraintName{"account_stats", "max_characters"})) << "the old view's constraint";
    EXPECT_EQ(log.name(idOf(kAccountStats, 0)), std::nullopt) << "the old view was not unique";
    EXPECT_EQ(log.name(idOf(again, 1)), (rc::ConstraintName{"account_stats", "max_level"}));
    EXPECT_EQ(log.name(idOf(again, 0)), (rc::ConstraintName{"account_stats", "unique"}));
}

// A RecordsRejected batch: every entry of the trailer resolves, whatever its position.
TEST(CheckedWritesConstraintName, EveryEntryOfARecordsRejectedCompletionResolves) {
    const Log log = twoViews();
    const struct { uint16_t index; uint32_t constraintId; } entries[] = {
        {0, idOf(kCharacterName, 0)}, {4, idOf(kAccountStats, 2)}, {9, idOf(kAccountStats, 1)}};
    std::vector<uint8_t> payload;
    for (const auto& e : entries) {
        const uint16_t code = 55;
        payload.insert(payload.end(), {uint8_t(e.index), uint8_t(e.index >> 8), uint8_t(code), uint8_t(code >> 8)});
        for (int k = 0; k < 4; ++k) payload.push_back(uint8_t(e.constraintId >> (8 * k)));
    }
    rc::Completion c;
    c.status = rc::OpStatus::RecordsRejected;
    c.value  = 500;
    const rc::BatchVerdicts verdicts(c, payload);
    ASSERT_EQ(verdicts.rejectedCount(), 3u);
    const rc::ConstraintName expected[] = {{"character_name", "unique"}, {"account_stats", "max_level"}, {"account_stats", "max_characters"}};
    for (uint32_t k = 0; k < 3; ++k) {
        const auto v = verdicts.rejection(k);
        EXPECT_FALSE(verdicts.accepted(v.index));
        EXPECT_EQ(verdicts.offsetOf(v.index), -1) << "a rejected record has no accepted offset";
        EXPECT_EQ(c.value + v.index, int64_t{500} + entries[k].index) << "the rejected record's own offset";
        EXPECT_EQ(verdicts.offsetOf(v.index + 1u), int64_t{501} + entries[k].index) << "the next record was accepted";
        EXPECT_EQ(log.name(v.constraintId), expected[k]) << k;
    }
}
