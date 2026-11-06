// CheckedWritesStatsTest.cpp - checked writes Part 08 Task 7 (QM4): GetStats on a real Worker.  Every counter that exists on the
// partition is served in ascending id order, each equal to its keeper's seam; the rest are absent.  Only a leader under its
// lease answers; a demoted partition answers the internal not-leader sentinel, never its old counters.
#include "support/CheckedWritesRig.h"
#include "node/CheckedWritesKeyedViewFixture.h"

#include <gtest/gtest.h>

#include <array>

using namespace rune::node;
using namespace rune::node::cw;
namespace pc = rune::protocol::client;

namespace {
using Entries = std::vector<std::pair<uint16_t, int64_t>>;
constexpr uint8_t kAllView = pc::kAcksWireView | pc::kAcksWireAll;
constexpr int32_t kProj = 0x40000602;
constexpr int32_t kStrangerTopic = 99, kStrangerView = 0x40000777;
constexpr uint16_t kUnknownObject = pc::StatsResponse::kUnknownObject;

Rec ins(uint64_t id) { return Rec{id, 1, 0, false, 1}; }
Rec upd(uint64_t id) { return Rec{id, 1, 0, false, 2}; }
Rec create() { return Rec{0, 1, 0, false, 1}; }

constexpr int64_t kPid = 42;

/// What the keepers' seams say a led topic partition serves.
Entries expectedTopic(VerdictRig& r, std::vector<std::pair<uint32_t, uint64_t>>* constraints = nullptr) {
    Entries e;
    const PartitionWriteState* w = r.writeState();
    if (w == nullptr) return e;
    if (w->verdicts != nullptr) {
        const VerdictSourceStats* st = r.stats();
        e = {{1, static_cast<int64_t>(st->rejectionsKeyExists)}, {2, static_cast<int64_t>(st->rejectionsKeyNotFound)},
             {3, static_cast<int64_t>(st->rejectionsConstraint)}, {4, r.replica->logEndOffset() - w->verdicts->judgedThrough()},
             {5, w->rejected.watermark()}, {6, static_cast<int64_t>(st->markersWritten)},
             {7, static_cast<int64_t>(w->rejected.countAtOrAfter(r.replica->lastCleanOffset()))}};
        if (constraints != nullptr)
            for (uint16_t i = 0; i < st->perConstraintCount; ++i) constraints->emplace_back(st->perConstraint[i].constraintId, st->perConstraint[i].rejections);
    }
    if (w->spec != nullptr && w->spec->autoIncrement) {
        if (const auto s = r.autoIncState()) {
            const int64_t parts = w->spec->partitionCount, p = r.partition();
            e.emplace_back(8, static_cast<int64_t>(s->nextN) * parts + p);
            e.emplace_back(9, s->reservedThroughN == 0 ? 0 : static_cast<int64_t>(s->reservedThroughN) * parts + p);
        }
        e.emplace_back(10, static_cast<int64_t>(r.autoIncStats().idsBurned));
    }
    return e;
}

Entries expectedView(VerdictRig& r, int32_t view) {
    const ViewFoldCounters* c = r.vs->foldCounters(view, r.partition());
    if (c == nullptr) return {};
    return {{33, static_cast<int64_t>(c->scanRows)}, {34, static_cast<int64_t>(c->scanPages)},
            {35, static_cast<int64_t>(c->transientRecordsEvicted)}, {36, static_cast<int64_t>(c->transientSourceResets)}};
}

struct StatsObject {
    enum Kind { KeyIndex, AutoInc, PlainTyped, Untyped, NotOwned, Projection, UnknownView };
};

struct StatsCellParam {
    StatsObject::Kind object;
    bool follower;
};

StatsCellParam cellOf(int n) { return {static_cast<StatsObject::Kind>(n / 2), n % 2 == 1}; }
constexpr int kStatsCells = 14;
}  // namespace

class StatsCell : public ::testing::TestWithParam<int> {};

TEST_P(StatsCell, ServesExactlyTheCountersThatExist) {
    const StatsCellParam cell = cellOf(GetParam());
    VerdictRig r;
    int32_t objectId = VerdictRig::kSrc;
    auto scope = pc::StatsScope::TopicPartition;
    switch (cell.object) {
    case StatsObject::KeyIndex:
        r.createKeyIndex();
        r.enableAcks();
        r.passes(60);
        ASSERT_TRUE(r.ack(r.produceFrame({ins(5)}, kAllView, kPid, 0)));
        ASSERT_TRUE(r.ack(r.produceFrame({ins(5), upd(9)}, kAllView, kPid, 1)));
        ASSERT_TRUE(r.driveUntilJudged());
        break;
    case StatsObject::AutoInc:
        r.createAutoIncTopic({.partitions = 1});
        r.enableAcks();
        r.passes(20);
        ASSERT_TRUE(r.ack(r.produceFrame({create(), create()}, kAllView, kPid, 0)));
        ASSERT_TRUE(r.driveUntilJudged());
        break;
    case StatsObject::PlainTyped:
        r.typed();
        break;
    case StatsObject::Untyped:
        break;
    case StatsObject::NotOwned:
        r.typed();
        objectId = kStrangerTopic;
        break;
    case StatsObject::Projection:
        r.typed();
        r.installProjection(kProj);
        ASSERT_EQ(r.produce({Rec{1, 10}, Rec{2, 20}}), 0);
        r.driveUntilFolded(kProj, r.leo());
        scope = pc::StatsScope::ViewPartition;
        objectId = kProj;
        break;
    case StatsObject::UnknownView:
        r.typed();
        scope = pc::StatsScope::ViewPartition;
        objectId = kStrangerView;
        break;
    }
    if (cell.follower) r.demote();

    const VerdictRig::StatsReply reply = r.getStats(scope, objectId, r.partition());
    if (cell.object == StatsObject::UnknownView) {
        EXPECT_FALSE(reply.notLeader) << "an unknown view is answered before the role is looked at";
        EXPECT_EQ(reply.status, kUnknownObject);
        EXPECT_TRUE(reply.stats.empty());
        return;
    }
    if (cell.follower || cell.object == StatsObject::NotOwned) {
        EXPECT_TRUE(reply.notLeader);
        EXPECT_TRUE(reply.stats.empty());
        EXPECT_TRUE(reply.constraints.empty());
        return;
    }
    EXPECT_FALSE(reply.notLeader);
    EXPECT_EQ(reply.status, pc::StatsResponse::kOk);
    std::vector<std::pair<uint32_t, uint64_t>> wantConstraints;
    const Entries want = scope == pc::StatsScope::ViewPartition ? expectedView(r, objectId) : expectedTopic(r, &wantConstraints);
    EXPECT_EQ(reply.stats, want);
    EXPECT_EQ(reply.constraints, wantConstraints);
    for (std::size_t i = 1; i < reply.stats.size(); ++i) EXPECT_LT(reply.stats[i - 1].first, reply.stats[i].first) << "ids ascend";
    switch (cell.object) {
    case StatsObject::KeyIndex:   EXPECT_EQ(reply.stats.size(), 7u); break;
    case StatsObject::AutoInc:    EXPECT_EQ(reply.stats.size(), 10u); break;
    case StatsObject::Projection: EXPECT_EQ(reply.stats.size(), 4u); break;
    default:                      EXPECT_TRUE(reply.stats.empty()) << "a plain or untyped topic keeps no counter"; break;
    }
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesStats, StatsCell, ::testing::Range(0, kStatsCells));

TEST(CheckedWritesStatsCount, CellCountIsPinned) {
    EXPECT_EQ(kStatsCells, 14);
    EXPECT_EQ(cellOf(kStatsCells - 1).object, StatsObject::UnknownView);
}

TEST(CheckedWritesStats, KeyIndexCountersAfterRejections) {
    VerdictRig r;
    r.createKeyIndex();
    r.enableAcks();
    r.passes(60);
    ASSERT_TRUE(r.ack(r.produceFrame({ins(5)}, kAllView, kPid, 0)));
    ASSERT_TRUE(r.ack(r.produceFrame({ins(5)}, kAllView, kPid, 1)));
    ASSERT_TRUE(r.ack(r.produceFrame({upd(9)}, kAllView, kPid, 2)));
    ASSERT_TRUE(r.driveUntilJudged());

    const auto reply = r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    ASSERT_EQ(reply.stats.size(), 7u);
    const VerdictSourceStats* st = r.worker->verdictStatsForTest(VerdictRig::kSrc, 0);
    ASSERT_NE(st, nullptr);
    EXPECT_EQ(reply.stats[0], (std::pair<uint16_t, int64_t>{1, 1}));
    EXPECT_EQ(reply.stats[1], (std::pair<uint16_t, int64_t>{2, 1}));
    EXPECT_EQ(reply.stats[2], (std::pair<uint16_t, int64_t>{3, 0}));
    EXPECT_EQ(reply.stats[3], (std::pair<uint16_t, int64_t>{4, 0}));
    EXPECT_EQ(reply.stats[4], (std::pair<uint16_t, int64_t>{5, r.writeState()->rejected.watermark()}));
    EXPECT_EQ(reply.stats[5], (std::pair<uint16_t, int64_t>{6, static_cast<int64_t>(st->markersWritten)}));
    EXPECT_EQ(reply.stats[6], (std::pair<uint16_t, int64_t>{7, 2}));
    EXPECT_GT(reply.stats[4].second, 0);
    EXPECT_GT(reply.stats[5].second, 0);
}

TEST(CheckedWritesStats, AutoIncrementCountersAfterCreates) {
    VerdictRig r;
    r.createAutoIncTopic({.partitions = 1});
    r.enableAcks();
    r.passes(20);
    for (int32_t seq = 0; seq < 3; ++seq) ASSERT_TRUE(r.ack(r.produceFrame({create()}, kAllView, kPid, seq)));

    auto idOf = [](const VerdictRig::StatsReply& reply, uint16_t id) {
        for (const auto& s : reply.stats) if (s.first == id) return std::optional<int64_t>(s.second);
        return std::optional<int64_t>();
    };
    auto reply = r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    const auto state = r.autoIncState();
    ASSERT_TRUE(state);
    EXPECT_EQ(idOf(reply, 8), 4);
    EXPECT_EQ(idOf(reply, 9), 4096);
    EXPECT_EQ(idOf(reply, 10), 0);
    EXPECT_EQ(idOf(reply, 8), static_cast<int64_t>(state->nextN) * 1 + 0);
    EXPECT_EQ(idOf(reply, 9), static_cast<int64_t>(state->reservedThroughN) * 1 + 0);

    r.failover();
    for (int i = 0; i < VerdictRig::kMaxPass && !r.autoIncState(); ++i) r.pass();
    ASSERT_TRUE(r.autoIncState()) << "the leadership scan never made the counter Ready";
    reply = r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    EXPECT_EQ(idOf(reply, 8), 4097);
    EXPECT_EQ(idOf(reply, 10), 4093) << "the reservation's unused ids are burned";
}

TEST(CheckedWritesStats, ViewCounters) {
    VerdictRig r;
    const rune::codec::Descriptor d = cwkeyed::characterSource();
    r.useSource(d, KeyShape{});
    r.passes(50);
    rune::view::ViewPlan plan;
    plan.viewName = "account_characters";
    plan.persistence = 1;
    plan.sources = {{VerdictRig::kSrc, "characterId"}};
    plan.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};
    plan.output = {{"gender", rune::codec::PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    const rune::codec::Descriptor* srcs[] = {&d};
    auto comp = rune::view::compileView(plan, srcs, cwkeyed::kViewTopic);
    ASSERT_TRUE(comp.has_value());
    r.installKeyedView(*comp);

    std::vector<RawRec> rows;
    for (uint64_t i = 1; i <= 12; ++i) {
        const std::string value = cwkeyed::valueOf(d, cwkeyed::Rec{i, 42, "n", static_cast<uint16_t>(100 + i)});
        rows.push_back({cwkeyed::wireKey(i), std::vector<uint8_t>(value.begin(), value.end())});
    }
    ASSERT_EQ(r.produceRaw(rows), 0);
    r.driveUntilFolded(cwkeyed::kViewTopic, r.leo());
    for (int scan = 0; scan < 2; ++scan) EXPECT_EQ(r.scanAll(cwkeyed::kViewTopic, cwkeyed::beKey({42}), 5).size(), 12u);

    const auto reply = r.getStats(pc::StatsScope::ViewPartition, cwkeyed::kViewTopic, 0);
    ASSERT_EQ(reply.stats.size(), 4u);
    const ViewFoldCounters* c = r.vs->foldCounters(cwkeyed::kViewTopic, 0);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(reply.stats[0], (std::pair<uint16_t, int64_t>{33, 24}));
    EXPECT_EQ(reply.stats[1], (std::pair<uint16_t, int64_t>{34, 6}));
    EXPECT_EQ(reply.stats[0].second, static_cast<int64_t>(c->scanRows));
    EXPECT_EQ(reply.stats[1].second, static_cast<int64_t>(c->scanPages));
    EXPECT_EQ(reply.stats[2], (std::pair<uint16_t, int64_t>{35, 0}));
    EXPECT_EQ(reply.stats[3], (std::pair<uint16_t, int64_t>{36, 0}));
}

TEST(CheckedWritesStatsRegression, FollowerNeverAnswersStaleCountersRegression) {
    VerdictRig r;
    r.createKeyIndex();
    r.enableAcks();
    r.passes(60);
    ASSERT_TRUE(r.ack(r.produceFrame({ins(5)}, kAllView, kPid, 0)));
    ASSERT_TRUE(r.ack(r.produceFrame({ins(5)}, kAllView, kPid, 1)));
    ASSERT_TRUE(r.driveUntilJudged());
    const auto before = r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    ASSERT_FALSE(before.notLeader);
    ASSERT_EQ(before.stats.size(), 7u);

    r.demote();
    const auto after = r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0);
    EXPECT_TRUE(after.notLeader) << "a demoted partition's counters are frozen: it answers the sentinel";
    EXPECT_TRUE(after.stats.empty());
    EXPECT_TRUE(after.constraints.empty());
}

namespace {
/// Installs account_characters over the rig's source (typed by characterSource()) without folding anything.
void installAccountView(VerdictRig& r, const rune::codec::Descriptor& d) {
    rune::view::ViewPlan plan;
    plan.viewName = "account_characters";
    plan.persistence = 1;
    plan.sources = {{VerdictRig::kSrc, "characterId"}};
    plan.keyComponents = {{"userId", 0, "userId"}, {"characterId", 0, "characterId"}};
    plan.output = {{"gender", rune::codec::PrimType::UInt16, 0, "gender", rune::view::MergeRule::Replace}};
    const rune::codec::Descriptor* srcs[] = {&d};
    auto comp = rune::view::compileView(plan, srcs, cwkeyed::kViewTopic);
    ASSERT_TRUE(comp.has_value());
    r.installKeyedView(*comp);
}
}  // namespace

TEST(CheckedWritesStatsRegression, ViewNotYetHostedAnswersTheSentinelRegression) {
    VerdictRig r;
    const rune::codec::Descriptor d = cwkeyed::characterSource();
    r.useSource(d, KeyShape{});
    r.passes(50);
    installAccountView(r, d);
    if (r.vs->instanceServeState(cwkeyed::kViewTopic, 0).hosted) GTEST_SKIP() << "the instance is hosted by the install";
    const auto reply = r.getStats(pc::StatsScope::ViewPartition, cwkeyed::kViewTopic, 0);
    EXPECT_TRUE(reply.notLeader) << "an unmaterialized instance has no counters to serve";
    EXPECT_TRUE(reply.stats.empty());
}

TEST(CheckedWritesStatsRegression, ExpiredReadLeaseAnswersTheSentinelRegression) {
    VerdictRig r;
    const rune::codec::Descriptor d = cwkeyed::characterSource();
    r.useSource(d, KeyShape{});
    r.passes(50);
    installAccountView(r, d);
    r.driveUntilFolded(cwkeyed::kViewTopic, r.leo());
    ASSERT_FALSE(r.getStats(pc::StatsScope::TopicPartition, VerdictRig::kSrc, 0).notLeader);
    ASSERT_FALSE(r.getStats(pc::StatsScope::ViewPartition, cwkeyed::kViewTopic, 0).notLeader);
    r.worker->requireViewReadLease();
    for (const auto scope : {pc::StatsScope::TopicPartition, pc::StatsScope::ViewPartition}) {
        const auto reply = r.getStats(scope, scope == pc::StatsScope::TopicPartition ? VerdictRig::kSrc : cwkeyed::kViewTopic, 0);
        EXPECT_TRUE(reply.notLeader) << "no lease, no counters, scope " << static_cast<int>(scope);
        EXPECT_TRUE(reply.stats.empty());
    }
}
