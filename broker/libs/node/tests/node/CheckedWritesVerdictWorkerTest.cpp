// Checked writes Part 04 Task 6: the verdict pipeline on a real Worker.
#include "CheckedWritesRig.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <random>
#include <variant>

using namespace rune::node;
using namespace rune::node::cw;

namespace cr = rune::storage::control_record;

namespace {
constexpr int64_t kMs = 1'000'000;
constexpr int32_t kRej = 0x40000601, kProj = 0x40000602;
constexpr uint64_t kObj = 77;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
constexpr int kPassCap = 300;

std::vector<uint8_t> slurp(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}
std::string committedFile(const VerdictRig& r, int32_t view) { return viewCheckpointFilePath(r.snapDir(), view, 0); }
}  // namespace

TEST(CheckedWritesVerdictWorker, RejectedRecordNeverReachesAnyView) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);   // the set change is logged before the records it is to judge
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}, {3, 30, 0}}), 0);
    r.passes(200);
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2));
    EXPECT_TRUE(r.hasRow(kProj, 3));
    EXPECT_FALSE(r.hasRow(kRej, 2));
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->rejectionsConstraint, 1u);
}

TEST(CheckedWritesVerdictWorker, MarkerIsWrittenAndCommitted) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    const auto types = r.controlTypes();
    EXPECT_EQ(std::count(types.begin(), types.end(), cr::kVerdictMarker), 1);
    EXPECT_EQ(r.stats()->markersWritten, 1u);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_EQ(r.replica->cleanerVerdictBound(), r.leo() - 1);   // W: the marker's finalThrough, the marker itself above it
    EXPECT_GE(judge->judgedThrough(), r.leo() - 1);
    EXPECT_EQ(r.replica->cleanerVerdictMarkerEnd(), r.leo());
}

TEST(CheckedWritesVerdictWorker, NothingRejectedNoMarkerBeforeTheInterval) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 0}}), 0);
    r.passes(20);   // ~20 ms of the 100 ms default
    auto count = [&] { const auto t = r.controlTypes(); return std::count(t.begin(), t.end(), cr::kVerdictMarker); };
    EXPECT_EQ(count(), 0);
    r.passes(200);
    EXPECT_EQ(count(), 1);
}

TEST(CheckedWritesVerdictWorker, IntervalKnobIsHonoured) {
    VerdictRig r;
    r.worker->setVerdictIntervalNs(20 * kMs);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}}), 0);
    r.passes(60);
    const auto t = r.controlTypes();
    EXPECT_EQ(std::count(t.begin(), t.end(), cr::kVerdictMarker), 1);
}

TEST(CheckedWritesVerdictWorkerRegression, NewLeaderRejudgesAboveWAndRemarksRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_EQ(r.produce({{3, 30, 9}, {4, 40, 0}}), 0);
    r.passes(300);
    EXPECT_FALSE(r.hasRow(kProj, 3));
    const int64_t leo = r.leo();
    r.restart();
    r.passes(500);
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2));
    EXPECT_FALSE(r.hasRow(kProj, 3));
    EXPECT_TRUE(r.hasRow(kProj, 4));
    EXPECT_FALSE(r.hasRow(kRej, 3));
    EXPECT_EQ(r.leo(), leo) << "the verdicts are already marked: nothing is re-marked";
}

TEST(CheckedWritesVerdictWorkerRegression, LeaderWithoutTheInstallWaitsRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    r.restart(/*reinstall=*/false);
    r.installProjection(kProj, "proj2");
    ASSERT_EQ(r.produce({{3, 30, 9}, {4, 40, 0}}), 0);
    r.passes(300);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_TRUE(judge->waitingForInstall());
    const int64_t held = judge->judgedThrough();
    EXPECT_TRUE(r.hasRow(kProj, 1)) << "below W the markers decide, without the install";
    EXPECT_FALSE(r.hasRow(kProj, 2));
    EXPECT_FALSE(r.hasRow(kProj, 4)) << "nothing above the judge is folded while the install is missing";
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(500);
    judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    EXPECT_FALSE(judge->waitingForInstall());
    EXPECT_GT(judge->judgedThrough(), held);
    EXPECT_TRUE(r.hasRow(kProj, 4));
    EXPECT_FALSE(r.hasRow(kProj, 3));
}

TEST(CheckedWritesVerdictWorkerRegression, FullIndexGrowsAndResumesRegression) {
    VerdictRig r;
    r.typed();
    r.passes(20);
    const_cast<PartitionWriteState*>(r.writeState())->rejected.reserve(8);
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    std::vector<Rec> recs;
    for (uint64_t i = 1; i <= 14; ++i) recs.push_back({i, int64_t(i), 9});
    ASSERT_EQ(r.produce(recs), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_GE(r.vs->verdictSource(VerdictRig::kSrc, 0)->judgedThrough(), r.dataEnd());
    EXPECT_EQ(r.stats()->verdictStalls, 1u) << "the index filled once, grew once and the judge resumed";
    EXPECT_EQ(r.writeState()->rejected.size(), 14u);
    EXPECT_GT(r.writeState()->rejected.capacity(), 8u);
}

TEST(CheckedWritesVerdictWorkerRegression, RetentionNeverDeletesTheLatestSetChangeRegression) {
    VerdictRig r(false, /*rollBytes=*/1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    for (uint64_t i = 1; i <= 6; ++i) ASSERT_EQ(r.produce({{i, int64_t(i), 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    const int64_t firstChange = r.readLog().changes.front().offset;
    // The cleaner proposes every sealed segment each cycle; the owner takes what the hold lets go.
    for (int i = 0; i < 300; ++i) {
        if (const auto snap = r.log->cleanerSnapshot()) {
            for (const auto& si : *snap) (void)r.log->enqueueDeletedSegment(si.baseOffset, si.lineage);
        }
        r.pass();
    }
    EXPECT_GT(r.log->logStartOffset(), firstChange) << "the first change record's segment went once a newer change was committed";
    EXPECT_FALSE(r.readLog().changes.empty());
    r.restart();
    r.passes(100);
    ASSERT_FALSE(r.writeState()->constraintSets.latest().empty()) << "after the restart the topic is still a verdict source";
    ASSERT_EQ(r.produce({{7, 7, 9}, {8, 8, 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.stats()->rejectionsConstraint, 1u);
    const int64_t sub = r.subscribe(SubKind::Raw, r.log->logStartOffset());
    bool sawAccepted = false;
    for (const auto& d : r.collect(sub).records) {
        EXPECT_NE(d.id, 7u) << "a rejected record was delivered";
        sawAccepted = sawAccepted || d.id == 8;
    }
    EXPECT_TRUE(sawAccepted);
}

TEST(CheckedWritesVerdictWorkerRegression, RetentionKeepsTheChangeGoverningWRegression) {
    // The judge is held back (the install is missing), so no marker covers the records after the first change.  The leader logs
    // the set again at the log end; once that commits, retention must still keep the first change: it governs the records
    // between W and the second one, which the next leader has to judge.
    VerdictRig r(false, /*rollBytes=*/1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    ASSERT_EQ(r.readLog().changes.front().offset, 0);
    r.restart(/*reinstall=*/false);
    r.installProjection(kProj, "proj2");
    ASSERT_EQ(r.produce({{1, 10, 9}}), 0);
    ASSERT_EQ(r.produce({{2, 20, 0}}), 0);
    for (int i = 0; i < 300; ++i) {
        if (const auto snap = r.log->cleanerSnapshot()) {
            for (const auto& si : *snap) (void)r.log->enqueueDeletedSegment(si.baseOffset, si.lineage);
        }
        r.pass();
    }
    ASSERT_FALSE(r.readLog().changes.empty());
    ASSERT_GE(r.readLog().changes.back().offset, 2) << "the set was logged again";
    ASSERT_TRUE(r.vs->verdictSource(VerdictRig::kSrc, 0)->waitingForInstall());
    ASSERT_EQ(r.writeState()->markers.committedThrough(), 0);
    EXPECT_EQ(r.log->logStartOffset(), 0) << "the change governing W went with its segment";
    r.restart(/*reinstall=*/false);
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj, "proj3");
    r.passes(500);
    EXPECT_FALSE(r.hasRow(kProj, 1)) << "the record W did not cover is judged by the next leader";
    EXPECT_TRUE(r.hasRow(kProj, 2));
    const int64_t sub = r.subscribe(SubKind::Raw, r.log->logStartOffset());
    for (const auto& d : r.collect(sub).records) EXPECT_NE(d.id, 1u) << "a rejected record was delivered";
}

TEST(CheckedWritesVerdictWorkerRegression, RetentionRefreshWaitsForTheLeadershipScanRegression) {
    // The first change sits in the oldest segment.  While this node follows, the other leader logs a second change deep enough
    // in the log that the leadership scan reaches it only after several passes.  A refresh of the half-scanned timeline on the
    // promotion would log the first set again and drop the second member.
    constexpr int32_t kRej2 = 0x40000603;
    VerdictRig r(false, /*rollBytes=*/16 * 1024);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.readLog().changes.front().offset, 0);
    ASSERT_EQ(r.produce({{1, 1, 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    r.demote();
    r.installRejectView(kRej2, kObj + 1, tagRule(8), "rej2");
    const auto data = [&r](uint64_t from) {   // one fetch per batch: the follower's log rolls its segments
        for (uint64_t b = 0; b < 40; ++b) {
            std::vector<Rec> recs;
            for (uint64_t i = 0; i < 200; ++i) recs.push_back({from + b * 200 + i, int64_t(i), 0});
            r.replicate({VerdictRig::batch(recs)});
        }
    };
    data(2);
    r.replicate({VerdictRig::changeBatch(r.writeState()->constraintSets.latest().id + 1,
                                         {{kObj, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(9).fingerprint()},
                                          {kObj + 1, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(8).fingerprint()}})});
    data(10000);
    const auto before = r.readLog().changes;
    ASSERT_EQ(before.back().members.size(), 2u);
    r.promote();
    r.passes(200);
    const auto after = r.readLog().changes;
    for (const auto& c : after) EXPECT_FALSE(c.offset > before.back().offset && c.members.size() != 2) << "a change at " << c.offset << " dropped a member";
    ASSERT_EQ(r.produce({{20000, 1, 8}, {20001, 2, 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_EQ(r.stats()->rejectionsConstraint, 1u) << "the second member still judges";
}

TEST(CheckedWritesVerdictWorkerRegression, CleanerVerdictBoundStartsClosedRegression) {
    VerdictRig r;
    EXPECT_EQ(r.replica->cleanerVerdictBound(), 0) << "a user partition is cleaned nothing until its worker says so";
    r.typed();
    r.passes(50);
    EXPECT_EQ(r.replica->cleanerVerdictBound(), rune::replication::PartitionReplica::kNoCleanerLso) << "a Ready partition with no constraint set is uncapped";
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 9}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    EXPECT_NE(r.replica->cleanerVerdictBound(), rune::replication::PartitionReplica::kNoCleanerLso);
}

TEST(CheckedWritesVerdictWorkerRegression, ActivationWithAnUncommittedMarkerMarksAboveItRegression) {
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(20);
    r.followerFetch();
    r.passes(20);
    r.demotePromote();   // the judge activates with the set change in the timeline
    r.followerFetch();
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.followerFetch();   // the records are committed: the judge marks them; no further fetch, so the marker stays above the HW
    while (r.readLog().markers.empty() || r.readLog().markers.back().offset < r.replica->highWatermark()) r.pass();
    const auto first = r.readLog().markers.back();
    ASSERT_EQ(first.entries.size(), 1u);
    r.demotePromote();
    ASSERT_EQ(r.produce({{3, 30, 9}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    const auto log = r.readLog();
    ASSERT_GE(log.markers.size(), 2u);
    const auto& second = log.markers[1];
    // Spec §8.5 ruling (06 re-mark fix): the uncommitted marker commits with the new leader's log, so it marks only above it.
    EXPECT_EQ(second.header.fromOffset, first.header.finalThrough) << "the new activation marks from its log's marker coverage";
    EXPECT_GT(second.header.finalThrough, first.header.finalThrough);
    for (const auto& e : second.entries) EXPECT_GE(std::get<0>(e), first.header.finalThrough) << "entry at " << std::get<0>(e);
    EXPECT_EQ(r.writeState()->rejected.mismatches(), 0u);
}

TEST(CheckedWritesVerdictWorker, DemotePromoteKeepsOwnVerdicts) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    r.demotePromote();
    r.passes(500);
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2));
    const auto t = r.controlTypes();
    EXPECT_EQ(std::count(t.begin(), t.end(), cr::kVerdictMarker), 1) << "the same verdicts are not marked twice";
}

TEST(CheckedWritesVerdictWorker, DeliveryWaitsDuringABuildWritesDoNot) {
    VerdictRig r;
    r.typed();
    r.installProjection(kProj);
    std::vector<Rec> first;
    for (uint64_t i = 1; i <= 500; ++i) first.push_back({i, int64_t(i), i == 7 ? uint8_t(9) : uint8_t(0)});
    ASSERT_EQ(r.produce(first), 0);
    r.passes(50);
    const int64_t before = r.leo();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(3);   // the change record is logged
    ASSERT_EQ(r.produce({{501, 1, 0}, {502, 2, 9}, {503, 3, 0}}), 0) << "writes are acked while the member builds";
    const int64_t A = before;   // the change record lands at the log end the install met
    ASSERT_FALSE(r.controlTypes().empty());
    EXPECT_EQ(r.controlTypes().front(), cr::kConstraintSetChange);
    for (int i = 0; i < 2000; ++i) {
        const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
        if (judge != nullptr && judge->judgedThrough() > A + 1) break;
        if (judge != nullptr) EXPECT_FALSE(r.hasRow(kProj, 503) && judge->judgedThrough() <= A + 1) << "nothing above A folds before the member is built";
        r.pass();
    }
    r.passes(300);
    EXPECT_TRUE(r.hasRow(kProj, 7)) << "pre-A records are never judged";
    EXPECT_FALSE(r.hasRow(kProj, 502));
    EXPECT_TRUE(r.hasRow(kProj, 503));
}

TEST(CheckedWritesVerdictWorker, HookFactsFollowTheTimeline) {
    VerdictRig r;
    r.typed();
    r.passes(20);
    const PartitionWriteState* w = r.worker->writeStateForTest(VerdictRig::kSrc, 0);
    ASSERT_NE(w, nullptr);
    auto facts = topicHookFactsOf(*w);
    EXPECT_FALSE(facts.verdictSource);
    EXPECT_FALSE(facts.constrainedSource);
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    facts = topicHookFactsOf(*w);
    EXPECT_TRUE(facts.verdictSource);
    EXPECT_TRUE(facts.constrainedSource);
}

TEST(CheckedWritesVerdictWorker, TruncationOfAnUncommittedTailChangesNoVerdict) {
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(20);
    r.followerFetch();
    r.passes(20);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    for (int i = 0; i < 6; ++i) { r.followerFetch(); r.passes(20); }
    const PartitionWriteState* w = r.worker->writeStateForTest(VerdictRig::kSrc, 0);
    ASSERT_NE(w, nullptr);
    const int64_t committedW = w->markers.committedThrough();
    ASSERT_GT(committedW, 2);
    ASSERT_EQ(r.produce({{3, 30, 9}}), 0);
    r.passes(300);   // no HW movement: the judge stays below the HW, nothing new is marked
    EXPECT_EQ(w->markers.appendedThrough(), committedW);
    const int64_t cut = r.leo();
    r.truncateTo(cut - 1);
    r.demotePromote();
    r.passes(300);
    EXPECT_EQ(w->markers.committedThrough(), committedW);
    EXPECT_EQ(w->markers.appendedThrough(), committedW);
    EXPECT_NE(w->rejected.find(2), nullptr) << "the committed rejection stays";
    EXPECT_EQ(w->rejected.find(cut - 1), nullptr);
}

TEST(CheckedWritesVerdictWorkerRegression, TruncatedOwnChangeAndMarkerLeaveTheTimelineRegression) {
    // This node logs a marker and a second set change that never replicate; another leader takes over, writes data where they
    // were and logs its own change later.  Once this node follows, truncates and re-leads, neither the cut change nor the cut
    // marker may count.
    constexpr int32_t kRej2 = 0x40000603;
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(20);
    r.followerFetch();
    r.passes(20);
    ASSERT_EQ(r.produce({{1, 1, 0}, {2, 2, 0}, {3, 3, 9}}), 0);
    const int64_t dataEnd = r.leo();
    r.followerFetch(dataEnd);
    r.passes(300);
    const int64_t cut = dataEnd;   // the marker of the rejection sits here, uncommitted
    ASSERT_EQ(r.leo(), cut + 1);
    ASSERT_EQ(r.replica->highWatermark(), cut);
    r.installRejectView(kRej2, kObj + 1, tagRule(8), "rej2");
    r.passes(20);
    ASSERT_EQ(r.readLog().changes.size(), 2u);
    const PartitionWriteState* w = r.worker->writeStateForTest(VerdictRig::kSrc, 0);
    ASSERT_NE(w, nullptr);
    r.demote();
    r.truncateReplicaTo(cut);
    ASSERT_EQ(r.leo(), cut);
    const std::vector<rune::storage::control_record::ChangeMember> both{
        {kObj, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(9).fingerprint()},
        {kObj + 1, static_cast<uint8_t>(rune::view::VerdictViewKind::TestReject), tagRule(8).fingerprint()}};
    r.replicate({VerdictRig::batch({{10, 1, 0}, {11, 1, 0}, {12, 1, 8}}), VerdictRig::changeBatch(7, both), VerdictRig::batch({{13, 1, 8}, {14, 1, 0}})});
    r.passes(50);
    int64_t lastMarkerThrough = 0;
    for (const auto& m : r.readLog().markers) lastMarkerThrough = std::max<int64_t>(lastMarkerThrough, m.header.finalThrough);
    EXPECT_LE(w->markers.committedThrough(), lastMarkerThrough) << "W passed a marker the log no longer holds";
    r.promote();
    r.passes(500);
    EXPECT_TRUE(r.hasRow(kProj, 10));
    EXPECT_TRUE(r.hasRow(kProj, 11));
    EXPECT_TRUE(r.hasRow(kProj, 12)) << "below the surviving change the record is judged under the old set";
    EXPECT_FALSE(r.hasRow(kProj, 13));
    EXPECT_TRUE(r.hasRow(kProj, 14));
}

// #809: two log cuts X < Y between two passes: the timeline must drop everything at and above X (the lowest cut), not only above Y.
TEST(CheckedWritesVerdictWorkerRegression, TwoCutsBetweenPassesTruncateTheTimelineAtTheLowerCutRegression) {
    constexpr int32_t kRej2 = 0x40000603;
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(20);
    r.followerFetch();
    r.passes(20);
    ASSERT_EQ(r.produce({{1, 1, 0}, {2, 2, 0}, {3, 3, 9}}), 0);
    const int64_t dataEnd = r.leo();
    r.followerFetch(dataEnd);
    r.passes(300);
    const int64_t X = dataEnd;   // the marker of the rejection sits here
    ASSERT_EQ(r.leo(), X + 1);
    r.installRejectView(kRej2, kObj + 1, tagRule(8), "rej2");   // a second set change above X
    r.passes(20);
    ASSERT_EQ(r.readLog().changes.size(), 2u);
    const PartitionWriteState* w = r.worker->writeStateForTest(VerdictRig::kSrc, 0);
    ASSERT_NE(w, nullptr);
    ASSERT_GE(w->markers.appendedThrough(), X);
    const int64_t changeAt = r.readLog().changes.back().offset;
    ASSERT_GE(changeAt, X);
    r.demote();
    // Cut 1 at X, then a foreign batch above it, then cut 2 at Y > X, with no worker pass in between.
    r.replica->truncateLogForTest(X);
    ASSERT_EQ(r.leo(), X);
    r.replicateNoPass({VerdictRig::batch({{10, 1, 0}, {11, 1, 0}, {12, 1, 0}})});
    const int64_t Y = X + 2;
    ASSERT_GT(r.leo(), Y);
    r.replica->truncateLogForTest(Y);
    ASSERT_EQ(r.replica->lastTruncationOffset(), Y);
    r.passes(1);
    EXPECT_LT(w->constraintSets.lastChangeOffset(), X) << "a set change in [X, Y) survived the two cuts";
    EXPECT_LT(w->markers.appendedThrough(), X) << "a marker in [X, Y) survived the two cuts";
}

TEST(CheckedWritesVerdictWorkerRegression, LeadershipScanPastTheLedgerCapacityKeepsWRegression) {
    // More markers in the log than the ledger holds.  The leadership scan feeds them in order without an HW in between, so the
    // ring filled and every later marker was dropped: W stayed at the 1024th until the leader re-marked the range.
    VerdictRig r;
    r.worker->setVerdictIntervalNs(kMs);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    const std::size_t want = MarkerLedger::kCapacity + 76;
    for (uint64_t i = 1; r.readLog().markers.size() < want; ++i) {
        ASSERT_EQ(r.produce({{i, int64_t(i), 9}}), 0);
        r.passes(4);
        ASSERT_LT(i, 3 * want);
    }
    r.passes(20);
    int64_t lastMarkerThrough = 0;
    for (const auto& m : r.readLog().markers) lastMarkerThrough = std::max<int64_t>(lastMarkerThrough, m.header.finalThrough);
    ASSERT_GT(r.readLog().markers.size(), MarkerLedger::kCapacity);
    r.rcScanBytesPerPass = 1 << 24;   // the whole log in one scan step
    r.restart(/*reinstall=*/false);
    r.passes(50);
    EXPECT_EQ(r.writeState()->markers.committedThrough(), lastMarkerThrough) << "the scan dropped the markers past the ledger capacity";
}

TEST(CheckedWritesVerdictWorkerRegression, FailoverRightAfterAChangeRecordJudgesWithTheSameSetRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(10);   // the change record is the only thing in the log
    r.restart();
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2));
    const auto t = r.controlTypes();
    EXPECT_EQ(std::count(t.begin(), t.end(), cr::kConstraintSetChange), 1) << "the new leader takes the set from the log";
}

TEST(CheckedWritesVerdictWorkerRegression, FailoverBetweenHwAndFoldJudgesTheSameRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}, {3, 30, 0}}), 0);
    r.restart();   // committed, not yet folded or judged by this node
    r.passes(500);
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2));
    EXPECT_TRUE(r.hasRow(kProj, 3));
    const auto t = r.controlTypes();
    EXPECT_EQ(std::count(t.begin(), t.end(), cr::kVerdictMarker), 1);
}

TEST(CheckedWritesVerdictWorker, CheckpointPushWaitsForAMarker) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 0}}), 0);
    r.passes(20);   // under the 100 ms interval: accepted-only progress is not marked yet
    EXPECT_TRUE(r.hasRow(kRej, 1));
    EXPECT_LT(r.agent->lastDurableOffset(kRej, 0), 0) << "no delta leaves before a marker covers its cursor";
    r.passes(300);
    EXPECT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
}

TEST(CheckedWritesVerdictWorker, RestartWithMarkersKeepsTheCheckpoint) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    const int64_t durable = r.agent->lastDurableOffset(kRej, 0);
    ASSERT_GT(durable, 0);
    r.restart();
    r.passes(300);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->verdictCheckpointsDiscarded, 0u);
    EXPECT_TRUE(r.hasRow(kRej, 1));
    EXPECT_FALSE(r.hasRow(kRej, 2));
    EXPECT_TRUE(listViewBackups(r.snapDir(), kRej, 0).empty()) << "a checkpoint that is kept is never backed up";   // V4
}

TEST(CheckedWritesVerdictRestartRegression, CheckpointAheadOfLocalMarkersIsDiscardedAndRefoldedRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    const int64_t beforeBatch = r.leo();
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    r.truncateTo(r.leo() - 1);   // the crash lost the marker (the last record of the log); the records below stay
    ASSERT_GT(r.leo(), beforeBatch);
    const std::vector<uint8_t> committed = slurp(committedFile(r, kRej));
    ASSERT_FALSE(committed.empty());
    r.restart();
    r.passes(500);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->verdictCheckpointsDiscarded, 1u);
    EXPECT_TRUE(r.hasRow(kRej, 1));
    EXPECT_FALSE(r.hasRow(kRej, 2)) << "refolded under the re-judged verdicts";
    // V1: the discarded checkpoint is moved aside, byte for byte, before the refold.
    const auto backups = listViewBackups(r.snapDir(), kRej, 0);
    ASSERT_EQ(backups.size(), 1u);
    EXPECT_NE(backups[0].find(".ahead-of-verdicts.delta"), std::string::npos) << backups[0];
    EXPECT_EQ(slurp(backups[0]), committed);
}

// V3: with backups off the discard removes the old checkpoint instead; the member refolds all the same.
TEST(CheckedWritesVerdictRestartRegression, DiscardedVerdictMemberWithBackupOffRemovesWithoutBackupRegression) {
    VerdictRig r;
    r.backupPolicy = ViewBackupPolicy{false, 3};
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    r.truncateTo(r.leo() - 1);
    r.restart();
    r.passes(500);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->verdictCheckpointsDiscarded, 1u);
    EXPECT_EQ(r.agent->discardsForTest(), 1);
    EXPECT_FALSE(std::filesystem::exists(viewBackupDir(r.snapDir()))) << "backups are off: nothing is kept";
    EXPECT_TRUE(r.hasRow(kRej, 1));
    EXPECT_FALSE(r.hasRow(kRej, 2));
}

// V2: a persisted member whose checkpoint cursor lies below the source's log start (retention deleted what it folded from)
// is backed up (below-log-start) and refolded from the log start, not from 0.
TEST(CheckedWritesVerdictRestartRegression, DiscardedVerdictMemberOverATrimmedLogIsBackedUpThenRefoldedRegression) {
    VerdictRig r(false, /*rollBytes=*/1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    const std::vector<uint8_t> oldCheckpoint = slurp(committedFile(r, kRej));
    ASSERT_FALSE(oldCheckpoint.empty());
    for (uint64_t i = 2; i <= 6; ++i) ASSERT_EQ(r.produce({{i, int64_t(i) * 10, 0}}), 0);
    r.installRejectView(kRej + 10, kObj + 1, tagRule(8), "rej2", /*persisted=*/true);   // a newer set change: the first one may go
    r.passes(100);
    for (uint64_t i = 7; i <= 8; ++i) ASSERT_EQ(r.produce({{i, int64_t(i) * 10, 0}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    for (int i = 0; i < 300; ++i) {
        if (const auto snap = r.log->cleanerSnapshot())
            for (const auto& si : *snap) (void)r.log->enqueueDeletedSegment(si.baseOffset, si.lineage);
        r.pass();
    }
    const int64_t logStart = r.log->logStartOffset();
    ASSERT_GT(logStart, 2) << "retention deleted the early segments";
    // The member's checkpoint is the old one: its cursor is below the log start.
    { std::ofstream f(committedFile(r, kRej), std::ios::binary | std::ios::trunc);
      f.write(reinterpret_cast<const char*>(oldCheckpoint.data()), static_cast<std::streamsize>(oldCheckpoint.size())); }
    r.restart();
    r.passes(600);
    ASSERT_TRUE(r.vs->allViewsReady());
    const auto backups = listViewBackups(r.snapDir(), kRej, 0);
    ASSERT_EQ(backups.size(), 1u);
    EXPECT_NE(backups[0].find(".below-log-start.delta"), std::string::npos) << backups[0];
    EXPECT_EQ(slurp(backups[0]), oldCheckpoint);
    EXPECT_FALSE(r.hasRow(kRej, 1)) << "its record was trimmed: not in the rebuilt view";
    EXPECT_TRUE(r.hasRow(kRej, 8));
}

// P4-4 over a retention-trimmed source log: the member's checkpoint is ahead of the local markers (the crash lost the last one) and
// the early records are gone.  The pre-discard rows are backed up exactly once (knob on) or not at all (knob off); the live member
// refolds from the current log start and re-judges the survivors.
namespace {
void ahead_of_markers_over_a_trimmed_log(bool backupsOn) {
    VerdictRig r(false, /*rollBytes=*/1);   // one batch per segment
    r.backupPolicy = ViewBackupPolicy{backupsOn, 3};
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    const int64_t o1 = r.leo();
    ASSERT_EQ(r.produce({{1, 10, 0}}), 0);
    r.passes(300);
    r.installRejectView(0x40000605, 8, tagRule(8), "rej8");   // not persisted: the source logs a second set change C2 above o1
    r.passes(50);
    const int64_t c2 = r.readLog().changes.back().offset;
    ASSERT_GT(c2, o1);
    ASSERT_EQ(r.produce({{2, 20, 0}, {3, 30, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    const std::vector<uint8_t> committed = slurp(committedFile(r, kRej));
    ASSERT_FALSE(committed.empty());
    (void)r.log->deleteOldSegments(0, c2);   // retention up to C2
    ASSERT_GT(r.log->logStartOffset(), o1);
    ASSERT_LE(r.log->logStartOffset(), c2);
    r.restart(true, r.leo() - 1);            // the crash loses the last marker: the checkpoint is ahead of the local markers
    r.passes(500);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->verdictCheckpointsDiscarded, 1u);
    EXPECT_FALSE(r.hasRow(kRej, 1)) << "refolded from the trimmed log start: id 1's record is gone";
    EXPECT_TRUE(r.hasRow(kRej, 2));
    EXPECT_FALSE(r.hasRow(kRej, 3)) << "re-judged";
    const auto backups = listViewBackups(r.snapDir(), kRej, 0);
    if (backupsOn) {
        ASSERT_EQ(backups.size(), 1u) << "exactly one backup per discard";
        EXPECT_NE(backups[0].find(".ahead-of-verdicts.delta"), std::string::npos) << backups[0];
        EXPECT_EQ(slurp(backups[0]), committed) << "the pre-discard rows, byte for byte";
    } else {
        EXPECT_TRUE(backups.empty());
        EXPECT_FALSE(std::filesystem::exists(viewBackupDir(r.snapDir())));
    }
}
}  // namespace

TEST(CheckedWritesVerdictRestartRegression, DiscardedVerdictMemberAheadOfMarkersOverATrimmedLogIsBackedUpThenRefoldedRegression) {
    ahead_of_markers_over_a_trimmed_log(/*backupsOn=*/true);
}

TEST(CheckedWritesVerdictRestartRegression, DiscardedVerdictMemberAheadOfMarkersOverATrimmedLogWithBackupOffKeepsNoBackupRegression) {
    ahead_of_markers_over_a_trimmed_log(/*backupsOn=*/false);
}

// V5: when the backup cannot be made, nothing is rebuilt over the unsaved checkpoint: the member stays unsettled and the judge
// does not pass it (no verdicts, no markers) for the rest of the boot.
TEST(CheckedWritesVerdictRestartRegression, BackupFailureKeepsTheVerdictMemberUnjudgedRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    r.truncateTo(r.leo() - 1);
    const std::vector<uint8_t> committed = slurp(committedFile(r, kRej));
    { std::ofstream blocker(viewBackupDir(r.snapDir()), std::ios::binary); blocker << "not a directory"; }
    r.restart();
    r.passes(300);
    const CheckpointState* st = r.vs->checkpointStateForTest(kRej, 0);
    ASSERT_NE(st, nullptr);
    ASSERT_TRUE(std::holds_alternative<CheckpointDiscardFailed>(*st));
    EXPECT_EQ(std::get<CheckpointDiscardFailed>(*st).reason, ViewDiscardReason::AheadOfVerdicts);
    EXPECT_FALSE(r.vs->allViewsReady());
    EXPECT_EQ(slurp(committedFile(r, kRej)), committed) << "the unsaved checkpoint is untouched";
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    const int64_t judged = judge->judgedThrough();
    const uint64_t markers = r.stats()->markersWritten;
    r.produce({{3, 30, 0}, {4, 40, 9}});
    r.passes(kPassCap);
    EXPECT_EQ(judge->judgedThrough(), judged) << "an unsettled member counts as behind: the judge does not pass it";
    EXPECT_EQ(r.stats()->markersWritten, markers) << "no verdicts are issued";
    EXPECT_FALSE(r.hasRow(kRej, 3));
}

// B1: a verdict member waiting on a discard (Failed: never retried; Pending: the ack is outstanding) has no work to do, so the
// worker must report 0 and idle instead of spinning.
TEST(CheckedWritesVerdictRestartRegression, DiscardFailedVerdictMemberLetsTheWorkerIdleRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    r.truncateTo(r.leo() - 1);
    { std::ofstream blocker(viewBackupDir(r.snapDir()), std::ios::binary); blocker << "not a directory"; }
    r.restart();
    r.passes(300);
    const CheckpointState* st = r.vs->checkpointStateForTest(kRej, 0);
    ASSERT_NE(st, nullptr);
    ASSERT_TRUE(std::holds_alternative<CheckpointDiscardFailed>(*st));
    r.passes(50);
    int sum = 0;
    for (int i = 0; i < 100; ++i) sum += r.pass();
    EXPECT_EQ(sum, 0) << "a member in DiscardFailed must let the worker idle";
}

TEST(CheckedWritesVerdictRestartRegression, DiscardPendingVerdictMemberLetsTheWorkerIdleRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9), "rej", /*persisted=*/true);
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    r.passes(300);
    ASSERT_GT(r.agent->lastDurableOffset(kRej, 0), 0);
    r.truncateTo(r.leo() - 1);
    r.pauseAgent = true;   // the discard frame goes out, its ack never comes back
    r.restart();
    r.passes(300);
    const CheckpointState* st = r.vs->checkpointStateForTest(kRej, 0);
    ASSERT_NE(st, nullptr);
    ASSERT_TRUE(std::holds_alternative<CheckpointDiscardPending>(*st));
    int sum = 0;
    for (int i = 0; i < 100; ++i) sum += r.pass();
    EXPECT_EQ(sum, 0) << "a member in DiscardPending must let the worker idle";
    ASSERT_TRUE(std::holds_alternative<CheckpointDiscardPending>(*r.vs->checkpointStateForTest(kRej, 0)));
    r.pauseAgent = false;
    r.passes(600);
    EXPECT_TRUE(r.vs->allViewsReady()) << "the member settles once the agent acks";
}

TEST(CheckedWritesVerdictWorkerRegression, RefoldBelowWIgnoresALaterRuleRegression) {
    VerdictRig r;
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 1}, {2, 20, 9}}), 0);
    r.passes(300);
    constexpr int32_t kRej2 = 0x40000603;
    r.replaceRejectView(kRej, kRej2, kObj, tagRule(1));
    r.passes(300);
    EXPECT_FALSE(r.hasRow(kRej2, 2)) << "below W the marker decides, not the new rule";
    EXPECT_TRUE(r.hasRow(kRej2, 1)) << "the new rule never judges what the marker already settled";
    ASSERT_EQ(r.produce({{3, 30, 1}, {4, 40, 9}}), 0);
    r.passes(300);
    EXPECT_FALSE(r.hasRow(kRej2, 3)) << "tag 1 is rejected after the replace";
    EXPECT_TRUE(r.hasRow(kRej2, 4));
}

TEST(CheckedWritesVerdictWorkerRegression, DroppedMemberStaysUntilTheJudgeIsPastTheDropRegression) {
    VerdictRig r(/*follower=*/true);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.installProjection(kProj);
    r.passes(50);
    r.followerFetch();
    ASSERT_EQ(r.produce({{1, 10, 1}, {2, 20, 9}}), 0);   // not replicated yet: the HW lags the drop below
    r.dropRejectView(kRej);
    r.passes(20);
    const int64_t dropAt = r.leo();
    ASSERT_TRUE(r.driveUntilJudged(dropAt)) << "the judge needs the dropped member to read the records before the drop";
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    EXPECT_TRUE(judge == nullptr || !judge->hasMember(kObj, tagRule(9).fingerprint())) << "left once a committed marker passed the drop";
    EXPECT_TRUE(r.hasRow(kProj, 1));
    EXPECT_FALSE(r.hasRow(kProj, 2)) << "judged with the dropped member";
}

// ---- Task 8 on the rig: the cleaner against a real Worker, replica and published bounds ----

TEST(CheckedWritesVerdictCleanerRegression, SwapAboveTheVerdictBoundIsNeverInstalledRegression) {
    VerdictRig r(/*follower=*/false, /*rollBytes=*/1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    for (int64_t v = 1; v <= 6; ++v) ASSERT_EQ(r.produce({{5, v, 0}}), 0);   // six copies of key 5
    ASSERT_TRUE(r.driveUntilJudged());
    const int64_t dataEnd = r.dataEnd();
    ASSERT_EQ(r.readLog().data.size(), 6u);
    const auto first = r.compact(1'000'000, /*seal=*/true, /*drain=*/false);   // produced and queued for the owner
    ASSERT_GT(first.bound, 1);
    // A leader change cut the markers the cleaner read: the bound the owner sees is below every member's base.
    r.replica->publishCleanerVerdictBound(1);
    (void)r.drainSwaps();
    EXPECT_EQ(r.readLog().data.size(), 6u) << "an output above the owner's verdict bound was installed";
    r.replica->publishCleanerVerdictBound(first.bound);
    EXPECT_GE(first.bound, dataEnd);
    (void)r.compact();
    const auto data = r.readLog().data;
    ASSERT_EQ(data.size(), 1u) << "once the bound passed the bases the next pass installs";
    EXPECT_EQ(data[0].v, 6);
}

TEST(CheckedWritesVerdictCleanerRegression, RemovedMarkerNeverStallsCoverageRegression) {
    VerdictRig r(false, 1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    ASSERT_EQ(r.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.produce({{3, 30, 0}, {4, 40, 9}}), 0);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.readLog().markers.size(), 2u);
    const int64_t secondTo = r.readLog().markers.back().header.finalThrough;
    int64_t rejectedOffset = -1;
    for (const auto& d : r.readLog().data) if (d.id == 4) rejectedOffset = d.offset;
    (void)r.compact(1'000'000);   // the first marker's records are gone: it is stamped
    r.replica->setLastCleanOffset(0);   // nothing new is dirty: the drop needs a pass from the log start (a leader without a checkpoint)
    (void)r.compact(1'005'000);   // past the delete horizon: dropped
    const auto log = r.readLog();
    ASSERT_EQ(log.markers.size(), 1u) << "the first marker was removed";
    EXPECT_EQ(log.markers[0].header.finalThrough, secondTo);
    for (const auto& d : log.data) EXPECT_NE(d.tag, 9) << "the compaction drops every rejected record";

    r.failover();
    const int64_t sub = r.subscribe(SubKind::PushRead, 0);
    r.passes(500);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_GE(judge->judgedThrough(), secondTo) << "the coverage comes from the second marker";
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->rejectionsConstraint, 0u) << "nothing below the second marker is judged again";
    EXPECT_EQ(r.stats()->markersWritten, 0u);
    EXPECT_NE(r.writeState()->rejected.find(rejectedOffset), nullptr);
    std::vector<uint64_t> ids;
    for (const auto& d : r.collect(sub).records) ids.push_back(d.id);
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 3}));
}

TEST(CheckedWritesVerdictCleaner, RigCompactionKeepsTheAcceptedLatest) {
    VerdictRig r(false, 1);
    r.typed();
    r.installRejectView(kRej, kObj, tagRule(9));
    r.passes(50);
    std::mt19937_64 rng(2026);
    std::map<uint64_t, std::pair<int64_t, bool>> latest;   // id -> (v, deleted) of the latest accepted record
    int64_t v = 0;
    for (int i = 0; i < 200; ++i) {
        std::vector<Rec> recs;
        for (int n = 1 + static_cast<int>(rng() % 3); n > 0; --n) {
            Rec rec{1 + rng() % 8, ++v, static_cast<uint8_t>(rng() % 10 == 0 ? 9 : rng() % 3), rng() % 12 == 0};
            recs.push_back(rec);
            if (rec.tag != 9 || rec.del) latest[rec.id] = {rec.v, rec.del};   // tombstones are never rejected
        }
        ASSERT_EQ(r.produce(recs), 0);
    }
    ASSERT_TRUE(r.driveUntilJudged());
    (void)r.compact();
    std::map<uint64_t, std::pair<int64_t, bool>> inLog;
    for (const auto& d : r.readLog().data) {
        EXPECT_FALSE(!d.del && d.tag == 9) << "rejected record at " << d.offset << " survived";
        inLog[d.id] = {d.v, d.del};
    }
    for (const auto& [id, want] : latest) {
        const auto it = inLog.find(id);
        ASSERT_NE(it, inLog.end()) << "key " << id << " lost";
        EXPECT_EQ(it->second, want) << "key " << id;
    }
}

// ---- #798: a replica that lags the verdict marker leads after the cleaner ran ----
namespace {
/// Every batch of `log` from `from`, as raw bytes, in offset order (the walk controlTypes() does).
std::vector<std::vector<uint8_t>> rawBatchesOf(rune::storage::Log& log, int64_t from, int64_t through) {
    std::vector<std::vector<uint8_t>> out;
    for (int64_t off = from; off < through;) {
        const auto v = log.readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            int32_t blen = 0;
            std::memcpy(&blen, v.data + p + 8, 4);
            const std::size_t total = 12 + static_cast<std::size_t>(blen);
            if (p + total > v.len) break;
            int64_t base = 0;
            std::memcpy(&base, v.data + p, 8);
            if (base >= through) return out;
            if (base >= from) out.emplace_back(v.data + p, v.data + p + total);
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}
}  // namespace

// Node 1 leads and judges two batches (markers M1 over the first, M2 over the second); node 2 fetched only up to M1.  The cleaner
// runs twice (stamp, then drop) with the replica's published bound: it must keep M1, because node 2 holds the first batch's rejected
// record but not M1.  Node 2 then leads from its own copy of the pre-compaction head plus what it fetches from node 1 afterwards:
// it delivers no rejected record and its views hold no row of one.
TEST(CheckedWritesVerdictWorkerRegression, LaggingReplicaLeadsAndDeliversNoRejectedRecordRegression) {
    VerdictRig a(/*follower=*/true, /*rollBytes=*/1);
    a.typed();
    a.installRejectView(kRej, kObj, tagRule(9));
    a.passes(50);
    ASSERT_EQ(a.produce({{1, 10, 0}, {2, 20, 9}}), 0);
    ASSERT_TRUE(a.driveUntilJudged());
    ASSERT_EQ(a.produce({{3, 30, 0}, {4, 40, 9}}), 0);
    ASSERT_TRUE(a.driveUntilJudged());
    const auto before = a.readLog();
    ASSERT_EQ(before.markers.size(), 2u);
    const int64_t M1 = before.markers.front().offset;
    int64_t rejectedFirst = -1, rejectedSecond = -1;
    for (const auto& d : before.data) {
        if (d.id == 2) rejectedFirst = d.offset;
        if (d.id == 4) rejectedSecond = d.offset;
    }
    ASSERT_LT(rejectedFirst, M1);
    ASSERT_GT(rejectedSecond, M1);
    // Node 2's copy of the head (everything below M1), taken before the cleaner runs.
    const auto head = rawBatchesOf(*a.log, 0, M1);
    ASSERT_FALSE(head.empty());
    a.followerFetch(M1);   // node 2 is behind M1
    const int64_t t0 = 1'000'000;
    (void)a.compact(t0);
    (void)a.compact(t0 + 5000);
    ASSERT_EQ(a.readLog().markers.size(), 2u) << "M1 is kept: a replica has not fetched it";
    ASSERT_EQ(a.readLog().markers.front().offset, M1);
    const auto tail = rawBatchesOf(*a.log, M1, a.leo());   // what node 2 fetches from node 1 afterwards
    ASSERT_FALSE(tail.empty());

    VerdictRig b(/*follower=*/true, 1);
    b.demote();   // a follower from the start: its log is only what node 1 replicates (offsets match node 1's)
    b.typed();
    b.installRejectView(kRej, kObj, tagRule(9));
    b.passes(20);
    ASSERT_EQ(b.leo(), 0);
    b.replicate(head);
    b.replicate(tail);
    b.promote();
    // Node 2 learns the first batch's rejection on taking the lead, from the replicated marker M1 (the kept one): its rejected index
    // holds exactly that record.  Were M1 dropped, the index would be empty and the record would be delivered.
    ASSERT_NE(b.writeState(), nullptr);
    EXPECT_TRUE(b.writeState()->rejected.isRejected(rejectedFirst)) << "learned from M1 when node 2 took the lead";
    for (int i = 0; i < 100; ++i) { b.followerFetch(); b.passes(5); }
    // The log of node 2 holds exactly the two markers node 1 replicated: M1 (over the first batch, rejecting its second record) and
    // M2 (over the second batch); node 2's judge adds none, because both batches are already covered.
    EXPECT_EQ(b.readLog().markers.size(), 2u);
    const int64_t s = b.subscribe(SubKind::Raw, 0);
    std::vector<uint64_t> ids;
    for (const auto& d : b.collect(s).records) ids.push_back(d.id);
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(ids, (std::vector<uint64_t>{1, 3})) << "no rejected record is delivered";
    EXPECT_TRUE(b.hasRow(kRej, 1));
    EXPECT_FALSE(b.hasRow(kRej, 2));
    EXPECT_TRUE(b.hasRow(kRej, 3));
    EXPECT_FALSE(b.hasRow(kRej, 4));
}
