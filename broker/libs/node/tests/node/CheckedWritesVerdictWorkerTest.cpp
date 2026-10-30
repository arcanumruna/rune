// Checked writes Part 04 Task 6: the verdict pipeline on a real Worker.
#include "CheckedWritesRig.h"

#include <algorithm>

using namespace rune::node;
using namespace rune::node::cw;

namespace cr = rune::storage::control_record;

namespace {
constexpr int64_t kMs = 1'000'000;
constexpr int32_t kRej = 0x40000601, kProj = 0x40000602;
constexpr uint64_t kObj = 77;

TestRejectRule tagRule(uint8_t tag) { TestRejectRule r; r.mode = TestRejectRule::Mode::TagEquals; r.tag = tag; return r; }
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
    r.restart();
    r.passes(500);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->verdictCheckpointsDiscarded, 1u);
    EXPECT_TRUE(r.hasRow(kRej, 1));
    EXPECT_FALSE(r.hasRow(kRej, 2)) << "refolded under the re-judged verdicts";
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
