// Checked writes Part 04 Task 6: the verdict pipeline on a real Worker.
#include "CheckedWritesRig.h"

#include <algorithm>
#include <map>
#include <random>

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
    (void)r.compact(1'000'000);   // the first marker's records are gone: it is stamped
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
    EXPECT_NE(r.writeState()->rejected.find(5), nullptr);
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
