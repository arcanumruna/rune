// PersistedOverTransientViewTest.cpp - checked writes Part 10 (spec 9a): a persisted view over a TRANSIENT source.
//
// Layer: the production WorkerViewSet, driven as Worker::driveViewsOf_ drives it, over a simulated source ring
// (TransientViewRig.h).  A restart replays the rig's checkpoint file image.  No Aeron, no sleeps, every drive loop capped.
#include <support/TransientViewRig.h>

#include <rune/transaction/ReadCommittedView.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <array>
#include <string>
#include <vector>

using namespace transient_view_rig;
using RowStatus = rune::node::WorkerViewSet::RowStatus;

namespace {

/// Captures spdlog output (level and text) for the scope's lifetime.
struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(512);
    std::shared_ptr<spdlog::logger> prev = spdlog::default_logger();
    LogCapture() {
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::trace);
        lg->set_pattern("%l %v");
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(prev); }
    std::vector<std::string> grep(const std::string& needle) const {
        std::vector<std::string> out;
        for (auto& l : sink->last_formatted()) if (l.find(needle) != std::string::npos) out.push_back(l);
        return out;
    }
};

// H1: key 1 moves from group 10 to group 20.
constexpr std::array<Rec, 4> kH1{Rec{1, 10, 5}, Rec{2, 10, 7}, Rec{3, 20, 4}, Rec{1, 20, 6}};

void writeH1AndFold(ViewSetRig& rig) {
    rig.append(kH1);
    ASSERT_TRUE(rig.driveUntilCaughtUp(1));
}

}  // namespace

TEST(CheckedWritesTransientViewRegression, RecoveredCheckpointOverATransientSourceLogsInfoAndCountsAResetRegression) {
    ViewSetRig rig("info");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    rig.newRing(0, 2);
    rig.restart();
    const std::array<Rec, 1> fresh{Rec{9, 30, 1}};
    rig.append(fresh);
    LogCapture cap;
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    const auto info = cap.grep("is from an earlier life of its transient source");
    ASSERT_EQ(info.size(), 1u);
    EXPECT_EQ(info[0].rfind("info ", 0), 0u) << info[0];
    EXPECT_TRUE(cap.grep("invariant breach").empty());
    EXPECT_EQ(rig.counters(Kind::Replace).transientSourceResets, 1u);
    EXPECT_EQ(rig.counters(Kind::Replace).transientRecordsEvicted, 0u);
    EXPECT_EQ(rig.hp(1), 6);
    EXPECT_EQ(rig.hp(2), 7);
    EXPECT_EQ(rig.hp(3), 4) << "checkpoint rows kept";
    EXPECT_EQ(rig.hp(9), 1) << "the new ring folds from its start";
}

TEST(CheckedWritesTransientViewRegression, FullRestartCheckpointAheadOfTheNewRingServesNotReadyUntilRefoldedRegression) {
    ViewSetRig rig("ahead");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    rig.newRing(0, 2);
    const std::array<Rec, 3> fresh{Rec{7, 30, 1}, Rec{8, 30, 2}, Rec{9, 30, 3}};   // logEnd 3 < the old cursor 4
    rig.append(fresh);
    rig.restart();

    (void)rig.pass(3, /*maxRecordsPerRead=*/1);
    EXPECT_EQ(rig.status(Kind::Replace, 7), RowStatus::NotReady);
    EXPECT_EQ(rig.status(Kind::Replace, 1), RowStatus::NotReady) << "checkpoint rows are not served while the ring is re-folded";
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));
    EXPECT_EQ(rig.status(Kind::Replace, 9), RowStatus::Ok);
    EXPECT_EQ(rig.hp(1), 6);
}

TEST(CheckedWritesTransientViewRegression, FullRestartRingPastTheOldCursorFoldsEveryNewRecordRegression) {
    ViewSetRig rig("past");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    rig.newRing(0, 2);
    std::vector<Rec> fresh;
    for (int64_t k = 11; k <= 19; ++k) fresh.push_back(Rec{k, 40, k});   // offsets 0..8, past the old cursor 4
    rig.append(fresh);
    rig.restart();
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    for (int64_t k = 11; k <= 19; ++k) EXPECT_EQ(rig.hp(k), k) << "key " << k;
    EXPECT_EQ(rig.hp(1), 6);
    EXPECT_EQ(rig.hp(2), 7);
    EXPECT_EQ(rig.hp(3), 4);
}

TEST(CheckedWritesTransientViewRegression, FailoverAfterAnEvictedTombstoneKeepsTheStaleRowAndCountsTheGapRegression) {
    ViewSetRig rig("tomb");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    // Another node leads: nothing folds here while these arrive and the oldest are evicted.
    const std::array<Rec, 2> gap{Rec{2, 10, 0, true}, Rec{3, 20, 9}};
    rig.append(gap);
    const std::array<Rec, 1> kept{Rec{4, 30, 1}};
    rig.append(kept);
    rig.evictTo(6);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    EXPECT_EQ(rig.hp(2), 7) << "the evicted tombstone never folded: the row stays (stale)";
    EXPECT_EQ(rig.hp(3), 4) << "the evicted overwrite never folded";
    EXPECT_EQ(rig.hp(4), 1);
    EXPECT_EQ(rig.counters(Kind::Replace).transientRecordsEvicted, 2u);
    EXPECT_EQ(rig.counters(Kind::Replace).transientSourceResets, 0u);

    const std::array<Rec, 1> fix{Rec{2, 10, 8}};
    rig.append(fix);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));
    EXPECT_EQ(rig.hp(2), 8) << "the next write of the key corrects it";
}

TEST(CheckedWritesTransientViewRegression, NodeRestartAboveTheCheckpointCountsAResetNotAnEvictionRegression) {
    ViewSetRig rig("above");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    rig.newRing(6, 1);   // the ring now starts at 6: a restart cannot tell what lay between the checkpoint and it
    rig.restart();
    const std::array<Rec, 1> kept{Rec{4, 30, 1}};
    rig.append(kept);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    EXPECT_EQ(rig.hp(2), 7);
    EXPECT_EQ(rig.hp(3), 4);
    EXPECT_EQ(rig.hp(4), 1);
    EXPECT_EQ(rig.counters(Kind::Replace).transientSourceResets, 1u);
    EXPECT_EQ(rig.counters(Kind::Replace).transientRecordsEvicted, 0u) << "PD5: a restart counts a reset";
}

TEST(CheckedWritesTransientViewRegression, NodeRestartRefoldOfAGroupMoveNeverDoubleCountsRegression) {
    ViewSetRig rig("move");
    rig.install(Kind::CountByGrp);
    rig.install(Kind::MinByGrp);
    writeH1AndFold(rig);
    rig.newRing(0, 1);   // the same lineage: the re-replicated copy of H1
    rig.append(kH1);
    rig.restart();
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    EXPECT_EQ(rig.count(10), 1);
    EXPECT_EQ(rig.count(20), 2) << "keys {1,3}; never 4";
    EXPECT_EQ(rig.minHp(10), 7);
    EXPECT_EQ(rig.minHp(20), 4);
}

TEST(CheckedWritesTransientViewRegression, SecondRestartReplaysBothLivesOfTheCheckpointRegression) {
    ViewSetRig rig("second");
    const std::array<Kind, 3> kinds{Kind::Replace, Kind::CountByGrp, Kind::MinByGrp};
    for (const Kind k : kinds) rig.install(k);
    writeH1AndFold(rig);
    rig.newRing(0, 2);
    rig.restart();
    const std::array<Rec, 1> second{Rec{5, 30, 2}};   // the second life's cursor is 1, below the first's 4
    rig.append(second);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));
    std::array<std::vector<std::vector<uint8_t>>, 3> image;
    for (std::size_t i = 0; i < kinds.size(); ++i) image[i] = rig.tableImage(kinds[i]);
    ASSERT_FALSE(image[0].empty());

    rig.newRing(0, 3);
    rig.restart();
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));
    for (std::size_t i = 0; i < kinds.size(); ++i) {
        EXPECT_EQ(rig.tableImage(kinds[i]), image[i]) << "view " << i;
        EXPECT_EQ(rig.views().foldCursorForTest(viewId(kinds[i]), 0), 0) << "reset to the empty ring's start";
    }
}

TEST(CheckedWritesTransientViewRegression, AbortedRangeInTheNewRingNeverDiscardsTheCheckpointRegression) {
    ViewSetRig rig("aborted");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    const auto before = rig.tableImage(Kind::Replace);

    rune::transaction::AbortedTxnIndex idx;
    idx.onAbortMarker(/*pid*/ 7, /*firstDataOffset*/ 0, /*abortMarkerOffset*/ 1);
    rig.newRing(0, 2);
    rig.appendTransactional(7, Rec{50, 10, 1});   // offset 0, below the old cursor 4
    rig.appendAbortMarker(7);                     // offset 1
    rune::view::AbortedTxnFilter filter;
    filter.ctx = &idx;
    filter.fn  = [](const void* ctx, int64_t pid, int64_t base) noexcept {
        return static_cast<const rune::transaction::AbortedTxnIndex*>(ctx)->isAborted(pid, base);
    };
    rig.restart(filter);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    EXPECT_EQ(rig.counters(Kind::Replace).skippedAborted, 1u);
    EXPECT_EQ(rig.hp(50), std::nullopt) << "the aborted record never folds";
    EXPECT_EQ(rig.tableImage(Kind::Replace), before) << "the checkpoint of a persisted view over a transient source always replays";
    EXPECT_EQ(rig.hp(1), 6);
}

TEST(CheckedWritesTransientViewRegression, FullRestartLosesOnlyTheUncheckpointedTailRegression) {
    ViewSetRig rig("tail");
    rig.install(Kind::Replace);
    writeH1AndFold(rig);
    const std::array<Rec, 1> tail{Rec{6, 30, 3}};
    rig.append(tail);
    ASSERT_TRUE(rig.driveUntilCaughtUp(1));   // one more delta
    rig.newRing(0, 2);
    rig.restart(rune::view::kNoAbortedTxns, /*dropLastDeltas=*/1);
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));

    EXPECT_EQ(rig.hp(6), std::nullopt) << "the tail that never reached the disk is lost (spec 9a)";
    EXPECT_EQ(rig.hp(1), 6) << "the rest is kept";
}
