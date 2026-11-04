// PersistedOverTransientViewTest.cpp - checked writes Part 10 (spec 9a): a persisted view over a TRANSIENT source.
//
// Layer: the production WorkerViewSet, driven as Worker::driveViewsOf_ drives it, over a simulated source ring
// (TransientViewRig.h).  A restart replays the rig's checkpoint file image.  No Aeron, no sleeps, every drive loop capped.
#include <support/TransientViewRig.h>

#include <rune/node/NsLimitAggregator.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/transaction/ReadCommittedView.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <ostream>
#include <random>
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
    filter.index = &idx;
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

// ---------------------------------------------------------------------------
// The recovery matrix: entity x history op x timing = 108 cells.
// ---------------------------------------------------------------------------
namespace {

enum class TvEntity : uint8_t { Replace, CountByGrp, MinByGrp };
enum class TvHistoryOp : uint8_t { Upserts, Overwrites, Tombstones, GroupMoves };
enum class TvTiming : uint8_t {
    FullRestartEmptyRing, FullRestartRingBelowOldCursor, FullRestartRingPastOldCursor, NodeRestartReReplicatedRing,
    NodeRestartRingStartAboveCheckpoint, FailoverLiveCursorRetained, FailoverLiveCursorEvicted, RingRecreatedLive,
    SecondFullRestart,
};
struct TvFoldCell { TvEntity entity; TvHistoryOp op; TvTiming timing; };

constexpr std::array<TvEntity, 3>    kEntities{TvEntity::Replace, TvEntity::CountByGrp, TvEntity::MinByGrp};
constexpr std::array<TvHistoryOp, 4> kOps{TvHistoryOp::Upserts, TvHistoryOp::Overwrites, TvHistoryOp::Tombstones, TvHistoryOp::GroupMoves};
constexpr std::array<TvTiming, 9>    kTimings{TvTiming::FullRestartEmptyRing, TvTiming::FullRestartRingBelowOldCursor,
                                              TvTiming::FullRestartRingPastOldCursor, TvTiming::NodeRestartReReplicatedRing,
                                              TvTiming::NodeRestartRingStartAboveCheckpoint, TvTiming::FailoverLiveCursorRetained,
                                              TvTiming::FailoverLiveCursorEvicted, TvTiming::RingRecreatedLive,
                                              TvTiming::SecondFullRestart};

std::vector<TvFoldCell> cells() {
    std::vector<TvFoldCell> out;
    for (const TvEntity e : kEntities)
        for (const TvHistoryOp o : kOps)
            for (const TvTiming t : kTimings) out.push_back({e, o, t});
    return out;
}

const char* name(TvEntity e) { return e == TvEntity::Replace ? "Replace" : e == TvEntity::CountByGrp ? "CountByGrp" : "MinByGrp"; }
const char* name(TvHistoryOp o) {
    switch (o) {
        case TvHistoryOp::Upserts: return "Upserts";
        case TvHistoryOp::Overwrites: return "Overwrites";
        case TvHistoryOp::Tombstones: return "Tombstones";
        case TvHistoryOp::GroupMoves: return "GroupMoves";
    }
    return "";
}
const char* name(TvTiming t) {
    switch (t) {
        case TvTiming::FullRestartEmptyRing: return "FullRestartEmptyRing";
        case TvTiming::FullRestartRingBelowOldCursor: return "FullRestartRingBelowOldCursor";
        case TvTiming::FullRestartRingPastOldCursor: return "FullRestartRingPastOldCursor";
        case TvTiming::NodeRestartReReplicatedRing: return "NodeRestartReReplicatedRing";
        case TvTiming::NodeRestartRingStartAboveCheckpoint: return "NodeRestartRingStartAboveCheckpoint";
        case TvTiming::FailoverLiveCursorRetained: return "FailoverLiveCursorRetained";
        case TvTiming::FailoverLiveCursorEvicted: return "FailoverLiveCursorEvicted";
        case TvTiming::RingRecreatedLive: return "RingRecreatedLive";
        case TvTiming::SecondFullRestart: return "SecondFullRestart";
    }
    return "";
}
void PrintTo(const TvFoldCell& c, std::ostream* os) { *os << name(c.entity) << "_" << name(c.op) << "_" << name(c.timing); }

Kind kindOf(TvEntity e) { return e == TvEntity::Replace ? Kind::Replace : e == TvEntity::CountByGrp ? Kind::CountByGrp : Kind::MinByGrp; }

// H1: six records over keys 1-4 and groups 10, 20, 30, with one overwrite (key 1) and one group move (key 3).
// Final per key: 1 -> (10, 6), 2 -> (10, 7), 3 -> (30, 4), 4 -> (30, 9).
constexpr std::array<Rec, 6> kMatrixH1{Rec{1, 10, 5}, Rec{2, 10, 7}, Rec{3, 20, 4}, Rec{4, 30, 9}, Rec{1, 10, 6}, Rec{3, 30, 4}};

std::vector<Rec> h2(TvHistoryOp op) {
    switch (op) {
        case TvHistoryOp::Upserts:    return {Rec{5, 10, 3}, Rec{6, 20, 2}, Rec{7, 30, 8}};
        case TvHistoryOp::Overwrites: return {Rec{1, 10, 60}, Rec{2, 10, 70}, Rec{3, 30, 40}};
        case TvHistoryOp::Tombstones: return {Rec{1, 0, 0, true}, Rec{2, 0, 0, true}, Rec{4, 0, 0, true}};
        case TvHistoryOp::GroupMoves: return {Rec{1, 20, 6}, Rec{2, 30, 7}, Rec{4, 10, 9}};
    }
    return {};
}

// What the instance wrote to the table must be exactly the model's, for every key (Replace) or group (aggregates).
void expectViewEqualsModel(const ViewSetRig& rig, Kind kind, const TransientViewModel& m) {
    for (int64_t k = 0; k < kKeyDomain; ++k) {
        switch (kind) {
            case Kind::Replace:
                ASSERT_EQ(rig.hp(k), m.hp(k)) << "key " << k;
                ASSERT_EQ(rig.grp(k), m.rows.count(k) ? std::optional<int64_t>(m.rows.at(k).grp) : std::nullopt) << "key " << k;
                break;
            case Kind::CountByGrp: ASSERT_EQ(rig.count(k), m.count(k)) << "group " << k; break;
            case Kind::MinByGrp:   ASSERT_EQ(rig.minHp(k), m.minHp(k)) << "group " << k; break;
        }
    }
}

}  // namespace

class CheckedWritesTransientViewMatrixRegression : public ::testing::TestWithParam<TvFoldCell> {};

TEST(CheckedWritesTransientViewMatrixCountRegression, CellCountIsPinned) { EXPECT_EQ(cells().size(), 108u); }

// capacity: the view's initial index slots.  The small one makes the index grow while the checkpoint replays and the ring re-folds.
static void runMatrixCell(const TvFoldCell& c, uint32_t capacity) {
    const Kind kind = kindOf(c.entity);
    ViewSetRig rig("cell");
    rig.install(kind, /*persisted=*/true, capacity);
    TransientViewModel model;

    rig.append(kMatrixH1);
    ASSERT_TRUE(rig.driveUntilCaughtUp(1));
    model.replay(kMatrixH1);

    uint64_t wantResets = 1, wantEvicted = 0;
    const std::array<Rec, 2> fresh2{Rec{31, 30, 3}, Rec{32, 10, 8}};
    // Three records written while another node leads (nothing folds here): an overwrite and a delete of H1 keys, a new key.
    const std::array<Rec, 3> gap{Rec{2, 10, 70}, Rec{4, 0, 0, true}, Rec{43, 20, 3}};
    switch (c.timing) {
        case TvTiming::FullRestartEmptyRing:
        case TvTiming::SecondFullRestart:
            rig.newRing(0, 2);
            rig.restart();
            break;
        case TvTiming::FullRestartRingBelowOldCursor:   // new ring [0, 2), old cursor 6
            rig.newRing(0, 2);
            rig.append(fresh2);
            rig.restart();
            model.replay(fresh2);
            break;
        case TvTiming::FullRestartRingPastOldCursor: {  // new ring [0, 9)
            std::vector<Rec> nine;
            for (int64_t k = 0; k < 9; ++k) nine.push_back(Rec{40 + k, 10 * (1 + k % 3), k + 1});
            rig.newRing(0, 2);
            rig.append(nine);
            rig.restart();
            model.replay(nine);
            break;
        }
        case TvTiming::NodeRestartReReplicatedRing:     // the same lineage: the ring is re-replicated from 0
            rig.newRing(0, 1);
            rig.append(kMatrixH1);
            rig.restart();
            model.replay(kMatrixH1);
            break;
        case TvTiming::NodeRestartRingStartAboveCheckpoint:   // offsets 6-7 were never checkpointed here; the ring starts at 8
            rig.newRing(8, 1);
            rig.restart();
            break;
        case TvTiming::FailoverLiveCursorRetained:
            rig.append(gap);
            model.replay(gap);
            wantResets = 0;
            break;
        case TvTiming::FailoverLiveCursorEvicted:
            rig.append(gap);
            rig.evictTo(rig.logEnd() - 1);   // the cursor (6) + 2: the first two never fold
            model.replay(std::span<const Rec>(gap).subspan(2));
            wantResets = 0;
            wantEvicted = 2;
            break;
        case TvTiming::RingRecreatedLive:               // incarnation 1 -> 2, new content from offset 0
            rig.newRing(0, 2);
            rig.append(fresh2);
            model.replay(fresh2);
            break;
    }

    const std::vector<Rec> second = h2(c.op);
    rig.append(second);
    model.replay(second);
    const int64_t probe = kind == Kind::Replace ? 1 : 10;
    (void)rig.pass(3, /*maxRecordsPerRead=*/1);
    EXPECT_EQ(rig.status(kind, probe), RowStatus::NotReady) << "(d) a re-fold in progress is never served";
    ASSERT_TRUE(rig.driveUntilCaughtUp(3));
    EXPECT_TRUE(rig.status(kind, probe) == RowStatus::Ok || rig.status(kind, probe) == RowStatus::NotFound);

    if (c.timing == TvTiming::SecondFullRestart) {      // H2 is folded and checkpointed; a second restart, then H3
        rig.newRing(0, 3);
        rig.restart();
        const std::array<Rec, 1> h3{Rec{8, 20, 4}};
        rig.append(h3);
        model.replay(h3);
        ASSERT_TRUE(rig.driveUntilCaughtUp(5));
    }

    expectViewEqualsModel(rig, kind, model);   // (a) rows, (c) no double count
    EXPECT_EQ(rig.counters(kind).transientSourceResets, wantResets) << "(b)";
    EXPECT_EQ(rig.counters(kind).transientRecordsEvicted, wantEvicted) << "(b)";

    const auto image = rig.tableImage(kind);   // (e) what was folded after the event is durable
    rig.newRing(0, 9);
    rig.restart();
    ASSERT_TRUE(rig.driveUntilCaughtUp(7));
    EXPECT_EQ(rig.tableImage(kind), image);
    expectViewEqualsModel(rig, kind, model);
    // The grouped views hold three rows, which fit the small index.
    if (capacity == 4 && kind == Kind::Replace) EXPECT_GT(rig.indexSlots(kind), 4u) << "the small table's index must have grown";
}

TEST_P(CheckedWritesTransientViewMatrixRegression, ViewEqualsThePerKeyModel) { runMatrixCell(GetParam(), 256); }

class CheckedWritesTransientViewSmallTableMatrixRegression : public ::testing::TestWithParam<TvFoldCell> {};

TEST_P(CheckedWritesTransientViewSmallTableMatrixRegression, ViewEqualsThePerKeyModel) { runMatrixCell(GetParam(), 4); }

auto cellName = [](const ::testing::TestParamInfo<TvFoldCell>& i) {
    return std::string(name(i.param.entity)) + "_" + name(i.param.op) + "_" + name(i.param.timing);
};
INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesTransientViewMatrixRegression, ::testing::ValuesIn(cells()), cellName);
INSTANTIATE_TEST_SUITE_P(Cell, CheckedWritesTransientViewSmallTableMatrixRegression, ::testing::ValuesIn(cells()), cellName);

// ---------------------------------------------------------------------------
// Model check: seeded random histories over all three views.
// ---------------------------------------------------------------------------
namespace {

struct ModelRun {
    ViewSetRig rig{"model"};
    TransientViewModel model;
    uint64_t resets = 0, evicted = 0;       // what the model expects the instances to have counted, summed over their lives
    uint64_t gotResets = 0, gotEvicted = 0; // what the dead instances counted
    uint64_t multiCutPasses = 0;   // passes that found more than one cut: coverage of what the skipped settles open up
    uint32_t incarnation = 1;
    uint64_t gen = 1;
    // What the next pass will find, since the last one: the instances are new (no pass yet, or a restart), the ring was
    // recreated, how many cuts it took and where the last one was.  `cursor` is the fold end of the last pass.
    bool fresh = true, recreated = false;
    int cuts = 0;
    int64_t lastCut = 0, cursor = 0;
    std::vector<std::string> steps;

    /// A restart reconciles (and counts) once per view that recovered a checkpoint cursor.
    [[nodiscard]] uint64_t recoveringViews() const {
        uint64_t n = 0;
        for (const Kind k : {Kind::Replace, Kind::CountByGrp, Kind::MinByGrp}) n += rig.hasCheckpoint(k);
        return n;
    }
    [[nodiscard]] uint64_t countedResets() const {
        uint64_t n = gotResets;
        for (const Kind k : {Kind::Replace, Kind::CountByGrp, Kind::MinByGrp}) n += rig.counters(k).transientSourceResets;
        return n;
    }
    void harvest() {
        for (const Kind k : {Kind::Replace, Kind::CountByGrp, Kind::MinByGrp}) {
            gotResets += rig.counters(k).transientSourceResets;
            gotEvicted += rig.counters(k).transientRecordsEvicted;
        }
    }
    /// One pass over everything that happened since the last one: the model replays the ring from where the views re-fold
    /// (the ring start after a restart or a recreation, or when more than one cut moved the generation), then the views run.
    void settle() {
        const int64_t start = rig.logStart();
        int64_t from = cursor;
        if (fresh) {
            resets += recoveringViews();
            from = start;
        } else if (recreated) {
            resets += 3;
            from = start;
        } else {
            if (cuts > 0) {
                const int64_t cut = cuts == 1 ? lastCut : start;
                if (cursor > cut) {
                    resets += 3;
                    from = std::max(cut, start);
                    multiCutPasses += cuts > 1;
                }
            }
            if (from < start) {
                evicted += 3 * static_cast<uint64_t>(start - from);
                from = start;
            }
        }
        model.replay(rig.ringRecs(from));
        cursor = rig.logEnd();
        fresh = recreated = false;
        cuts = 0;
        gen += 2;
        ASSERT_TRUE(rig.driveUntilCaughtUp(gen));
    }
};

Rec randomRec(std::mt19937_64& rng) {
    static constexpr std::array<int64_t, 3> kGroups{10, 20, 30};
    const int64_t key = 1 + static_cast<int64_t>(rng() % 8);
    if (rng() % 4 == 0) return Rec{key, 0, 0, true};
    return Rec{key, kGroups[rng() % 3], 1 + static_cast<int64_t>(rng() % 50)};
}
std::vector<Rec> randomRecs(std::mt19937_64& rng, std::size_t n) {
    std::vector<Rec> out;
    for (std::size_t i = 0; i < n; ++i) out.push_back(randomRec(rng));
    return out;
}

void expectAllViewsEqualModel(const ModelRun& r) {
    for (const Kind k : {Kind::Replace, Kind::CountByGrp, Kind::MinByGrp}) expectViewEqualsModel(r.rig, k, r.model);
}

// (e): the checkpoint of what is folded now is the whole table - a full restart with an empty ring changes nothing.
void expectDurable(ModelRun& r) {
    ASSERT_NO_FATAL_FAILURE(r.settle());
    std::array<std::vector<std::vector<uint8_t>>, 3> image;
    const std::array<Kind, 3> kinds{Kind::Replace, Kind::CountByGrp, Kind::MinByGrp};
    for (std::size_t i = 0; i < kinds.size(); ++i) image[i] = r.rig.tableImage(kinds[i]);
    const std::vector<Rec> ring = r.rig.ringRecs();
    const int64_t start = r.rig.logStart();
    r.harvest();
    const uint64_t recovering = r.recoveringViews();
    r.rig.newRing(0, ++r.incarnation);
    r.rig.restart();
    r.gen += 2;
    ASSERT_TRUE(r.rig.driveUntilCaughtUp(r.gen));
    for (std::size_t i = 0; i < kinds.size(); ++i) ASSERT_EQ(r.rig.tableImage(kinds[i]), image[i]) << "(e) view " << i;
    r.resets += recovering;
    // Put the ring back: the same records at the same offsets, as a restarted node would hold them.
    r.harvest();
    r.rig.newRing(start, ++r.incarnation);
    r.rig.append(ring);
    r.rig.restart();
    r.gen += 2;
    ASSERT_TRUE(r.rig.driveUntilCaughtUp(r.gen));
    r.resets += recovering;
    r.model.replay(ring);
    r.cursor = r.rig.logEnd();
    ASSERT_NO_FATAL_FAILURE(expectAllViewsEqualModel(r));
}

}  // namespace

TEST(CheckedWritesTransientViewModelRegression, RandomHistoriesMatchThePerKeyModel) {
    uint64_t multiCutPasses = 0, restartThenRecreate = 0;
    for (uint64_t seed = 1; seed <= 200; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937_64 rng(seed);
        ModelRun r;
        for (const Kind k : {Kind::Replace, Kind::CountByGrp, Kind::MinByGrp}) r.rig.install(k);
        for (int step = 0; step < 40; ++step) {
            const unsigned what = static_cast<unsigned>(rng() % 10);
            if (what < 4) {                                     // write a batch of 1-3 records
                const std::vector<Rec> recs = randomRecs(rng, 1 + rng() % 3);
                r.steps.push_back("write " + std::to_string(recs.size()));
                r.rig.append(recs);
            } else if (what == 4) {                             // full restart: a fresh ring, 0-2 records in it
                const std::vector<Rec> fresh = randomRecs(rng, rng() % 3);
                r.steps.push_back("full restart " + std::to_string(fresh.size()));
                r.harvest();
                r.rig.newRing(0, ++r.incarnation);
                r.rig.append(fresh);
                r.rig.restart();
                r.fresh = true;
            } else if (what == 5) {                             // node restart: the same lineage, 0-3 oldest evicted
                const std::vector<Rec> ring = r.rig.ringRecs();
                const std::size_t drop = std::min<std::size_t>(rng() % 4, ring.size());
                r.steps.push_back("node restart evicting " + std::to_string(drop));
                const int64_t start = r.rig.logStart() + static_cast<int64_t>(drop);
                r.harvest();
                r.rig.newRing(start, ++r.incarnation);
                r.rig.append(std::span<const Rec>(ring).subspan(drop));
                r.rig.restart();
                r.fresh = true;
            } else if (what == 6) {                             // failover: records written elsewhere, 0-3 of them evicted
                const std::vector<Rec> gap = randomRecs(rng, rng() % 4);
                const std::size_t drop = rng() % (gap.size() + 1);
                r.steps.push_back("failover gap " + std::to_string(gap.size()) + " evicting " + std::to_string(drop));
                const int64_t end = r.rig.logEnd();
                r.rig.append(gap);
                r.rig.evictTo(end + static_cast<int64_t>(drop));
            } else if (what == 7) {                             // the ring is recreated under the live views
                const std::vector<Rec> fresh = randomRecs(rng, rng() % 3);
                r.steps.push_back("ring recreated " + std::to_string(fresh.size()));
                restartThenRecreate += r.fresh && r.recoveringViews() > 0;
                r.rig.newRing(0, ++r.incarnation);
                r.rig.append(fresh);
                r.recreated = true;
                r.cuts = 0;
            } else if (what == 8) {                             // the ring is cut below the cursor (generation + 1)
                const int64_t start = r.rig.logStart(), end = r.rig.logEnd();
                const int64_t cut = start + static_cast<int64_t>(rng() % static_cast<uint64_t>(end - start + 1));
                const std::vector<Rec> replacement = randomRecs(rng, rng() % 3);
                r.steps.push_back("cut at " + std::to_string(cut) + " with " + std::to_string(replacement.size()));
                r.rig.cutAt(cut, replacement);
                ++r.cuts;
                r.lastCut = cut;
            } else {
                r.steps.push_back("durability probe");
                ASSERT_NO_FATAL_FAILURE(expectDurable(r));
            }
            // A step may leave the views unsettled: the next step then adds to what the one pass finds.  The views are
            // compared with the model, and the counters checked, only after a pass.
            if (what != 9 && step + 1 < 40 && rng() % 3 == 0) {
                r.steps.back() += " (unsettled)";
                continue;
            }
            ASSERT_NO_FATAL_FAILURE(r.settle());
            ASSERT_NO_FATAL_FAILURE(expectAllViewsEqualModel(r)) << "step " << step << ": " << r.steps.back();
            EXPECT_EQ(r.countedResets(), r.resets) << "step " << step << ": " << r.steps.back();
            if (::testing::Test::HasFailure()) {
                for (const std::string& s : r.steps) ADD_FAILURE() << s;
                return;
            }
        }
        ASSERT_NO_FATAL_FAILURE(expectDurable(r));
        r.harvest();
        EXPECT_EQ(r.gotResets, r.resets) << "transientSourceResets summed over every instance";
        EXPECT_EQ(r.gotEvicted, r.evicted) << "transientRecordsEvicted summed over every instance";
        multiCutPasses += r.multiCutPasses;
        if (::testing::Test::HasFailure()) {
            for (const std::string& s : r.steps) ADD_FAILURE() << s;
            return;
        }
    }
    EXPECT_GT(multiCutPasses, 0u) << "no history moved the generation by more than one between two passes";
    EXPECT_GT(restartThenRecreate, 0u) << "no history recreated the ring between a restart and its first pass";
}

// ---------------------------------------------------------------------------
// Namespace cell: a view over a transient source belongs to the source topic's namespace, like any view.  A real Worker
// drives the view set over a real transient replica whose ring carries the namespace tag.
// ---------------------------------------------------------------------------
TEST(CheckedWritesTransientViewNamespaceRegression, ViewOverATransientSourceIsChargedPausedAndThrottledWithItsNamespaceRegression) {
    using namespace rune::node;
    namespace pc = rune::protocol::client;
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs   = 21;
    constexpr int32_t  kView = 0x40000B00;

    rune::storage::InMemoryLog ring{1 << 20, 1000};
    ring.setNsTag(kNs, kSlot);
    rune::replication::InMemoryTransport transport;
    rune::replication::PartitionConfig cfg{};
    cfg.topicId = kSrcTopic; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
    cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20;
    cfg.pushOnAppend = false;
    rune::replication::PartitionReplica replica(cfg, rune::replication::Role::Follower, 0,
                                                std::vector<rune::replication::NodeId>{1},
                                                std::vector<rune::replication::NodeId>{1}, &ring, &transport);
    replica.becomeLeader(1, {1});

    const rune::codec::Descriptor src = transient_view_rig::detail::source();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(transient_view_rig::detail::plan(Kind::Replace, /*persisted*/ 0), srcs, kView).value();
    ViewSnapshotChannel channel;
    WorkerViewSet vs(channel, (std::filesystem::temp_directory_path() / ("rune-tv-ns-" + std::to_string(::getpid()))).string());
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ true);
    rune::protocol::NamespaceLimits limits;
    vs.onNsSlotAssign(kSlot, kNs, limits);

    Worker w(0);
    w.setOwnedDirect(Worker::OwnedSet{&replica});
    w.setViewSet(&vs);
    w.applyNsSlotAssignDirect(kSlot, kNs, limits);
    int64_t now = 0;
    auto passes = [&](int n) { for (int i = 0; i < n; ++i) w.doWork(now += 1'000'000LL); };
    auto put = [&](int64_t key, int64_t hp) {
        const std::string k = transient_view_rig::detail::keyBytes(key), v = transient_view_rig::detail::value(Rec{key, 10, hp});
        ASSERT_GE(replica.appendKeyed(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size()),
                                      std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()),
                                      /*acksAll=*/false), 0);
    };
    auto read = [&](int64_t key) {
        WorkerOp op;
        op.type = WorkerOp::Type::GetViewRow;
        op.topicId = kView; op.partition = 0; op.sessionId = 1; op.correlationId = 42;
        op.nextOffset = key; op.hwBound = 0;
        EXPECT_TRUE(w.tryEnqueueOp(std::move(op)));
        passes(1);
        WorkerReturn got{};
        int n = 0;
        w.drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ViewRowResult) { got = std::move(r); ++n; } });
        EXPECT_EQ(n, 1);
        return got;
    };

    put(7, 55);
    passes(200);   // bounded: the replication state and the read-committed state build, then the fold
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(kView, 0, 7, L), nullptr) << "the worker folds a persisted view over a transient source";
    EXPECT_GT(vs.nsViewMemBytes(kSlot), 0u) << "its table is charged to the source topic's namespace";
    EXPECT_EQ(read(7).errorCode, 0);

    w.applyNsViewMemOverDirect(kSlot, kNs, true);
    put(8, 66);
    passes(20);
    EXPECT_TRUE(vs.instanceServeState(kView, 0).paused) << "over the namespace's view memory: paused";
    EXPECT_EQ(read(7).errorCode, static_cast<int16_t>(pc::ViewRowResponse::kViewPaused));
    w.applyNsViewMemOverDirect(kSlot, kNs, false);
    passes(20);
    EXPECT_FALSE(vs.instanceServeState(kView, 0).paused);
    EXPECT_NE(vs.getRow(kView, 0, 8, L), nullptr) << "resumed from the paused offset: the record appended while paused folds";

    w.applyNsThrottleDirect(kSlot, kNs, ThrottleKind::QueryCycles, now + 50'000'000LL);
    EXPECT_EQ(read(7).errorCode, static_cast<int16_t>(pc::ViewRowResponse::kThrottled)) << "read-throttled like any view";
}
