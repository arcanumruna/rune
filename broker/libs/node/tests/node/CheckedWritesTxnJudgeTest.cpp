// Part 11 Task 4: TxnJudgeState, the TxnJudge and marker feed, the overlay pool.
#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <system_error>

#include <rune/node/RcState.h>
#include <rune/node/VerdictSource.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include "support/CheckedWritesRig.h"

using namespace rune::node;
namespace cr = rune::storage::control_record;

namespace {
TxnJudgeState reserved() {
    TxnJudgeState t;
    t.reserve();
    return t;
}
}  // namespace

TEST(CheckedWritesTxnState, JudgeRecordThenMarkerDecides) {
    auto t = reserved();
    DeferredTxn* e = t.onJudgeRecord(500, {77, 3, 400});
    ASSERT_NE(e, nullptr);
    ASSERT_EQ(t.find(77), e);
    EXPECT_EQ(e->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(t.earliestUndecidedFirst(), 400);
    t.onMarker(77, 520, true);
    EXPECT_EQ(t.find(77)->markerOffset, 520);
    EXPECT_TRUE(t.find(77)->commit);
    EXPECT_EQ(t.find(77)->phase, TxnPhase::AwaitJudge) << "only the judge finalizes";
    EXPECT_EQ(t.earliestUndecidedFirst(), INT64_MAX);
}

// F1: a marker of a producer that did not vote is the judge's to count, not the state's.
TEST(CheckedWritesTxnState, MarkerWithoutAnEntryCountsNothingRegression) {
    auto t = reserved();
    t.onMarker(99, 530, false);
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    t.onMarker(77, 520, true);
    t.onMarker(77, 520, true);
    EXPECT_EQ(t.stats.markerWithoutVote, 0u);
}

TEST(CheckedWritesTxnState, TruncateDropsEntriesAtOrAbove) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_NE(t.onJudgeRecord(700, {78, 3, 650}), nullptr);
    t.truncateFrom(600);
    EXPECT_NE(t.find(77), nullptr);
    EXPECT_EQ(t.find(78), nullptr);
}

TEST(CheckedWritesTxnState, SecondTxnOfOneProducerReplacesTheDecidedOne) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    t.onMarker(77, 520, true);
    DeferredTxn* e = t.onJudgeRecord(900, {77, 3, 850});
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->firstOffset, 850);
    EXPECT_EQ(e->markerOffset, -1);
    EXPECT_EQ(t.find(77)->judgeOffset, 900);
    EXPECT_EQ(t.earliestUndecidedFirst(), 850);
}

TEST(CheckedWritesTxnState, ParkedVotesAreBounded) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxParkedVotes; ++i) EXPECT_TRUE(t.parkVote({static_cast<int64_t>(i), 1, 77, 3}));
    EXPECT_FALSE(t.parkVote({999, 1, 77, 3}));
}

TEST(CheckedWritesTxnState, FullTableRefusesTheRecord) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)}), nullptr);
    EXPECT_EQ(t.onJudgeRecord(5000, {5000, 1, 4000}), nullptr);
}

// F2: an abort that lands while a vote is parked answers nothing; the judge decides.
TEST(CheckedWritesTxnState, MarkerDoesNotAnswerAParkedVoteRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3}));
    t.onMarker(77, 520, false);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 0);
    t.find(77)->phase = TxnPhase::Finalized;
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 1);
}

// F3: decided entries make room, so the table never fills for good.
TEST(CheckedWritesTxnState, FullTableReusesAFinalizedEntryRegression) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        DeferredTxn* e = t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)});
        ASSERT_NE(e, nullptr);
        e->phase = TxnPhase::Finalized;
    }
    ASSERT_NE(t.onJudgeRecord(5000, {5000, 1, 4000}), nullptr);
    EXPECT_EQ(t.find(0), nullptr) << "the oldest finalized entry went";
    EXPECT_NE(t.find(1), nullptr);
    t.find(5000)->phase = TxnPhase::Judging;
    for (std::size_t i = 1; i < TxnJudgeState::kMaxTxns; ++i) t.find(static_cast<int64_t>(i))->overlay = 0;
    EXPECT_EQ(t.onJudgeRecord(6000, {6000, 1, 5000}), nullptr) << "an overlay-holding entry is never reused";
}

// F5: a producer's next J waits for the overlay of its previous transaction; a re-fed older J changes nothing.
TEST(CheckedWritesTxnState, ReplaceNeverDropsAnOverlayHolderRegression) {
    auto t = reserved();
    DeferredTxn* e = t.onJudgeRecord(500, {77, 3, 400});
    ASSERT_NE(e, nullptr);
    e->overlay = 2;
    EXPECT_EQ(t.onJudgeRecord(900, {77, 3, 850}), nullptr);
    EXPECT_EQ(t.find(77)->judgeOffset, 500);
    e->overlay = -1;
    ASSERT_NE(t.onJudgeRecord(900, {77, 3, 850}), nullptr);
    EXPECT_EQ(t.onJudgeRecord(500, {77, 3, 400}), t.find(77));
    EXPECT_EQ(t.find(77)->judgeOffset, 900);
}

// F7: a replace frees the old entry's slot.
TEST(CheckedWritesTxnState, ReplaceFreesTheOldSlotRegression) {
    auto t = reserved();
    for (std::size_t i = 1; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)}), nullptr);
    ASSERT_NE(t.onJudgeRecord(5000, {77000, 1, 4000}), nullptr);
    t.onMarker(77000, 5100, true);
    ASSERT_NE(t.onJudgeRecord(6000, {77000, 1, 5500}), nullptr);
    EXPECT_EQ(t.find(77000)->firstOffset, 5500);
}

TEST(CheckedWritesTxnState, DrainAnsweredTakesLockedAndFinalized) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_NE(t.onJudgeRecord(600, {78, 3, 450}), nullptr);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3}));
    ASSERT_TRUE(t.parkVote({2, 0, 78, 3}));
    t.find(77)->phase = TxnPhase::Locked;
    EXPECT_EQ(t.lockedCount(), 1u);
    int64_t seen = 0;
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote& v, const DeferredTxn&) { seen = v.token; }), 1);
    EXPECT_EQ(seen, 1);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 0);
}

TEST(CheckedWritesTxnState, FeedHandlesType19AndMarkers) {
    PartitionWriteState s;
    verdictFeedBegin(&s, 0, 1000);
    std::array<uint8_t, cr::kTxnJudgeBatchBytes> b{};
    ASSERT_EQ(cr::buildTxnJudge(b, {77, 3, 400}), b.size());
    verdictFeedBatch(&s, b.data(), b.size(), 500);
    ASSERT_NE(s.txn.find(77), nullptr);
    EXPECT_EQ(s.txn.find(77)->judgeOffset, 500);
    EXPECT_EQ(s.txn.earliestUndecidedFirst(), 400);
    verdictFeedMarker(&s, 77, 520, true);
    EXPECT_EQ(s.txn.find(77)->markerOffset, 520);
    EXPECT_EQ(s.txn.earliestUndecidedFirst(), INT64_MAX);
    std::array<uint8_t, cr::reservationBatchBytes()> other{};
    ASSERT_EQ(cr::buildReservation(other, 10), other.size());
    verdictFeedBatch(&s, other.data(), other.size(), 600);
    EXPECT_EQ(s.txn.find(77)->judgeOffset, 500);
    verdictFeedBegin(&s, 0, 450);
    EXPECT_EQ(s.txn.find(77), nullptr);
}

TEST(CheckedWritesTxnPool, AcquireReleaseAndLockSet) {
    TxnOverlayPool pool;
    pool.reserveFor(4, 8);
    std::array<int32_t, rune::view::kTxnOverlaysPerWorker> ids{};
    for (std::size_t i = 0; i < ids.size(); ++i) {
        ids[i] = pool.acquire(i % 2 == 0 ? 100 : 200);
        ASSERT_GE(ids[i], 0);
    }
    EXPECT_EQ(pool.acquire(100), -1);
    const rune::view::TxnLockSet set = pool.lockSetFor(100);
    EXPECT_EQ(set.count, ids.size() / 2);
    for (uint8_t i = 0; i < set.count; ++i) EXPECT_EQ(set.locked[i]->sourceKey(), 100u);
    const auto key = rune::view::ViewKey::fromU64(5);
    ASSERT_NE(pool.at(ids[0]).put(0, rune::view::OverlayTable::KeyIndex, key, nullptr, 1), nullptr);
    EXPECT_TRUE(pool.lockSetFor(100).contains(0, rune::view::OverlayTable::KeyIndex, key));
    EXPECT_FALSE(pool.lockSetFor(200).contains(0, rune::view::OverlayTable::KeyIndex, key));
    pool.release(ids[0]);
    EXPECT_EQ(pool.lockSetFor(100).count, ids.size() / 2 - 1);
    EXPECT_EQ(pool.acquire(300), ids[0]);
    EXPECT_EQ(pool.at(ids[0]).rows(), 0u);
}

namespace {
struct ScanLog {
    std::filesystem::path dir = rune::node::cw::rigTmpPath("txnscan");
    rune::storage::Log    log{dir.string(), 64LL * 1024 * 1024};
    ~ScanLog() { std::error_code ec; std::filesystem::remove_all(dir, ec); }
    void append(const std::vector<uint8_t>& b) {
        int64_t base = 0;
        ASSERT_GE(log.appendNoSyncRestamp(b.data(), b.size(), 0, base), 0);
    }
    void appendFiller(int64_t pid, int32_t seq) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq);
        b.append("k", "v", 1000);
        append(b.build());
    }
    void appendJudge(int64_t pid, int64_t first) {
        std::vector<uint8_t> b(cr::kTxnJudgeBatchBytes);
        ASSERT_EQ(cr::buildTxnJudge(b, {pid, 0, first}), b.size());
        append(b);
    }
};
RcVerdictHooks txnHooks(PartitionWriteState& s) {
    return RcVerdictHooks{.begin = &verdictFeedBegin, .batch = &verdictFeedBatch, .ctx = &s, .marker = &verdictFeedMarker};
}
}  // namespace

// F4: a marker appended while the leadership scan has not yet reached the J is applied after the scan, not lost.
TEST(CheckedWritesTxnState, MarkerAppendedDuringTheScanLandsAfterItRegression) {
    ScanLog l;
    l.appendFiller(9, 0);
    l.appendFiller(9, 1);
    l.appendJudge(77, 0);
    PartitionWriteState s;
    RcState rc;
    rc.reserve(8);
    rc.setVerdictHooks(txnHooks(s));
    RcFacts f;
    f.leader = true; f.epoch = 1;
    (void)rc.advance(f, &l.log, /*budget=*/1);
    ASSERT_TRUE(rc.scanning());
    ASSERT_EQ(s.txn.find(77), nullptr) << "the scan has not reached the J";
    rc.onMarker(77, 1000, true);
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    ASSERT_NE(s.txn.find(77), nullptr);
    EXPECT_EQ(s.txn.find(77)->markerOffset, 1000);
    EXPECT_TRUE(s.txn.find(77)->commit);
}

// F4: a Ready append reaches the feed; a scanned COMMIT / ABORT carries the header pid and its base offset.
TEST(CheckedWritesTxnState, ReadyAppendAndScannedMarkersReachTheFeedRegression) {
    ScanLog l;
    l.appendJudge(77, 0);
    l.append(rune::transaction::ControlMarker::build(77, 0, rune::transaction::ControlType::Abort, 1));
    l.appendJudge(78, 2);
    PartitionWriteState s;
    RcState rc;
    rc.reserve(8);
    rc.setVerdictHooks(txnHooks(s));
    RcFacts f;
    f.leader = true; f.epoch = 1;
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    ASSERT_NE(s.txn.find(77), nullptr);
    EXPECT_EQ(s.txn.find(77)->markerOffset, 1);
    EXPECT_FALSE(s.txn.find(77)->commit);
    ASSERT_NE(s.txn.find(78), nullptr);
    EXPECT_EQ(s.txn.find(78)->markerOffset, -1);
    rc.onMarker(78, 7, true);
    EXPECT_EQ(s.txn.find(78)->markerOffset, 7);
    EXPECT_TRUE(s.txn.find(78)->commit);
}
