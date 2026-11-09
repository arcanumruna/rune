// Part 11 Task 4: TxnJudgeState, the TxnJudge and marker feed, the overlay pool.
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <system_error>

#include <rune/node/RcState.h>
#include <rune/node/VerdictSource.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/view/ViewRow.h>
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
    EXPECT_EQ(t.earliestUndecidedFirst(), 400) << "Task 7 (T5 N1): K waits for the judge's finalize, not for the marker";
    t.find(77)->phase = TxnPhase::Finalized;
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
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3, 500}));
    t.onMarker(77, 520, false);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn*) {}), 0);
    t.find(77)->phase = TxnPhase::Finalized;
    t.find(77)->judged = true;
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn*) {}), 1);
}

// Fix round 1 F5: an entry the table gives up (evicted when full, or replaced by its producer's next J) still holds the cleaner bound
// at its J while its marker lies at or above the committed markers' coverage, and lets it go once that coverage passes the marker.
TEST(CheckedWritesTxnState, GivenUpEntryStillHoldsTheCleanerBoundRegression) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        const auto o = static_cast<int64_t>(i) * 10 + 100;
        DeferredTxn* e = t.onJudgeRecord(o, {static_cast<int64_t>(i), 1, o - 5});
        ASSERT_NE(e, nullptr);
        t.onMarker(static_cast<int64_t>(i), o + 20'000, true);
        e->phase = TxnPhase::Finalized;
    }
    const int64_t w = 15'000;   // every marker lies above it: every J is needed
    EXPECT_EQ(t.judgeRecordsNeededFrom(w), 100);
    ASSERT_NE(t.onJudgeRecord(20'000, {5000, 1, 19'990}), nullptr) << "the oldest Finalized entry (J 100) is evicted";
    EXPECT_EQ(t.find(0), nullptr);
    EXPECT_EQ(t.judgeRecordsNeededFrom(w), 100) << "the evicted J is still needed: its marker lies above the coverage";
    EXPECT_EQ(t.judgeRecordsNeededFrom(20'101), 110) << "past the evicted marker: the bound moves to the next J needed";
    t.onMarker(1, 20'110, true);
    ASSERT_NE(t.onJudgeRecord(30'000, {1, 1, 29'990}), nullptr) << "the producer's next J replaces its decided entry (J 110)";
    EXPECT_EQ(t.judgeRecordsNeededFrom(20'101), 110) << "the replaced J is still needed";
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

// N2: a Finalized entry whose vote is still parked is not reused.
TEST(CheckedWritesTxnState, FullTableKeepsAFinalizedEntryWithAParkedVoteRegression) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        DeferredTxn* e = t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)});
        ASSERT_NE(e, nullptr);
        e->phase = TxnPhase::Finalized;
        e->judged = true;
    }
    ASSERT_TRUE(t.parkVote({1, 0, 0, 1, 10}));
    ASSERT_NE(t.onJudgeRecord(5000, {5000, 1, 4000}), nullptr);
    EXPECT_NE(t.find(0), nullptr);
    EXPECT_EQ(t.find(1), nullptr) << "the next finalized entry went instead";
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn*) {}), 1);
}

// F6: a rescan judges the kept entries anew; a Finalized entry whose marker was cut away is undecided again.
TEST(CheckedWritesTxnState, TruncateResetsTheKeptEntriesRegression) {
    auto t = reserved();
    DeferredTxn* e = t.onJudgeRecord(500, {77, 3, 400});
    ASSERT_NE(e, nullptr);
    t.onMarker(77, 620, true);
    e->phase = TxnPhase::Finalized;
    e->overlay = 3;
    e->records = 9;
    e->answer.code = 5;
    t.truncateFrom(600);
    EXPECT_EQ(e->markerOffset, -1);
    EXPECT_EQ(e->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(e->overlay, -1);
    EXPECT_EQ(e->records, 0);
    EXPECT_EQ(e->answer.code, 0);
    EXPECT_EQ(t.earliestUndecidedFirst(), 400);
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
    t.onMarker(77, 520, true);
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

// T5 re-review 2: a producer's next TxnJudge never replaces an entry without a marker (an AwaitJudge one left by an overlay wait, or one
// owed its marker): K would pass it and a committed transaction would never be applied.
TEST(CheckedWritesTxnState, ReplaceNeverDropsAnUndecidedEntryRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    EXPECT_EQ(t.onJudgeRecord(900, {77, 3, 850}), nullptr);
    EXPECT_EQ(t.find(77)->judgeOffset, 500);
    EXPECT_EQ(t.earliestUndecidedFirst(), 400);
    EXPECT_EQ(t.stats.undecidedReplaceRefusals, 1u);
    t.onMarker(77, 600, true);
    ASSERT_NE(t.onJudgeRecord(900, {77, 3, 850}), nullptr);
    EXPECT_EQ(t.find(77)->firstOffset, 850);
}

// T5 fix-1 M2 / Task 7: a vote is drained only with the judge's answer; an entry finalized without a judgement answers it with
// none (the worker says NotLeader, never an unjudged default), and a rescan keeps a judged decided entry's answer.
TEST(CheckedWritesTxnState, DrainNeverAnswersWithAnUnjudgedDefaultRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    t.onMarker(77, 520, true);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3, 500}));
    t.finalizeDecided(600, 600);
    ASSERT_EQ(t.find(77)->phase, TxnPhase::Finalized);
    int none = 0;
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote&, const DeferredTxn* d) { none += d == nullptr; }), 1);
    EXPECT_EQ(none, 1);
    ASSERT_TRUE(t.parkVote({2, 0, 77, 3, 500}));
    t.find(77)->judged = true;
    t.truncateFrom(510);   // the marker is cut: judged anew
    EXPECT_FALSE(t.find(77)->judged);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn*) {}), 0);
    t.find(77)->phase = TxnPhase::Finalized;
    t.find(77)->judged = true;
    t.find(77)->markerOffset = 505;
    t.truncateFrom(510);   // decided below the cut: the judgement stands
    EXPECT_TRUE(t.find(77)->judged);
    const DeferredTxn* got = nullptr;
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote&, const DeferredTxn* d) { got = d; }), 1);
    EXPECT_NE(got, nullptr);
}

// REGRESSION (Task 8 review 3, m-1): a rescan's reset drops an entry's hold below an applied ordinary span.  A Finalized, unjudged
// entry kept it and held K at the old span's first record, not its own, until finalizeDecided ran past its marker.
TEST(CheckedWritesTxnState, TruncateDropsTheHoldBelowASpanRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    t.holdBelowSpan(300, 450);   // an ordinary span [300, 450] applied around 400
    ASSERT_EQ(t.earliestUndecidedFirst(), 300);
    t.onMarker(77, 520, true);
    t.find(77)->phase = TxnPhase::Finalized;
    t.truncateFrom(510);   // the marker is cut: judged anew
    ASSERT_EQ(t.find(77)->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(t.earliestUndecidedFirst(), 400) << "K is held at T's own first record";
}

// Task 7: a vote bound to an older J is never answered from the producer's next transaction; a scanning vote is the worker's.
TEST(CheckedWritesTxnState, VoteBoundToAReplacedJudgeIsAnsweredWithNoneRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3, 500}));
    ASSERT_TRUE(t.parkVote({2, 0, 77, 3, -1}));
    t.onMarker(77, 520, false);
    t.find(77)->phase = TxnPhase::Finalized;
    ASSERT_NE(t.onJudgeRecord(900, {77, 3, 850}), nullptr);
    t.find(77)->phase = TxnPhase::Locked;
    t.find(77)->judged = true;
    std::vector<std::pair<int64_t, bool>> seen;
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote& v, const DeferredTxn* d) { seen.emplace_back(v.token, d != nullptr); }), 1);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0], std::make_pair(int64_t(1), false));
    EXPECT_EQ(t.parkedCount(), 1u);
    EXPECT_EQ(t.takeScanning(77, 900, [](const ParkedVote&) {}), 1);
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote& v, const DeferredTxn* d) { seen.emplace_back(v.token, d != nullptr); }), 1);
    EXPECT_EQ(seen[1], std::make_pair(int64_t(2), true));
}

TEST(CheckedWritesTxnState, DrainAnsweredTakesLockedAndFinalized) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_NE(t.onJudgeRecord(600, {78, 3, 450}), nullptr);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3, 500}));
    ASSERT_TRUE(t.parkVote({2, 0, 78, 3, 600}));
    t.find(77)->phase = TxnPhase::Locked;
    t.find(77)->judged = true;
    EXPECT_EQ(t.lockedCount(), 1u);
    int64_t seen = 0;
    EXPECT_EQ(t.drainAnswered([&](const ParkedVote& v, const DeferredTxn*) { seen = v.token; }), 1);
    EXPECT_EQ(seen, 1);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn*) {}), 0);
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
    EXPECT_EQ(s.txn.earliestUndecidedFirst(), 400) << "undecided until the judge finalizes it";
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

// N1: a rescan that keeps the state re-feeds the earlier transaction's marker; it must not decide the producer's open newer one.
TEST(CheckedWritesTxnState, RescanNeverGivesANewerTxnAnOlderMarkerRegression) {
    ScanLog l;
    l.appendJudge(77, 0);
    l.append(rune::transaction::ControlMarker::build(77, 0, rune::transaction::ControlType::Commit, 1));
    l.appendJudge(77, 2);
    PartitionWriteState s;
    RcState rc;
    rc.reserve(8);
    rc.setVerdictHooks(txnHooks(s));
    RcFacts f;
    f.leader = true; f.epoch = 1;
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    ASSERT_EQ(s.txn.earliestUndecidedFirst(), 2);
    f.leadershipGeneration = 2;
    (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.scanning());
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    ASSERT_NE(s.txn.find(77), nullptr);
    EXPECT_EQ(s.txn.find(77)->judgeOffset, 2);
    EXPECT_EQ(s.txn.find(77)->markerOffset, -1);
    EXPECT_EQ(s.txn.earliestUndecidedFirst(), 2);
}

// ---- Part 11 Task 5: deferral and run-ahead in driveJudge ----------------------------------------------------------------

namespace {
using P = rune::codec::PrimType;
using rune::view::VerdictViewKind;
constexpr uint16_t kViolation = 55;
constexpr uint8_t  kInsert = 1;

rune::codec::Descriptor tagSource() {
    return rune::codec::DescriptorBuilder("rec", 1).add("id", P::Int64, 1, true).add("tag", P::Int32).build();
}
rune::view::ViewPlan tagView() {
    rune::view::ViewPlan v;
    v.viewName = "rec_tag";
    v.key      = "id";
    v.sources  = {{5, "id"}};
    v.output   = {{"id", P::Int64, 0, "id", rune::view::MergeRule::Replace}, {"tag", P::Int32, 0, "tag", rune::view::MergeRule::Replace}};
    return v;
}
void put16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void put32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void put64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }
std::string_view bytesOf(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Rec { int64_t id; int32_t tag = 1; bool tombstone = false; uint8_t intent = 0; uint8_t keyLen = 8; };
Rec del(int64_t id) { return {id, 1, true}; }

// A committed log of whole batches; a TxnJudge or a marker reaches the feed as the RcState hooks would pass it on append.
struct TxnLog {
    PartitionWriteState& feed;
    std::vector<uint8_t> bytes;
    struct Span { std::size_t pos; int64_t base, last; };
    std::vector<Span>    batches;
    int64_t              next = 0;

    void data(const std::vector<Rec>& recs, int64_t txnPid = -1, bool atomic = false) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        if (txnPid >= 0) {
            b.setTransactional(true);
            b.setProducerId(txnPid);
            b.setProducerEpoch(0);
        }
        for (const Rec& r : recs) {
            std::vector<uint8_t> k, v;
            put64(k, uint64_t(r.id));
            k.resize(std::max<std::size_t>(8, r.keyLen));
            if (!r.tombstone) { put16(v, 12); put16(v, 1); put64(v, uint64_t(r.id)); put32(v, uint32_t(r.tag)); }
            b.append(bytesOf(k), bytesOf(v), 0, r.intent);
        }
        std::vector<uint8_t> batch = b.build();
        if (atomic) {   // P11-6's bit, under the CRC
            namespace pb = rune::protocol;
            batch[std::size_t(pb::batch::kAttributesOffset)] |= uint8_t(pb::batch::kAttrAtomic);
            const uint32_t crc = pb::crc32c(batch.data() + pb::batch::kAttributesOffset, batch.size() - std::size_t(pb::batch::kAttributesOffset));
            for (int i = 0; i < 4; ++i) batch[std::size_t(pb::batch::kCrcOffset + i)] = uint8_t(crc >> (8 * i));
        }
        push(std::move(batch), int64_t(recs.size()));
    }
    void control() {   // an empty verdict marker
        std::vector<uint8_t> c(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(c, cr::MarkerHeader{0, 0, 0, 0}, {});
        push(std::move(c), 1);
    }
    void judge(int64_t pid, int64_t first, bool fed = true) {
        std::vector<uint8_t> b(cr::kTxnJudgeBatchBytes);
        (void)cr::buildTxnJudge(b, {pid, 0, first});
        const int64_t at = next;
        push(b, 1);
        if (fed) verdictFeedBatch(&feed, b.data(), b.size(), at);
    }
    void marker(int64_t pid, bool commit) {
        const int64_t at = next;
        push(rune::transaction::ControlMarker::build(pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 1), 1);
        verdictFeedMarker(&feed, pid, at, commit);
    }
    void push(std::vector<uint8_t> batch, int64_t count) {
        for (int i = 0; i < 8; ++i) batch[std::size_t(i)] = uint8_t(uint64_t(next) >> (8 * i));   // baseOffset (outside the CRC)
        batches.push_back({bytes.size(), next, next + count - 1});
        bytes.insert(bytes.end(), batch.begin(), batch.end());
        next += count;
    }
    void truncateFrom(int64_t at) {   // whole batches only
        while (!batches.empty() && batches.back().base >= at) {
            bytes.resize(batches.back().pos);
            batches.pop_back();
        }
        next = at;
    }
    rune::storage::LogReadView read(int64_t from, bool oneBatch = false) const {
        for (std::size_t i = 0; i < batches.size(); ++i) {
            const Span& s = batches[i];
            const std::size_t end = oneBatch && i + 1 < batches.size() ? batches[i + 1].pos : bytes.size();
            if (s.last >= from) return {bytes.data() + s.pos, end - s.pos, s.base, next};
        }
        return {};
    }
};

// Task 6's members: the key index, TestReject{CapLiveKeys{cap}} (objectId 7) and TestReject{TagEquals{9}} (objectId 8).
struct VoteMembers { uint32_t cap = 3; };

// The key index, plus TestReject{TagEquals{9}} (objectId 7) when the set is constrained, or Task 6's members.  The pool and the
// batch overlay are bound, reserved for 4,096-record transactions.  start() leaves the judge at 10: with Task 6's members the
// tables are empty.
struct JudgeRig {
    static constexpr int32_t kRecordsMax = 4096;
    rune::codec::Descriptor       src = tagSource();
    PartitionWriteState           st;
    TxnLog                        log{st};
    VerdictSource                 vs{5, 0};
    TxnOverlayPool                pool;
    std::array<rune::view::TxnOverlay, 1> batch;
    std::unique_ptr<ViewInstance> index, reject, capped;
    rune::view::CompileResult     comp, capComp;
    ConstraintSet                 set;
    bool                          emptyStart = false;

    explicit JudgeRig(bool constrained) {
        addIndex();
        if (constrained) reject = tagMember(comp, 7, tagRule());
    }
    explicit JudgeRig(VoteMembers v) : emptyStart(true) {
        addIndex();
        TestRejectRule cap;
        cap.mode = TestRejectRule::Mode::CapLiveKeys;
        cap.cap  = v.cap;
        capped = tagMember(capComp, 7, cap);
        reject = tagMember(comp, 8, tagRule());
    }
    static TestRejectRule tagRule() {
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag  = 9;
        return rule;
    }
    void addIndex() {
        set.id = 1;
        index = std::make_unique<ViewInstance>(keyIndexPlan(5, src.find("id")->id), keyIndexRowLayout(), src, false, 0, 4096, kKeyIndexChunkBytes);
        vs.addVerdictView(index.get(), VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
        set.members[set.count++] = {0, VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint};
        // Per record: the key index 1 row, each projection 2.
        pool.reserveFor(std::size_t(kRecordsMax) * 5, 64);
        batch[0].reserve(std::size_t(kRecordsMax) * 5, 64);
        vs.bindTxn(&pool, batch, kRecordsMax);
    }
    std::unique_ptr<ViewInstance> tagMember(rune::view::CompileResult& c, uint64_t objectId, const TestRejectRule& rule) {
        const rune::codec::Descriptor* s[] = {&src};
        c = rune::view::compileView(tagView(), s, int32_t(objectId)).value();
        auto inst = std::make_unique<ViewInstance>(c.plan, c.rowLayout, src, false, 0, 8192, 1u << 16);
        vs.installTestVerdictViewForTest(inst.get(), objectId, rule);
        set.members[set.count++] = {objectId, VerdictViewKind::TestReject, rule.fingerprint()};
        return inst;
    }
    void start() {
        st.constraintSets.onChange(0, set);
        log.control();
        std::vector<Rec> fill;
        for (int64_t id = 100; id < 109; ++id) fill.push_back(emptyStart ? del(id) : Rec{id});
        log.data(fill);
        vs.bindState(&st);
        st.verdicts = &vs;
        vs.bindLeader(nullptr, 1, 0);
        drive();
        ASSERT_EQ(vs.judgeCursor(), 10);
    }
    // A read of exactly [failFrom, failTo) (any end when failTo is INT64_MAX) comes back empty failReads times; oneBatch reads return
    // one batch each.
    int64_t failFrom = -1, failTo = -1;
    int     failReads = 0;
    bool    oneBatch = false;
    int64_t countFrom = INT64_MAX;   // reads from at or above it are counted in counted
    int     counted = 0;
    int64_t countAt = -1;            // reads from exactly it are counted in countedAt
    int     countedAt = 0;
    int64_t countTo = -1;            // reads up to exactly it are counted in countedTo
    int     countedTo = 0;
    int64_t logStart = 0;            // a read from below it comes back empty, as the log's below its start
    int drive(int64_t hw = INT64_MAX, int64_t now = 0) {
        return vs.driveJudge([&](int64_t from, int64_t to) {
            counted += from >= countFrom;
            countedAt += from == countAt;
            countedTo += to == countTo;
            if (from < logStart) return rune::storage::LogReadView{};
            if (failReads > 0 && from == failFrom && (to == failTo || failTo == INT64_MAX)) {
                --failReads;
                return rune::storage::LogReadView{};
            }
            return log.read(from, oneBatch);
        }, std::min(hw, log.next), now, st);
    }
    // Drives until 77 is judged; false if it is not within `passes`.
    bool judge77(int passes = 16) {
        for (int i = 0; i < passes; ++i) {
            if (st.txn.find(77) != nullptr && st.txn.find(77)->judged) return true;
            drive();
        }
        return st.txn.find(77) != nullptr && st.txn.find(77)->judged;
    }
    [[nodiscard]] bool indexed(int64_t id) const { return index->table.get(uint64_t(id)) != nullptr; }
    [[nodiscard]] bool rowed(int64_t id) const { return reject->table.get(uint64_t(id)) != nullptr; }
    [[nodiscard]] int64_t tagOf(int64_t id) const {
        const uint8_t* row = reject->table.get(uint64_t(id));
        return row == nullptr ? -1 : rune::view::readSlot(row, comp.rowLayout, 1).i;
    }
    // Every member table's rows for ids 0..199, as bytes: a judgement at J changes none of them.
    [[nodiscard]] std::vector<std::vector<uint8_t>> tables() const {
        std::vector<std::vector<uint8_t>> out;
        for (const ViewInstance* inst : {index.get(), reject.get(), capped.get()}) {
            if (inst == nullptr) continue;
            out.push_back({uint8_t(inst->table.size())});
            for (uint64_t id = 0; id < 200; ++id)
                if (const uint8_t* row = inst->table.get(id)) out.emplace_back(row, row + inst->table.rowLen());
        }
        return out;
    }
    [[nodiscard]] std::size_t freeOverlays() {
        std::array<int32_t, rune::view::kTxnOverlaysPerWorker> got{};
        std::size_t n = 0;
        for (int32_t i; (i = pool.acquire(999)) >= 0;) got[n++] = i;
        for (std::size_t k = 0; k < n; ++k) pool.release(got[k]);
        return n;
    }
    [[nodiscard]] std::vector<int64_t> markerEnds() const {
        std::vector<int64_t> out;
        for (const auto& b : vs.appendedForTest()) {
            const auto c = cr::parseControl(b.data(), b.size());
            if (!c || c->type != cr::kVerdictMarker) continue;
            if (const auto m = cr::parseMarker(*c)) out.push_back(m->header.finalThrough);
        }
        return out;
    }
    // The offsets the appended verdict markers list.
    [[nodiscard]] std::vector<int64_t> markedOffsets() const {
        std::vector<int64_t> out;
        for (const auto& b : vs.appendedForTest()) {
            const auto c = cr::parseControl(b.data(), b.size());
            if (!c || c->type != cr::kVerdictMarker) continue;
            if (const auto m = cr::parseMarker(*c))
                for (uint32_t i = 0; i < m->header.count; ++i) out.push_back(m->offsetAt(i));
        }
        return out;
    }
};
}  // namespace

TEST(CheckedWritesTxnJudge, UndecidedTxnWithoutJudgeRecordWaitsAtItsFirstRecord) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.data({{5, 9}});
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_EQ(r.vs.judgedThrough(), 12);
    EXPECT_TRUE(r.indexed(2));
    EXPECT_FALSE(r.indexed(5)) << "offset 14 is not judged";
    EXPECT_FALSE(r.st.rejected.isRejected(14));
}

TEST(CheckedWritesTxnJudge, TxnWithACommittedJudgeIsDeferredAndTheJudgeRunsAhead) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.data({{5, 9}});
    r.log.judge(77, 12);
    r.drive(16);
    EXPECT_TRUE(r.st.rejected.isRejected(14)) << "the plain record past the transaction is judged";
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_EQ(r.vs.judgedThrough(), 12);
    EXPECT_FALSE(r.st.rejected.isRejected(12));
    EXPECT_FALSE(r.st.rejected.isRejected(13));
    EXPECT_FALSE(r.indexed(3));
    EXPECT_FALSE(r.indexed(4));
    EXPECT_FALSE(r.rowed(3));
    EXPECT_EQ(r.st.txn.find(77)->records, 2);
    EXPECT_EQ(r.st.judgedThrough, 12) << "the published position is judgedThrough()";
}

TEST(CheckedWritesTxnJudge, OrdinaryCommittedTxnIsAppliedAtItsMarker) {
    JudgeRig r(false);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.data({{5, 1, false, kInsert}});
    r.log.marker(77, true);
    r.drive(15);
    EXPECT_EQ(r.vs.judgeCursor(), 12) << "the marker is not committed: 77 is undecided";
    EXPECT_FALSE(r.indexed(5));
    r.drive(16);
    EXPECT_TRUE(r.indexed(3));
    EXPECT_TRUE(r.indexed(4));
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.vs.lastIntentForTest(), 0) << "14 (an insert) was applied before 77's records";
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_EQ(r.vs.judgedThrough(), 16);
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 0u);
}

// P11-3a: 77's delete of id 4 at 12 is older than the plain upsert at 14, so the key's row is the record at 14 (as after compaction).
TEST(CheckedWritesTxnJudgeRegression, SupersededOrdinaryRecordAppliesNothingRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{4}, {9}});
    r.log.data({del(4), {6}}, 77);
    r.log.data({{4}});
    r.log.marker(77, true);
    r.drive();
    EXPECT_TRUE(r.indexed(4)) << "the superseded delete applied nothing";
    EXPECT_TRUE(r.indexed(6));
    EXPECT_EQ(r.vs.judgeCursor(), 16);
}

// P11-3a: a record of another committed transaction applied before M supersedes too; an aborted one does not.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryRecordSupersededByAnEarlierCommittedTxnRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{4}, {8}});
    r.log.data({del(4), del(8)}, 77);
    r.log.data({{4}}, 88);
    r.log.data({{8}}, 99);
    r.log.marker(88, true);
    r.log.marker(99, false);
    r.log.marker(77, true);
    r.drive();
    EXPECT_TRUE(r.indexed(4)) << "88 committed below 77's marker";
    EXPECT_FALSE(r.indexed(8)) << "99 aborted: 77's delete applies";
    EXPECT_EQ(r.vs.judgeCursor(), 19);
}

TEST(CheckedWritesTxnJudge, AbortedOrdinaryTxnIsSkipped) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3, 9}, {4, 9}}, 77);
    r.log.data({{5}});
    r.log.marker(77, false);
    r.drive();
    EXPECT_FALSE(r.indexed(3));
    EXPECT_FALSE(r.indexed(4));
    EXPECT_FALSE(r.rowed(3));
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_EQ(r.vs.judgedThrough(), 16);
}

TEST(CheckedWritesTxnJudge, MarkersNeverPassAnUndecidedTxn) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.data({{5, 9}});
    r.log.judge(77, 12);
    r.log.data({{21, 9}});
    r.drive(INT64_MAX, 200'000'000);
    r.drive(INT64_MAX, 400'000'000);
    ASSERT_FALSE(r.markerEnds().empty());
    for (const int64_t end : r.markerEnds()) EXPECT_LE(end, 12);
    r.log.marker(77, true);
    r.drive(INT64_MAX, 2'000'000'000);
    EXPECT_EQ(r.markerEnds().back(), r.log.next) << "decided: a marker covers the run-ahead records judged before";
}

// A new leadership activates at K: K never passes the undecided 77, and the re-judge from K defers 77 again.
TEST(CheckedWritesTxnJudgeRegression, ActivationKNeverPassesAnUndecidedTxnRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.data({{5, 9}});
    r.log.judge(77, 12);
    r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_LE(r.st.rejected.coveredThrough(), 12);
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_FALSE(r.vs.stalled()) << "the run-ahead verdicts above K are judged again, not refused out of order";
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_EQ(r.vs.judgedThrough(), 12);
    EXPECT_EQ(r.st.txn.find(77)->records, 2) << "re-deferred once";
    EXPECT_TRUE(r.st.rejected.isRejected(14));
    EXPECT_EQ(r.st.rejected.size(), 1u);
    EXPECT_LE(r.st.rejected.coveredThrough(), 12);
}

// A decided ordinary transaction whose marker the judge has not reached yet holds judgedThrough (and so K) at its first record.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryTxnAwaitingItsMarkerHoldsJudgedThroughRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{3}, {4}}, 77);   // 10-11
    r.log.data({{5}}, 88);        // 12, open
    r.log.marker(77, true);       // 13
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    EXPECT_LE(r.st.rejected.coveredThrough(), 10);
    r.vs.bindLeader(nullptr, 2, 0);
    r.log.marker(88, true);       // 14
    r.drive();
    EXPECT_TRUE(r.indexed(3));
    EXPECT_TRUE(r.indexed(4));
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.vs.judgedThrough(), 15);
}

TEST(CheckedWritesTxnJudge, FullTxnTableWaitsNeverJudges) {
    JudgeRig r(true);
    r.start();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(r.st.txn.onJudgeRecord(100'000 + int64_t(i), {1000 + int64_t(i), 0, 50'000 + int64_t(i)}), nullptr);
    r.log.data({{1}, {2}});
    r.log.data({{3, 9}, {4}}, 77);
    r.log.judge(77, 12);   // the feed's onJudgeRecord is refused
    r.log.data({{5, 9}});
    ASSERT_EQ(r.st.txn.find(77), nullptr);
    r.drive();
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_FALSE(r.st.rejected.isRejected(12));
    EXPECT_FALSE(r.st.rejected.isRejected(15));
    EXPECT_FALSE(r.indexed(3));
    EXPECT_EQ(r.st.txn.stats.judgeRecordRefusals, 1u) << "one wait, counted once";
    r.st.txn.find(1000)->phase = TxnPhase::Finalized;
    r.drive();
    ASSERT_NE(r.st.txn.find(77), nullptr) << "the judge re-offered the TxnJudge";
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_TRUE(r.st.rejected.isRejected(15));
    EXPECT_FALSE(r.st.rejected.isRejected(12));
}

// F5: a producer's next TxnJudge is refused while its previous entry holds an overlay; the judge waits there, counted, and goes on
// once the overlay is released.
TEST(CheckedWritesTxnJudgeRegression, JudgeRecordBehindAnOverlayHolderWaitsAndIsCountedRegression) {
    JudgeRig r(true);
    r.start();
    DeferredTxn* old = r.st.txn.onJudgeRecord(5, {77, 0, 3});
    ASSERT_NE(old, nullptr);
    r.st.txn.onMarker(77, 8, true);
    old->phase   = TxnPhase::Judging;
    old->overlay = 0;
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.judge(77, 12);
    r.log.data({{5, 9}});
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_EQ(r.st.txn.find(77)->judgeOffset, 5);
    EXPECT_EQ(r.st.txn.stats.judgeRecordRefusals, 1u);
    old->overlay = -1;
    old->phase   = TxnPhase::Finalized;
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->judgeOffset, 14);
    EXPECT_EQ(r.vs.judgeCursor(), 16);
    EXPECT_EQ(r.vs.judgedThrough(), 12);
}

// I1: the feed drops the marker of a transaction whose TxnJudge it refused (no entry takes it).  After the re-offer, the marker is
// still found when it reaches the HW, so judgedThrough moves on.
TEST(CheckedWritesTxnJudgeRegression, ReofferedJudgeRecordGetsTheMarkerTheFeedDroppedRegression) {
    JudgeRig r(true);
    r.start();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(r.st.txn.onJudgeRecord(100'000 + int64_t(i), {1000 + int64_t(i), 0, 50'000 + int64_t(i)}), nullptr);
    r.log.data({{1}, {2}});            // 10-11
    r.log.data({{3}, {4}}, 77);        // 12-13
    r.log.judge(77, 12);               // 14, refused by the feed
    r.log.data({{5}});                 // 15
    r.log.marker(77, false);           // 16, dropped by the feed
    ASSERT_EQ(r.st.txn.find(77), nullptr);
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    r.st.txn.find(1000)->phase = TxnPhase::Finalized;
    r.drive(16);                       // the HW below the abort
    ASSERT_NE(r.st.txn.find(77), nullptr);
    EXPECT_LT(r.st.txn.find(77)->markerOffset, 0);
    EXPECT_EQ(r.vs.judgeCursor(), 16) << "deferred: the judge ran ahead";
    EXPECT_EQ(r.vs.judgedThrough(), 12);
    r.drive();                         // the HW above the abort
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, 16);
    EXPECT_FALSE(r.st.txn.find(77)->commit);
    EXPECT_EQ(r.vs.judgedThrough(), 17);
}

// I1: a marker that comes after the re-offer reaches the entry through the feed; it is not fed twice.
TEST(CheckedWritesTxnJudgeRegression, ReofferedJudgeRecordTakesALaterMarkerFromTheFeedRegression) {
    JudgeRig r(true);
    r.start();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(r.st.txn.onJudgeRecord(100'000 + int64_t(i), {1000 + int64_t(i), 0, 50'000 + int64_t(i)}), nullptr);
    r.log.data({{3}, {4}}, 77);        // 10-11
    r.log.judge(77, 10);               // 12, refused by the feed
    r.drive();
    r.st.txn.find(1000)->phase = TxnPhase::Finalized;
    r.drive();
    ASSERT_NE(r.st.txn.find(77), nullptr);
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    r.log.marker(77, true);            // 13, fed: the entry exists now
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, 13);
    r.drive();
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, 13);
    EXPECT_TRUE(r.st.txn.find(77)->commit);
    EXPECT_EQ(r.vs.judgedThrough(), 14);
}

// M4: the search for an owed marker reads at most kTxnProbeBytesPerPass per pass and resumes where it stopped.
TEST(CheckedWritesTxnJudgeRegression, OwedMarkerSearchKeepsTheProbeBudgetRegression) {
    JudgeRig r(false);
    r.start();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(r.st.txn.onJudgeRecord(100'000 + int64_t(i), {1000 + int64_t(i), 0, 500'000 + int64_t(i)}), nullptr);
    r.log.data({{3}, {4}}, 77);        // 10-11
    r.log.judge(77, 10);               // 12, refused by the feed
    std::vector<Rec> big;
    for (int64_t id = 0; id < 40'000; ++id) big.push_back({1'000'000 + id});
    r.log.data(big);
    const std::size_t firstBig = r.log.batches.size() - 1;
    r.log.data(big);
    for (const std::size_t b : {firstBig, firstBig + 1}) {
        const std::size_t end = b + 1 < r.log.batches.size() ? r.log.batches[b + 1].pos : r.log.bytes.size();
        ASSERT_GT(end - r.log.batches[b].pos, kTxnProbeBytesPerPass);
    }
    r.log.marker(77, true);            // dropped by the feed
    r.drive();
    r.st.txn.find(1000)->phase = TxnPhase::Finalized;
    r.drive();                         // re-offered: deferred, the marker owed
    ASSERT_NE(r.st.txn.find(77), nullptr);
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    r.drive();                         // the first big batch spends the budget
    EXPECT_LT(r.st.txn.find(77)->markerOffset, 0);
    r.drive();                         // the second
    EXPECT_LT(r.st.txn.find(77)->markerOffset, 0);
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, r.log.next - 1);
    EXPECT_EQ(r.vs.judgedThrough(), r.log.next);
}

// Re-review 3: a rescan resets every kept entry to AwaitJudge; the judge finalizes the ones a kept marker decided, so a full table
// can reuse them.
TEST(CheckedWritesTxnJudgeRegression, KeptMarkerFinalizesAfterARescanSoAFullTableReusesRegression) {
    JudgeRig r(true);
    r.start();
    std::vector<Rec> more;
    for (int64_t id = 1; id <= 40; ++id) more.push_back({id});
    r.log.data(more);   // 10-49
    r.drive();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        const int64_t pid = 1000 + int64_t(i);
        DeferredTxn* t = r.st.txn.onJudgeRecord(20, {pid, 0, 15});
        ASSERT_NE(t, nullptr);
        r.st.txn.onMarker(pid, 30, true);
        t->phase = TxnPhase::Finalized;
    }
    verdictFeedBegin(&r.st, 0, r.log.next);   // a rescan that keeps every entry and its marker
    ASSERT_EQ(r.st.txn.find(1000)->phase, TxnPhase::AwaitJudge);
    ASSERT_EQ(r.st.txn.onJudgeRecord(60, {5000, 0, 55}), nullptr) << "reset entries are live until the judge sees them";
    r.drive();
    EXPECT_EQ(r.st.txn.find(1000)->phase, TxnPhase::Finalized);
    EXPECT_NE(r.st.txn.onJudgeRecord(60, {5000, 0, 55}), nullptr);
}

// Re-review 3 and M2: a vote parked across a rescan is never answered from the old leadership's answer, nor with an unjudged
// default.  The rescan clears the answer; the vote waits until the new leadership judges 77 at its TxnJudge.
TEST(CheckedWritesTxnJudgeRegression, ParkedVoteWaitsAcrossARescanWithTheOldAnswerClearedRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.judge(77, 12);   // 14
    r.drive();
    DeferredTxn* t = r.st.txn.find(77);
    ASSERT_EQ(t->phase, TxnPhase::Locked);
    r.vs.releaseTxnOverlays(r.st.txn);
    t->phase  = TxnPhase::Finalized;   // the old leadership's no
    t->answer = {63, 7, 13};
    ASSERT_TRUE(r.st.txn.parkVote({1, 0, 77, 0, 14}));
    verdictFeedBegin(&r.st, 0, r.log.next);
    t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(t->answer.code, 0) << "the rescan cleared the old leadership's answer";
    EXPECT_FALSE(t->judged);
    std::vector<TxnPhase> drainedAt;
    std::vector<uint16_t> codes;
    const auto drain = [&] {
        return r.st.txn.drainAnswered([&](const ParkedVote&, const DeferredTxn* d) { drainedAt.push_back(d->phase); codes.push_back(d->answer.code); });
    };
    EXPECT_EQ(drain(), 0) << "AwaitJudge: the vote stays parked";
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    ASSERT_EQ(drain(), 1) << "judged again at J";
    EXPECT_EQ(drainedAt[0], TxnPhase::Locked);
    EXPECT_EQ(codes[0], 0) << "the new judgement's yes, never the pre-rescan answer";
    EXPECT_EQ(drain(), 0);
}

// F6: overlays are released before the leadership scan truncates the table, whether a rescan (new generation) or a reset
// (revalidate() on a demotion) started it, and on a re-activation; no pool slot leaks.
TEST(CheckedWritesTxnJudgeRegression, NoOverlayLeaksAcrossAResetOrARescanRegression) {
    ScanLog l;
    l.appendFiller(9, 0);
    l.appendJudge(77, 0);
    l.appendJudge(78, 0);
    PartitionWriteState s;
    VerdictSource vs{5, 0};
    TxnOverlayPool pool;
    pool.reserveFor(4, 8);
    vs.bindTxn(&pool, {}, 4);
    vs.bindState(&s);
    s.verdicts = &vs;
    RcState rc;
    rc.reserve(8);
    rc.setVerdictHooks(txnHooks(s));
    RcFacts f;
    f.leader = true; f.epoch = 1;
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    const auto hold = [&] {
        s.txn.find(77)->overlay = pool.acquire(1);
        s.txn.find(78)->overlay = pool.acquire(1);
        ASSERT_GE(s.txn.find(78)->overlay, 0);
    };
    const auto freeSlots = [&] {
        std::array<int32_t, rune::view::kTxnOverlaysPerWorker> got{};
        std::size_t n = 0;
        for (int32_t i; (i = pool.acquire(2)) >= 0;) got[n++] = i;
        for (std::size_t k = 0; k < n; ++k) pool.release(got[k]);
        return n;
    };
    hold();
    f.leadershipGeneration = 2;   // a rescan
    (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.scanning());
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(freeSlots(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(s.txn.find(77)->overlay, -1);
    hold();
    f.leader = false;
    (void)rc.revalidate(f);       // a reset, then the scan of the next leadership
    ASSERT_FALSE(rc.ready());
    f.leader = true;
    f.leadershipGeneration = 3;
    for (int i = 0; i < 16 && !rc.ready(); ++i) (void)rc.advance(f, &l.log, 1);
    ASSERT_TRUE(rc.ready());
    EXPECT_EQ(freeSlots(), rune::view::kTxnOverlaysPerWorker);
    hold();
    vs.bindLeader(nullptr, 4, 0);   // a re-activation without a scan
    vs.driveJudge([&](int64_t, int64_t) { return rune::storage::LogReadView{}; }, 0, 0, s);
    EXPECT_EQ(freeSlots(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(s.txn.find(78)->overlay, -1);
}

// ---- Task 6: judging at J, locks, finalize, the conflict rule, atomic batches, the marker-without-vote path ----
namespace {
constexpr uint8_t  kExisting = 2;
constexpr uint32_t kCapId = 0x00070001, kTagId = 0x00080001;
}  // namespace

TEST(CheckedWritesTxnJudge, YesVoteLocksAndChangesNoTable) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1}, {2}}, 77);   // 10-11
    r.log.judge(77, 10);          // 12
    const auto before = r.tables();
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->phase, TxnPhase::Locked);
    EXPECT_TRUE(t->judged);
    EXPECT_EQ(t->answer.code, 0);
    EXPECT_GE(t->overlay, 0);
    EXPECT_EQ(r.tables(), before);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    EXPECT_EQ(r.st.txn.stats.votesYes, 1u);
}

TEST(CheckedWritesTxnJudge, NoVoteCarriesTheFirstFailingRecord) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1}, {2, 9}, {3, 9}}, 77);   // 10-12
    r.log.judge(77, 10);
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    EXPECT_EQ(t->phase, TxnPhase::Finalized);
    EXPECT_EQ(t->answer.code, kViolation);
    EXPECT_EQ(t->answer.constraintId, kTagId);
    EXPECT_EQ(t->answer.failingOffset, 11);
    EXPECT_EQ(t->overlay, -1);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(r.st.txn.stats.votesNoConstraint, 1u);
    EXPECT_EQ(r.st.rejected.size(), 0u) << "a vote rejects no offset";
}

TEST(CheckedWritesTxnJudge, TwoCreatesOfOneNameInOneTxnVoteNo) {
    JudgeRig r(VoteMembers{1});
    r.start();
    r.log.data({{5}, {6}}, 77);   // 10-11
    r.log.judge(77, 10);
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    EXPECT_EQ(t->answer.code, kViolation);
    EXPECT_EQ(t->answer.constraintId, kCapId);
    EXPECT_EQ(t->answer.failingOffset, 11) << "the chain counted the first create";
}

TEST(CheckedWritesTxnJudge, TombstonesInATxnAreNeverRejected) {
    JudgeRig r(VoteMembers{0});
    r.start();
    r.log.data({del(1), del(2)}, 77);
    r.log.judge(77, 10);
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

TEST(CheckedWritesTxnJudge, LockedRowPausesThenJudgesAgainstTheFinalState) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 2}}, 77);                // 10
    r.log.judge(77, 10);                     // 11
    r.log.data({{9}});                       // 12
    r.log.data({{1, 3, false, kInsert}});    // 13, id 1 is locked by 77
    r.drive(INT64_MAX, 1'000);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    EXPECT_TRUE(r.rowed(9)) << "J + 1 was judged";
    EXPECT_EQ(r.vs.judgeCursor(), 13);
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    EXPECT_FALSE(r.rowed(1));
    EXPECT_EQ(r.st.txn.stats.lockPauses, 1u);
    r.drive(INT64_MAX, 5'000);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
    EXPECT_EQ(r.st.txn.stats.lockPauses, 1u) << "one pause, counted once";
    EXPECT_GE(r.st.txn.stats.foldPausedOnTxnLockNs, 4'000u);
    r.log.marker(77, true);                  // 14
    r.drive(INT64_MAX, 6'000);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(r.tagOf(1), 2) << "77 applied";
    ASSERT_NE(r.st.rejected.find(13), nullptr) << "judged against the state including 77";
    EXPECT_EQ(r.st.rejected.find(13)->code, 56);
    EXPECT_EQ(r.vs.judgeCursor(), 15);
    EXPECT_EQ(r.vs.judgedThrough(), 15);
}

TEST(CheckedWritesTxnJudge, AbortAfterYesReleasesTheOverlay) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 2}}, 77);
    r.log.judge(77, 10);
    r.log.data({{9}});
    r.log.data({{1, 3, false, kInsert}});
    r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 13);
    r.log.marker(77, false);
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_FALSE(r.st.rejected.isRejected(13)) << "judged against the old state";
    EXPECT_EQ(r.tagOf(1), 3);
    EXPECT_EQ(r.vs.judgeCursor(), 15);
}

TEST(CheckedWritesTxnJudge, CommitFinalizesBeforeTheCursorReachesTheMarker) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 2}}, 77);   // 10
    r.log.judge(77, 10);        // 11
    r.log.data({{1, 3}});       // 12, paused on 77's lock
    r.log.data({{20}});         // 13
    r.log.marker(77, true);     // 14
    r.drive(14);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked) << "the marker is not below the HW";
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    r.drive(15);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized) << "committed in the pass the marker fell below the HW";
    EXPECT_EQ(r.tagOf(1), 3) << "77 applied first, then the paused record";
    EXPECT_TRUE(r.rowed(20));
    EXPECT_EQ(r.vs.judgeCursor(), 15);
}

TEST(CheckedWritesTxnJudgeRegression, OutsideWriteNewerThanTheTxnsLastWriteVotesConflictRegression) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});          // 10-11
    r.log.data({{4}, {6}}, 77);      // 12-13
    r.log.data({{4}});               // 14
    r.log.data({{7}});               // 15
    r.log.judge(77, 12);             // 16
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    EXPECT_FALSE(r.st.rejected.isRejected(14));
    EXPECT_TRUE(r.rowed(4));
    EXPECT_EQ(t->phase, TxnPhase::Finalized);
    EXPECT_EQ(t->answer.code, 63);
    EXPECT_EQ(t->answer.constraintId, 0u);
    EXPECT_EQ(t->answer.failingOffset, 14);
    EXPECT_EQ(r.st.txn.stats.votesNoConflict, 1u);
}

// After the commit the view's id 4 is 77's record at 15, the latest record of id 4 in the log (what compaction keeps).
TEST(CheckedWritesTxnJudgeRegression, OutsideWriteOlderThanTheTxnsLastWriteVotesYesRegression) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});          // 10-11
    r.log.data({{4, 2}}, 77);        // 12
    r.log.data({{8}});               // 13
    r.log.data({{4, 3}});            // 14
    r.log.data({{4, 5}}, 77);        // 15
    r.log.judge(77, 12);             // 16
    r.drive();
    ASSERT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
    EXPECT_EQ(r.tagOf(4), 3);
    r.log.marker(77, true);          // 17
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(r.tagOf(4), 5);
}

TEST(CheckedWritesTxnJudge, RejectedOutsideWriteIsNoConflict) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{4}, {6}}, 77);
    r.log.data({{4, 9}});            // 14, rejected
    r.log.data({{7}});
    r.log.judge(77, 12);
    r.drive();
    EXPECT_TRUE(r.st.rejected.isRejected(14));
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

TEST(CheckedWritesTxnJudge, AtomicBatchOneBadRecordRejectsTheWholeBatch) {
    JudgeRig r(VoteMembers{});
    r.start();
    const auto before = r.tables();
    r.log.data({{1}, {2, 9}, {3}}, -1, true);   // 10-12
    {
        const auto& s = r.log.batches.back();
        const rune::protocol::RecordBatchReader rd(r.log.bytes.data() + s.pos, r.log.bytes.size() - s.pos);
        ASSERT_TRUE(rd.ensureValid()) << "the CRC covers the bit";
        ASSERT_TRUE(rd.attributes() & rune::protocol::batch::kAttrAtomic);
    }
    r.drive();
    for (const int64_t o : {10, 11, 12}) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, kViolation);
        EXPECT_EQ(e->constraintId, kTagId);
    }
    EXPECT_EQ(r.tables(), before);
    EXPECT_EQ(r.vs.judgeCursor(), 13);

    JudgeRig plain(VoteMembers{});
    plain.start();
    plain.log.data({{1}, {2, 9}, {3}});
    plain.drive();
    EXPECT_FALSE(plain.st.rejected.isRejected(10));
    EXPECT_TRUE(plain.st.rejected.isRejected(11));
    EXPECT_FALSE(plain.st.rejected.isRejected(12));
    EXPECT_TRUE(plain.rowed(1));
    EXPECT_TRUE(plain.rowed(3));
}

TEST(CheckedWritesTxnJudge, AtomicBatchPassesAppliesAllInOneStep) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1}, {2}, {3}}, -1, true);
    r.drive();
    EXPECT_TRUE(r.rowed(1));
    EXPECT_TRUE(r.rowed(2));
    EXPECT_TRUE(r.rowed(3));
    EXPECT_TRUE(r.indexed(3));
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
}

TEST(CheckedWritesTxnJudgeRegression, CommitMarkerWithoutJudgeJudgesAllOrNothingAtTheMarkerRegression) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2, 9}, {3}}, 77);   // 10-12, no TxnJudge
    r.log.data({{20}, {21}, {22}, {23}, {24}, {25}, {26}});   // 13-19
    r.log.marker(77, true);               // 20
    r.drive(INT64_MAX, 200'000'000);
    r.drive(INT64_MAX, 2'000'000'000);
    for (const int64_t o : {10, 11, 12}) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, kViolation);
        EXPECT_EQ(e->constraintId, kTagId) << "every record carries the first failure";
    }
    EXPECT_FALSE(r.rowed(1));
    EXPECT_FALSE(r.rowed(3));
    EXPECT_TRUE(r.rowed(26));
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
    EXPECT_EQ(r.vs.judgeCursor(), 21);
    const auto marked = r.markedOffsets();
    for (const int64_t o : {10, 11, 12}) EXPECT_NE(std::find(marked.begin(), marked.end(), o), marked.end()) << o;
}

TEST(CheckedWritesTxnJudge, NoFreeOverlayWaitsAtJ) {
    JudgeRig r(VoteMembers{});
    r.start();
    std::array<int32_t, rune::view::kTxnOverlaysPerWorker> held{};
    for (int32_t& i : held) ASSERT_GE(i = r.pool.acquire(4242), 0);
    r.log.data({{1}, {2, 9}}, 77);   // 10-11
    r.log.judge(77, 10);             // 12
    r.drive();
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(r.st.txn.stats.txnOverlayWaits, 1u) << "one wait, counted once";
    for (const int32_t i : held) r.pool.release(i);
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    EXPECT_EQ(t->answer.code, kViolation);
    EXPECT_EQ(t->answer.constraintId, kTagId);
    EXPECT_EQ(t->answer.failingOffset, 11);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
}

namespace {
// A 4,096-record 77 at 10, ~1 MiB of tombstone batches (16 passes at the test's 64 KiB scan bound), a plain upsert of one of 77's keys, then 77's TxnJudge.
void spanningLog(JudgeRig& r, std::size_t tailBytes = 1u << 20) {
    std::vector<Rec> txn;
    for (int64_t id = 0; id < JudgeRig::kRecordsMax; ++id) txn.push_back({10'000 + id});
    r.log.data(txn, 77);
    std::vector<Rec> dels;
    for (int64_t i = 0; i < 3000; ++i) dels.push_back(del(1'000'000 + i));
    const std::size_t from = r.log.bytes.size();
    while (r.log.bytes.size() - from < tailBytes) r.log.data(dels);
    r.log.data({{10'005}});
    r.log.judge(77, 10);
}
}  // namespace

TEST(CheckedWritesTxnJudge, JudgingSpansPassesWithABoundedRead) {
    JudgeRig bounded(VoteMembers{100'000});
    bounded.vs.setVoteScanBytesForTest(64u << 10);
    bounded.start();
    spanningLog(bounded);
    int judgingPasses = 0, passes = 0;
    for (; passes < 400 && !bounded.st.txn.find(77)->judged; ++passes) {
        bounded.drive();
        if (bounded.st.txn.find(77)->phase == TxnPhase::Judging) ++judgingPasses;
    }
    ASSERT_TRUE(bounded.st.txn.find(77)->judged);
    EXPECT_GT(judgingPasses, 2) << "the read and the scan resumed across passes";

    JudgeRig unbounded(VoteMembers{100'000});
    unbounded.vs.setVoteScanBytesForTest(SIZE_MAX);
    unbounded.start();
    spanningLog(unbounded);
    for (int i = 0; i < 400 && unbounded.st.txn.find(77)->phase != TxnPhase::Finalized; ++i) {
        unbounded.drive();
        EXPECT_NE(unbounded.st.txn.find(77)->phase, TxnPhase::Judging) << "one unbounded pass judges the whole transaction";
    }
    const TxnVoteAnswer& a = bounded.st.txn.find(77)->answer;
    const TxnVoteAnswer& b = unbounded.st.txn.find(77)->answer;
    EXPECT_EQ(a.code, 63);
    EXPECT_EQ(a.code, b.code);
    EXPECT_EQ(a.constraintId, b.constraintId);
    EXPECT_EQ(a.failingOffset, b.failingOffset);
    EXPECT_EQ(a.failingOffset, bounded.log.next - 2);
}

// The production per-pass scan budget: a log one batch past it is not judged in one pass.
TEST(CheckedWritesTxnJudgeRegression, DefaultPassBudgetBoundsTheJudgeReadRegression) {
    JudgeRig r(VoteMembers{100'000});
    r.start();
    spanningLog(r, kVoteScanBytesPerPass + (64u << 10));
    int passes = 0;
    for (; passes < 400 && !r.st.txn.find(77)->judged; ++passes) r.drive();
    ASSERT_TRUE(r.st.txn.find(77)->judged);
    EXPECT_GE(passes, 2) << "the default budget ended the first pass";
}

// F3: a chain paused on a lock mid-transaction re-chains from the first record; the answer equals an unpaused run's.
TEST(CheckedWritesTxnJudgeRegression, ChainPausedMidTransactionRestartsFromTheFirstRecordRegression) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{2}}, 66);                                            // 10
    r.log.judge(66, 10);                                              // 11
    r.log.data({{1, 1, false, kInsert}, {2, 1, false, kExisting}}, 77);   // 12-13: 13 is locked by 66
    r.log.judge(77, 12);                                              // 14
    r.drive();
    ASSERT_EQ(r.st.txn.find(66)->phase, TxnPhase::Locked);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Judging);
    EXPECT_FALSE(r.st.txn.find(77)->judged);
    EXPECT_EQ(r.vs.judgeCursor(), 14);
    EXPECT_EQ(r.st.txn.stats.lockPauses, 1u);
    r.log.marker(66, true);                                           // 15
    r.drive();
    ASSERT_TRUE(r.st.txn.find(77)->judged);

    JudgeRig u(VoteMembers{});
    u.start();
    u.log.data({{2}});
    u.log.data({{50}});
    u.log.data({{1, 1, false, kInsert}, {2, 1, false, kExisting}}, 77);
    u.log.judge(77, 12);
    u.drive();
    ASSERT_TRUE(u.st.txn.find(77)->judged);
    EXPECT_EQ(u.st.txn.stats.lockPauses, 0u);
    const TxnVoteAnswer& a = r.st.txn.find(77)->answer;
    const TxnVoteAnswer& b = u.st.txn.find(77)->answer;
    EXPECT_EQ(a.code, 0) << "the restarted chain saw 66's id 2, and its own insert of id 1 once";
    EXPECT_EQ(a.code, b.code);
    EXPECT_EQ(a.failingOffset, b.failingOffset);
    EXPECT_EQ(r.st.txn.find(77)->phase, u.st.txn.find(77)->phase);
}

// N2 / Task 7 (T7 review 1, F1): an ordinary transaction with more keys than the one-step set is applied in segments, never
// refused, and folds as the plain writes in log order would: a key another writer deletes after T's last write of it stays deleted
// (superseded), one T writes again after the delete is indexed, whichever segment holds it.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryTxnPastTheKeySetIsAppliedEqualToAPlainFoldRegression) {
    const auto write = [](JudgeRig& r, int64_t pid) {
        std::vector<Rec> txn;
        for (std::size_t i = 0; i < 2048; ++i) txn.push_back({20'000 + int64_t(i)});
        r.log.data(txn, pid);   // 10..2057
        r.log.data({del(20'005), del(20'007), del(21'500)});
        r.log.data({{20'005}}, pid);
        if (pid >= 0) r.log.marker(pid, true);
        r.log.data({{5}});
    };
    JudgeRig plain(false);
    plain.start();
    write(plain, -1);
    for (int i = 0; i < 4; ++i) plain.drive();
    JudgeRig r(false);
    r.start();
    ASSERT_EQ(r.pool.oneStep().keyCapacity(), 1024u);
    write(r, 77);
    for (int i = 0; i < 8 && r.vs.judgeCursor() < r.log.next; ++i) r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), r.log.next) << "passed the marker";
    EXPECT_EQ(r.vs.judgedThrough(), r.log.next);
    EXPECT_EQ(r.st.txn.stats.supersedeKeysOverflow, 1u);
    EXPECT_EQ(r.st.rejected.nextRejectedAtOrAfter(0), INT64_MAX) << "nothing refused";
    EXPECT_TRUE(r.indexed(20'005));
    EXPECT_FALSE(r.indexed(20'007));
    EXPECT_FALSE(r.indexed(21'500));
    for (int64_t id = 20'000; id < 20'000 + 2048; ++id) ASSERT_EQ(r.indexed(id), plain.indexed(id)) << id;
    EXPECT_TRUE(r.indexed(5));
}

// Task 7 (T7 review 1, F1): an ordinary transaction spanning more other producers' markers than the set holds is applied too.  The
// markers are noted in windows, and a batch whose decision lies past a window is looked up: a delete committed far into the span
// supersedes, one aborted far into it does not, and one committed past T's marker applies after T.  Budgeted (T7 re-review 2 carry):
// with one batch a walk per pass the marker refills resume too, and give the same answer.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryTxnPastTheMarkerListIsAppliedEqualToAPlainFoldRegression) {
    for (const bool budgeted : {false, true}) {
        SCOPED_TRACE(budgeted ? "budgeted" : "one pass");
        JudgeRig r(false);
        r.start();
        r.log.data({{1}, {2}, {3}, {4}}, 77);   // 10..13, its first records
        for (int64_t p = 0; p < 1100; ++p) {
            if (p == 5) r.log.data({del(2)}, 5001);
            if (p == 10) r.log.data({del(3)}, 5003);
            if (p == 1050) r.log.data({del(1)}, 5000);
            if (p == 1090) r.log.marker(5000, true);
            if (p == 1095) r.log.marker(5001, false);
            r.log.data({{30'000 + p}}, 1000 + p);
            r.log.marker(1000 + p, true);
        }
        r.log.data({del(4)});   // a plain delete, then T's last write of 4
        r.log.data({{4}}, 77);
        const int64_t marker = r.log.next;
        r.log.marker(77, true);
        r.log.marker(5003, true);
        if (budgeted) {
            r.oneBatch = true;
            r.vs.setVoteScanBytesForTest(1);
            r.countTo = marker;
        }
        for (int i = 0; i < (budgeted ? 40'000 : 40) && r.vs.judgeCursor() < r.log.next; ++i) {
            r.countedTo = 0;
            r.drive();
            if (budgeted && r.vs.judgeCursor() <= marker) ASSERT_LE(r.countedTo, 2) << "drive " << i;
        }
        EXPECT_EQ(r.vs.judgeCursor(), r.log.next);
        EXPECT_GT(r.vs.judgedThrough(), marker);
        EXPECT_EQ(r.st.txn.stats.supersedeKeysOverflow, 0u) << "the keys fit";
        EXPECT_EQ(r.st.rejected.nextRejectedAtOrAfter(0), INT64_MAX) << "nothing refused";
        EXPECT_FALSE(r.indexed(1)) << "5000's delete committed below T's marker supersedes T's insert";
        EXPECT_TRUE(r.indexed(2)) << "5001's delete aborted";
        EXPECT_FALSE(r.indexed(3)) << "5003's delete commits after T";
        EXPECT_TRUE(r.indexed(4)) << "T wrote 4 again after the plain delete";
        EXPECT_TRUE(r.indexed(30'000));
        EXPECT_TRUE(r.indexed(30'000 + 1099));
    }
}

// Task 7 (T6 re-review 2): on the judgement path the same overflow is a deterministic no: a marker list past its slots takes every
// other transaction as applied, so a record of one on T's key above T's last write answers TxnConflict.
TEST(CheckedWritesTxnJudgeRegression, JudgementPastTheMarkerListVotesADeterministicConflictRegression) {
    const auto run = [] {
        JudgeRig r(true);
        r.start();
        r.log.data({{1}}, 77);   // 10
        for (int64_t p = 0; p < 1100; ++p) {
            r.log.data({{30'000 + p}}, 1000 + p);
            r.log.marker(1000 + p, false);   // aborted: no conflict, were they tracked
        }
        r.log.data({{1}}, 999);   // an aborted writer of T's key, past the list
        r.log.marker(999, false);
        r.log.judge(77, 10);
        EXPECT_TRUE(r.judge77(64));
        return r.st.txn.find(77)->answer.code;
    };
    const uint16_t a = run();
    EXPECT_EQ(a, uint16_t(rune::protocol::client::ErrorCode::TxnConflict));
    EXPECT_EQ(run(), a);
}

// Re-review 2: the owed-marker search rotates its start each pass, so a marker far behind never starves another; across a
// re-activation too.
TEST(CheckedWritesTxnJudgeRegression, OwedMarkerSearchRotatesAcrossAReactivationRegression) {
    JudgeRig r(false);
    r.start();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i)
        ASSERT_NE(r.st.txn.onJudgeRecord(100'000 + int64_t(i), {1000 + int64_t(i), 0, 500'000 + int64_t(i)}), nullptr);
    r.log.data({{3}}, 77);            // 10
    r.log.judge(77, 10);              // 11, refused by the feed
    r.log.data({{5}}, 88);            // 12
    r.log.judge(88, 12);              // 13, refused
    std::vector<Rec> big;
    for (int64_t id = 0; id < 20'000; ++id) big.push_back({1'000'000 + id});
    r.log.data(big);
    r.log.marker(88, true);           // dropped by the feed
    const int64_t m88 = r.log.next - 1;
    for (int i = 0; i < 4; ++i) r.log.data(big);
    r.log.marker(77, true);           // dropped: without the rotation 77's search would take every pass until it is found
    r.drive();
    r.st.txn.find(1000)->phase = TxnPhase::Finalized;
    r.st.txn.find(1001)->phase = TxnPhase::Finalized;
    r.drive();
    ASSERT_NE(r.st.txn.find(77), nullptr);
    ASSERT_NE(r.st.txn.find(88), nullptr);
    r.vs.bindLeader(nullptr, 2, 0);
    for (int i = 0; i < 16 && r.st.txn.find(88)->markerOffset < 0; ++i) r.drive();
    EXPECT_EQ(r.st.txn.find(88)->markerOffset, m88);
    EXPECT_LT(r.st.txn.find(77)->markerOffset, 0) << "88's marker was found while 77's search was still behind";
    for (int i = 0; i < 16 && r.st.txn.find(77)->markerOffset < 0; ++i) r.drive();
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, r.log.next - 1);
}

// ---- Task 6 fix round 1 ----

// I1: whether a deferred q committed comes from the log, not from q's entry.  T's judgement spans passes; between them pid 66's next
// TxnJudge replaces q's Finalized entry.  q's record at 11 is above T's last write of id 4 and q committed: no, {63, 0, 11}.
TEST(CheckedWritesTxnJudgeRegression, DeferredQsDecisionSurvivesItsEntrysReplacementRegression) {
    JudgeRig r(VoteMembers{100});
    r.vs.setVoteScanBytesForTest(64u << 10);
    r.start();
    r.log.data({{4, 2}}, 77);   // 10
    r.log.data({{4, 3}}, 66);   // 11
    r.log.judge(66, 11);        // 12
    std::vector<Rec> dels;
    for (int64_t i = 0; i < 3000; ++i) dels.push_back(del(1'000'000 + i));
    const std::size_t from = r.log.bytes.size();
    while (r.log.bytes.size() - from < (1u << 20) + (64u << 10)) r.log.data(dels);
    r.log.judge(77, 10);        // J_77
    r.log.marker(66, true);     // q's commit, past J_77
    bool mid = false;
    for (int i = 0; i < 64 && !mid; ++i) {
        r.drive();
        mid = r.st.txn.find(66)->phase == TxnPhase::Finalized && r.st.txn.find(77)->phase == TxnPhase::Judging;
    }
    ASSERT_TRUE(mid) << "q finalized while T's judgement is in progress";
    const int64_t next66 = r.log.next;
    r.log.data({{70}}, 66);
    r.log.judge(66, next66);    // fed: replaces q's entry
    ASSERT_EQ(r.st.txn.find(66)->firstOffset, next66);
    ASSERT_TRUE(r.judge77(200));
    const TxnVoteAnswer& a = r.st.txn.find(77)->answer;
    EXPECT_EQ(a.code, 63);
    EXPECT_EQ(a.constraintId, 0u);
    EXPECT_EQ(a.failingOffset, 11);
}

// I2: P11-3a counts a deferred T0 judged below O's marker and committed past it.  O's tombstone of id 4 is older than T0's insert, so
// after both the key index holds id 4, as a plain fold (O at 13, then T0 at 20) does.
TEST(CheckedWritesTxnJudgeRegression, OrdinarySupersededByADeferredTxnCommittedPastItsMarkerRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({del(4)}, 55);                     // 10, O
    r.log.data({{4, 1, false, kInsert}}, 66);     // 11, T0
    r.log.judge(66, 11);                          // 12
    r.log.marker(55, true);                       // 13
    r.log.data({{20}, {21}, {22}, {23}, {24}, {25}});   // 14-19
    r.log.marker(66, true);                       // 20
    r.drive(20);
    EXPECT_EQ(r.st.txn.find(66)->phase, TxnPhase::Locked);
    EXPECT_EQ(r.vs.judgeCursor(), 13) << "O waits for T0's marker below the HW";
    r.drive();
    r.drive();
    EXPECT_EQ(r.st.txn.find(66)->phase, TxnPhase::Finalized);
    EXPECT_TRUE(r.indexed(4));
    EXPECT_TRUE(r.indexed(25));
    EXPECT_EQ(r.vs.judgeCursor(), 21);
}

// N2: while O waits for T0's marker past its own, the resolution walk resumes where it stopped and reads at most the vote scan
// budget per pass (one-batch reads, a budget of one batch), never the backlog from O's marker again.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryResolutionWalkIsBudgetedAndResumesRegression) {
    JudgeRig r(false);
    r.start();
    r.oneBatch = true;
    r.log.data({del(4)}, 55);                     // 10, O
    r.log.data({{4, 1, false, kInsert}}, 66);     // 11, T0
    r.log.judge(66, 11);                          // 12
    r.log.marker(55, true);                       // 13
    for (int64_t id = 30; id < 50; ++id) r.log.data({{id}});   // 14-33
    r.vs.setVoteScanBytesForTest(1);
    for (int i = 0; i < 16 && r.st.txn.find(66)->phase != TxnPhase::Locked; ++i) r.drive();
    ASSERT_EQ(r.st.txn.find(66)->phase, TxnPhase::Locked);
    ASSERT_EQ(r.vs.judgeCursor(), 13);
    r.countFrom = 14;
    for (int i = 0; i < 24; ++i) {
        r.counted = 0;
        r.drive();
        EXPECT_LE(r.counted, 1) << "pass " << i;
    }
    EXPECT_EQ(r.vs.judgeCursor(), 13) << "O still waits for T0's marker";
    r.log.marker(66, true);                       // 34
    r.countFrom = INT64_MAX;
    for (int i = 0; i < 16 && r.vs.judgeCursor() == 13; ++i) r.drive();   // one batch a walk a pass, the apply walk included
    EXPECT_GT(r.vs.judgeCursor(), 13);
    EXPECT_TRUE(r.indexed(4)) << "T0's insert supersedes O's delete";
}

// REGRESSION (T7 re-review 2 carry; Task 8 review 3, I-1): the intent, collect, supersede and apply walks of an ordinary transaction
// resume and share one byte budget per pass.  A pass once read the span three times, unbudgeted, and the apply walk read the whole
// segment, plain traffic included, in the pass that applied it.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryTxnSpanReadIsBudgetedPerPassRegression) {
    const auto write = [](JudgeRig& r, int64_t pid) {
        r.log.data({{1000}, {1001}, {1002}}, pid);   // 10..12
        for (int64_t id = 2000; id < 2040; ++id) {
            if (id == 2010) r.log.data({del(1001)});
            if (id == 2020) r.log.data({del(1002)});
            r.log.data({{id}});
        }
        r.log.data({{1002}}, pid);
        if (pid >= 0) r.log.marker(pid, true);
    };
    JudgeRig plain(false);
    plain.start();
    write(plain, -1);
    for (int i = 0; i < 4; ++i) plain.drive();
    JudgeRig r(false);
    r.start();
    write(r, 77);
    const int64_t marker = r.log.next - 1;
    const int64_t span = marker - 10;   // one record per batch past T's first: span - 2 batches
    r.oneBatch = true;
    r.vs.setVoteScanBytesForTest(1);
    r.countTo = marker;
    int drives = 0;
    for (; drives < 4 * span && r.vs.judgeCursor() < r.log.next; ++drives) {
        r.countedTo = 0;
        r.drive();
        EXPECT_LE(r.countedTo, 2) << "drive " << drives << ": one batch a walk";
    }
    EXPECT_EQ(r.vs.judgeCursor(), r.log.next);
    EXPECT_GT(drives, span) << "the collect and supersede walks resumed over passes";
    EXPECT_EQ(r.st.rejected.nextRejectedAtOrAfter(0), INT64_MAX);
    EXPECT_FALSE(r.indexed(1001)) << "the plain delete above T's last 1001 supersedes it";
    EXPECT_TRUE(r.indexed(1002)) << "T's last 1002 is above the plain delete";
    for (int64_t id = 0; id < 200; ++id) EXPECT_EQ(r.indexed(id), plain.indexed(id)) << id;
    for (int64_t id = 1000; id < 2040; ++id) EXPECT_EQ(r.indexed(id), plain.indexed(id)) << id;
}

// T7 review 2, N1: the one-step set is the worker's, shared by its sources.  O's segment is parked on T0's marker; another source
// applies its own ordinary transaction meanwhile and takes the set (review 4, I1).  O collects its keys anew, so T0's insert still
// supersedes O's delete.
TEST(CheckedWritesTxnJudgeRegression, SegmentResumedAfterAnotherSourceTookTheKeySetRecollectsRegression) {
    JudgeRig r(false);
    JudgeRig other(false);
    other.vs.bindTxn(&r.pool, other.batch, JudgeRig::kRecordsMax);
    r.start();
    other.start();
    r.log.data({del(4)}, 55);                     // 10, O
    r.log.data({{4, 1, false, kInsert}}, 66);     // 11, T0
    r.log.judge(66, 11);                          // 12
    r.log.marker(55, true);                       // 13
    r.log.data({{20}, {21}, {22}, {23}, {24}, {25}});   // 14-19
    r.log.marker(66, true);                       // 20
    r.drive(20);
    r.drive(20);   // the lookup the check stopped for scans to the HW
    ASSERT_EQ(r.vs.judgeCursor(), 13) << "O waits for T0's marker below the HW";
    ASSERT_TRUE(r.pool.oneStepHeld());
    other.log.data({{50}, {51}}, 88);
    other.log.marker(88, true);
    for (int i = 0; i < 4 && other.vs.judgeCursor() < other.log.next; ++i) other.drive();
    ASSERT_EQ(other.vs.judgeCursor(), other.log.next);
    ASSERT_TRUE(other.indexed(51));
    for (int i = 0; i < 8 && r.vs.judgeCursor() < r.log.next; ++i) r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), r.log.next);
    JudgeRig plain(false);
    plain.start();
    plain.log.data({del(4)});
    plain.log.data({{4, 1, false, kInsert}});
    plain.log.data({{20}, {21}, {22}, {23}, {24}, {25}});
    for (int i = 0; i < 4; ++i) plain.drive();
    EXPECT_TRUE(r.indexed(4)) << "T0's insert supersedes O's delete";
    for (int64_t id = 0; id < 200; ++id) EXPECT_EQ(r.indexed(id), plain.indexed(id)) << id;
}

namespace {
// An ordinary transaction 77 over 1,100 other producers' markers: 5000's delete of 1 lies past the marker window (a refill), 5001's
// aborted delete of 2 does not supersede.
void pastTheMarkerList(JudgeRig& r) {
    r.log.data({{1}, {2}, {3}}, 77);
    for (int64_t p = 0; p < 1100; ++p) {
        if (p == 5) r.log.data({del(2)}, 5001);
        if (p == 1050) r.log.data({del(1)}, 5000);
        if (p == 1090) r.log.marker(5000, true);
        if (p == 1095) r.log.marker(5001, false);
        r.log.data({{30'000 + p}}, 1000 + p);
        r.log.marker(1000 + p, true);
    }
    r.log.marker(77, true);
    r.log.data({{5}});
}
}  // namespace

// T7 review 3, N3: two sources of one worker, each in a segment that stops for a refill, never take the one-step set from each
// other: the holder keeps it until its transaction ends, the other waits.  Both reach the log's end equal to a lone source's fold.
TEST(CheckedWritesTxnJudgeRegression, TwoSourcesNeedingRefillsBothFinishRegression) {
    JudgeRig lone(false);
    lone.start();
    pastTheMarkerList(lone);
    for (int i = 0; i < 40 && lone.vs.judgeCursor() < lone.log.next; ++i) lone.drive();
    ASSERT_EQ(lone.vs.judgeCursor(), lone.log.next);
    ASSERT_FALSE(lone.indexed(1));
    ASSERT_TRUE(lone.indexed(2));
    JudgeRig a(false);
    JudgeRig b(false);
    b.vs.bindTxn(&a.pool, b.batch, JudgeRig::kRecordsMax);
    a.start();
    b.start();
    pastTheMarkerList(a);
    pastTheMarkerList(b);
    for (int i = 0; i < 80 && (a.vs.judgeCursor() < a.log.next || b.vs.judgeCursor() < b.log.next); ++i) {
        a.drive();
        b.drive();
    }
    EXPECT_EQ(a.vs.judgeCursor(), a.log.next);
    EXPECT_EQ(b.vs.judgeCursor(), b.log.next);
    EXPECT_FALSE(a.pool.oneStepHeld()) << "released at the end of the transaction";
    EXPECT_EQ(a.tables(), lone.tables());
    EXPECT_EQ(b.tables(), lone.tables());
}

// T7 review 3, N3: a source dropped (or demoted: the worker releases the claim of a judge it no longer drives) mid-segment gives
// the set back, and the source waiting for it proceeds.  So does one whose member is removed (review 4, I2): it waits for the
// install, not driven.  O's lookup reads one batch per pass, so it is not parked on T0's marker and keeps the set until then.
TEST(CheckedWritesTxnJudgeRegression, DroppedHolderReleasesTheOneStepSetRegression) {
    for (const int how : {0, 1, 2}) {
        SCOPED_TRACE(how == 0 ? "dropped" : how == 1 ? "demoted" : "view removed");
        JudgeRig r(false);
        JudgeRig other(false);
        other.vs.bindTxn(&r.pool, other.batch, JudgeRig::kRecordsMax);
        r.start();
        other.start();
        r.log.data({del(4)}, 55);                     // 10, O
        r.log.data({{4, 1, false, kInsert}}, 66);     // 11, T0
        r.log.judge(66, 11);                          // 12
        r.log.marker(55, true);                       // 13
        for (int64_t id = 30; id < 70; ++id) r.log.data({{id}});   // 14-53
        r.vs.setVoteScanBytesForTest(1);
        for (int i = 0; i < 16 && !r.pool.oneStepHeld(); ++i) r.drive();   // the intent search and the collect take a batch a pass
        ASSERT_EQ(r.vs.judgeCursor(), 13) << "O waits for T0's marker";
        ASSERT_TRUE(r.pool.oneStepHeld());
        other.log.data({{50}, {51}}, 88);
        const int64_t marker = other.log.next;
        other.log.marker(88, true);
        for (int i = 0; i < 4; ++i) other.drive();
        ASSERT_EQ(other.vs.judgeCursor(), marker) << "88 waits for the set";
        ASSERT_FALSE(other.indexed(51));
        if (how == 0) r.vs.releaseTxnOverlays(r.st.txn);
        else if (how == 1) r.vs.releaseOneStep();
        else {
            r.vs.removeVerdictView(r.index->plan.viewTopicId);
            r.drive();
            ASSERT_TRUE(r.vs.waitingForInstall());
        }
        EXPECT_FALSE(r.pool.oneStepHeld());
        for (int i = 0; i < 4 && other.vs.judgeCursor() <= marker; ++i) other.drive();
        EXPECT_GT(other.vs.judgeCursor(), marker);
        EXPECT_TRUE(other.indexed(51));
    }
}

// T7 review 4, M2: a capacity stall mid-segment gives the set back (a stalled judge is not driven), and the other source proceeds.
// The next leadership applies O whole.
TEST(CheckedWritesTxnJudgeRegression, StalledHolderReleasesTheOneStepSetRegression) {
    JudgeRig r(false);
    JudgeRig other(false);
    other.vs.bindTxn(&r.pool, other.batch, JudgeRig::kRecordsMax);
    r.start();
    other.start();
    r.log.data({{1}, {2}, {3}}, 55);              // 10-12, O
    r.log.marker(55, true);                       // 13
    r.vs.failCapacityAtForTest(11);
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_FALSE(r.pool.oneStepHeld());
    other.log.data({{50}, {51}}, 88);
    other.log.marker(88, true);
    for (int i = 0; i < 4 && other.vs.judgeCursor() < other.log.next; ++i) other.drive();
    EXPECT_EQ(other.vs.judgeCursor(), other.log.next);
    EXPECT_TRUE(other.indexed(51));
    r.vs.bindLeader(nullptr, 2, 0);
    for (int i = 0; i < 4 && r.vs.judgeCursor() < r.log.next; ++i) r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.judgeCursor(), r.log.next);
    EXPECT_TRUE(r.indexed(1));
    EXPECT_TRUE(r.indexed(3));
}

// T7 review 4, I1: T0 writes partitions A and B of one worker.  A's O waits for T0's marker, which waits for B's vote; B's 88 needs
// the one-step set before B's judge reaches T0's J.  B takes the set from A, parked on T0's marker, and judges the J; once T0's
// markers land A collects anew and both reach the log's end, A equal to a plain fold.
TEST(CheckedWritesTxnJudgeRegression, ParkedHolderLetsAnotherPartitionJudgeTheVoteItWaitsForRegression) {
    JudgeRig a(false);
    JudgeRig b(false);
    b.vs.bindTxn(&a.pool, b.batch, JudgeRig::kRecordsMax);
    a.start();
    b.start();
    a.log.data({del(4)}, 55);                     // 10, O
    a.log.data({{4, 1, false, kInsert}}, 66);     // 11, T0
    a.log.judge(66, 11);                          // 12
    a.log.marker(55, true);                       // 13
    a.drive();
    ASSERT_EQ(a.st.txn.find(66)->phase, TxnPhase::Locked);
    ASSERT_EQ(a.vs.judgeCursor(), 13) << "O waits for T0's marker";
    ASSERT_TRUE(a.pool.oneStepHeld());
    b.log.data({{50}, {51}}, 88);                 // 10-11, 88
    b.log.data({{60, 1, false, kInsert}}, 66);    // 12, T0
    b.log.marker(88, true);                       // 13
    b.log.judge(66, 12);                          // 14
    for (int i = 0; i < 8 && (b.st.txn.find(66) == nullptr || !b.st.txn.find(66)->judged); ++i) {
        b.drive();
        a.drive();
    }
    ASSERT_NE(b.st.txn.find(66), nullptr);
    EXPECT_TRUE(b.st.txn.find(66)->judged) << "B's vote for T0 is answerable while A waits";
    EXPECT_TRUE(b.indexed(51));
    EXPECT_EQ(a.vs.judgeCursor(), 13);
    a.log.marker(66, true);
    b.log.marker(66, true);
    for (int i = 0; i < 8 && (a.vs.judgeCursor() < a.log.next || b.vs.judgeCursor() < b.log.next); ++i) {
        a.drive();
        b.drive();
    }
    EXPECT_EQ(a.vs.judgeCursor(), a.log.next);
    EXPECT_EQ(b.vs.judgeCursor(), b.log.next);
    EXPECT_TRUE(b.indexed(60));
    EXPECT_FALSE(a.pool.oneStepHeld());
    JudgeRig plain(false);
    plain.start();
    plain.log.data({del(4)});
    plain.log.data({{4, 1, false, kInsert}});
    for (int i = 0; i < 4; ++i) plain.drive();
    EXPECT_TRUE(a.indexed(4)) << "T0's insert supersedes O's delete";
    EXPECT_EQ(a.tables(), plain.tables());
}

// T7 review 5, N1: the same cycle through a lock pause.  T0's judged write of key 4 lies below A's O, so O's apply pauses on T0's
// held row; the lock clears at T0's marker, which waits for B's vote.  B takes the set from the paused A and judges the J.
TEST(CheckedWritesTxnJudgeRegression, LockPausedHolderLetsAnotherPartitionJudgeTheVoteItWaitsForRegression) {
    JudgeRig a(false);
    JudgeRig b(false);
    b.vs.bindTxn(&a.pool, b.batch, JudgeRig::kRecordsMax);
    a.start();
    b.start();
    a.log.data({{4, 1, false, kInsert}}, 66);     // 10, T0
    a.log.judge(66, 10);                          // 11
    a.log.data({del(4)}, 55);                     // 12, O
    a.log.marker(55, true);                       // 13
    for (int i = 0; i < 4 && a.st.txn.stats.lockPauses == 0; ++i) a.drive();
    ASSERT_EQ(a.st.txn.find(66)->phase, TxnPhase::Locked);
    ASSERT_EQ(a.st.txn.stats.lockPauses, 1u) << "O's delete pauses on T0's row";
    ASSERT_EQ(a.vs.judgeCursor(), 13);
    ASSERT_TRUE(a.pool.oneStepHeld());
    b.log.data({{50}, {51}}, 88);                 // 10-11, 88
    b.log.data({{60, 1, false, kInsert}}, 66);    // 12, T0
    b.log.marker(88, true);                       // 13
    b.log.judge(66, 12);                          // 14
    for (int i = 0; i < 8 && (b.st.txn.find(66) == nullptr || !b.st.txn.find(66)->judged); ++i) {
        b.drive();
        a.drive();
    }
    ASSERT_NE(b.st.txn.find(66), nullptr);
    EXPECT_TRUE(b.st.txn.find(66)->judged) << "B's vote for T0 is answerable while A is paused";
    EXPECT_TRUE(b.indexed(51));
    EXPECT_EQ(a.vs.judgeCursor(), 13);
    a.log.marker(66, true);
    b.log.marker(66, true);
    for (int i = 0; i < 8 && (a.vs.judgeCursor() < a.log.next || b.vs.judgeCursor() < b.log.next); ++i) {
        a.drive();
        b.drive();
    }
    EXPECT_EQ(a.vs.judgeCursor(), a.log.next);
    EXPECT_EQ(b.vs.judgeCursor(), b.log.next);
    EXPECT_TRUE(b.indexed(60));
    EXPECT_FALSE(a.pool.oneStepHeld());
    JudgeRig plain(false);
    plain.start();
    plain.log.data({{4, 1, false, kInsert}});
    plain.log.data({del(4)});
    for (int i = 0; i < 4; ++i) plain.drive();
    EXPECT_FALSE(a.indexed(4)) << "O's delete applies after T0's insert";
    EXPECT_EQ(a.tables(), plain.tables());
}

// I3: accepted(r), an ordinary transaction committed below J.
TEST(CheckedWritesTxnJudge, OrdinaryCommittedBelowJConflicts) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});     // 10-11
    r.log.data({{4}}, 77);      // 12
    r.log.data({{4}}, 88);      // 13
    r.log.marker(88, true);     // 14
    r.log.judge(77, 12);        // 15
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 63);
    EXPECT_EQ(r.st.txn.find(77)->answer.failingOffset, 13);
}

TEST(CheckedWritesTxnJudge, OrdinaryAbortedBelowJIsNoConflict) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{4}}, 77);
    r.log.data({{4}}, 88);
    r.log.marker(88, false);
    r.log.judge(77, 12);
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

TEST(CheckedWritesTxnJudge, OrdinaryCommittedAboveJIsNoConflict) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}});     // 10-11
    r.log.data({{4}}, 77);      // 12
    r.log.data({{4}}, 88);      // 13
    r.log.judge(77, 12);        // 14
    r.log.marker(88, true);     // 15
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

// I3: accepted(r), a deferred q judged below J whose marker lies past J.
TEST(CheckedWritesTxnJudge, DeferredCommittedBelowJConflicts) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{4, 2}}, 77);   // 10
    r.log.data({{4, 3}}, 66);   // 11
    r.log.judge(66, 11);        // 12
    r.log.judge(77, 10);        // 13
    r.log.marker(66, true);     // 14
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 63);
    EXPECT_EQ(r.st.txn.find(77)->answer.failingOffset, 11);
}

TEST(CheckedWritesTxnJudge, DeferredAbortedBelowJIsNoConflict) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{4, 2}}, 77);
    r.log.data({{4, 3}}, 66);
    r.log.judge(66, 11);
    r.log.judge(77, 10);
    r.log.marker(66, false);
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

TEST(CheckedWritesTxnJudge, DeferredJudgedAboveJIsNoConflict) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{4, 2}}, 77);   // 10
    r.log.data({{4, 3}}, 66);   // 11
    r.log.judge(77, 10);        // 12
    r.log.judge(66, 11);        // 13
    r.log.marker(66, true);     // 14
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
}

// M1: the intent search's failed read leaves the transaction unclassified; the next pass finds the intent and judges it (P11-7).
TEST(CheckedWritesTxnJudgeRegression, IntentSearchReadFailureNeverClassifiesOrdinaryRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{1, 1, false, kInsert}, {100, 1, false, kInsert}}, 77);   // 10-11: id 100 exists
    r.log.marker(77, true);                                               // 12
    r.failFrom = 10;
    r.failTo = 12;
    r.failReads = 1;
    r.drive();
    r.drive();
    EXPECT_EQ(r.failReads, 0);
    for (const int64_t o : {10, 11}) {
        ASSERT_NE(r.st.rejected.find(o), nullptr) << o;
        EXPECT_EQ(r.st.rejected.find(o)->code, 56);
    }
    EXPECT_FALSE(r.indexed(1));
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
}

// M4: P11-7's pass path applies the transaction at its marker.
TEST(CheckedWritesTxnJudge, CommitMarkerWithoutJudgeAppliesAPassingTxn) {
    JudgeRig r(VoteMembers{100});
    r.start();
    r.log.data({{1}, {2}, {3}}, 77);   // 10-12
    r.log.marker(77, true);            // 13
    r.drive();
    EXPECT_TRUE(r.rowed(1));
    EXPECT_TRUE(r.rowed(3));
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
    EXPECT_EQ(r.vs.judgeCursor(), 14);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
}

// M4: P11-1 (b), the source is constrained at the marker (the set a change record between T's first record and its marker logs) though
// the set at T's first record is not.
TEST(CheckedWritesTxnJudge, ConstrainedSourceClassifiesAMarkerWithoutVote) {
    JudgeRig r(true);
    ConstraintSet later = r.set;
    later.id = 2;
    r.set.count--;   // the logged set has no TestReject member yet
    r.start();
    r.log.data({{1}, {2}}, 77);   // 10-11
    r.log.control();              // 12: the set change's record
    r.st.constraintSets.onChange(12, later);
    r.log.marker(77, true);       // 13
    r.drive();
    EXPECT_TRUE(r.indexed(1));
    EXPECT_TRUE(r.indexed(2));
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
}

// P11-7 is deterministic over the log: a set registered past T's marker (the node ran ahead of the judge) does not make T "should have
// voted"; every leader and every refold classifies it as an ordinary transaction.
TEST(CheckedWritesTxnJudgeRegression, MarkerClassificationIgnoresASetRegisteredPastTheMarkerRegression) {
    JudgeRig r(true);
    ConstraintSet later = r.set;
    later.id = 2;
    r.set.count--;
    r.start();
    r.log.data({{1}, {2, 9}}, 77);   // 10-11
    r.log.marker(77, true);          // 12
    r.log.control();                 // 13: a view is created after the marker
    r.st.constraintSets.onChange(13, later);
    ASSERT_TRUE(r.st.isConstrainedSource());
    r.drive();
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 0u);
    EXPECT_TRUE(r.indexed(1));
    EXPECT_TRUE(r.indexed(2));
    EXPECT_EQ(r.st.rejected.size(), 0u);
}

// The same registration ahead of the judge must not change the outcome: a marker judgement caps T's size (TxnTooLarge rejects every
// record), the ordinary apply has no cap, so a leader that classified T by the newest set would reject what another applies.
TEST(CheckedWritesTxnJudgeRegression, SetRegisteredPastTheMarkerNeverRejectsATooLargeUnvotedTxnRegression) {
    JudgeRig r(true);
    ConstraintSet later = r.set;
    later.id = 2;
    r.set.count--;
    r.vs.bindTxn(&r.pool, r.batch, 2);
    r.start();
    r.log.data({{1}, {2}, {3}}, 77);   // 10-12, over the cap of 2
    r.log.marker(77, true);            // 13
    r.log.control();                   // 14: a view is created after the marker
    r.st.constraintSets.onChange(14, later);
    r.drive();
    EXPECT_EQ(r.st.rejected.size(), 0u);
    for (const int64_t id : {1, 2, 3}) EXPECT_TRUE(r.indexed(id)) << id;
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 0u);
}

// L9 / Q4: a constrained member created after T's first record and before its marker (the coordinator lagged, so no vote was asked):
// P11-7 judges all of T at the marker under the set at the marker, and T's violating record is rejected.
TEST(CheckedWritesTxnJudgeRegression, MemberCreatedMidTransactionRejectsAtTheMarkerRegression) {
    JudgeRig r(true);
    const ConstraintSet constrained = r.set;
    r.set.count--;   // the logged set has no TestReject member yet
    r.start();
    r.log.data({{1}, {2, 9}, {3}}, 77);   // 10-12, written while the source was unconstrained
    ConstraintSet later = constrained;
    later.id = 2;
    r.log.control();                      // 13: the set change's record
    r.st.constraintSets.onChange(13, later);   // the member is created
    r.log.marker(77, true);               // 14, no TxnJudge
    r.drive();
    r.drive();
    for (const int64_t o : {10, 11, 12}) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, kViolation);
        EXPECT_EQ(e->constraintId, rune::view::constraintIdOf(7, 1));
    }
    EXPECT_FALSE(r.rowed(1));
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
}

// M4: P11-1 (c), an intent on a source with no constrained member.
TEST(CheckedWritesTxnJudge, IntentClassifiesAMarkerWithoutVote) {
    JudgeRig r(false);
    r.start();
    r.log.data({{1}, {100, 1, false, kInsert}}, 77);   // 10-11: id 100 exists
    r.log.marker(77, true);
    r.drive();
    EXPECT_TRUE(r.st.rejected.isRejected(10));
    EXPECT_EQ(r.st.rejected.find(11)->code, 56);
    EXPECT_FALSE(r.indexed(1));
    EXPECT_EQ(r.st.txn.stats.markerWithoutVote, 1u);
}

// M4: an atomic batch touching a locked row pauses; after the commit it is judged against the state including 77.
TEST(CheckedWritesTxnJudge, AtomicBatchPausesOnALockedRow) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 2}}, 77);                // 10
    r.log.judge(77, 10);                     // 11
    r.log.data({{5}, {1, 3}}, -1, true);     // 12-13
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), 12);
    EXPECT_EQ(r.st.txn.stats.lockPauses, 1u);
    EXPECT_FALSE(r.rowed(5));
    r.log.marker(77, true);                  // 14
    r.drive();
    EXPECT_TRUE(r.rowed(5));
    EXPECT_EQ(r.tagOf(1), 3);
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(r.vs.judgeCursor(), 15);
}

// M4: finalizeLocked_ resumes after a failed read where it stopped.
TEST(CheckedWritesTxnJudge, FinalizeResumesAfterAFailedRead) {
    JudgeRig r(VoteMembers{});
    r.oneBatch = true;
    r.start();
    r.log.data({{1}}, 77);      // 10
    r.log.data({{2}}, 77);      // 11
    r.log.judge(77, 10);        // 12
    r.log.marker(77, true);     // 13
    r.drive(13);
    ASSERT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    r.failFrom = 11;
    r.failTo = 12;
    r.failReads = 1;
    r.drive();
    EXPECT_EQ(r.failReads, 0);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    EXPECT_TRUE(r.rowed(1));
    EXPECT_FALSE(r.rowed(2));
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_TRUE(r.rowed(2));
    EXPECT_EQ(r.vs.judgeCursor(), 14);
}

// M2: a capacity stall in an atomic batch applies none of it; the next leadership judges and applies it whole.
TEST(CheckedWritesTxnJudgeRegression, AtomicStallAppliesNothingRegression) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 1, false, kInsert}, {2, 1, false, kInsert}, {3, 1, false, kInsert}}, -1, true);   // 10-12
    const auto before = r.tables();
    r.vs.failCapacityAtForTest(11);
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_EQ(r.tables(), before) << "a stall leaves no part of the batch applied";
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_TRUE(r.rowed(1));
    EXPECT_TRUE(r.rowed(2));
    EXPECT_TRUE(r.rowed(3));
    EXPECT_EQ(r.vs.judgeCursor(), 13);
}

// N1 case 1: after a stall in atomic batch B, a rescan truncates B away and another atomic batch lands at B's base.  It is judged
// in full: its constraint failure rejects it whole, no record of it is applied unjudged.
TEST(CheckedWritesTxnJudgeRegression, AtomicBatchAtAReusedBaseIsJudgedAnewRegression) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1, 1, false, kInsert}, {2, 1, false, kInsert}, {3, 1, false, kInsert}}, -1, true);   // 10-12
    r.vs.failCapacityAtForTest(11);
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    r.log.truncateFrom(10);
    verdictFeedBegin(&r.st, 0, 10);
    r.log.data({{4, 1, false, kInsert}, {5, 9, false, kInsert}}, -1, true);   // 10-11, id 5 fails TagEquals{9}
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_TRUE(r.st.rejected.isRejected(10));
    EXPECT_TRUE(r.st.rejected.isRejected(11));
    for (const int64_t id : {1, 2, 3, 4, 5}) {
        EXPECT_FALSE(r.indexed(id)) << id;
        EXPECT_FALSE(r.rowed(id)) << id;
    }
    EXPECT_EQ(r.vs.judgeCursor(), 12);
}

// N1 case 2: an undecided 77 holds K below atomic batch B, which stalls; the next activation judges from K < B, and a member
// rebuilt to K (simulated: B's rows removed) gets all of B, as a plain fold would.
TEST(CheckedWritesTxnJudgeRegression, AtomicBatchAboveKAppliesWholeToARebuiltMemberRegression) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{20}}, 77);                                                                              // 10
    r.log.judge(77, 10);                                                                                 // 11
    r.log.data({{1, 1, false, kInsert}, {2, 1, false, kInsert}, {3, 1, false, kInsert}}, -1, true);   // 12-14
    r.vs.failCapacityAtForTest(13);
    for (int i = 0; i < 8 && !r.vs.stalled(); ++i) r.drive();
    ASSERT_TRUE(r.vs.stalled());
    ASSERT_LE(r.st.rejected.coveredThrough(), 10);
    for (const int64_t id : {1, 2, 3}) {
        r.index->table.erase(uint64_t(id));
        r.reject->table.erase(uint64_t(id));
        r.capped->table.erase(uint64_t(id));
    }
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.st.rejected.size(), 0u);
    for (const int64_t id : {1, 2, 3}) {
        EXPECT_TRUE(r.indexed(id)) << id;
        EXPECT_TRUE(r.rowed(id)) << id;
    }
    EXPECT_EQ(r.vs.judgeCursor(), 15);
}

// M5: a key wider than any topic key changes no member row, so it never overflows the key set: no wait, no TxnTooLarge.
TEST(CheckedWritesTxnJudgeRegression, WideKeyNeverWedgesAnOrdinaryTxnRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{1}, {2, 1, false, 0, 20}}, 77);   // 10-11
    r.log.marker(77, true);                        // 12
    r.drive();
    EXPECT_TRUE(r.indexed(1));
    EXPECT_EQ(r.vs.judgeCursor(), 13);
    EXPECT_EQ(r.st.txn.stats.supersedeKeysOverflow, 0u);
}

TEST(CheckedWritesTxnJudgeRegression, WideKeyInATxnVotesYesRegression) {
    JudgeRig r(VoteMembers{});
    r.start();
    r.log.data({{1}, {2, 1, false, 0, 20}}, 77);
    r.log.judge(77, 10);
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
}

// ---- Task 7: the carried judge fixes ----

// T6 review 1: an abort before J decides the entry from the log at once (the judge meets the marker first): no overlay is taken,
// nothing is applied, K passes it, and the producer's next J is judged.
TEST(CheckedWritesTxnJudgeRegression, AbortBeforeJudgeIsDecidedAtTheMarkerRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}}, 77);   // 10, 11
    r.log.marker(77, false);      // 12
    r.log.judge(77, 10);          // 13: late
    r.log.data({{3}});            // 14
    r.drive();
    r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->phase, TxnPhase::Finalized);
    EXPECT_EQ(t->markerOffset, 12);
    EXPECT_FALSE(t->judged);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_FALSE(r.indexed(1));
    EXPECT_TRUE(r.indexed(3));
    EXPECT_EQ(r.vs.judgedThrough(), 15);
    r.log.data({{4}}, 77);        // 15: the producer's next transaction
    r.log.judge(77, 15);          // 16
    ASSERT_TRUE(r.judge77());
    EXPECT_EQ(r.st.txn.find(77)->judgeOffset, 16);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
}

// The same with the J fed after the judge passed the marker: judged at J, the chained read meets the producer's own marker first.
TEST(CheckedWritesTxnJudgeRegression, AbortBeforeJudgeFedLateIsDecidedAtJRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}}, 77);        // 10
    r.log.marker(77, false);      // 11
    r.log.data({{3}});            // 12
    r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 13);
    r.log.judge(77, 10);          // 13
    r.drive();
    r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(r.vs.judgedThrough(), 14);
    EXPECT_EQ(r.st.txn.stats.votesYes + r.st.txn.stats.votesNo, 0u) << "no judgement";
    // Task 7 (T7 review 1, F2): decided at its marker, so the producer's next transaction is judged at its J.
    EXPECT_EQ(r.st.txn.find(77)->markerOffset, 11);
    r.log.data({{5}}, 77);        // 14
    r.log.judge(77, 14);          // 15
    r.log.marker(77, true);       // 16
    for (int i = 0; i < 6 && r.vs.judgedThrough() < 17; ++i) r.drive();
    const DeferredTxn* t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->firstOffset, 14) << "the next TxnJudge replaced the decided entry";
    EXPECT_TRUE(t->judged);
    EXPECT_EQ(r.st.txn.stats.votesYes, 1u);
    EXPECT_EQ(r.st.txn.stats.undecidedReplaceRefusals, 0u);
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.vs.judgedThrough(), 17);
}

// A commit before its J: the records the judge passed as deferred are applied at that marker.
TEST(CheckedWritesTxnJudgeRegression, CommitBeforeJudgeAppliesAtTheMarkerRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{1}, {2}}, 77);
    r.log.marker(77, true);
    r.log.judge(77, 10);
    r.drive();
    r.drive();
    EXPECT_TRUE(r.indexed(1));
    EXPECT_TRUE(r.indexed(2));
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(r.vs.judgedThrough(), 14);
}

// T6 re-review 2: a TxnJudge decides a batch only for the batch's own transaction (its epoch, first <= base).
TEST(CheckedWritesTxnKeySet, AJudgeOfAnotherTransactionDecidesNothingRegression) {
    TxnKeySet k;
    k.reserve(8, 8);
    k.noteJudge(5, 0, 160, 200);   // a later transaction's J (first 160 > base 150)
    EXPECT_EQ(k.appliedAt(5, 0, 150), TxnKeySet::Applied::No) << "not the batch's J: no marker of its own is noted";
    k.clear();
    k.noteJudge(5, 1, 100, 200);   // another epoch's
    EXPECT_EQ(k.appliedAt(5, 0, 150), TxnKeySet::Applied::No);
    k.clear();
    k.noteJudge(5, 0, 100, 200);   // its own
    EXPECT_EQ(k.appliedAt(5, 0, 150), TxnKeySet::Applied::Unknown);
    k.noteMarker(5, 300, true);
    EXPECT_EQ(k.appliedAt(5, 0, 150), TxnKeySet::Applied::Yes);
}

// T6 review 1: bindTxn with a larger cap never wipes a key set a judgement is using; it grows once free.
TEST(CheckedWritesTxnPoolRegression, ReserveKeysNeverWipesASetInUseRegression) {
    TxnOverlayPool pool;
    pool.reserveFor(4, 8);
    pool.reserveKeys(16);
    const int32_t o = pool.acquire(1);
    ASSERT_GE(o, 0);
    ASSERT_TRUE(pool.keysAt(o).note("abc", 7));
    pool.reserveKeys(4096);
    EXPECT_TRUE(pool.keysPending());
    ASSERT_NE(pool.keysAt(o).find("abc"), nullptr) << "kept";
    EXPECT_EQ(pool.keysAt(o).keyCapacity(), 16u);
    EXPECT_EQ(pool.keysAt(o == 0 ? 1 : 0).keyCapacity(), 4096u) << "a free one grew";
    pool.release(o);
    pool.reserveKeys(0);
    EXPECT_FALSE(pool.keysPending());
    EXPECT_EQ(pool.keysAt(o).keyCapacity(), 4096u);
}

// Key sets deferred while in use grow at the next cold bindTxn once free (a judgement on a smaller set caps at what it holds).
TEST(CheckedWritesTxnJudgeRegression, BindTxnGrowsDeferredKeySetsOnceFreeRegression) {
    JudgeRig r(true);
    r.start();
    std::array<int32_t, rune::view::kTxnOverlaysPerWorker> held{};
    for (auto& h : held) h = r.pool.acquire(9);
    r.pool.reserveKeys(8192);   // all in use: deferred
    for (const int32_t h : held) r.pool.release(h);
    r.vs.bindTxn(&r.pool, r.batch, 8192);
    EXPECT_FALSE(r.pool.keysPending()) << "grown at the next cold call";
}

// T6 review 1: a member joining mid-judgement restarts it from T's first record.
TEST(CheckedWritesTxnJudgeRegression, MemberChangeRestartsTheJudgementRegression) {
    JudgeRig r(true);
    r.start();
    r.oneBatch = true;
    r.vs.setVoteScanBytesForTest(1);
    r.log.data({{1}}, 77);
    r.log.data({{2}}, 77);
    r.log.data({{3}}, 77);   // 10..12
    r.log.judge(77, 10);     // 13
    r.drive();
    r.drive();
    r.drive();
    r.countAt = 10;
    rune::view::CompileResult c;
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::TagEquals;
    rule.tag = 7;
    auto extra = r.tagMember(c, 9, rule);   // not in the logged set: it changes the members, not the verdicts
    ASSERT_TRUE(r.judge77(32));
    EXPECT_EQ(r.st.txn.find(77)->answer.code, 0);
    EXPECT_GT(r.countedAt, 0) << "read again from T's first record";
}

// Task 5 carry: a re-activation without a rescan discards a transient member that folded the run-ahead past K.
TEST(CheckedWritesTxnJudgeRegression, ReactivationDiscardsATransientMembersRunAheadRegression) {
    JudgeRig r(true);
    r.start();
    int discards = 0;
    r.vs.setViewHooks({&discards, nullptr, [](void* ctx, ViewInstance& inst, ViewDiscardReason) {
                           ++*static_cast<int*>(ctx);
                           inst.foldCursor = 0;
                       }, nullptr});
    r.log.data({{1}}, 77);   // 10
    r.log.judge(77, 10);     // 11
    r.log.data({{5}});       // 12
    r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 13);
    ASSERT_EQ(r.vs.judgedThrough(), 10);
    ASSERT_TRUE(r.indexed(5)) << "run-ahead";
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_EQ(discards, 2) << "both members held run-ahead rows";
}

// T5 N1 / Task 5 carry: a Locked entry reset by a rescan before its finalize keeps its apply.
TEST(CheckedWritesTxnJudgeRegression, ResetLockedEntryIsAppliedAfterARescanRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}}, 77);   // 10, 11
    r.log.judge(77, 10);          // 12
    r.log.data({{5}});            // 13
    r.log.marker(77, true);       // 14
    r.drive(14);                  // the marker is not committed yet
    ASSERT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    ASSERT_GT(r.vs.judgeCursor(), 12);
    verdictFeedBegin(&r.st, 0, r.log.next);   // a rescan that keeps everything
    verdictFeedMarker(&r.st, 77, 14, true);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    for (int i = 0; i < 6; ++i) r.drive();
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_TRUE(r.indexed(1));
    EXPECT_TRUE(r.indexed(2));
    EXPECT_EQ(r.vs.judgedThrough(), 15);
}

// Task 5 carry: with the HW as the bound, an empty set is passed only below the LSO.
TEST(CheckedWritesTxnJudgeRegression, EmptySetIsPassedOnlyBelowTheLsoRegression) {
    JudgeRig r(true);
    r.start();
    ConstraintSet empty;
    empty.id = 2;
    r.log.control();   // 10: the change to the empty set
    r.st.constraintSets.onChange(10, empty);
    r.log.data({{1}}, 77);   // 11: open
    r.log.data({{2}});       // 12
    r.vs.driveJudge([&](int64_t from, int64_t) { return r.log.read(from); }, r.log.next, 0, r.st, {}, 11);
    EXPECT_EQ(r.vs.judgeCursor(), 11) << "the open transaction holds an empty set's pass";
    r.vs.driveJudge([&](int64_t from, int64_t) { return r.log.read(from); }, r.log.next, 0, r.st, {}, r.log.next);
    EXPECT_EQ(r.vs.judgeCursor(), 13);
}

// T6 review 1: an atomic batch longer than a persisted member's cap is judged (it waited at the cap forever), and a commit at least
// the cap long ends its pass.
TEST(CheckedWritesTxnJudgeRegression, AtomicBatchPastTheCheckpointCapIsJudgedRegression) {
    JudgeRig r(VoteMembers{20});
    r.start();
    r.index->persisted = true;
    r.index->maxRecordsPerPass = 4;
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    std::vector<Rec> big;
    for (int64_t id = 1; id <= 10; ++id) big.push_back({id});
    r.log.data(big, -1, true);   // 10..19
    for (int i = 0; i < 4; ++i) r.drive(INT64_MAX, int64_t(i + 1) * 1'000'000'000);
    EXPECT_EQ(r.vs.judgeCursor(), 20);
    EXPECT_TRUE(r.rowed(10));
}

TEST(CheckedWritesTxnJudgeRegression, CommitPastTheCheckpointCapEndsItsPassRegression) {
    JudgeRig r(VoteMembers{20});
    r.start();
    r.index->persisted = true;
    r.index->maxRecordsPerPass = 4;
    VerdictViewHooks hooks;   // every checkpoint goes out: the run-ahead cap counts from the last one (P11-8 (a))
    hooks.checkpoint = [](void*, ViewInstance&) { return 0; };
    r.vs.setViewHooks(hooks);
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    std::vector<Rec> t;
    for (int64_t id = 1; id <= 6; ++id) t.push_back({id});
    r.log.data(t, 77);       // 10..15
    r.log.judge(77, 10);     // 16
    r.log.marker(77, true);  // 17
    r.log.data({{50}});      // 18
    for (int i = 0; i < 6 && r.vs.judgeCursor() < 17; ++i) r.drive(17, int64_t(i + 1) * 1'000'000'000);
    ASSERT_EQ(r.st.txn.find(77)->phase, TxnPhase::Locked);
    r.drive(INT64_MAX, 10'000'000'000);
    EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized);
    EXPECT_FALSE(r.rowed(50)) << "nothing more in the pass that applied it";
    r.drive(INT64_MAX, 11'000'000'000);
    EXPECT_TRUE(r.rowed(50));
}

// Task 7 (T7 review 1, F4): an atomic batch one record past txnJudgedRecordsMax, the bound its overlay is sized by, is rejected whole
// with 62 and the judge moves on; it used to stall on the overlay's capacity for good.  One at the knob is judged whole.
TEST(CheckedWritesTxnJudgeRegression, AtomicBatchPastTheKnobIsRejectedWholeNeverStallsRegression) {
    JudgeRig r(true);
    r.start();
    r.batch[0].reserve(3 * 3, r.batch[0].maxRowLen());   // 3 records over the key index (1 row) and the tag member (2)
    r.vs.bindTxn(&r.pool, r.batch, 3);
    r.log.data({{1}, {2}, {3}, {4}}, -1, true);   // 10..13: knob + 1
    r.log.data({{5}});                            // 14
    r.drive();
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.judgedThrough(), 15);
    for (int64_t o = 10; o < 14; ++o) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, uint16_t(rune::protocol::client::ErrorCode::TxnTooLarge));
    }
    EXPECT_FALSE(r.indexed(1));
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.st.txn.stats.atomicTooLarge, 1u);
    r.log.data({{6}, {7}, {8}}, -1, true);   // 15..17: at the knob
    r.drive();
    EXPECT_EQ(r.vs.judgedThrough(), 18);
    EXPECT_TRUE(r.indexed(6));
    EXPECT_TRUE(r.indexed(8));
    EXPECT_EQ(r.st.txn.stats.atomicTooLarge, 1u);
}

// REGRESSION (P11-0, D3, Task 8 step 6): a member joining at a set change A replays [0, A] unjudged.  A voted transaction T whose J
// lies below A and whose marker is not yet in the log is past the LSO: the joiner holds T's records in its fence, and the switch
// waits at A until T is decided; then the joiner applies T at J (or drops it), as the active members did.  Applying T at its own
// offsets left an aborted transaction's rows in the joiner, and showed a committed one before its marker.
TEST(CheckedWritesTxnJudgeRegression, SwitchSetJoinerNeverAppliesPastTheLsoRegression) {
    for (const bool commit : {false, true}) {
        JudgeRig r(true);
        const ConstraintSet base = r.set;
        rune::view::CompileResult joinComp;
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag  = 77;
        const auto joiner = r.tagMember(joinComp, 9, rule);
        ConstraintSet later = r.set;
        later.id = 2;
        r.set = base;
        r.start();
        r.log.data({{50}, {51}}, 77);   // 10-11: T
        r.log.data({{60}});             // 12
        r.log.judge(77, 10);            // 13: J
        r.st.constraintSets.onChange(14, later);
        r.log.control();                // 14: the joiner enters
        r.log.data({{61}});             // 15
        for (int i = 0; i < 8; ++i) r.drive();
        ASSERT_TRUE(r.st.txn.find(77) != nullptr && r.st.txn.find(77)->judged) << commit;
        EXPECT_TRUE(joiner->table.get(50) == nullptr) << commit << ": T is undecided";
        EXPECT_TRUE(joiner->table.get(51) == nullptr) << commit << ": T is undecided";
        EXPECT_GT(r.st.txn.stats.joinerTxnWaits, 0u) << commit << ": the wait is counted (review 2 m-d)";
        r.log.marker(77, commit);       // 16
        for (int i = 0; i < 8; ++i) r.drive();
        EXPECT_EQ(r.vs.judgeCursor(), 17) << commit;
        EXPECT_EQ(joiner->table.get(50) != nullptr, commit) << commit;
        EXPECT_EQ(joiner->table.get(51) != nullptr, commit) << commit;
        EXPECT_TRUE(joiner->table.get(60) != nullptr) << commit;
        EXPECT_TRUE(joiner->table.get(61) != nullptr) << commit;
        EXPECT_EQ(r.indexed(50), commit) << commit;
    }
}

// REGRESSION (T7 re-review 2, D3, Task 8 step 6): a replace candidate recovering K inside a voted transaction T (a run-ahead
// checkpoint: firstOffset < K < J) while T is undecided holds T's records past K in its fence and waits at J; at the decision it
// applies all of T from firstOffset (commit) or none of it (abort).  Before, it folded T's records past K at their own offsets
// before the backfill waited, and an abort left them in the candidate.
TEST(CheckedWritesTxnJudgeRegression, CandidateInsideAnUndecidedTxnWaitsAtItsJudgeRegression) {
    for (const bool commit : {false, true}) {
        JudgeRig r(true);
        r.start();
        r.log.data({{50}}, 77);   // 10: T
        r.log.data({{51}}, 77);   // 11: T
        r.log.data({{60}});       // 12
        r.log.judge(77, 10);      // 13: J
        r.log.data({{61}});       // 14
        for (int i = 0; i < 8; ++i) r.drive();
        ASSERT_TRUE(r.st.txn.find(77) != nullptr && r.st.txn.find(77)->judged) << commit;
        ASSERT_EQ(r.vs.judgeCursor(), 15) << commit;
        rune::view::CompileResult candComp;
        const rune::codec::Descriptor* s[] = {&r.src};
        candComp = rune::view::compileView(tagView(), s, 9).value();
        ViewInstance cand(candComp.plan, candComp.rowLayout, r.src, false, 0, 8192, 1u << 16);
        cand.foldCursor = 11;     // recovered inside T: the run-ahead checkpoint holds none of T's records
        r.vs.addCandidate(&cand, VerdictViewKind::TestReject, 9, 0x5eed);
        for (int i = 0; i < 8; ++i) r.drive();
        EXPECT_TRUE(cand.table.get(51) == nullptr) << commit << ": T is undecided";
        EXPECT_LE(cand.foldCursor, 13) << commit << ": the candidate waits at J";
        EXPECT_EQ(r.st.txn.stats.joinerTxnWaits, 0u) << commit << ": a candidate's wait is not a joiner's";
        r.log.marker(77, commit);  // 15
        for (int i = 0; i < 8; ++i) r.drive();
        EXPECT_EQ(cand.table.get(50) != nullptr, commit) << commit;
        EXPECT_EQ(cand.table.get(51) != nullptr, commit) << commit;
        EXPECT_TRUE(cand.table.get(60) != nullptr) << commit;
        EXPECT_TRUE(cand.table.get(61) != nullptr) << commit;
        EXPECT_EQ(r.st.txn.stats.checkpointBackfills, commit ? 1u : 0u) << commit;
        r.vs.removeCandidate(9, 0x5eed);
    }
}

// REGRESSION (Task 8 review 2, m-c): a backfill is counted once per transaction.  A candidate with a per-pass cap of 1 applies T at
// J over several passes, and every re-visit of J counted another backfill.
TEST(CheckedWritesTxnJudgeRegression, BackfillAppliedOverSeveralPassesCountsOnceRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{50}}, 77);   // 10: T
    r.log.data({{51}}, 77);   // 11: T
    r.log.data({{52}}, 77);   // 12: T
    r.log.data({{60}});       // 13
    r.log.judge(77, 10);      // 14: J
    r.log.marker(77, true);   // 15
    for (int i = 0; i < 8; ++i) r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 16);
    const rune::codec::Descriptor* s[] = {&r.src};
    const rune::view::CompileResult candComp = rune::view::compileView(tagView(), s, 9).value();
    ViewInstance cand(candComp.plan, candComp.rowLayout, r.src, false, 0, 8192, 1u << 16);
    cand.foldCursor = 11;      // recovered inside T
    cand.maxRecordsPerPass = 1;
    r.vs.addCandidate(&cand, VerdictViewKind::TestReject, 9, 0x5eed);
    for (int i = 0; i < 16 && cand.foldCursor < 16; ++i) r.drive();
    EXPECT_TRUE(cand.table.get(50) != nullptr && cand.table.get(51) != nullptr && cand.table.get(52) != nullptr);
    EXPECT_EQ(r.st.txn.stats.checkpointBackfills, 1u);
    r.vs.removeCandidate(9, 0x5eed);
}

// REGRESSION (Task 8 review 2, m-e): a member replay's fold stats reach the member's counters.  A failed read-back of a span at J
// was counted by the worker's fold only; path B dropped it, and never warned.
TEST(CheckedWritesTxnJudgeRegression, ReplayReadFailureIsCountedOnTheMemberRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{50}}, 77);   // 10: T
    r.log.data({{51}}, 77);   // 11: T
    r.log.data({{60}});       // 12
    r.log.judge(77, 10);      // 13: J
    r.log.marker(77, true);   // 14
    for (int i = 0; i < 8; ++i) r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 15);
    const rune::codec::Descriptor* s[] = {&r.src};
    const rune::view::CompileResult candComp = rune::view::compileView(tagView(), s, 9).value();
    ViewInstance cand(candComp.plan, candComp.rowLayout, r.src, false, 0, 8192, 1u << 16);
    cand.foldCursor = 11;     // recovered inside T: J reads T back from 10
    r.failFrom = 10;
    r.failTo = INT64_MAX;
    r.failReads = 2;          // the read and its retry to past J
    r.vs.addCandidate(&cand, VerdictViewKind::TestReject, 9, 0x5eed);
    r.drive();
    EXPECT_EQ(cand.counters.txnReadFailed, 1u);
    EXPECT_TRUE(cand.table.get(51) == nullptr);
    for (int i = 0; i < 8 && cand.foldCursor < 15; ++i) r.drive();
    EXPECT_TRUE(cand.table.get(50) != nullptr && cand.table.get(51) != nullptr) << "the next pass retries";
    EXPECT_EQ(cand.counters.txnReadFailed, 1u);
    r.vs.removeCandidate(9, 0x5eed);
}

// Task 8 review 2, m-f: an active member whose replay meets an undecided T at J drops T's span (D3) and gets T from
// finalizeLocked_, as every active member does; an abort leaves nothing of T.
TEST(CheckedWritesTxnJudgeRegression, ActiveMemberDropsAnUndecidedTxnAtJAndGetsItAtFinalizeRegression) {
    for (const bool commit : {false, true}) {
        JudgeRig r(true);
        r.start();
        r.log.data({{50}}, 77);   // 10: T
        r.log.data({{51}}, 77);   // 11: T
        r.log.judge(77, 10);      // 12: J
        r.log.data({{60}});       // 13
        for (int i = 0; i < 4; ++i) r.drive();
        ASSERT_EQ(r.vs.judgeCursor(), 14) << commit;
        r.reject->foldCursor = 10;   // behind the judge: its replay covers T and J
        r.drive();
        EXPECT_EQ(r.reject->foldCursor, 14) << commit << ": the replay passes J";
        EXPECT_EQ(r.reject->spanFence.count, 0) << commit << ": T's span is dropped at J";
        EXPECT_TRUE(r.reject->table.get(50) == nullptr && r.reject->table.get(51) == nullptr) << commit;
        r.log.marker(77, commit);    // 14
        for (int i = 0; i < 4; ++i) r.drive();
        EXPECT_EQ(r.st.txn.find(77)->phase, TxnPhase::Finalized) << commit;
        EXPECT_EQ(r.reject->table.get(50) != nullptr, commit) << commit;
        EXPECT_EQ(r.reject->table.get(51) != nullptr, commit) << commit;
        EXPECT_TRUE(r.reject->table.get(60) != nullptr) << commit;
        EXPECT_EQ(r.reject->spanFence.count, 0) << commit;
    }
}

// Task 8 review 2, m-f: a member behind the judge replays an ordinary T's batches below the judge's cursor, which opens T's span in
// its fence; endOrdinary_ closes it.  Left open it would veto the member's checkpoints for good (its cursor passes the marker).
TEST(CheckedWritesTxnJudgeRegression, OrdinaryApplicationClosesAReplayedSpanRegression) {
    JudgeRig r(false);
    r.start();
    r.log.data({{3}, {4}}, 77);   // 10-11
    r.log.data({{5}}, 88);        // 12, open
    r.log.marker(77, true);       // 13
    r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 12);
    r.index->foldCursor = 10;
    r.drive();
    ASSERT_EQ(r.index->foldCursor, 12);
    ASSERT_EQ(r.index->spanFence.count, 1) << "the replay holds 77's batches in the fence";
    r.log.marker(88, true);       // 14
    for (int i = 0; i < 4; ++i) r.drive();
    EXPECT_TRUE(r.indexed(3));
    EXPECT_TRUE(r.indexed(4));
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.index->foldCursor, 15);
    EXPECT_EQ(r.index->spanFence.count, 0);
    EXPECT_FALSE(r.index->spanFence.straddles(r.index->foldCursor));
}

// REGRESSION (Task 8 review 4, N-1; review 2 I-B, member half): retention cut a committed T's first batch (T.first < log start <= J).
// A member below the log start with rows is discarded once and refolds from the log start; at J its backfill starts at the log
// start, so T's retained records apply and the member catches up.  The backfill from T.first read below the log start, failed, and
// the open span straddling the log start discarded the member again at every J, for as long as J was retained.
TEST(CheckedWritesTxnJudgeRegression, BackfillBelowTheLogStartRefoldsOnceRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{50}}, 77);   // 10: T, cut by retention
    r.log.data({{51}}, 77);   // 11: T
    r.log.data({{60}});       // 12
    r.log.judge(77, 10);      // 13: J
    r.log.marker(77, true);   // 14
    r.log.data({{61}});       // 15
    for (int i = 0; i < 8; ++i) r.drive();
    ASSERT_EQ(r.vs.judgeCursor(), 16);
    int discards = 0;
    r.vs.setViewHooks({&discards, nullptr, [](void* ctx, ViewInstance& inst, ViewDiscardReason why) {
                           EXPECT_EQ(why, ViewDiscardReason::BelowLogStart);
                           ++*static_cast<int*>(ctx);
                           for (uint64_t id = 0; id < 200; ++id) (void)inst.table.erase(id);
                           inst.spanFence.clear();
                           inst.foldCursor = 0;
                       }, nullptr});
    const rune::codec::Descriptor* s[] = {&r.src};
    const rune::view::CompileResult candComp = rune::view::compileView(tagView(), s, 9).value();
    ViewInstance cand(candComp.plan, candComp.rowLayout, r.src, false, 0, 8192, 1u << 16);
    (void)cand.table.put(uint64_t(5));   // folded from records retention has since removed
    cand.foldCursor = 6;
    r.logStart = 11;
    r.vs.bindLeader(nullptr, 1, 11);
    r.vs.addCandidate(&cand, VerdictViewKind::TestReject, 9, 0x5eed);
    for (int i = 0; i < 16; ++i) r.drive();
    EXPECT_EQ(discards, 1) << "one discard, no repeated refold";
    EXPECT_EQ(cand.foldCursor, 16);
    EXPECT_EQ(cand.spanFence.count, 0);
    EXPECT_EQ(cand.counters.txnReadFailed, 0u);
    EXPECT_TRUE(cand.table.get(50) == nullptr) << "below the log start: gone";
    EXPECT_TRUE(cand.table.get(51) != nullptr) << "T's retained record applies at J";
    EXPECT_TRUE(cand.table.get(60) != nullptr && cand.table.get(61) != nullptr);
    r.vs.removeCandidate(9, 0x5eed);
}

// REGRESSION (Task 8 review 1, m1): a checkpoint push the span fence vetoes leaves nothing durable, so the run-ahead cap (P11-8 (a))
// still counts from the member's last real push.  A persisted candidate waiting at J inside T has every push vetoed; recording
// them as pushes let the judge run a further cap past a checkpoint that was never written.
TEST(CheckedWritesTxnJudgeRegression, VetoedPushNeverMovesTheRunAheadCapRegression) {
    JudgeRig r(true);
    r.start();
    VerdictViewHooks hooks;
    hooks.checkpoint = [](void*, ViewInstance&) { return 0; };
    r.vs.setViewHooks(hooks);
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    r.log.data({{50}}, 77);                              // 10: T
    for (int64_t id = 100; id < 120; ++id) r.log.data({{id}});   // 11..30
    r.log.judge(77, 10);                                 // 31: J
    r.log.data({{61}});                                  // 32
    for (int i = 0; i < 8; ++i) r.drive();
    ASSERT_TRUE(r.st.txn.find(77) != nullptr && r.st.txn.find(77)->judged);
    ASSERT_EQ(r.vs.judgeCursor(), 33);
    const rune::codec::Descriptor* s[] = {&r.src};
    rune::view::CompileResult candComp = rune::view::compileView(tagView(), s, 9).value();
    ViewInstance cand(candComp.plan, candComp.rowLayout, r.src, true, 0, 8192, 1u << 16);
    cand.maxRecordsPerPass = 8;
    r.vs.addCandidate(&cand, VerdictViewKind::TestReject, 9, 0x5eed);
    for (int i = 0; i < 20; ++i) r.drive();
    ASSERT_EQ(cand.foldCursor, 31) << "the candidate waits at J";
    for (int64_t id = 200; id < 240; ++id) r.log.data({{id}});   // 33..72
    for (int i = 0; i < 40; ++i) r.drive();
    EXPECT_LT(r.vs.judgeCursor(), 31 + 8) << "the cap counts from the candidate's last real push, below T";
    r.vs.removeCandidate(9, 0x5eed);
}

// 12b: a replicated atomic batch past txnJudgedRecordsMax needs one index entry per record.  With fewer free than that the judge
// waits on a retryable stall, not a permanent one; the worker's growth reaches size + n in one step (not just 2x the capacity), and
// the retry rejects the whole batch.  Nothing is held while it waits.
TEST(CheckedWritesTxnJudgeRegression, AtomicRejectionLargerThanTheFreeIndexGrowsItRegression) {
    JudgeRig r(true);
    r.st.rejected.reserve(1);
    r.start();
    r.batch[0].reserve(3 * 3, r.batch[0].maxRowLen());
    r.vs.bindTxn(&r.pool, r.batch, 3);
    r.log.data({{1}, {2}, {3}, {4}}, -1, true);   // 10..13: knob + 1, 4 entries into a capacity of 1
    r.log.data({{5}});                            // 14
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_TRUE(r.st.growOwed);
    EXPECT_EQ(r.st.growNeed, 4u);
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_FALSE(r.pool.oneStepHeld());
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
    EXPECT_EQ(r.vs.judgeCursor(), 10);
    r.st.growRejected();
    r.vs.unstall();
    EXPECT_FALSE(r.st.growOwed);
    EXPECT_EQ(r.st.growNeed, 0u);
    ASSERT_FALSE(r.vs.stalled());
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.judgedThrough(), 15);
    for (int64_t o = 10; o < 14; ++o) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, uint16_t(rune::protocol::client::ErrorCode::TxnTooLarge));
    }
    EXPECT_TRUE(r.indexed(5));
    EXPECT_EQ(r.st.txn.stats.atomicTooLarge, 1u);
}

// 12b: the same for P11-7's rejection of an unvoted transaction at its marker.
TEST(CheckedWritesTxnJudgeRegression, UnvotedTxnRejectionLargerThanTheFreeIndexGrowsItRegression) {
    JudgeRig r(VoteMembers{100});
    r.st.rejected.reserve(1);
    r.start();
    r.log.data({{1}, {2, 9}, {3}}, 77);   // 10-12, no TxnJudge
    r.log.marker(77, true);               // 13
    r.drive(INT64_MAX, 200'000'000);
    r.drive(INT64_MAX, 2'000'000'000);
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_TRUE(r.st.growOwed);
    EXPECT_EQ(r.st.growNeed, 3u);
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_FALSE(r.pool.oneStepHeld());
    r.st.growRejected();
    EXPECT_FALSE(r.st.growOwed);
    EXPECT_EQ(r.st.growNeed, 0u);
    r.vs.unstall();
    ASSERT_FALSE(r.vs.stalled());
    r.drive(INT64_MAX, 3'000'000'000);
    r.drive(INT64_MAX, 4'000'000'000);
    EXPECT_FALSE(r.vs.stalled());
    for (const int64_t o : {10, 11, 12}) {
        const auto* e = r.st.rejected.find(o);
        ASSERT_NE(e, nullptr) << o;
        EXPECT_EQ(e->code, kViolation);
    }
    EXPECT_FALSE(r.rowed(1));
    EXPECT_EQ(r.vs.judgeCursor(), 14);
    EXPECT_EQ(r.freeOverlays(), rune::view::kTxnOverlaysPerWorker);
}

// 12b fix: a rejection larger than the index's ceiling can never fit: growth and unstall leave the judge stalled.
TEST(CheckedWritesTxnJudgeRegression, RejectionLargerThanTheIndexCeilingStaysStalledRegression) {
    JudgeRig r(true);
    r.st.rejected.reserve(1);
    r.st.rejectedMax = 3;
    r.start();
    r.batch[0].reserve(3 * 3, r.batch[0].maxRowLen());
    r.vs.bindTxn(&r.pool, r.batch, 3);
    r.log.data({{1}, {2}, {3}, {4}}, -1, true);   // 10..13: knob + 1, 4 entries against a ceiling of 3
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_FALSE(r.st.growOwed);
    r.st.growRejected();
    r.vs.unstall();
    r.drive();
    EXPECT_TRUE(r.vs.stalled());
    EXPECT_EQ(r.st.rejected.size(), 0u);
    EXPECT_EQ(r.st.rejected.capacity(), 1u);
    EXPECT_EQ(r.vs.judgeCursor(), 10);
}

// 12b fix: a rejection that fits the ceiling alone but not beside the entries held waits for retention, not for good: growth clamps at
// the ceiling, the judge stays stalled while the room is short (no stall/unstall loop), and resumes once retention frees it.
TEST(CheckedWritesTxnJudgeRegression, RejectionBesideHeldEntriesWaitsForRetentionRegression) {
    JudgeRig r(true);
    r.st.rejected.reserve(3);
    r.st.rejectedMax = 5;
    r.start();
    r.batch[0].reserve(3 * 3, r.batch[0].maxRowLen());
    r.vs.bindTxn(&r.pool, r.batch, 2);
    r.log.data({{1}, {2}, {3}}, -1, true);   // 10..12: fills the capacity of 3
    r.log.data({{4}, {5}, {6}}, -1, true);   // 13..15: needs 6 against a ceiling of 5
    r.drive();
    ASSERT_TRUE(r.vs.stalled());
    EXPECT_EQ(r.st.rejected.size(), 3u);
    EXPECT_EQ(r.st.growNeed, 5u) << "clamped to the ceiling";
    r.st.growRejected();
    EXPECT_EQ(r.st.rejected.capacity(), 5u);
    r.vs.unstall();
    EXPECT_TRUE(r.vs.stalled()) << "2 free of the 3 it waits for";
    r.st.rejected.dropBelow(11);
    r.vs.unstall();
    ASSERT_FALSE(r.vs.stalled());
    r.drive();
    EXPECT_FALSE(r.vs.stalled());
    for (int64_t o = 13; o < 16; ++o) EXPECT_NE(r.st.rejected.find(o), nullptr) << o;
    EXPECT_EQ(r.vs.judgedThrough(), 16);
}
