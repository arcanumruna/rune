// Part 11 Task 4: TxnJudgeState, the TxnJudge and marker feed, the overlay pool.
#include <gtest/gtest.h>

#include <array>

#include <rune/node/VerdictSource.h>

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
    EXPECT_EQ(t.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(t.earliestUndecidedFirst(), INT64_MAX);
    t.onMarker(99, 530, false);
    EXPECT_EQ(t.stats.markerWithoutVote, 1u);
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
