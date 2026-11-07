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
    t.find(77)->judged = true;
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

// N2: a Finalized entry whose vote is still parked is not reused.
TEST(CheckedWritesTxnState, FullTableKeepsAFinalizedEntryWithAParkedVoteRegression) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        DeferredTxn* e = t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)});
        ASSERT_NE(e, nullptr);
        e->phase = TxnPhase::Finalized;
        e->judged = true;
    }
    ASSERT_TRUE(t.parkVote({1, 0, 0, 1}));
    ASSERT_NE(t.onJudgeRecord(5000, {5000, 1, 4000}), nullptr);
    EXPECT_NE(t.find(0), nullptr);
    EXPECT_EQ(t.find(1), nullptr) << "the next finalized entry went instead";
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 1);
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

// T5 fix-1 M2: a vote is drained only with the judge's answer; an entry finalized without a judgement keeps it parked.
TEST(CheckedWritesTxnState, DrainNeverAnswersWithAnUnjudgedDefaultRegression) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    t.onMarker(77, 520, true);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3}));
    t.finalizeDecided(600, 600);
    ASSERT_EQ(t.find(77)->phase, TxnPhase::Finalized);
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 0);
    t.find(77)->judged = true;
    t.truncateFrom(510);   // a rescan judges the entry anew
    EXPECT_FALSE(t.find(77)->judged);
    t.find(77)->phase = TxnPhase::Finalized;
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 0);
    t.find(77)->judged = true;
    EXPECT_EQ(t.drainAnswered([](const ParkedVote&, const DeferredTxn&) {}), 1);
}

TEST(CheckedWritesTxnState, DrainAnsweredTakesLockedAndFinalized) {
    auto t = reserved();
    ASSERT_NE(t.onJudgeRecord(500, {77, 3, 400}), nullptr);
    ASSERT_NE(t.onJudgeRecord(600, {78, 3, 450}), nullptr);
    ASSERT_TRUE(t.parkVote({1, 0, 77, 3}));
    ASSERT_TRUE(t.parkVote({2, 0, 78, 3}));
    t.find(77)->phase = TxnPhase::Locked;
    t.find(77)->judged = true;
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

struct Rec { int64_t id; int32_t tag = 1; bool tombstone = false; uint8_t intent = 0; };
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
        }
        for (const Rec& r : recs) {
            std::vector<uint8_t> k, v;
            put64(k, uint64_t(r.id));
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
    rune::storage::LogReadView read(int64_t from) const {
        for (const Span& s : batches)
            if (s.last >= from) return {bytes.data() + s.pos, bytes.size() - s.pos, s.base, next};
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
    int drive(int64_t hw = INT64_MAX, int64_t now = 0) {
        return vs.driveJudge([&](int64_t from, int64_t) { return log.read(from); }, std::min(hw, log.next), now, st);
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
    ASSERT_TRUE(r.st.txn.parkVote({1, 0, 77, 0}));
    verdictFeedBegin(&r.st, 0, r.log.next);
    t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(t->answer.code, 0) << "the rescan cleared the old leadership's answer";
    EXPECT_FALSE(t->judged);
    std::vector<TxnPhase> drainedAt;
    std::vector<uint16_t> codes;
    const auto drain = [&] {
        return r.st.txn.drainAnswered([&](const ParkedVote&, const DeferredTxn& d) { drainedAt.push_back(d.phase); codes.push_back(d.answer.code); });
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
// A 4,096-record 77 at 10, ~8 MiB of tombstone batches, a plain upsert of one of 77's keys, then 77's TxnJudge.
void spanningLog(JudgeRig& r) {
    std::vector<Rec> txn;
    for (int64_t id = 0; id < JudgeRig::kRecordsMax; ++id) txn.push_back({10'000 + id});
    r.log.data(txn, 77);
    std::vector<Rec> dels;
    for (int64_t i = 0; i < 3000; ++i) dels.push_back(del(1'000'000 + i));
    const std::size_t from = r.log.bytes.size();
    while (r.log.bytes.size() - from < (8u << 20)) r.log.data(dels);
    r.log.data({{10'005}});
    r.log.judge(77, 10);
}
}  // namespace

TEST(CheckedWritesTxnJudge, JudgingSpansPassesWithABoundedRead) {
    JudgeRig bounded(VoteMembers{100'000});
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

// N2: an ordinary transaction with more keys than the P11-3a scan tracks waits at its marker, counted once; it never applies a
// record the scan could not check.
TEST(CheckedWritesTxnJudgeRegression, OrdinaryTxnPastTheKeySetWaitsNeverMisappliesRegression) {
    JudgeRig r(false);
    r.start();
    const std::size_t n = r.pool.oneStep().keyCapacity() + 1;
    std::vector<Rec> txn;
    for (std::size_t i = 0; i < n; ++i) txn.push_back({20'000 + int64_t(i)});
    r.log.data(txn, 77);   // 10..
    const int64_t marker = r.log.next;
    r.log.marker(77, true);
    r.drive();
    r.drive();
    EXPECT_EQ(r.vs.judgeCursor(), marker) << "waits at the marker";
    EXPECT_EQ(r.vs.judgedThrough(), 10);
    EXPECT_FALSE(r.indexed(20'000));
    EXPECT_FALSE(r.indexed(20'000 + int64_t(n) - 1));
    EXPECT_EQ(r.st.txn.stats.supersedeKeysOverflow, 1u);
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
    for (int64_t id = 0; id < 40'000; ++id) big.push_back({1'000'000 + id});
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
