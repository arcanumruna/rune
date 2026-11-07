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

// N2: a Finalized entry whose vote is still parked is not reused.
TEST(CheckedWritesTxnState, FullTableKeepsAFinalizedEntryWithAParkedVoteRegression) {
    auto t = reserved();
    for (std::size_t i = 0; i < TxnJudgeState::kMaxTxns; ++i) {
        DeferredTxn* e = t.onJudgeRecord(static_cast<int64_t>(i) + 10, {static_cast<int64_t>(i), 1, static_cast<int64_t>(i)});
        ASSERT_NE(e, nullptr);
        e->phase = TxnPhase::Finalized;
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

    void data(const std::vector<Rec>& recs, int64_t txnPid = -1) {
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
        push(b.build(), int64_t(recs.size()));
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

// The key index, plus TestReject{TagEquals{9}} (objectId 7) when the set is constrained.  start() leaves the judge at 10.
struct JudgeRig {
    rune::codec::Descriptor       src = tagSource();
    PartitionWriteState           st;
    TxnLog                        log{st};
    VerdictSource                 vs{5, 0};
    std::unique_ptr<ViewInstance> index, reject;
    rune::view::CompileResult     comp;
    ConstraintSet                 set;

    explicit JudgeRig(bool constrained) {
        set.id = 1;
        index = std::make_unique<ViewInstance>(keyIndexPlan(5, src.find("id")->id), keyIndexRowLayout(), src, false, 0, 4096, kKeyIndexChunkBytes);
        vs.addVerdictView(index.get(), VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
        set.members[set.count++] = {0, VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint};
        if (!constrained) return;
        TestRejectRule rule;
        rule.mode = TestRejectRule::Mode::TagEquals;
        rule.tag  = 9;
        const rune::codec::Descriptor* s[] = {&src};
        comp   = rune::view::compileView(tagView(), s, 7).value();
        reject = std::make_unique<ViewInstance>(comp.plan, comp.rowLayout, src, false, 0, 256, 1u << 16);
        vs.installTestVerdictViewForTest(reject.get(), 7, rule);
        set.members[set.count++] = {7, VerdictViewKind::TestReject, rule.fingerprint()};
    }
    void start() {
        st.constraintSets.onChange(0, set);
        log.control();
        std::vector<Rec> fill;
        for (int64_t id = 100; id < 109; ++id) fill.push_back({id});
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
    [[nodiscard]] std::vector<int64_t> markerEnds() const {
        std::vector<int64_t> out;
        for (const auto& b : vs.appendedForTest()) {
            const auto c = cr::parseControl(b.data(), b.size());
            if (!c || c->type != cr::kVerdictMarker) continue;
            if (const auto m = cr::parseMarker(*c)) out.push_back(m->header.finalThrough);
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
    old->phase   = TxnPhase::Locked;
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

// Re-review 3: a vote parked across a rescan is never answered from the old leadership's answer.  The rescan clears it, and the vote
// waits until the entry leaves AwaitJudge.  In Task 5 that is finalizeDecided at the committed marker, so the drained answer is the
// cleared default, not a judgement (Task 6's judgeTxn_ produces one).
TEST(CheckedWritesTxnJudgeRegression, ParkedVoteWaitsAcrossARescanWithTheOldAnswerClearedRegression) {
    JudgeRig r(true);
    r.start();
    r.log.data({{1}, {2}});
    r.log.data({{3}, {4}}, 77);
    r.log.judge(77, 12);   // 14
    r.drive();
    DeferredTxn* t = r.st.txn.find(77);
    t->phase       = TxnPhase::Finalized;   // the old leadership's no
    t->answer      = {63, 7, 13};
    ASSERT_TRUE(r.st.txn.parkVote({1, 0, 77, 0}));
    verdictFeedBegin(&r.st, 0, r.log.next);
    t = r.st.txn.find(77);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->phase, TxnPhase::AwaitJudge);
    EXPECT_EQ(t->answer.code, 0) << "the rescan cleared the old leadership's answer";
    EXPECT_EQ(t->answer.constraintId, 0u);
    std::vector<TxnPhase> drainedAt;
    std::vector<uint16_t> codes;
    const auto drain = [&] {
        return r.st.txn.drainAnswered([&](const ParkedVote&, const DeferredTxn& d) { drainedAt.push_back(d.phase); codes.push_back(d.answer.code); });
    };
    EXPECT_EQ(drain(), 0) << "AwaitJudge: the vote stays parked";
    r.vs.bindLeader(nullptr, 2, 0);
    r.drive();
    EXPECT_EQ(drain(), 0) << "no marker yet: still AwaitJudge";
    r.log.marker(77, true);   // 15
    r.drive(15);
    EXPECT_EQ(drain(), 0) << "the marker is not below the HW yet";
    r.drive();
    ASSERT_EQ(drain(), 1);
    EXPECT_EQ(drainedAt[0], TxnPhase::Finalized) << "drained only once the entry left AwaitJudge";
    EXPECT_NE(codes[0], 63) << "never the pre-rescan answer";
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
