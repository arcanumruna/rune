// Part 11 Task 8: the transaction span fence (contract addition 13) and the fold that applies a transaction at its commit marker
// (P11-0, P11-9).
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/FoldWalk.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/TxnSpanFence.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewRow.h>

namespace {

using rune::view::TxnSpanFence;
using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

TEST(CheckedWritesTxnSpanFence, AddFindClose) {
    TxnSpanFence f;
    EXPECT_EQ(f.find(7), nullptr);
    ASSERT_TRUE(f.add(7, 10));
    ASSERT_TRUE(f.add(9, 13));
    ASSERT_NE(f.find(7), nullptr);
    EXPECT_EQ(f.find(7)->first, 10);
    EXPECT_EQ(f.find(9)->first, 13);
    f.close(7);
    EXPECT_EQ(f.find(7), nullptr);
    EXPECT_EQ(f.find(9)->first, 13);
    EXPECT_EQ(f.count, 1);
    f.close(42);   // not open: nothing changes
    EXPECT_EQ(f.count, 1);
}

TEST(CheckedWritesTxnSpanFence, The65thSpanIsRefusedUntilOneCloses) {
    TxnSpanFence f;
    for (int64_t pid = 0; pid < 64; ++pid) ASSERT_TRUE(f.add(pid, 100 + pid));
    EXPECT_FALSE(f.add(64, 200));
    EXPECT_EQ(f.find(64), nullptr);
    f.close(5);
    EXPECT_TRUE(f.add(64, 200));
    EXPECT_EQ(f.find(64)->first, 200);
    for (int64_t pid = 0; pid < 64; ++pid)
        if (pid != 5) EXPECT_NE(f.find(pid), nullptr) << pid;
}

TEST(CheckedWritesTxnSpanFence, StraddlesOnlyAboveAnOpenFirst) {
    TxnSpanFence f;
    EXPECT_FALSE(f.straddles(1'000'000));
    ASSERT_TRUE(f.add(3, 10));
    EXPECT_FALSE(f.straddles(10));   // a checkpoint AT first holds nothing of the span: a replay from it meets every batch
    EXPECT_TRUE(f.straddles(11));
    f.close(3);
    EXPECT_FALSE(f.straddles(11));
}

TEST(CheckedWritesTxnSpanFence, FencedWhileAnApplicationIsInProgress) {
    TxnSpanFence f;
    EXPECT_FALSE(f.fenced());
    ASSERT_TRUE(f.add(3, 10));
    EXPECT_FALSE(f.fenced());   // an open span alone never holds reads (P11-9): its rows are simply not there yet
    f.applyingThrough = 12;
    EXPECT_TRUE(f.fenced());
    f.close(3);   // the application completed with the span: the walk resets its resume point
    f.applyingThrough = -1;
    EXPECT_FALSE(f.fenced());
    f.atomicThrough = 40;
    EXPECT_TRUE(f.fenced());
    f.clear();
    EXPECT_FALSE(f.fenced());
    EXPECT_EQ(f.count, 0);
}

// ---------------------------------------------------------------------------------------------------------------------------
// The fold: one in-memory log, a projection (orderId -> amount) and an aggregate by userId (count(*), last(amount)).

// Closing a span other than the one being applied keeps the application's resume point.
TEST(CheckedWritesTxnSpanFenceRegression, ClosingAnotherSpanKeepsTheApplicationRegression) {
    TxnSpanFence f;
    ASSERT_TRUE(f.add(7, 10));
    ASSERT_TRUE(f.add(8, 12));
    f.applyingThrough = 14;
    f.close(8);
    EXPECT_EQ(f.applyingThrough, 14);
    EXPECT_TRUE(f.fenced());
}

// The key set hashes with the worker's seed, as the view tables do: another seed places keys elsewhere.
TEST(CheckedWritesTxnSpanFenceRegression, SupersedeKeysHashWithTheWorkersSeedRegression) {
    rune::view::SupersedeKeys a, b;
    b.seed(0x9E3779B97F4A7C15ull);
    int moved = 0;
    for (int k = 0; k < 16; ++k) {
        const std::string key = "key-" + std::to_string(k);
        moved += a.homeForTest(key) != b.homeForTest(key) ? 1 : 0;
    }
    EXPECT_GE(moved, 12);
}

rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .build();
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Rec { int64_t key; int64_t user; int64_t amount; };

void recrc(std::vector<uint8_t>& b) {
    namespace pb = rune::protocol::batch;
    const uint32_t crc = rune::protocol::crc32c(b.data() + pb::kAttributesOffset, b.size() - std::size_t(pb::kAttributesOffset));
    for (int i = 0; i < 4; ++i) b[std::size_t(pb::kCrcOffset + i)] = uint8_t(crc >> (8 * i));
}

// The log: whole batches at dense offsets.  read(from, to) answers every batch from the one holding `from` to the end, as a disk
// segment read does (the fold stops at its own bound).
struct TxnLog {
    std::vector<uint8_t> bytes;
    std::vector<std::size_t> pos;
    std::vector<int64_t> last;
    int64_t end = 0;

    void push(std::vector<uint8_t> b, int64_t n) {
        pos.push_back(bytes.size());
        bytes.insert(bytes.end(), b.begin(), b.end());
        end += n;
        last.push_back(end - 1);
    }
    // pid >= 0: a transactional batch of that producer.
    int64_t data(const std::vector<Rec>& recs, int64_t pid = -1, bool atomic = false) {
        const int64_t base = end;
        rune::protocol::RecordBatchBuilder b(base, 0);
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0); b.setTransactional(true); }
        for (const Rec& r : recs) {
            std::vector<uint8_t> k, v;
            le64(k, uint64_t(r.key));
            le16(v, 24); le16(v, 1); le64(v, uint64_t(r.key)); le64(v, uint64_t(r.user)); le64(v, uint64_t(r.amount));
            b.append(sv(k), sv(v), 0);
        }
        std::vector<uint8_t> out = b.build();
        if (atomic) {
            out[std::size_t(rune::protocol::batch::kAttributesOffset)] |= uint8_t(rune::protocol::batch::kAttrAtomic);
            recrc(out);
        }
        push(std::move(out), int64_t(recs.size()));
        return base;
    }
    // A commit (type 0) or abort (type 1) marker, laid out as ControlMarker::build writes it.
    // control: a non-transactional control batch of `pid` (a TxnJudge stand-in, for a JudgeHook).
    int64_t marker(int64_t pid, bool commit, bool transactional = true) {
        const int64_t base = end;
        std::vector<uint8_t> b;
        le64(b, uint64_t(base));
        for (int i = 0; i < 4; ++i) b.push_back(uint8_t(63 >> (8 * i)));
        for (int i = 0; i < 4; ++i) b.push_back(0xFF);
        b.push_back(2);
        for (int i = 0; i < 4; ++i) b.push_back(0);
        le16(b, transactional ? 0x0030 : 0x0020);
        for (int i = 0; i < 4; ++i) b.push_back(0);
        le64(b, 0); le64(b, 0); le64(b, uint64_t(pid));
        le16(b, 0);
        for (int i = 0; i < 4; ++i) b.push_back(0xFF);
        b.push_back(1); b.push_back(0); b.push_back(0); b.push_back(0);
        b.push_back(0); b.push_back(0); b.push_back(0); b.push_back(0x04);
        le16(b, commit ? 0 : 1);
        b.push_back(0x0C);
        le16(b, 1);
        for (int i = 0; i < 4; ++i) b.push_back(0);
        b.push_back(0);
        EXPECT_EQ(b.size(), 75u);
        recrc(b);
        push(std::move(b), 1);
        return base;
    }
    // Faults: a read from `emptyAt` answers empty once after `emptySkip` good ones; every read from [failFrom, failTo) answers empty.
    mutable int64_t emptyAt = -1;
    mutable int     emptySkip = 0;
    int64_t failFrom = -1, failTo = -1;

    [[nodiscard]] rune::storage::LogReadView read(int64_t from, int64_t /*to*/) const {
        if (from == emptyAt && emptySkip-- == 0) {
            emptyAt = -1;
            return {};
        }
        if (from >= failFrom && from < failTo) return {};
        for (std::size_t i = 0; i < pos.size(); ++i) {
            if (last[i] < from) continue;
            return {bytes.data() + pos[i], bytes.size() - pos[i], i == 0 ? 0 : last[i - 1] + 1, end};
        }
        return {};
    }
};

// One view instance of either kind and its fence, driven pass by pass as WorkerViewSet::driveView_ drives it.
struct Folder {
    bool aggregate;
    bool withFence;
    rune::codec::Descriptor src = orderSource();
    rune::view::CompileResult comp;
    std::optional<rune::view::AggLayout> agg;
    rune::view::VersionDescriptorCache vdc{src};
    std::unique_ptr<rune::view::ViewTable> view, contributors, groups;
    TxnSpanFence fence;
    rune::view::SupersedeKeys ownKeys;
    rune::view::SupersedeKeys* keys = &ownKeys;   // a test may hand two folders one set, as a worker's instances share it
    rune::view::JudgeHook judge{};
    int64_t cursor = 0;

    explicit Folder(bool aggregateView, bool fenced = true) : aggregate(aggregateView), withFence(fenced) {
        const rune::codec::Descriptor* srcs[] = {&src};
        rune::view::ViewPlan v;
        v.sources = {{5, aggregate ? "userId" : "orderId"}};
        if (aggregate) {
            v.viewName = "user_last"; v.key = "userId";
            v.output = {{"userId", P::Int64, 0, "userId", R::Replace}, {"n", P::Int64, 0, "", R::CountAll},
                        {"lastAmt", P::Int64, 0, "amount", R::Last}};
        } else {
            v.viewName = "order_amt"; v.key = "orderId";
            v.output = {{"orderId", P::Int64, 0, "orderId", R::Replace}, {"amount", P::Int64, 0, "amount", R::Replace}};
        }
        comp = rune::view::compileView(v, srcs, 9).value();
        view = std::make_unique<rune::view::ViewTable>(comp.rowLayout, 64, 1u << 16);
        if (aggregate) {
            agg = rune::view::buildAggLayout(comp.plan, src).value();
            contributors = std::make_unique<rune::view::ViewTable>(agg->contributorRow, 64, 1u << 16);
            groups = std::make_unique<rune::view::ViewTable>(agg->groupRow, 64, 1u << 16);
        }
    }

    rune::view::FoldStats pass(const TxnLog& log, int64_t bound, int64_t cap, const rune::view::FoldSkip& skip = rune::view::kNoSkip) {
        const int64_t passBound = cap > 0 ? std::min(bound, cursor + cap) : bound;
        if (cursor >= passBound) return {};
        const rune::storage::LogReadView lv = log.read(cursor, passBound);
        if (lv.empty()) return {};
        auto rd = [&log](int64_t f, int64_t t) { return log.read(f, t); };
        rune::view::TxnFold tf{fence, *keys, rune::view::spanReaderOf(rd), cap, judge};
        rune::view::TxnFold* txn = withFence ? &tf : nullptr;
        if (aggregate)
            return rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, *agg, vdc, *view, *contributors, *groups, lv, passBound, cursor, skip, txn);
        return rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, *view, lv, passBound, cursor, skip, txn);
    }
    [[nodiscard]] bool has(int64_t key) const { return view->get(uint64_t(key)) != nullptr; }
    [[nodiscard]] int64_t slot(int64_t key, uint16_t s) const {
        const uint8_t* row = view->get(uint64_t(key));
        return row == nullptr ? -1 : rune::view::readSlot(row, comp.rowLayout, s).i;
    }
    [[nodiscard]] int present(std::initializer_list<int64_t> keys) const {
        int n = 0;
        for (const int64_t k : keys) n += has(k) ? 1 : 0;
        return n;
    }
    [[nodiscard]] bool sameRows(const Folder& o, int64_t maxKey) const {
        if (view->size() != o.view->size()) return false;
        for (int64_t k = 0; k <= maxKey; ++k) {
            const uint8_t* a = view->get(uint64_t(k));
            const uint8_t* b = o.view->get(uint64_t(k));
            if ((a == nullptr) != (b == nullptr)) return false;
            if (a != nullptr && std::memcmp(a, b, comp.rowLayout.rowLen) != 0) return false;
        }
        return true;
    }
};

// Plain traffic folds exactly as without a fence: same cursors pass by pass, same rows, the fence never opens or holds a read.
TEST(CheckedWritesTxnVisibility, NoTransactionsNoFenceFold) {
    TxnLog log;
    for (int64_t i = 0; i < 12; ++i) log.data({{i % 5, i % 3, i * 10}, {(i + 1) % 5, i % 2, i}});
    log.data({{1, 1, 7}, {2, 1, 8}, {3, 2, 9}}, -1, false);
    for (const bool aggregate : {false, true}) {
        SCOPED_TRACE(aggregate ? "aggregate" : "projection");
        Folder fenced(aggregate), plain(aggregate, false);
        while (fenced.cursor < log.end) {
            fenced.pass(log, log.end, 2);
            plain.pass(log, log.end, 2);
            ASSERT_EQ(fenced.cursor, plain.cursor);
            EXPECT_EQ(fenced.fence.count, 0);
            EXPECT_FALSE(fenced.fence.fenced());
        }
        EXPECT_TRUE(fenced.sameRows(plain, 5));
    }
}

// P11-0: T's record (offset 0) applies at its marker (offset 2), after the plain record at 1: last(amount) of the group is T's.
TEST(CheckedWritesTxnVisibility, CommittedTxnAppliesAtItsMarkerNotAtItsOffsets) {
    TxnLog log;
    log.data({{2, 1, 10}}, 7);
    log.data({{3, 1, 20}});
    const int64_t m = log.marker(7, true);
    Folder f(true);
    f.pass(log, m, 0);
    EXPECT_EQ(f.cursor, m);
    EXPECT_EQ(f.slot(1, 1), 1) << "T is not applied below its marker";
    EXPECT_EQ(f.slot(1, 2), 20);
    EXPECT_EQ(f.fence.count, 1);
    EXPECT_TRUE(f.fence.straddles(m));
    EXPECT_FALSE(f.fence.fenced()) << "an open span alone never holds reads";
    f.pass(log, log.end, 0);
    EXPECT_EQ(f.cursor, log.end);
    EXPECT_EQ(f.slot(1, 1), 2);
    EXPECT_EQ(f.slot(1, 2), 10) << "marker order, not offset order";
    EXPECT_EQ(f.fence.count, 0);
}

// An abort marker closes the span: nothing of it is applied, even without the aborted index.
TEST(CheckedWritesTxnVisibility, AbortMarkerDropsTheSpan) {
    TxnLog log;
    log.data({{2, 1, 10}, {4, 1, 11}}, 7);
    log.data({{3, 1, 20}});
    log.marker(7, false);
    Folder f(false);
    f.pass(log, log.end, 0);
    EXPECT_EQ(f.cursor, log.end);
    EXPECT_FALSE(f.has(2));
    EXPECT_FALSE(f.has(4));
    EXPECT_TRUE(f.has(3));
    EXPECT_EQ(f.fence.count, 0);
}

// P11-9: a committed transaction of 6 records, split in 3 batches between plain records, folded with a pass cap of 2.  At every
// pass either the fence holds reads or all 6 rows are there or none is.
TEST(CheckedWritesTxnVisibility, CommittedTxnLargerThanThePassCapIsNeverReadHalfAppliedFold) {
    TxnLog log;
    log.data({{1, 1, 1}, {2, 1, 2}}, 7);
    log.data({{10, 2, 10}});
    log.data({{3, 1, 3}, {4, 1, 4}}, 7);
    log.data({{11, 2, 11}});
    log.data({{5, 1, 5}, {6, 1, 6}}, 7);
    log.marker(7, true);
    log.data({{12, 2, 12}});
    Folder f(false);
    int fencedPasses = 0, passes = 0;
    while (f.cursor < log.end && passes < 64) {
        f.pass(log, log.end, 2);
        ++passes;
        if (f.fence.fenced()) { ++fencedPasses; continue; }
        const int n = f.present({1, 2, 3, 4, 5, 6});
        EXPECT_TRUE(n == 0 || n == 6) << "pass " << passes << " shows " << n << " of 6";
    }
    EXPECT_EQ(f.cursor, log.end);
    EXPECT_EQ(f.present({1, 2, 3, 4, 5, 6, 10, 11, 12}), 9);
    EXPECT_EQ(fencedPasses, 2) << "6 records at 2 per pass: two passes end inside the application";
    EXPECT_EQ(f.fence.count, 0);
}

// P11-9: an atomic batch of 6 the pass cap of 2 cuts: reads hold until the last part is applied.
TEST(CheckedWritesTxnVisibility, AtomicBatchSplitByThePassCapIsFencedFold) {
    TxnLog log;
    log.data({{9, 2, 9}});
    log.data({{1, 1, 1}, {2, 1, 2}, {3, 1, 3}, {4, 1, 4}, {5, 1, 5}, {6, 1, 6}}, -1, true);
    log.data({{10, 2, 10}});
    Folder f(false);
    int fencedPasses = 0;
    while (f.cursor < log.end) {
        f.pass(log, log.end, 2);
        if (f.fence.fenced()) { ++fencedPasses; continue; }
        const int n = f.present({1, 2, 3, 4, 5, 6});
        EXPECT_TRUE(n == 0 || n == 6) << "cursor " << f.cursor << " shows " << n << " of 6";
    }
    EXPECT_EQ(fencedPasses, 3);
    EXPECT_EQ(f.present({1, 2, 3, 4, 5, 6, 9, 10}), 8);
}

// P11-9: a non-atomic batch the cap cuts is not fenced (it is many independent writes).
TEST(CheckedWritesTxnVisibility, PlainBatchSplitByThePassCapIsNotFenced) {
    TxnLog log;
    log.data({{1, 1, 1}, {2, 1, 2}, {3, 1, 3}, {4, 1, 4}});
    Folder f(false);
    f.pass(log, log.end, 2);
    EXPECT_EQ(f.present({1, 2, 3, 4}), 2);
    EXPECT_FALSE(f.fence.fenced());
}

// 65 transactions open at once: the 65th cannot wait (no span closes while the fold stands below every marker), so it applies at its
// own offset.  Nothing is skipped: after the markers every row is there.
TEST(CheckedWritesTxnVisibility, SpanTableFullNeverWaitsNeverSkips) {
    TxnLog log;
    for (int64_t pid = 0; pid < 65; ++pid) log.data({{pid + 1, 1, pid}}, pid);
    const int64_t firstMarker = log.end;
    for (int64_t pid = 0; pid < 65; ++pid) log.marker(pid, true);
    Folder f(false);
    const rune::view::FoldStats st = f.pass(log, firstMarker, 0);
    EXPECT_EQ(st.txnSpanTableFull, 1u);
    EXPECT_EQ(f.cursor, firstMarker);
    EXPECT_EQ(f.fence.count, 64);
    EXPECT_EQ(f.view->size(), 1u);
    EXPECT_TRUE(f.has(65)) << "the 65th applies at its offset";
    f.pass(log, log.end, 0);
    EXPECT_EQ(f.cursor, log.end);
    EXPECT_EQ(f.view->size(), 65u);
    EXPECT_EQ(f.fence.count, 0);
}

// A pass cap leaves a marker whose application would not fit this pass to the next one, unfenced (nothing of it applied yet).
TEST(CheckedWritesTxnVisibility, MarkerOverTheBudgetWaitsForTheNextPassUnfenced) {
    TxnLog log;
    log.data({{1, 1, 1}}, 7);
    log.data({{10, 2, 10}});
    const int64_t m = log.marker(7, true);
    Folder f(false);
    f.pass(log, log.end, 2);   // walks 0..1: the marker at 2 is past this pass's bound
    EXPECT_EQ(f.cursor, m);
    EXPECT_FALSE(f.has(1));
    EXPECT_FALSE(f.fence.fenced());
    f.pass(log, log.end, 2);
    EXPECT_TRUE(f.has(1));
    EXPECT_EQ(f.cursor, log.end);
}

// D1 (P11-3a on every fold path): T writes key 2 at 0, a plain record writes it at 1, T commits at 2.  The plain record was applied
// before the marker, so T's record is superseded: the row is the plain one, and the aggregate counts only it.  That is also what the
// log keeps once the cleaner drops T's record.
TEST(CheckedWritesTxnVisibility, RecordAppliedBeforeTheMarkerSupersedesTheTransactions) {
    TxnLog log;
    log.data({{2, 1, 10}, {4, 1, 40}}, 7);
    log.data({{2, 1, 20}});
    log.marker(7, true);
    Folder p(false), a(true);
    const rune::view::FoldStats st = p.pass(log, log.end, 0);
    a.pass(log, log.end, 0);
    EXPECT_EQ(st.txnSuperseded, 1u);
    EXPECT_EQ(p.slot(2, 1), 20) << "the plain record supersedes T's on key 2";
    EXPECT_EQ(p.slot(4, 1), 40) << "T's record on a key nobody else wrote applies";
    EXPECT_EQ(a.slot(1, 1), 2) << "count(*): the plain record and T's key-4 record";
    EXPECT_EQ(a.slot(1, 2), 40);
}

// Only a write applied before the marker supersedes: a transaction that commits before T's marker does, one still open at it (it
// applies after T), an aborted one and a record above T's never do.
TEST(CheckedWritesTxnVisibility, OnlyAWriteAppliedBeforeTheMarkerSupersedes) {
    {
        TxnLog log;   // T8 commits below M7: superseded
        log.data({{2, 1, 10}}, 7);
        log.data({{2, 1, 20}}, 8);
        log.marker(8, true);
        log.marker(7, true);
        Folder p(false);
        p.pass(log, log.end, 0);
        EXPECT_EQ(p.slot(2, 1), 20);
    }
    {
        TxnLog log;   // T8 still open at M7: T7 applies, then T8 at its marker
        log.data({{2, 1, 10}}, 7);
        log.data({{2, 1, 20}}, 8);
        log.marker(7, true);
        const int64_t m8 = log.marker(8, true);
        Folder a(true);
        a.pass(log, m8, 0);
        EXPECT_EQ(a.slot(1, 1), 1);
        EXPECT_EQ(a.slot(1, 2), 10) << "T7 applied, not superseded";
        a.pass(log, log.end, 0);
        EXPECT_EQ(a.slot(1, 1), 1) << "T8 replaces key 2's contribution";
        EXPECT_EQ(a.slot(1, 2), 20);
    }
    {
        TxnLog log;   // T8 aborted (the partition's aborted index, as every fold path is handed): never supersedes
        log.data({{2, 1, 10}}, 7);
        const int64_t t8 = log.data({{2, 1, 20}}, 8);
        const int64_t a8 = log.marker(8, false);
        log.marker(7, true);
        rune::transaction::AbortedTxnIndex idx;
        idx.onAbortMarker(8, t8, a8);
        const rune::view::FoldSkip skip{.aborted = {&idx}};
        Folder p(false);
        p.pass(log, log.end, 0, skip);
        EXPECT_EQ(p.slot(2, 1), 10);
    }
    {
        TxnLog log;   // the plain record lies below T's: T's is the later write
        log.data({{2, 1, 20}});
        log.data({{2, 1, 10}}, 7);
        log.marker(7, true);
        Folder p(false);
        p.pass(log, log.end, 0);
        EXPECT_EQ(p.slot(2, 1), 10);
    }
}

// P11-3a is per key, as the live judge decides it: T writes x into g1, a plain record moves x to g2, T moves x to g3.  The plain
// record lies below last_T(x), so neither of T's records is superseded (a per-record rule skipped x@10).  This fold's last()
// retracts, so the rows agree either way here; the decision is what a member's replay must share with the judge.
TEST(CheckedWritesTxnVisibilityRegression, SupersedeIsPerKeyAboveTheTransactionsLastRecordRegression) {
    TxnLog log;
    log.data({{9, 1, 5}});
    log.data({{2, 1, 10}}, 7);
    log.data({{2, 2, 20}});
    log.data({{2, 3, 30}}, 7);
    log.marker(7, true);
    Folder a(true), p(false);
    const rune::view::FoldStats st = a.pass(log, log.end, 0);
    p.pass(log, log.end, 0);
    EXPECT_EQ(st.txnSuperseded, 0u);
    EXPECT_EQ(a.slot(1, 1), 1);
    EXPECT_EQ(a.slot(1, 2), 5) << "x passed through g1 and left it";
    EXPECT_EQ(a.slot(3, 2), 30);
    EXPECT_EQ(p.slot(2, 1), 30);
}

// A read-back that comes back empty below the marker (a compacted gap, an I/O error, a damaged segment) never closes the span: the
// read is retried to the marker, and a failure that lasts holds the application where it stands, fenced, until reads work again.
TEST(CheckedWritesTxnVisibilityRegression, EmptyReadBackMidSpanNeverClosesTheSpanRegression) {
    TxnLog log;
    log.data({{1, 1, 10}, {2, 1, 20}}, 7);
    log.data({{3, 1, 30}, {4, 1, 40}}, 7);
    log.data({{5, 1, 50}, {6, 1, 60}}, 7);
    const int64_t m = log.marker(7, true);
    Folder once(false);
    for (int i = 0; i < 20 && once.fence.applyingThrough < 0; ++i) once.pass(log, log.end, 2);
    ASSERT_EQ(once.fence.applyingThrough, 1);
    log.emptyAt = 2;   // the next pass's apply read from 2 (its notes are kept from the last pass) answers empty once
    log.emptySkip = 0;
    for (int i = 0; i < 20 && once.cursor <= m; ++i) once.pass(log, log.end, 2);
    EXPECT_EQ(log.emptyAt, -1) << "the fault fired";
    EXPECT_EQ(once.present({1, 2, 3, 4, 5, 6}), 6);

    Folder p(false);
    for (int i = 0; i < 20 && p.fence.applyingThrough < 0; ++i) p.pass(log, log.end, 2);
    ASSERT_EQ(p.fence.applyingThrough, 1);
    log.failFrom = 0;
    log.failTo = m;
    for (int i = 0; i < 3; ++i) {
        const rune::view::FoldStats st = p.pass(log, log.end, 2);
        EXPECT_EQ(st.txnReadFailed, 1u);
    }
    EXPECT_EQ(p.cursor, m);
    EXPECT_TRUE(p.fence.fenced()) << "the applied half stays fenced";
    EXPECT_EQ(p.fence.count, 1);
    EXPECT_EQ(p.present({1, 2, 3, 4, 5, 6}), 2);
    log.failFrom = log.failTo = -1;
    for (int i = 0; i < 20 && p.cursor <= m; ++i) p.pass(log, log.end, 2);
    EXPECT_EQ(p.present({1, 2, 3, 4, 5, 6}), 6);
    EXPECT_FALSE(p.fence.fenced());
}

// A span with more bytes below its marker than one pass may read: the note read stops at the budget and resumes where it stopped,
// unfenced and with nothing of T visible until the notes are complete; it never re-reads the span from its start.
namespace {
int64_t plainTraffic(TxnLog& log, std::size_t bytes) {
    int64_t n = 0;
    while (log.bytes.size() < bytes) {
        std::vector<Rec> b;
        for (int i = 0; i < 1000; ++i, ++n) b.push_back({100 + n % 1000, n % 7, n});
        log.data(b);
    }
    return n;
}
}

TEST(CheckedWritesTxnVisibilityRegression, NoteReadPastThePassBudgetResumesRegression) {
    TxnLog log;
    log.data({{1, 1, 10}, {2, 1, 20}}, 7);
    plainTraffic(log, 3 * rune::view::kTxnReadBackBytesPerPass);
    log.data({{2, 1, 21}});   // supersedes T's key 2: the decision needs the notes to reach the marker
    const int64_t m = log.marker(7, true);
    Folder p(false);
    while (p.cursor < m) p.pass(log, log.end, 0);
    ASSERT_EQ(p.cursor, m);
    p.pass(log, log.end, 0);
    EXPECT_EQ(p.cursor, m) << "the notes are not complete after one budget";
    EXPECT_FALSE(p.fence.fenced()) << "nothing of T is applied yet: reads are served";
    EXPECT_EQ(p.present({1}), 0);
    int passes = 1;
    for (; passes < 10 && p.cursor <= m; ++passes) p.pass(log, log.end, 0);
    EXPECT_LE(passes, 8) << "about 3 MiB of notes and 3 MiB of apply read at 1 MiB a pass";
    EXPECT_EQ(p.slot(1, 1), 10);
    EXPECT_EQ(p.slot(2, 1), 21);
}

// REGRESSION (Task 8 review 2, m-a): the application of a small transaction inside long plain traffic reads back only to T's last
// record.  It read on to the marker, at the pass budget, with the view's reads held the whole time.
TEST(CheckedWritesTxnVisibilityRegression, SmallTxnInLongPlainTrafficAppliesWithoutAFencedPassRegression) {
    TxnLog log;
    log.data({{1, 1, 10}}, 7);
    plainTraffic(log, 3 * rune::view::kTxnReadBackBytesPerPass);
    const int64_t m = log.marker(7, true);
    Folder p(false);
    while (p.cursor < m) p.pass(log, log.end, 0);
    int fenced = 0, passes = 0;
    for (; passes < 10 && p.cursor <= m; ++passes) {
        p.pass(log, log.end, 0);
        fenced += p.fence.fenced() ? 1 : 0;
    }
    EXPECT_EQ(fenced, 0);
    EXPECT_LE(passes, 4) << "three passes of notes, the application in the last";
    EXPECT_EQ(p.slot(1, 1), 10);
}

// REGRESSION (Task 8 review 2, m-a): every marker a call applies shares the call's read-back budget.  Each application had its own,
// so a pass over N markers read N budgets.
TEST(CheckedWritesTxnVisibilityRegression, ReadBackBudgetIsSharedByTheCallsMarkersRegression) {
    TxnLog log;
    log.data({{1, 1, 10}}, 7);
    plainTraffic(log, rune::view::kTxnReadBackBytesPerPass * 7 / 10);
    log.marker(7, true);
    log.data({{2, 1, 20}}, 8);
    plainTraffic(log, log.bytes.size() + rune::view::kTxnReadBackBytesPerPass * 7 / 10);
    const int64_t m8 = log.marker(8, true);
    Folder p(false);
    p.pass(log, log.end, 0);
    EXPECT_EQ(p.slot(1, 1), 10);
    EXPECT_FALSE(p.has(2)) << "the second marker's notes wait for the next pass";
    EXPECT_EQ(p.cursor, m8);
    for (int i = 0; i < 4 && p.cursor <= m8; ++i) p.pass(log, log.end, 0);
    EXPECT_EQ(p.slot(2, 1), 20);
}

// REGRESSION (Task 8 review 2, m-c): a transaction applied at its TxnJudge reads its span back once.  It was also read for supersede
// notes D2 does not need (P11-3 voted no on every superseder), so a span of 0.7 MiB took two passes of the 1 MiB read-back.
TEST(CheckedWritesTxnVisibilityRegression, JudgedSpanIsReadBackOnceWithoutNotesRegression) {
    TxnLog log;
    log.data({{1, 1, 10}}, 7);
    plainTraffic(log, rune::view::kTxnReadBackBytesPerPass * 7 / 10);
    log.data({{2, 1, 20}}, 7);
    const int64_t j = log.marker(7, true, false);
    log.data({{3, 1, 30}});
    Folder p(false);
    p.judge = {nullptr, [](void*, const uint8_t*, std::size_t, int64_t) { return rune::view::JudgeDecision{rune::view::JudgeStep::Apply, 7}; }};
    const rune::view::FoldStats st = p.pass(log, log.end, 0);
    EXPECT_GT(p.cursor, j) << "J is applied in the pass that reaches it";
    EXPECT_EQ(p.slot(1, 1), 10);
    EXPECT_EQ(p.slot(2, 1), 20);
    EXPECT_EQ(p.fence.count, 0);
    EXPECT_EQ(st.txnNoteWaits, 0u);
}

// Two instances of a worker share its key set.  While one notes, the other's marker waits; a holder that is not driven for a whole
// call of the waiter loses the set, and notes again from where it stood when it is driven again.  Both end as if alone.
TEST(CheckedWritesTxnVisibilityRegression, SharedKeySetWaitsForItsHolderThenTakesAnIdleOneRegression) {
    TxnLog log;
    log.data({{1, 1, 10}, {2, 1, 20}}, 7);
    plainTraffic(log, 3 * rune::view::kTxnReadBackBytesPerPass);   // three passes of notes: the application itself is one batch
    log.data({{2, 1, 21}});
    const int64_t m = log.marker(7, true);
    Folder alone(false), a(false), b(false);
    b.keys = &a.ownKeys;
    while (alone.cursor <= m) alone.pass(log, log.end, 0);
    while (a.cursor < m) a.pass(log, log.end, 0);
    while (b.cursor < m) b.pass(log, log.end, 0);
    a.pass(log, log.end, 0);   // a holds the set, its notes half read
    EXPECT_EQ(b.pass(log, log.end, 0).txnNoteWaits, 1u);
    a.pass(log, log.end, 0);
    EXPECT_EQ(b.pass(log, log.end, 0).txnNoteWaits, 1u) << "a used the set since b's last look";
    EXPECT_EQ(b.pass(log, log.end, 0).txnNoteWaits, 0u) << "a was not driven: b takes the set";
    for (int i = 0; i < 20 && (a.cursor <= m || b.cursor <= m); ++i) {
        b.pass(log, log.end, 0);
        a.pass(log, log.end, 0);
    }
    EXPECT_TRUE(a.sameRows(alone, 1100));
    EXPECT_TRUE(b.sameRows(alone, 1100));
}

// A holder whose read-back keeps failing (a damaged segment) gives the shared key set up on every failure: another instance's
// marker applies within two calls, and the failing one still finishes once its reads work again.
TEST(CheckedWritesTxnVisibilityRegression, FailingHolderNeverHoldsTheSharedKeySetRegression) {
    TxnLog logA, logB;
    for (TxnLog* log : {&logA, &logB}) {
        log->data({{1, 1, 10}, {2, 1, 20}}, 7);
        log->data({{3, 1, 30}});
        log->marker(7, true);
    }
    const int64_t m = logA.end - 1;
    Folder alone(false), a(false), b(false);
    b.keys = &a.ownKeys;
    while (alone.cursor <= m) alone.pass(logA, logA.end, 2);
    a.pass(logA, logA.end, 2);   // a stands below the marker; every read back from T's first batch fails from here on
    logA.failFrom = 0;
    logA.failTo = 1;
    for (int i = 0; i < 3; ++i) a.pass(logA, logA.end, 2);
    ASSERT_EQ(a.cursor, m);
    EXPECT_EQ(a.pass(logA, logA.end, 2).txnReadFailed, 1u);
    int calls = 0;
    for (; calls < 6 && b.cursor <= m; ++calls) {
        b.pass(logB, logB.end, 2);
        a.pass(logA, logA.end, 2);
    }
    EXPECT_LE(calls, 3) << "b waited on a holder that never progresses";
    EXPECT_TRUE(b.sameRows(alone, 3));
    EXPECT_EQ(a.cursor, m);
    logA.failFrom = logA.failTo = -1;
    for (int i = 0; i < 4 && a.cursor <= m; ++i) a.pass(logA, logA.end, 2);
    EXPECT_TRUE(a.sameRows(alone, 3));
}

// A transaction with more keys than one segment notes applies in segments, each superseded by the writes above it; with a pass cap
// of 3 the result equals the uncapped fold.
TEST(CheckedWritesTxnVisibility, SegmentsOfManyKeysSupersedeLikeOne) {
    TxnLog log;
    std::vector<Rec> t;
    for (int64_t k = 0; k < int64_t(rune::view::SupersedeKeys::kMaxKeys) + 40; ++k) t.push_back({k, k % 3, k});
    log.data(t, 7);
    log.data({{5, 0, 1000}, {int64_t(rune::view::SupersedeKeys::kMaxKeys) + 7, 1, 2000}});
    log.marker(7, true);
    Folder whole(false), capped(false);
    const rune::view::FoldStats st = whole.pass(log, log.end, 0);
    EXPECT_EQ(st.txnSuperseded, 2u) << "one superseded key in each segment";
    EXPECT_EQ(whole.slot(5, 1), 1000);
    EXPECT_EQ(whole.slot(int64_t(rune::view::SupersedeKeys::kMaxKeys) + 7, 1), 2000);
    for (int i = 0; i < 2000 && capped.cursor < log.end; ++i) capped.pass(log, log.end, 3);
    EXPECT_EQ(capped.cursor, log.end);
    EXPECT_TRUE(capped.sameRows(whole, int64_t(rune::view::SupersedeKeys::kMaxKeys) + 40));
}

} // namespace
