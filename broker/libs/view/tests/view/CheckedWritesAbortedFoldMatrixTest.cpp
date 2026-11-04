// #796: the aborted-fold bound/cursor matrix (plan AD2/AD3) and a model check, over the THREE real entry points:
// foldBatch, foldAggregateBatch without blocks, foldAggregateBatch with blocks.
//
// skippedAborted counts BATCHES (shipped, documented in FoldStats.h): a batch the bound cuts is not counted and
// stops the walk; the pass that later passes it whole counts it once.
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewRow.h>

#include "AbortedFoldFixtures.h"

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {

rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", P::Int64, 1, true)
        .add("userId", P::Int64, 1)
        .add("amount", P::Int64, 1)
        .build();
}

rune::view::ViewPlan projectionPlan() {
    rune::view::ViewPlan v;
    v.viewName = "order_amt"; v.key = "orderId"; v.sources = {{5, "orderId"}};
    v.output = {{"orderId", P::Int64, 0, "orderId", R::Replace}, {"amount", P::Int64, 0, "amount", R::Replace}};
    return v;
}
rune::view::ViewPlan aggregatePlan(bool withMax) {
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace}, {"n", P::Int64, 0, "", R::CountAll}};
    if (withMax) v.output.push_back({"maxAmt", P::Int64, 0, "amount", R::Max});
    v.output.push_back({"total", P::Int64, 0, "amount", R::Sum});
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }
std::vector<uint8_t> keyBytes(int64_t id) { std::vector<uint8_t> k; le64(k, uint64_t(id)); return k; }
std::vector<uint8_t> orderValue(int64_t id, int64_t user, int64_t amount) {
    std::vector<uint8_t> b;
    le16(b, 24); le16(b, 1);
    le64(b, uint64_t(id)); le64(b, uint64_t(user)); le64(b, uint64_t(amount));
    return b;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Rec { int64_t key; bool tomb; int64_t user; int64_t amount; };

// A data batch of consecutive records from `base`; pid >= 0 makes it a transactional batch of that producer.
std::vector<uint8_t> dataBatch(int64_t base, const std::vector<Rec>& recs, int64_t pid) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0); b.setTransactional(true); }
    for (const auto& r : recs) {
        const auto k = keyBytes(r.key);
        if (r.tomb) b.append(sv(k), {}, 0);
        else { const auto v = orderValue(r.key, r.user, r.amount); b.append(sv(k), sv(v), 0); }
    }
    return b.build();
}
std::vector<uint8_t> controlBatch(int64_t base, int64_t pid) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    b.setControl(true); b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.append("c", "v", 0);
    return b.build();
}

enum class Entry : int { Projection = 0, AggregateNoBlocks = 1, AggregateBlocks = 2 };
constexpr std::array<Entry, 3> kEntries{Entry::Projection, Entry::AggregateNoBlocks, Entry::AggregateBlocks};
const char* entryName(Entry e) {
    return e == Entry::Projection ? "foldBatch" : e == Entry::AggregateNoBlocks ? "foldAggregateBatch" : "foldAggregateBatch+blocks";
}

// One view + its fold state, driven through the real entry point of `kind`.
struct Engine {
    Entry kind;
    rune::codec::Descriptor src = orderSource();
    rune::view::CompileResult comp;
    std::optional<rune::view::AggLayout> agg;
    rune::view::VersionDescriptorCache vdc{src};
    std::unique_ptr<rune::view::ViewTable> view, contributors, groups, blocks;

    explicit Engine(Entry k) : kind(k) {
        const rune::codec::Descriptor* srcs[] = {&src};
        const auto plan = k == Entry::Projection ? projectionPlan() : aggregatePlan(k == Entry::AggregateBlocks);
        comp = rune::view::compileView(plan, srcs, 9).value();
        view = std::make_unique<rune::view::ViewTable>(comp.rowLayout, 64, 1u << 16);
        if (k != Entry::Projection) {
            agg = rune::view::buildAggLayout(comp.plan, src).value();
            contributors = std::make_unique<rune::view::ViewTable>(agg->contributorRow, 64, 1u << 16);
            groups = std::make_unique<rune::view::ViewTable>(agg->groupRow, 64, 1u << 16);
            if (k == Entry::AggregateBlocks) blocks = std::make_unique<rune::view::ViewTable>(agg->blockRow, 64, 1u << 16);
        }
    }

    rune::view::FoldStats pass(const std::vector<uint8_t>& bytes, int64_t nextOffset, int64_t bound, int64_t& cursor,
                               const rune::view::FoldSkip& skip) {
        // The real contract: a LogReadView is WHOLE batches, starting at the batch that holds the cursor.
        size_t pos = 0;
        int64_t baseOffset = cursor;
        while (pos < bytes.size()) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            EXPECT_TRUE(r.ensureValid());
            if (r.baseOffset() + r.lastOffsetDelta() >= cursor) { baseOffset = r.baseOffset(); break; }
            pos += static_cast<size_t>(r.totalLength());
        }
        const rune::storage::LogReadView lv{bytes.data() + pos, bytes.size() - pos, baseOffset, nextOffset};
        if (kind == Entry::Projection)
            return rune::view::foldBatch(comp.plan, comp.rowLayout, vdc, *view, lv, bound, cursor, skip);
        if (kind == Entry::AggregateNoBlocks)
            return rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, *agg, vdc, *view, *contributors, *groups,
                                                  lv, bound, cursor, skip);
        return rune::view::foldAggregateBatch(comp.plan, comp.rowLayout, *agg, vdc, *view, *contributors, *groups,
                                              blocks.get(), lv, bound, cursor, skip);
    }

    static void sameTable(const rune::view::ViewTable& a, const rune::view::ViewTable& b, uint32_t rowLen,
                          const char* what) {
        EXPECT_EQ(a.size(), b.size()) << what;
        for (uint64_t k = 0; k < 32; ++k) {
            const uint8_t* x = a.get(k);
            const uint8_t* y = b.get(k);
            ASSERT_EQ(x == nullptr, y == nullptr) << what << " key " << k;
            if (x != nullptr) EXPECT_EQ(0, std::memcmp(x, y, rowLen)) << what << " key " << k;
        }
    }
    // The view, and for aggregates the contributors and groups, equal the other engine's, byte for byte.
    void expectSameAs(const Engine& o) const {
        sameTable(*view, *o.view, comp.rowLayout.rowLen, "view");
        if (agg) {
            sameTable(*contributors, *o.contributors, agg->contributorRow.rowLen, "contributors");
            sameTable(*groups, *o.groups, agg->groupRow.rowLen, "groups");
        }
    }
};

// ----- the matrix log: plain [0..3], aborted pid 5 [4..6], plain [7] -----
struct MatrixLog {
    std::vector<uint8_t> all;        // with the aborted batch
    std::vector<uint8_t> committed;  // the aborted batch removed, offsets kept
    abortedfix::OneAbortedRange aborted{5, 4, 7};
    MatrixLog() : aborted(5, 4, 7) {
        const auto b0 = dataBatch(0, {{1, false, 1, 10}, {2, false, 1, 20}, {3, false, 2, 30}, {4, false, 2, 40}}, -1);
        const auto b1 = dataBatch(4, {{5, false, 1, 50}, {6, false, 3, 60}, {1, true, 0, 0}}, 5);
        const auto b2 = dataBatch(7, {{7, false, 3, 70}}, -1);
        all = abortedfix::concatBytes({b0, b1, b2});
        committed = abortedfix::concatBytes({b0, b2});
    }
};
constexpr int64_t kLogEnd = 8;

struct Cell {
    const char* name;
    int64_t from;
    std::vector<int64_t> bounds;
    std::vector<int64_t> cursors;
    std::vector<uint64_t> skipped;   // per pass
    std::vector<uint64_t> applied;   // per pass
};

std::vector<Cell> cells() {
    std::vector<Cell> c;
    c.push_back({"base>=bound", 0, {4}, {4}, {0}, {4}});
    c.push_back({"base<bound<=last", 4, {5, 8}, {5, 8}, {0, 1}, {0, 1}});
    c.push_back({"last<bound", 0, {8}, {8}, {1}, {5}});
    c.push_back({"resume-inside", 5, {7}, {7}, {1}, {0}});
    Cell cap{"capped-seq-cap1", 0, {}, {}, {}, {}};
    for (int64_t b = 1; b <= 8; ++b) {
        cap.bounds.push_back(b); cap.cursors.push_back(b);
        cap.skipped.push_back(b == 7 ? 1 : 0);                 // counted on the pass that passes it whole
        cap.applied.push_back(b <= 4 || b == 8 ? 1 : 0);       // records 0..3, then 7
    }
    c.push_back(std::move(cap));
    return c;
}

} // namespace

TEST(CheckedWritesAbortedFoldMatrixRegression, BoundAndCursorCellsRegression) {
    const MatrixLog log;
    for (const Entry e : kEntries) {
        for (const Cell& cell : cells()) {
            SCOPED_TRACE(std::string(entryName(e)) + " / " + cell.name);
            Engine eng(e);
            Engine ref(e);   // the committed-only log, no skip filter: the reference
            int64_t cur = cell.from, refCur = cell.from;
            uint64_t sumSkipped = 0, sumApplied = 0, refApplied = 0;
            const rune::view::FoldSkip skip{.aborted = log.aborted.filter()};
            for (size_t i = 0; i < cell.bounds.size(); ++i) {
                SCOPED_TRACE("pass " + std::to_string(i) + " bound " + std::to_string(cell.bounds[i]));
                const auto st = eng.pass(log.all, kLogEnd, cell.bounds[i], cur, skip);
                const auto rs = ref.pass(log.committed, kLogEnd, cell.bounds[i], refCur, rune::view::kNoSkip);
                EXPECT_EQ(cur, cell.cursors[i]);
                EXPECT_LE(cur, cell.bounds[i]) << "the cursor never passes the bound";
                EXPECT_EQ(st.skippedAborted, cell.skipped[i]);
                EXPECT_EQ(st.applied, cell.applied[i]);
                EXPECT_EQ(refCur, cur);
                sumSkipped += st.skippedAborted; sumApplied += st.applied; refApplied += rs.applied;
            }
            EXPECT_EQ(sumApplied, refApplied);
            if (cell.name == std::string("capped-seq-cap1")) { EXPECT_EQ(sumSkipped, 1u); EXPECT_EQ(sumApplied, 5u); }
            eng.expectSameAs(ref);
        }
    }
}

TEST(CheckedWritesAbortedFoldMatrixCountRegression, CellCountIsPinnedRegression) {
    EXPECT_EQ(cells().size() * kEntries.size(), 15u);
}

// ----- the model: random transactional logs fold to the committed-only fold -----
namespace {
struct ModelLog {
    std::vector<uint8_t> all;
    std::vector<uint8_t> committed;   // aborted data batches and control batches removed, offsets kept
    rune::transaction::AbortedTxnIndex idx;
    int64_t next = 0;
    uint64_t abortedBatches = 0;
};

ModelLog buildModel(uint64_t seed) {
    std::mt19937_64 rng(seed);
    ModelLog m;
    // Every batch in offset order; a transactional data batch carries its transaction's number so the outcome
    // (commit or abort, known only at the marker) can be applied when the log is assembled.
    struct Entry { std::vector<uint8_t> bytes; int txn; bool control; };   // txn < 0: plain
    std::vector<Entry> entries;
    std::map<int, bool> aborted;   // txn number -> aborted?
    struct Open { int txn; int64_t first; uint64_t batches; };
    std::map<int64_t, Open> open;
    int nextTxn = 0;
    auto records = [&](int n) {
        std::vector<Rec> rs;
        for (int i = 0; i < n; ++i)
            rs.push_back({int64_t(rng() % 16), rng() % 100 < 10, int64_t(rng() % 4), int64_t(rng() % 100) - 20});
        return rs;
    };
    auto closeTxn = [&](int64_t pid, bool commit) {
        const auto it = open.find(pid);
        entries.push_back({controlBatch(m.next, pid), -1, true});
        aborted[it->second.txn] = !commit;
        if (!commit) { m.idx.onAbortMarker(pid, it->second.first, m.next); m.abortedBatches += it->second.batches; }
        m.next += 1;
        open.erase(it);
    };
    for (int step = 0; step < 64; ++step) {
        const uint64_t what = rng() % 4;
        if (what == 0) {   // plain write
            const int n = 1 + int(rng() % 3);
            entries.push_back({dataBatch(m.next, records(n), -1), -1, false});
            m.next += n;
        } else if (what == 1) {   // a transactional batch of 1..4 records
            const int64_t pid = 7 + int64_t(rng() % 3);
            const int n = 1 + int(rng() % 4);
            auto it = open.find(pid);
            if (it == open.end()) it = open.emplace(pid, Open{nextTxn++, m.next, 0}).first;
            entries.push_back({dataBatch(m.next, records(n), pid), it->second.txn, false});
            ++it->second.batches;
            m.next += n;
        } else {   // commit (what == 2) or abort (what == 3) marker of an open producer
            if (open.empty()) continue;
            auto it = open.begin();
            std::advance(it, long(rng() % open.size()));
            closeTxn(it->first, what == 2);
        }
    }
    while (!open.empty()) closeTxn(open.begin()->first, false);
    for (const Entry& e : entries) {
        m.all.insert(m.all.end(), e.bytes.begin(), e.bytes.end());
        if (!e.control && (e.txn < 0 || !aborted[e.txn])) m.committed.insert(m.committed.end(), e.bytes.begin(), e.bytes.end());
    }
    return m;
}
} // namespace

TEST(CheckedWritesAbortedFoldModelRegression, RandomTxnLogsFoldToTheCommittedOnlyFoldRegression) {
    static constexpr int64_t kCaps[] = {1, 2, 3, 7, -1};
    for (uint64_t seed = 1; seed <= 300; ++seed) {
        SCOPED_TRACE("seed=" + std::to_string(seed));
        const ModelLog m = buildModel(seed);
        std::mt19937_64 rng(seed ^ 0x9e3779b97f4a7c15ull);
        const int64_t cap = kCaps[rng() % 5];
        const Entry e = kEntries[seed % 3];
        Engine eng(e), ref(e);
        const rune::view::FoldSkip skip{.aborted = rune::view::AbortedTxnFilter{&m.idx}};

        // Reference: the committed-only log, one unbounded pass, no skip filter.
        int64_t refCur = 0;
        ref.pass(m.committed, m.next, m.next, refCur, rune::view::kNoSkip);

        int64_t cur = 0;
        uint64_t sumSkipped = 0;
        int passes = 0;
        while (cur < m.next) {
            ASSERT_LT(++passes, 100000) << "hard cap: the cursor stopped advancing";
            const int64_t bound = cap < 0 ? m.next : std::min<int64_t>(m.next, cur + cap);
            const int64_t before = cur;
            const auto st = eng.pass(m.all, m.next, bound, cur, skip);
            sumSkipped += st.skippedAborted;
            ASSERT_LE(cur, bound) << "(c) cursor <= bound";
            ASSERT_GE(cur, before);
            // a cap-1 pass can sit inside an aborted batch (the cut batch is not counted): progress is still made
            // by the bound moving on; guard against a stuck cursor with the pass cap above.
            if (cur == before && bound == before) FAIL() << "empty bound";
        }
        EXPECT_EQ(cur, m.next) << "(d) the final cursor is the log end";
        EXPECT_EQ(sumSkipped, m.abortedBatches) << "(b) every aborted data batch counted once";
        eng.expectSameAs(ref);   // (a)
    }
}
