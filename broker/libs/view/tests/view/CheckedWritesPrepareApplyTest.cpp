// Part 04 Tasks 1, 2 and 4: prepare/apply and the rejected skip.  The identity tests pin that the folds are the same
// code before and after the split, against a frozen copy (LegacyFoldReference).
#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <random>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/AggFold.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/RecordFold.h>
#include <rune/view/Verdict.h>
#include <rune/view/ViewFold.h>

#include "LegacyFoldReference.h"

using P = rune::codec::PrimType;
using R = rune::view::MergeRule;

namespace {

// ---- projection fixture: {characterId Int64 key, hp Int32} ------------------------------------------------------
rune::codec::Descriptor charSource() {
    return rune::codec::DescriptorBuilder("character", 1).add("characterId", P::Int64, 1, true).add("hp", P::Int32).build();
}
rune::view::ViewPlan hpView() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp";
    v.key      = "characterId";
    v.sources  = {{5, "characterId"}};
    v.output   = {{"characterId", P::Int64, 0, "characterId", R::Replace}, {"hp", P::Int32, 0, "hp", R::Replace}};
    return v;
}

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }

std::vector<uint8_t> keyBytes(uint64_t id) { std::vector<uint8_t> b; le64(b, id); return b; }
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

std::vector<uint8_t> character(int64_t id, int32_t hp) {
    std::vector<uint8_t> b;
    le16(b, 12); le16(b, 1); le64(b, uint64_t(id)); le32(b, uint32_t(hp));
    return b;
}

// ---- aggregate fixture: order {orderId key, userId, amount} -----------------------------------------------------
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 1).add("orderId", P::Int64, 1, true).add("userId", P::Int64, 1).add("amount", P::Int64, 1).build();
}
rune::view::ViewPlan userTotals() {   // slots: 0 userId | 1 n | 2 total
    rune::view::ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace}, {"n", P::Int64, 0, "", R::CountAll}, {"total", P::Int64, 0, "amount", R::Sum}};
    return v;
}
rune::view::ViewPlan userMax() {      // slots: 0 userId | 1 n | 2 mx (Max: value blocks)
    rune::view::ViewPlan v;
    v.viewName = "user_max"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace}, {"n", P::Int64, 0, "", R::CountAll}, {"mx", P::Int64, 0, "amount", R::Max}};
    return v;
}
rune::view::ViewPlan userLast() {     // Last-only: linked, no blocks
    rune::view::ViewPlan v;
    v.viewName = "user_last"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", P::Int64, 0, "userId", R::Replace}, {"last", P::Int64, 0, "amount", R::Last}};
    return v;
}
std::vector<uint8_t> order(int64_t id, int64_t user, int64_t amount) {
    std::vector<uint8_t> b;
    le16(b, 24); le16(b, 1); le64(b, uint64_t(id)); le64(b, uint64_t(user)); le64(b, uint64_t(amount));
    return b;
}

struct AggRig {
    rune::codec::Descriptor src = orderSource();
    rune::view::CompileResult comp;
    rune::view::AggLayout agg;
    rune::view::VersionDescriptorCache vdc{src};
    rune::view::ViewTable view, contributors, groups, blocks;
    explicit AggRig(const rune::view::ViewPlan& plan)
        : comp(compile(plan, src)), agg(rune::view::buildAggLayout(comp.plan, src).value()),
          view(comp.rowLayout, 256, 1u << 16), contributors(agg.contributorRow, 256, 1u << 16),
          groups(agg.groupRow, 256, 1u << 16), blocks(agg.blockRow, 256, 1u << 16) {}
    static rune::view::CompileResult compile(const rune::view::ViewPlan& plan, const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return rune::view::compileView(plan, srcs, 9).value();
    }
    [[nodiscard]] rune::view::ViewTable* blk() { return agg.numBlockCols > 0 ? &blocks : nullptr; }
    [[nodiscard]] rune::view::AggregateFold fold() {
        return {comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, blk()};
    }
};

using Snapshot = std::map<uint64_t, std::vector<uint8_t>>;
Snapshot snapshot(rune::view::ViewTable& t, uint32_t rowLen) {
    Snapshot s;
    t.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t key) {
        if (!deleted) s[key] = std::vector<uint8_t>(row, row + rowLen);
    });
    return s;
}
// A snapshot that does not drain: probe every key a test uses.
Snapshot probe(const rune::view::ViewTable& t, uint32_t rowLen, uint64_t keys) {
    Snapshot s;
    for (uint64_t k = 0; k < keys; ++k)
        if (const uint8_t* r = t.get(k)) s[k] = std::vector<uint8_t>(r, r + rowLen);
    return s;
}

struct AbortedPid {   // every batch of `pid` is aborted
    int64_t pid;
    [[nodiscard]] rune::view::AbortedTxnFilter filter() const {
        rune::view::AbortedTxnFilter f;
        f.ctx = this;
        f.fn  = [](const void* c, int64_t p, int64_t) noexcept { return p == static_cast<const AbortedPid*>(c)->pid; };
        return f;
    }
};

}  // namespace

// ---- Task 1 -----------------------------------------------------------------------------------------------------

struct ProjRig {
    rune::codec::Descriptor src = charSource();
    rune::view::CompileResult comp = [&] {
        const rune::codec::Descriptor* s[] = {&src};
        return rune::view::compileView(hpView(), s, 0x40000007).value();
    }();
    rune::view::ViewTable table{comp.rowLayout, 256, 1u << 16};
    rune::view::VersionDescriptorCache vdc{src};
    rune::view::Prospect prospect;
    rune::view::ProjectionFold pf{comp.plan, comp.rowLayout, vdc, table, P::Int64};
    ProjRig() { prospect.reserve(comp.rowLayout, nullptr); }
    void put(int64_t id, int32_t hp) {
        const auto k = keyBytes(uint64_t(id)); const auto v = character(id, hp);
        rune::view::SourceRecord r{sv(k), sv(v), 0, 0, nullptr};
        ASSERT_EQ(rune::view::prepareProjection(pf, r, prospect), rune::view::PrepareStatus::Ok);
        rune::view::applyProjection(pf, r, prospect);
    }
};

TEST(CheckedWritesPrepareApply, ProjectionPrepareWritesNothing) {
    ProjRig g;
    g.put(1, 10);
    (void)g.table.dirtyRids();
    const auto dirtyBefore = g.table.dirtyRids().size();
    const auto k = keyBytes(1); const auto v = character(1, 20);
    rune::view::SourceRecord r{sv(k), sv(v), 0, 0, nullptr};
    ASSERT_EQ(rune::view::prepareProjection(g.pf, r, g.prospect), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(g.prospect.op, rune::view::ProspectOp::Upsert);
    EXPECT_TRUE(g.prospect.rows[0].existed);
    EXPECT_EQ(rune::view::readSlot(g.prospect.rows[0].image, g.comp.rowLayout, 1).i, 20);
    EXPECT_EQ(rune::view::readSlot(g.table.get(1), g.comp.rowLayout, 1).i, 10);
    EXPECT_EQ(g.table.size(), 1u);
    EXPECT_EQ(g.table.dirtyRids().size(), dirtyBefore);
}

TEST(CheckedWritesPrepareApply, ProjectionApplyEqualsTheImage) {
    ProjRig g;
    const auto k = keyBytes(7); const auto v = character(7, 33);
    rune::view::SourceRecord r{sv(k), sv(v), 0, 0, nullptr};
    ASSERT_EQ(rune::view::prepareProjection(g.pf, r, g.prospect), rune::view::PrepareStatus::Ok);
    EXPECT_FALSE(g.prospect.rows[0].existed);
    std::vector<uint8_t> image(g.prospect.rows[0].image, g.prospect.rows[0].image + g.comp.rowLayout.rowLen);
    rune::view::applyProjection(g.pf, r, g.prospect);
    ASSERT_NE(g.table.get(7), nullptr);
    EXPECT_EQ(std::memcmp(g.table.get(7), image.data(), image.size()), 0);
}

TEST(CheckedWritesPrepareApply, TombstonePreparesErase) {
    ProjRig g;
    g.put(1, 10);
    const auto k = keyBytes(1);
    rune::view::SourceRecord r{sv(k), {}, 0, 0, nullptr};
    ASSERT_EQ(rune::view::prepareProjection(g.pf, r, g.prospect), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(g.prospect.op, rune::view::ProspectOp::Erase);
    EXPECT_TRUE(g.prospect.rows[0].removed);
    EXPECT_NE(g.table.get(1), nullptr);
    rune::view::applyProjection(g.pf, r, g.prospect);
    EXPECT_EQ(g.table.get(1), nullptr);
}

TEST(CheckedWritesPrepareApply, MalformedWireKeyPreparesMalformed) {
    ProjRig g;
    const std::string k(7, 'x'); const auto v = character(1, 1);
    rune::view::SourceRecord r{k, sv(v), 0, 0, nullptr};
    EXPECT_EQ(rune::view::prepareProjection(g.pf, r, g.prospect), rune::view::PrepareStatus::Malformed);
    EXPECT_EQ(g.prospect.op, rune::view::ProspectOp::None);
    rune::view::applyProjection(g.pf, r, g.prospect);
    EXPECT_EQ(g.table.size(), 0u);
}

TEST(CheckedWritesPrepareApply, ConstraintIdPacksObjectIdAndOrdinal) {
    EXPECT_EQ(rune::view::constraintIdOf(5, 1), 0x00050001u);
    EXPECT_EQ(rune::view::constraintIdOf(0xABCD, 0), 0xABCD0000u);
}

TEST(CheckedWritesPrepareApply, ViewKeyFromU64RoundTrips) {
    for (const uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{0x0102}, uint64_t{1} << 63, UINT64_MAX}) {
        const auto k = rune::view::ViewKey::fromU64(v);
        EXPECT_EQ(k.len, 8u);
        EXPECT_EQ(k.asU64(), v);
    }
}

namespace {
struct Log {
    std::vector<uint8_t> bytes;
    int64_t next = 0;
    // A data batch of `n` records: id = rng % 16, 15 % tombstones; `pid >= 0` makes it transactional.
    void data(std::mt19937_64& rng, int n, int64_t pid) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0); b.setTransactional(true); }
        for (int i = 0; i < n; ++i) {
            const auto k = keyBytes(rng() % 16);
            if (rng() % 100 < 15) b.append(sv(k), {}, 0);
            else { const auto v = character(int64_t(rng() % 16), int32_t(rng() % 1000)); b.append(sv(k), sv(v), 0); }
        }
        const auto bb = b.build();
        bytes.insert(bytes.end(), bb.begin(), bb.end());
        next += n;
    }
    void control() {
        rune::protocol::RecordBatchBuilder b(next, 0);
        b.setControl(true);
        b.append("c", "v", 0);
        const auto bb = b.build();
        bytes.insert(bytes.end(), bb.begin(), bb.end());
        next += 1;
    }
    // Aggregate log: key = orderId (8 B), value = order(...).
    void orders(std::mt19937_64& rng, int n, int64_t pid) {
        rune::protocol::RecordBatchBuilder b(next, 0);
        if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0); b.setTransactional(true); }
        for (int i = 0; i < n; ++i) {
            const auto k = keyBytes(rng() % 16);
            if (rng() % 100 < 15) b.append(sv(k), {}, 0);
            else { const auto v = order(int64_t(rng() % 16), int64_t(rng() % 4), int64_t(rng() % 100) - 20); b.append(sv(k), sv(v), 0); }
        }
        const auto bb = b.build();
        bytes.insert(bytes.end(), bb.begin(), bb.end());
        next += n;
    }
};

// Walks the whole batches of `bytes` and yields each batch's span.
template <class Fn>
void forEachBatch(const std::vector<uint8_t>& bytes, Fn&& fn) {
    size_t pos = 0;
    while (pos < bytes.size()) {
        rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
        ASSERT_TRUE(r.ensureValid());
        fn(bytes.data() + pos, static_cast<size_t>(r.totalLength()), r);
        pos += static_cast<size_t>(r.totalLength());
    }
}
}  // namespace

TEST(CheckedWritesPrepareApplyRegression, ProjectionFoldIsByteIdenticalToLegacyRegression) {
    for (uint64_t seed = 1; seed <= 200; ++seed) {
        std::mt19937_64 rng(seed);
        Log log;
        for (int i = 0; i < 64; ++i) {
            const auto roll = rng() % 10;
            if (roll == 0) log.control();
            else log.data(rng, 1 + int(rng() % 8), roll == 1 ? 5 : (roll == 2 ? 6 : -1));
        }
        const AbortedPid ab{5};
        const rune::view::FoldSkip skip{.aborted = ab.filter()};
        for (const int64_t cap : {int64_t{1}, int64_t{3}, int64_t{1} << 40}) {
            ProjRig cur, old;
            int64_t curC = 0, oldC = 0;
            rune::view::FoldStats cs{}, os{};
            int guard = 0;
            const rune::storage::LogReadView lv{log.bytes.data(), log.bytes.size(), 0, log.next};
            while (curC < log.next && guard++ < 2000) {
                const int64_t bound = std::min(log.next, curC + cap);
                const auto a = rune::view::foldBatch(cur.comp.plan, cur.comp.rowLayout, cur.vdc, cur.table, lv, bound, curC, skip);
                cs.applied += a.applied; cs.deleted += a.deleted; cs.skippedControl += a.skippedControl;
                cs.skippedAborted += a.skippedAborted; cs.skippedMalformed += a.skippedMalformed;
                const int64_t before = oldC;
                const auto b = legacy_fold::foldBatch(old.comp.plan, old.comp.rowLayout, old.vdc, old.table, lv, bound, oldC, skip);
                os.applied += b.applied; os.deleted += b.deleted; os.skippedControl += b.skippedControl;
                os.skippedAborted += b.skippedAborted; os.skippedMalformed += b.skippedMalformed;
                ASSERT_EQ(curC, oldC) << "seed " << seed << " cap " << cap << " from " << before;
            }
            ASSERT_LT(guard, 2000);
            EXPECT_EQ(snapshot(cur.table, cur.comp.rowLayout.rowLen), snapshot(old.table, old.comp.rowLayout.rowLen)) << "seed " << seed;
            EXPECT_EQ(cs.applied, os.applied); EXPECT_EQ(cs.deleted, os.deleted);
            EXPECT_EQ(cs.skippedAborted, os.skippedAborted); EXPECT_EQ(cs.skippedControl, os.skippedControl);
        }
    }
}

// ---- Task 2 -----------------------------------------------------------------------------------------------------

namespace {
struct AggProspect {
    AggRig rig;
    rune::view::Prospect p;
    explicit AggProspect(const rune::view::ViewPlan& plan) : rig(plan) { p.reserve(rig.comp.rowLayout, &rig.agg); }
    rune::view::PrepareStatus prepare(int64_t id, int64_t user, int64_t amount, bool tomb = false) {
        const auto k = keyBytes(uint64_t(id)); const auto v = order(id, user, amount);
        auto f = rig.fold();
        key_ = k; val_ = v;
        return rune::view::prepareAggregate(f, {sv(key_), tomb ? std::string_view{} : sv(val_), 0, 0, nullptr}, p);
    }
    void apply() { auto f = rig.fold(); rune::view::applyAggregate(f, {}, p); }
    void put(int64_t id, int64_t user, int64_t amount, bool tomb = false) {
        ASSERT_EQ(prepare(id, user, amount, tomb), rune::view::PrepareStatus::Ok);
        apply();
    }
    std::vector<uint8_t> key_, val_;
    struct All { Snapshot v, c, g, b; bool operator==(const All&) const = default; };
    All all() {
        return {probe(rig.view, rig.comp.rowLayout.rowLen, 8), probe(rig.contributors, rig.agg.contributorRow.rowLen, 32),
                probe(rig.groups, rig.agg.groupRow.rowLen, 8), Snapshot{{rig.blocks.size(), {}}}};
    }
    int64_t col(const rune::view::ProspectRow& r, uint16_t slot) { return rune::view::readSlot(r.image, rig.comp.rowLayout, slot).i; }
};
}  // namespace

TEST(CheckedWritesPrepareApply, AggregatePrepareWritesNothing) {
    AggProspect a(userTotals());
    a.put(1, 100, 10);
    const auto before = a.all();
    ASSERT_EQ(a.prepare(2, 100, 30), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(a.p.op, rune::view::ProspectOp::Upsert);
    EXPECT_EQ(a.col(a.p.rows[0], 1), 2);
    EXPECT_EQ(a.col(a.p.rows[0], 2), 40);
    EXPECT_EQ(a.all(), before);
}

TEST(CheckedWritesPrepareApply, AggregateMoveProspectsBothGroups) {
    AggProspect a(userTotals());
    a.put(1, 100, 10);
    ASSERT_EQ(a.prepare(1, 200, 10), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(a.p.op, rune::view::ProspectOp::Move);
    EXPECT_EQ(a.p.rows[0].key.asU64(), 200u);
    EXPECT_EQ(a.col(a.p.rows[0], 1), 1);
    EXPECT_EQ(a.p.rows[1].key.asU64(), 100u);
    EXPECT_TRUE(a.p.rows[1].removed);
    a.apply();
    EXPECT_EQ(a.rig.view.get(100), nullptr);
    ASSERT_NE(a.rig.view.get(200), nullptr);
}

TEST(CheckedWritesPrepareApply, MaxRetractionRescansReadOnly) {
    AggProspect a(userMax());
    a.put(1, 100, 10);
    a.put(2, 100, 30);
    const size_t blocksBefore = a.rig.blocks.size();
    ASSERT_EQ(a.prepare(2, 100, 0, /*tomb*/ true), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(a.p.op, rune::view::ProspectOp::Erase);
    EXPECT_EQ(a.col(a.p.rows[0], 2), 10);
    EXPECT_EQ(a.rig.blocks.size(), blocksBefore);
    EXPECT_EQ(rune::view::readSlot(a.rig.view.get(100), a.rig.comp.rowLayout, 2).i, 30);
}

TEST(CheckedWritesPrepareApplyRegression, AggregateFoldIsByteIdenticalToLegacyRegression) {
    const rune::view::ViewPlan plans[] = {userTotals(), userMax(), userLast()};
    for (const auto& plan : plans) {
        for (uint64_t seed = 1; seed <= 200; ++seed) {
            std::mt19937_64 rng(seed);
            Log log;
            for (int i = 0; i < 48; ++i) {
                const auto roll = rng() % 10;
                if (roll == 0) log.control(); else log.orders(rng, 1 + int(rng() % 8), roll == 1 ? 5 : (roll == 2 ? 6 : -1));
            }
            const AbortedPid ab{5};
            const rune::view::FoldSkip skip{.aborted = ab.filter()};
            AggRig legacy(plan), direct(plan);
            AggProspect loop(plan);
            const rune::storage::LogReadView lv{log.bytes.data(), log.bytes.size(), 0, log.next};
            int64_t lc = 0, dc = 0;
            const auto ls = legacy_fold::foldAggregateBatch(legacy.comp.plan, legacy.comp.rowLayout, legacy.agg, legacy.vdc, legacy.view,
                                                           legacy.contributors, legacy.groups, legacy.blk(), lv, log.next, lc, skip);
            const auto ds = rune::view::foldAggregateBatch(direct.comp.plan, direct.comp.rowLayout, direct.agg, direct.vdc, direct.view,
                                                          direct.contributors, direct.groups, direct.blk(), lv, log.next, dc, skip);
            rune::view::FoldStats loopStats{};
            forEachBatch(log.bytes, [&](const uint8_t* b, size_t len, rune::protocol::RecordBatchReader& r) {
                if ((r.attributes() & rune::protocol::batch::kAttrControl) != 0) { ++loopStats.skippedControl; return; }
                if (skip.aborted.skips(r, r.attributes())) { ++loopStats.skippedAborted; return; }
                rune::protocol::forEachRecordSpan(b, len, [&](const rune::protocol::RecordSpan& s) {
                    auto f = loop.rig.fold();
                    const rune::view::SourceRecord rec{s.key, s.value, s.offset, 0, nullptr};
                    const auto st = rune::view::prepareAggregate(f, rec, loop.p);
                    ASSERT_NE(st, rune::view::PrepareStatus::CapacityExhausted);
                    rune::view::applyAggregate(f, rec, loop.p);
                    loopStats.applied += loop.p.stats.applied; loopStats.deleted += loop.p.stats.deleted;
                    loopStats.skippedMalformed += loop.p.stats.skippedMalformed;
                    loopStats.aggInconsistent += loop.p.stats.aggInconsistent;
                });
            });
            ASSERT_EQ(lc, dc);
            EXPECT_EQ(ls.applied, ds.applied); EXPECT_EQ(ls.deleted, ds.deleted);
            EXPECT_EQ(ls.skippedAborted, ds.skippedAborted); EXPECT_EQ(ls.aggInconsistent, ds.aggInconsistent);
            EXPECT_EQ(loopStats.applied, ds.applied) << "seed " << seed; EXPECT_EQ(loopStats.deleted, ds.deleted);
            EXPECT_EQ(loopStats.skippedAborted, ds.skippedAborted); EXPECT_EQ(loopStats.aggInconsistent, ds.aggInconsistent);
            const auto snap = [](AggRig& r) {
                return std::array<Snapshot, 4>{snapshot(r.view, r.comp.rowLayout.rowLen), snapshot(r.contributors, r.agg.contributorRow.rowLen),
                                               snapshot(r.groups, r.agg.groupRow.rowLen), snapshot(r.blocks, r.agg.blockRow.rowLen)};
            };
            const auto sl = snap(legacy), sd = snap(direct), sp = snap(loop.rig);
            for (size_t t = 0; t < 4; ++t) {
                EXPECT_EQ(sl[t], sd[t]) << "direct vs legacy, table " << t << " seed " << seed;
                EXPECT_EQ(sl[t], sp[t]) << "prepare+apply vs legacy, table " << t << " seed " << seed;
            }
        }
    }
}

TEST(CheckedWritesPrepareApply, OverlayOverflowIsCapacityExhaustedNeverAVerdict) {
    AggProspect a(userTotals());
    a.put(1, 100, 10);
    a.p.overlay.reserve(a.rig.comp.rowLayout, a.rig.agg, 1);   // one slot per table: a Move touches two groups
    const auto before = a.all();
    EXPECT_EQ(a.prepare(1, 200, 10), rune::view::PrepareStatus::CapacityExhausted);
    EXPECT_EQ(a.all(), before);
}

// ---- Task 4 -----------------------------------------------------------------------------------------------------

namespace {
rune::storage::RejectedOffsetIndex rejecting(std::initializer_list<int64_t> offsets) {
    rune::storage::RejectedOffsetIndex idx;
    idx.reserve(16);
    for (const int64_t o : offsets) EXPECT_TRUE(idx.add(o, 55, 1));
    return idx;
}
}  // namespace

TEST(CheckedWritesPrepareApplyRegression, FoldSkipsRejectedRecordsInBothFoldsRegression) {
    // Six single-record batches, ids 0..5, all in group 7; the index rejects offsets 2 and 4.
    std::vector<uint8_t> proj, aggLog;
    for (int64_t i = 0; i < 6; ++i) {
        const auto k = keyBytes(uint64_t(i));
        const auto cv = character(i, int32_t(10 + i));
        rune::protocol::RecordBatchBuilder pb(i, 0);
        pb.append(sv(k), sv(cv), 0);
        const auto p = pb.build();
        proj.insert(proj.end(), p.begin(), p.end());
        const auto ov = order(i, 7, 5);
        rune::protocol::RecordBatchBuilder ab(i, 0);
        ab.append(sv(k), sv(ov), 0);
        const auto a = ab.build();
        aggLog.insert(aggLog.end(), a.begin(), a.end());
    }
    const auto idx = rejecting({2, 4});
    const rune::view::FoldSkip skip{.rejected = &idx};

    ProjRig pr;
    int64_t pc = 0;
    const auto ps = rune::view::foldBatch(pr.comp.plan, pr.comp.rowLayout, pr.vdc, pr.table, {proj.data(), proj.size(), 0, 6}, 6, pc, skip);
    EXPECT_EQ(pr.table.size(), 4u);
    EXPECT_EQ(pr.table.get(2), nullptr);
    EXPECT_EQ(pr.table.get(4), nullptr);
    EXPECT_EQ(ps.skippedRejected, 2u);
    EXPECT_EQ(pc, 6);

    AggRig ar(userTotals());
    int64_t ac = 0;
    const auto as = rune::view::foldAggregateBatch(ar.comp.plan, ar.comp.rowLayout, ar.agg, ar.vdc, ar.view, ar.contributors, ar.groups,
                                                  {aggLog.data(), aggLog.size(), 0, 6}, 6, ac, skip);
    EXPECT_EQ(rune::view::readSlot(ar.view.get(7), ar.comp.rowLayout, 1).i, 4);
    EXPECT_EQ(as.skippedRejected, 2u);
    EXPECT_EQ(ac, 6);
}

TEST(CheckedWritesPrepareApplyRegression, RejectedSkipHonoursThePassBoundRegression) {
    std::vector<uint8_t> proj;
    for (int64_t i = 0; i < 6; ++i) {
        const auto k = keyBytes(uint64_t(i));
        const auto cv = character(i, 1);
        rune::protocol::RecordBatchBuilder pb(i, 0);
        pb.append(sv(k), sv(cv), 0);
        const auto p = pb.build();
        proj.insert(proj.end(), p.begin(), p.end());
    }
    const auto idx = rejecting({2, 4});
    const rune::view::FoldSkip skip{.rejected = &idx};
    ProjRig pr;
    int64_t cursor = 0;
    uint64_t skipped = 0;
    for (int64_t bound = 1; bound <= 6; ++bound) {
        const auto st = rune::view::foldBatch(pr.comp.plan, pr.comp.rowLayout, pr.vdc, pr.table, {proj.data(), proj.size(), 0, 6}, bound, cursor, skip);
        skipped += st.skippedRejected;
        EXPECT_EQ(cursor, bound);
    }
    EXPECT_EQ(skipped, 2u);
    EXPECT_EQ(pr.table.size(), 4u);
}
