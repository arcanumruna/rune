// libs/view/tests/view/CheckedWritesTxnOverlayTest.cpp
//
// Part 11 Task 3: chained prepares into a TxnOverlay, the lock set and PrepareStatus::Locked.  The key-index case is in
// CheckedWritesKeyIndexTest.cpp (the key index lives in rune_node).
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/view/AggFold.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/TxnOverlay.h>
#include <rune/view/UniqueClaims.h>
#include <rune/view/ViewFold.h>

#include "AllocWindow.h"

namespace {

using rune::codec::PrimType;
using namespace rune::view;

constexpr int32_t kViewTopic = 0x40000007;

void le16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); }
void le64(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8 * i))); }
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }
std::vector<uint8_t> keyBytes(uint64_t id) { std::vector<uint8_t> b; le64(b, id); return b; }

// ---- aggregate fixture: order {orderId key, userId, amount} ---------------------------------------------------------
rune::codec::Descriptor orderSource() {
    return rune::codec::DescriptorBuilder("order", 1).add("orderId", PrimType::Int64, 1, true).add("userId", PrimType::Int64, 1).add("amount", PrimType::Int64, 1).build();
}
ViewPlan userTotals() {   // slots: 0 userId | 1 n | 2 total
    ViewPlan v;
    v.viewName = "user_totals"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", PrimType::Int64, 0, "userId", MergeRule::Replace}, {"n", PrimType::Int64, 0, "", MergeRule::CountAll}, {"total", PrimType::Int64, 0, "amount", MergeRule::Sum}};
    return v;
}
ViewPlan userMax() {      // slots: 0 userId | 1 n | 2 mx (value blocks)
    ViewPlan v;
    v.viewName = "user_max"; v.key = "userId"; v.sources = {{5, "userId"}};
    v.output = {{"userId", PrimType::Int64, 0, "userId", MergeRule::Replace}, {"n", PrimType::Int64, 0, "", MergeRule::CountAll}, {"mx", PrimType::Int64, 0, "amount", MergeRule::Max}};
    return v;
}
std::vector<uint8_t> order(int64_t id, int64_t user, int64_t amount) {
    std::vector<uint8_t> b;
    le16(b, 24); le16(b, 1); le64(b, uint64_t(id)); le64(b, uint64_t(user)); le64(b, uint64_t(amount));
    return b;
}

struct AggRig {
    rune::codec::Descriptor src = orderSource();
    CompileResult comp;
    AggLayout agg;
    VersionDescriptorCache vdc{src};
    ViewTable view, contributors, groups, blocks;
    Prospect p;
    TxnOverlay chain;
    explicit AggRig(const ViewPlan& plan)
        : comp(compile(plan, src)), agg(buildAggLayout(comp.plan, src).value()),
          view(comp.rowLayout, 256, 1u << 16), contributors(agg.contributorRow, 256, 1u << 16),
          groups(agg.groupRow, 256, 1u << 16), blocks(agg.blockRow, 256, 1u << 16) {
        p.reserve(comp.rowLayout, &agg);
        chain.reserve(1024, std::max({comp.rowLayout.rowLen, agg.contributorRow.rowLen, agg.groupRow.rowLen, agg.blockRow.rowLen}));
    }
    static CompileResult compile(const ViewPlan& plan, const rune::codec::Descriptor& s) {
        const rune::codec::Descriptor* srcs[] = {&s};
        return compileView(plan, srcs, 9).value();
    }
    [[nodiscard]] AggregateFold fold(const TxnLockSet* locks = nullptr) {
        AggregateFold f{comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, agg.numBlockCols > 0 ? &blocks : nullptr};
        f.locks = locks;
        return f;
    }
    // Prepare (and apply when `chain` is null) one order; user < 0 is a tombstone.
    PrepareStatus prepare(int64_t id, int64_t user, int64_t amount, TxnOverlay* ch, bool apply = false, const TxnLockSet* locks = nullptr) {
        const auto k = keyBytes(uint64_t(id));
        const auto v = user < 0 ? std::vector<uint8_t>{} : order(id, user, amount);
        const SourceRecord rec{sv(k), sv(v), 0, 0, nullptr};
        AggregateFold f = fold(locks);
        const PrepareStatus st = prepareAggregate(f, rec, p, ch);
        if (apply && st == PrepareStatus::Ok) applyAggregate(f, rec, p);
        return st;
    }
    [[nodiscard]] int64_t col(const ProspectRow& r, uint16_t slot) const { return readSlot(r.image, comp.rowLayout, slot).i; }
};

using Snapshot = std::map<uint64_t, std::vector<uint8_t>>;
Snapshot probe(const ViewTable& t, uint32_t rowLen, uint64_t keys) {
    Snapshot s;
    for (uint64_t k = 0; k < keys; ++k)
        if (const uint8_t* r = t.get(k)) s[k] = std::vector<uint8_t>(r, r + rowLen);
    return s;
}
Snapshot drained(ViewTable& t, uint32_t rowLen) {
    Snapshot s;
    t.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t key) {
        if (!deleted) s[key] = std::vector<uint8_t>(row, row + rowLen);
    });
    return s;
}

// ---- unique projection fixture: character(characterId u64 key, gender u16, name string(16)), unique (lower(name)) ----
rune::codec::Descriptor charSource() {
    rune::codec::DescriptorBuilder b("character", 1);
    b.add("characterId", PrimType::UInt64, 1, true);
    b.add("gender", PrimType::UInt16);
    b.addFixedStr("name", 16, 1);
    return b.build();
}
std::vector<uint8_t> character(const rune::codec::Descriptor& d, uint64_t id, const std::string& name) {
    std::vector<uint8_t> rec(4 + d.blockLength, 0);
    rec[0] = uint8_t(d.blockLength); rec[1] = uint8_t(d.blockLength >> 8); rec[2] = uint8_t(d.version);
    const auto* idf = d.find("characterId");
    for (int i = 0; i < 8; ++i) rec[4 + idf->offset + i] = uint8_t(id >> (8 * i));
    const auto* nf = d.find("name");
    for (size_t i = 0; i < name.size(); ++i) rec[4 + nf->offset + i] = uint8_t(name[i]);
    return rec;
}
ViewKey nameKey(const std::string& lowered) {
    ViewKey k;
    k.len = 24;
    std::copy(lowered.begin(), lowered.end(), k.bytes.begin());
    return k;
}

struct UniqueRig {
    rune::codec::Descriptor src = charSource();
    bool unique;
    CompileResult comp = [&] {
        ViewPlan v;
        v.viewName = "character_name";
        v.sources = {{5, "characterId"}};
        v.unicodeVersion = rune::codec::kUnicodeVersion;
        v.unique = unique;
        v.keyComponents = {{"name", 1, "nameKey"}};
        if (!unique) v.keyComponents.push_back({"characterId", 0, "characterId"});   // identifies the record: the view is owner-mapped, not unique
        else v.output.push_back({"characterId", PrimType::UInt64, 0, "characterId", MergeRule::Replace});
        v.output.push_back({"gender", PrimType::UInt16, 0, "gender", MergeRule::Replace});
        const rune::codec::Descriptor* srcs[] = {&src};
        return compileView(v, srcs, kViewTopic).value();
    }();
    KeyedFoldSpec spec = *buildKeyedFoldSpec(comp.plan, src);
    VersionDescriptorCache vdc{src};
    ViewTable table{comp.rowLayout, 256, 1u << 16, {uint8_t(spec.keyWidth), uint8_t(spec.prefixWidth), 7}};
    ViewTable owners{[&] { RowLayout l; l.rowLen = spec.keyWidth; return l; }(), 256, 1u << 16, {spec.wireKeyWidth, 0, 7}};
    UniqueClaims claims{spec.keyWidth, spec.wireKeyWidth, comp.rowLayout.rowLen, PrimType::UInt64, 7, 256, 1u << 16};
    Prospect p;
    TxnOverlay chain;
    explicit UniqueRig(bool uniq = true) : unique(uniq) {
        p.reserve(comp.rowLayout, nullptr);
        chain.reserve(256, 256);
    }
    PrepareStatus prepare(uint64_t id, const std::string& name, TxnOverlay* ch, bool apply = false, const TxnLockSet* locks = nullptr) {
        const auto k = keyBytes(id);
        const auto v = name.empty() ? std::vector<uint8_t>{} : character(src, id, name);
        const SourceRecord rec{sv(k), sv(v), 0, 0, nullptr};
        ProjectionFold f{comp.plan, comp.rowLayout, vdc, table, PrimType::UInt64, &spec, &owners, unique ? &claims : nullptr};
        f.locks = locks;
        const PrepareStatus st = prepareProjection(f, rec, p, ch);
        if (apply && st == PrepareStatus::Ok) applyProjection(f, rec, p);
        return st;
    }
};

TxnLockSet lockSetOf(const TxnOverlay& o) {
    TxnLockSet l;
    l.locked[0] = &o;
    l.count = 1;
    return l;
}

}  // namespace

TEST(CheckedWritesTxnOverlay, ChainedAggregatePrepareSeesTheEarlierRecord) {
    AggRig r(userTotals());
    for (int64_t id = 1; id <= 11; ++id) ASSERT_EQ(r.prepare(id, 100, 1, nullptr, true), PrepareStatus::Ok);
    const auto view = probe(r.view, r.comp.rowLayout.rowLen, 300), contributors = probe(r.contributors, r.agg.contributorRow.rowLen, 2000),
               groups = probe(r.groups, r.agg.groupRow.rowLen, 300);

    ASSERT_EQ(r.prepare(1001, 100, 5, &r.chain), PrepareStatus::Ok);
    EXPECT_EQ(r.col(r.p.rows[0], 1), 12);
    ASSERT_EQ(r.prepare(1002, 100, 5, &r.chain), PrepareStatus::Ok);
    EXPECT_EQ(r.col(r.p.rows[0], 1), 13);
    EXPECT_EQ(r.col(r.p.rows[0], 2), 21);
    EXPECT_TRUE(r.p.rows[0].existed);

    EXPECT_EQ(probe(r.view, r.comp.rowLayout.rowLen, 300), view);
    EXPECT_EQ(probe(r.contributors, r.agg.contributorRow.rowLen, 2000), contributors);
    EXPECT_EQ(probe(r.groups, r.agg.groupRow.rowLen, 300), groups);
}

TEST(CheckedWritesTxnOverlay, ChainedProjectionRenameThenReuse) {
    UniqueRig r;
    ASSERT_EQ(r.prepare(7, "foo", nullptr, true), PrepareStatus::Ok);
    const size_t tableSize = r.table.size(), ownersSize = r.owners.size(), indexSize = r.claims.indexTable().size();

    ASSERT_EQ(r.prepare(7, "bar", &r.chain), PrepareStatus::Ok);
    EXPECT_FALSE(r.p.rows[0].ownedByOther);
    ASSERT_EQ(r.prepare(9, "foo", &r.chain), PrepareStatus::Ok);
    EXPECT_FALSE(r.p.rows[0].ownedByOther);

    // Without the chain "foo" is still c7's.
    ASSERT_EQ(r.prepare(9, "foo", nullptr), PrepareStatus::Ok);
    EXPECT_TRUE(r.p.rows[0].ownedByOther);

    EXPECT_EQ(r.table.size(), tableSize);
    EXPECT_EQ(r.owners.size(), ownersSize);
    EXPECT_EQ(r.claims.indexTable().size(), indexSize);
    EXPECT_NE(r.table.get(nameKey("foo")), nullptr);
    EXPECT_EQ(r.table.get(nameKey("bar")), nullptr);
}

TEST(CheckedWritesTxnOverlay, LockedRowReportsLocked) {
    AggRig r(userTotals());
    ASSERT_EQ(r.prepare(1, 100, 5, nullptr, true), PrepareStatus::Ok);
    TxnOverlay held;
    held.reserve(64, r.chain.rows() + 512);
    ASSERT_EQ(r.prepare(2, 100, 5, &held), PrepareStatus::Ok);
    const TxnLockSet locks = lockSetOf(held);
    const auto groups = probe(r.groups, r.agg.groupRow.rowLen, 300), view = probe(r.view, r.comp.rowLayout.rowLen, 300);

    EXPECT_EQ(r.prepare(3, 100, 5, nullptr, false, &locks), PrepareStatus::Locked);
    EXPECT_EQ(probe(r.groups, r.agg.groupRow.rowLen, 300), groups);
    EXPECT_EQ(probe(r.view, r.comp.rowLayout.rowLen, 300), view);
    EXPECT_EQ(r.prepare(3, 200, 5, nullptr, false, &locks), PrepareStatus::Ok);
    EXPECT_EQ(r.prepare(1, -1, 0, nullptr, false, &locks), PrepareStatus::Locked);   // retracting from the locked group
}

TEST(CheckedWritesTxnOverlay, LockedNeverReportedWithoutALockSet) {
    AggRig r(userTotals());
    TxnOverlay held;
    held.reserve(64, 512);
    ASSERT_EQ(r.prepare(2, 100, 5, &held), PrepareStatus::Ok);
    EXPECT_EQ(r.prepare(3, 100, 5, nullptr), PrepareStatus::Ok);
}

TEST(CheckedWritesTxnOverlay, UniqueClaimsAreLockedToo) {
    UniqueRig r;
    TxnOverlay held;
    held.reserve(64, 256);
    ASSERT_EQ(r.prepare(1, "foo", &held), PrepareStatus::Ok);
    const TxnLockSet locks = lockSetOf(held);
    EXPECT_EQ(r.prepare(2, "foo", nullptr, false, &locks), PrepareStatus::Locked);
    EXPECT_EQ(r.prepare(2, "bar", nullptr, false, &locks), PrepareStatus::Ok);
    EXPECT_EQ(r.table.size(), 0u);
}

TEST(CheckedWritesTxnOverlay, ErasingAnAbsentRowLocksIt) {
    ViewTable t(RowLayout{.rowLen = 8}, 64, 1u << 12);
    TxnOverlay chain;
    chain.reserve(4, 8);
    TxnAccess x{&chain, nullptr, 0};
    TxnTable table(&t, OverlayTable::View, &x);
    EXPECT_FALSE(table.erase(ViewKey::fromU64(5)));
    EXPECT_FALSE(table.erase(uint64_t{6}));
    EXPECT_TRUE(chain.contains(0, OverlayTable::View, ViewKey::fromU64(5)));
    EXPECT_TRUE(chain.contains(0, OverlayTable::View, ViewKey::fromU64(6)));
    EXPECT_EQ(t.size(), 0u);
}

TEST(CheckedWritesTxnOverlay, ReadingAnAbsentRowLocksIt) {
    AggRig r(userTotals());
    ASSERT_EQ(r.prepare(5, -1, 0, &r.chain), PrepareStatus::Ok);   // a tombstone of an unknown contributor
    const TxnLockSet locks = lockSetOf(r.chain);
    EXPECT_EQ(r.prepare(5, 100, 1, nullptr, false, &locks), PrepareStatus::Locked);
    EXPECT_EQ(r.prepare(6, 100, 1, nullptr, false, &locks), PrepareStatus::Ok);
}

TEST(CheckedWritesTxnOverlay, AChainDoesNotLockItself) {
    AggRig r(userTotals());
    ASSERT_EQ(r.prepare(1, 100, 1, &r.chain), PrepareStatus::Ok);
    const TxnLockSet locks = lockSetOf(r.chain);
    EXPECT_EQ(r.prepare(2, 100, 1, &r.chain, false, &locks), PrepareStatus::Ok);
}

TEST(CheckedWritesTxnOverlay, RowLongerThanTheReservationIsFullNotCorruption) {
    TxnOverlay o;
    o.reserve(4, 8);
    EXPECT_EQ(o.put(0, OverlayTable::View, ViewKey::fromU64(1), nullptr, 16), nullptr);
    EXPECT_TRUE(o.full());
}

TEST(CheckedWritesTxnOverlay, OverlayRowsShorterThanATableRowAreCapacityExhausted) {
    AggRig a(userTotals());
    TxnOverlay tiny;
    tiny.reserve(64, 1);
    EXPECT_EQ(a.prepare(1, 100, 5, &tiny), PrepareStatus::CapacityExhausted);
    EXPECT_EQ(tiny.rows(), 0u);
    UniqueRig u;
    TxnOverlay narrow;
    narrow.reserve(16, 8);
    EXPECT_EQ(u.prepare(1, "foo", &narrow), PrepareStatus::CapacityExhausted);
    EXPECT_EQ(narrow.rows(), 0u);
    for (std::size_t i = 0; i < narrow.maxRowLen(); ++i) EXPECT_EQ(narrow.spare()[i], 0) << i;   // nothing was put into the spare row
}

TEST(CheckedWritesTxnOverlay, ARowThatDecidesAConflictIsLocked) {
    UniqueRig u(false);   // owner-mapped, not unique
    ASSERT_EQ(u.prepare(2, "foo", nullptr), PrepareStatus::Ok);
    const ViewKey n = u.p.rows[0].key;
    // A stored row at n that c2 does not own, and c5 owning n: the shape a key conflict decides on.
    u.table.put(n);
    std::memcpy(u.owners.put(ViewKey::fromBytes(keyBytes(5))), n.bytes.data(), u.spec.keyWidth);
    ASSERT_EQ(u.prepare(2, "foo", &u.chain), PrepareStatus::Ok);
    EXPECT_EQ(u.p.stats.keyConflicts, 1u);
    const TxnLockSet locks = lockSetOf(u.chain);
    EXPECT_EQ(u.prepare(5, "", nullptr, false, &locks), PrepareStatus::Locked);   // c5 leaving would free the row c2 lost to
    EXPECT_EQ(u.prepare(3, "", nullptr, false, &locks), PrepareStatus::Ok);
    EXPECT_NE(u.table.get(n), nullptr);
}

TEST(CheckedWritesTxnOverlay, ARowAnotherSourceOwnsIsLockedForUniqueToo) {
    UniqueRig u;
    ASSERT_EQ(u.prepare(1, "foo", nullptr, true), PrepareStatus::Ok);
    ASSERT_EQ(u.prepare(2, "foo", &u.chain), PrepareStatus::Ok);
    EXPECT_TRUE(u.p.rows[0].ownedByOther);
    const TxnLockSet locks = lockSetOf(u.chain);
    EXPECT_EQ(u.prepare(1, "", nullptr, false, &locks), PrepareStatus::Locked);
}

TEST(CheckedWritesTxnOverlay, OwnerMappedMineThenOutsideMoveIsLocked) {
    UniqueRig u(false);
    ASSERT_EQ(u.prepare(1, "foo", nullptr, true), PrepareStatus::Ok);
    ASSERT_EQ(u.prepare(1, "foo", &u.chain), PrepareStatus::Ok);   // updated in place: the row it holds is the one it owns
    const TxnLockSet locks = lockSetOf(u.chain);
    EXPECT_EQ(u.prepare(1, "bar", nullptr, false, &locks), PrepareStatus::Locked);   // a Move off that row
    EXPECT_EQ(u.prepare(1, "", nullptr, false, &locks), PrepareStatus::Locked);
    EXPECT_EQ(u.prepare(2, "bar", nullptr, false, &locks), PrepareStatus::Ok);
}

TEST(CheckedWritesTxnOverlay, UniqueMineThenOutsideDeleteIsLocked) {
    UniqueRig u;
    ASSERT_EQ(u.prepare(1, "foo", nullptr, true), PrepareStatus::Ok);
    ASSERT_EQ(u.prepare(1, "foo", &u.chain), PrepareStatus::Ok);
    const TxnLockSet locks = lockSetOf(u.chain);
    EXPECT_EQ(u.prepare(1, "", nullptr, false, &locks), PrepareStatus::Locked);
}

TEST(CheckedWritesTxnOverlay, UniqueMineThenOutsideMoveIsLocked) {
    UniqueRig u;
    ASSERT_EQ(u.prepare(1, "foo", nullptr, true), PrepareStatus::Ok);
    ASSERT_EQ(u.prepare(1, "foo", &u.chain), PrepareStatus::Ok);
    const TxnLockSet locks = lockSetOf(u.chain);
    EXPECT_EQ(u.prepare(1, "bar", nullptr, false, &locks), PrepareStatus::Locked);
    EXPECT_EQ(u.prepare(2, "bar", nullptr, false, &locks), PrepareStatus::Ok);
}

TEST(CheckedWritesTxnOverlay, AggregateAbsentTombstoneOnAFullChainIsCapacityExhausted) {
    AggRig r(userTotals());
    TxnOverlay none;
    none.reserve(0, 512);   // the absent-contributor lock has no room
    EXPECT_EQ(r.prepare(5, -1, 0, &none), PrepareStatus::CapacityExhausted);
    EXPECT_TRUE(none.full());
}

TEST(CheckedWritesTxnOverlay, FullOverlayIsCapacityExhausted) {
    AggRig r(userTotals());
    ASSERT_EQ(r.prepare(1, 100, 5, nullptr, true), PrepareStatus::Ok);
    TxnOverlay tiny;
    tiny.reserve(2, 512);
    EXPECT_EQ(r.prepare(1, 200, 5, &tiny), PrepareStatus::CapacityExhausted);   // a Move: contributor, two groups, two view rows
    EXPECT_TRUE(tiny.full());
}

// The resident pages among those holding [p, p + bytes).
static std::size_t residentPages(const void* p, std::size_t bytes) {
    const auto page = static_cast<uintptr_t>(sysconf(_SC_PAGESIZE));
    const uintptr_t from = reinterpret_cast<uintptr_t>(p) & ~(page - 1);
    const std::size_t len = reinterpret_cast<uintptr_t>(p) + bytes - from;
#if defined(__APPLE__)
    std::vector<char> vec((len + page - 1) / page);
#else
    std::vector<unsigned char> vec((len + page - 1) / page);
#endif
    EXPECT_EQ(mincore(reinterpret_cast<void*>(from), len, vec.data()), 0);
    return static_cast<std::size_t>(std::count_if(vec.begin(), vec.end(), [](auto v) { return (v & 1) != 0; }));
}

// T7 perf: a reserve sized for the largest transaction writes none of its rows.  Filling them at every restart and member change
// made a checked-writes model seed about 10x slower; the row pages stay unmapped until a transaction puts a row there.  The same holds
// for the entries (review 5, M4).
TEST(CheckedWritesTxnOverlayRegression, ReserveTouchesNoRowRegression) {
    TxnOverlay o;
    o.reserve(std::size_t(1) << 20, 256);   // 256 MiB of rows
    uint8_t* last = o.spare() - 256;   // the last row
    EXPECT_EQ(residentPages(last, 1), 0u) << "the reserve wrote the rows";
    EXPECT_EQ(residentPages(o.lastEntryForTest(), 1), 0u) << "the reserve wrote the entries";
    uint8_t* row = o.put(1, OverlayTable::View, ViewKey::fromU64(7), nullptr, 64);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(residentPages(row, 1), 1u);
    EXPECT_EQ(row[63], 0) << "a put without a base row is zeroes";
}

// A calloc'd arena was zeroed again whenever the allocator handed back a freed block: a fifth of a constraint model seed.  Run under
// a scribbling malloc (CheckedWritesTxnOverlaySpareRowUnderScribbleRegression), the rows keep the scribble and only the spare row is zeroed.
TEST(CheckedWritesTxnOverlayRegression, ReserveZeroesOnlyTheSpareRowRegression) {
    if (std::getenv("MallocScribble") == nullptr && std::getenv("MALLOC_PERTURB_") == nullptr) GTEST_SKIP() << "needs a scribbling malloc";
    constexpr std::size_t kRows = std::size_t(1) << 14;
    constexpr uint32_t kLen = 256;
    TxnOverlay o;
    o.reserve(kRows, kLen);
    const uint8_t* rows = o.spare() - kRows * kLen;
    EXPECT_NE(rows[0], 0) << "the reserve zeroed the rows";
    EXPECT_NE(rows[kRows * kLen - 1], 0) << "the reserve zeroed the rows";
    for (uint32_t i = 0; i < kLen; ++i) ASSERT_EQ(o.spare()[i], 0) << i;
}

// T7 review 5, N3: clear() empties only the index slots its rows took.  An atomic batch clears its overlay twice, and the index is
// sized for the largest transaction (8 MiB for one aggregate member), so a whole-index clear put that memset on the duty pass.
TEST(CheckedWritesTxnOverlayRegression, ClearTouchesOnlyTheSlotsItsRowsTookRegression) {
    TxnOverlay o;
    o.reserve(std::size_t(1) << 23, 1);   // a 64 MiB index: calloc maps it fresh on every allocator
    const std::size_t bytes = o.slotsForTest() * sizeof(int32_t);
    ASSERT_NE(o.put(1, OverlayTable::KeyIndex, ViewKey::fromU64(7), nullptr, 1), nullptr);
    o.clear();
    const std::size_t pages = bytes / static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
    EXPECT_LT(residentPages(o.indexForTest(), bytes), pages / 4) << "the clear wrote the whole index";
    EXPECT_FALSE(o.contains(1, OverlayTable::KeyIndex, ViewKey::fromU64(7)));
    ASSERT_NE(o.put(1, OverlayTable::KeyIndex, ViewKey::fromU64(8), nullptr, 1), nullptr);
    EXPECT_TRUE(o.contains(1, OverlayTable::KeyIndex, ViewKey::fromU64(8)));
    EXPECT_FALSE(o.contains(1, OverlayTable::KeyIndex, ViewKey::fromU64(7)));
}

TEST(CheckedWritesTxnOverlay, ReservedOverlayAllocatesNothing) {
    AggRig r(userMax());
    UniqueRig u;
    std::vector<std::vector<uint8_t>> keys, orders, chars;   // built outside the window
    const char* names[] = {"foo", "bar", "baz"};
    for (int i = 0; i < 16; ++i) {
        keys.push_back(keyBytes(uint64_t(i)));
        orders.push_back(order(i, 1 + i % 4, i * 7 % 37));
        chars.push_back(character(u.src, uint64_t(i), names[i % 3]));
    }
    const auto prepareBoth = [&](int i) {
        const SourceRecord a{sv(keys[i % 16]), sv(orders[(i * 5) % 16]), 0, 0, nullptr};
        AggregateFold af = r.fold();
        const PrepareStatus sa = prepareAggregate(af, a, r.p, &r.chain);
        const SourceRecord c{sv(keys[i % 16]), sv(chars[(i * 7) % 16]), 0, 0, nullptr};
        ProjectionFold pf{u.comp.plan, u.comp.rowLayout, u.vdc, u.table, PrimType::UInt64, &u.spec, &u.owners, &u.claims};
        return std::pair{sa, prepareProjection(pf, c, u.p, &u.chain)};
    };
    ASSERT_EQ(prepareBoth(0), (std::pair{PrepareStatus::Ok, PrepareStatus::Ok}));   // first sight of the writer version allocates
    TxnLockSet locks = lockSetOf(r.chain);
    rune::testing::AllocWindow w;
    for (int i = 0; i < 4096; ++i) {
        AggregateFold lf = r.fold(&locks);
        const SourceRecord lr{sv(keys[i % 16]), sv(orders[i % 16]), 0, 0, nullptr};
        (void)prepareAggregate(lf, lr, r.p);   // Ok or Locked, never an allocation
    }
    for (int i = 0; i < 4096; ++i) ASSERT_EQ(prepareBoth(i), (std::pair{PrepareStatus::Ok, PrepareStatus::Ok}));
    EXPECT_EQ(w.count(), 0u);
}

TEST(CheckedWritesTxnOverlayRegression, ChainedPreparesThenDirectApplyEqualSequentialFoldRegression) {
    for (const ViewPlan& plan : {userTotals(), userMax()}) {
        for (uint64_t seed = 1; seed <= 200; ++seed) {
            std::mt19937_64 rng(seed);
            struct Op { int64_t id; int64_t user; int64_t amount; };
            std::vector<Op> ops;
            for (size_t i = 0, n = 1 + rng() % 64; i < n; ++i)
                ops.push_back({int64_t(rng() % 16), rng() % 5 == 0 ? -1 : int64_t(1 + rng() % 4), int64_t(rng() % 50)});

            AggRig chained(plan), sequential(plan);
            for (const Op& o : ops) {
                ASSERT_EQ(chained.prepare(o.id, o.user, o.amount, &chained.chain), PrepareStatus::Ok) << "seed " << seed;
                ASSERT_EQ(sequential.prepare(o.id, o.user, o.amount, nullptr, true), PrepareStatus::Ok);
            }
            const std::array<Snapshot, 4> seq = {drained(sequential.view, sequential.comp.rowLayout.rowLen), drained(sequential.contributors, sequential.agg.contributorRow.rowLen),
                                                 drained(sequential.groups, sequential.agg.groupRow.rowLen), drained(sequential.blocks, sequential.agg.blockRow.rowLen)};

            // The chain's images, row by row; the tables themselves still hold the before-images.
            const auto imageOf = [&](OverlayTable id, const ViewTable& t, uint64_t key) -> const uint8_t* {
                bool erased = false;
                if (const uint8_t* c = chained.chain.get(0, id, ViewKey::fromU64(key), erased)) return c;
                return erased ? nullptr : t.get(key);
            };
            const auto check = [&](OverlayTable id, const ViewTable& t, uint32_t rowLen, uint64_t keys, const Snapshot& want) {
                for (uint64_t k = 0; k < keys; ++k) {
                    const uint8_t* got = imageOf(id, t, k);
                    const auto it = want.find(k);
                    ASSERT_EQ(got != nullptr, it != want.end()) << "table " << int(id) << " key " << k << " seed " << seed;
                    if (got != nullptr) EXPECT_EQ(std::vector<uint8_t>(got, got + rowLen), it->second) << "table " << int(id) << " key " << k << " seed " << seed;
                }
            };
            check(OverlayTable::View, chained.view, chained.comp.rowLayout.rowLen, 8, seq[0]);
            check(OverlayTable::Contributors, chained.contributors, chained.agg.contributorRow.rowLen, 16, seq[1]);
            check(OverlayTable::Groups, chained.groups, chained.agg.groupRow.rowLen, 8, seq[2]);
            for (const auto& [k, row] : seq[3]) {
                const uint8_t* got = imageOf(OverlayTable::Blocks, chained.blocks, k);
                ASSERT_NE(got, nullptr) << "blocks key " << k << " seed " << seed;
                EXPECT_EQ(std::vector<uint8_t>(got, got + row.size()), row);
            }
            EXPECT_EQ(chained.view.size() + chained.contributors.size() + chained.groups.size() + chained.blocks.size(), 0u);

            for (const Op& o : ops) ASSERT_EQ(chained.prepare(o.id, o.user, o.amount, nullptr, true), PrepareStatus::Ok);
            EXPECT_EQ(drained(chained.view, chained.comp.rowLayout.rowLen), seq[0]) << "seed " << seed;
            EXPECT_EQ(drained(chained.contributors, chained.agg.contributorRow.rowLen), seq[1]) << "seed " << seed;
            EXPECT_EQ(drained(chained.groups, chained.agg.groupRow.rowLen), seq[2]) << "seed " << seed;
            EXPECT_EQ(drained(chained.blocks, chained.agg.blockRow.rowLen), seq[3]) << "seed " << seed;
        }
    }
}
