// Lease topics Task 7: a lease topic's Revoke / Expire is a tombstone and a Checkpoint is invisible to every DATA fold
// (user projection, keyed projection, aggregate, verdict members other than the lease index; spec 6.6).
#include "CheckedWritesRig.h"

#include <rune/codec/Validator.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/view/AggFold.h>
#include <rune/view/KeyedFold.h>
#include <rune/view/ViewFold.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;
namespace rv  = rune::view;

namespace {

constexpr uint16_t kV1Columns = 8;
constexpr int32_t kProj = 40, kByAddr = 41, kCnt = 42;
constexpr uint8_t kRevokeAttr     = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);   // Revoke | existing
constexpr uint8_t kExpireAttr     = uint8_t(rec::LeaseOp::Expire) << rec::kAttrLeaseOpShift;
constexpr uint8_t kCheckpointAttr = uint8_t(rec::LeaseOp::Checkpoint) << rec::kAttrLeaseOpShift;

std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}

void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}

// [u16 blockLength][u16 schema version][version, ttl, holder, remaining, id, addr(16)].
// Lease expiry runs on the real lease clock: ttl and checkpoint remaining are minutes, so no lease expires mid-test even under
// TSan (a 5 s ttl / 1.234 s remaining expired lease 1 across the restart rebuild of PersistedRebuildFoldsLikeLiveRegression).
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t remaining = 0, uint64_t holder = 1, uint64_t ttl = 600'000) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, holder, 8);
    putLe(b, 28, remaining, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    return b;
}
RawRec grantRec(uint64_t id, const std::string& addr) { return {keyOf(id), leaseValue(id, 0, addr), 1}; }
RawRec updateRec(uint64_t id, int64_t ver, const std::string& addr) { return {keyOf(id), leaseValue(id, ver, addr, 0, 9), 2}; }
RawRec revokeRec(uint64_t id, int64_t ver, const std::string& addr) { return {keyOf(id), leaseValue(id, ver, addr), kRevokeAttr}; }
RawRec expireRec(uint64_t id, int64_t ver, const std::string& addr) { return {keyOf(id), leaseValue(id, ver, addr), kExpireAttr}; }
RawRec checkpointRec(uint64_t id, int64_t ver, const std::string& addr, uint64_t remaining = 543'210) {
    return {keyOf(id), leaseValue(id, ver, addr, remaining), kCheckpointAttr};
}

struct Ready {
    VerdictRig r;
    explicit Ready(int64_t rollBytes = 64LL * 1024 * 1024, std::optional<rune::codec::Descriptor> src = std::nullopt, uint32_t maxKeys = 1000, RigTuning tuning = {}) : r(false, rollBytes, tuning) {
        r.createLeaseTopic(maxKeys, std::move(src));
        r.passes(30);
    }
    bool send(const std::vector<RawRec>& recs) {
        r.demote();
        r.replicate({VerdictRig::rawBatch(recs)});
        r.promote();
        r.passes(30);
        return r.driveUntilJudged();
    }
};

struct Cols {
    std::vector<std::optional<uint64_t>> v;
    std::size_t                          slots = 0;
};
Cols colsOf(VerdictRig& r, uint64_t id, uint16_t count) {
    Cols c;
    for (uint16_t s = 0; s < count; ++s) c.v.push_back(r.vs->leaseColumnForTest(VerdictRig::kSrc, 0, id, s, &c.slots));
    return c;
}
int64_t liveVersion(VerdictRig& r, uint64_t id) {
    const Cols c = colsOf(r, id, kV1Columns);
    return c.v[0].has_value() ? int64_t(*c.v[0]) : -1;
}

// ── user views over the lease source ──
rv::ViewPlan projPlan() {
    rv::ViewPlan v;
    v.viewName = "proj"; v.key = "id"; v.sources = {{VerdictRig::kSrc, "id"}};
    v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace},
                {"addr", rune::codec::PrimType::FixedStr, 0, "addr", rv::MergeRule::Replace}};
    v.persistence = 1;
    return v;
}
rv::ViewPlan byAddrPlan() {
    rv::ViewPlan v;
    v.viewName = "byAddr"; v.sources = {{VerdictRig::kSrc, "id"}};
    // (addr, id): a key that does not include the source key is refused by the compiler unless the view is unique.
    v.keyComponents = {{"addr", 0, "addrKey"}, {"id", 0, "idKey"}};
    v.output = {{"addr", rune::codec::PrimType::FixedStr, 0, "addr", rv::MergeRule::Replace},
                {"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace}};
    v.persistence = 1;
    return v;
}
rv::ViewPlan cntPlan() {
    rv::ViewPlan v;
    v.viewName = "cnt"; v.key = "id"; v.sources = {{VerdictRig::kSrc, "id"}};
    v.output = {{"id", rune::codec::PrimType::UInt64, 0, "id", rv::MergeRule::Replace},
                {"n", rune::codec::PrimType::Int64, 0, "", rv::MergeRule::CountAll}};
    v.persistence = 1;
    return v;
}
rv::CompileResult compile(const rv::ViewPlan& v, int32_t topic) {
    static const rune::codec::Descriptor src = VerdictRig::leaseSourceDescriptor();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto c = rv::compileView(v, srcs, topic);
    EXPECT_TRUE(c.has_value()) << (c.has_value() ? "" : v.viewName + ": " + c.error().message);
    return std::move(c).value();
}

// A view key: a uint64 component is big-endian, a fixed string is zero padded to its width.
std::vector<uint8_t> idKey(uint64_t id) {
    std::vector<uint8_t> k;
    for (int i = 7; i >= 0; --i) k.push_back(uint8_t(id >> (8 * i)));
    return k;
}
std::vector<uint8_t> addrKey(const std::string& a, uint64_t id) {
    std::vector<uint8_t> k(16, 0);
    std::memcpy(k.data(), a.data(), std::min<std::size_t>(a.size(), 16));
    const auto tail = idKey(id);
    k.insert(k.end(), tail.begin(), tail.end());
    return k;
}

void installAll(VerdictRig& r) {
    r.installKeyedView(compile(projPlan(), kProj));
    r.installKeyedView(compile(byAddrPlan(), kByAddr));
    r.installKeyedView(compile(cntPlan(), kCnt));
    r.passes(30);
}

// The same three views, persisted (persistence 0): a restart recovers them from their checkpoints and rebuilds the tail.
void installAllPersisted(VerdictRig& r) {
    auto persisted = [](rv::ViewPlan v) { v.persistence = 0; return v; };
    r.installKeyedView(compile(persisted(projPlan()), kProj), /*persisted=*/true);
    r.installKeyedView(compile(persisted(byAddrPlan()), kByAddr), /*persisted=*/true);
    r.installKeyedView(compile(persisted(cntPlan()), kCnt), /*persisted=*/true);
    r.passes(30);
}

std::optional<std::vector<uint8_t>> rowOf(VerdictRig& r, int32_t view, const std::vector<uint8_t>& key) {
    const auto got = r.getViewRowK(view, key);
    if (got.code != rune::protocol::client::ViewRowResponse::kOk) return std::nullopt;
    return got.row;
}
int64_t readI64(const std::vector<uint8_t>& row, std::size_t at) {
    int64_t v = 0;
    std::memcpy(&v, row.data() + at, 8);
    return v;
}
bool containsBytes(const std::vector<uint8_t>& row, const std::string& needle) {
    return std::search(row.begin(), row.end(), needle.begin(), needle.end()) != row.end();
}

struct Snapshot {
    std::optional<std::vector<uint8_t>> proj, byAddr, cnt;
    bool operator==(const Snapshot&) const = default;
};
Snapshot snap(VerdictRig& r, uint64_t id, const std::string& addr) {
    return {rowOf(r, kProj, idKey(id)), rowOf(r, kByAddr, addrKey(addr, id)), rowOf(r, kCnt, idKey(id))};
}
/// Which views differ between two snapshots, with each side's row bytes (hex), for a failure message.
std::string diffOf(const Snapshot& a, const Snapshot& b) {
    const auto hex = [](const std::optional<std::vector<uint8_t>>& v) {
        if (!v) return std::string("none");
        std::string out;
        char h[4];
        for (const uint8_t c : *v) { std::snprintf(h, sizeof h, "%02x", c); out += h; }
        return out;
    };
    std::string out;
    if (a.proj != b.proj) out += " proj{" + hex(a.proj) + " vs " + hex(b.proj) + "}";
    if (a.byAddr != b.byAddr) out += " byAddr{" + hex(a.byAddr) + " vs " + hex(b.byAddr) + "}";
    if (a.cnt != b.cnt) out += " cnt{" + hex(a.cnt) + " vs " + hex(b.cnt) + "}";
    return out;
}

const rune::storage::RejectedOffsetIndex::Entry* firstRejection(VerdictRig& r) {
    const PartitionWriteState* w = r.writeState();
    if (w == nullptr) return nullptr;
    const int64_t off = w->rejected.nextRejectedAtOrAfter(0);
    return off == INT64_MAX ? nullptr : w->rejected.find(off);
}

struct Stored { int64_t offset; uint8_t attrs; };
std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (int64_t off = 0; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
            const std::size_t total = static_cast<std::size_t>(rd.totalLength());
            if (total < 12 || p + total > v.len) break;
            if ((rd.attributes() & rune::protocol::batch::kAttrControl) == 0)
                (void)rune::protocol::forEachRecordSpan(v.data + p, total, [&](const rune::protocol::RecordSpan& s) { out.push_back({s.offset, s.attributes}); });
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}

// ── fold-function harness ──
struct Folded {
    std::vector<uint8_t> bytes;
    int64_t              n = 0;
    rune::storage::LogReadView view() const { return {bytes.data(), bytes.size(), 0, n}; }
};
Folded batchOf(const std::vector<RawRec>& recs) { return {VerdictRig::rawBatch(recs), int64_t(recs.size())}; }

}  // namespace

TEST(LeasePredicateSitesRegression, FoldBatchTraitsRegression) {
    const rune::codec::Descriptor src = VerdictRig::leaseSourceDescriptor();
    const rv::CompileResult comp = compile(projPlan(), kProj);
    ASSERT_FALSE(rv::buildKeyedFoldSpec(comp.plan, src).has_value()) << "proj is a legacy-key plan";
    auto run = [&](const std::vector<RawRec>& recs, bool lease, rv::ViewTable& table, int64_t& cursor) {
        rv::VersionDescriptorCache vdc(src);
        const Folded f = batchOf(recs);
        rv::FoldSkip skip;
        skip.traits.lease = lease;
        return rv::foldBatch(comp.plan, comp.rowLayout, vdc, table, f.view(), f.n, cursor, skip);
    };
    {   // [grant, Expire]: traits off upserts twice, traits on deletes
        rv::ViewTable off(comp.rowLayout);
        int64_t cur = 0;
        const rv::FoldStats s = run({grantRec(1, "a1"), expireRec(1, 0, "a1")}, false, off, cur);
        EXPECT_EQ(s.applied, 2u);
        EXPECT_NE(off.get(uint64_t(1)), nullptr);
        rv::ViewTable on(comp.rowLayout);
        cur = 0;
        const rv::FoldStats t = run({grantRec(1, "a1"), expireRec(1, 0, "a1")}, true, on, cur);
        EXPECT_EQ(t.applied, 1u);
        EXPECT_EQ(t.deleted, 1u);
        EXPECT_EQ(on.get(uint64_t(1)), nullptr);
        EXPECT_EQ(cur, 2);
    }
    {   // [grant, Revoke] the same
        rv::ViewTable on(comp.rowLayout);
        int64_t cur = 0;
        const rv::FoldStats t = run({grantRec(1, "a1"), revokeRec(1, 0, "a1")}, true, on, cur);
        EXPECT_EQ(t.deleted, 1u);
        EXPECT_EQ(on.get(uint64_t(1)), nullptr);
    }
    {   // [grant, Checkpoint]: invisible
        rv::ViewTable on(comp.rowLayout);
        int64_t cur = 0;
        const rv::FoldStats t = run({grantRec(1, "a1"), checkpointRec(1, 0, "a1")}, true, on, cur);
        EXPECT_EQ(t.skippedControl, 1u);
        EXPECT_EQ(t.applied, 1u);
        EXPECT_EQ(cur, 2) << "the cursor is past both records";
        EXPECT_NE(on.get(uint64_t(1)), nullptr);
        rv::ViewTable off(comp.rowLayout);
        cur = 0;
        const rv::FoldStats u = run({grantRec(1, "a1"), checkpointRec(1, 0, "a1")}, false, off, cur);
        EXPECT_EQ(u.skippedControl, 0u);
        EXPECT_EQ(u.applied, 2u);
    }
}

TEST(LeasePredicateSitesRegression, AggregateTraitsRegression) {
    const rune::codec::Descriptor src = VerdictRig::leaseSourceDescriptor();
    const rv::CompileResult comp = compile(cntPlan(), kCnt);
    ASSERT_FALSE(rv::buildKeyedFoldSpec(comp.plan, src).has_value()) << "cnt is a legacy-key plan";
    const rv::AggLayout agg = rv::buildAggLayout(comp.plan, src).value();
    struct Out { rv::FoldStats st; bool present = false; int64_t n = -1; };
    auto run = [&](const std::vector<RawRec>& recs, bool lease) {
        rv::VersionDescriptorCache vdc(src);
        rv::ViewTable view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow);
        const Folded f = batchOf(recs);
        rv::FoldSkip skip;
        skip.traits.lease = lease;
        int64_t cursor = 0;
        Out o;
        o.st = rv::foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups, f.view(), f.n, cursor, skip);
        EXPECT_EQ(cursor, f.n);
        const uint8_t* row = view.get(uint64_t(1));
        o.present = row != nullptr;
        if (row != nullptr) {
            o.n = readI64(std::vector<uint8_t>(row, row + comp.rowLayout.rowLen), comp.rowLayout.slotOffset[1]);   // output 1 is n
        }
        return o;
    };
    const Out cp = run({grantRec(1, "a1"), checkpointRec(1, 0, "a1")}, true);
    EXPECT_EQ(cp.st.skippedControl, 1u);
    EXPECT_EQ(cp.st.applied, 1u);
    EXPECT_TRUE(cp.present);
    EXPECT_EQ(cp.n, 1);
    const Out cpOff = run({grantRec(1, "a1"), checkpointRec(1, 0, "a1")}, false);
    EXPECT_EQ(cpOff.st.skippedControl, 0u);
    EXPECT_EQ(cpOff.st.applied, 2u) << "traits off: the checkpoint is folded as an upsert";
    const Out ex = run({grantRec(1, "a1"), expireRec(1, 0, "a1")}, true);
    EXPECT_FALSE(ex.present) << "the Expire retracts the contributor: the group is gone";
    EXPECT_EQ(ex.st.deleted, 1u);
    const Out exOff = run({grantRec(1, "a1"), expireRec(1, 0, "a1")}, false);
    EXPECT_TRUE(exOff.present) << "traits off: the Expire upserts";
}

TEST(LeasePredicateSitesRegression, KeyedProjectionTraitsRegression) {
    const rune::codec::Descriptor src = VerdictRig::leaseSourceDescriptor();
    const rv::CompileResult comp = compile(byAddrPlan(), kByAddr);
    const auto spec = rv::buildKeyedFoldSpec(comp.plan, src);
    ASSERT_TRUE(spec.has_value());
    ASSERT_TRUE(spec->ownerMapped);
    auto run = [&](const std::vector<RawRec>& recs, bool lease, std::size_t& rows, std::size_t& owners) {
        rv::VersionDescriptorCache vdc(src);
        rv::ViewTable table(comp.rowLayout, 256, 1u << 16, {uint8_t(spec->keyWidth), uint8_t(spec->prefixWidth), 7});
        rv::RowLayout ol;
        ol.rowLen = spec->keyWidth;
        rv::ViewTable ownerTable(ol, 256, 1u << 16, {spec->wireKeyWidth, 0, 7});
        rv::Prospect p;
        p.reserve(comp.rowLayout, nullptr);
        rv::ProjectionFold pf{comp.plan, comp.rowLayout, vdc, table, rune::codec::PrimType::FixedStr, &*spec, &ownerTable};
        const Folded f = batchOf(recs);
        rv::FoldSkip skip;
        skip.traits.lease = lease;
        int64_t cursor = 0;
        const rv::FoldStats s = rv::foldProjectionBatch(pf, p, f.view(), f.n, cursor, skip);
        EXPECT_EQ(cursor, f.n);
        rows   = table.size();
        owners = ownerTable.size();
        return s;
    };
    std::size_t rows = 99, owners = 99;
    const rv::FoldStats on = run({grantRec(1, "a1"), revokeRec(1, 0, "a1")}, true, rows, owners);
    EXPECT_EQ(on.deleted, 1u);
    EXPECT_EQ(rows, 0u) << "no a1 row";
    EXPECT_EQ(owners, 0u) << "no owner entry for id1";
    const rv::FoldStats off = run({grantRec(1, "a1"), revokeRec(1, 0, "a1")}, false, rows, owners);
    EXPECT_EQ(off.deleted, 0u);
    EXPECT_EQ(rows, 1u);
    EXPECT_EQ(owners, 1u);
    const rv::FoldStats cp = run({grantRec(1, "a1"), checkpointRec(1, 0, "a1")}, true, rows, owners);
    EXPECT_EQ(cp.skippedControl, 1u);
    EXPECT_EQ(rows, 1u);
}

TEST(LeasePredicateWorkerRegression, RevokeDeletesUserViewRowsRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAll(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "a2")}));
    r.passes(30);
    ASSERT_TRUE(snap(r, 1, "a1").proj && snap(r, 1, "a1").byAddr && snap(r, 1, "a1").cnt);
    ASSERT_TRUE(snap(r, 2, "a2").proj && snap(r, 2, "a2").byAddr && snap(r, 2, "a2").cnt);
    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    ASSERT_TRUE(g.send({revokeRec(1, ver, "a1")}));
    r.passes(30);
    const Snapshot one = snap(r, 1, "a1"), two = snap(r, 2, "a2");
    EXPECT_FALSE(one.proj.has_value());
    EXPECT_FALSE(one.byAddr.has_value());
    EXPECT_FALSE(one.cnt.has_value());
    EXPECT_TRUE(two.proj.has_value());
    EXPECT_TRUE(two.byAddr.has_value());
    EXPECT_TRUE(two.cnt.has_value());
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    EXPECT_EQ(firstRejection(r), nullptr);
}

TEST(LeasePredicateWorkerRegression, ExpireDeletesUserViewRowsRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAll(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "a2")}));
    r.passes(30);
    ASSERT_TRUE(snap(r, 1, "a1").proj && snap(r, 1, "a1").byAddr && snap(r, 1, "a1").cnt);
    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    ASSERT_TRUE(g.send({expireRec(1, ver, "a1")}));
    r.passes(30);
    const Snapshot one = snap(r, 1, "a1"), two = snap(r, 2, "a2");
    EXPECT_FALSE(one.proj.has_value());
    EXPECT_FALSE(one.byAddr.has_value());
    EXPECT_FALSE(one.cnt.has_value());
    EXPECT_TRUE(two.proj.has_value());
    EXPECT_TRUE(two.byAddr.has_value());
    EXPECT_TRUE(two.cnt.has_value());
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    EXPECT_EQ(firstRejection(r), nullptr);
}

TEST(LeasePredicateWorkerRegression, CheckpointInvisibleToUserViewsRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAll(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    ASSERT_TRUE(r.driveUntilJudged());   // the live fold has every record (a fixed pass count races the driver under TSan)
    r.passes(30);
    const Snapshot before = snap(r, 1, "a1");
    ASSERT_TRUE(before.proj && before.byAddr && before.cnt);
    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    // The checkpoint's addr differs from the row's: a user view that folded it would show "ck" (a pure copy of the row could not tell).
    ASSERT_TRUE(g.send({checkpointRec(1, ver, "ck", 543'210)}));
    r.passes(30);
    EXPECT_EQ(firstRejection(r), nullptr) << "the checkpoint was judged and accepted";
    const Cols c = colsOf(r, 1, kV1Columns);
    ASSERT_TRUE(c.v[3].has_value());
    EXPECT_EQ(*c.v[3], 543'210u) << "the lease index took the checkpoint's remaining";
    EXPECT_TRUE(snap(r, 1, "a1") == before) << "all three user-view rows are byte-identical";
}

TEST(LeasePredicateWorkerRegression, UpdateStillUpsertsUserViewsRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAll(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    r.passes(30);
    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    ASSERT_TRUE(g.send({updateRec(1, ver, "b1")}));
    r.passes(30);
    EXPECT_EQ(firstRejection(r), nullptr);
    const auto proj = rowOf(r, kProj, idKey(1));
    ASSERT_TRUE(proj.has_value());
    EXPECT_TRUE(containsBytes(*proj, "b1"));
    EXPECT_FALSE(containsBytes(*proj, "a1"));
    EXPECT_TRUE(rowOf(r, kByAddr, addrKey("b1", 1)).has_value());
    EXPECT_FALSE(rowOf(r, kByAddr, addrKey("a1", 1)).has_value());
}

TEST(LeasePredicateWorkerRegression, RebuildFoldsLikeLiveRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAll(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "a2"), grantRec(3, "a3")}));
    r.passes(30);
    ASSERT_TRUE(g.send({checkpointRec(1, liveVersion(r, 1), "ck")}));   // a differing addr: a fold of it would show
    ASSERT_TRUE(g.send({updateRec(2, liveVersion(r, 2), "b2")}));
    ASSERT_TRUE(g.send({revokeRec(3, liveVersion(r, 3), "a3")}));
    ASSERT_TRUE(r.driveUntilJudged());   // the live fold has every record (a fixed pass count races the driver under TSan)
    r.passes(30);
    const std::pair<uint64_t, const char*> keys[] = {{1, "a1"}, {2, "a2"}, {2, "b2"}, {3, "a3"}};
    std::vector<Snapshot> before;
    for (const auto& [id, addr] : keys) before.push_back(snap(r, id, addr));
    EXPECT_TRUE(before[0].proj.has_value());
    EXPECT_FALSE(before[3].proj.has_value());
    EXPECT_FALSE(before[3].byAddr.has_value());
    EXPECT_FALSE(before[3].cnt.has_value());

    r.restart();
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    for (std::size_t i = 0; i < before.size(); ++i) {
        const Snapshot after = snap(r, keys[i].first, keys[i].second);
        EXPECT_TRUE(after == before[i]) << "id " << keys[i].first << " addr " << keys[i].second << " after vs before:" << diffOf(after, before[i]);
    }
    const Snapshot b2 = snap(r, 2, "b2");
    ASSERT_TRUE(b2.proj.has_value());
    EXPECT_TRUE(containsBytes(*b2.proj, "b2")) << "the update is folded by value";
    EXPECT_TRUE(b2.byAddr.has_value());
    EXPECT_FALSE(snap(r, 2, "a2").byAddr.has_value()) << "the update moved the addr index off a2";
}

TEST(LeasePredicateWorkerRegression, PersistedRebuildFoldsLikeLiveRegression) {
    Ready g;
    VerdictRig& r = g.r;
    installAllPersisted(r);
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "a2"), grantRec(3, "a3")}));
    r.passes(30);
    // The checkpoint files are taken BEFORE the lease records: the restart recovers them and the rebuild folds the rest.
    for (int i = 0; i < 40 && (r.agent->lastDurableOffset(kProj, 0) <= 0 || r.agent->lastDurableOffset(kByAddr, 0) <= 0 ||
                               r.agent->lastDurableOffset(kCnt, 0) <= 0); ++i) r.passes(50);
    ASSERT_GT(r.agent->lastDurableOffset(kProj, 0), 0);
    ASSERT_GT(r.agent->lastDurableOffset(kByAddr, 0), 0);
    ASSERT_GT(r.agent->lastDurableOffset(kCnt, 0), 0);
    r.pauseAgent = true;   // the checkpoint files stay at the grants: the restart has the lease records left to rebuild
    ASSERT_TRUE(g.send({checkpointRec(1, liveVersion(r, 1), "ck")}));   // a differing addr: a fold of it would show
    ASSERT_TRUE(g.send({updateRec(2, liveVersion(r, 2), "b2")}));
    ASSERT_TRUE(g.send({revokeRec(3, liveVersion(r, 3), "a3")}));
    ASSERT_TRUE(r.driveUntilJudged());   // the live fold has every record (a fixed pass count races the driver under TSan)
    r.passes(30);
    const std::pair<uint64_t, const char*> keys[] = {{1, "a1"}, {2, "a2"}, {2, "b2"}, {3, "a3"}};
    std::vector<Snapshot> before;
    for (const auto& [id, addr] : keys) before.push_back(snap(r, id, addr));
    EXPECT_TRUE(before[0].proj.has_value());
    EXPECT_FALSE(before[3].proj.has_value());
    EXPECT_FALSE(before[3].byAddr.has_value());
    EXPECT_FALSE(before[3].cnt.has_value());

    const auto stored = storedRecords(r);
    ASSERT_EQ(stored.size(), 6u);
    for (const int32_t v : {kProj, kByAddr, kCnt})
        ASSERT_LE(r.agent->lastDurableOffset(v, 0), stored[3].offset) << "vacuity guard: view " << v << " checkpoint is below the lease records";
    // A view of a judged source rebuilds only up to what the judge knows: the judge re-judges the log first, then the views
    // recover their checkpoints, so the restart rebuild (not the steady fold) folds the lease records.
    r.restart(/*reinstall=*/false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.reinstallAll();
    r.pass();
    for (const int32_t v : {kProj, kByAddr, kCnt}) {
        const auto* inst = r.vs->instanceForTest(v, 0);
        ASSERT_NE(inst, nullptr);
        EXPECT_GT(inst->foldCursor, 0) << "view " << v << " recovered";
    }
    r.pauseAgent = false;   // the old views' queued frames drain now, after recovery, by design
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    for (std::size_t i = 0; i < before.size(); ++i) {
        const Snapshot after = snap(r, keys[i].first, keys[i].second);
        EXPECT_TRUE(after == before[i]) << "id " << keys[i].first << " addr " << keys[i].second << " after vs before:" << diffOf(after, before[i]);
    }
    const Snapshot b2 = snap(r, 2, "b2");
    ASSERT_TRUE(b2.proj.has_value());
    EXPECT_TRUE(containsBytes(*b2.proj, "b2")) << "the update is folded by value";
    EXPECT_TRUE(b2.byAddr.has_value());
    EXPECT_FALSE(snap(r, 2, "a2").byAddr.has_value()) << "the update moved the addr index off a2";
}

TEST(LeasePredicateWorkerRegression, VerdictMemberSeesLeaseTombstoneRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    r.passes(30);
    const auto first = storedRecords(r);
    ASSERT_EQ(first.size(), 1u);
    const int64_t grantAt = first[0].offset;
    // The checkpoint and the revoke land in the next offsets (a leadership change may log a control batch between them): every
    // offset above the grant is listed, and the log tells which two data records got them.
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::OffsetSet;
    for (int i = 0; i < 8; ++i) rule.offsets[std::size_t(i)] = grantAt + 1 + i;
    rule.offsetCount = 8;
    r.installRejectView(43, 9001, rule, "rej");
    r.passes(60);
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const ConstraintSet& set = w->constraintSets.latest();
    ASSERT_GE(set.count, 2u) << "the reject view is a member of the logged set after the lease index";
    EXPECT_EQ(set.members[0].kind, rv::VerdictViewKind::LeaseIndex);
    EXPECT_EQ(set.members[1].kind, rv::VerdictViewKind::TestReject);

    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    const auto rej0 = rowOf(r, 43, idKey(1));
    ASSERT_TRUE(rej0.has_value()) << "vacuity guard: the member's table holds the grant";
    ASSERT_TRUE(g.send({checkpointRec(1, ver, "ck")}));
    r.passes(30);
    const auto rej1 = rowOf(r, 43, idKey(1));
    ASSERT_TRUE(rej1.has_value());
    EXPECT_EQ(*rej1, *rej0) << "a checkpoint is invisible to a data member's table";
    ASSERT_TRUE(g.send({revokeRec(1, ver, "a1")}));
    r.passes(30);
    const auto all = storedRecords(r);
    ASSERT_EQ(all.size(), 3u);
    EXPECT_GT(all[1].offset, grantAt);
    EXPECT_LE(all[2].offset, grantAt + 8) << "the listed offsets cover both records";
    EXPECT_EQ(all[1].attrs, kCheckpointAttr);
    EXPECT_EQ(all[2].attrs, kRevokeAttr);
    EXPECT_EQ(firstRejection(r), nullptr) << "a lease tombstone and a checkpoint are never rejected by a data member";
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u) << "the revoke took the lease index row";
    EXPECT_FALSE(rowOf(r, 43, idKey(1)).has_value()) << "the revoke deleted the member's row";

    r.restart();
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    EXPECT_EQ(firstRejection(r), nullptr) << "the replay judged the checkpoint and revoke as lease records";
    EXPECT_FALSE(rowOf(r, 43, idKey(1)).has_value()) << "the replay deleted the member's row";
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u);
}

TEST(LeasePredicateWorkerRegression, NonLeaseTopicUnchangedRegression) {
    VerdictRig r;
    r.typed();
    r.installProjection(kProj);
    r.passes(30);
    const std::vector<uint8_t> key = idKey(1);
    ASSERT_EQ(r.produce({Rec{1, 10}}), 0);
    r.driveUntilFolded(kProj, r.leo());
    const auto first = rowOf(r, kProj, key);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(r.produce({Rec{1, 0, 0, /*del=*/true}}), 0);
    r.driveUntilFolded(kProj, r.leo());
    EXPECT_FALSE(rowOf(r, kProj, key).has_value()) << "an empty value still deletes";
    ASSERT_EQ(r.produce({Rec{1, 30, 0, false, /*intent=*/0}}), 0);
    r.driveUntilFolded(kProj, r.leo());
    const auto again = rowOf(r, kProj, key);
    ASSERT_TRUE(again.has_value()) << "a non-empty value upserts (intent bits need a key index on a plain topic)";
    EXPECT_NE(*again, *first);
    EXPECT_EQ(std::count(again->begin(), again->end(), uint8_t(30)) >= 1, true);

    // Lease op bits on a plain topic are never read: both records are plain upserts (a typed value as produce() encodes it).
    const auto plainRec = [](uint64_t id, int64_t v, uint8_t attrs) {
        RawRec rr;
        std::string k(8, '\0');
        std::vector<uint8_t> val;
        const auto le16 = [&](uint16_t x) { val.push_back(uint8_t(x)); val.push_back(uint8_t(x >> 8)); };
        le16(18); le16(1);
        for (int i = 0; i < 8; ++i) { k[std::size_t(i)] = char(uint8_t(id >> (8 * i))); val.push_back(uint8_t(id >> (8 * i))); }
        for (int i = 0; i < 8; ++i) val.push_back(uint8_t(uint64_t(v) >> (8 * i)));
        le16(0);
        rr.key = k; rr.value = val; rr.attrs = attrs;
        return rr;
    };
    r.demote();
    r.replicate({VerdictRig::rawBatch({plainRec(2, 30, kRevokeAttr), plainRec(3, 40, kCheckpointAttr)})});
    r.promote();
    r.passes(30);
    r.driveUntilFolded(kProj, r.leo());
    const auto row2 = rowOf(r, kProj, idKey(2));
    const auto row3 = rowOf(r, kProj, idKey(3));
    ASSERT_TRUE(row2.has_value()) << "a plain topic never reads lease op bits";
    ASSERT_TRUE(row3.has_value()) << "a plain topic never reads lease op bits";
    EXPECT_GE(std::count(row2->begin(), row2->end(), uint8_t(30)), 1) << "the revoke-bit record upserted its value";
    EXPECT_GE(std::count(row3->begin(), row3->end(), uint8_t(40)), 1) << "the checkpoint-bit record upserted its value";
    EXPECT_NE(*row2, *row3);
}

// Spec A.3 / A.6: a Carry is Put with intent 0 on a lease topic; the fence is a Checkpoint whose version is -1.
TEST(LeasePredicateRegression, CarryAndFencePredicatesRegression) {
    for (int op = 0; op < 4; ++op) {
        for (int intent = 0; intent < 4; ++intent) {
            for (const bool lease : {true, false}) {
                const uint8_t attrs = uint8_t((op << rec::kAttrLeaseOpShift) | intent);
                const rec::TopicTraits t{.lease = lease};
                const bool expected = lease && op == int(rec::LeaseOp::Put) && intent == 0;
                EXPECT_EQ(rec::isLeaseCarry(attrs, t), expected) << "op " << op << " intent " << intent << " lease " << lease;
            }
        }
    }
    const auto valueWith = [](int64_t version) {
        std::vector<uint8_t> v(40, 0);
        std::memcpy(v.data() + 4, &version, 8);
        return v;
    };
    const uint8_t ckpt = uint8_t(rec::LeaseOp::Checkpoint) << rec::kAttrLeaseOpShift;
    const uint8_t put  = uint8_t(rec::LeaseOp::Put) << rec::kAttrLeaseOpShift;
    const rec::TopicTraits lease{.lease = true}, plain{};
    const auto fence = valueWith(-1), five = valueWith(5);
    EXPECT_TRUE(rec::isLeaseFence(ckpt, fence.data(), fence.size(), lease));
    EXPECT_FALSE(rec::isLeaseFence(ckpt, five.data(), five.size(), lease));
    EXPECT_FALSE(rec::isLeaseFence(put, fence.data(), fence.size(), lease));
    EXPECT_FALSE(rec::isLeaseFence(ckpt, fence.data(), 11, lease));
    EXPECT_TRUE(rec::isLeaseFence(ckpt, fence.data(), 12, lease));
    EXPECT_FALSE(rec::isLeaseFence(ckpt, fence.data(), fence.size(), plain));
}
