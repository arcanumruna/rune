// Lease topics Task 8: the cleaner's lease mode through a real rig - a rebuild of the lease index after a clean equals the one
// before it (the grant's TTL survives; checkpoints of the live version survive).
#include "CheckedWritesRig.h"

#include <rune/protocol/LeaseRecord.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;

namespace {

constexpr uint16_t kV1Columns = 8;
constexpr uint8_t kRevokeAttr     = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);   // Revoke | existing
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
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t remaining = 0, uint64_t holder = 1, uint64_t ttl = 5000) {
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
RawRec grantRec(uint64_t id, uint64_t ttl = 5000) { return {keyOf(id), leaseValue(id, 0, "g", 0, 1, ttl), 1}; }
RawRec updateRec(uint64_t id, int64_t ver, uint64_t ttl) { return {keyOf(id), leaseValue(id, ver, "u", 0, 9, ttl), 2}; }
RawRec revokeRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, "r"), kRevokeAttr}; }
RawRec checkpointRec(uint64_t id, int64_t ver, uint64_t remaining) { return {keyOf(id), leaseValue(id, ver, "c", remaining), kCheckpointAttr}; }
RawRec expireRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, "e"), uint8_t(uint8_t(rec::LeaseOp::Expire) << rec::kAttrLeaseOpShift)}; }   // the broker's: intent 0

struct Ready {
    VerdictRig r;
    explicit Ready(int64_t rollBytes) : r(false, rollBytes, RigTuning{}) {
        r.createLeaseTopic(1000, std::nullopt);
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
    bool operator==(const Cols& o) const { return v == o.v && slots == o.slots; }
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
struct Stored { int64_t offset; uint8_t attrs; };
std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (int64_t off = 0; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) {   // a cleaned gap: continue at the next segment
            const auto bases = r.log->segmentBaseOffsets();
            const auto next = std::upper_bound(bases.begin(), bases.end(), off);
            if (next == bases.end()) break;
            off = *next;
            continue;
        }
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


// After a restart: drive until the lease index is rebuilt from the whole log (a bounded number of passes, never a bare loop).
void rebuilt(VerdictRig& r) {
    for (int i = 0; i < VerdictRig::kMaxPass * 4 && !r.leaseReady(); ++i) r.pass();
    ASSERT_TRUE(r.leaseReady()) << "the lease index never became ready after the restart";
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
}
}  // namespace

TEST(LeaseCleanerRebuildRegression, KeepsGrantTtlAfterCleanRegression) {
    Ready g(/*rollBytes=*/1);
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, 5000)}));
    const int64_t ver = liveVersion(r, 1);
    ASSERT_GE(ver, 0);
    std::vector<RawRec> ups;
    for (uint64_t i = 0; i < 100; ++i) ups.push_back(updateRec(1, ver, 7000 + i));
    ASSERT_TRUE(g.send(ups));
    ASSERT_EQ(colsOf(r, 1, kV1Columns).v[1], std::optional<uint64_t>(5000)) << "the judge keeps the grant's TTL on an update";
    const std::size_t before = storedRecords(r).size();
    ASSERT_EQ(before, 101u);
    const auto c = r.compact();
    EXPECT_GT(c.installed, 0) << "vacuity guard: the clean installed a rewrite";
    EXPECT_LT(storedRecords(r).size(), before) << "vacuity guard: the clean dropped records";
    r.restart();
    rebuilt(r);
    const Cols after = colsOf(r, 1, kV1Columns);
    ASSERT_TRUE(after.v[0].has_value()) << "the row exists after the rebuild";
    EXPECT_EQ(int64_t(*after.v[0]), ver) << "the version is the grant's";
    EXPECT_EQ(after.v[1], std::optional<uint64_t>(5000)) << "the TTL is the grant's, not an update's";
    EXPECT_EQ(after.v[2], std::optional<uint64_t>(9)) << "the latest update is applied after the grant (its holder)";
}

TEST(LeaseCleanerRebuildRegression, RebuildAfterCleanEqualsBeforeRegression) {
    constexpr uint64_t kKeys = 50;
    constexpr uint64_t kUpdates = 4000;
    constexpr uint64_t kCheckpointEvery = kUpdates / 40;
    Ready g(/*rollBytes=*/1);
    VerdictRig& r = g.r;
    std::vector<RawRec> grants;
    for (uint64_t id = 1; id <= kKeys; ++id) grants.push_back(grantRec(id, 5000 + id));
    ASSERT_TRUE(g.send(grants));
    std::vector<int64_t> ver(kKeys + 1, -1);
    for (uint64_t id = 1; id <= kKeys; ++id) { ver[id] = liveVersion(r, id); ASSERT_GE(ver[id], 0); }
    std::vector<RawRec> all;
    for (uint64_t round = 0; round < kUpdates; ++round) {
        for (uint64_t id = 1; id <= kKeys; ++id) {
            all.push_back(updateRec(id, ver[id], 7000 + round));
            if ((round + 1) % kCheckpointEvery == 0) all.push_back(checkpointRec(id, ver[id], 9000 - round));
        }
    }
    // One more update per key AFTER the last checkpoint round: every key's newest checkpoint is older than its latest update.
    for (uint64_t id = 1; id <= kKeys; ++id) all.push_back(updateRec(id, ver[id], 7000 + kUpdates));
    for (uint64_t id = 1; id <= 5; ++id) all.push_back(revokeRec(id, ver[id]));
    for (std::size_t at = 0; at < all.size(); at += 2048) {
        const std::vector<RawRec> part(all.begin() + std::ptrdiff_t(at), all.begin() + std::ptrdiff_t(std::min(all.size(), at + 2048)));
        ASSERT_TRUE(g.send(part));
    }
    std::vector<Cols> snap;
    for (uint64_t id = 1; id <= kKeys; ++id) snap.push_back(colsOf(r, id, kV1Columns));
    for (uint64_t id = 1; id <= 5; ++id) ASSERT_FALSE(snap[id - 1].v[0].has_value()) << "vacuity guard: id " << id << " is revoked";
    for (uint64_t id = 6; id <= kKeys; ++id) ASSERT_TRUE(snap[id - 1].v[0].has_value());
    const std::size_t before = storedRecords(r).size();
    int installed = 0;
    for (int i = 0; i < 3; ++i) installed += r.compact().installed;
    const std::size_t afterClean = storedRecords(r).size();
    EXPECT_GT(installed, 0) << "vacuity guard: the clean installed rewrites";
    EXPECT_LT(afterClean, before / 2) << "vacuity guard: the clean dropped most records (" << before << " -> " << afterClean << ")";
    r.restart();
    rebuilt(r);
    for (uint64_t id = 1; id <= kKeys; ++id) {
        const Cols after = colsOf(r, id, kV1Columns);
        EXPECT_TRUE(after == snap[id - 1]) << "id " << id << ": the rebuilt lease row differs from the one before the clean";
    }
}

// Clean, write more, clean again, then a leader change and a restart: the rebuilt rows equal the live ones.  Covers a regranted key
// (its live version is not its first grant), a broker-expired key, and updates and checkpoints on both sides of two cleans.
TEST(LeaseCleanerRebuildRegression, CleanWriteCleanRestartRegression) {
    constexpr uint64_t kKeys = 10;
    Ready g(/*rollBytes=*/1);
    VerdictRig& r = g.r;
    std::vector<RawRec> grants;
    for (uint64_t id = 1; id <= kKeys; ++id) grants.push_back(grantRec(id, 5000 + id));
    ASSERT_TRUE(g.send(grants));
    std::vector<int64_t> ver(kKeys + 1, -1);
    for (uint64_t id = 1; id <= kKeys; ++id) { ver[id] = liveVersion(r, id); ASSERT_GE(ver[id], 0); }

    // Key 1 is revoked and granted again: its live version is the regrant's offset.
    const int64_t firstGrant = ver[1];
    ASSERT_TRUE(g.send({revokeRec(1, ver[1])}));
    ASSERT_TRUE(g.send({grantRec(1, 6000)}));
    ver[1] = liveVersion(r, 1);
    ASSERT_GE(ver[1], 0);
    ASSERT_NE(ver[1], firstGrant) << "vacuity guard: key 1 has a new version";
    int64_t regrantOffset = -1;
    for (const Stored& st : storedRecords(r)) if (st.attrs == 1) regrantOffset = std::max(regrantOffset, st.offset);
    ASSERT_EQ(ver[1], regrantOffset) << "the regrant's version is its record's offset";

    // Key 2 is expired by the broker.
    ASSERT_TRUE(g.send({expireRec(2, ver[2])}));
    ASSERT_EQ(liveVersion(r, 2), -1) << "vacuity guard: key 2 is gone";

    const auto round = [&](uint64_t n, uint64_t ttlBase) {
        std::vector<RawRec> recs;
        for (uint64_t id = 1; id <= kKeys; ++id) {
            if (id == 2) continue;
            for (uint64_t u = 0; u < n; ++u) recs.push_back(updateRec(id, ver[id], ttlBase + u));
            recs.push_back(checkpointRec(id, ver[id], 9000 - ttlBase));
        }
        ASSERT_TRUE(g.send(recs));
    };
    round(5, 7000);
    const std::size_t before1 = storedRecords(r).size();
    const auto c1 = r.compact();
    EXPECT_GT(c1.installed, 0) << "vacuity guard: the first clean installed a rewrite";
    EXPECT_LT(storedRecords(r).size(), before1) << "vacuity guard: the first clean dropped records";
    round(5, 8000);
    const std::size_t before2 = storedRecords(r).size();
    const auto c2 = r.compact();
    EXPECT_GT(c2.installed, 0) << "vacuity guard: the second clean installed a rewrite";
    EXPECT_LT(storedRecords(r).size(), before2) << "vacuity guard: the second clean dropped records";

    std::vector<Cols> snap;
    for (uint64_t id = 1; id <= kKeys; ++id) snap.push_back(colsOf(r, id, kV1Columns));
    ASSERT_TRUE(snap[0].v[0].has_value());
    EXPECT_EQ(int64_t(*snap[0].v[0]), regrantOffset) << "key 1's version is the regrant's offset";
    EXPECT_FALSE(snap[1].v[0].has_value()) << "key 2 is expired";

    const auto expectSame = [&](const char* leg) {
        for (uint64_t id = 1; id <= kKeys; ++id)
            EXPECT_TRUE(colsOf(r, id, kV1Columns) == snap[id - 1]) << leg << ": id " << id << " differs from the row before";
    };
    r.demotePromote();
    rebuilt(r);
    expectSame("after the leader change");
    r.restart();
    rebuilt(r);
    expectSame("after the restart");
}
