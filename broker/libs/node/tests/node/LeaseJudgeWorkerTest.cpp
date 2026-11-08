// Lease topics Task 6: the lease index inside a real worker (the leadership and layout rebuilds, the hook facts).
#include "CheckedWritesRig.h"

#include <rune/protocol/LeaseRecord.h>

#include <cstring>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;

namespace {

constexpr uint16_t kV1Columns = 8;   // 4 system + id + addr + the two offset columns
constexpr uint16_t kV2Columns = 9;   // + port

std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}

void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}

// [u16 blockLength][u16 schema version][version, ttl, holder, remaining, id, addr(16)][port at v2].
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint16_t schema = 1, uint32_t port = 0, uint64_t holder = 1) {
    const uint16_t block = schema >= 2 ? 60 : 56;
    std::vector<uint8_t> b(4u + block, 0);
    putLe(b, 0, block, 2);
    putLe(b, 2, schema, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, 5000, 8);
    putLe(b, 20, holder, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    if (schema >= 2) putLe(b, 60, port, 4);
    return b;
}
RawRec grantRec(uint64_t id, const std::string& addr) { return {keyOf(id), leaseValue(id, 0, addr), 1}; }
RawRec updateRec(uint64_t id, int64_t ver, const std::string& addr, uint32_t port) { return {keyOf(id), leaseValue(id, ver, addr, 2, port, 9), 2}; }

/// A lease topic with its lease index logged.  The records enter the way a follower's replication delivers them (demote,
/// replicate, promote): the produce hook's lease rules belong to the hook, this suite is about the judge's rebuilds.
struct Ready {
    VerdictRig r;
    Ready() {
        r.createLeaseTopic();
        r.passes(30);
    }
    /// One batch committed into the log and judged by the next leadership.
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

}  // namespace

TEST(LeaseJudgeWorkerRegression, ProvidesKeyIndexRegression) {
    VerdictRig r;
    r.createLeaseTopic();
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const auto facts = topicHookFactsOf(*w);
    EXPECT_TRUE(facts.keyIndexActive) << "a lease topic's logged set has the lease index as member 0";
    EXPECT_FALSE(facts.keyIndex) << "the spec's key.index fact is unchanged";
    EXPECT_TRUE(facts.verdictSource);
}

TEST(LeaseJudgeWorkerRegression, RebuildPerLeadershipRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "b2"), grantRec(3, "c3")}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 3u);
    std::vector<Cols> before;
    for (uint64_t id = 1; id <= 3; ++id) before.push_back(colsOf(r, id, kV1Columns));
    ASSERT_EQ(before[0].slots, kV1Columns);
    ASSERT_TRUE(before[0].v[0].has_value());
    ASSERT_NE(r.stats(), nullptr);
    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;

    r.passes(60);   // the same leadership: nothing rebuilds
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds);

    r.demotePromote();   // a new generation rebuilds from the local log
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 3u);
    for (uint64_t id = 1; id <= 3; ++id) {
        const Cols after = colsOf(r, id, kV1Columns);
        EXPECT_EQ(after.v, before[id - 1].v) << "row " << id;
    }
    r.passes(60);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1) << "no further rebuild without a new leadership";
}

TEST(LeaseJudgeWorkerRegression, RebuildPerLayoutRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1"), grantRec(2, "b2")}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
    const Cols one = colsOf(r, 1, kV1Columns), two = colsOf(r, 2, kV1Columns);
    ASSERT_EQ(one.slots, kV1Columns);
    ASSERT_NE(r.stats(), nullptr);
    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;

    r.addLeasePort();   // the evolving descriptor gains a field id: the row layout has a new column
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1) << "one rebuild for the new layout";
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
    for (const auto& [id, old] : {std::pair<uint64_t, const Cols*>{1, &one}, {2, &two}}) {
        const Cols now = colsOf(r, id, kV2Columns);
        ASSERT_EQ(now.slots, kV2Columns);
        for (uint16_t s = 0; s < 6; ++s) EXPECT_EQ(now.v[s], old->v[s]) << "row " << id << " column " << s;   // system columns, id, addr
        EXPECT_FALSE(now.v[6].has_value()) << "the new port column is null";
        EXPECT_EQ(now.v[7], old->v[6]) << "granted_offset moved one slot right";
        EXPECT_EQ(now.v[8], old->v[7]) << "updated_offset moved one slot right";
    }

    // The rebuilt index judges a v2 record: update fills the port.
    const int64_t version = int64_t(*colsOf(r, 1, kV2Columns).v[0]);
    ASSERT_TRUE(g.send({updateRec(1, version, "n1", 4242)}));
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    const Cols upd = colsOf(r, 1, kV2Columns);
    ASSERT_TRUE(upd.v[6].has_value());
    EXPECT_EQ(*upd.v[6], 4242u);
    EXPECT_EQ(upd.v[0], one.v[0]) << "the version is unchanged by an update";
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 2) << "the layout rebuild and the leadership that delivered the record; the record itself rebuilds nothing";
}
