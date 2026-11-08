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
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint16_t schema = 1, uint32_t port = 0, uint64_t holder = 1, uint64_t ttl = 5000) {
    const uint16_t block = schema >= 2 ? 60 : 56;
    std::vector<uint8_t> b(4u + block, 0);
    putLe(b, 0, block, 2);
    putLe(b, 2, schema, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
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
    explicit Ready(int64_t rollBytes = 64LL * 1024 * 1024, std::optional<rune::codec::Descriptor> src = std::nullopt) : r(false, rollBytes) {
        r.createLeaseTopic(1000, std::move(src));
        r.passes(30);
    }
    /// One batch committed into the log and judged by the next leadership.
    bool send(const std::vector<RawRec>& recs) {
        sendNoJudge(recs);
        return r.driveUntilJudged();
    }
    /// The same without waiting for the judge (a record the judge holds is never judged).
    void sendNoJudge(const std::vector<RawRec>& recs) {
        r.demote();
        r.replicate({VerdictRig::rawBatch(recs)});
        r.promote();
        r.passes(30);
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

// ═══ Fix round: the writer comes from the spec, an unknown layout holds, a compacted log replays whole ═══

namespace {

using rune::codec::PrimType;

/// {lease system columns, id (key, 4), port uint16 (5), addr string(16) (6)}; `portUntil` retires the port (a Drop).
rune::codec::Descriptor portTopic(uint16_t latest, uint16_t portUntil) {
    rune::codec::DescriptorBuilder d("inst", latest);
    d.add("lease_version", PrimType::Int64).add("lease_ttl_ms", PrimType::UInt64).add("lease_holder", PrimType::UInt64)
        .add("lease_remaining_ms", PrimType::UInt64).add("id", PrimType::UInt64, 1, true)
        .addTagged("port", 5, PrimType::UInt16, 1, portUntil, false, {})
        .addFixedStrTagged("addr", 6, 16, 1, 0xFFFF, false);
    return d.build();
}

/// schema 1: block 58 (port at 44, addr at 46); schema 2 (port dropped): block 56 (addr at 44).
std::vector<uint8_t> portValue(uint64_t id, int64_t ver, uint16_t schema, uint16_t port, const std::string& addr, uint64_t holder = 1) {
    const uint16_t block = schema >= 2 ? 56 : 58;
    std::vector<uint8_t> b(4u + block, 0);
    putLe(b, 0, block, 2);
    putLe(b, 2, schema, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, 5000, 8);
    putLe(b, 20, holder, 8);
    putLe(b, 36, id, 8);
    if (schema >= 2) {
        std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    } else {
        putLe(b, 44, port, 2);
        std::memcpy(b.data() + 46, addr.data(), std::min<std::size_t>(addr.size(), 16));
    }
    return b;
}

uint64_t packed(const std::string& s) {
    uint64_t v = 0;
    std::memcpy(&v, s.data(), std::min<std::size_t>(8, s.size()));
    return v;
}

/// The first rejected offset's entry (nullptr: nothing was rejected).
const rune::storage::RejectedOffsetIndex::Entry* firstRejection(VerdictRig& r) {
    const PartitionWriteState* w = r.writeState();
    if (w == nullptr) return nullptr;
    const int64_t off = w->rejected.nextRejectedAtOrAfter(0);
    return off == INT64_MAX ? nullptr : w->rejected.find(off);
}

struct OrderOutcome {
    std::vector<std::optional<uint64_t>> k1, k2;   // ttl, holder, id, addr (8 bytes), port
};
void runSchemaOrder(bool schemaUpdateFirst, OrderOutcome& out) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    const rune::codec::Descriptor v2 = VerdictRig::leaseSourceDescriptor(2);
    RawRec grant2{keyOf(2), leaseValue(2, 0, "b2", 2, 4242), 1};

    // The v2 record reaches this node (as a follower) before the second schema op.  SchemaUpdate first: the layout is the merge of
    // the evolving descriptor and the installed versions, so the install alone moves it and the record is judged.  FilterSchema
    // first: the writer version is not installed, the judge holds at the record.
    if (schemaUpdateFirst) {
        r.leaseSchemaUpdate(v2, 2);
        g.sendNoJudge({grant2});
    } else {
        r.leaseFilterSchema(v2, 2);   // the layout has `port`, the writer version is not installed yet
        g.sendNoJudge({grant2});
    }
    r.passes(60);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    ASSERT_NE(r.stats(), nullptr);
    if (schemaUpdateFirst) {
        EXPECT_EQ(r.stats()->leaseLayoutWaits, 0u) << "the install alone made the layout";
        EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
        EXPECT_EQ(firstRejection(r), nullptr);
    } else {
        EXPECT_GE(r.stats()->leaseLayoutWaits, 1u) << "the judge holds, it does not guess";
        EXPECT_EQ(judge->judgedThrough(), r.dataEnd() - 1) << "held AT the record: nothing judged, nothing past it";
        EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
        EXPECT_EQ(firstRejection(r), nullptr) << "no verdict while the layout is missing";
        EXPECT_FALSE(judge->stalled());
    }

    if (schemaUpdateFirst) r.leaseFilterSchema(v2, 2);   // the evolving descriptor arrives: no new field id, no further rebuild
    else r.leaseSchemaUpdate(v2, 2);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(firstRejection(r), nullptr);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
    for (const uint16_t s : {1, 2, 4, 5, 6}) {
        out.k1.push_back(colsOf(r, 1, kV2Columns).v[s]);
        out.k2.push_back(colsOf(r, 2, kV2Columns).v[s]);
    }
    EXPECT_EQ(out.k2[4], 4242u) << "the v2 row has its port";
    EXPECT_FALSE(out.k1[4].has_value()) << "the v1 row has none";
}

}  // namespace

TEST(LeaseJudgeWorkerRegression, LeaseDropFieldKeepsFencingRegression) {
    Ready g(64LL * 1024 * 1024, portTopic(1, 0xFFFF));
    VerdictRig& r = g.r;
    constexpr uint16_t kCols = 9;   // 4 system + id + port + addr + the two offsets: the dropped column stays
    ASSERT_TRUE(g.send({RawRec{keyOf(1), portValue(1, 0, 1, 7, "a1"), 1}}));
    const Cols granted = colsOf(r, 1, kCols);
    ASSERT_EQ(granted.slots, kCols);
    ASSERT_TRUE(granted.v[0].has_value());
    EXPECT_EQ(granted.v[5], 7u);
    EXPECT_EQ(granted.v[6], packed("a1"));
    const int64_t version = int64_t(*granted.v[0]);
    ASSERT_NE(r.stats(), nullptr);

    // The v2 records (written after `port` was dropped) reach this node before the schema ops: a second grant of the live key
    // and an update with the live version.  The judge holds at the first one (v2 is not installed yet).
    g.sendNoJudge({RawRec{keyOf(1), portValue(1, 0, 2, 0, "zz"), 1}, RawRec{keyOf(1), portValue(1, version, 2, 0, "n2", 9), 2}});
    r.passes(30);
    EXPECT_GE(r.stats()->leaseLayoutWaits, 1u);
    EXPECT_EQ(colsOf(r, 1, kCols).v[6], packed("a1"));
    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;

    // AlterSchema Drop port: SchemaUpdate v2 + FilterSchema.  No new field id: nothing rebuilds, the index keeps its v1 copy.
    const rune::codec::Descriptor dropped = portTopic(2, 2);
    r.leaseSchemaUpdate(dropped, 2);
    r.leaseFilterSchema(dropped, 2);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(10);

    // The second grant of a live key is KeyExists (not the malformed-record answer of a stale writer, never an accept).
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->rejected.size(), 1u);
    const auto* e = firstRejection(r);
    ASSERT_NE(e, nullptr) << "the second grant of a live key was accepted";
    EXPECT_EQ(e->code, 56);

    // The update at v2 with the live version is accepted: addr changes, the dropped port projects null.
    const Cols upd = colsOf(r, 1, kCols);
    EXPECT_EQ(upd.v[0], granted.v[0]);
    EXPECT_EQ(upd.v[6], packed("n2"));
    EXPECT_FALSE(upd.v[5].has_value()) << "the dropped port is null";
    EXPECT_EQ(upd.v[2], 9u);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds) << "a Drop adds no field id: no rebuild";
}

// R7: the order "schema ops first, then a v2 grant of a live key": KeyExists (56).
TEST(LeaseJudgeWorkerRegression, LeaseDropFieldSchemaFirstKeepsFencingRegression) {
    Ready g(64LL * 1024 * 1024, portTopic(1, 0xFFFF));
    VerdictRig& r = g.r;
    constexpr uint16_t kCols = 9;
    ASSERT_TRUE(g.send({RawRec{keyOf(1), portValue(1, 0, 1, 7, "a1"), 1}}));
    const Cols granted = colsOf(r, 1, kCols);
    ASSERT_TRUE(granted.v[0].has_value());
    const rune::codec::Descriptor dropped = portTopic(2, 2);
    r.leaseSchemaUpdate(dropped, 2);
    r.leaseFilterSchema(dropped, 2);
    r.passes(30);
    ASSERT_TRUE(g.send({RawRec{keyOf(1), portValue(1, 0, 2, 0, "zz"), 1}}));
    const auto* e = firstRejection(r);
    ASSERT_NE(e, nullptr) << "the second grant of a live key was accepted";
    EXPECT_EQ(e->code, 56);
    EXPECT_EQ(colsOf(r, 1, kCols).v, granted.v) << "the live row is untouched";
}

TEST(LeaseJudgeWorkerRegression, LeaseSchemaOpsInEitherOrderRegression) {
    OrderOutcome updateFirst, filterFirst;
    ASSERT_NO_FATAL_FAILURE(runSchemaOrder(true, updateFirst));
    ASSERT_NO_FATAL_FAILURE(runSchemaOrder(false, filterFirst));
    EXPECT_EQ(updateFirst.k1, filterFirst.k1);
    EXPECT_EQ(updateFirst.k2, filterFirst.k2) << "the same final rows whichever op arrives first";
}

TEST(LeaseJudgeWorkerRegression, LeaseRebuildAfterCompactionRegression) {
    Ready g(/*rollBytes=*/1);
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({RawRec{keyOf(1), leaseValue(1, 0, "a1", 1, 0, 1, 7000), 1}}));   // grant, TTL 7000
    const int64_t version = int64_t(*colsOf(r, 1, kV1Columns).v[0]);
    ASSERT_TRUE(g.send({RawRec{keyOf(1), leaseValue(1, version, "u1", 1, 0, 9, 7000), 2}}));   // update, holder 9: carries the row's TTL (the judge requires it)
    ASSERT_EQ(r.readLog().data.size(), 2u);
    const Cols live = colsOf(r, 1, kV1Columns);   // the live row before the demote
    ASSERT_EQ(live.v[1], 7000u);

    for (int i = 0; i < 6 && r.readLog().data.size() > 1; ++i) {
        r.replica->setLastCleanOffset(0);
        (void)r.compact(1'000'000 + i * 5'000);
    }
    ASSERT_EQ(r.readLog().data.size(), 1u) << "the grant is compacted away: only the update of key 1 is left";

    ASSERT_NE(r.stats(), nullptr);
    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;
    r.demotePromote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(20);
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u) << "the live lease survived the rebuild from a compacted log";
    const Cols c = colsOf(r, 1, kV1Columns);
    EXPECT_EQ(c.v[0], uint64_t(version)) << "the version the update carries";
    EXPECT_EQ(c.v[1], 7000u) << "the grant's TTL, carried by the update";
    EXPECT_EQ(c.v[2], 9u);
    EXPECT_EQ(c.v[5], packed("u1"));
    EXPECT_EQ(c.v, live.v) << "the whole rebuilt row (every column, offsets included) equals the live row captured before the demote";

    // The fence holds: a second grant of the key is KeyExists.
    ASSERT_TRUE(g.send({grantRec(1, "zz")}));
    const auto* e = firstRejection(r);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->code, 56);
    EXPECT_EQ(colsOf(r, 1, kV1Columns).v[5], packed("u1"));
}

// R2: the lease layout depends on the installed versions (SchemaUpdate) as well as on FilterSchema.  An AddField whose
// FilterSchema never arrives must not hold a lease write forever.
TEST(LeaseJudgeWorkerRegression, LeaseLayoutWithoutFilterSchemaRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    ASSERT_NE(r.stats(), nullptr);

    // The v2 grant is replicated before its version is installed here: the judge holds at it.
    g.sendNoJudge({RawRec{keyOf(2), leaseValue(2, 0, "b2", 2, 4242), 1}});
    r.passes(30);
    ASSERT_GE(r.stats()->leaseLayoutWaits, 1u);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    const uint64_t rebuilds = r.stats()->keyIndexRebuilds;

    // Then the SchemaUpdate alone (no FilterSchema for v2, ever; no new leadership): the install moves the layout and the judge resumes.
    r.leaseSchemaUpdate(VerdictRig::leaseSourceDescriptor(2), 2);
    ASSERT_TRUE(r.driveUntilJudged()) << "held forever: the layout waited for a FilterSchema that never comes";
    r.passes(20);
    EXPECT_EQ(firstRejection(r), nullptr);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u) << "the v2 grant is accepted";
    const Cols two = colsOf(r, 2, kV2Columns);
    ASSERT_EQ(two.slots, kV2Columns) << "the row has the new column";
    EXPECT_EQ(two.v[6], 4242u);
    EXPECT_FALSE(colsOf(r, 1, kV2Columns).v[6].has_value());
    EXPECT_EQ(r.stats()->keyIndexRebuilds, rebuilds + 1) << "one rebuild for the new layout, no leadership change";
    EXPECT_TRUE(r.leaseReady());
}

TEST(LeaseJudgeWorkerRegression, LeaseIndexReadyRegression) {
    Ready g;
    VerdictRig& r = g.r;
    r.vs->setLeaseReplayCapForTest(3);   // applies to every index built from here on
    r.passes(30);
    EXPECT_TRUE(r.leaseReady());
    std::vector<RawRec> grants;
    for (uint64_t id = 1; id <= 8; ++id) grants.push_back(grantRec(id, "h"));
    ASSERT_TRUE(g.send(grants));
    r.passes(30);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 8u);
    EXPECT_TRUE(r.leaseReady());
    const uint32_t epoch0 = r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0);

    // A new leadership whose activation has not run yet (the judge is held): not ready.
    r.holdJudge(true);
    r.demotePromote();
    r.passes(30);
    EXPECT_FALSE(r.leaseReady());
    r.holdJudge(false);

    // The replay is bounded to 3 records a pass: not ready while the index trails the judge, ready after, never wrongly.
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    bool sawBehind = false;
    for (int i = 0; i < 400; ++i) {
        r.pass();
        const bool behind = judge->activated() && r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0) < judge->judgedThrough();
        if (behind) {
            sawBehind = true;
            EXPECT_FALSE(r.leaseReady()) << "pass " << i << ": the replay is still behind the judge";
        }
    }
    EXPECT_TRUE(sawBehind) << "vacuity guard: the replay took more than one pass";
    EXPECT_TRUE(r.leaseReady());
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 8u);
    const uint32_t epoch1 = r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0);
    EXPECT_GT(epoch1, epoch0) << "the rebuild bumped the epoch";

    // A stale layout (an AddField reached the evolving descriptor): not ready until rebuilt.
    r.holdJudge(true);
    r.addLeasePort();
    r.passes(30);
    EXPECT_FALSE(r.leaseReady());
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    EXPECT_TRUE(r.leaseReady());
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 8u);
    EXPECT_GT(r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0), epoch1);

    // R3: the readiness checks the replica's CURRENT facts, which the caller passes.
    const uint64_t gen = r.replica->leadershipGeneration();
    ASSERT_TRUE(r.leaseReady());
    EXPECT_FALSE(r.vs->leaseIndexReady(VerdictRig::kSrc, 0, gen, true, /*hwConfirmed=*/false)) << "the HW has not reached the leadership start";
    EXPECT_FALSE(r.vs->leaseIndexReady(VerdictRig::kSrc, 0, gen, /*isLeader=*/false, true));
    EXPECT_FALSE(r.vs->leaseIndexReady(VerdictRig::kSrc, 0, gen + 1, true, true)) << "the replica moved on to another leadership than the judge's";
    r.demote();
    EXPECT_FALSE(r.leaseReady()) << "after the demote the index is a stale leader's";
    r.holdJudge(true);
    r.promote();
    EXPECT_FALSE(r.leaseReady()) << "re-promoted: not ready until the judge activates under the new leadership";
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    EXPECT_TRUE(r.leaseReady());
}

// R7: the REPLAY (not the judge) holds on a version this node has not installed yet, and resumes after the install.
TEST(LeaseJudgeWorkerRegression, LeaseReplayHoldsOnUninstalledVersionRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    r.addLeasePort();
    ASSERT_TRUE(g.send({RawRec{keyOf(2), leaseValue(2, 0, "b2", 2, 4242), 1}}));   // judged (v2 installed), then covered by a marker
    r.passes(200);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);

    // The node restarts knowing v1 only: the judge's start is read from the markers, so the v2 record is REPLAYED, not judged.
    r.setSourceForRestart(VerdictRig::leaseSourceDescriptor(1));
    r.restart();
    r.passes(200);
    ASSERT_NE(r.stats(), nullptr);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    EXPECT_GE(r.stats()->leaseLayoutWaits, 1u) << "the replay waits for the version, it does not skip the record";
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u) << "the v2 record is not folded without its layout";
    EXPECT_LT(r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0), judge->judgedThrough()) << "vacuity guard: the judge is past the held record";
    EXPECT_FALSE(r.leaseReady());
    EXPECT_FALSE(judge->stalled());
    EXPECT_EQ(firstRejection(r), nullptr);

    r.leaseSchemaUpdate(VerdictRig::leaseSourceDescriptor(2), 2);
    r.leaseFilterSchema(VerdictRig::leaseSourceDescriptor(2), 2);
    r.passes(200);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u) << "the replay resumed after the install";
    EXPECT_EQ(colsOf(r, 2, kV2Columns).v[6], 4242u);
    EXPECT_EQ(firstRejection(r), nullptr);
    EXPECT_TRUE(r.leaseReady());
}
