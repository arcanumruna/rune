// Lease topics Task 6: the lease index inside a real worker (the leadership and layout rebuilds, the hook facts).
#include "CheckedWritesRig.h"

#include <rune/codec/Validator.h>
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
    explicit Ready(int64_t rollBytes = 64LL * 1024 * 1024, std::optional<rune::codec::Descriptor> src = std::nullopt, uint32_t maxKeys = 1000, RigTuning tuning = {}) : r(false, rollBytes, tuning) {
        r.createLeaseTopic(maxKeys, std::move(src));
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

constexpr uint8_t kRevokeAttr = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);   // Revoke | existing

struct Stored { int64_t offset; uint8_t attrs; std::string key; std::vector<uint8_t> value; bool crcOk; };
// Every data record in the log, in offset order, straight from the segments.
std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (int64_t off = 0; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
            const std::size_t total = static_cast<std::size_t>(rd.totalLength());
            if (total < 12 || p + total > v.len) break;
            if ((rd.attributes() & rune::protocol::batch::kAttrControl) == 0) {
                const bool crc = rd.ensureValid();
                (void)rune::protocol::forEachRecordSpan(v.data + p, total, [&](const rune::protocol::RecordSpan& s) {
                    out.push_back({s.offset, s.attributes, std::string(s.key), std::vector<uint8_t>(s.value.begin(), s.value.end()), crc});
                });
            }
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}
uint64_t le(const std::vector<uint8_t>& v, std::size_t at, int bytes = 8) {
    uint64_t x = 0;
    for (int i = 0; i < bytes; ++i) x |= uint64_t(v[at + std::size_t(i)]) << (8 * i);
    return x;
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
    ASSERT_TRUE(g.send({RawRec{keyOf(1), leaseValue(1, version, "u0", 1, 0, 8, 7000), 2}}));   // update, holder 8: superseded below
    ASSERT_TRUE(g.send({RawRec{keyOf(1), leaseValue(1, version, "u1", 1, 0, 9, 7000), 2}}));   // update, holder 9: carries the row's TTL (the judge requires it)
    ASSERT_EQ(r.readLog().data.size(), 3u);
    const int64_t latest = r.readLog().data.back().offset;
    const Cols live = colsOf(r, 1, kV1Columns);   // the live row before the demote
    ASSERT_EQ(live.v[1], 7000u);

    for (int i = 0; i < 6 && r.readLog().data.size() > 2; ++i) {
        r.replica->setLastCleanOffset(0);
        (void)r.compact(1'000'000 + i * 5'000);
    }
    const auto kept = r.readLog().data;
    ASSERT_EQ(kept.size(), 2u) << "the superseded update is compacted away; the live grant and the latest update are kept";
    EXPECT_EQ(kept[0].offset, version) << "the live version's grant survives compaction";
    EXPECT_EQ(kept[1].offset, latest);

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

    // U1: an update the index could not track (leaseUntrackedThrough) holds readiness until the judge has passed its offset.
    PartitionWriteState* w = const_cast<PartitionWriteState*>(r.writeState());
    ASSERT_NE(w, nullptr);
    const int64_t judged = w->judgedThrough;
    ASSERT_GT(judged, 0);
    w->leaseUntrackedThrough = judged;
    EXPECT_FALSE(r.leaseReady()) << "judgedThrough == leaseUntrackedThrough: that offset is not judged yet";
    w->leaseUntrackedThrough = judged - 1;
    EXPECT_TRUE(r.leaseReady()) << "judgedThrough > leaseUntrackedThrough";
    w->leaseUntrackedThrough = INT64_MAX;
    EXPECT_FALSE(r.leaseReady());
    w->leaseUntrackedThrough = -1;
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

// Fix-2 carry-in: a lease index adopted under an already-activated judge must replay the log under that leadership (it holds a
// row layout, not a fold), so the judge activates again; marking it live (kAdoptedLive) left it never ready.
TEST(LeaseJudgeWorkerRegression, LeaseAdoptedUnderActiveJudgeBecomesReadyRegression) {
    VerdictRig r;
    r.typed();
    TestRejectRule rule;
    rule.mode = TestRejectRule::Mode::TagEquals;
    rule.tag = 9;
    r.installRejectView(0x40000001, 7, rule);   // a view over the topic: its judge exists and activates before the lease spec
    r.passes(60);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    ASSERT_TRUE(judge->activated()) << "the judge is already active when the lease spec arrives";
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const int64_t otherCursor = r.vs->foldCursorForTest(0x40000001, 0);
    const std::size_t otherRejected = w->rejected.size();
    const ViewInstance* other = r.vs->instanceForTest(0x40000001, 0);
    ASSERT_NE(other, nullptr);
    r.createLeaseTopic();   // the WRITE_SPEC with the lease block, after the activation
    bool ready = false;
    for (int i = 0; i < 400 && !ready; ++i) {
        r.pass();
        ready = r.leaseReady();
    }
    EXPECT_TRUE(ready) << "the adopted lease index never became ready";
    // adopting the lease index does not touch the other member: its view state and the rejected index are as they were
    EXPECT_EQ(r.vs->instanceForTest(0x40000001, 0), other) << "the other member's view is the same instance, not rebuilt";
    EXPECT_GE(r.vs->foldCursorForTest(0x40000001, 0), otherCursor) << "its fold only moved forward";
    EXPECT_EQ(w->rejected.size(), otherRejected);
    EXPECT_EQ(w->rejected.nextRejectedAtOrAfter(0), INT64_MAX) << "nothing was rejected";
}

// Task 5a: a lease grant produced through the worker's real produce path (the ack sink knows the session's client version,
// the cluster is at the leases level, acks carry View) is stamped with its offset and the TTL raised, then judged and applied.
TEST(LeaseWriteHookWorkerRegression, GrantStampedRaisedAndJudgedRegression) {
    Ready g;
    VerdictRig& r = g.r;
    WorkerOp level;
    level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
    r.send(std::move(level));
    r.enableAcks(rune::protocol::client::kClientVersionLeases);
    r.passes(30);

    // ttl 0 (raised to the topic's min, 1000) and ttl 5000 (kept); the client's own lease_version (7777) is overwritten.
    const RawRec a{keyOf(1), leaseValue(1, 7777, "a1", 1, 0, 1, 0), 1};
    const RawRec b{keyOf(2), leaseValue(2, 7777, "b2", 1, 0, 1, 5000), 1};
    const int64_t c = r.produceBytes(VerdictRig::rawBatch({a, b}, 11, 0), rune::protocol::client::kAcksWireView, 11, 0);   // idempotent: an explicit-key intent needs it (checked writes 11.2)
    const auto ack = r.ack(c);
    ASSERT_TRUE(ack);
    EXPECT_EQ(ack->code, 0);
    EXPECT_TRUE(ack->hasVerdicts);
    EXPECT_TRUE(ack->verdicts.empty()) << "the judge accepted both grants";
    r.passes(30);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 2u);
    const Cols c1 = colsOf(r, 1, kV1Columns), c2 = colsOf(r, 2, kV1Columns);
    ASSERT_TRUE(c1.v[0].has_value());
    ASSERT_TRUE(c2.v[0].has_value());
    EXPECT_EQ(int64_t(*c1.v[0]), ack->baseOffset) << "lease_version is the record's offset";
    EXPECT_EQ(int64_t(*c2.v[0]), ack->baseOffset + 1);
    // the STORED records (not only the row) carry the stamped version and the raised ttl
    const auto stored = storedRecords(r);
    ASSERT_EQ(stored.size(), 2u);
    for (std::size_t i = 0; i < stored.size(); ++i) {
        EXPECT_EQ(int64_t(le(stored[i].value, 4)), stored[i].offset) << "the stored lease_version bytes are the record's own offset";
        EXPECT_TRUE(stored[i].crcOk);
    }
    EXPECT_EQ(le(stored[0].value, 12), 1000u);
    EXPECT_EQ(le(stored[1].value, 12), 5000u);
    EXPECT_EQ(*c1.v[1], 1000u) << "ttl 0 raised to min_ttl";
    EXPECT_EQ(*c2.v[1], 5000u);

    // The same write from an older session is refused by the hook (a definite InvalidRequest), nothing stored.
    const int64_t leo = r.leo();
    r.enableAcks(17);
    const int64_t c3 = r.produceBytes(VerdictRig::rawBatch({{keyOf(3), leaseValue(3, 0, "c3", 1, 0, 1, 5000), 1}}, 11, 2), rune::protocol::client::kAcksWireView, 11, 2);
    const auto ack3 = r.ack(c3);
    ASSERT_TRUE(ack3);
    EXPECT_EQ(ack3->code, int16_t(rune::protocol::client::ErrorCode::InvalidRequest));
    EXPECT_EQ(r.leo(), leo);
    EXPECT_EQ(r.worker->hookRefusalsForTest(HookRefusal::LeaseClientVersion), 1u) << "refused by the lease client-version rule, not another one";
}

// ── Task 5b: revokes through the real produce path ──

namespace {
struct Leased {
    Ready g;
    VerdictRig& r = g.r;
    int32_t seq = 0;
    // A Strict (durabilityTier 2) ack waits on a real fsync that takes wall time: the pass budget grows the way the
    // checked-writes matrix's strictFactor does, and a wait still ends as soon as the ack is in.
    int ackPasses = 600;
    explicit Leased(uint32_t maxKeys = 1000, uint32_t arenaEntries = 0, RigTuning tuning = {})
        : g(64LL * 1024 * 1024, std::nullopt, maxKeys, tuning), ackPasses(tuning.durabilityTier == 2 ? 600 * 50 : 600) {
        WorkerOp level;
        level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
        r.send(std::move(level));
        r.enableAcks(rune::protocol::client::kClientVersionLeases, arenaEntries);
        r.passes(30);
    }
    std::optional<VerdictRig::Ack> produce(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), rune::protocol::client::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        return r.ack(c, ackPasses);
    }
    std::optional<VerdictRig::Ack> produceNoSeq(const std::vector<RawRec>& recs, int32_t at) {   // a refused batch does not consume its sequence
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, at), rune::protocol::client::kAcksWireView, 11, at);
        return r.ack(c, ackPasses);
    }
};
}  // namespace

TEST(LeaseWriteHookWorkerRegression, RevokeReencodedFullRowRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto ack = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(ack);
    ASSERT_EQ(ack->code, 0);
    const int64_t grantOffset = ack->baseOffset;
    r.passes(30);
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u);
    r.addLeasePort();   // v2 adds `port`: the row layout gains a column and is rebuilt
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    // a client revoke at v1 with every user column zeroed
    const int64_t before = r.leo();
    const auto rack = l.produce({RawRec{keyOf(1), leaseValue(1, grantOffset, "", 1), kRevokeAttr}});
    ASSERT_TRUE(rack);
    EXPECT_EQ(rack->code, 0);
    EXPECT_TRUE(rack->verdicts.empty()) << "the judge accepted the revoke";
    r.passes(30);
    const auto stored = storedRecords(r);
    const Stored* rv = nullptr;
    int revokes = 0;
    for (const auto& s : stored) if (s.offset >= before && s.attrs == kRevokeAttr) { rv = &s; ++revokes; }
    ASSERT_EQ(revokes, 1) << "exactly one stored revoke";
    ASSERT_NE(rv, nullptr);
    EXPECT_TRUE(rv->crcOk);
    EXPECT_EQ(le(rv->value, 2, 2), 2u) << "re-encoded at the latest installed version";
    ASSERT_EQ(rv->value.size(), 64u);
    EXPECT_EQ(int64_t(le(rv->value, 4)), grantOffset) << "lease_version from the row";
    EXPECT_EQ(le(rv->value, 12), 5000u) << "ttl from the row";
    EXPECT_EQ(le(rv->value, 36), 1u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(rv->value.data()) + 44, 2), "a1") << "the user columns are the row's, not the client's zeros";
    EXPECT_EQ(le(rv->value, 60, 4), 0u);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u) << "the row is gone";
    EXPECT_EQ(firstRejection(r), nullptr);
}

TEST(LeaseWriteHookWorkerRegression, RevokeNoRowUnchangedRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t before = r.leo();
    const auto absentIn = leaseValue(2, 5, "zz", 1);
    const auto otherIn = leaseValue(1, g->baseOffset + 50, "yy", 1);
    const auto a1 = l.produce({RawRec{keyOf(2), absentIn, kRevokeAttr}});
    ASSERT_TRUE(a1);
    const auto a2 = l.produce({RawRec{keyOf(1), otherIn, kRevokeAttr}});
    ASSERT_TRUE(a2);
    ASSERT_EQ(a1->code, 0);
    ASSERT_EQ(a2->code, 0);
    ASSERT_EQ(a1->verdicts.size(), 1u);
    EXPECT_EQ(std::get<1>(a1->verdicts[0]), 62);
    ASSERT_EQ(a2->verdicts.size(), 1u);
    EXPECT_EQ(std::get<1>(a2->verdicts[0]), 62);
    std::vector<std::vector<uint8_t>> values;
    for (const auto& s : storedRecords(r)) if (s.offset >= before) values.push_back(s.value);
    ASSERT_EQ(values.size(), 2u);
    EXPECT_EQ(values[0], absentIn) << "stored bytes equal the input";
    EXPECT_EQ(values[1], otherIn);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 1u) << "the live lease is untouched";
}

TEST(LeaseWriteHookWorkerRegression, RevokeBehindUpdateThrottledRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t ver = g->baseOffset;
    r.holdJudge(true);   // the update below is appended but not judged
    const int64_t cu = r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(1), leaseValue(1, ver, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    r.passes(30);
    const int64_t leoAfterUpdate = r.leo();
    ASSERT_TRUE(r.leaseReady()) << "vacuity guard: the index is ready, so the refusal below is the lastAppended rule";
    const auto th = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, ver, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(th);
    EXPECT_EQ(th->code, int16_t(rune::protocol::client::ErrorCode::Throttled));
    EXPECT_EQ(th->retryAfter, 1'000'000) << "retry-after 1 ms";
    EXPECT_EQ(r.leo(), leoAfterUpdate) << "nothing stored";
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    const auto au = r.ack(cu);
    ASSERT_TRUE(au);
    EXPECT_EQ(au->code, 0);
    EXPECT_TRUE(au->verdicts.empty());
    const int64_t before = r.leo();
    const auto ok = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, ver, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(ok);
    EXPECT_EQ(ok->code, 0);
    EXPECT_TRUE(ok->verdicts.empty());
    int revokes = 0;
    for (const auto& s : storedRecords(r))
        if (s.offset >= before && s.attrs == kRevokeAttr) {
            ++revokes;
            EXPECT_EQ(std::string(reinterpret_cast<const char*>(s.value.data()) + 44, 2), "zz") << "the stored revoke carries the UPDATED columns";
        }
    EXPECT_EQ(revokes, 1) << "exactly one stored revoke";
    r.passes(30);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u);
}

// ── U1: updates appended before the lease index tracked them (spec 6.2, L19) ──
namespace {
// 4 batches of 2048 grants (8192 accepted records = kLeaseFoldEventCap): one judge pass fills its lease event buffer with them and
// stops before the next batch, so the index is ready (activated, replayed up to the judge) with that next batch still unjudged.
void appendBulkGrants(Leased& l) {
    for (int b = 0; b < 4; ++b) {
        std::vector<RawRec> recs;
        for (uint64_t i = 0; i < 2048; ++i) { const uint64_t id = 100 + uint64_t(b) * 2048 + i; recs.push_back(RawRec{keyOf(id), leaseValue(id, 0, "b"), 1}); }
        (void)l.r.produceBytes(VerdictRig::rawBatch(recs, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
        l.seq += 2048;
        l.r.passes(3);
    }
}
// Release the held judge and pass until it has activated: its first pass judges the bulk and stops before the update.
void releaseUntilReadyWithTailUnjudged(VerdictRig& r) {
    r.holdJudge(false);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    for (int i = 0; i < 20 && !judge->activated(); ++i) r.pass();
    ASSERT_TRUE(judge->activated());
    ASSERT_GE(r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0), judge->judgedThrough()) << "vacuity guard: the index replayed up to the judge, so only leaseUntrackedThrough keeps it not ready";
    ASSERT_LT(judge->judgedThrough(), r.leo()) << "vacuity guard: the update is still unjudged";
}
void expectRevokeThrottledThenCarriesUpdate(Leased& l, int64_t ver, bool releaseHold = false) {
    VerdictRig& r = l.r;
    const int64_t leo = r.leo();
    const auto th = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, ver, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(th);
    EXPECT_EQ(th->code, int16_t(rune::protocol::client::ErrorCode::Throttled)) << "the update is not judged yet";
    EXPECT_EQ(th->retryAfter, 1'000'000);
    EXPECT_EQ(r.leo(), leo) << "nothing stored";
    if (releaseHold) r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    const int64_t before = r.leo();
    const auto ok = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, ver, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(ok);
    EXPECT_EQ(ok->code, 0);
    EXPECT_TRUE(ok->verdicts.empty());
    int revokes = 0;
    for (const auto& s : storedRecords(r))
        if (s.offset >= before && s.attrs == kRevokeAttr) {
            ++revokes;
            EXPECT_EQ(std::string(reinterpret_cast<const char*>(s.value.data()) + 44, 2), "zz") << "the stored revoke carries the UPDATED columns";
        }
    EXPECT_EQ(revokes, 1);
}
}  // namespace

TEST(LeaseWriteHookWorkerRegression, RevokeAfterLayoutRebuildSeesPendingUpdateRegression) {
    Leased l(10000);
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t ver = g->baseOffset;
    r.holdJudge(true);
    appendBulkGrants(l);
    (void)r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(1), leaseValue(1, ver, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    r.passes(3);
    r.addLeasePort();   // an AddField: the layout rebuild replaces the lease table, whose lastAppended started at -1
    r.passes(5);
    releaseUntilReadyWithTailUnjudged(r);
    expectRevokeThrottledThenCarriesUpdate(l, ver);
}

// The tail is 8192 grants then the update: a judge pass stops before it (the lease event buffer is full).
TEST(LeaseWriteHookWorkerRegression, RevokeAfterFailoverSeesTailUpdateRegression) {
    Leased l(10000);
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t ver = g->baseOffset;
    r.holdJudge(true);
    appendBulkGrants(l);
    (void)r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(1), leaseValue(1, ver, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    r.passes(3);
    r.demote();
    r.promote();   // a new leadership: a fresh lease table
    r.passes(5);
    releaseUntilReadyWithTailUnjudged(r);
    expectRevokeThrottledThenCarriesUpdate(l, ver);
}

TEST(LeaseWriteHookWorkerRegression, UpdateWhileNotReadyTracksRegression) {
    Leased l(10000);
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t ver = g->baseOffset;
    r.demote();
    r.holdJudge(true);
    r.promote();
    r.passes(30);
    ASSERT_FALSE(r.leaseReady()) << "vacuity guard: the judge has not activated, so the index is not ready while the updates arrive";
    appendBulkGrants(l);
    (void)r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(1), leaseValue(1, ver, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    r.passes(3);
    releaseUntilReadyWithTailUnjudged(r);
    expectRevokeThrottledThenCarriesUpdate(l, ver);
}

TEST(LeaseWriteHookWorkerRegression, RevokeWhileIndexNotReadyThrottledRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    r.demote();
    r.holdJudge(true);
    r.promote();
    r.passes(30);
    ASSERT_FALSE(r.leaseReady()) << "vacuity guard: the judge has not activated under the new leadership";
    const int64_t leo = r.leo();
    const auto th = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, g->baseOffset, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(th);
    EXPECT_EQ(th->code, int16_t(rune::protocol::client::ErrorCode::Throttled));
    EXPECT_EQ(th->retryAfter, 1'000'000);
    EXPECT_EQ(r.leo(), leo);
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    ASSERT_TRUE(r.leaseReady());
    const auto ok = l.produceNoSeq({RawRec{keyOf(1), leaseValue(1, g->baseOffset, "", 1), kRevokeAttr}}, l.seq);
    ASSERT_TRUE(ok);
    EXPECT_EQ(ok->code, 0);
}

// ── lease sweep 1 ──

// Item 2: the fold event buffer (~320 KiB) is a leader's; a demoted partition gives it back and stops counting it.
TEST(LeaseJudgeWorkerRegression, LeaseEventBufferReleasedOnDemoteRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const std::size_t reserved = w->leaseEvents.reservedBytes();
    ASSERT_GT(reserved, 0u);
    const std::size_t counted = r.vs->leaseViewMemoryForTest(VerdictRig::kSrc, 0);
    EXPECT_GE(counted, reserved) << "the buffer is part of the lease view's memory";

    r.demote();
    r.passes(10);
    EXPECT_EQ(w->leaseEvents.reservedBytes(), 0u) << "released when the partition stops leading";
    const std::size_t after = r.vs->leaseViewMemoryForTest(VerdictRig::kSrc, 0);
    EXPECT_EQ(counted - after, reserved) << "and no longer counted";

    r.promote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    EXPECT_EQ(w->leaseEvents.reservedBytes(), reserved) << "reserved again on the next leadership";
    EXPECT_GE(r.vs->leaseViewMemoryForTest(VerdictRig::kSrc, 0), reserved);
    EXPECT_TRUE(r.leaseReady());
}

// Lease topics Task 21: a worker counts the lease rows of the partitions it LEADS against the namespace's slot (max-leases): the
// fold charges them, a demote uncharges (a follower holds no lease state), the next leader pass charges again, and a freed
// slot reads zero.
TEST(LeaseJudgeWorkerRegression, LeaseCountChargedWhileLeadingRegression) {
    constexpr uint16_t kLeaseNsSlot = 3;
    constexpr uint32_t kLeaseNsId = 91;
    VerdictRig r(false);
    r.worker->applyNsSlotAssignDirect(kLeaseNsSlot, kLeaseNsId, rune::protocol::NamespaceLimits{});
    r.log->setNsTag(kLeaseNsId, kLeaseNsSlot);
    r.createLeaseTopic(1000, std::nullopt);
    r.passes(30);
    const auto send = [&](const std::vector<RawRec>& recs) {
        r.demote();
        r.replicate({VerdictRig::rawBatch(recs)});
        r.promote();
        r.passes(30);
        return r.driveUntilJudged();
    };
    ASSERT_TRUE(send({grantRec(1, "a1"), grantRec(2, "a2"), grantRec(3, "a3")}));
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 3u) << "the leader counts its live lease rows";
    EXPECT_EQ(r.vs->nsSlotMismatchesForTest(), 0u);

    ASSERT_TRUE(send({grantRec(4, "a4")}));
    r.passes(30);
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 4u) << "a delta, not a re-add";

    r.demote();
    r.passes(10);
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 0u) << "a follower holds no lease state: it counts none";

    r.promote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 4u) << "the next leader pass charges again, from the rebuilt index";

    r.worker->applyNsSlotReleaseDirect(kLeaseNsSlot, kLeaseNsId);
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 0u) << "a freed slot reads zero";
    r.passes(30);
    EXPECT_EQ(r.vs->nsLeases(kLeaseNsSlot), 0u) << "and a stale instance never charges the slot again";
}

// Batch F (C3): freeing a namespace slot zeroes its lease count AT ONCE, while the key index of the dropped namespace still holds
// charged rows, and the slot placed for another namespace starts from zero and is never charged by the stale instance.  Mutation:
// skip the nsLeases_ reset in WorkerViewSet::onNsSlotFreed -> the freed slot still reads the old count, red.
TEST(LeaseJudgeWorkerRegression, NsLeasesSlotFreedResetsRegression) {
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kOldNs = 91;
    constexpr uint32_t kNewNs = 92;
    VerdictRig r(false);
    r.worker->applyNsSlotAssignDirect(kSlot, kOldNs, rune::protocol::NamespaceLimits{});
    r.log->setNsTag(kOldNs, kSlot);
    r.createLeaseTopic(1000, std::nullopt);
    r.passes(30);
    r.demote();
    r.replicate({VerdictRig::rawBatch({grantRec(1, "a1"), grantRec(2, "a2")})});
    r.promote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    ASSERT_EQ(r.vs->nsLeases(kSlot), 2u) << "precondition: the old namespace is charged";

    r.worker->applyNsSlotReleaseDirect(kSlot, kOldNs);
    EXPECT_EQ(r.vs->nsLeases(kSlot), 0u) << "the freed slot reads zero while the key index is still present";
    r.worker->applyNsSlotAssignDirect(kSlot, kNewNs, rune::protocol::NamespaceLimits{});
    EXPECT_EQ(r.vs->nsLeases(kSlot), 0u) << "the slot placed for another namespace starts from zero";
    r.passes(30);
    EXPECT_EQ(r.vs->nsLeases(kSlot), 0u) << "and the stale instance of the old namespace never charges it";
}

// Item 4: AddField is a SchemaUpdate plus a FilterSchema; whichever arrives first, the index is rebuilt ONCE.
TEST(LeaseJudgeWorkerRegression, LeaseSchemaOpsRebuildOnceRegression) {
    for (const bool updateFirst : {true, false}) {
        SCOPED_TRACE(updateFirst ? "SchemaUpdate first" : "FilterSchema first");
        Ready g;
        VerdictRig& r = g.r;
        ASSERT_TRUE(g.send({grantRec(1, "a1")}));
        r.passes(60);
        ASSERT_NE(r.stats(), nullptr);
        const uint64_t before = r.stats()->keyIndexRebuilds;
        const rune::codec::Descriptor v2 = VerdictRig::leaseSourceDescriptor(2);
        if (updateFirst) { r.leaseSchemaUpdate(v2, 2); r.passes(30); r.leaseFilterSchema(v2, 2); }
        else             { r.leaseFilterSchema(v2, 2); r.passes(30); r.leaseSchemaUpdate(v2, 2); }
        r.passes(60);
        ASSERT_TRUE(r.driveUntilJudged());
        r.passes(30);
        EXPECT_EQ(r.stats()->keyIndexRebuilds - before, 1u);
        EXPECT_EQ(colsOf(r, 1, kV2Columns).slots, kV2Columns) << "vacuity guard: the layout did change";
    }
}

// Item 5: a FilterSchema whose build throws (client version 0). A non-lease topic keeps its evolving descriptor; a lease topic
// still gets the layout bump (the index must not stay on a layout the SchemaUpdate already outgrew).
TEST(LeaseJudgeWorkerRegression, FilterSchemaThrowKeepsEvolvingForNonLeaseRegression) {
    VerdictRig r;
    r.typed();
    r.passes(30);
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    ASSERT_FALSE(w->spec->lease);
    ASSERT_TRUE(w->spec->evolving.has_value());
    const std::size_t fields = w->spec->evolving->fields.size();
    rune::codec::DescriptorBuilder b("t", 2);
    b.add("id", rune::codec::PrimType::UInt64, 1, true).add("v", rune::codec::PrimType::Int64).add("tag", rune::codec::PrimType::UInt16)
        .add("extra", rune::codec::PrimType::UInt32, 2);
    WorkerOp f;
    f.type = WorkerOp::Type::FilterSchema; f.topicId = VerdictRig::kSrc; f.clientVersion = 0;   // FilterSchema::build refuses version 0
    f.schemaUpdateBlob = rune::codec::serialize(b.build());
    r.send(std::move(f));
    r.passes(10);
    ASSERT_NE(r.worker, nullptr);
    EXPECT_GE(r.worker->filterSchemaErrorsForTest(), 1u) << "vacuity guard: the build did throw";
    EXPECT_EQ(w->spec->evolving->fields.size(), fields) << "a refused FilterSchema leaves the evolving descriptor as it was";
}

TEST(LeaseJudgeWorkerRegression, FilterSchemaThrowLeaseLayoutBumpedRegression) {
    Ready g;
    VerdictRig& r = g.r;
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const uint32_t gen = w->spec->leaseLayoutGen;
    r.leaseFilterSchema(VerdictRig::leaseSourceDescriptor(2), 0);   // a new field id, and a build that throws
    r.passes(10);
    ASSERT_GE(r.worker->filterSchemaErrorsForTest(), 1u) << "vacuity guard: the build did throw";
    EXPECT_EQ(w->spec->leaseLayoutGen, gen + 1) << "the layout bump does not depend on the build";
    ASSERT_TRUE(w->spec->evolving.has_value());
    EXPECT_EQ(w->spec->evolving->fields.size(), 7u) << "the lease topic's evolving descriptor has the new field";
}

// Item 9: a v2 Enum whose whitelist excludes 0, never set by the v1 grant.  The revoke is re-encoded at v2 from the row: the Enum
// must hold a whitelisted value or the stored record fails the v2 Validator.
TEST(LeaseWriteHookWorkerRegression, RevokeNullEnumPassesValidatorRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto ack = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(ack);
    ASSERT_EQ(ack->code, 0);
    r.passes(30);
    rune::codec::DescriptorBuilder b("inst", 2);
    b.add("lease_version", PrimType::Int64).add("lease_ttl_ms", PrimType::UInt64).add("lease_holder", PrimType::UInt64)
        .add("lease_remaining_ms", PrimType::UInt64).add("id", PrimType::UInt64, 1, true).addFixedStr("addr", 16)
        .add("kind", PrimType::Enum, 2, false, {5, 6});
    const rune::codec::Descriptor v2 = b.build();
    r.leaseSchemaUpdate(v2, 2);
    r.leaseFilterSchema(v2, 2);
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    const int64_t before = r.leo();
    const auto rack = l.produce({RawRec{keyOf(1), leaseValue(1, ack->baseOffset, "", 1), kRevokeAttr}});
    ASSERT_TRUE(rack);
    ASSERT_EQ(rack->code, 0);
    EXPECT_TRUE(rack->verdicts.empty());
    r.passes(30);
    const rune::codec::Descriptor w2 = rune::codec::materializeAsOf(v2, 2);
    const rune::codec::Validator validator(w2);
    int revokes = 0;
    for (const auto& s : storedRecords(r))
        if (s.offset >= before && s.attrs == kRevokeAttr) {
            ++revokes;
            EXPECT_EQ(le(s.value, 2, 2), 2u);
            const auto res = validator.validate(s.value);
            EXPECT_TRUE(res.ok) << (res.reason != nullptr ? res.reason : "");
        }
    EXPECT_EQ(revokes, 1);
}

// Item 12: leaseUntrackedThrough belongs to one activation. A value left by an earlier leadership (an untracked update at an
// offset a truncation later removed) is overwritten, not kept by a max.
TEST(LeaseJudgeWorkerRegression, LeaseUntrackedResetOnActivationRegression) {
    Ready g;
    VerdictRig& r = g.r;
    ASSERT_TRUE(g.send({grantRec(1, "a1")}));
    r.passes(30);
    PartitionWriteState* w = const_cast<PartitionWriteState*>(r.writeState());
    ASSERT_NE(w, nullptr);
    ASSERT_TRUE(r.leaseReady());
    w->leaseUntrackedThrough = r.leo() + 500;   // a stale mark above anything the log will hold soon
    EXPECT_FALSE(r.leaseReady());
    r.demote();
    r.promote();
    r.passes(30);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    EXPECT_LT(w->leaseUntrackedThrough, r.leo()) << "the stale mark did not survive the activation";
    EXPECT_EQ(w->leaseUntrackedThrough, w->judgedThrough - 1) << "assigned LEO - 1 at activation";
    EXPECT_TRUE(r.leaseReady()) << "ready once the judge is past LEO";
}

// Item 13 (b): an update of a key with no row, while the index is ready, still raises the mark to the batch's last offset (the
// hook cannot track it in the row, so readiness waits for the judge to pass it).
TEST(LeaseWriteHookWorkerRegression, UpdateOfKeyWithoutRowRaisesUntrackedRegression) {
    Leased l;
    VerdictRig& r = l.r;
    ASSERT_TRUE(r.leaseReady());
    const PartitionWriteState* w = r.writeState();
    ASSERT_NE(w, nullptr);
    const int64_t mark = w->leaseUntrackedThrough;
    r.holdJudge(true);
    const int64_t c = r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(99), leaseValue(99, 5, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    (void)c;
    r.passes(5);
    EXPECT_EQ(w->leaseUntrackedThrough, r.leo() - 1) << "the appended update's offset";
    EXPECT_GT(w->leaseUntrackedThrough, mark);
    EXPECT_FALSE(r.leaseReady()) << "not ready until the judge has passed it";
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(30);
    EXPECT_TRUE(r.leaseReady());
}

// Item 13 (a): activation has COMPLETED, the replay still trails the judge (one batch a pass), and an update is appended.  The
// revoke must wait, then carry the update's columns.  Key 1's row is already replayed, so lastAppended is what holds the revoke
// here; the leaseUntrackedThrough raise for an unrecordable update is pinned at the hook (UpdateUntrackedRaisesMarkRegression):
// in a worker the replay is gated on the judge, so it cannot catch up while an update with no row is still unjudged.
TEST(LeaseWriteHookWorkerRegression, UpdateAfterActivationWhileReplayTrailsRegression) {
    Leased l(10000);
    VerdictRig& r = l.r;
    const auto g = l.produce({RawRec{keyOf(1), leaseValue(1, 0, "a1"), 1}});
    ASSERT_TRUE(g);
    r.passes(30);
    const int64_t ver = g->baseOffset;
    r.vs->setLeaseReplayCapForTest(1);   // every index built from here on replays one batch a pass
    r.demote();
    r.holdJudge(true);
    r.promote();
    r.passes(30);
    appendBulkGrants(l);
    r.holdJudge(false);
    const VerdictSource* judge = r.vs->verdictSource(VerdictRig::kSrc, 0);
    ASSERT_NE(judge, nullptr);
    for (int i = 0; i < 20 && !judge->activated(); ++i) r.pass();
    ASSERT_TRUE(judge->activated());
    ASSERT_LT(r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0), judge->judgedThrough()) << "vacuity guard: activation completed, the replay trails the judge";
    (void)r.produceBytes(VerdictRig::rawBatch({RawRec{keyOf(1), leaseValue(1, ver, "zz"), 2}}, 11, l.seq), rune::protocol::client::kAcksWireView, 11, l.seq);
    ++l.seq;
    r.pass();   // the worker appends the update
    ASSERT_GT(r.leo(), r.vs->keyIndexFoldCursorForTest(VerdictRig::kSrc, 0)) << "vacuity guard: appended, the replay has not reached it";
    ASSERT_FALSE(r.leaseReady());
    expectRevokeThrottledThenCarriesUpdate(l, ver);
}

// ── Task 5c: the View ack's lease trailer (L2) and known-old duplicates (L6) ──

namespace {
using Lease = std::vector<std::pair<int64_t, uint64_t>>;
using Verd = std::vector<std::tuple<uint16_t, uint16_t, uint32_t>>;
constexpr int16_t kDupSeq = int16_t(rune::protocol::client::ErrorCode::DuplicateSequence);
constexpr uint8_t kUpdateAttr = 2;   // Put | existing
RawRec grantTtl(uint64_t id, uint64_t ttl) { return RawRec{keyOf(id), leaseValue(id, 0, "g", 1, 0, 1, ttl), 1}; }
RawRec updateTtl(uint64_t id, int64_t ver, uint64_t ttl) { return RawRec{keyOf(id), leaseValue(id, ver, "u", 1, 0, 1, ttl), kUpdateAttr}; }
RawRec revokeOf(uint64_t id, int64_t ver) { return RawRec{keyOf(id), leaseValue(id, ver, "", 1), kRevokeAttr}; }
}  // namespace

TEST(LeaseWriteHookWorkerRegression, LeaseAckTrailerRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const auto g = l.produce({grantTtl(3, 4000), grantTtl(4, 6000), grantTtl(5, 7000)});
    ASSERT_TRUE(g);
    ASSERT_EQ(g->code, 0);
    r.passes(30);
    const int64_t v3 = g->baseOffset, v4 = g->baseOffset + 1, v5 = g->baseOffset + 2;
    const auto a = l.produce({grantTtl(1, 0), grantTtl(2, 3000), updateTtl(3, v3, 1), revokeOf(4, v4), updateTtl(5, v5 + 99, 1)});
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    ASSERT_TRUE(a->hasLease);
    ASSERT_EQ(a->leases.size(), 5u);
    EXPECT_EQ(a->leases[0], (std::pair<int64_t, uint64_t>{a->baseOffset + 0, 1000})) << "ttl 0 raised to min_ttl";
    EXPECT_EQ(a->leases[1], (std::pair<int64_t, uint64_t>{a->baseOffset + 1, 3000}));
    EXPECT_EQ(a->leases[2], (std::pair<int64_t, uint64_t>{v3, 4000})) << "an update keeps the row's ttl, not the record's 1";
    EXPECT_EQ(a->leases[3], (std::pair<int64_t, uint64_t>{v4, 6000})) << "the revoke carries the row's version and ttl";
    EXPECT_EQ(a->leases[4], (std::pair<int64_t, uint64_t>{-1, 0})) << "a rejected record";
    ASSERT_EQ(a->verdicts.size(), 1u);
    EXPECT_EQ(std::get<0>(a->verdicts[0]), 4);
    EXPECT_EQ(std::get<1>(a->verdicts[0]), 62);
    EXPECT_EQ(a->frameLen, 54u + 2 + 8 * 1 + 3 + 16 * 5);
}

TEST(LeaseWriteHookWorkerRegression, LeaseInWindowRetryRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const int32_t at = l.seq;
    const auto a = l.produce({grantTtl(1, 3000)});
    ASSERT_TRUE(a);
    ASSERT_EQ(a->code, 0);
    ASSERT_TRUE(a->hasLease);
    ASSERT_EQ(a->leases.size(), 1u);
    r.passes(30);
    const int64_t leo = r.leo();
    const int64_t c = r.produceBytes(VerdictRig::rawBatch({grantTtl(1, 3000)}, 11, at), rune::protocol::client::kAcksWireView, 11, at);
    const auto b = r.ack(c, 3000);
    ASSERT_TRUE(b);
    EXPECT_EQ(b->code, 0);
    EXPECT_EQ(b->baseOffset, a->baseOffset);
    EXPECT_TRUE(b->hasLease);
    EXPECT_EQ(b->leases, a->leases);
    EXPECT_EQ(r.leo(), leo) << "nothing was appended again";
}

namespace {
// Producer 11 writes six lease batches (one record each), then `fillers` more grants so the first six are known-old, and
// resends nothing: the caller resends.  Returns the six batches with their sequences.
struct DupBatches {
    std::vector<std::vector<RawRec>> batches;
    std::vector<int32_t> seqs;
    int64_t v1 = 0;
};
DupBatches writeDupBatches(Leased& l, int fillers) {
    DupBatches d;
    const auto send = [&](std::vector<RawRec> recs) {
        d.seqs.push_back(l.seq);
        const auto a = l.produce(recs);
        EXPECT_TRUE(a);
        EXPECT_EQ(a ? a->code : -1, 0);
        l.r.passes(30);
        d.batches.push_back(std::move(recs));
        return a;
    };
    const auto g1 = send({grantTtl(1, 3000)});                   // 0: grant k1 (stays live)
    d.v1 = g1->baseOffset;
    const auto g2 = send({grantTtl(2, 3000)});                   // 1: grant k2
    send({revokeOf(2, g2->baseOffset)});                         // 2: revoke k2 (k2 ends)
    send({updateTtl(1, d.v1, 3000)});                            // 3: update of live k1
    send({updateTtl(2, g2->baseOffset, 3000)});                  // 4: update of k2, refused (no row), stored
    send({revokeOf(1, d.v1 + 50)});                              // 5: revoke of k1 at another version, refused, stored
    for (int i = 0; i < fillers; ++i) {
        const auto f = l.produce({grantTtl(100 + uint64_t(i), 3000)});
        EXPECT_TRUE(f);
        l.r.passes(30);
    }
    return d;
}
}  // namespace

TEST(LeaseWriteHookWorkerRegression, LeaseKnownOldDuplicateTableRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const DupBatches d = writeDupBatches(l, 40);
    r.settle(150);
    const int64_t leo = r.leo();
    const uint64_t unknown = r.stats()->verdictUnknownDuplicates;
    const std::vector<Verd> expect = {
        Verd{{0, 21, 0}},   // grant k1: the client maps it to VerdictUnknown
        Verd{{0, 21, 0}},   // grant k2
        Verd{{0, 62, 0}},   // revoke k2
        Verd{},             // update of live k1: Ok
        Verd{{0, 62, 0}},   // update of k2, not live
        Verd{{0, 62, 0}},   // revoke of k1 at another version
    };
    for (std::size_t i = 0; i < d.batches.size(); ++i) {
        const auto a = l.produceNoSeq(d.batches[i], d.seqs[i]);
        ASSERT_TRUE(a) << "batch " << i;
        EXPECT_EQ(a->code, 0) << "batch " << i;
        EXPECT_EQ(a->baseOffset, -1) << "batch " << i << ": a known-old duplicate has no offset";
        ASSERT_TRUE(a->hasLease) << "batch " << i;
        EXPECT_EQ(a->leases, (Lease{{-1, 0}})) << "batch " << i;
        EXPECT_EQ(a->verdicts, expect[i]) << "batch " << i;
        EXPECT_EQ(r.leo(), leo) << "batch " << i << ": nothing appended";
    }
    EXPECT_EQ(r.stats()->verdictUnknownDuplicates, unknown + 6);
}

TEST(LeaseWriteHookWorkerRegression, LeaseKnownOldDuplicateIndexNotReadyThrottledRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const DupBatches d = writeDupBatches(l, 40);
    r.settle(150);
    const int64_t leo = r.leo();
    r.demote();
    r.holdJudge(true);
    r.promote();
    r.passes(30);
    ASSERT_FALSE(r.leaseReady()) << "vacuity guard: the judge has not activated under the new leadership";
    const auto th = l.produceNoSeq(d.batches[3], d.seqs[3]);   // the update of live k1
    ASSERT_TRUE(th);
    EXPECT_EQ(th->code, int16_t(rune::protocol::client::ErrorCode::Throttled));
    EXPECT_EQ(th->retryAfter, 1'000'000);
    EXPECT_EQ(r.leo(), leo) << "nothing appended";
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(60);
    ASSERT_TRUE(r.leaseReady());
    const auto ok = l.produceNoSeq(d.batches[3], d.seqs[3]);
    ASSERT_TRUE(ok);
    EXPECT_EQ(ok->code, 0);
    EXPECT_EQ(ok->baseOffset, -1);
    EXPECT_TRUE(ok->hasLease);
    EXPECT_TRUE(ok->verdicts.empty()) << "the update is live";
    EXPECT_EQ(r.leo(), leo);
}

// Review C1 (lease Task 5c): a known-old duplicate is found before the hook, so its key and value were never checked.  A 4 KiB
// key on an update was copied into a 128-byte ViewKey on the worker's stack; a short value was read as version 0.  Each one
// is now answered "not live" (62).
TEST(LeaseWriteHookWorkerRegression, LeaseKnownOldDuplicateOversizedKeyAdversarialRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const DupBatches d = writeDupBatches(l, 40);
    r.settle(150);
    const int64_t leo = r.leo();
    const int32_t seq = d.seqs[3];   // the slot of the update of live k1
    const std::vector<RawRec> bad[] = {
        {RawRec{std::string(4096, 'k'), leaseValue(1, d.v1, "u", 1, 0, 1, 3000), kUpdateAttr}},   // far wider than any view key
        {RawRec{std::string(7, 'k'), leaseValue(1, d.v1, "u", 1, 0, 1, 3000), kUpdateAttr}},      // not the key width
        {RawRec{keyOf(1), std::vector<uint8_t>(6, 0), kUpdateAttr}},                               // too short for the system columns
    };
    for (std::size_t i = 0; i < std::size(bad); ++i) {
        const auto a = l.produceNoSeq(bad[i], seq);
        ASSERT_TRUE(a) << i;
        EXPECT_EQ(a->code, 0) << i;
        EXPECT_EQ(a->baseOffset, -1) << i;
        ASSERT_TRUE(a->hasLease) << i;
        EXPECT_EQ(a->leases, (Lease{{-1, 0}})) << i;
        EXPECT_EQ(a->verdicts, (Verd{{0, 62, 0}})) << i;
        EXPECT_EQ(r.leo(), leo) << i;
    }
    const auto live = l.produceNoSeq(d.batches[3], seq);   // the real duplicate is still answered live
    ASSERT_TRUE(live);
    EXPECT_TRUE(live->verdicts.empty());
}

// Review I2 (lease Task 5c): a known-old update live at its version was answered Ok because the version compare was assumed to
// be its only judgment.  Once any other constraint member was logged for the partition, the original may have been rejected by
// it, so the duplicate is {DuplicateSequence} (the client's VerdictUnknown), never Ok.
TEST(LeaseWriteHookWorkerRegression, LeaseKnownOldUpdateWithOtherMemberIsUnknownRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const DupBatches d = writeDupBatches(l, 40);
    r.settle(150);
    TestRejectRule none;
    none.mode = TestRejectRule::Mode::OffsetSet;   // rejects nothing
    r.installRejectView(/*viewTopicId=*/77, /*objectId=*/0x77, none);
    for (int i = 0; i < 200 && r.writeState()->constraintSets.latest().count < 2; ++i) r.pass();
    ASSERT_EQ(r.writeState()->constraintSets.latest().count, 2) << "vacuity guard: the reject view joined the lease index's set";
    r.passes(60);
    ASSERT_TRUE(r.leaseReady());
    const auto a = l.produceNoSeq(d.batches[3], d.seqs[3]);   // the update of live k1
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(a->baseOffset, -1);
    EXPECT_EQ(a->verdicts, (Verd{{0, 21, 0}})) << "another member may have rejected the original";
}

// Review minor (lease Task 5c): a known-old lease duplicate without View acks gets the plain duplicate answer, no trailer.
TEST(LeaseWriteHookWorkerRegression, LeaseKnownOldDuplicateWithoutViewHasNoTrailerRegression) {
    Leased l;
    VerdictRig& r = l.r;
    const DupBatches d = writeDupBatches(l, 40);
    r.settle(150);
    const int64_t c = r.produceBytes(VerdictRig::rawBatch(d.batches[3], 11, d.seqs[3]), rune::protocol::client::kAcksWireAll, 11, d.seqs[3]);
    const auto a = r.ack(c);
    ASSERT_TRUE(a);
    EXPECT_EQ(a->code, 0);
    EXPECT_FALSE(a->hasLease);
    EXPECT_TRUE(a->verdicts.empty());
}

TEST(LeaseWriteHookWorkerRegression, LeaseAckArenaFullDefersRegression) {
    RigTuning tuning;
    tuning.ackTimeoutNs = 50'000'000;
    Leased l(1000, /*arenaEntries=*/8, tuning);
    VerdictRig& r = l.r;
    // 5 records need 10 lease units: the 8-unit arena can never hold them.
    const auto big = l.produce({grantTtl(1, 0), grantTtl(2, 0), grantTtl(3, 0), grantTtl(4, 0), grantTtl(5, 0)});
    ASSERT_TRUE(big) << "answered at the deadline";
    EXPECT_EQ(big->code, int16_t(rune::protocol::client::ErrorCode::Internal)) << "the trailer never fit: deferred until the deadline";
    EXPECT_FALSE(big->hasLease);
    // nothing leaked: 4-record batches (8 units) and a single one still fit, over and over
    for (uint64_t i = 0; i < 3; ++i) {
        const auto four = l.produce({grantTtl(10 + 4 * i, 0), grantTtl(11 + 4 * i, 0), grantTtl(12 + 4 * i, 0), grantTtl(13 + 4 * i, 0)});
        ASSERT_TRUE(four) << i;
        EXPECT_EQ(four->code, 0);
        EXPECT_EQ(four->leases.size(), 4u);
    }
    const auto one = l.produce({grantTtl(99, 0)});
    ASSERT_TRUE(one);
    EXPECT_EQ(one->code, 0);
    EXPECT_EQ(one->leases.size(), 1u);
}

// An error ack never carries a trailer, and the runs a known-old duplicate reserved at append are released when its ack turns
// into an error (here NotLeader: demoted while the duplicate waits on its fsync).  The arena holds 21 duplicates' runs (3 units
// each); 21 of them parked and failed, then 21 more answered, only works when every failed one released its runs.
TEST(LeaseWriteHookWorkerRegression, LeaseAckRunsFreedOnErrorRegression) {
    RigTuning tuning;
    tuning.durabilityTier = 2;
    Leased l(1000, /*arenaEntries=*/64, tuning);
    VerdictRig& r = l.r;
    constexpr int kDups = 21;
    std::vector<std::vector<RawRec>> batches;
    std::vector<int32_t> seqs;
    for (int i = 0; i < kDups; ++i) {
        batches.push_back({grantTtl(1 + uint64_t(i), 3000)});
        seqs.push_back(l.seq);
        const auto a = l.produce(batches.back());
        ASSERT_TRUE(a) << i;
        ASSERT_EQ(a->code, 0);
    }
    for (int i = 0; i < 40; ++i) ASSERT_TRUE(l.produce({grantTtl(1000 + uint64_t(i), 3000)})) << "filler " << i;
    r.settle(150);
    const int64_t leo = r.leo();
    std::vector<int64_t> corrs;
    for (int i = 0; i < kDups; ++i)
        corrs.push_back(r.produceBytes(VerdictRig::rawBatch(batches[std::size_t(i)], 11, seqs[std::size_t(i)]), rune::protocol::client::kAcksWireView, 11, seqs[std::size_t(i)]));
    r.pass();      // every duplicate is appended-and-parked on its fsync
    r.demote();    // ... and fails NotLeader before it fires
    int notLeader = 0;
    for (const int64_t c : corrs) {
        const auto a = r.ack(c, l.ackPasses);
        ASSERT_TRUE(a);
        EXPECT_NE(a->code, 0);
        EXPECT_FALSE(a->hasLease) << "an error ack never carries a trailer";
        if (a->code == int16_t(rune::protocol::client::ErrorCode::NotLeader)) ++notLeader;
    }
    EXPECT_EQ(notLeader, kDups) << "vacuity guard: every duplicate was parked with its runs, then failed";
    r.promote();
    r.passes(60);
    ASSERT_TRUE(r.driveUntilJudged(-1, VerdictRig::kMaxPass * 50));   // Strict: the markers commit after their fsync
    r.passes(60);
    ASSERT_TRUE(r.leaseReady());
    for (int i = 0; i < kDups; ++i) {
        const auto a = l.produceNoSeq(batches[std::size_t(i)], seqs[std::size_t(i)]);
        ASSERT_TRUE(a) << i;
        EXPECT_EQ(a->code, 0) << i;
        EXPECT_TRUE(a->hasLease) << i;
        EXPECT_EQ(a->verdicts, (Verd{{0, 21, 0}})) << i;
    }
    EXPECT_EQ(r.leo(), leo);
}

// (F16) The lease rebuild epoch is monotonic per WorkerViewSet: a lease index erased and re-created takes a strictly greater epoch than
// the one before, so a runtime of the old index can never match the new one.  Mutation: back to a per-instance counter: red.
TEST(LeaseJudgeWorkerRegression, RebuildEpochMonotonicAcrossRecreateRegression) {
    Ready g;
    VerdictRig& r = g.r;
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    const uint32_t epoch0 = r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0);
    ASSERT_GT(epoch0, 0u);
    ASSERT_NE(r.writeState(), nullptr);
    const_cast<PartitionWriteState*>(r.writeState())->verdicts = nullptr;   // the write state must not keep the erased judge
    r.vs->dropVerdictSource(VerdictRig::kSrc, 0);   // the judge and its lease index are gone
    EXPECT_EQ(r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0), 0u);
    for (int i = 0; i < 400 && !(r.leaseReady() && r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0) != 0); ++i) r.pass();
    ASSERT_TRUE(r.leaseReady());
    EXPECT_GT(r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0), epoch0);
}
