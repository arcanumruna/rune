// Lease topics Task 30 (spec A.3, A.6): the rebirth fence state machine and carry emission of a TRANSIENT lease partition's runtime, over a
// real memory-backed PartitionReplica (the ring), the real judge (VerdictSource + the transient lease index) and the real LeaseRuntime.
// The lease clock is a plain integer the test advances; nothing waits on wall time and every drive loop has a hard iteration cap.
#include <gtest/gtest.h>

#include "AllocWindow.h"

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/node/VerdictSource.h>
#include <rune/node/lease/LeaseIndex.h>
#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/RejectedOffsetIndex.h>
#include <rune/view/ViewRow.h>

#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

using namespace rune::node;
using namespace rune::replication;
namespace cr  = rune::storage::control_record;
namespace rec = rune::protocol::record;
using P = rune::codec::PrimType;
using rune::node::lease::LeaseRuntime;
using rune::view::VerdictViewKind;

namespace {

constexpr int64_t  kMs      = 1'000'000;
constexpr int      kHardCap = 10'000;
constexpr uint64_t kMaxTtl  = 10'000;   // the topic's lease.max_ttl (ms)
constexpr uint64_t kL1 = 0x1111'2222'3333'4444ull, kL2 = 0x5555'6666'7777'8888ull;

rune::codec::Descriptor leaseDesc() {
    rune::codec::DescriptorBuilder d("inst", 1);
    d.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, /*isKey=*/true).addFixedStr("addr", 16);
    return d.build();
}
void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}
uint64_t getLe(const std::vector<uint8_t>& b, std::size_t at, int bytes = 8) {
    uint64_t x = 0;
    for (int i = 0; i < bytes; ++i) x |= uint64_t(b[at + std::size_t(i)]) << (8 * i);
    return x;
}
std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, uint64_t ttl, uint64_t remaining) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, 1, 8);
    putLe(b, 28, remaining, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, "addr", 4);
    return b;
}
std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

PartitionConfig tcfg() {
    PartitionConfig c;
    c.topicId = 5; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000'000'000LL;
    c.replicaLagMaxMs = 600'000; c.replicaLagMaxOffset = 1'000'000'000;
    c.pushOnAppend = false; c.maxFetchBytes = 1 << 20;
    return c;
}

struct Rec { int64_t offset; uint8_t attrs; std::string key; std::vector<uint8_t> value; };

/// A transient lease partition: the ring (replica), the judge over the transient lease index, and the runtime under test.
struct FRig {
    int64_t                       now      = 1'000'000'000;
    int64_t                       extendNs = 300 * kMs;
    rune::codec::Descriptor       evolving = leaseDesc();
    TopicWriteSpec                spec;
    PartitionWriteState           st;
    VerdictSource                 vs{5, 0};
    std::unique_ptr<ViewInstance> index;
    ConstraintSet                 set;
    rune::storage::InMemoryLog    mem;
    InMemoryTransport             transport;
    std::unique_ptr<PartitionReplica> replica;
    lease::LeaseParkTable         parks;
    lease::LeaseEmitWork          work{256 * 1024};
    std::unique_ptr<LeaseRuntime> rt;
    LeaseLineageNote              note{};

    explicit FRig(std::size_t ringBytes = 48 * 1024, bool transient = true) : mem(ringBytes, 100'000) {
        spec.topicId        = 5;
        spec.keyFieldId     = 4;
        spec.keyWidth       = 8;
        spec.lease          = rune::protocol::LeaseSpec{};
        spec.lease->maxKeys = 1000;
        spec.lease->maxTtlMs = kMaxTtl;
        spec.lease->expiryBatch = 64;
        spec.leaseTransient = transient;
        spec.installVersion(1, rune::codec::materializeAsOf(evolving, 1));
        spec.evolving = evolving;
        st.spec = &spec;
        st.leaseEvents.reserveOnce();
        rune::view::ViewTableKeys keys{8, 0, 0, ~uint64_t{0}, transient ? kLeaseTransientAuxBytes : kLeaseAuxBytes};
        index = std::make_unique<ViewInstance>(leaseIndexPlan(5, 4), leaseIndexRowLayout(evolving), evolving, false, 0, 4096, kLeaseIndexChunkBytes, keys);
        index->leaseTransient.transient = transient;
        vs.addVerdictView(index.get(), VerdictViewKind::LeaseIndex, 0, kLeaseIndexPlanFingerprint);
        set.id = 1;
        set.members[set.count++] = {0, VerdictViewKind::LeaseIndex, kLeaseIndexPlanFingerprint};
        replica = std::make_unique<PartitionReplica>(tcfg(), Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &mem, &transport);
        replica->setLeaseTransient(transient);
        // The verdict marker at offset 0 (what the worker's judge starts from), then the judge.
        std::vector<uint8_t> c(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(c, cr::MarkerHeader{0, 0, 0, 0}, {});
        EXPECT_GE(mem.appendNoSync(c.data(), c.size(), 0), 0);
        st.constraintSets.onChange(0, set);
        vs.bindState(&st);
        vs.bindLeader(nullptr, 1, 0);
        judge();
    }

    void judge() {
        (void)vs.driveJudge([&](int64_t from, int64_t) { return mem.readView(from, 1 << 20); }, replica->logEndOffset(), 0, st);
    }
    [[nodiscard]] LeaseIndexRef ref() {
        return LeaseIndexRef{&index->table, &index->layout, &index->evolvingSrc, &index->leaseCover, &index->leaseTransient};
    }
    [[nodiscard]] LeaseWriter writer() const { return latestLeaseWriter(spec); }
    [[nodiscard]] const LeaseTransientState& ts() const { return index->leaseTransient; }
    [[nodiscard]] int64_t leo() const { return replica->logEndOffset(); }

    /// A new runtime at the current lease time (the old one is dropped: a leader change).
    void makeRuntime(bool transient = true, uint32_t chunk = lease::kLeasePromotionChunk) {
        rt = std::make_unique<LeaseRuntime>(1, 1, 1000, now, 5, 0, extendNs, chunk, transient);
        if (chunk != 0) {
            const LeaseIndexRef r = ref();
            for (int i = 0; i < kHardCap && !rt->promotionDone(); ++i) (void)rt->promote(r, *spec.lease, now);
        }
    }
    /// The judge's pass, then the runtime folds the events (no runtime: they are dropped, as in the worker).
    void fold() {
        judge();
        if (rt) {
            const LeaseIndexRef r = ref();
            rt->onEvents(st.leaseEvents.buf, now, parks, *spec.lease, &r);
        }
        st.leaseEvents.buf.clear();
    }
    uint32_t fence() {
        const LeaseIndexRef r = ref();
        return rt->fenceStep(note, int32_t(replica->epoch()), r, work, writer(), now, kMaxTtl, *replica);
    }
    uint32_t carry(uint32_t maxRecords = 64) {
        const LeaseIndexRef r = ref();
        return rt->emitCarries(work, r, writer(), st.judgedThrough, now, 64 * 1024, 64 * 1024, maxRecords, *replica);
    }
    void pushFloor() {
        const LeaseIndexRef r = ref();
        replica->setLeaseFloor(leaseTransientFloor(*r.table, *r.transient));
    }
    void setNote(uint64_t lineage, uint32_t generation, int32_t epoch = 1) { note = LeaseLineageNote{epoch, lineage, generation}; }

    int64_t appendRaw(const std::vector<std::tuple<std::string, std::vector<uint8_t>, uint8_t>>& recs) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        for (const auto& [k, v, a] : recs) b.append(k, sv(v), 0, a);
        const auto batch = b.build();
        const int64_t base = replica->appendLeaseBatch(batch);
        EXPECT_GE(base, 0);
        return base;
    }
    /// Client grants of keys [first, first + n) in one batch (judged by the next fold).
    int64_t grants(uint64_t first, uint64_t n, uint64_t ttl = 5000) {
        std::vector<std::tuple<std::string, std::vector<uint8_t>, uint8_t>> recs;
        for (uint64_t k = first; k < first + n; ++k) recs.emplace_back(keyOf(k), leaseValue(k, 0, ttl, 0), uint8_t(1));
        return appendRaw(recs);
    }
    /// A fence record, appended by the test (what a previous leader left in the ring).
    int64_t fenceRecord(uint64_t lineage, uint64_t remainingMs) {
        std::vector<uint8_t> v(4u + writer().descriptor->blockLength, 0);
        const std::size_t len = encodeLeaseFenceValue(writer(), kMaxTtl, remainingMs, v.data(), v.size());
        EXPECT_NE(len, 0u);
        return appendRaw({{keyOf(lineage), v, uint8_t(rec::withLeaseOp(0, rec::LeaseOp::Checkpoint))}});
    }
    [[nodiscard]] uint32_t rid(uint64_t key) {
        uint32_t r = kLeaseAnchorNil;
        uint8_t k[8];
        for (int i = 0; i < 8; ++i) k[i] = uint8_t(key >> (8 * i));
        (void)index->table.find(rune::view::ViewKey::fromBytes({k, 8}), r);
        return r;
    }
    /// Checkpoint fillers of `key` (the row's current version and a fixed remaining): dead ring bytes behind the anchors, until the carry
    /// thresholds hold.
    void fillToCarry(uint64_t key) {
        judge();
        const uint32_t r = rid(key);
        ASSERT_NE(r, kLeaseAnchorNil);
        const int64_t ver = int64_t(getLeVersion(r));
        for (int i = 0; i < kHardCap; ++i) {
            if (shouldCarry()) return;
            appendRaw({{keyOf(key), leaseValue(key, ver, 5000, 1000), uint8_t(rec::withLeaseOp(0, rec::LeaseOp::Checkpoint))}});
        }
        FAIL() << "the carry threshold was never reached";
    }
    [[nodiscard]] bool shouldCarry() {
        const int64_t floor = leaseTransientFloor(index->table, ts());
        return floor != INT64_MAX && leaseShouldCarry(replica->transientBytesFrom(floor), ts().anchors.liveBytes, replica->transientCapacityBytes());
    }
    [[nodiscard]] int64_t getLeVersion(uint32_t r) { return leaseRowVersion(index->table.rowOf(r), index->layout); }

    /// Every data record from `from` on.
    [[nodiscard]] std::vector<Rec> recordsFrom(int64_t from) {
        std::vector<Rec> out;
        for (int64_t off = from; off < leo();) {
            const auto v = mem.readView(off, 1 << 20);
            if (v.empty()) break;
            for (std::size_t p = 0; p + 12 <= v.len;) {
                rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
                const std::size_t total = static_cast<std::size_t>(rd.totalLength());
                if (total < 12 || p + total > v.len) break;
                if (rd.baseOffset() >= from && (rd.attributes() & rune::protocol::batch::kAttrControl) == 0)
                    rd.forEachRecord([&](std::string_view k, std::string_view val, int64_t o, int64_t, uint8_t a) {
                        out.push_back({o, a, std::string(k), std::vector<uint8_t>(val.begin(), val.end())});
                    });
                p += total;
            }
            off = v.nextOffset;
        }
        return out;
    }
    [[nodiscard]] int64_t deadlineOf() const {
        const auto* f = std::get_if<LeaseRuntime::FenceFencing>(&rt->fenceState());
        return f == nullptr ? -1 : f->deadlineNs;
    }
};

constexpr uint8_t kFenceAttr = uint8_t(rec::LeaseOp::Checkpoint) << rec::kAttrLeaseOpShift;

// The fence record's fields.
struct FenceRec { uint64_t lineage; int64_t version; uint64_t ttl, remaining; uint8_t attrs; };
FenceRec parseFence(const Rec& r) {
    EXPECT_EQ(r.key.size(), 8u);
    uint64_t l = 0;
    for (int i = 0; i < 8 && i < int(r.key.size()); ++i) l |= uint64_t(uint8_t(r.key[std::size_t(i)])) << (8 * i);
    return {l, int64_t(getLe(r.value, 4)), getLe(r.value, 12), getLe(r.value, 28), r.attrs};
}

constexpr uint8_t kAwait = 0, kDeciding = 1, kFencing = 2, kOpen = 3;

}  // namespace

// ═══ The A.6 decision table ═══

TEST(LeaseFenceDecisionRegression, TableRowsRegression) {
    struct Row {
        const char* name;
        int64_t     priorLineage;     // 0: no fence record in the ring
        uint64_t    priorRemaining;
        uint32_t    generation;
        uint32_t    appended;         // records the first step appends
        uint8_t     codeAfterStep;
        uint64_t    recordRemaining;  // the appended record's lease_remaining_ms
        uint8_t     codeAfterFold;
        int64_t     deadlineMs;       // the Fencing deadline offset past ready + extend (-1: not fencing)
    };
    const int64_t kOther = int64_t(kL2);
    const Row rows[] = {
        {"open fence of L, gen 0",          int64_t(kL1), 0,    0, 0, kOpen,     0,       kOpen,    -1},
        {"open fence of L, gen 3",          int64_t(kL1), 0,    3, 0, kOpen,     0,       kOpen,    -1},
        {"L remaining 5000, gen 0",         int64_t(kL1), 5000, 0, 0, kFencing,  0,       kFencing, 5000},
        {"L remaining 5000, gen 4",         int64_t(kL1), 5000, 4, 0, kFencing,  0,       kFencing, 5000},
        {"none, gen 0",                     0,            0,    0, 1, kDeciding, 0,       kOpen,    -1},
        {"none, gen 1",                     0,            0,    1, 1, kDeciding, kMaxTtl, kFencing, int64_t(kMaxTtl)},
        {"none, gen 2",                     0,            0,    2, 1, kDeciding, kMaxTtl, kFencing, int64_t(kMaxTtl)},
        {"other lineage open, gen 0",       kOther,       0,    0, 1, kDeciding, 0,       kOpen,    -1},
        {"other lineage remaining 7000, gen 0", kOther,   7000, 0, 1, kDeciding, 0,       kOpen,    -1},
        {"other lineage open, gen 3",       kOther,       0,    3, 1, kDeciding, kMaxTtl, kFencing, int64_t(kMaxTtl)},
        {"other lineage remaining 7000, gen 3", kOther,   7000, 3, 1, kDeciding, kMaxTtl, kFencing, int64_t(kMaxTtl)},
    };
    for (const Row& row : rows) {
        SCOPED_TRACE(row.name);
        FRig r;
        if (row.priorLineage != 0) {
            r.fenceRecord(uint64_t(row.priorLineage), row.priorRemaining);
            r.fold();
            ASSERT_TRUE(r.ts().fence.present);
        }
        r.makeRuntime();
        const int64_t readyNs = r.now;
        r.setNote(kL1, row.generation);
        ASSERT_EQ(r.rt->fenceStateCode(), kAwait) << "a new transient runtime awaits its lineage";
        EXPECT_EQ(r.rt->grantHoldNs(r.now), kMs);
        const int64_t before = r.leo();
        EXPECT_EQ(r.fence(), row.appended);
        EXPECT_EQ(r.rt->fenceStateCode(), row.codeAfterStep);
        EXPECT_EQ(r.leo() - before, int64_t(row.appended));
        if (row.codeAfterStep == kFencing) EXPECT_EQ(r.deadlineOf(), readyNs + r.extendNs + int64_t(row.deadlineMs) * kMs);
        if (row.codeAfterStep == kOpen) EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
        else EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
        if (row.appended != 0) {
            const auto recs = r.recordsFrom(before);
            ASSERT_EQ(recs.size(), 1u);
            const FenceRec f = parseFence(recs[0]);
            EXPECT_EQ(f.lineage, kL1) << "the key is the lineage token, 8 bytes little-endian";
            EXPECT_EQ(f.version, rec::kLeaseFenceVersion);
            EXPECT_EQ(f.remaining, row.recordRemaining);
            EXPECT_EQ(f.ttl, kMaxTtl);
            EXPECT_EQ(f.attrs, kFenceAttr) << "a Checkpoint with intent 0";
            EXPECT_EQ(r.rt->fenceStateCode(), kDeciding);
            EXPECT_EQ(r.rt->grantHoldNs(r.now), kMs);
            EXPECT_EQ(r.fence(), 0u) << "the record is not folded yet: nothing more is appended";
            EXPECT_EQ(r.leo() - before, 1);
            r.fold();
            EXPECT_EQ(r.fence(), 0u);
            EXPECT_EQ(r.rt->fenceStateCode(), row.codeAfterFold);
            if (row.codeAfterFold == kFencing) EXPECT_EQ(r.deadlineOf(), readyNs + r.extendNs + int64_t(row.deadlineMs) * kMs);
            if (row.codeAfterFold == kOpen) EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
            else EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
        }
        EXPECT_EQ(r.rt->stats().fenceAppended, uint64_t(row.appended));
    }
}

TEST(LeaseFenceDecisionRegression, NeverEarlyRegression) {
    FRig r;
    r.fenceRecord(kL1, 5000);
    r.fold();
    r.makeRuntime();
    r.setNote(kL1, 1);
    ASSERT_EQ(r.fence(), 0u);
    ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
    const int64_t deadline = r.deadlineOf();
    ASSERT_EQ(deadline, r.now + r.extendNs + 5000 * kMs);
    // The hold: ceil to whole ms, clamped to [1 ms, 1 s].
    r.now = deadline - 5 * 1000 * kMs;
    EXPECT_EQ(r.rt->grantHoldNs(r.now), 1000 * kMs);
    r.now = deadline - 250'500'000;
    EXPECT_EQ(r.rt->grantHoldNs(r.now), 251 * kMs);
    r.now = deadline - 1;
    EXPECT_EQ(r.rt->grantHoldNs(r.now), kMs);
    const int64_t before = r.leo();
    EXPECT_EQ(r.fence(), 0u) << "one nanosecond before the deadline nothing is appended";
    EXPECT_EQ(r.leo(), before);
    EXPECT_EQ(r.rt->fenceStateCode(), kFencing);
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
    // At the deadline the open record is appended in the same call; grants wait until it folds.
    r.now = deadline;
    EXPECT_EQ(r.fence(), 1u);
    EXPECT_EQ(r.rt->fenceStateCode(), kDeciding);
    const auto recs = r.recordsFrom(before);
    ASSERT_EQ(recs.size(), 1u);
    const FenceRec f = parseFence(recs[0]);
    EXPECT_EQ(f.lineage, kL1);
    EXPECT_EQ(f.remaining, 0u) << "the open record";
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0) << "still held until the open record folds";
    r.fold();
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
    EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
    EXPECT_EQ(r.leo() - before, 1);
}

TEST(LeaseFenceDecisionRegression, AwaitRegression) {
    FRig r;
    r.makeRuntime();
    const int64_t before = r.leo();
    // No note at all.
    EXPECT_EQ(r.note.leaderEpoch, -1);
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kAwait);
    EXPECT_EQ(r.rt->grantHoldNs(r.now), kMs);
    // A note of another leadership (epoch).
    r.setNote(kL1, 2, /*epoch=*/2);
    ASSERT_EQ(r.replica->epoch(), 1);
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kAwait);
    EXPECT_EQ(r.rt->grantHoldNs(r.now), kMs);
    EXPECT_EQ(r.leo(), before) << "nothing is appended while the lineage is unknown";
    EXPECT_TRUE(r.rt->hasPendingWork(r.now)) << "the idle skip must not hide an awaiting fence";
    // The matching note.
    r.setNote(kL1, 0);
    EXPECT_EQ(r.fence(), 1u);
    EXPECT_NE(r.rt->fenceStateCode(), kAwait);
}

TEST(LeaseFenceDecisionRegression, RefusedAppendRetriedRegression) {
    FRig r;
    r.makeRuntime();
    r.setNote(kL1, 0);
    const int64_t before = r.leo();
    r.replica->setAppendRefusedForTest(true);
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kDeciding);
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
    EXPECT_EQ(r.rt->stats().fenceAppendDeferred, 1u);
    EXPECT_EQ(r.leo(), before);
    EXPECT_TRUE(r.rt->hasPendingWork(r.now));
    r.replica->setAppendRefusedForTest(false);
    EXPECT_EQ(r.fence(), 1u) << "the next pass appends";
    EXPECT_EQ(r.leo() - before, 1);
    r.fold();
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
}

TEST(LeaseFenceDecisionRegression, LeaderChangeEachStateRegression) {
    // Deciding, record NOT folded (the old leader's append was lost or never replayed): a gen >= 1 partition fences again in full.
    {
        SCOPED_TRACE("Deciding, unfolded, gen 2");
        FRig r;
        r.makeRuntime();
        r.setNote(kL1, 2);
        ASSERT_EQ(r.fence(), 1u);
        const int64_t oldThen = std::get<LeaseRuntime::FenceDeciding>(r.rt->fenceState()).thenDeadlineNs;
        // The record is not in the new leader's index (never folded): destroy the runtime, a later ready time.
        r.now += 3000 * kMs;
        r.index->leaseTransient.fence = LeaseFenceState{};   // the new leader's replay did not find the record
        r.makeRuntime();
        const int64_t before = r.leo();
        EXPECT_EQ(r.fence(), 1u);
        const auto recs = r.recordsFrom(before);
        ASSERT_EQ(recs.size(), 1u);
        EXPECT_EQ(parseFence(recs[0]).remaining, kMaxTtl) << "fenced again in full";
        const int64_t newThen = std::get<LeaseRuntime::FenceDeciding>(r.rt->fenceState()).thenDeadlineNs;
        EXPECT_GE(newThen, oldThen);
        EXPECT_EQ(newThen, r.now + r.extendNs + int64_t(kMaxTtl) * kMs);
    }
    // Deciding, record folded into the index but the old state machine had not moved: the new runtime decides from the table.
    for (const uint32_t gen : {0u, 2u}) {
        SCOPED_TRACE("Deciding, folded, gen " + std::to_string(gen));
        FRig r;
        r.makeRuntime();
        r.setNote(kL1, gen);
        ASSERT_EQ(r.fence(), 1u);
        const int64_t oldThen = std::get<LeaseRuntime::FenceDeciding>(r.rt->fenceState()).thenDeadlineNs;
        r.fold();
        r.now += 3000 * kMs;
        r.makeRuntime();
        const int64_t before = r.leo();
        EXPECT_EQ(r.fence(), 0u) << "the folded record decides: nothing is appended";
        EXPECT_EQ(r.leo(), before);
        if (gen == 0) {
            EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
        } else {
            ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
            EXPECT_GE(r.deadlineOf(), oldThen) << "never earlier than the old leader's deadline";
            EXPECT_EQ(r.deadlineOf(), r.now + r.extendNs + int64_t(kMaxTtl) * kMs);
        }
    }
    // Fencing.
    {
        SCOPED_TRACE("Fencing");
        FRig r;
        r.makeRuntime();
        r.setNote(kL1, 2);
        ASSERT_EQ(r.fence(), 1u);
        r.fold();
        ASSERT_EQ(r.fence(), 0u);
        ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
        const int64_t oldDeadline = r.deadlineOf();
        r.now += 4000 * kMs;
        r.makeRuntime();
        const int64_t before = r.leo();
        EXPECT_EQ(r.fence(), 0u);
        EXPECT_EQ(r.leo(), before);
        ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
        EXPECT_GE(r.deadlineOf(), oldDeadline);
    }
    // Open.
    {
        SCOPED_TRACE("Open");
        FRig r;
        r.makeRuntime();
        r.setNote(kL1, 0);
        ASSERT_EQ(r.fence(), 1u);
        r.fold();
        ASSERT_EQ(r.fence(), 0u);
        ASSERT_EQ(r.rt->fenceStateCode(), kOpen);
        r.now += 4000 * kMs;
        r.makeRuntime();
        const int64_t before = r.leo();
        EXPECT_EQ(r.fence(), 0u);
        EXPECT_EQ(r.leo(), before);
        EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
        EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
    }
}

TEST(LeaseFenceDecisionRegression, NewLineageRedecidesRegression) {
    FRig r;
    r.makeRuntime();
    r.setNote(kL1, 0);
    ASSERT_EQ(r.fence(), 1u);
    r.fold();
    ASSERT_EQ(r.fence(), 0u);
    ASSERT_EQ(r.rt->fenceStateCode(), kOpen);
    // A note of another leadership does not move an Open fence.
    r.setNote(kL2, 1, /*epoch=*/7);
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
    // The same leadership, a new lineage: decided again in the same call.
    r.setNote(kL2, 1);
    EXPECT_TRUE(r.rt->fenceNoteChanged(r.note, 1)) << "the idle skip must see it";
    const int64_t before = r.leo();
    EXPECT_EQ(r.fence(), 1u);
    const auto recs = r.recordsFrom(before);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(parseFence(recs[0]).lineage, kL2);
    EXPECT_EQ(parseFence(recs[0]).remaining, kMaxTtl);
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
    r.fold();
    EXPECT_EQ(r.fence(), 0u);
    ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
    EXPECT_EQ(r.deadlineOf(), r.rt->readyNs() + r.extendNs + int64_t(kMaxTtl) * kMs);
    EXPECT_GT(r.rt->grantHoldNs(r.now), 0);
}

TEST(LeaseFenceDecisionRegression, PersistedNeverFencesRegression) {
    FRig r(48 * 1024, /*transient=*/false);
    r.makeRuntime(/*transient=*/false);
    EXPECT_EQ(r.rt->fenceStateCode(), kOpen) << "open from construction";
    EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
    r.grants(1, 4);
    r.fold();
    r.setNote(kL1, 5);
    const int64_t before = r.leo();
    EXPECT_EQ(r.fence(), 0u);
    EXPECT_EQ(r.rt->fenceStateCode(), kOpen);
    EXPECT_EQ(r.rt->grantHoldNs(r.now), 0);
    EXPECT_EQ(r.carry(), 0u);
    EXPECT_EQ(r.leo(), before) << "a persisted runtime appends neither a fence nor a carry";
    EXPECT_TRUE(r.rt->promotionDone());
    EXPECT_FALSE(r.rt->hasPendingWork(r.now)) << "a persisted runtime is idle with nothing due";
    EXPECT_EQ(r.rt->stats().fenceAppended, 0u);
    EXPECT_EQ(r.rt->stats().carried, 0u);
    EXPECT_FALSE(r.rt->fenceNoteChanged(r.note, 1));
}

// ═══ Carry ═══

TEST(LeaseCarryEmitRegression, ThresholdsRegression) {
    constexpr std::size_t cap = 1000;
    EXPECT_FALSE(leaseShouldCarry(cap / 2, 0, cap)) << "used = cap / 2";
    EXPECT_TRUE(leaseShouldCarry(cap / 2 + 1, 0, cap));
    EXPECT_TRUE(leaseShouldCarry(cap / 2 + 1, cap / 2 + 1 - cap / 4, cap)) << "used - live = cap / 4";
    EXPECT_FALSE(leaseShouldCarry(cap / 2 + 1, cap / 2 + 1 - cap / 4 + 1, cap)) << "used - live = cap / 4 - 1";
    EXPECT_TRUE(leaseShouldCarry(900, 900 - cap / 4, cap));
    EXPECT_FALSE(leaseShouldCarry(900, 900 - cap / 4 + 1, cap));
    EXPECT_FALSE(leaseShouldCarry(900, 0, 0)) << "cap 0";
    EXPECT_FALSE(leaseShouldCarry(900, 901, cap)) << "live > used";
    EXPECT_FALSE(leaseShouldCarry(0, 0, cap));
}

// A transient lease leader keeps the latest constraint-set change in its ring: the ring floor is the lease index's, held down to the
// change.  Without it the change is evicted like any record and the next leader never judges.
// Mutation: leaseLeaderFloor returns anchorsFloor unchanged -> the first expectation is red.
TEST(LeaseCarryEmitRegression, ConstraintChangeHeldRegression) {
    EXPECT_EQ(leaseLeaderFloor(500, 40), 40) << "the change is older than every anchor: it is held";
    EXPECT_EQ(leaseLeaderFloor(INT64_MAX, 40), 40) << "nothing anchored: the change alone is the floor";
    EXPECT_EQ(leaseLeaderFloor(30, 40), 30) << "an older anchor already holds the ring below the change";
    EXPECT_EQ(leaseLeaderFloor(40, 40), 40);
    EXPECT_EQ(leaseLeaderFloor(500, -1), 500) << "no change logged: the index floor alone";
    EXPECT_EQ(leaseLeaderFloor(INT64_MAX, -1), INT64_MAX) << "no change, nothing anchored: no floor";
    EXPECT_EQ(leaseLeaderFloor(500, 0), 0) << "a change at offset 0 is a real offset";
}

TEST(LeaseCarryEmitRegression, CarriesHeadKeysRegression) {
    FRig r;
    r.makeRuntime();
    r.grants(1, 8);
    r.fold();
    ASSERT_EQ(r.ts().anchors.count, 8u);
    r.fillToCarry(8);
    r.fold();
    const int64_t floorBefore = leaseTransientFloor(r.index->table, r.ts());
    const int64_t headBefore  = leaseAnchorHead(r.index->table, r.ts().anchors);
    EXPECT_EQ(floorBefore, headBefore);
    const int64_t before = r.leo();
    ASSERT_EQ(r.carry(3), 3u);
    const auto recs = r.recordsFrom(before);
    ASSERT_EQ(recs.size(), 3u);
    for (std::size_t i = 0; i < 3; ++i) {
        SCOPED_TRACE(i);
        const uint64_t key = i + 1;
        EXPECT_EQ(recs[i].key, keyOf(key));
        EXPECT_EQ(recs[i].attrs, rec::withLeaseOp(0, rec::LeaseOp::Put)) << "a Carry: Put, intent 0";
        ASSERT_EQ(recs[i].value.size(), 60u);
        // The row verbatim: version (the grant's offset), ttl, holder, remaining, the user columns.
        EXPECT_EQ(int64_t(getLe(recs[i].value, 4)), r.getLeVersion(r.rid(key)));
        EXPECT_EQ(int64_t(getLe(recs[i].value, 4)), 1 + int64_t(i)) << "the grants are the offsets 1..8";
        EXPECT_EQ(getLe(recs[i].value, 12), 5000u);
        EXPECT_EQ(getLe(recs[i].value, 20), 1u);
        EXPECT_EQ(getLe(recs[i].value, 28), 0u);
        EXPECT_EQ(getLe(recs[i].value, 36), key);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(recs[i].value.data()) + 44, 4), "addr");
        EXPECT_EQ(leaseLastAppended(r.index->table, r.rid(key)), recs[i].offset) << "the gate closes until the Carry is judged";
    }
    EXPECT_EQ(r.rt->stats().carried, 3u);
    EXPECT_EQ(r.rt->stats().carryBatches, 1u);
    // Judged: the anchors move to the carries, the head and the floor rise.
    r.fold();
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.rid(1)), recs[0].offset);
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.rid(3)), recs[2].offset);
    EXPECT_EQ(r.ts().anchors.count, 8u);
    EXPECT_EQ(leaseAnchorHead(r.index->table, r.ts().anchors), leaseAnchorOf(r.index->table, r.rid(4)));
    EXPECT_GT(leaseTransientFloor(r.index->table, r.ts()), floorBefore);
    EXPECT_EQ(r.getLeVersion(r.rid(1)), 1) << "a carry does not change the row's version";
    EXPECT_EQ(r.st.rejected.find(recs[0].offset), nullptr) << "the judge accepts a Carry of the live version";
    // The floor reaches the ring: the replica evicts only below it.
    r.pushFloor();
    EXPECT_EQ(r.replica->leaseFloor(), leaseTransientFloor(r.index->table, r.ts()));
}

TEST(LeaseCarryEmitRegression, GateClosedSkipsRegression) {
    {
        SCOPED_TRACE("gate closed");
        FRig r;
        r.makeRuntime();
        r.grants(1, 8);
        r.fold();
        r.fillToCarry(8);
        r.fold();
        // An update of key 1 is appended but not judged yet: its lastAppended is not below judgedThrough.
        setLeaseLastAppended(r.index->table, r.rid(1), r.st.judgedThrough);
        const int64_t before = r.leo();
        EXPECT_EQ(r.carry(3), 2u);
        EXPECT_EQ(r.rt->stats().carryDeferred, 1u);
        const auto recs = r.recordsFrom(before);
        ASSERT_EQ(recs.size(), 2u);
        EXPECT_EQ(recs[0].key, keyOf(2)) << "the head key is skipped, the next ones are carried";
        EXPECT_EQ(recs[1].key, keyOf(3));
        EXPECT_EQ(leaseLastAppended(r.index->table, r.rid(1)), r.st.judgedThrough) << "the skipped key is untouched";
    }
    {
        SCOPED_TRACE("entry not Armed (Free before promotion)");
        FRig r;
        r.makeRuntime(true, /*chunk=*/0);   // no promotion pass
        r.grants(1, 8);
        r.fold();                           // a Granted fold arms its entry ...
        r.fillToCarry(8);
        // ... so build a runtime that has not seen the grants: its entries are not covered / Free.
        r.makeRuntime(true, /*chunk=*/0);
        r.fold();
        const int64_t before = r.leo();
        EXPECT_EQ(r.carry(3), 0u);
        EXPECT_EQ(r.rt->stats().carryDeferred, 3u);
        EXPECT_EQ(r.leo(), before);
        // After the promotion pass the entries are Armed and the head keys carry.
        const LeaseIndexRef ref = r.ref();
        r.rt->setPromotionChunkForTest(lease::kLeasePromotionChunk);
        for (int i = 0; i < kHardCap && !r.rt->promotionDone(); ++i) (void)r.rt->promote(ref, *r.spec.lease, r.now);
        EXPECT_EQ(r.carry(3), 3u);
    }
}

TEST(LeaseCarryEmitRegression, NoDoubleCarryRegression) {
    FRig r;
    r.makeRuntime();
    r.grants(1, 8);
    r.fold();
    r.fillToCarry(8);
    r.fold();
    ASSERT_EQ(r.carry(2), 2u);
    const int64_t after1 = r.leo();
    EXPECT_EQ(r.carry(2), 0u) << "the Carries are not judged yet: their keys are not carried again";
    EXPECT_EQ(r.leo(), after1);
    EXPECT_EQ(r.rt->stats().carryDeferred, 2u);
    EXPECT_EQ(r.rt->stats().carried, 2u);
    r.fold();
    const int64_t before = r.leo();
    EXPECT_EQ(r.carry(2), 2u);
    const auto recs = r.recordsFrom(before);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].key, keyOf(3));
    EXPECT_EQ(recs[1].key, keyOf(4));
}

namespace {
/// Fence first, then 8 grants, then fillers: the fence is the oldest anchor.  `open`: the fence is open (gen 0); else fencing (gen 2).
void buildFenceOldest(FRig& r, bool open) {
    r.makeRuntime();
    r.setNote(kL1, open ? 0 : 2);
    ASSERT_EQ(r.fence(), 1u);
    r.fold();
    ASSERT_EQ(r.fence(), 0u);
    r.grants(1, 8);
    r.fold();
    if (open) {
        ASSERT_EQ(r.rt->fenceStateCode(), kOpen);
    } else {
        ASSERT_EQ(r.rt->fenceStateCode(), kFencing);
    }
    ASSERT_TRUE(r.ts().fence.present);
    r.fillToCarry(8);
    r.fold();
    ASSERT_LT(r.ts().fence.offset, leaseAnchorHead(r.index->table, r.ts().anchors));
    ASSERT_EQ(leaseTransientFloor(r.index->table, r.ts()), r.ts().fence.offset) << "the fence is the floor";
}
}  // namespace

TEST(LeaseCarryEmitRegression, FenceCarriedWhenOldestRegression) {
    for (const bool open : {false, true}) {
        SCOPED_TRACE(open ? "open" : "fencing");
        FRig r;
        buildFenceOldest(r, open);
        const int64_t deadline = open ? 0 : r.deadlineOf();
        r.now += 2000 * kMs;
        const int64_t before = r.leo();
        ASSERT_EQ(r.carry(3), 4u) << "the fence record, then three rows";
        const auto recs = r.recordsFrom(before);
        ASSERT_EQ(recs.size(), 4u);
        const FenceRec f = parseFence(recs[0]);
        EXPECT_EQ(f.lineage, kL1);
        EXPECT_EQ(f.version, rec::kLeaseFenceVersion);
        EXPECT_EQ(f.ttl, kMaxTtl);
        EXPECT_EQ(f.attrs, kFenceAttr);
        if (open) {
            EXPECT_EQ(f.remaining, 0u) << "an open fence is carried open";
        } else {
            const int64_t diff = deadline - r.now;
            EXPECT_EQ(f.remaining, uint64_t(diff / kMs + (diff % kMs != 0 ? 1 : 0))) << "ceil_ms(deadline - now)";
            EXPECT_EQ(f.remaining, 8300u);
        }
        EXPECT_EQ(recs[1].key, keyOf(1));
        EXPECT_EQ(r.rt->fenceCarryFromForTest(), recs[0].offset);
        // Not appended again until it folds (the carried rows are gated too: nothing is appended at all).
        const int64_t after = r.leo();
        EXPECT_EQ(r.carry(3), 0u);
        EXPECT_EQ(r.leo(), after);
        r.fold();
        EXPECT_EQ(r.ts().fence.offset, recs[0].offset);
        EXPECT_EQ(r.ts().fence.remainingMs, f.remaining);
        (void)r.carry(3);
        EXPECT_EQ(r.rt->fenceCarryFromForTest(), -1) << "folded: no longer in flight";
        // The carried fence is no longer the oldest anchor.
        EXPECT_GT(r.ts().fence.offset, leaseAnchorHead(r.index->table, r.ts().anchors));
        // The state machine is untouched by the carry.
        EXPECT_EQ(r.rt->fenceStateCode(), open ? kOpen : kFencing);
        EXPECT_EQ(r.rt->stats().fenceAppended, 2u) << "the decision's record and the carry";
    }
}

TEST(LeaseCarryAllocRegression, ZeroAllocRegression) {
    FRig r;
    r.makeRuntime();
    r.setNote(kL1, 2);
    r.grants(1, 8);
    r.fold();
    r.fillToCarry(8);
    r.fold();
    // Warm-up: one full decide / append / fold / carry cycle (the replica's scratch and the batch buffers grow once).
    ASSERT_EQ(r.fence(), 1u);
    r.fold();
    (void)r.fence();
    (void)r.carry(2);
    r.fold();
    (void)r.fence();
    (void)r.carry(2);
    r.fold();
    uint64_t allocs = 0;
    uint32_t carried = 0;
    for (int i = 0; i < 6; ++i) {
        {
            rune::testing::AllocWindow w;
            (void)r.fence();
            carried += r.carry(2);
            allocs += w.count();
        }
        r.fold();   // the judge and the event fold are outside the window: they are not part of Task 30
    }
    EXPECT_GT(carried, 0u) << "the passes did real carry work";
    EXPECT_EQ(allocs, 0u);
}
