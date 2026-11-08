// Lease topics Task 6: the lease index as a verdict member (unit level: no worker, no replica).
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/node/VerdictSource.h>
#include <rune/node/lease/LeaseIndex.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/RejectedOffsetIndex.h>
#include <rune/transaction/AbortedTxnIndex.h>
#include <rune/view/ViewRow.h>

using namespace rune::node;
namespace cr  = rune::storage::control_record;
namespace rec = rune::protocol::record;
using P = rune::codec::PrimType;
using rune::view::VerdictViewKind;

namespace {

constexpr uint16_t kKeyExists = 56, kNotFound = 62, kLimit = 63, kInvalid = 39;
constexpr uint16_t kStale = rune::view::kVerdictStaleBroker;

// Row slots of the test descriptor: 0..3 system, 4 id (key), 5 addr, 6 port, then the two offset columns.
constexpr uint16_t kIdSlot = 4, kAddrSlot = 5, kPortSlot = 6, kGrantedSlot = 7, kUpdatedSlot = 8;

// The evolving descriptor: lease system columns, id uint64 (key), addr string(16), port uint32 (added in `portSince`).
rune::codec::Descriptor evolvingDesc(uint16_t latest = 1, uint16_t portSince = 1) {
    rune::codec::DescriptorBuilder d("inst", latest);
    d.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, /*isKey=*/true).addFixedStr("addr", 16).add("port", P::UInt32, portSince);
    return d.build();
}

void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}

// One lease record value: [u16 blockLength][u16 schema version][block].
struct LV {
    int64_t     ver = 0;
    uint64_t    ttl = 1000, holder = 0, rem = 0, id = 0;
    std::string addr;
    uint32_t    port = 0;
    uint16_t    schema = 1;
    bool        withPort = true;   // a v1 writer of a topic whose port arrived in v2 has no port
};
std::vector<uint8_t> valueOf(const LV& v) {
    const uint16_t block = v.withPort ? 60 : 56;
    std::vector<uint8_t> b(4u + block, 0);
    putLe(b, 0, block, 2);
    putLe(b, 2, v.schema, 2);
    putLe(b, 4, uint64_t(v.ver), 8);
    putLe(b, 12, v.ttl, 8);
    putLe(b, 20, v.holder, 8);
    putLe(b, 28, v.rem, 8);
    putLe(b, 36, v.id, 8);
    std::memcpy(b.data() + 44, v.addr.data(), std::min<std::size_t>(v.addr.size(), 16));
    if (v.withPort) putLe(b, 60, v.port, 4);
    return b;
}

constexpr uint8_t kGrantA   = 1;                                                              // Put | insert
constexpr uint8_t kUpdateA  = 2;                                                              // Put | existing
constexpr uint8_t kRevokeA  = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);   // Revoke | existing
constexpr uint8_t kExpireA  = uint8_t(rec::LeaseOp::Expire) << rec::kAttrLeaseOpShift;         // broker: intent 0
constexpr uint8_t kCheckptA = uint8_t(rec::LeaseOp::Checkpoint) << rec::kAttrLeaseOpShift;

struct LRec {
    uint64_t             id;
    uint8_t              attrs;
    std::vector<uint8_t> value;
};
LRec grant(uint64_t id, LV v = {}) { v.id = id; return {id, kGrantA, valueOf(v)}; }
LRec update(uint64_t id, int64_t ver, LV v = {}) { v.id = id; v.ver = ver; return {id, kUpdateA, valueOf(v)}; }
LRec revoke(uint64_t id, int64_t ver) { LV v; v.id = id; v.ver = ver; return {id, kRevokeA, valueOf(v)}; }
LRec expire(uint64_t id, int64_t ver) { LV v; v.id = id; v.ver = ver; return {id, kExpireA, valueOf(v)}; }
LRec checkpoint(uint64_t id, int64_t ver, uint64_t rem) { LV v; v.id = id; v.ver = ver; v.rem = rem; return {id, kCheckptA, valueOf(v)}; }

std::string_view sv(const std::vector<uint8_t>& b) { return {reinterpret_cast<const char*>(b.data()), b.size()}; }

struct Log {
    std::vector<uint8_t> bytes;
    struct Span { std::size_t pos; int64_t base, last; };
    std::vector<Span>    batches;
    int64_t              next = 0;

    int64_t data(const std::vector<LRec>& recs, int64_t abortedPid = -1) {
        const int64_t first = next;
        rune::protocol::RecordBatchBuilder b(next, 0);
        if (abortedPid >= 0) {
            b.setTransactional(true);
            b.setProducerId(abortedPid);
        }
        for (const LRec& r : recs) {
            std::vector<uint8_t> k(8);
            putLe(k, 0, r.id, 8);
            b.append(sv(k), sv(r.value), 0, r.attrs);
        }
        push(b.build(), int64_t(recs.size()));
        return first;
    }
    void control() {
        std::vector<uint8_t> c(cr::markerBatchBytes(0));
        (void)cr::buildVerdictMarker(c, cr::MarkerHeader{0, 0, 0, 0}, {});
        for (int i = 0; i < 8; ++i) c[std::size_t(i)] = uint8_t(uint64_t(next) >> (8 * i));
        push(std::move(c), 1);
    }
    void push(std::vector<uint8_t> batch, int64_t count) {
        batches.push_back({bytes.size(), next, next + count - 1});
        bytes.insert(bytes.end(), batch.begin(), batch.end());
        next += count;
    }
    rune::storage::LogReadView read(int64_t from) const {
        for (const Span& s : batches)
            if (s.last >= from) return {bytes.data() + s.pos, bytes.size() - s.pos, s.base, next};
        return {};
    }
};

struct Rig {
    rune::codec::Descriptor       evolving;
    TopicWriteSpec                spec;
    Log                           log;
    PartitionWriteState           st;
    VerdictSource                 vs{5, 0};
    std::unique_ptr<ViewInstance> index;
    ConstraintSet                 set;

    explicit Rig(uint32_t maxKeys = 100, rune::codec::Descriptor ev = evolvingDesc()) : evolving(std::move(ev)) {
        spec.topicId    = 5;
        spec.keyFieldId = 4;
        spec.keyWidth   = 8;
        spec.lease      = rune::protocol::LeaseSpec{};
        spec.lease->maxKeys = maxKeys;
        st.spec = &spec;
        st.leaseEvents.reserveOnce();
        index = std::make_unique<ViewInstance>(leaseIndexPlan(5, 4), leaseIndexRowLayout(evolving), evolving, false, 0, 4096, kLeaseIndexChunkBytes);
        vs.addVerdictView(index.get(), VerdictViewKind::LeaseIndex, 0, kLeaseIndexPlanFingerprint);
        set.id = 1;
        set.members[set.count++] = {0, VerdictViewKind::LeaseIndex, kLeaseIndexPlanFingerprint};
    }
    void start() {
        st.constraintSets.onChange(0, set);
        log.control();
        vs.bindState(&st);
        vs.bindLeader(nullptr, 1, 0);
        drive();
    }
    int drive() {
        return vs.driveJudge([&](int64_t from, int64_t) { return log.read(from); }, log.next, 0, st);
    }
    // Appends one batch, drives, and returns the first offset.
    int64_t append(const std::vector<LRec>& recs) {
        const int64_t first = log.data(recs);
        drive();
        return first;
    }
    // One record in its own batch; returns its verdict code (0 = accepted).
    uint16_t judgeOne(const LRec& r, int64_t* offset = nullptr) {
        const int64_t o = append({r});
        if (offset != nullptr) *offset = o;
        const auto* e = st.rejected.find(o);
        return e == nullptr ? 0 : e->code;
    }
    [[nodiscard]] const uint8_t* row(uint64_t id) const { return index->table.get(id); }
    [[nodiscard]] uint64_t col(uint64_t id, uint16_t slot) const {
        const uint8_t* r = row(id);
        uint64_t v = 0;
        if (r != nullptr) std::memcpy(&v, r + index->layout.slotOffset[slot], 8);
        return v;
    }
    [[nodiscard]] uint32_t col32(uint64_t id, uint16_t slot) const {
        uint32_t v = 0;
        if (const uint8_t* r = row(id)) std::memcpy(&v, r + index->layout.slotOffset[slot], 4);
        return v;
    }
    [[nodiscard]] std::string addr(uint64_t id) const {
        const uint8_t* r = row(id);
        if (r == nullptr) return "<absent>";
        std::string s(reinterpret_cast<const char*>(r + index->layout.slotOffset[kAddrSlot]), 16);
        return s.substr(0, s.find('\0'));
    }
    [[nodiscard]] bool isNull(uint64_t id, uint16_t slot) const { return rune::view::isNull(row(id), index->layout, slot); }
};

}  // namespace

// ═══ judgeLease: the pure table ═══

TEST(LeaseJudgeTableRegression, JudgeLeaseTableRegression) {
    struct Row { const char* name; uint8_t attrs; uint16_t absent, equal, different; };
    const auto at = [](rec::LeaseOp op, uint8_t intent) { return rec::withLeaseOp(intent, op); };
    const Row rows[] = {
        {"grant",            at(rec::LeaseOp::Put, 1),        0,      kKeyExists, kKeyExists},
        {"update",           at(rec::LeaseOp::Put, 2),        kNotFound, 0,       kNotFound},
        {"revoke",           at(rec::LeaseOp::Revoke, 2),     kNotFound, 0,       kNotFound},
        {"expire",           at(rec::LeaseOp::Expire, 0),     kStale, 0,          kStale},
        {"checkpoint",       at(rec::LeaseOp::Checkpoint, 0), kStale, 0,          kStale},
        {"upsert",           at(rec::LeaseOp::Put, 0),        kInvalid, kInvalid, kInvalid},
        {"putIntent3",       at(rec::LeaseOp::Put, 3),        kInvalid, kInvalid, kInvalid},
        {"revokeInsert",     at(rec::LeaseOp::Revoke, 1),     kInvalid, kInvalid, kInvalid},
        {"revokeNoIntent",   at(rec::LeaseOp::Revoke, 0),     kInvalid, kInvalid, kInvalid},
        {"expireInsert",     at(rec::LeaseOp::Expire, 1),     kInvalid, kInvalid, kInvalid},
        {"expireExisting",   at(rec::LeaseOp::Expire, 2),     kInvalid, kInvalid, kInvalid},
        {"checkpointInsert", at(rec::LeaseOp::Checkpoint, 1), kInvalid, kInvalid, kInvalid},
        {"checkpointExist",  at(rec::LeaseOp::Checkpoint, 2), kInvalid, kInvalid, kInvalid},
    };
    for (const Row& r : rows) {
        SCOPED_TRACE(r.name);
        EXPECT_EQ(judgeLease({false, 0}, r.attrs, 7, 0, 10), r.absent);
        EXPECT_EQ(judgeLease({true, 7}, r.attrs, 7, 1, 10), r.equal);
        EXPECT_EQ(judgeLease({true, 7}, r.attrs, 8, 1, 10), r.different);
    }
    // max_keys: the grant is judged against the live count; every other record ignores it.
    const uint8_t g = at(rec::LeaseOp::Put, 1);
    EXPECT_EQ(judgeLease({false, 0}, g, 0, 9, 10), 0);
    EXPECT_EQ(judgeLease({false, 0}, g, 0, 10, 10), kLimit);
    EXPECT_EQ(judgeLease({false, 0}, g, 0, 11, 10), kLimit);
    EXPECT_EQ(judgeLease({false, 0}, g, 0, 0, 0), kLimit);
    EXPECT_EQ(judgeLease({true, 7}, g, 7, 10, 10), kKeyExists);   // an existing key is KeyExists before it is a limit
    EXPECT_EQ(judgeLease({true, 7}, at(rec::LeaseOp::Put, 2), 7, 10, 10), 0);
    EXPECT_EQ(judgeLease({true, 7}, at(rec::LeaseOp::Expire, 0), 7, 10, 10), 0);
}

// ═══ Through the real VerdictSource ═══

TEST(LeaseJudgeTableRegression, TableRegression) {
    Rig r(/*maxKeys=*/2);
    r.start();
    int64_t g1 = 0, u1 = 0, c1 = 0, g2 = 0;

    // absent
    EXPECT_EQ(r.judgeOne(update(1, 99)), kNotFound);
    EXPECT_EQ(r.judgeOne(revoke(1, 99)), kNotFound);
    EXPECT_EQ(r.judgeOne(expire(1, 99)), kStale);
    EXPECT_EQ(r.judgeOne(checkpoint(1, 99, 5)), kStale);
    EXPECT_EQ(r.index->table.size(), 0u);

    // grant: the version is the offset (the value's own version is ignored), remaining starts at 0
    LV a; a.ver = 12345; a.ttl = 1000; a.holder = 5; a.rem = 999; a.addr = "a1"; a.port = 7;
    ASSERT_EQ(r.judgeOne(grant(1, a), &g1), 0);
    ASSERT_NE(r.row(1), nullptr);
    EXPECT_EQ(int64_t(r.col(1, 0)), g1);
    EXPECT_EQ(r.col(1, 1), 1000u);
    EXPECT_EQ(r.col(1, 2), 5u);
    EXPECT_EQ(r.col(1, 3), 0u);
    EXPECT_EQ(r.col(1, kIdSlot), 1u);
    EXPECT_EQ(r.addr(1), "a1");
    EXPECT_EQ(r.col32(1, kPortSlot), 7u);
    EXPECT_EQ(int64_t(r.col(1, kGrantedSlot)), g1);
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), g1);

    // equal version: a second grant is KeyExists and changes nothing
    LV other; other.addr = "zz";
    EXPECT_EQ(r.judgeOne(grant(1, other)), kKeyExists);
    EXPECT_EQ(r.addr(1), "a1");

    // update with the live version: replaces addr, port and the holder only
    LV b; b.ttl = 9999; b.rem = 777; b.holder = 8; b.addr = "b2"; b.port = 9;
    ASSERT_EQ(r.judgeOne(update(1, g1, b), &u1), 0);
    EXPECT_EQ(int64_t(r.col(1, 0)), g1);
    EXPECT_EQ(r.col(1, 1), 1000u);   // ttl unchanged
    EXPECT_EQ(r.col(1, 2), 8u);      // holder replaced
    EXPECT_EQ(r.col(1, 3), 0u);      // remaining unchanged
    EXPECT_EQ(r.addr(1), "b2");
    EXPECT_EQ(r.col32(1, kPortSlot), 9u);
    EXPECT_EQ(int64_t(r.col(1, kGrantedSlot)), g1);
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), u1);

    // different version: update and revoke are LeaseNotFound, nothing changes
    LV c; c.addr = "c3"; c.port = 1; c.holder = 1;
    EXPECT_EQ(r.judgeOne(update(1, g1 + 1, c)), kNotFound);
    EXPECT_EQ(r.judgeOne(revoke(1, g1 + 1)), kNotFound);
    EXPECT_EQ(r.addr(1), "b2");
    EXPECT_EQ(r.col(1, 2), 8u);
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), u1);

    // checkpoint with the live version: remaining and updated_offset only
    ASSERT_EQ(r.judgeOne(checkpoint(1, g1, 555), &c1), 0);
    EXPECT_EQ(r.col(1, 3), 555u);
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), c1);
    EXPECT_EQ(int64_t(r.col(1, 0)), g1);
    EXPECT_EQ(r.col(1, 1), 1000u);
    EXPECT_EQ(r.col(1, 2), 8u);
    EXPECT_EQ(r.addr(1), "b2");
    EXPECT_EQ(int64_t(r.col(1, kGrantedSlot)), g1);

    // stale broker records leave the row alone
    EXPECT_EQ(r.judgeOne(checkpoint(1, g1 + 1, 1)), kStale);
    EXPECT_EQ(r.judgeOne(expire(1, g1 + 1)), kStale);
    EXPECT_EQ(r.col(1, 3), 555u);
    ASSERT_NE(r.row(1), nullptr);

    // max_keys 2: the second grant fits, the third does not
    ASSERT_EQ(r.judgeOne(grant(2), &g2), 0);
    EXPECT_EQ(r.judgeOne(grant(3)), kLimit);
    EXPECT_EQ(r.row(3), nullptr);
    EXPECT_EQ(r.index->table.size(), 2u);

    // the rest: shapes the judge never accepts
    EXPECT_EQ(r.judgeOne({1, 0, valueOf({})}), kInvalid);                                               // upsert
    EXPECT_EQ(r.judgeOne({1, uint8_t(rec::withLeaseOp(1, rec::LeaseOp::Revoke)), valueOf({})}), kInvalid);
    EXPECT_EQ(r.judgeOne({1, uint8_t(rec::withLeaseOp(1, rec::LeaseOp::Expire)), valueOf({})}), kInvalid);
    EXPECT_EQ(r.judgeOne({1, 3, valueOf({})}), kInvalid);                                               // reserved intent
    EXPECT_EQ(r.judgeOne({1, kGrantA, {}}), kInvalid);                                                  // empty value
    EXPECT_EQ(r.index->table.size(), 2u);

    // revoke / expire with the live version end the lease; a freed slot admits a grant again
    EXPECT_EQ(r.judgeOne(revoke(2, g2 + 1)), kNotFound);
    ASSERT_EQ(r.judgeOne(revoke(2, g2)), 0);
    EXPECT_EQ(r.row(2), nullptr);
    EXPECT_EQ(r.judgeOne(grant(3)), 0);
    ASSERT_EQ(r.judgeOne(expire(1, g1)), 0);
    EXPECT_EQ(r.row(1), nullptr);
    EXPECT_EQ(r.index->table.size(), 1u);
    EXPECT_EQ(r.judgeOne(revoke(1, g1)), kNotFound);

    const auto& s = r.vs.stats();
    EXPECT_EQ(s.rejectionsKeyExists, 1u);
    EXPECT_EQ(s.rejectionsLeaseLimit, 1u);
    EXPECT_GE(s.rejectionsLeaseNotFound, 6u);
    EXPECT_EQ(s.leaseRejectedStaleBroker, 4u);
    EXPECT_FALSE(r.vs.stalled());
}

TEST(LeaseJudgeTableRegression, EventsRegression) {
    Rig r;
    r.start();
    int64_t g1 = 0, u = 0, cp = 0, rv = 0, g2 = 0, ex = 0;
    LV a; a.ttl = 4000; a.addr = "x"; a.holder = 1;
    ASSERT_EQ(r.judgeOne(grant(1, a), &g1), 0);
    EXPECT_EQ(r.judgeOne(update(1, g1 + 5)), kNotFound);          // rejected: no event
    LV b; b.addr = "y"; b.holder = 2;
    ASSERT_EQ(r.judgeOne(update(1, g1, b), &u), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, g1, 1500), &cp), 0);
    EXPECT_EQ(r.judgeOne(expire(1, g1 + 5)), kStale);              // rejected: no event
    ASSERT_EQ(r.judgeOne(revoke(1, g1), &rv), 0);
    LV c; c.ttl = 6000;
    ASSERT_EQ(r.judgeOne(grant(1, c), &g2), 0);
    ASSERT_EQ(r.judgeOne(expire(1, g2), &ex), 0);

    const auto& ev = r.st.leaseEvents.buf;
    ASSERT_EQ(ev.size(), 6u);
    EXPECT_EQ(ev[0].kind, LeaseEventKind::Granted);
    EXPECT_EQ(ev[0].version, g1);
    EXPECT_EQ(ev[0].offset, g1);
    EXPECT_EQ(ev[0].ttlMs, 4000u);
    EXPECT_EQ(ev[0].remainingMs, 0u);
    const uint32_t rid = ev[0].rid;
    EXPECT_EQ(ev[1].kind, LeaseEventKind::Updated);
    EXPECT_EQ(ev[1].rid, rid);
    EXPECT_EQ(ev[1].version, g1);
    EXPECT_EQ(ev[1].offset, u);
    EXPECT_EQ(ev[1].ttlMs, 4000u);
    EXPECT_EQ(ev[2].kind, LeaseEventKind::Checkpointed);
    EXPECT_EQ(ev[2].rid, rid);
    EXPECT_EQ(ev[2].version, g1);
    EXPECT_EQ(ev[2].offset, cp);
    EXPECT_EQ(ev[2].remainingMs, 1500u);
    EXPECT_EQ(ev[3].kind, LeaseEventKind::Ended);
    EXPECT_EQ(ev[3].endOp, uint8_t(rec::LeaseOp::Revoke));
    EXPECT_EQ(ev[3].rid, rid);
    EXPECT_EQ(ev[3].version, g1);
    EXPECT_EQ(ev[3].offset, rv);
    EXPECT_EQ(ev[3].remainingMs, 1500u);
    EXPECT_EQ(ev[4].kind, LeaseEventKind::Granted);
    EXPECT_EQ(ev[4].version, g2);
    EXPECT_EQ(ev[4].offset, g2);
    EXPECT_EQ(ev[4].ttlMs, 6000u);
    EXPECT_EQ(ev[5].kind, LeaseEventKind::Ended);
    EXPECT_EQ(ev[5].endOp, uint8_t(rec::LeaseOp::Expire));
    EXPECT_EQ(ev[5].rid, ev[4].rid);
    EXPECT_EQ(ev[5].version, g2);
    EXPECT_EQ(ev[5].offset, ex);
}

TEST(LeaseJudgeTableRegression, EventBackPressureRegression) {
    Rig r;
    r.start();
    const int64_t before = r.vs.judgedThrough();
    LeaseEvent filler;
    for (std::size_t i = 0; i + 1 < kLeaseFoldEventCap; ++i) r.st.leaseEvents.buf.push_back(filler);   // cap - 1 held
    const int64_t first = r.append({grant(1), grant(2)});
    EXPECT_EQ(r.vs.judgedThrough(), before);                  // the 2-record batch waits for the consumer
    EXPECT_EQ(r.row(1), nullptr);
    EXPECT_EQ(r.st.leaseEvents.buf.size(), kLeaseFoldEventCap - 1);
    EXPECT_FALSE(r.vs.stalled());
    r.st.leaseEvents.buf.clear();
    r.drive();
    EXPECT_EQ(r.vs.judgedThrough(), first + 2);
    EXPECT_NE(r.row(1), nullptr);
    EXPECT_NE(r.row(2), nullptr);
    EXPECT_EQ(r.st.leaseEvents.buf.size(), 2u);
    EXPECT_EQ(r.vs.stats().leaseEventOverflows, 0u);
}

TEST(LeaseJudgeTableRegression, CapacityVerdictDeterministicRegression) {
    Rig a(/*maxKeys=*/2);
    a.start();
    const int64_t first = a.append({grant(1), grant(2), grant(3)});
    const auto* e = a.st.rejected.find(first + 2);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->code, kLimit);
    EXPECT_EQ(a.st.rejected.find(first), nullptr);
    EXPECT_EQ(a.st.rejected.find(first + 1), nullptr);
    ASSERT_EQ(a.index->table.size(), 2u);

    // A replay of the log (a rebuild) skips the rejected record: the same rows.
    rune::view::ViewTable table(a.index->layout, 4096, kLeaseIndexChunkBytes);
    rune::view::VersionDescriptorCache vdc(a.evolving);
    rune::view::Prospect prospect;
    prospect.reserve(a.index->layout, nullptr);
    LeaseIndexFold f{table, a.index->layout, a.evolving, 8};
    const rune::view::FoldSkip skip{.aborted = {}, .rejected = &a.st.rejected};
    int64_t cursor = 0;
    (void)foldLeaseIndexBatch(f, vdc, prospect, a.log.read(0), a.log.next, cursor, skip);
    EXPECT_EQ(cursor, a.log.next);
    EXPECT_EQ(table.size(), 2u);
    EXPECT_EQ(table.get(uint64_t(3)), nullptr);
    for (uint64_t id : {1, 2}) {
        const uint8_t* x = table.get(id);
        const uint8_t* y = a.row(id);
        ASSERT_NE(x, nullptr);
        ASSERT_NE(y, nullptr);
        EXPECT_EQ(std::memcmp(x, y, a.index->layout.rowLen), 0) << "row " << id;
    }

    // A fresh judge over the same log reaches the same verdicts and rows.
    Rig b(/*maxKeys=*/2);
    b.log = a.log;
    b.start();
    ASSERT_NE(b.st.rejected.find(first + 2), nullptr);
    EXPECT_EQ(b.st.rejected.find(first + 2)->code, kLimit);
    EXPECT_EQ(b.st.rejected.size(), a.st.rejected.size());
    EXPECT_EQ(b.index->table.size(), 2u);
    EXPECT_EQ(b.row(3), nullptr);
}

TEST(LeaseJudgeTableRegression, BrokerRecordsNeverStallRegression) {
    Rig r;
    r.st.rejected.reserve(1);
    r.start();
    int64_t g = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g), 0);
    EXPECT_EQ(r.judgeOne(grant(1)), kKeyExists);   // a client rejection fills the index
    EXPECT_EQ(r.st.rejected.size(), 1u);
    EXPECT_FALSE(r.vs.stalled());

    int64_t stale = 0;
    EXPECT_EQ(r.judgeOne(expire(1, g + 100), &stale), kStale);   // takes the reserved slot
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.stats().leaseRejectedStaleBroker, 1u);
    EXPECT_EQ(r.vs.judgedThrough(), stale + 1);
    EXPECT_NE(r.st.rejected.find(stale), nullptr);
    EXPECT_TRUE(r.st.staleBrokerLogged);
    ASSERT_NE(r.row(1), nullptr);

    // The documented limit: a second stale broker record has no slot left and stalls Full.
    int64_t second = 0;
    (void)r.judgeOne(checkpoint(1, g + 100, 5), &second);
    EXPECT_TRUE(r.vs.stalled());
    EXPECT_EQ(r.vs.judgedThrough(), second);
}

TEST(LeaseJudgeTableRegression, TokensOnlyGrowRegression) {
    Rig r;
    r.start();
    int64_t o1 = 0, o2 = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &o1), 0);
    ASSERT_EQ(r.judgeOne(expire(1, o1)), 0);
    ASSERT_EQ(r.judgeOne(grant(1), &o2), 0);
    EXPECT_GT(o2, o1);
    EXPECT_EQ(int64_t(r.col(1, 0)), o2);
    EXPECT_EQ(r.judgeOne(update(1, o1)), kNotFound);   // the first lease's token is dead
    EXPECT_EQ(r.judgeOne(expire(1, o1)), kStale);
    EXPECT_EQ(r.judgeOne(update(1, o2)), 0);
}

TEST(LeaseJudgeTableRegression, ProjectionAcrossVersionsRegression) {
    // v1 {id, addr}; v2 adds port.  The row has the port column from the start; a v1 record leaves it null.
    Rig r(100, evolvingDesc(/*latest=*/2, /*portSince=*/2));
    r.start();
    LV v1; v1.schema = 1; v1.withPort = false; v1.addr = "old"; v1.holder = 3;
    int64_t g = 0, u = 0, u2 = 0;
    ASSERT_EQ(r.judgeOne(grant(1, v1), &g), 0);
    EXPECT_EQ(r.addr(1), "old");
    EXPECT_TRUE(r.isNull(1, kPortSlot));

    LV v2; v2.schema = 2; v2.addr = "new"; v2.port = 42; v2.holder = 4;
    ASSERT_EQ(r.judgeOne(update(1, g, v2), &u), 0);
    EXPECT_FALSE(r.isNull(1, kPortSlot));
    EXPECT_EQ(r.col32(1, kPortSlot), 42u);
    EXPECT_EQ(r.addr(1), "new");
    EXPECT_EQ(r.col(1, 2), 4u);

    // a v1 update after: the writer has no port, so the column is null again
    LV v1b; v1b.schema = 1; v1b.withPort = false; v1b.addr = "back";
    ASSERT_EQ(r.judgeOne(update(1, g, v1b), &u2), 0);
    EXPECT_TRUE(r.isNull(1, kPortSlot));
    EXPECT_EQ(r.addr(1), "back");
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), u2);

    // a grant straight at v2 fills the port
    LV v2b; v2b.schema = 2; v2b.port = 77;
    ASSERT_EQ(r.judgeOne(grant(2, v2b)), 0);
    EXPECT_EQ(r.col32(2, kPortSlot), 77u);
}

TEST(LeaseJudgeTableRegression, ProjectLeaseRowRegression) {
    const rune::codec::Descriptor ev = evolvingDesc(2, 2);
    const rune::view::RowLayout layout = leaseIndexRowLayout(ev);
    const rune::codec::Descriptor out = leaseIndexOutDescriptor(ev);
    ASSERT_EQ(layout.slotType.size(), 9u);
    EXPECT_EQ(out.fields[7].name, "lease_granted_offset");
    EXPECT_EQ(out.fields[8].name, "lease_updated_offset");
    const rune::codec::Descriptor w1 = rune::codec::materializeAsOf(ev, 1), w2 = rune::codec::materializeAsOf(ev, 2);
    std::vector<uint8_t> row(layout.rowLen, 0xAA);

    LV v; v.ver = 5; v.ttl = 6; v.holder = 7; v.rem = 8; v.id = 9; v.addr = "host"; v.port = 10; v.schema = 2;
    const auto b2 = valueOf(v);
    ASSERT_EQ(projectLeaseRow(row.data(), layout, ev, w2, sv(b2)), rune::view::PrepareStatus::Ok);
    uint64_t x = 0;
    std::memcpy(&x, row.data() + layout.slotOffset[0], 8);
    EXPECT_EQ(x, 5u);
    std::memcpy(&x, row.data() + layout.slotOffset[kIdSlot], 8);
    EXPECT_EQ(x, 9u);
    EXPECT_FALSE(rune::view::isNull(row.data(), layout, kPortSlot));
    EXPECT_EQ(0, std::memcmp(row.data() + layout.slotOffset[kAddrSlot], "host", 4));
    std::memcpy(&x, row.data() + layout.slotOffset[kGrantedSlot], 8);
    EXPECT_EQ(x, 0u);   // the offset columns stay zero

    v.schema = 1; v.withPort = false;
    const auto b1 = valueOf(v);
    ASSERT_EQ(projectLeaseRow(row.data(), layout, out, w1, sv(b1)), rune::view::PrepareStatus::Ok);   // the out descriptor works too
    EXPECT_TRUE(rune::view::isNull(row.data(), layout, kPortSlot));
    EXPECT_FALSE(rune::view::isNull(row.data(), layout, kAddrSlot));

    // shorter than the writer's block
    EXPECT_EQ(projectLeaseRow(row.data(), layout, ev, w2, sv(b1)), rune::view::PrepareStatus::Malformed);
    EXPECT_EQ(projectLeaseRow(row.data(), layout, ev, w2, std::string_view(sv(b2)).substr(0, 20)), rune::view::PrepareStatus::Malformed);
    EXPECT_EQ(projectLeaseRow(row.data(), layout, ev, w2, std::string_view{}), rune::view::PrepareStatus::Malformed);

    // a writer whose field has another width or type than the layout (cannot come from evolution)
    rune::codec::DescriptorBuilder narrow("inst", 2);
    narrow.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, true).addFixedStr("addr", 8).add("port", P::UInt32, 2);
    const rune::codec::Descriptor wn = narrow.build();
    EXPECT_EQ(projectLeaseRow(row.data(), layout, ev, wn, sv(b2)), rune::view::PrepareStatus::Malformed);
    rune::codec::DescriptorBuilder wide("inst", 2);
    wide.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, true).addFixedStr("addr", 16).add("port", P::UInt64, 2);
    const rune::codec::Descriptor ww = wide.build();
    std::vector<uint8_t> big = b2;
    big.resize(big.size() + 8, 0);
    EXPECT_EQ(projectLeaseRow(row.data(), layout, ev, ww, sv(big)), rune::view::PrepareStatus::Malformed);
}
