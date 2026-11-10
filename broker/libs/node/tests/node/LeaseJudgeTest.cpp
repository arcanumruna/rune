// Lease topics Task 6: the lease index as a verdict member (unit level: no worker, no replica).
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <cstring>
#include <random>
#include <memory>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Validator.h>
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
    std::size_t          keyLen = 8;   // the wire key's width (a wrong one is malformed)
};
LRec grant(uint64_t id, LV v = {}) { v.id = id; return {id, kGrantA, valueOf(v)}; }
LRec update(uint64_t id, int64_t ver, LV v = {}) { v.id = id; v.ver = ver; return {id, kUpdateA, valueOf(v)}; }
LRec revoke(uint64_t id, int64_t ver, uint64_t ttl = 1000) { LV v; v.id = id; v.ver = ver; v.ttl = ttl; return {id, kRevokeA, valueOf(v)}; }
LRec expire(uint64_t id, int64_t ver) { LV v; v.id = id; v.ver = ver; return {id, kExpireA, valueOf(v)}; }
LRec checkpoint(uint64_t id, int64_t ver, uint64_t rem, LV v = {}) { v.id = id; v.ver = ver; v.rem = rem; return {id, kCheckptA, valueOf(v)}; }

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
            std::vector<uint8_t> k(r.keyLen);
            putLe(k, 0, r.id, int(std::min<std::size_t>(8, r.keyLen)));
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

    /// `installedThrough`: the schema versions the spec has installed (0 = all of the evolving descriptor's).
    /// `mode`: Legacy = no aux (the pre-Task-29 tests), Persisted = the production persisted lease table (aux 8), Transient = the production
    /// transient one (aux 32, spec.leaseTransient, the anchor list kept).
    enum class Mode : uint8_t { Legacy, Persisted, Transient };
    explicit Rig(uint32_t maxKeys = 100, rune::codec::Descriptor ev = evolvingDesc(), uint16_t installedThrough = 0, Mode mode = Mode::Legacy) : evolving(std::move(ev)) {
        spec.topicId    = 5;
        spec.keyFieldId = 4;
        spec.keyWidth   = 8;
        spec.lease      = rune::protocol::LeaseSpec{};
        spec.lease->maxKeys = maxKeys;
        spec.leaseTransient = mode == Mode::Transient;
        for (uint16_t v = 1; v <= (installedThrough != 0 ? installedThrough : evolving.version); ++v) spec.installVersion(v, rune::codec::materializeAsOf(evolving, v));   // the writers' descriptors come from the spec
        spec.evolving = evolving;
        st.spec = &spec;
        st.leaseEvents.reserveOnce();
        rune::view::ViewTableKeys keys{8, 0, 0, ~uint64_t{0}, mode == Mode::Transient ? kLeaseTransientAuxBytes : mode == Mode::Persisted ? kLeaseAuxBytes : 0u};
        index = std::make_unique<ViewInstance>(leaseIndexPlan(5, 4), leaseIndexRowLayout(evolving), evolving, false, 0, 4096, kLeaseIndexChunkBytes, keys);
        index->leaseTransient.transient = spec.leaseTransient;
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
    [[nodiscard]] uint32_t ridOf(uint64_t id) const { return ridIn(index->table, id); }
    static uint32_t ridIn(const rune::view::ViewTable& t, uint64_t id) {
        uint8_t k[8];
        putKey(k, id);
        uint32_t rid = kLeaseAnchorNil;
        (void)t.find(rune::view::ViewKey::fromBytes({k, 8}), rid);
        return rid;
    }
    static void putKey(uint8_t* k, uint64_t id) { for (int i = 0; i < 8; ++i) k[i] = uint8_t(id >> (8 * i)); }
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
        {"carry",            at(rec::LeaseOp::Put, 0),        kStale, 0,          kStale},   // Task 29: Put + Upsert(0) is a Carry (was InvalidRequest x3)
        {"putIntent3",       at(rec::LeaseOp::Put, 3),        kInvalid, kInvalid, kInvalid},
        {"revokeInsert",     at(rec::LeaseOp::Revoke, 1),     kInvalid, kInvalid, kInvalid},
        {"revokeNoIntent",   at(rec::LeaseOp::Revoke, 0),     kInvalid, kInvalid, kInvalid},
        {"expireInsert",     at(rec::LeaseOp::Expire, 1),     kInvalid, kInvalid, kInvalid},
        {"expireExisting",   at(rec::LeaseOp::Expire, 2),     kInvalid, kInvalid, kInvalid},
        {"checkpointInsert", at(rec::LeaseOp::Checkpoint, 1), kInvalid, kInvalid, kInvalid},
        {"checkpointExist",  at(rec::LeaseOp::Checkpoint, 2), kInvalid, kInvalid, kInvalid},
    };
    constexpr uint64_t kTtl = 100;
    for (const Row& r : rows) {
        SCOPED_TRACE(r.name);
        EXPECT_EQ(judgeLease({false, 0, 0}, r.attrs, 7, 0, 0, 10), r.absent);
        EXPECT_EQ(judgeLease({true, 7, kTtl}, r.attrs, 7, 0, 1, 10), r.equal);
        EXPECT_EQ(judgeLease({true, 7, kTtl}, r.attrs, 8, 0, 1, 10), r.different);
    }
    // max_keys: the grant is judged against the live count; every other record ignores it.
    const uint8_t g = at(rec::LeaseOp::Put, 1);
    EXPECT_EQ(judgeLease({false, 0, 0}, g, 0, 0, 9, 10), 0);
    EXPECT_EQ(judgeLease({false, 0, 0}, g, 0, 0, 10, 10), kLimit);
    EXPECT_EQ(judgeLease({false, 0, 0}, g, 0, 0, 11, 10), kLimit);
    EXPECT_EQ(judgeLease({false, 0, 0}, g, 0, 0, 0, 0), kLimit);
    EXPECT_EQ(judgeLease({true, 7, kTtl}, g, 7, 0, 10, 10), kKeyExists);   // an existing key is KeyExists before it is a limit
    EXPECT_EQ(judgeLease({true, 7, kTtl}, at(rec::LeaseOp::Put, 2), 7, 0, 10, 10), 0);
    EXPECT_EQ(judgeLease({true, 7, kTtl}, at(rec::LeaseOp::Expire, 0), 7, 0, 10, 10), 0);
}

// A grant with remaining != 0 is refused; an update or revoke's TTL and remaining are not checked (spec 6.4).  Not-found / version answers keep precedence.
TEST(LeaseJudgeTableRegression, JudgeLeaseExactTtlAndRemainingRegression) {
    const auto at = [](rec::LeaseOp op, uint8_t intent) { return rec::withLeaseOp(intent, op); };
    const uint8_t grantA = at(rec::LeaseOp::Put, 1), updateA = at(rec::LeaseOp::Put, 2), revokeA = at(rec::LeaseOp::Revoke, 2);
    const LeaseRowState row{true, 7, 100};
    const LeaseRowState none{false, 0, 0};
    // A Put with remaining != 0 is refused, whatever the intent.
    EXPECT_EQ(judgeLease(none, grantA, 0, 1, 0, 10), kInvalid);
    // Spec 6.4: an update or revoke's TTL and remaining are NOT checked (the write hook refuses a client remaining != 0; the
    // judge keeps the row's TTL and remaining), so every one of these is accepted.
    EXPECT_EQ(judgeLease(row, updateA, 7, 1, 1, 10), 0);
    EXPECT_EQ(judgeLease(row, revokeA, 7, 1, 1, 10), 0);
    EXPECT_EQ(judgeLease(row, updateA, 7, 0, 1, 10), 0);
    EXPECT_EQ(judgeLease(row, revokeA, 7, 0, 1, 10), 0);
    // Precedence: the not-found / version answers come first, then KeyExists / the limit.
    EXPECT_EQ(judgeLease(none, updateA, 7, 1, 0, 10), kNotFound);
    EXPECT_EQ(judgeLease(row, updateA, 8, 1, 1, 10), kNotFound);
    EXPECT_EQ(judgeLease(none, revokeA, 7, 1, 0, 10), kNotFound);
    EXPECT_EQ(judgeLease(row, revokeA, 8, 1, 1, 10), kNotFound);
    EXPECT_EQ(judgeLease(row, grantA, 0, 1, 1, 10), kKeyExists);
    EXPECT_EQ(judgeLease(none, grantA, 0, 1, 10, 10), kLimit);
    // A grant takes its TTL as written (any value); broker records are not checked for either column.
    EXPECT_EQ(judgeLease(none, grantA, 0, 0, 0, 10), 0);
    EXPECT_EQ(judgeLease(row, at(rec::LeaseOp::Expire, 0), 7, 9, 1, 10), 0);
    EXPECT_EQ(judgeLease(row, at(rec::LeaseOp::Checkpoint, 0), 7, 9, 1, 10), 0);
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

    // a grant carrying remaining != 0 is refused (R1: every client record carries remaining 0)
    LV bad; bad.rem = 999; bad.addr = "bad";
    EXPECT_EQ(r.judgeOne(grant(1, bad)), kInvalid);
    EXPECT_EQ(r.row(1), nullptr);

    // grant: the version is the offset (the value's own version is ignored), remaining starts at 0
    LV a; a.ver = 12345; a.ttl = 1000; a.holder = 5; a.addr = "a1"; a.port = 7;
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

    // an update that carries another TTL or remaining != 0 is ACCEPTED (spec 6.4: not checked); the row keeps its own TTL and remaining
    LV wrongTtl; wrongTtl.ttl = 9999; wrongTtl.addr = "t";
    EXPECT_EQ(r.judgeOne(update(1, g1, wrongTtl)), 0);
    EXPECT_EQ(r.addr(1), "t");
    EXPECT_EQ(r.col(1, 1), 1000u);
    LV withRem; withRem.rem = 777; withRem.addr = "r";
    EXPECT_EQ(r.judgeOne(update(1, g1, withRem)), 0);
    EXPECT_EQ(r.addr(1), "r");
    EXPECT_EQ(r.col(1, 1), 1000u);
    EXPECT_EQ(r.col(1, 3), 0u);

    // update with the live version: replaces addr, port and the holder only
    LV b; b.ttl = 1000; b.holder = 8; b.addr = "b2"; b.port = 9;
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
    EXPECT_EQ(r.judgeOne({1, 0, valueOf({})}), kStale);                                                 // a Carry of another version (Task 29; was InvalidRequest)
    EXPECT_EQ(r.judgeOne({1, uint8_t(rec::withLeaseOp(1, rec::LeaseOp::Revoke)), valueOf({})}), kInvalid);
    EXPECT_EQ(r.judgeOne({1, uint8_t(rec::withLeaseOp(1, rec::LeaseOp::Expire)), valueOf({})}), kInvalid);
    EXPECT_EQ(r.judgeOne({1, 3, valueOf({})}), kInvalid);                                               // reserved intent
    EXPECT_EQ(r.judgeOne({1, kGrantA, {}}), kInvalid);                                                  // empty value
    EXPECT_EQ(r.index->table.size(), 2u);

    // malformed records are refused (39), never silently accepted: a short value, a key of the wrong width
    const uint64_t malformed = r.vs.stats().keyIndexSkippedMalformed;
    EXPECT_EQ(r.judgeOne({9, kGrantA, std::vector<uint8_t>(4 + 31, 0)}), kInvalid);                      // < 4 + 32 bytes
    EXPECT_EQ(r.judgeOne({9, kGrantA, valueOf({}), 4}), kInvalid);                                       // a 4-byte key on an 8-byte index
    EXPECT_EQ(r.judgeOne({9, kGrantA, valueOf({}), 9}), kInvalid);                                       // a 9-byte key
    EXPECT_EQ(r.judgeOne({9, kUpdateA, std::vector<uint8_t>(5, 0)}), kInvalid);
    EXPECT_EQ(r.vs.stats().keyIndexSkippedMalformed, malformed + 4);
    EXPECT_EQ(r.row(9), nullptr);
    EXPECT_EQ(r.index->table.size(), 2u);
    EXPECT_FALSE(r.vs.stalled());

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
    EXPECT_EQ(s.leaseRejectedStaleBroker, 5u);   // + the Carry of another version (Task 29: was an InvalidRequest, now a stale broker record)
    EXPECT_FALSE(r.vs.stalled());
}

TEST(LeaseJudgeTableRegression, EventsRegression) {
    Rig r;
    r.start();
    int64_t g1 = 0, u = 0, cp = 0, rv = 0, g2 = 0, ex = 0;
    LV a; a.ttl = 4000; a.addr = "x"; a.holder = 1;
    ASSERT_EQ(r.judgeOne(grant(1, a), &g1), 0);
    EXPECT_EQ(r.judgeOne(update(1, g1 + 5)), kNotFound);          // rejected: no event
    LV b; b.ttl = 4000; b.addr = "y"; b.holder = 2;
    ASSERT_EQ(r.judgeOne(update(1, g1, b), &u), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, g1, 1500), &cp), 0);
    EXPECT_EQ(r.judgeOne(expire(1, g1 + 5)), kStale);              // rejected: a BrokerRejected event (Task 10)
    ASSERT_EQ(r.judgeOne(revoke(1, g1, 4000), &rv), 0);
    LV c; c.ttl = 6000;
    ASSERT_EQ(r.judgeOne(grant(1, c), &g2), 0);
    ASSERT_EQ(r.judgeOne(expire(1, g2), &ex), 0);

    const auto& ev = r.st.leaseEvents.buf;
    ASSERT_EQ(ev.size(), 7u);
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
    EXPECT_EQ(ev[3].kind, LeaseEventKind::BrokerRejected) << "the stale Expire of the live row";
    EXPECT_EQ(ev[3].rid, rid);
    EXPECT_EQ(ev[3].version, g1 + 5);
    EXPECT_EQ(ev[4].kind, LeaseEventKind::Ended);
    EXPECT_EQ(ev[4].endOp, uint8_t(rec::LeaseOp::Revoke));
    EXPECT_EQ(ev[4].rid, rid);
    EXPECT_EQ(ev[4].version, g1);
    EXPECT_EQ(ev[4].offset, rv);
    EXPECT_EQ(ev[4].remainingMs, 1500u);
    EXPECT_EQ(ev[5].kind, LeaseEventKind::Granted);
    EXPECT_EQ(ev[5].version, g2);
    EXPECT_EQ(ev[5].offset, g2);
    EXPECT_EQ(ev[5].ttlMs, 6000u);
    EXPECT_EQ(ev[6].kind, LeaseEventKind::Ended);
    EXPECT_EQ(ev[6].endOp, uint8_t(rec::LeaseOp::Expire));
    EXPECT_EQ(ev[6].rid, ev[5].rid);
    EXPECT_EQ(ev[6].version, g2);
    EXPECT_EQ(ev[6].offset, ex);
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
    rune::view::Prospect prospect;
    prospect.reserve(a.index->layout, nullptr);
    LeaseIndexFold f{table, a.index->layout, a.evolving, 8};
    const rune::view::FoldSkip skip{.aborted = {}, .rejected = &a.st.rejected};
    int64_t cursor = 0;
    (void)foldLeaseIndexBatch(f, a.spec, prospect, a.log.read(0), a.log.next, cursor, skip);
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

// A broker Expire / Checkpoint whose key has no row and whose projection is malformed is a stale broker record like any other:
// it takes the reserved stale-broker slot (never stalls the judge); a malformed CLIENT record is still InvalidRequest.
TEST(LeaseJudgeTableRegression, MalformedStaleBrokerRecordRegression) {
    {
        Rig c;
        c.start();
        LRec truncatedGrant = grant(7);
        truncatedGrant.value.resize(40);   // shorter than the writer's block: malformed projection
        EXPECT_EQ(c.judgeOne(truncatedGrant), kInvalid) << "a malformed client record keeps the normal answer";
    }
    Rig r;
    r.st.rejected.reserve(1);
    r.start();
    int64_t g = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g), 0);
    EXPECT_EQ(r.judgeOne(grant(1)), kKeyExists);   // a client rejection fills the one-slot index
    EXPECT_EQ(r.st.rejected.size(), 1u);

    LRec badCheckpoint = checkpoint(9, 5, 5);   // no row for key 9
    badCheckpoint.value.resize(40);
    int64_t at = 0;
    EXPECT_EQ(r.judgeOne(badCheckpoint, &at), kStale) << "the reserved stale-broker slot, not InvalidRequest";
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.stats().leaseRejectedStaleBroker, 1u);
    EXPECT_EQ(r.vs.judgedThrough(), at + 1);
    EXPECT_EQ(r.row(9), nullptr);
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

// ═══ Fix round: Pending (the layout is not there yet) ═══

TEST(LeaseJudgeTableRegression, PendingWhenTheLayoutIsMissingRegression) {
    // The layout is the v1 evolving descriptor (no port); the writer is v2 (has port, field id 6).
    const rune::codec::Descriptor ev1 = evolvingDesc(1, /*portSince=*/9);   // port is not live at v1: the layout still has its column
    const rune::codec::Descriptor ev2 = evolvingDesc(2, 2);
    rune::codec::DescriptorBuilder noPort("inst", 1);
    noPort.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, true).addFixedStr("addr", 16);
    const rune::codec::Descriptor oldLayoutSrc = noPort.build();
    const rune::view::RowLayout oldLayout = leaseIndexRowLayout(oldLayoutSrc);
    const rune::codec::Descriptor w2 = rune::codec::materializeAsOf(ev2, 2);
    LV v; v.schema = 2; v.port = 5;
    const auto b2 = valueOf(v);
    std::vector<uint8_t> row(oldLayout.rowLen, 0);

    // a writer holding a field id the layout lacks
    EXPECT_EQ(projectLeaseRow(row.data(), oldLayout, oldLayoutSrc, w2, sv(b2)), rune::view::PrepareStatus::Pending);
    // ...and through the prepare: no verdict, no operation
    rune::view::ViewTable table(oldLayout, 4096, kLeaseIndexChunkBytes);
    rune::view::Prospect prospect;
    prospect.reserve(oldLayout, nullptr);
    const LeaseIndexFold f{table, oldLayout, oldLayoutSrc, 8};
    std::vector<uint8_t> key(8, 0);
    key[0] = 1;
    rune::view::SourceRecord rec{sv(key), sv(b2), 5, rec::intentOf(kGrantA), &w2, kGrantA};
    EXPECT_EQ(prepareLeaseIndex(f, rec, prospect), rune::view::PrepareStatus::Pending);
    EXPECT_EQ(prospect.op, rune::view::ProspectOp::None);

    // a null writer descriptor (the version is not installed): Pending, for a long enough value
    rec.writer = nullptr;
    EXPECT_EQ(prepareLeaseIndex(f, rec, prospect), rune::view::PrepareStatus::Pending);
    // ...but a value too short to be a lease record is Malformed whatever the writer
    const std::vector<uint8_t> shortValue(10, 0);
    rec.value = sv(shortValue);
    EXPECT_EQ(prepareLeaseIndex(f, rec, prospect), rune::view::PrepareStatus::Malformed);
    // a matching writer is Ok
    const rune::codec::Descriptor w1 = rune::codec::materializeAsOf(oldLayoutSrc, 1);
    LV v1; v1.schema = 1; v1.withPort = false;
    const auto b1 = valueOf(v1);
    rec.value = sv(b1);
    rec.writer = &w1;
    EXPECT_EQ(prepareLeaseIndex(f, rec, prospect), rune::view::PrepareStatus::Ok);
    (void)ev1;
}

TEST(LeaseJudgeTableRegression, JudgeHoldsOnAnUninstalledVersionRegression) {
    Rig r(100, evolvingDesc(2, 2), /*installedThrough=*/1);   // the spec knows v1 only
    r.start();
    const int64_t before = r.vs.judgedThrough();
    int64_t g1 = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g1), 0) << "a v1 record is judged";
    LV v2; v2.schema = 2; v2.port = 11;
    const int64_t pendingAt = r.append({grant(2, v2), grant(3)});
    EXPECT_EQ(r.vs.judgedThrough(), pendingAt) << "held AT the v2 record: nothing judged, nothing past it";
    EXPECT_EQ(r.st.rejected.find(pendingAt), nullptr);
    EXPECT_EQ(r.row(2), nullptr);
    EXPECT_EQ(r.row(3), nullptr);
    EXPECT_FALSE(r.vs.stalled());
    EXPECT_EQ(r.vs.stats().leaseLayoutWaits, 1u);
    r.drive();
    r.drive();
    EXPECT_EQ(r.vs.stats().leaseLayoutWaits, 1u) << "counted once per wait, not once per retry";
    EXPECT_EQ(r.vs.judgedThrough(), pendingAt);
    EXPECT_GT(pendingAt, before);

    // the version arrives: the next duty cycle judges the held record and the rest
    ASSERT_TRUE(r.spec.installVersion(2, rune::codec::materializeAsOf(r.evolving, 2)));
    r.drive();
    EXPECT_EQ(r.vs.judgedThrough(), pendingAt + 2);
    ASSERT_NE(r.row(2), nullptr);
    EXPECT_EQ(r.col32(2, kPortSlot), 11u);
    EXPECT_NE(r.row(3), nullptr);
    EXPECT_TRUE(r.isNull(3, kPortSlot)) << "grant(3) is a v1 record: no port";
}

TEST(LeaseJudgeTableRegression, ReplayStopsAtAnUninstalledVersionRegression) {
    Rig r(100, evolvingDesc(2, 2), /*installedThrough=*/1);
    Log log;
    log.control();
    LV v2; v2.schema = 2; v2.port = 11;
    const int64_t first = log.data({grant(1), grant(2, v2), grant(3)});   // one batch: the stop is mid-batch

    rune::view::ViewTable table(r.index->layout, 4096, kLeaseIndexChunkBytes);
    rune::view::Prospect prospect;
    prospect.reserve(r.index->layout, nullptr);
    LeaseIndexFold f{table, r.index->layout, r.evolving, 8};
    const rune::view::FoldSkip skip{.aborted = {}, .rejected = &r.st.rejected};
    int64_t cursor = 0;
    bool held = false;
    (void)foldLeaseIndexBatch(f, r.spec, prospect, log.read(0), log.next, cursor, skip, &held);
    EXPECT_TRUE(held);
    EXPECT_EQ(cursor, first + 1) << "the fold stops AT the record whose version is not installed";
    EXPECT_NE(table.get(uint64_t(1)), nullptr);
    EXPECT_EQ(table.get(uint64_t(2)), nullptr);
    EXPECT_EQ(table.get(uint64_t(3)), nullptr);

    ASSERT_TRUE(r.spec.installVersion(2, rune::codec::materializeAsOf(r.evolving, 2)));
    held = false;
    (void)foldLeaseIndexBatch(f, r.spec, prospect, log.read(cursor), log.next, cursor, skip, &held);
    EXPECT_FALSE(held);
    EXPECT_EQ(cursor, log.next);
    EXPECT_NE(table.get(uint64_t(2)), nullptr);
    EXPECT_NE(table.get(uint64_t(3)), nullptr);
}

// ═══ Fix round: replay of a compacted log keeps every live lease ═══

namespace {

struct Replayed {
    std::unique_ptr<rune::view::ViewTable> table;
    rune::view::RowLayout                  layout;
    int64_t                                cursor = 0;
};
Replayed replayLog(Rig& r, const Log& log) {
    Replayed out;
    out.layout = r.index->layout;
    out.table = std::make_unique<rune::view::ViewTable>(out.layout, 4096, kLeaseIndexChunkBytes);
    rune::view::Prospect prospect;
    prospect.reserve(out.layout, nullptr);
    LeaseIndexFold f{*out.table, out.layout, r.evolving, 8};
    const rune::view::FoldSkip skip{.aborted = {}, .rejected = &r.st.rejected};
    (void)foldLeaseIndexBatch(f, r.spec, prospect, log.read(0), log.next, out.cursor, skip);
    return out;
}
uint64_t colOf(const Replayed& x, uint64_t id, uint16_t slot) {
    const uint8_t* row = x.table->get(id);
    uint64_t v = 0;
    if (row != nullptr) std::memcpy(&v, row + x.layout.slotOffset[slot], 8);
    return v;
}
std::string addrOf(const Replayed& x, uint64_t id) {
    const uint8_t* row = x.table->get(id);
    if (row == nullptr) return "<absent>";
    std::string s(reinterpret_cast<const char*>(row + x.layout.slotOffset[kAddrSlot]), 16);
    return s.substr(0, s.find('\0'));
}

}  // namespace

TEST(LeaseJudgeTableRegression, LeaseReplayCompactedLogRegression) {
    Rig r;
    constexpr int64_t V = 1;   // the grant's offset, carried by every later record of the lease
    LV up; up.holder = 8; up.addr = "u4"; up.port = 9; up.ttl = 7000;
    LV ck; ck.holder = 3; ck.addr = "c3"; ck.port = 2; ck.ttl = 7000;

    // (f) the full, uncompacted log: grant at offset 1, update at 2
    Log full;
    full.control();
    LV gv; gv.ttl = 7000;   // the grant's TTL: an update keeps it, a replay without the grant takes it from the update's image
    const int64_t g = full.data({grant(1, gv)});
    ASSERT_EQ(g, V);
    const int64_t uFull = full.data({update(1, V, up)});
    const Replayed f = replayLog(r, full);
    ASSERT_EQ(f.table->size(), 1u);

    // (a) update only: the grant was compacted away (offset 1 is a control batch, the update stays at offset 2)
    Log a;
    a.control();
    a.control();
    const int64_t uA = a.data({update(1, V, up)});
    ASSERT_EQ(uA, uFull);
    const Replayed ra = replayLog(r, a);
    ASSERT_EQ(ra.table->size(), 1u) << "the lease survives a replay that sees no grant";
    EXPECT_EQ(int64_t(colOf(ra, 1, 0)), V);
    EXPECT_EQ(colOf(ra, 1, 1), 7000u);
    EXPECT_EQ(colOf(ra, 1, 2), 8u);
    EXPECT_EQ(colOf(ra, 1, 3), 0u);
    EXPECT_EQ(addrOf(ra, 1), "u4");
    EXPECT_EQ(uint32_t(colOf(ra, 1, kPortSlot) & 0xFFFFFFFFu), 9u);
    EXPECT_EQ(int64_t(colOf(ra, 1, kGrantedSlot)), V);
    EXPECT_EQ(int64_t(colOf(ra, 1, kUpdatedSlot)), uA);
    // (f) = (a), column by column (the grant's own columns equal the update's where the update replaced them)
    ASSERT_EQ(std::memcmp(f.table->get(uint64_t(1)), ra.table->get(uint64_t(1)), f.layout.rowLen), 0) << "full log and compacted log agree";

    // (b) Checkpoint(o3), then Update(o4): remaining from the checkpoint, user columns from the update
    Log b;
    b.control();
    b.control();
    const int64_t cB = b.data({checkpoint(1, V, 555, ck)});
    const int64_t uB = b.data({update(1, V, up)});
    const Replayed rb = replayLog(r, b);
    ASSERT_EQ(rb.table->size(), 1u);
    EXPECT_EQ(int64_t(colOf(rb, 1, 0)), V);
    EXPECT_EQ(colOf(rb, 1, 3), 555u);
    EXPECT_EQ(colOf(rb, 1, 2), 8u);
    EXPECT_EQ(addrOf(rb, 1), "u4");
    EXPECT_EQ(int64_t(colOf(rb, 1, kGrantedSlot)), V);
    EXPECT_EQ(int64_t(colOf(rb, 1, kUpdatedSlot)), uB);
    (void)cB;

    // (c) Update(o2), then Checkpoint(o3): the same values (the checkpoint changes only remaining and updated)
    Log c;
    c.control();
    c.control();
    c.data({update(1, V, up)});
    const int64_t cC = c.data({checkpoint(1, V, 555, ck)});
    const Replayed rc = replayLog(r, c);
    ASSERT_EQ(rc.table->size(), 1u);
    for (const uint16_t s : {0, 1, 2, 3, 4, 5, 6, 7}) EXPECT_EQ(colOf(rc, 1, s), colOf(rb, 1, s)) << "slot " << s;
    EXPECT_EQ(int64_t(colOf(rc, 1, kUpdatedSlot)), cC);

    // (d) a Checkpoint alone: the row, with the remaining
    Log d;
    d.control();
    d.control();
    const int64_t cD = d.data({checkpoint(1, V, 555, ck)});
    const Replayed rd = replayLog(r, d);
    ASSERT_EQ(rd.table->size(), 1u);
    EXPECT_EQ(int64_t(colOf(rd, 1, 0)), V);
    EXPECT_EQ(colOf(rd, 1, 3), 555u);
    EXPECT_EQ(colOf(rd, 1, 2), 3u);
    EXPECT_EQ(addrOf(rd, 1), "c3");
    EXPECT_EQ(int64_t(colOf(rd, 1, kGrantedSlot)), V);
    EXPECT_EQ(int64_t(colOf(rd, 1, kUpdatedSlot)), cD);

    // (e) update, then Revoke: no row
    Log e;
    e.control();
    e.control();
    e.data({update(1, V, up)});
    e.data({revoke(1, V)});
    EXPECT_EQ(replayLog(r, e).table->size(), 0u);
}

// ═══ Fix round 2 (R1): the log is self-describing; a replay equals the live row ═══

namespace {

std::vector<uint8_t> rowBytes(const uint8_t* row, const rune::view::RowLayout& l) { return std::vector<uint8_t>(row, row + l.rowLen); }

}  // namespace

TEST(LeaseJudgeTableRegression, LeaseReplayGrantKeptEqualsLiveRegression) {
    // Spec 6.4: an update's TTL is not checked; the judge keeps the row's TTL.  The Task 8 cleaner keeps each live key's grant
    // record, so a replay is of [grant, update] or [grant, checkpoint, update] and builds the row from the grant: it equals the
    // live row byte for byte (every slot, the null bits, the offsets).  (Update-only replay equality is Task 8's contract.)
    Rig r;
    r.start();
    LV gv; gv.ttl = 1000; gv.holder = 1; gv.addr = "g1";
    int64_t g = 0, up = 0;
    ASSERT_EQ(r.judgeOne(grant(1, gv), &g), 0);
    LV u500; u500.ttl = 500; u500.holder = 8; u500.addr = "u4"; u500.port = 9;
    ASSERT_EQ(r.judgeOne(update(1, g, u500), &up), 0) << "an update carrying another TTL is accepted";
    EXPECT_EQ(r.col(1, 1), 1000u) << "the live row keeps the grant's TTL";
    EXPECT_EQ(r.col(1, 3), 0u);
    const std::vector<uint8_t> live = rowBytes(r.row(1), r.index->layout);
    const Replayed whole = replayLog(r, r.log);
    ASSERT_EQ(whole.table->size(), 1u);
    EXPECT_EQ(rowBytes(whole.table->get(uint64_t(1)), whole.layout), live);
    EXPECT_EQ(colOf(whole, 1, 1), 1000u);

    // [grant, checkpoint, update]
    Rig r2;
    r2.start();
    int64_t g2 = 0, cp2 = 0, up2 = 0;
    ASSERT_EQ(r2.judgeOne(grant(1, gv), &g2), 0);
    ASSERT_EQ(r2.judgeOne(checkpoint(1, g2, 555, gv), &cp2), 0);
    ASSERT_EQ(r2.judgeOne(update(1, g2, u500), &up2), 0);
    ASSERT_EQ(r2.col(1, 3), 555u);
    EXPECT_EQ(r2.col(1, 1), 1000u);
    const std::vector<uint8_t> live2 = rowBytes(r2.row(1), r2.index->layout);
    const Replayed whole2 = replayLog(r2, r2.log);
    ASSERT_EQ(whole2.table->size(), 1u);
    EXPECT_EQ(rowBytes(whole2.table->get(uint64_t(1)), whole2.layout), live2);
    EXPECT_EQ(colOf(whole2, 1, 1), 1000u);
    EXPECT_EQ(colOf(whole2, 1, 3), 555u);
}

TEST(LeaseJudgeTableRegression, LeaseReplayTtlCheckpointThenUpdateRegression) {
    // Live: grant, checkpoint (the broker copies the row), update.  Compacted replay: checkpoint, update (the grant is gone).
    Rig r;
    r.start();
    LV gv; gv.ttl = 7000; gv.holder = 1; gv.addr = "g1";
    int64_t g = 0, cp = 0, up = 0;
    ASSERT_EQ(r.judgeOne(grant(1, gv), &g), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, g, 555, gv), &cp), 0);
    LV u; u.ttl = 7000; u.holder = 8; u.addr = "u4"; u.port = 3;
    ASSERT_EQ(r.judgeOne(update(1, g, u), &up), 0);
    const std::vector<uint8_t> live = rowBytes(r.row(1), r.index->layout);
    ASSERT_EQ(r.col(1, 3), 555u);

    Log c;
    for (int64_t i = 0; i < cp; ++i) c.control();
    ASSERT_EQ(c.data({checkpoint(1, g, 555, gv)}), cp);
    ASSERT_EQ(c.data({update(1, g, u)}), up);
    const Replayed rc = replayLog(r, c);
    ASSERT_EQ(rc.table->size(), 1u);
    EXPECT_EQ(rowBytes(rc.table->get(uint64_t(1)), rc.layout), live);

    // And the other order (update, then checkpoint) over the same live history.
    Rig r2;
    r2.start();
    int64_t g2 = 0, up2 = 0, cp2 = 0;
    ASSERT_EQ(r2.judgeOne(grant(1, gv), &g2), 0);
    ASSERT_EQ(r2.judgeOne(update(1, g2, u), &up2), 0);
    ASSERT_EQ(r2.judgeOne(checkpoint(1, g2, 555, u), &cp2), 0);
    const std::vector<uint8_t> live2 = rowBytes(r2.row(1), r2.index->layout);
    Log c2;
    for (int64_t i = 0; i < up2; ++i) c2.control();
    ASSERT_EQ(c2.data({update(1, g2, u)}), up2);
    ASSERT_EQ(c2.data({checkpoint(1, g2, 555, u)}), cp2);
    const Replayed rc2 = replayLog(r2, c2);
    ASSERT_EQ(rc2.table->size(), 1u);
    EXPECT_EQ(rowBytes(rc2.table->get(uint64_t(1)), rc2.layout), live2);
}

// ═══ Fix round 2 (R2): a version the spec refused is a definite answer ═══

TEST(LeaseJudgeTableRegression, LeaseRefusedVersionRefusesRegression) {
    Rig r;
    // v2 breaks the lease system-column layout (lease_ttl_ms first): installVersion refuses it.
    rune::codec::DescriptorBuilder bad("inst", 2);
    bad.add("lease_ttl_ms", P::UInt64).add("lease_version", P::Int64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, true).addFixedStr("addr", 16);
    ASSERT_FALSE(r.spec.installVersion(2, bad.build()));
    ASSERT_TRUE(r.spec.versionRefused(2));
    EXPECT_FALSE(r.spec.versionRefused(1));
    EXPECT_FALSE(r.spec.versionRefused(3)) << "a version that was never seen is not refused: the judge holds for it";
    r.start();
    const int64_t before = r.vs.judgedThrough();
    LV v2; v2.schema = 2; v2.withPort = false; v2.addr = "r";
    const int64_t first = r.append({grant(1, v2), grant(2)});
    ASSERT_GT(first, before - 1);
    const auto* e = r.st.rejected.find(first);
    ASSERT_NE(e, nullptr) << "the refused version's record was held or accepted";
    EXPECT_EQ(e->code, kInvalid);
    EXPECT_EQ(r.row(1), nullptr);
    EXPECT_EQ(r.st.rejected.find(first + 1), nullptr) << "the judge moved on to the next record";
    EXPECT_NE(r.row(2), nullptr);
    EXPECT_EQ(r.vs.judgedThrough(), first + 2);
    EXPECT_EQ(r.vs.stats().leaseLayoutWaits, 0u) << "a definite answer is not a wait";
    EXPECT_FALSE(r.vs.stalled());

    // A version past maxVersion that was never installed is still a hold, not a refusal.
    LV v3; v3.schema = 3; v3.withPort = false;
    const int64_t at = r.append({grant(3, v3)});
    EXPECT_EQ(r.vs.judgedThrough(), at);
    EXPECT_EQ(r.st.rejected.find(at), nullptr);
    EXPECT_EQ(r.vs.stats().leaseLayoutWaits, 1u);
}

TEST(LeaseJudgeTableRegression, LeaseCoverCacheAndCheckpointImageRegression) {
    const rune::codec::Descriptor ev = evolvingDesc(2, 2);
    const rune::view::RowLayout layout = leaseIndexRowLayout(ev);
    const rune::codec::Descriptor w1 = rune::codec::materializeAsOf(ev, 1);
    rune::view::ViewTable table(layout, 4096, kLeaseIndexChunkBytes);
    rune::view::Prospect prospect;
    prospect.reserve(layout, nullptr);
    LeaseCoverCache cache;
    const LeaseIndexFold f{table, layout, ev, 8, &cache};
    LV v1; v1.schema = 1; v1.withPort = false; v1.id = 1;
    const auto b1 = valueOf(v1);
    std::vector<uint8_t> key(8, 0);
    key[0] = 1;
    rune::view::SourceRecord put{sv(key), sv(b1), 5, rec::intentOf(kGrantA), &w1, kGrantA};
    ASSERT_EQ(prepareLeaseIndex(f, put, prospect, false, /*installSeq=*/7), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(cache.entries[1].installSeq, 7u) << "the proof is cached per writer version, keyed on its install sequence";
    EXPECT_EQ(cache.coverCacheFillsForTest(), 1u);
    ASSERT_EQ(prepareLeaseIndex(f, put, prospect, false, 7), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(cache.coverCacheFillsForTest(), 1u) << "the same install: no rebuild";
    // Another install of the same version (a new descriptor object with identical ids) is a new entry, even at the same address.
    const rune::codec::Descriptor w1b = w1;
    put.writer = &w1b;
    ASSERT_EQ(prepareLeaseIndex(f, put, prospect, false, 8), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(cache.entries[1].installSeq, 8u);
    EXPECT_EQ(cache.coverCacheFillsForTest(), 2u);
    // installSeq 0 never uses the cache.
    ASSERT_EQ(prepareLeaseIndex(f, put, prospect, false, 0), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(cache.coverCacheFillsForTest(), 2u);

    // A Checkpoint's image is built only for an absent row.
    rune::view::SourceRecord cp{sv(key), sv(b1), 6, 0, &w1, kCheckptA};
    ASSERT_EQ(prepareLeaseIndex(f, cp, prospect, false, 8), rune::view::PrepareStatus::Ok);
    EXPECT_NE(prospect.rows[0].image, nullptr) << "absent row: the checkpoint carries the image a compacted-log replay creates it from";
    LeaseEvent ev1;
    ASSERT_EQ(prepareLeaseIndex(f, put, prospect, false, 8), rune::view::PrepareStatus::Ok);
    LeaseIndexFold fm{table, layout, ev, 8, &cache};
    ASSERT_TRUE(applyLeaseRecord(fm, put, prospect, ev1));
    ASSERT_EQ(prepareLeaseIndex(f, cp, prospect, false, 8), rune::view::PrepareStatus::Ok);
    EXPECT_EQ(prospect.rows[0].image, nullptr) << "present row: no image is projected on the live path";
}

// A spec that reinstalls a version (identical field ids, a new descriptor object) gives it a new install sequence, and the judge's
// cover cache refills (it was keyed on the descriptor pointer before, which a reused address can alias).
TEST(LeaseJudgeTableRegression, LeaseCoverCacheKeyedOnInstallRegression) {
    Rig r;
    r.start();
    const uint32_t seq1 = r.spec.versionFor(1)->installSeq;
    ASSERT_NE(seq1, 0u);
    int64_t g = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g), 0);
    const uint64_t fills = r.index->leaseCover.coverCacheFillsForTest();
    EXPECT_GE(fills, 1u);
    ASSERT_EQ(r.judgeOne(update(1, g)), 0);
    EXPECT_EQ(r.index->leaseCover.coverCacheFillsForTest(), fills) << "same install: the entry is reused";
    ASSERT_TRUE(r.spec.installVersion(1, rune::codec::materializeAsOf(r.evolving, 1)));   // a new object, identical ids
    EXPECT_GT(r.spec.versionFor(1)->installSeq, seq1);
    ASSERT_EQ(r.judgeOne(update(1, g)), 0);
    EXPECT_EQ(r.index->leaseCover.coverCacheFillsForTest(), fills + 1) << "a reinstall rebuilds the entry";
}

// The writer's slot map (built once, cold) projects and re-encodes exactly like the per-field scan.
TEST(LeaseJudgeTableRegression, LeaseProjectionSlotMapRegression) {
    // evolving: system 0..3, id (key), addr (v1-v2, dropped at v3), port (since v2), flags (v1+), note (since v3)
    rune::codec::DescriptorBuilder b("inst", 3);
    b.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, /*isKey=*/true)
        .addFixedStrTagged("addr", 5, 16, 1, 3, false)
        .addTagged("port", 6, P::UInt32, 2, 0xFFFF, false, {})
        .addTagged("flags", 7, P::UInt16, 1, 0xFFFF, false, {})
        .addFixedStrTagged("note", 8, 5, 3, 0xFFFF, false);
    const rune::codec::Descriptor ev = b.build();
    const rune::view::RowLayout layout = leaseIndexRowLayout(ev);
    LeaseCoverCache cache;
    std::mt19937_64 rng(0x510700F);
    std::vector<uint8_t> viaMap(layout.rowLen), viaScan(layout.rowLen), value(70000), enc1(70000), enc2(70000);
    std::vector<uint8_t> row(layout.rowLen);
    for (int iter = 0; iter < 500; ++iter) {
        const uint16_t v = static_cast<uint16_t>(1 + iter % 3);
        const rune::codec::Descriptor w = rune::codec::materializeAsOf(ev, v);
        SCOPED_TRACE("iter " + std::to_string(iter) + " writer v" + std::to_string(v));
        rune::view::PrepareStatus st{};
        const uint16_t* map = leaseSlotMapFor(cache, v, /*installSeq=*/v, layout, ev, w, &st);
        ASSERT_EQ(st, rune::view::PrepareStatus::Ok);
        ASSERT_NE(map, nullptr);
        const std::size_t len = 4u + w.blockLength;
        for (std::size_t k = 0; k < len; ++k) value[k] = uint8_t(rng());
        const std::string_view sv2(reinterpret_cast<const char*>(value.data()), len);
        std::fill(viaMap.begin(), viaMap.end(), uint8_t{0xAA});
        std::fill(viaScan.begin(), viaScan.end(), uint8_t{0x55});
        ASSERT_EQ(projectLeaseRow(viaMap.data(), layout, ev, w, sv2, map), rune::view::PrepareStatus::Ok);
        ASSERT_EQ(projectLeaseRow(viaScan.data(), layout, ev, w, sv2, nullptr), rune::view::PrepareStatus::Ok);
        ASSERT_EQ(viaMap, viaScan) << "the slot-map projection is the scan's, byte for byte (null bits included)";
        // Re-encoding a random row (garbage under null slots) through the map equals the scan's.
        for (uint8_t& x : row) x = uint8_t(rng());
        const uint16_t n = static_cast<uint16_t>(layout.slotType.size() - kLeaseOffsetSlots);
        for (uint16_t s = 0; s < layout.slotType.size(); ++s) rune::view::clearNull(row.data(), layout, s);
        for (uint16_t s = kLeaseSystemSlots; s < n; ++s)
            if (rng() % 3 == 0) rune::view::setNull(row.data(), layout, s);
        const std::size_t m1 = encodeLeaseValue(row.data(), layout, ev, w, v, enc1.data(), enc1.size(), map);
        const std::size_t m2 = encodeLeaseValue(row.data(), layout, ev, w, v, enc2.data(), enc2.size(), nullptr);
        ASSERT_EQ(m1, len);
        ASSERT_EQ(m1, m2);
        ASSERT_EQ(std::memcmp(enc1.data(), enc2.data(), m1), 0);
    }
    EXPECT_EQ(cache.coverCacheFillsForTest(), 3u) << "one build per writer version, not per record";
    // A writer field the layout lacks is Pending, a type mismatch is Malformed; neither is cached.
    rune::codec::DescriptorBuilder small("inst", 1);
    small.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, true);
    const rune::codec::Descriptor smallEv = small.build();
    const rune::view::RowLayout smallLayout = leaseIndexRowLayout(smallEv);
    LeaseCoverCache c2;
    rune::view::PrepareStatus st{};
    EXPECT_EQ(leaseSlotMapFor(c2, 3, 9, smallLayout, smallEv, rune::codec::materializeAsOf(ev, 3), &st), nullptr);
    EXPECT_EQ(st, rune::view::PrepareStatus::Pending);
    EXPECT_EQ(c2.coverCacheFillsForTest(), 0u);
}

// ═══ encodeLeaseValue: the exact inverse of projectLeaseRow (lease topics Task 5b) ═══

TEST(LeaseEncodeRoundTripRegression, EncodeRoundTripRegression) {
    // evolving: system 0..3, id (key), addr (live v1-v2, dropped at v3), port (since v2), flags (v1+), note (since v3)
    rune::codec::DescriptorBuilder b("inst", 3);
    b.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, /*isKey=*/true)
        .addFixedStrTagged("addr", 5, 16, 1, 3, false)
        .addTagged("port", 6, P::UInt32, 2, 0xFFFF, false, {})
        .addTagged("flags", 7, P::UInt16, 1, 0xFFFF, false, {})
        .addFixedStrTagged("note", 8, 5, 3, 0xFFFF, false);
    const rune::codec::Descriptor ev = b.build();
    const rune::view::RowLayout layout = leaseIndexRowLayout(ev);
    const uint16_t n = static_cast<uint16_t>(layout.slotType.size() - kLeaseOffsetSlots);
    ASSERT_EQ(n, ev.fields.size());
    std::mt19937_64 rng(0xC0FFEE);
    std::vector<uint8_t> row(layout.rowLen), value(70000), back(layout.rowLen);
    for (int iter = 0; iter < 500; ++iter) {
        for (uint8_t& x : row) x = uint8_t(rng());   // garbage everywhere, including the bytes of null slots
        for (uint16_t s = 0; s < layout.slotType.size(); ++s) rune::view::clearNull(row.data(), layout, s);
        for (uint16_t s = rune::node::kLeaseSystemSlots; s < n; ++s)
            if (rng() % 3 == 0) rune::view::setNull(row.data(), layout, s);
        for (uint16_t v = 1; v <= 3; ++v) {
            SCOPED_TRACE("iter " + std::to_string(iter) + " writer v" + std::to_string(v));
            const rune::codec::Descriptor w = rune::codec::materializeAsOf(ev, v);
            const std::size_t m = encodeLeaseValue(row.data(), layout, ev, w, v, value.data(), value.size());
            ASSERT_EQ(m, 4u + w.blockLength);
            EXPECT_EQ(value[2] | (value[3] << 8), v);
            EXPECT_EQ(value[0] | (value[1] << 8), w.blockLength);
            ASSERT_EQ(rune::node::projectLeaseRow(back.data(), layout, ev, w, std::string_view(reinterpret_cast<const char*>(value.data()), m)),
                      rune::view::PrepareStatus::Ok);
            for (uint16_t s = 0; s < n; ++s) {
                const bool covered = std::any_of(w.fields.begin(), w.fields.end(), [&](const auto& f) { return f.id == ev.fields[s].id; });
                if (!covered) {
                    EXPECT_TRUE(rune::view::isNull(back.data(), layout, s)) << "slot " << s << " is not in the writer";
                } else if (rune::view::isNull(row.data(), layout, s)) {
                    EXPECT_FALSE(rune::view::isNull(back.data(), layout, s)) << "slot " << s;
                    for (uint32_t k = 0; k < layout.slotWidth[s]; ++k) ASSERT_EQ(back[layout.slotOffset[s] + k], 0) << "a null slot encodes as zeros, slot " << s;
                } else {
                    EXPECT_FALSE(rune::view::isNull(back.data(), layout, s));
                    ASSERT_EQ(std::memcmp(back.data() + layout.slotOffset[s], row.data() + layout.slotOffset[s], layout.slotWidth[s]), 0) << "slot " << s;
                }
            }
            // too small a capacity writes nothing and answers 0
            EXPECT_EQ(encodeLeaseValue(row.data(), layout, ev, w, v, value.data(), m - 1), 0u);
        }
    }
}

// A null Enum slot (a column the row never had) is re-encoded as the field's first whitelisted value, not zero: zero may not be in
// this version's whitelist and the stored revoke must pass the Validator.  Every other null stays zeros.
TEST(LeaseEncodeRoundTripRegression, NullEnumEncodesFirstWhitelistedValueRegression) {
    rune::codec::DescriptorBuilder b("inst", 2);
    b.add("lease_version", P::Int64).add("lease_ttl_ms", P::UInt64).add("lease_holder", P::UInt64).add("lease_remaining_ms", P::UInt64)
        .add("id", P::UInt64, 1, /*isKey=*/true)
        .add("kind", P::Enum, 2, false, {3, 7})
        .add("flags", P::UInt16, 2);
    const rune::codec::Descriptor ev = b.build();
    const rune::view::RowLayout layout = leaseIndexRowLayout(ev);
    const rune::codec::Descriptor w = rune::codec::materializeAsOf(ev, 2);
    std::vector<uint8_t> row(layout.rowLen, 0), value(4096);
    for (uint16_t s = 0; s < layout.slotType.size(); ++s) rune::view::clearNull(row.data(), layout, s);
    rune::view::setNull(row.data(), layout, 5);   // kind
    rune::view::setNull(row.data(), layout, 6);   // flags
    const std::size_t m = encodeLeaseValue(row.data(), layout, ev, w, 2, value.data(), value.size());
    ASSERT_EQ(m, 4u + w.blockLength);
    const auto& kind = *std::find_if(w.fields.begin(), w.fields.end(), [](const auto& f) { return f.name == "kind"; });
    const auto& flags = *std::find_if(w.fields.begin(), w.fields.end(), [](const auto& f) { return f.name == "flags"; });
    EXPECT_EQ(value[4 + kind.offset], 3) << "the first whitelisted value";
    EXPECT_EQ(value[4 + flags.offset], 0) << "any other null is zeros";
    const rune::codec::ValResult ok = rune::codec::Validator(w).validate({value.data(), m});
    EXPECT_TRUE(ok.ok) << (ok.reason != nullptr ? ok.reason : "");
    // a non-null Enum is copied as it is
    rune::view::clearNull(row.data(), layout, 5);
    const uint32_t seven = 7;
    std::memcpy(row.data() + layout.slotOffset[5], &seven, 4);
    ASSERT_EQ(encodeLeaseValue(row.data(), layout, ev, w, 2, value.data(), value.size()), m);
    EXPECT_EQ(value[4 + kind.offset], 7);
}


// ═══ Task 29: the anchor list, the Carry, the fence record ═══

namespace {

constexpr uint8_t kCarryA = 0;   // Put | intent 0
LRec carry(uint64_t id, int64_t ver, LV v = {}) { v.id = id; v.ver = ver; return {id, kCarryA, valueOf(v)}; }

// The live row of `id` as the value a broker Carry would re-append.
LV liveValueOf(const Rig& r, uint64_t id) {
    LV v;
    v.id = id;
    v.ver = int64_t(r.col(id, 0));
    v.ttl = r.col(id, 1);
    v.holder = r.col(id, 2);
    v.rem = r.col(id, 3);
    v.addr = r.addr(id);
    v.port = r.col32(id, kPortSlot);
    return v;
}
LRec carryOfLive(const Rig& r, uint64_t id) { return carry(id, int64_t(r.col(id, 0)), liveValueOf(r, id)); }

uint32_t recBytes(const LRec& r) { return kLeaseAnchorRecordOverhead + uint32_t(r.keyLen) + uint32_t(r.value.size()); }

struct Anchored { int64_t anchor; uint32_t bytes; };
using AnchorModel = std::map<uint64_t, Anchored>;

// Walks the list head -> tail; checks every invariant against the model.  `idSlotOffset`: the key column's row offset.
void expectList(const rune::view::ViewTable& t, const rune::view::RowLayout& layout, const LeaseTransientState& ts, const AnchorModel& model) {
    const auto u32 = [&](uint32_t rid, std::size_t off) { uint32_t v; std::memcpy(&v, t.aux(rid) + off, 4); return v; };
    uint32_t rid = ts.anchors.head, prev = kLeaseAnchorNil;
    int64_t last = -1;
    uint64_t bytes = 0;
    std::map<uint64_t, Anchored> seen;
    std::size_t steps = 0;
    while (rid != kLeaseAnchorNil) {
        ASSERT_LE(++steps, model.size() + 1) << "the list loops or is longer than the model";
        ASSERT_EQ(u32(rid, 20), prev) << "prev mirrors next";
        const int64_t anchor = leaseAnchorOf(t, rid);
        ASSERT_GT(anchor, last) << "strictly increasing anchors";
        last = anchor;
        uint64_t key;
        std::memcpy(&key, t.rowOf(rid) + layout.slotOffset[kIdSlot], 8);
        seen[key] = {anchor, leaseAnchorBytesOf(t, rid)};
        bytes += leaseAnchorBytesOf(t, rid);
        prev = rid;
        rid = u32(rid, 24);
    }
    ASSERT_EQ(ts.anchors.tail, prev);
    ASSERT_EQ(seen.size(), model.size());
    for (const auto& [k, m] : model) {
        const auto it = seen.find(k);
        ASSERT_NE(it, seen.end()) << "key " << k;
        EXPECT_EQ(it->second.anchor, m.anchor) << "key " << k;
        EXPECT_EQ(it->second.bytes, m.bytes) << "key " << k;
    }
    EXPECT_EQ(ts.anchors.count, model.size());
    EXPECT_EQ(ts.anchors.liveBytes, bytes);
    int64_t minAnchor = INT64_MAX;
    uint64_t modelBytes = 0;
    for (const auto& [k, m] : model) { minAnchor = std::min(minAnchor, m.anchor); modelBytes += m.bytes; }
    EXPECT_EQ(leaseAnchorHead(t, ts.anchors), minAnchor);
    EXPECT_EQ(ts.anchors.liveBytes, modelBytes);
    EXPECT_EQ(t.size(), model.size());
}

// Replays the whole log of `r` into a fresh table with a fresh transient state.
struct TransientReplay {
    rune::view::RowLayout                  layout;
    std::unique_ptr<rune::view::ViewTable> table;
    LeaseTransientState                    ts;
    int64_t                                cursor = 0;
    TransientReplay(Rig& r, int64_t from, bool withState = true) : layout(r.index->layout) {
        rune::view::ViewTableKeys keys{8, 0, 0, ~uint64_t{0}, kLeaseTransientAuxBytes};
        table = std::make_unique<rune::view::ViewTable>(layout, 4096, kLeaseIndexChunkBytes, keys);
        ts.transient = true;
        rune::view::Prospect prospect;
        prospect.reserve(layout, nullptr);
        LeaseIndexFold f{*table, layout, r.evolving, 8, nullptr, withState ? &ts : nullptr};
        const rune::view::FoldSkip skip{.aborted = {}, .rejected = &r.st.rejected};
        cursor = from;
        (void)foldLeaseIndexBatch(f, r.spec, prospect, r.log.read(from), r.log.next, cursor, skip);
    }
    [[nodiscard]] uint64_t col(uint64_t id, uint16_t slot) const {
        const uint8_t* row = table->get(id);
        uint64_t v = 0;
        if (row != nullptr) std::memcpy(&v, row + layout.slotOffset[slot], 8);
        return v;
    }
    [[nodiscard]] uint32_t rid(uint64_t id) const { return Rig::ridIn(*table, id); }
};

}  // namespace

TEST(LeaseCarryJudgeRegression, JudgeTableRegression) {
    const uint8_t carryAttrs = rec::withLeaseOp(0, rec::LeaseOp::Put);
    constexpr uint64_t kTtl = 100;
    EXPECT_EQ(judgeLease({false, 0, 0}, carryAttrs, 7, 0, 0, 10), kStale);        // absent
    EXPECT_EQ(judgeLease({true, 7, kTtl}, carryAttrs, 7, 0, 1, 10), 0);           // present, same version
    EXPECT_EQ(judgeLease({true, 7, kTtl}, carryAttrs, 8, 0, 1, 10), kStale);      // present, different version
    EXPECT_EQ(judgeLease({true, 7, kTtl}, carryAttrs, 7, 4000, 1, 10), 0);        // a checkpointed row's carry (remaining > 0)
    EXPECT_EQ(judgeLease({true, 7, kTtl}, carryAttrs, 7, 0, 10, 10), 0);          // the live-key limit is not consulted
}

TEST(LeaseCarryJudgeRegression, LiveCarryRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
    r.start();
    int64_t g = 0, ck = 0, c1 = 0, c2 = 0, c3 = 0, c4 = 0;
    LV a; a.ttl = 5000; a.holder = 9; a.addr = "carried"; a.port = 77;
    ASSERT_EQ(r.judgeOne(grant(1, a), &g), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, g, 3000), &ck), 0);
    ASSERT_EQ(r.col(1, 3), 3000u);
    const std::size_t rowLen = r.index->layout.rowLen;
    const std::vector<uint8_t> before(r.row(1), r.row(1) + rowLen);
    const std::size_t events = r.st.leaseEvents.buf.size();

    // a carry of the live row: accepted, the row byte-identical, the anchor moves, a Carried event
    ASSERT_EQ(r.judgeOne(carryOfLive(r, 1), &c1), 0);
    EXPECT_EQ(r.st.rejected.find(c1), nullptr);
    EXPECT_EQ(std::memcmp(before.data(), r.row(1), rowLen), 0) << "a Carry modifies no column, no offset column";
    EXPECT_EQ(int64_t(r.col(1, kUpdatedSlot)), ck) << "lease_updated_offset is the checkpoint's, not the carry's";
    EXPECT_EQ(r.col(1, 3), 3000u);
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.ridOf(1)), c1);
    ASSERT_EQ(r.st.leaseEvents.buf.size(), events + 1);
    const LeaseEvent& ev = r.st.leaseEvents.buf.back();
    EXPECT_EQ(ev.kind, LeaseEventKind::Carried);
    EXPECT_EQ(ev.rid, r.ridOf(1));
    EXPECT_EQ(ev.version, g);
    EXPECT_EQ(ev.offset, c1);
    EXPECT_EQ(ev.remainingMs, 3000u);
    EXPECT_EQ(ev.ttlMs, 5000u);

    // another version: stale, BrokerRejected, nothing moves
    const std::size_t events2 = r.st.leaseEvents.buf.size();
    EXPECT_EQ(r.judgeOne(carry(1, g + 1, liveValueOf(r, 1)), &c2), kStale);
    EXPECT_EQ(std::memcmp(before.data(), r.row(1), rowLen), 0);
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.ridOf(1)), c1);
    ASSERT_EQ(r.st.leaseEvents.buf.size(), events2 + 1);
    EXPECT_EQ(r.st.leaseEvents.buf.back().kind, LeaseEventKind::BrokerRejected);

    // an absent key
    EXPECT_EQ(r.judgeOne(carry(2, 5), &c3), kStale);
    EXPECT_EQ(r.row(2), nullptr);

    // a malformed carry of a present key (value cut below the system columns): stale, not InvalidRequest
    LRec bad = carryOfLive(r, 1);
    bad.value.resize(4 + rec::kLeaseSystemBytes - 1);
    EXPECT_EQ(r.judgeOne(bad, &c4), kStale);
    EXPECT_EQ(std::memcmp(before.data(), r.row(1), rowLen), 0);
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.ridOf(1)), c1);
    EXPECT_EQ(r.index->leaseTransient.anchors.count, 1u);
}

TEST(LeaseAnchorListRegression, RandomSequencesMatchModelRegression) {
    for (uint32_t seed = 1; seed <= 200; ++seed) {
        SCOPED_TRACE(seed);
        std::mt19937_64 rng(seed);
        Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
        r.start();
        AnchorModel model;
        std::map<uint64_t, int64_t> versionOf;
        for (int op = 0; op < 300; ++op) {
            const uint64_t id = 1 + rng() % 32;
            const bool present = model.count(id) != 0;
            const int64_t right = present ? versionOf[id] : 5, ver = (rng() % 4 != 0) ? right : right + 1 + int64_t(rng() % 50);
            LV v; v.addr = "s" + std::to_string(op); v.holder = rng() % 5;
            LRec rec1;
            enum Kind { Grant, Update, Carry, Revoke, Expire, Checkpoint } kind = Kind(rng() % 6);
            switch (kind) {
            case Grant:      rec1 = grant(id, v); break;
            case Update:     rec1 = update(id, ver, v); break;
            case Carry: {
                LV lv = present ? liveValueOf(r, id) : v;
                rec1 = carry(id, ver, lv);
                break;
            }
            case Revoke:     rec1 = revoke(id, ver); break;
            case Expire:     rec1 = expire(id, ver); break;
            case Checkpoint: rec1 = checkpoint(id, ver, 1 + rng() % 4000, v); break;
            }
            int64_t off = 0;
            const uint16_t code = r.judgeOne(rec1, &off);
            if (code == 0) {
                switch (kind) {
                case Grant:      model[id] = {off, recBytes(rec1)}; versionOf[id] = off; break;
                case Update:
                case Carry:      model[id] = {off, recBytes(rec1)}; break;
                case Revoke:
                case Expire:     model.erase(id); break;
                case Checkpoint: break;   // a checkpoint of a present row does not move the anchor
                }
            }
            expectList(r.index->table, r.index->layout, r.index->leaseTransient, model);
            if (::testing::Test::HasFatalFailure()) return;
            if (::testing::Test::HasFailure()) { ADD_FAILURE() << "op " << op << " kind " << int(kind) << " code " << code; return; }
        }
        // the whole log replayed into a fresh table builds the same list
        TransientReplay rp(r, 0);
        ASSERT_EQ(rp.table->size(), model.size());
        expectList(*rp.table, rp.layout, rp.ts, model);
        if (::testing::Test::HasFailure()) return;
        for (const auto& [k, m] : model) EXPECT_EQ(rp.col(k, 0), r.col(k, 0)) << "version of key " << k;
    }
}

TEST(LeaseAnchorListRegression, ReusedRowIdStartsUnlinkedRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
    r.start();
    int64_t gA = 0, gB = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &gA), 0);
    const uint32_t ridA = r.ridOf(1);
    ASSERT_EQ(r.judgeOne(revoke(1, gA)), 0);
    ASSERT_EQ(r.judgeOne(grant(2), &gB), 0);
    ASSERT_EQ(r.ridOf(2), ridA) << "B reuses A's row id";
    const LeaseAnchorList& l = r.index->leaseTransient.anchors;
    EXPECT_EQ(l.count, 1u);
    EXPECT_EQ(l.head, ridA);
    EXPECT_EQ(l.tail, ridA);
    EXPECT_EQ(leaseAnchorOf(r.index->table, ridA), gB);
    EXPECT_EQ(leaseAnchorHead(r.index->table, l), gB);
    AnchorModel model{{2, {gB, kLeaseAnchorRecordOverhead + 8 + uint32_t(valueOf({}).size())}}};
    expectList(r.index->table, r.index->layout, r.index->leaseTransient, model);
}

TEST(LeaseAnchorListRegression, PersistedKeepsEightBytesRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Persisted);
    r.start();
    EXPECT_EQ(r.index->table.auxBytesPerRow(), kLeaseAuxBytes);
    EXPECT_EQ(kLeaseAuxBytes, 8u);
    int64_t g = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g), 0);
    ASSERT_EQ(r.judgeOne(update(1, g)), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, g, 100)), 0);
    ASSERT_EQ(r.judgeOne(carryOfLive(r, 1)), 0);
    const LeaseAnchorList& l = r.index->leaseTransient.anchors;
    EXPECT_EQ(l.count, 0u);
    EXPECT_EQ(l.liveBytes, 0u);
    EXPECT_EQ(l.head, kLeaseAnchorNil);
    EXPECT_EQ(leaseAnchorOf(r.index->table, r.ridOf(1)), -1);
    EXPECT_EQ(leaseLastAppended(r.index->table, r.ridOf(1)), -1);
    setLeaseLastAppended(r.index->table, r.ridOf(1), 1234);
    EXPECT_EQ(leaseLastAppended(r.index->table, r.ridOf(1)), 1234);
    EXPECT_EQ(leaseAnchorHead(r.index->table, l), INT64_MAX);
}

TEST(LeaseCarryReplayRegression, RingAfterGrantRebuildsRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
    r.start();
    int64_t gA = 0, ck = 0, cr1 = 0, gB = 0, u2 = 0;
    LV a; a.ttl = 7000; a.holder = 11; a.addr = "ring"; a.port = 4242;
    ASSERT_EQ(r.judgeOne(grant(1, a), &gA), 0);
    ASSERT_EQ(r.judgeOne(checkpoint(1, gA, 2500), &ck), 0);
    ASSERT_EQ(r.judgeOne(carryOfLive(r, 1), &cr1), 0);
    ASSERT_EQ(r.judgeOne(grant(2), &gB), 0);
    const uint32_t bytesA = recBytes(carryOfLive(r, 1)), bytesB = recBytes(grant(2));

    const auto check = [&](const TransientReplay& rp, const char* what) {
        SCOPED_TRACE(what);
        ASSERT_NE(rp.table->get(uint64_t(1)), nullptr);
        EXPECT_EQ(int64_t(rp.col(1, 0)), gA) << "version == the grant offset";
        EXPECT_EQ(rp.col(1, 1), 7000u);
        EXPECT_EQ(rp.col(1, 2), 11u);
        EXPECT_EQ(rp.col(1, 3), 2500u);
        EXPECT_EQ(int64_t(rp.col(1, kGrantedSlot)), gA);
        const uint8_t* row = rp.table->get(uint64_t(1));
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(row + rp.layout.slotOffset[kAddrSlot]), 4), "ring");
        uint32_t port; std::memcpy(&port, row + rp.layout.slotOffset[kPortSlot], 4);
        EXPECT_EQ(port, 4242u);
        EXPECT_EQ(leaseAnchorOf(*rp.table, rp.rid(1)), cr1);
        EXPECT_EQ(leaseAnchorOf(*rp.table, rp.rid(2)), gB);
        EXPECT_EQ(rp.ts.anchors.head, rp.rid(1));
        EXPECT_EQ(rp.ts.anchors.tail, rp.rid(2));
        EXPECT_EQ(rp.ts.anchors.liveBytes, uint64_t(bytesA) + bytesB);
    };
    const TransientReplay fromCarry(r, cr1);
    check(fromCarry, "ring starts at the carry");
    EXPECT_EQ(int64_t(fromCarry.col(1, kUpdatedSlot)), cr1);
    const TransientReplay fromZero(r, 0);
    check(fromZero, "replay from 0");
    EXPECT_EQ(fromZero.col(1, 3), fromCarry.col(1, 3));

    // carry, then an update of the holder: the ring starts at the carry, the update moves the anchor
    LV h2 = liveValueOf(r, 1); h2.holder = 99;
    ASSERT_EQ(r.judgeOne(update(1, gA, h2), &u2), 0);
    const TransientReplay again(r, cr1);
    EXPECT_EQ(again.col(1, 2), 99u);
    EXPECT_EQ(leaseAnchorOf(*again.table, again.rid(1)), u2);
    EXPECT_EQ(again.ts.anchors.tail, again.rid(1));
}

namespace {
LRec fenceRec(uint64_t lineage, uint64_t ttl, uint64_t remaining) {
    LV v; v.ttl = ttl;
    return checkpoint(lineage, rec::kLeaseFenceVersion, remaining, v);
}
}  // namespace

TEST(LeaseFenceRecordRegression, LiveAndReplayRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
    r.start();
    constexpr uint64_t kLineage = 0x1122334455667788ull;
    int64_t g = 0;
    ASSERT_EQ(r.judgeOne(grant(1), &g), 0);
    const uint64_t malformed = r.vs.stats().keyIndexSkippedMalformed;
    const std::size_t events = r.st.leaseEvents.buf.size();
    const std::size_t size = r.index->table.size();

    int64_t f1 = 0, f2 = 0;
    EXPECT_EQ(r.judgeOne(fenceRec(kLineage, 5000, 3000), &f1), 0);
    EXPECT_EQ(r.st.rejected.find(f1), nullptr);
    EXPECT_EQ(r.index->table.size(), size);
    EXPECT_EQ(r.st.leaseEvents.buf.size(), events) << "a fence record changes nothing in the wheels";
    EXPECT_EQ(r.vs.stats().keyIndexSkippedMalformed, malformed);
    const LeaseFenceState& fe = r.index->leaseTransient.fence;
    EXPECT_TRUE(fe.present);
    EXPECT_EQ(fe.lineage, kLineage);
    EXPECT_EQ(fe.ttlMs, 5000u);
    EXPECT_EQ(fe.remainingMs, 3000u);
    EXPECT_EQ(fe.offset, f1);
    EXPECT_EQ(r.index->leaseTransient.fenceMalformed, 0u);

    EXPECT_EQ(r.judgeOne(fenceRec(kLineage, 5000, 0), &f2), 0);
    EXPECT_EQ(fe.remainingMs, 0u);
    EXPECT_EQ(fe.offset, f2);
    EXPECT_EQ(r.row(kLineage), nullptr);

    TransientReplay rp(r, 0);
    EXPECT_TRUE(rp.ts.fence.present);
    EXPECT_EQ(rp.ts.fence.lineage, kLineage);
    EXPECT_EQ(rp.ts.fence.ttlMs, 5000u);
    EXPECT_EQ(rp.ts.fence.remainingMs, 0u);
    EXPECT_EQ(rp.ts.fence.offset, f2);
    EXPECT_EQ(rp.ts.fenceMalformed, 0u);
    EXPECT_EQ(rp.table->size(), 1u);

    // the fold's key-width check does not apply to a fence record: a fold over a 16-byte key index takes it too
    {
        rune::view::ViewTableKeys keys{16, 0, 0, ~uint64_t{0}, kLeaseTransientAuxBytes};
        rune::view::ViewTable table(r.index->layout, 4096, kLeaseIndexChunkBytes, keys);
        LeaseTransientState ts; ts.transient = true;
        rune::view::Prospect prospect;
        prospect.reserve(r.index->layout, nullptr);
        LeaseIndexFold f{table, r.index->layout, r.evolving, 16, nullptr, &ts};
        const rune::view::FoldSkip skip{.aborted = {}, .rejected = &r.st.rejected};
        int64_t cursor = f1;
        const auto st = foldLeaseIndexBatch(f, r.spec, prospect, r.log.read(f1), r.log.next, cursor, skip);
        EXPECT_EQ(st.skippedMalformed, 0u);
        EXPECT_EQ(table.size(), 0u);
        EXPECT_TRUE(ts.fence.present);
        EXPECT_EQ(ts.fence.offset, f2);
    }
}

TEST(LeaseFenceRecordRegression, PersistedIgnoredRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Persisted);
    r.start();
    int64_t f = 0;
    EXPECT_EQ(r.judgeOne(fenceRec(0xABCDull, 5000, 3000), &f), 0);
    EXPECT_EQ(r.st.rejected.find(f), nullptr);
    EXPECT_EQ(r.index->table.size(), 0u);
    EXPECT_FALSE(r.index->leaseTransient.fence.present);
    EXPECT_EQ(r.index->leaseTransient.fenceIgnored, 1u);
    EXPECT_EQ(r.index->leaseTransient.fenceMalformed, 0u);
}

TEST(LeaseFenceRecordRegression, MalformedRegression) {
    Rig r(100, evolvingDesc(), 0, Rig::Mode::Transient);
    r.start();
    LRec bad = fenceRec(0x77, 5000, 3000);
    bad.keyLen = 4;
    int64_t f = 0;
    EXPECT_EQ(r.judgeOne(bad, &f), 0);
    EXPECT_EQ(r.st.rejected.find(f), nullptr);
    EXPECT_EQ(r.index->leaseTransient.fenceMalformed, 1u);
    EXPECT_FALSE(r.index->leaseTransient.fence.present);
    EXPECT_EQ(r.index->table.size(), 0u);
    EXPECT_EQ(r.index->leaseTransient.fenceIgnored, 0u);
}
