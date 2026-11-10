// LeaseWriteHookTest.cpp - lease topics Task 5a: the lease branch of CheckedWriteHook (spec 6.4 refusals, grant stamping,
// the TTL raise).  Unit level on the hook with a real InMemoryLog; the worker leg is in LeaseJudgeWorkerTest.
#include "support/CheckedWritesBatches.h"
#include "support/LeaseHookRig.h"

#include <gtest/gtest.h>

#include <rune/storage/ProducePath.h>
#include <rune/storage/ProducePathImpl.h>
#include <rune/storage/ProducerStateManager.h>

#include <functional>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::leasehook;
namespace pc = rune::protocol::client;

namespace {

constexpr ErrorCode IR  = ErrorCode::InvalidRequest;
constexpr ErrorCode OKC = ErrorCode::OK;
constexpr ErrorCode TTL = ErrorCode::LeaseTtlTooLarge;
constexpr ErrorCode NLE = ErrorCode::NamespaceLimitExceeded;
using H = HookRefusal;

uint64_t maxTtl() { return rune::protocol::LeaseSpec{}.maxTtlMs; }
uint64_t minTtl() { return rune::protocol::LeaseSpec{}.minTtlMs; }

struct Row {
    const char*                     name;
    std::function<void(Rig&)>       setup;
    std::function<std::vector<uint8_t>()> batch;
    ErrorCode                       code;
    H                               why;   // checked only when not None
};

std::vector<uint8_t> one(const LRec& r) { return batchOf({r}); }
LRec withAttrs(LRec r, uint8_t attrs) { r.attrs = attrs; return r; }
LRec withValue(LRec r, std::vector<uint8_t> v) { r.value = std::move(v); return r; }
std::vector<uint8_t> manyGrants(int n) {
    std::vector<LRec> recs;
    for (int i = 0; i < n; ++i) recs.push_back(grantOf(uint64_t(i + 1)));
    return batchOf(recs);
}
std::vector<uint8_t> manyUpdates(int n) {
    std::vector<LRec> recs;
    for (int i = 0; i < n; ++i) recs.push_back(updateOf(uint64_t(i + 1), 3));
    return batchOf(recs);
}

// A single-record batch whose value length is -1 (a null value).  Built from a record with a 1-byte value: the value-length
// varint becomes zigzag(-1) = 1, the value byte goes, and the record length, batch length and CRC follow.
std::vector<uint8_t> nullValueBatch(uint8_t attrs) {
    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    b.append(keyOf(1), "x", 1'000'000, attrs);
    auto v = b.build();
    namespace pb = rune::protocol::batch;
    const std::size_t rec0 = pb::kRecordBatchOverhead;
    // [len][attrs][tsDelta][offsetDelta][keyLen][key 8][valueLen][value][headerCount]
    const std::size_t vlen = rec0 + 1 + 1 + 1 + 1 + 1 + 8;
    EXPECT_EQ(v[vlen], 0x02) << "valueLen 1, zigzag";
    v[vlen] = 0x01;                                  // valueLen -1
    v.erase(v.begin() + std::ptrdiff_t(vlen + 1));   // no value byte
    v[rec0] = uint8_t(v[rec0] - 2);                  // record length 15 -> 14 (zigzag 30 -> 28)
    putLe(v, 8, uint64_t(v.size() - 12), 4);
    putLe(v, pb::kCrcOffset, rune::protocol::crc32c(v.data() + pb::kAttributesOffset, v.size() - pb::kAttributesOffset), 4);
    return v;
}

struct Produced {
    ErrorCode   code = ErrorCode::OK;
    H           why  = H::None;
    int64_t     base = -1;
};

// The real produce core with the hook: dedupe, check, stage, append, onAppended.
Produced produce(Rig& r, const std::vector<uint8_t>& batch) {
    rune::storage::ProducerStateManager psm;
    CheckedWriteHook hook(r.state, r.facts, r.scratch);
    const auto out = rune::storage::handleProduceBatch<rune::storage::InMemoryLog, CheckedWriteHook>(
        batch.data(), batch.size(), 1, 0, -1, 0, -1, r.tlog.log, psm, false, 0, 1, 1, false, hook);
    return {out.error, hook.refusal(), out.baseOffset};
}

}  // namespace

TEST(LeaseWriteHookRegression, RefusalTableRegression) {
    const auto none = [](Rig&) {};
    const auto withIdx = [](Rig& r) { r.withIndex(); };   // a revoke needs a ready lease index (else Throttled)
    const uint8_t kRevokeInsert = 1 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);
    const uint8_t kExpireC = uint8_t(rec::LeaseOp::Expire) << rec::kAttrLeaseOpShift;
    const uint8_t kCheckptC = uint8_t(rec::LeaseOp::Checkpoint) << rec::kAttrLeaseOpShift;
    const std::vector<Row> rows = {
        // the refusals, one per rule, in the spec's order
        {"client version 17", [](Rig& r) { r.facts.clientVersion = 17; }, [] { return one(grantOf(1)); }, IR, H::LeaseClientVersion},
        {"client version 0", [](Rig& r) { r.facts.clientVersion = 0; }, [] { return one(grantOf(1)); }, IR, H::LeaseClientVersion},
        {"level 5", [](Rig& r) { r.facts.levelLeases = false; }, [] { return one(grantOf(1)); }, IR, H::LeaseClientVersion},
        {"acks without View", [](Rig& r) { r.facts.acks = pc::kAcksWireAll; }, [] { return one(grantOf(1)); }, IR, H::LeaseNeedsView},
        {"acks none", [](Rig& r) { r.facts.acks = 0; }, [] { return one(grantOf(1)); }, IR, H::LeaseNeedsView},
        {"transactional", [](Rig& r) { r.facts.transactional = true; }, [] { return one(grantOf(1)); }, IR, H::LeaseInTxn},
        {"2049 records", none, [] { return manyGrants(2049); }, IR, H::LeaseBatchTooLarge},
        {"put upsert", none, [&] { return one(withAttrs(grantOf(1), 0)); }, IR, H::LeaseBadRecord},
        {"put intent 3", none, [&] { return one(withAttrs(grantOf(1), 3)); }, IR, H::LeaseBadRecord},
        {"revoke insert", none, [&] { return one(withAttrs(revokeOf(1, 5), kRevokeInsert)); }, IR, H::LeaseBadRecord},
        {"client expire", none, [&] { return one(withAttrs(updateOf(1, 5), kExpireC)); }, IR, H::LeaseBadRecord},
        {"client checkpoint", none, [&] { return one(withAttrs(updateOf(1, 5), kCheckptC)); }, IR, H::LeaseBadRecord},
        {"non-idempotent write", [](Rig& r) { r.facts.idempotent = false; }, [] { return one(grantOf(1)); }, IR, H::ExplicitKeyNeedsIdempotentAndView},
        {"null value (grant)", none, [] { return nullValueBatch(kGrantA); }, IR, H::None},
        {"null value (update)", none, [] { return nullValueBatch(kUpdateA); }, IR, H::LeaseBadRecord},
        {"empty value", none, [&] { return one(withValue(grantOf(1), {})); }, IR, H::None},
        {"empty value (update)", none, [&] { return one(withValue(updateOf(1, 5), {})); }, IR, H::LeaseBadRecord},
        {"35-byte value", none, [&] { auto v = leaseValue(1, 0, 5000); v.resize(35); return one(withValue(grantOf(1), v)); }, IR, H::LeaseBadRecord},
        {"remaining 1 (grant)", none, [] { return one({1, kGrantA, leaseValue(1, 0, 5000, 1)}); }, IR, H::LeaseBadRecord},
        {"remaining 1 (update)", none, [] { return one({1, kUpdateA, leaseValue(1, 5, 5000, 1)}); }, IR, H::LeaseBadRecord},
        {"update version -1", none, [] { return one(updateOf(1, -1)); }, IR, H::LeaseBadRecord},
        {"revoke version -1", none, [] { return one(revokeOf(1, -1)); }, IR, H::LeaseBadRecord},
        {"grant ttl max+1", none, [] { return one(grantOf(1, maxTtl() + 1)); }, TTL, H::LeaseTtlTooLarge},
        {"grant while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(grantOf(1)); }, NLE, H::LeaseViewMemOver},
        {"grant while leasesOver", [](Rig& r) { r.facts.leasesOver = true; }, [] { return one(grantOf(1)); }, NLE, H::LeaseMaxLeases},
        {"grant while viewMemOver AND leasesOver", [](Rig& r) { r.facts.viewMemOver = true; r.facts.leasesOver = true; }, [] { return one(grantOf(1)); }, NLE, H::LeaseViewMemOver},
        {"bit 4 set", none, [] { return one(withAttrs(grantOf(1), kGrantA | 0x10)); }, IR, H::ReservedBits},
        {"bit 7 set", none, [] { return one(withAttrs(updateOf(1, 5), kUpdateA | 0x80)); }, IR, H::ReservedBits},
        // accepted
        {"grant", none, [] { return one(grantOf(1)); }, OKC, H::None},
        {"update version 0", none, [] { return one(updateOf(1, 0)); }, OKC, H::None},
        {"revoke version 0", withIdx, [] { return one(revokeOf(1, 0)); }, OKC, H::None},
        {"revoke Existing", withIdx, [] { return one(revokeOf(1, 5)); }, OKC, H::None},
        {"2048 records", none, [] { return manyGrants(2048); }, OKC, H::None},
        {"update while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(updateOf(1, 5)); }, OKC, H::None},
        {"revoke while viewMemOver", [](Rig& r) { r.withIndex(); r.facts.viewMemOver = true; }, [] { return one(revokeOf(1, 5)); }, OKC, H::None},
        {"update while leasesOver", [](Rig& r) { r.facts.leasesOver = true; }, [] { return one(updateOf(1, 5)); }, OKC, H::None},
        {"revoke while leasesOver", [](Rig& r) { r.withIndex(); r.facts.leasesOver = true; }, [] { return one(revokeOf(1, 5)); }, OKC, H::None},
        {"update ttl differs", none, [] { return one(updateOf(1, 5, 7)); }, OKC, H::None},
        {"grant ttl max", none, [] { return one(grantOf(1, maxTtl())); }, OKC, H::None},
        {"grant ttl 0 (raised, not refused)", none, [] { return one(grantOf(1, 0)); }, OKC, H::None},
        // order: the first failing row of the spec table wins
        {"version 17 AND no View", [](Rig& r) { r.facts.clientVersion = 17; r.facts.acks = 1; }, [] { return one(grantOf(1)); }, IR, H::LeaseClientVersion},
        {"no View AND transactional", [](Rig& r) { r.facts.acks = 1; r.facts.transactional = true; }, [] { return one(grantOf(1)); }, IR, H::LeaseNeedsView},
        {"transactional AND 2049", [](Rig& r) { r.facts.transactional = true; }, [] { return manyGrants(2049); }, IR, H::LeaseInTxn},
        {"2049 AND bad record", none, [] {
             std::vector<LRec> recs;
             for (int i = 0; i < 2049; ++i) recs.push_back(updateOf(uint64_t(i + 1), -1));
             return batchOf(recs);
         }, IR, H::LeaseBatchTooLarge},
        {"remaining 1 AND ttl max+1", none, [] { return one({1, kGrantA, leaseValue(1, 0, maxTtl() + 1, 1)}); }, IR, H::LeaseBadRecord},
        {"ttl max+1 AND viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(grantOf(1, maxTtl() + 1)); }, TTL, H::LeaseTtlTooLarge},
        // the first failing record decides; batch-wide rules (version, View, transaction, count) come before the record walk
        {"grant ttl max+1 THEN update remaining 1", none, [] { return batchOf({grantOf(1, maxTtl() + 1), {2, kUpdateA, leaseValue(2, 5, 5000, 1)}}); }, TTL, H::LeaseTtlTooLarge},
        {"update remaining 1 THEN grant ttl max+1", none, [] { return batchOf({{2, kUpdateA, leaseValue(2, 5, 5000, 1)}, grantOf(1, maxTtl() + 1)}); }, IR, H::LeaseBadRecord},
        {"bad update AND grant while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return batchOf({grantOf(1), updateOf(2, -1)}); }, IR, H::LeaseBadRecord},
    };
    for (const Row& row : rows) {
        SCOPED_TRACE(row.name);
        Rig r;
        row.setup(r);
        const auto batch = row.batch();
        const int64_t endBefore = r.tlog.logEndOffset();
        const Produced o = produce(r, batch);   // through handleProduceBatch: a refusal must leave the log untouched
        EXPECT_EQ(o.code, row.code);
        if (row.why != H::None) EXPECT_EQ(o.why, row.why);
        if (row.code != OKC) EXPECT_EQ(r.tlog.logEndOffset(), endBefore) << "a refused batch is not stored";
        else EXPECT_GT(r.tlog.logEndOffset(), endBefore);
    }
}

TEST(LeaseWriteHookRegression, BatchLargerThanScratchIsInternalRegression) {
    Rig r;
    r.scratch.resize(200);   // a grant batch is copied to the scratch to be stamped: 5 grants do not fit
    const auto batch = manyGrants(5);
    ASSERT_GT(batch.size(), r.scratch.size());
    const Outcome o = r.write(batch, false);
    EXPECT_EQ(o.code, ErrorCode::Internal);
    EXPECT_EQ(r.tlog.logEndOffset(), 0) << "nothing stored";
    const Produced p = produce(r, batch);
    EXPECT_EQ(p.code, ErrorCode::Internal);
    EXPECT_EQ(r.tlog.logEndOffset(), 0);
}

TEST(LeaseWriteHookRegression, TtlRaisedNotRefusedRegression) {
    struct Case { uint64_t in; uint64_t stored; };
    const Case cases[] = {{0, minTtl()}, {minTtl() - 1, minTtl()}, {minTtl(), minTtl()}, {maxTtl(), maxTtl()}, {minTtl() + 1, minTtl() + 1}};
    for (const Case& c : cases) {
        SCOPED_TRACE(c.in);
        Rig r;
        const Outcome o = r.write(one(grantOf(1, c.in)));
        ASSERT_EQ(o.code, OKC);
        (void)rune::protocol::forEachRecordSpan(o.copy.data(), o.copy.size(), [&](const rune::protocol::RecordSpan& s) {
            EXPECT_EQ(getLe(reinterpret_cast<const uint8_t*>(s.value.data()) + rec::kLeaseTtlValueOffset), c.stored);
        });
    }
    Rig r;
    const Outcome o = r.check(one(grantOf(1, maxTtl() + 1)));
    EXPECT_EQ(o.code, TTL);
    // a custom spec: the raise uses THIS topic's min
    rune::protocol::LeaseSpec ls;
    ls.minTtlMs = 4000;
    Rig custom(ls);
    const Outcome oc = custom.write(one(grantOf(1, 1000)));
    ASSERT_EQ(oc.code, OKC);
    (void)rune::protocol::forEachRecordSpan(oc.copy.data(), oc.copy.size(), [&](const rune::protocol::RecordSpan& s) {
        EXPECT_EQ(getLe(reinterpret_cast<const uint8_t*>(s.value.data()) + rec::kLeaseTtlValueOffset), 4000u);
    });
}

TEST(LeaseWriteHookRegression, VersionStampingRegression) {
    Rig r;
    r.preload(7);
    ASSERT_EQ(r.tlog.logEndOffset(), 7);
    for (const int n : {1, 2, 1000}) {
        SCOPED_TRACE(n);
        const int64_t end = r.tlog.logEndOffset();
        const Outcome o = r.write(manyGrants(n));
        ASSERT_EQ(o.code, OKC);
        ASSERT_EQ(o.base, end);
        EXPECT_NE(o.staged, nullptr);
        int seen = 0;
        ASSERT_TRUE(rune::protocol::forEachRecordSpan(o.copy.data(), o.copy.size(), [&](const rune::protocol::RecordSpan& s) {
            EXPECT_EQ(int64_t(getLe(reinterpret_cast<const uint8_t*>(s.value.data()) + rec::kLeaseVersionValueOffset)), end + int64_t(s.index));
            ++seen;
        }));
        EXPECT_EQ(seen, n);
        EXPECT_EQ(getLe(o.copy.data() + rune::protocol::batch::kCrcOffset, 4),
                  rune::protocol::crc32c(o.copy.data() + rune::protocol::batch::kAttributesOffset, o.copy.size() - rune::protocol::batch::kAttributesOffset));
        // what the log stored carries the same versions: record i of the stored batch has offset base + i
        const auto view = r.tlog.log.readView(end, 1 << 22);
        ASSERT_NE(view.data, nullptr);
        EXPECT_EQ(rune::protocol::RecordBatchReader(view.data, view.len).baseOffset(), end);
    }
}

TEST(LeaseWriteHookRegression, MixedBatchOnlyGrantsPatchedRegression) {
    Rig r;
    r.preload(7);
    const auto in = batchOf({grantOf(1), updateOf(2, 3, 9), grantOf(3, 0)});
    const Outcome o = r.write(in);
    ASSERT_EQ(o.code, OKC);
    std::vector<rune::protocol::RecordSpan> a, b;
    (void)rune::protocol::forEachRecordSpan(in.data(), in.size(), [&](const rune::protocol::RecordSpan& s) { a.push_back(s); });
    (void)rune::protocol::forEachRecordSpan(o.copy.data(), o.copy.size(), [&](const rune::protocol::RecordSpan& s) { b.push_back(s); });
    ASSERT_EQ(a.size(), 3u);
    ASSERT_EQ(b.size(), 3u);
    ASSERT_EQ(a[1].len, b[1].len);
    EXPECT_EQ(std::memcmp(in.data() + a[1].pos, o.copy.data() + b[1].pos, a[1].len), 0) << "the update's bytes are identical";
    const auto ver = [&](const rune::protocol::RecordSpan& s) { return int64_t(getLe(reinterpret_cast<const uint8_t*>(s.value.data()) + rec::kLeaseVersionValueOffset)); };
    const auto ttl = [&](const rune::protocol::RecordSpan& s) { return getLe(reinterpret_cast<const uint8_t*>(s.value.data()) + rec::kLeaseTtlValueOffset); };
    EXPECT_EQ(ver(b[0]), 7);
    EXPECT_EQ(ver(b[1]), 3) << "the update keeps the client's version";
    EXPECT_EQ(ver(b[2]), 9);
    EXPECT_EQ(ttl(b[1]), 9u);
    EXPECT_EQ(ttl(b[2]), minTtl());
    EXPECT_EQ(getLe(o.copy.data() + rune::protocol::batch::kCrcOffset, 4),
              rune::protocol::crc32c(o.copy.data() + rune::protocol::batch::kAttributesOffset, o.copy.size() - rune::protocol::batch::kAttributesOffset));
}

TEST(LeaseWriteHookRegression, UpdatesOnlyZeroCopyRegression) {
    Rig r;
    r.preload(3);
    r.withIndex();
    for (const auto& batch : {manyUpdates(1), manyUpdates(50), batchOf({updateOf(1, 4), revokeOf(2, 6)})}) {
        const Outcome o = r.write(batch, /*keepCopy=*/false);
        ASSERT_EQ(o.code, OKC);
        EXPECT_EQ(o.staged, batch.data()) << "no grant: the input is appended as it is";
        EXPECT_EQ(o.stagedLen, batch.size());
    }
}

TEST(LeaseWriteHookRegression, NonLeaseUnchangedRegression) {
    // A key-indexed non-lease topic: bits 2-3 stay reserved.
    TopicWriteSpec spec;
    cw::specFor(cw::Entity::TypedLegacy, spec);
    PartitionWriteState state;
    state.spec = &spec;
    WriteFacts facts;
    facts.levelCheckedWrites = true;
    facts.idempotent = true;
    facts.acks = 3;
    const cw::TypedRecord rec0 = cw::validRecord();
    for (const uint8_t bits : {uint8_t(0x04), uint8_t(0x08), uint8_t(0x0C)}) {
        rune::protocol::RecordBatchBuilder b(0, 1'000'000);
        b.append(rec0.key, rec0.value, 1'000'000, bits);
        const auto batch = b.build();
        CheckedWriteHook hook(state, facts, {});
        EXPECT_EQ(hook.check(batch.data(), batch.size(), 1), IR) << int(bits);
        EXPECT_EQ(hook.refusal(), H::ReservedBits) << int(bits);
    }
}

// ── Task 5b ──

namespace {

uint16_t schemaOf(const uint8_t* value) { return uint16_t(value[2] | (value[3] << 8)); }
const uint8_t* valueOfSpan(const rune::protocol::RecordSpan& s) { return reinterpret_cast<const uint8_t*>(s.value.data()); }

std::vector<rune::protocol::RecordSpan> spansOf(const std::vector<uint8_t>& b) {
    std::vector<rune::protocol::RecordSpan> v;
    (void)rune::protocol::forEachRecordSpan(b.data(), b.size(), [&](const rune::protocol::RecordSpan& s) { v.push_back(s); });
    return v;
}

bool crcOk(const std::vector<uint8_t>& b) {
    return getLe(b.data() + rune::protocol::batch::kCrcOffset, 4) ==
           rune::protocol::crc32c(b.data() + rune::protocol::batch::kAttributesOffset, b.size() - rune::protocol::batch::kAttributesOffset);
}

// The expected re-encoded value of row `id` at the topic's latest version.
std::vector<uint8_t> expectedRevokeValue(Rig& r, uint64_t id, uint16_t version) {
    const uint8_t* row = r.index->table.get(rune::view::ViewKey::fromU64(id));
    const auto* w = r.spec.descriptorFor(version);
    std::vector<uint8_t> out(4 + w->blockLength);
    const std::size_t n = encodeLeaseValue(row, r.index->layout, r.index->desc, *w, version, out.data(), out.size());
    EXPECT_EQ(n, out.size());
    return out;
}

}  // namespace

TEST(LeaseWriteHookRegression, LeaseUnannouncedSessionRetriableRegression) {
    struct Case { int32_t version; ErrorCode code; H why; };
    const Case cases[] = {
        {-1, ErrorCode::NotLeader, H::LeaseSessionUnannounced},
        {0, IR, H::LeaseClientVersion},
        {17, IR, H::LeaseClientVersion},
        {18, OKC, H::None},
        {19, OKC, H::None},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE(c.version);
        Rig r;
        r.facts.clientVersion = c.version;
        const Outcome o = r.write(one(grantOf(1)), false);
        EXPECT_EQ(o.code, c.code);
        EXPECT_EQ(o.why, c.why);
        if (c.code != OKC) EXPECT_EQ(r.tlog.logEndOffset(), 0) << "nothing stored";
    }
}

TEST(LeaseWriteHookRegression, VersionStampingNearInt64MaxBaseOffsetRegression) {
    for (const int64_t hdrBase : {int64_t(INT64_MAX) - 1, int64_t(1000), int64_t(INT64_MIN)}) {
        SCOPED_TRACE(hdrBase);
        Rig r;
        r.preload(7);
        std::vector<LRec> recs;
        for (int i = 0; i < 5; ++i) recs.push_back(grantOf(uint64_t(i + 1)));
        const Outcome o = r.write(batchOf(recs, hdrBase));
        ASSERT_EQ(o.code, OKC);
        int seen = 0;
        ASSERT_TRUE(rune::protocol::forEachRecordSpan(o.copy.data(), o.copy.size(), [&](const rune::protocol::RecordSpan& s) {
            EXPECT_EQ(int64_t(getLe(valueOfSpan(s) + rec::kLeaseVersionValueOffset)), 7 + int64_t(s.index)) << "the offset the append assigns, whatever the client's base";
            ++seen;
        }));
        EXPECT_EQ(seen, 5);
    }
}

TEST(LeaseWriteHookRegression, RevokeReencodedFullRowRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.preload(3);
    ASSERT_EQ(r.index->layout.slotType.size(), 9u);
    r.addRow(1, /*ver=*/5, /*ttl=*/7000, /*remaining=*/300, "alpha", 5);
    r.addRow(2, 6, 7000, 0, "beta", 6);
    // a v1 revoke with the user columns zeroed, between a grant and an update
    const auto in = batchOf({grantOf(9), revokeOf(1, 5), updateOf(2, 6)});
    const Outcome o = r.write(in);
    ASSERT_EQ(o.code, OKC);
    ASSERT_NE(o.staged, in.data());
    ASSERT_TRUE(crcOk(o.copy));
    const auto sp = spansOf(o.copy);
    ASSERT_EQ(sp.size(), 3u);
    EXPECT_EQ(sp[1].attributes, kRevokeA) << "op bits and intent kept";
    EXPECT_EQ(sp[1].key, keyOf(1));
    EXPECT_EQ(schemaOf(valueOfSpan(sp[1])), 2) << "the stored revoke is at the latest installed version";
    const auto want = expectedRevokeValue(r, 1, 2);
    ASSERT_EQ(sp[1].value.size(), want.size());
    EXPECT_EQ(std::memcmp(valueOfSpan(sp[1]), want.data(), want.size()), 0);
    EXPECT_EQ(std::string(sp[1].value.substr(44, 5)), "alpha") << "the user columns come from the row, not from the client's zeros";
    EXPECT_EQ(getLe(valueOfSpan(sp[1]) + 60, 4), 5u);
    EXPECT_EQ(getLe(valueOfSpan(sp[1]) + 28, 8), 300u) << "remaining, from the row";
    // the other records: the grant stamped, the update untouched
    EXPECT_EQ(int64_t(getLe(valueOfSpan(sp[0]) + rec::kLeaseVersionValueOffset)), 3);
    const auto insp = spansOf(in);
    EXPECT_EQ(std::memcmp(in.data() + insp[2].pos, o.copy.data() + sp[2].pos, insp[2].len), 0);
    // the stored log holds the same bytes
    const auto view = r.tlog.log.readView(o.base, 1 << 22);
    ASSERT_NE(view.data, nullptr);
    EXPECT_EQ(std::memcmp(view.data + 8, o.copy.data() + 8, o.copy.size() - 8), 0) << "stored = staged (past the base offset)";
}

TEST(LeaseWriteHookRegression, RevokeNoRowUnchangedRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.state.judgedThrough = INT64_MAX;
    r.addRow(1, 5);
    // an absent key, and a live key with another version: the record is appended as it is (the judge answers LeaseNotFound)
    for (const auto& in : {one(revokeOf(2, 5)), one(revokeOf(1, 6)), batchOf({revokeOf(2, 5), revokeOf(1, 4)})}) {
        const Outcome o = r.write(in);
        ASSERT_EQ(o.code, OKC);
        EXPECT_EQ(o.staged, in.data()) << "nothing to re-encode: zero-copy";
        EXPECT_EQ(o.copy, in);
    }
    // mixed: only the matching revoke changes
    const auto in = batchOf({revokeOf(2, 5), revokeOf(1, 5)});
    const Outcome o = r.write(in);
    ASSERT_EQ(o.code, OKC);
    const auto a = spansOf(in), b = spansOf(o.copy);
    EXPECT_EQ(std::memcmp(in.data() + a[0].pos, o.copy.data() + b[0].pos, a[0].len), 0);
    EXPECT_EQ(schemaOf(valueOfSpan(b[1])), 2);
    // a topic without an index (null table): unchanged, no crash
    Rig bare;
    const auto in2 = one(revokeOf(1, 5));
    bare.facts.leaseIndexReady = true;
    const Outcome o2 = bare.write(in2, false);
    EXPECT_EQ(o2.code, ErrorCode::Throttled) << "no index: a revoke waits";
}

TEST(LeaseWriteHookRegression, UpdateThenRevokeSameBatchRefusedRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.addRow(1, 5);
    r.addRow(2, 6);
    r.state.judgedThrough = INT64_MAX;   // every earlier append is applied: only the same-batch rule is under test
    const int64_t end = r.tlog.logEndOffset();
    const Outcome refused = r.write(batchOf({updateOf(1, 5), revokeOf(1, 5)}), false);
    EXPECT_EQ(refused.code, IR);
    EXPECT_EQ(refused.why, H::LeaseUpdateThenRevoke);
    EXPECT_EQ(r.tlog.logEndOffset(), end) << "nothing stored";
    const Outcome far = r.write(batchOf({updateOf(3, 5), grantOf(9), revokeOf(7, 5), updateOf(1, 5), revokeOf(1, 5)}), false);   // refused even when far apart
    EXPECT_EQ(far.why, H::LeaseUpdateThenRevoke);
    EXPECT_EQ(r.write(batchOf({updateOf(1, 5), revokeOf(2, 6)}), false).code, OKC) << "different keys";
    EXPECT_EQ(r.write(batchOf({revokeOf(1, 5), updateOf(1, 5)}), false).code, OKC) << "revoke then update: the judge answers the update 64";
    EXPECT_EQ(r.write(batchOf({updateOf(1, 5), updateOf(1, 5)}), false).code, OKC);
    EXPECT_EQ(r.write(batchOf({revokeOf(1, 5), revokeOf(1, 5)}), false).code, OKC);
    // many updates (hash clustering), one revoke of the last: still found
    std::vector<LRec> recs;
    for (int i = 0; i < 1500; ++i) recs.push_back(updateOf(uint64_t(i + 100), 5));
    recs.push_back(revokeOf(1599, 5));
    EXPECT_EQ(r.write(batchOf(recs), false).why, H::LeaseUpdateThenRevoke);
    recs.back() = revokeOf(1600, 5);
    EXPECT_EQ(r.write(batchOf(recs), false).code, OKC);
}

TEST(LeaseWriteHookRegression, LastAppendedRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.preload(4);
    r.addRow(1, 5);
    r.addRow(2, 6);
    r.addRow(3, 7);
    for (uint64_t id = 1; id <= 3; ++id) ASSERT_EQ(r.lastAppendedOf(id), -1);
    // updates k1, revokes k2, grants k4 (no row): lastAppended of k1 and k2 = the batch's last offset; k3 untouched
    const auto in = batchOf({updateOf(1, 5), revokeOf(2, 6), grantOf(4)});
    const Outcome o = r.write(in, false);
    ASSERT_EQ(o.code, OKC);
    EXPECT_EQ(r.lastAppendedOf(1), o.base + 2);
    EXPECT_EQ(r.lastAppendedOf(2), o.base + 2);
    EXPECT_EQ(r.lastAppendedOf(3), -1);
    // a refused batch changes nothing
    const Outcome bad = r.write(batchOf({updateOf(3, 7), updateOf(3, -1)}), false);
    ASSERT_NE(bad.code, OKC);
    EXPECT_EQ(r.lastAppendedOf(3), -1);
    // a batch whose append fails (stage ran, onAppended did not) changes nothing
    const Outcome failed = r.write(batchOf({updateOf(3, 7)}), false, /*append=*/false);
    EXPECT_EQ(failed.code, ErrorCode::TransientLogFull);
    EXPECT_EQ(r.lastAppendedOf(3), -1) << "written only after a successful append";
    // grants record nothing; a key without a row gets nothing
    const Outcome g = r.write(batchOf({grantOf(5), updateOf(99, 1)}), false);
    ASSERT_EQ(g.code, OKC);
    EXPECT_EQ(r.lastAppendedOf(99), -2);
    EXPECT_EQ(r.lastAppendedOf(3), -1);
}

// Item 13: an update the index cannot record raises leaseUntrackedThrough to the batch's last offset in onAppended, so readiness
// waits for the judge to pass it.  The index cannot record it when there is no index yet (null table) or when the key has no row;
// a not-ready index with a row records it in lastAppended (and the not-ready raise is kept as well).
TEST(LeaseWriteHookRegression, UpdateUntrackedRaisesMarkRegression) {
    {   // no index: the hook records nothing anywhere but the mark
        Rig bare;
        bare.preload(4);
        bare.state.leaseUntrackedThrough = -1;
        const Outcome o = bare.write(batchOf({updateOf(1, 5), updateOf(2, 5)}), false);
        ASSERT_EQ(o.code, OKC);
        EXPECT_EQ(bare.state.leaseUntrackedThrough, o.base + 1) << "the batch's last offset";
        const Outcome failed = bare.write(one(updateOf(1, 5)), false, /*append=*/false);
        EXPECT_EQ(failed.code, ErrorCode::TransientLogFull);
        EXPECT_EQ(bare.state.leaseUntrackedThrough, o.base + 1) << "raised only after a successful append";
        const Outcome g = bare.write(one(grantOf(9)), false);
        ASSERT_EQ(g.code, OKC);
        EXPECT_EQ(bare.state.leaseUntrackedThrough, o.base + 1) << "a grant is never untracked";
    }
    {   // a ready index, the key has a row: tracked in lastAppended, the mark stays
        Rig r;
        r.withIndex();
        r.preload(4);
        r.addRow(1, 5);
        r.state.leaseUntrackedThrough = -1;
        const Outcome o = r.write(one(updateOf(1, 5)), false);
        ASSERT_EQ(o.code, OKC);
        EXPECT_EQ(r.lastAppendedOf(1), o.base);
        EXPECT_EQ(r.state.leaseUntrackedThrough, -1);
        r.facts.leaseIndexReady = false;   // not ready: recorded in the row and the mark
        const Outcome n = r.write(one(updateOf(1, 5)), false);
        ASSERT_EQ(n.code, OKC);
        EXPECT_EQ(r.lastAppendedOf(1), n.base);
        EXPECT_EQ(r.state.leaseUntrackedThrough, n.base);
    }
}

TEST(LeaseWriteHookRegression, RevokeBehindUpdateThrottledRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    r.preload(4);
    r.addRow(1, 5);
    r.state.judgedThrough = 4;
    const Outcome upd = r.write(one(updateOf(1, 5)), false);
    ASSERT_EQ(upd.code, OKC);   // appended at offset 4; the judge has not run
    ASSERT_EQ(r.lastAppendedOf(1), 4);
    const Outcome behind = r.write(one(revokeOf(1, 5)), false);
    EXPECT_EQ(behind.code, ErrorCode::Throttled);
    EXPECT_EQ(behind.why, H::LeaseRevokeBehindUpdate);
    EXPECT_EQ(behind.retryAfterNs, 1'000'000);
    EXPECT_EQ(r.tlog.logEndOffset(), 5) << "nothing stored";
    r.state.judgedThrough = 5;   // the judge applied it
    const Outcome ok = r.write(one(revokeOf(1, 5)), false);
    EXPECT_EQ(ok.code, OKC);
    EXPECT_EQ(r.lastAppendedOf(1), 5) << "the revoke itself is now pending";
    // an update of the same key is not held back by a pending update (only a revoke re-encodes from the row)
    EXPECT_EQ(r.write(one(updateOf(1, 5)), false).code, OKC);
    // a pending REVOKE holds the next revoke back too
    EXPECT_EQ(r.write(one(revokeOf(1, 5)), false).code, ErrorCode::Throttled);
    // a definite refusal wins over the retriable one
    EXPECT_EQ(r.write(batchOf({revokeOf(1, 5), updateOf(2, -1)}), false).code, IR);
}

// Lease topics Task 30 (spec A.6): the rebirth fence holds GRANTS only.  A hold > 0 answers a clean grant Throttled with the hold as its
// retry-after and stores nothing; updates, revokes and a hold of 0 are judged as usual; a definite refusal still wins over the hold.
TEST(LeaseWriteHookRegression, FenceHoldsGrantsOnlyRegression) {
    constexpr int64_t kHold = 7'000'000;
    {
        Rig r;
        r.preload(4);
        r.facts.leaseGrantHoldNs = kHold;
        const Outcome g = r.write(one(grantOf(1)), false);
        EXPECT_EQ(g.code, ErrorCode::Throttled);
        EXPECT_EQ(g.why, H::LeaseFenced);
        EXPECT_EQ(g.retryAfterNs, kHold);
        EXPECT_EQ(r.tlog.logEndOffset(), 4) << "nothing stored";
        // a batch with a grant among updates is held as a whole
        EXPECT_EQ(r.write(batchOf({updateOf(2, 5), grantOf(1)}), false).code, ErrorCode::Throttled);
        // a definite refusal wins over the hold (a grant over max_ttl)
        EXPECT_EQ(r.write(one(grantOf(1, maxTtl() + 1)), false).code, TTL);
        EXPECT_EQ(r.write(batchOf({grantOf(1), updateOf(2, -1)}), false).code, IR);
        EXPECT_EQ(r.tlog.logEndOffset(), 4);
    }
    {
        Rig r;
        r.installV2();
        r.withIndex();
        r.preload(4);
        r.addRow(1, 5);
        r.facts.leaseGrantHoldNs = kHold;
        const Outcome u = r.write(one(updateOf(1, 5)), false);
        EXPECT_EQ(u.code, OKC) << "an update is never held";
        EXPECT_EQ(u.why, H::None);
        r.state.judgedThrough = r.tlog.logEndOffset();
        const Outcome v = r.write(one(revokeOf(1, 5)), false);
        EXPECT_EQ(v.code, OKC) << "a revoke is never held";
        EXPECT_EQ(v.why, H::None);
    }
    {
        Rig r;
        r.preload(4);
        r.facts.leaseGrantHoldNs = 0;
        EXPECT_EQ(r.write(one(grantOf(1)), false).code, OKC) << "hold 0: grants are judged as usual";
    }
}

TEST(LeaseWriteHookRegression, RevokeWhileIndexNotReadyThrottledRegression) {
    Rig r;
    r.withIndex();
    r.state.judgedThrough = INT64_MAX;
    r.addRow(1, 5);
    r.facts.leaseIndexReady = false;
    const Outcome o = r.write(one(revokeOf(1, 5)), false);
    EXPECT_EQ(o.code, ErrorCode::Throttled);
    EXPECT_EQ(o.why, H::LeaseRevokeIndexNotReady);
    EXPECT_EQ(o.retryAfterNs, 1'000'000);
    EXPECT_EQ(r.tlog.logEndOffset(), 0);
    EXPECT_EQ(r.write(batchOf({grantOf(2), updateOf(1, 5)}), false).code, OKC) << "grants and updates do not need the index";
    r.facts.leaseIndexReady = true;
    EXPECT_EQ(r.write(one(revokeOf(1, 5)), false).code, OKC);
    r.facts.leaseIndex = {};
    EXPECT_EQ(r.write(one(revokeOf(1, 5)), false).code, ErrorCode::Throttled) << "no index at all";
}

TEST(LeaseWriteHookRegression, RevokeManyRowsReencodedRegression) {
    Rig r;
    r.installV2();
    r.withIndex();
    std::vector<LRec> recs;
    for (uint64_t id = 1; id <= 2048; ++id) {
        r.addRow(id, int64_t(id), 7000, 0, "x", uint32_t(id));
        recs.push_back(revokeOf(id, int64_t(id)));
    }
    const auto in = batchOf(recs);
    const Outcome o = r.write(in);
    ASSERT_EQ(o.code, OKC);
    ASSERT_TRUE(crcOk(o.copy));
    const auto sp = spansOf(o.copy);
    ASSERT_EQ(sp.size(), 2048u);
    for (std::size_t i = 0; i < sp.size(); ++i) {
        ASSERT_EQ(schemaOf(valueOfSpan(sp[i])), 2) << i;
        ASSERT_EQ(getLe(valueOfSpan(sp[i]) + 60, 4), i + 1) << "the port column (v2 only) comes from the row";
    }
}

// ONE bound (leaseScratchBytes): a revoke grows to the widest fixed block, so a batch of minimal client revokes is refused
// (39, nothing stored) as soon as its re-encoding outgrows the scratch.
TEST(LeaseWriteHookRegression, RevokeBatchFitsScratchRegression) {
    constexpr std::size_t kIntake = 100'000;
    constexpr std::size_t kScratch = leaseScratchBytes(kIntake);
    static_assert(kScratch == 2 * kIntake);
    const auto revokes = [](int n) {
        std::vector<LRec> recs;
        for (int i = 1; i <= n; ++i) recs.push_back(revokeOf(uint64_t(i), int64_t(i)));
        return batchOf(recs);
    };
    const auto fresh = [&](Rig& r) {
        r.scratch.assign(kScratch, 0);
        r.installWide(65535);
        r.withIndex();
        for (uint64_t id = 1; id <= 4; ++id) r.addRow(id, int64_t(id), 7000);
    };
    {   // 3 revokes re-encode to 3 * 65539 bytes + framing: they fit
        Rig r;
        fresh(r);
        const Outcome o = r.write(revokes(3));
        ASSERT_EQ(o.code, OKC);
        ASSERT_LE(o.copy.size(), kScratch);
        ASSERT_TRUE(crcOk(o.copy));
        const auto sp = spansOf(o.copy);
        ASSERT_EQ(sp.size(), 3u);
        for (const auto& s : sp) {
            EXPECT_EQ(schemaOf(valueOfSpan(s)), 2);
            EXPECT_EQ(s.value.size(), 4u + 65535u) << "re-encoded at the widest block";
        }
    }
    {   // one record more does not: refused, nothing stored
        Rig r;
        fresh(r);
        const int64_t end = r.tlog.logEndOffset();
        const auto batch = revokes(4);
        ASSERT_LT(batch.size(), kScratch) << "the client's batch itself is small";
        const Produced p = produce(r, batch);
        EXPECT_EQ(p.code, IR);
        EXPECT_EQ(p.why, H::LeaseRevokeBatchTooLarge);
        EXPECT_EQ(r.tlog.logEndOffset(), end) << "nothing stored";
        for (uint64_t id = 1; id <= 4; ++id) EXPECT_EQ(r.lastAppendedOf(id), -1);
    }
}
