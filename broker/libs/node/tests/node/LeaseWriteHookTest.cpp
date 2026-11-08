// LeaseWriteHookTest.cpp - lease topics Task 5a: the lease branch of CheckedWriteHook (spec 6.4 refusals, grant stamping,
// the TTL raise).  Unit level on the hook with a real InMemoryLog; the worker leg is in LeaseJudgeWorkerTest.
#include "support/CheckedWritesBatches.h"
#include "support/LeaseHookRig.h"

#include <gtest/gtest.h>

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

}  // namespace

TEST(LeaseWriteHookRegression, RefusalTableRegression) {
    const auto none = [](Rig&) {};
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
        {"empty value", none, [&] { return one(withValue(grantOf(1), {})); }, IR, H::None},
        {"empty value (update)", none, [&] { return one(withValue(updateOf(1, 5), {})); }, IR, H::LeaseBadRecord},
        {"35-byte value", none, [&] { auto v = leaseValue(1, 0, 5000); v.resize(35); return one(withValue(grantOf(1), v)); }, IR, H::LeaseBadRecord},
        {"remaining 1 (grant)", none, [] { return one({1, kGrantA, leaseValue(1, 0, 5000, 1)}); }, IR, H::LeaseBadRecord},
        {"remaining 1 (update)", none, [] { return one({1, kUpdateA, leaseValue(1, 5, 5000, 1)}); }, IR, H::LeaseBadRecord},
        {"update version -1", none, [] { return one(updateOf(1, -1)); }, IR, H::LeaseBadRecord},
        {"revoke version -1", none, [] { return one(revokeOf(1, -1)); }, IR, H::LeaseBadRecord},
        {"grant ttl max+1", none, [] { return one(grantOf(1, maxTtl() + 1)); }, TTL, H::LeaseTtlTooLarge},
        {"grant while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(grantOf(1)); }, NLE, H::LeaseViewMemOver},
        {"bit 4 set", none, [] { return one(withAttrs(grantOf(1), kGrantA | 0x10)); }, IR, H::ReservedBits},
        {"bit 7 set", none, [] { return one(withAttrs(updateOf(1, 5), kUpdateA | 0x80)); }, IR, H::ReservedBits},
        // accepted
        {"grant", none, [] { return one(grantOf(1)); }, OKC, H::None},
        {"update version 0", none, [] { return one(updateOf(1, 0)); }, OKC, H::None},
        {"revoke version 0", none, [] { return one(revokeOf(1, 0)); }, OKC, H::None},
        {"revoke Existing", none, [] { return one(revokeOf(1, 5)); }, OKC, H::None},
        {"2048 records", none, [] { return manyGrants(2048); }, OKC, H::None},
        {"update while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(updateOf(1, 5)); }, OKC, H::None},
        {"revoke while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return one(revokeOf(1, 5)); }, OKC, H::None},
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
        {"bad update AND grant while viewMemOver", [](Rig& r) { r.facts.viewMemOver = true; }, [] { return batchOf({grantOf(1), updateOf(2, -1)}); }, IR, H::LeaseBadRecord},
    };
    for (const Row& row : rows) {
        SCOPED_TRACE(row.name);
        Rig r;
        row.setup(r);
        const auto batch = row.batch();
        const int64_t endBefore = r.tlog.logEndOffset();
        const Outcome o = r.write(batch, /*keepCopy=*/false);
        EXPECT_EQ(o.code, row.code);
        if (row.why != H::None) EXPECT_EQ(o.why, row.why);
        if (row.code != OKC) EXPECT_EQ(r.tlog.logEndOffset(), endBefore) << "a refused batch is not stored";
        else EXPECT_GT(r.tlog.logEndOffset(), endBefore);
    }
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
