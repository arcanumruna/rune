// Lease topics Task 10: the expiry emitter on a real worker (LeaseRuntime::emit, Worker::leaseDuty_, PartitionReplica::appendLeaseBatch).
//
// Every test drives the lease clock by hand (setLeaseClockForTest): the wheels run on it, never on wall time.  The clock override is
// installed BEFORE the first worker pass, so the runtime (created when the index first becomes ready) and every poll see one
// monotonic clock.
#include "CheckedWritesRig.h"
#include "AllocWindow.h"

#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/storage/testing/DemandChannelForTest.h>

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;
using rune::node::lease::LeaseEntryState;

namespace {

constexpr int64_t kMs = 1'000'000;

std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}
void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}
uint64_t le(const std::vector<uint8_t>& v, std::size_t at, int bytes = 8) {
    uint64_t x = 0;
    for (int i = 0; i < bytes; ++i) x |= uint64_t(v[at + std::size_t(i)]) << (8 * i);
    return x;
}
// [u16 blockLength][u16 schema version][version, ttl, holder, remaining, id, addr(16)].
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t ttl = 5000) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, 1, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    return b;
}
RawRec grantRec(uint64_t id, const std::string& addr = "a", uint64_t ttl = 5000) { return {keyOf(id), leaseValue(id, 0, addr, ttl), 1}; }
RawRec updateRec(uint64_t id, int64_t ver, const std::string& addr) { return {keyOf(id), leaseValue(id, ver, addr), 2}; }
constexpr uint8_t kRevokeAttr = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);
RawRec revokeRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, ""), kRevokeAttr}; }
constexpr uint8_t kExpireAttr = uint8_t(rec::LeaseOp::Expire) << rec::kAttrLeaseOpShift;

struct Stored { int64_t offset; uint8_t attrs; std::string key; std::vector<uint8_t> value; };
struct StoredBatch {
    int64_t base = 0; int32_t epoch = 0; int64_t pid = 0; int16_t pep = 0; int32_t seq = 0;
    int64_t baseTs = 0, maxTs = 0; bool control = false, valid = false;
    std::vector<Stored> recs;
    std::vector<int64_t> tsDelta;
    std::size_t bytes = 0;
};
/// Every batch of the log from `fromOffset`, with its header, straight from the segments.
std::vector<StoredBatch> storedBatches(VerdictRig& r, int64_t fromOffset = 0) {
    std::vector<StoredBatch> out;
    for (int64_t off = fromOffset; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
            const std::size_t total = static_cast<std::size_t>(rd.totalLength());
            if (total < 12 || p + total > v.len) break;
            if (rd.baseOffset() >= fromOffset) {
                StoredBatch b;
                b.base = rd.baseOffset(); b.epoch = rd.partitionLeaderEpoch(); b.pid = rd.producerId(); b.pep = rd.producerEpoch();
                b.seq = rd.baseSequence(); b.baseTs = rd.baseTimestamp(); b.maxTs = rd.maxTimestamp();
                b.control = (rd.attributes() & rune::protocol::batch::kAttrControl) != 0;
                b.valid = rd.ensureValid();
                b.bytes = total;
                if (!b.control) {
                    rd.forEachRecord([&](std::string_view k, std::string_view val, int64_t o, int64_t ts, uint8_t a) {
                        b.recs.push_back({o, a, std::string(k), std::vector<uint8_t>(val.begin(), val.end())});
                        b.tsDelta.push_back(ts - b.baseTs);
                    });
                }
                out.push_back(std::move(b));
            }
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}
std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (const auto& b : storedBatches(r)) if (!b.control) out.insert(out.end(), b.recs.begin(), b.recs.end());
    return out;
}
std::vector<Stored> expires(VerdictRig& r) {
    std::vector<Stored> out;
    for (auto& s : storedRecords(r)) if (rec::leaseOpOf(s.attrs) == rec::LeaseOp::Expire) out.push_back(std::move(s));
    return out;
}
std::size_t countOp(VerdictRig& r, rec::LeaseOp op) {
    std::size_t n = 0;
    for (const auto& s : storedRecords(r)) n += rec::leaseOpOf(s.attrs) == op ? 1 : 0;
    return n;
}

/// A lease topic on a real worker with the lease clock under the test's control.
struct Em {
    int64_t leaseNow = 1'000'000'000;   // declared before the rig: outlives the worker that reads it
    VerdictRig r;
    int32_t seq = 0;
    int ackPasses;
    explicit Em(uint32_t maxKeys = 1000, RigTuning tuning = {}, bool follower = false, bool acks = true, int64_t rollBytes = 64LL * 1024 * 1024)
        : r(follower, rollBytes, tuning), ackPasses(tuning.durabilityTier == 2 ? 600 * 50 : 600) {
        r.worker->setLeaseClockForTest(&leaseNow);
        r.createLeaseTopic(maxKeys);
        r.passes(30);
        WorkerOp level;
        level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
        r.send(std::move(level));
        if (acks) r.enableAcks(rune::protocol::client::kClientVersionLeases, 0);
        r.passes(30);
    }
    void advance(int64_t ns) { leaseNow += ns; }
    void advanceMs(int64_t ms) { leaseNow += ms * kMs; }
    std::optional<VerdictRig::Ack> produce(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), rune::protocol::client::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        return r.ack(c, ackPasses);
    }
    /// Sent without waiting for the answer (the judge may be held).
    int64_t send(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), rune::protocol::client::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        return c;
    }
    lease::LeaseRuntime* rt() { return r.worker->leaseRuntimeForTest(VerdictRig::kSrc, 0); }
    const lease::LeaseRuntimeStats& st() { return rt()->stats(); }
    uint32_t ridOf(uint64_t key) {
        const auto lk = r.vs->leaseIndexFor(VerdictRig::kSrc, 0, r.replica->leadershipGeneration(), true, true);
        uint32_t rid = UINT32_MAX;
        if (lk.ref.table == nullptr || !lk.ref.table->ridOf(rune::view::ViewKey::fromU64(key), rid)) return UINT32_MAX;
        return rid;
    }
    const lease::LeaseEntry& entry(uint64_t key) { return rt()->timers().entry(ridOf(key)); }
    bool linked(uint64_t key) { return rt()->timers().deadline().linked(ridOf(key)); }
    /// Passes until the lease runtime exists (it is created on the first cycle the index is ready).
    bool waitRuntime(int maxPasses = 300) {
        for (int i = 0; i < maxPasses && rt() == nullptr; ++i) r.pass();
        return rt() != nullptr;
    }
    /// The grant offset of `key` (its lease_version).
    int64_t versionOf(uint64_t key) {
        for (const auto& s : storedRecords(r)) if (s.key == keyOf(key) && rec::leaseOpOf(s.attrs) == rec::LeaseOp::Put) return s.offset;
        return -1;
    }
    int64_t corr = 5000;
    /// One LeaseKeepAlive op for `key` at `version`, then ONE pass: the first answered entry's code (0xFFFF: no answer) and ttl.
    struct KaOut { uint16_t code = 0xFFFF; uint64_t ttl = 0; };
    KaOut keepAlive(uint64_t key, int64_t version) {
        std::vector<uint8_t> tail;
        const std::string k = keyOf(key);
        tail.push_back(uint8_t(k.size()));
        tail.insert(tail.end(), k.begin(), k.end());
        for (int i = 0; i < 8; ++i) tail.push_back(uint8_t(uint64_t(version) >> (8 * i)));
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseKeepAlive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = 77; op.correlationId = ++corr; op.recordCount = 1;
        op.leaseTail = std::span<const uint8_t>(tail.data(), tail.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        r.leaseKaAnswers.clear();
        r.pass();
        KaOut out;
        if (!r.leaseKaAnswers.empty() && r.leaseKaAnswers.back().chunk.size() >= 12) {
            const uint8_t* p = r.leaseKaAnswers.back().chunk.data();
            out.code = uint16_t(p[2] | (p[3] << 8));
            for (int j = 0; j < 8; ++j) out.ttl |= uint64_t(p[4 + j]) << (8 * j);
        }
        r.leaseKaAnswers.clear();
        return out;
    }
    /// One LeaseTimeToLive op for `key`, then ONE pass: the answer's error code and remaining ms (-2: no answer).
    struct TtlOut { int16_t code = -1; int64_t remainingMs = -2; };
    TtlOut timeToLive(uint64_t key) {
        r.leaseTtlAnswers.clear();
        const std::string k = keyOf(key);
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseTimeToLive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = 5; op.correlationId = ++corr;
        op.leaseTail = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        r.pass();
        TtlOut out;
        if (r.leaseTtlAnswers.size() == 1) { out.code = r.leaseTtlAnswers[0].errorCode; out.remainingMs = r.leaseTtlAnswers[0].baseOffset; }
        r.leaseTtlAnswers.clear();
        return out;
    }
};

constexpr uint16_t kNotLeaderCode = static_cast<uint16_t>(rune::protocol::client::ErrorCode::NotLeader);

/// Nothing carried over from an earlier runtime: every counter is zero except the promotion pass's own (Task 12).
bool statsAllZero(const lease::LeaseRuntimeStats& st) {
    static const lease::LeaseRuntimeStats zero{};
    lease::LeaseRuntimeStats c = st;
    c.promotionLinked = 0;
    return std::memcmp(&c, &zero, sizeof(c)) == 0;
}

}  // namespace

// 1. One batch, the exact shape the spec fixes (9.1).
TEST(LeaseEmitterRegression, BatchShapeRegression) {
    Em e;
    VerdictRig& r = e.r;
    const auto ack = e.produce({grantRec(1, "a1"), grantRec(2, "b2"), grantRec(3, "c3")});
    ASSERT_TRUE(ack);
    ASSERT_EQ(ack->code, 0);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    const uint32_t rids[3] = {e.ridOf(1), e.ridOf(2), e.ridOf(3)};
    for (uint32_t rid : rids) ASSERT_NE(rid, UINT32_MAX);
    for (uint64_t k = 1; k <= 3; ++k) ASSERT_EQ(e.entry(k).state, LeaseEntryState::Armed) << "key " << k;
    const std::vector<Stored> grants = storedRecords(r);
    ASSERT_EQ(grants.size(), 3u);

    const int64_t leoBefore = r.leo();
    e.advanceMs(5000 + 1);
    r.pass();
    const auto batches = storedBatches(r, leoBefore);
    ASSERT_EQ(batches.size(), 1u) << "one batch";
    const StoredBatch& b = batches[0];
    EXPECT_EQ(b.pid, -1);
    EXPECT_EQ(b.pep, -1);
    EXPECT_EQ(b.seq, -1);
    EXPECT_EQ(b.baseTs, 0);
    EXPECT_EQ(b.maxTs, 0);
    EXPECT_FALSE(b.control);
    EXPECT_TRUE(b.valid);
    EXPECT_EQ(b.epoch, static_cast<int32_t>(r.replica->epoch()));
    EXPECT_EQ(b.base, leoBefore);
    ASSERT_EQ(b.recs.size(), 3u);
    std::vector<bool> seen(4, false);
    for (std::size_t i = 0; i < b.recs.size(); ++i) {
        const Stored& x = b.recs[i];
        EXPECT_EQ(b.tsDelta[i], 0);
        EXPECT_EQ(x.attrs, rec::withLeaseOp(0, rec::LeaseOp::Expire));
        EXPECT_EQ(x.attrs, 0x08);
        ASSERT_EQ(x.key.size(), 8u);
        const uint64_t key = uint64_t(uint8_t(x.key[0]));
        ASSERT_TRUE(key >= 1 && key <= 3);
        seen[key] = true;
        const Stored* g = nullptr;
        for (const auto& s : grants) if (s.key == x.key) g = &s;
        ASSERT_NE(g, nullptr);
        ASSERT_EQ(x.value.size(), g->value.size());
        EXPECT_EQ(le(x.value, 4), uint64_t(g->offset)) << "lease_version = the grant's offset";
        EXPECT_EQ(le(x.value, 4), le(g->value, 4));
        EXPECT_EQ(le(x.value, 12), le(g->value, 12));
        EXPECT_EQ(le(x.value, 20), le(g->value, 20));
        EXPECT_EQ(le(x.value, 28), le(g->value, 28));
        EXPECT_TRUE(std::equal(x.value.begin() + 32, x.value.end(), g->value.begin() + 32)) << "the user columns are the row's";
    }
    EXPECT_TRUE(seen[1] && seen[2] && seen[3]);

    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(3);
    for (uint64_t k = 1; k <= 3; ++k) EXPECT_FALSE(r.vs->leaseColumnForTest(VerdictRig::kSrc, 0, k, 0).has_value()) << "row " << k << " is gone";
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->leaseRejectedStaleBroker, 0u);
    for (uint32_t rid : rids) EXPECT_EQ(e.rt()->timers().entry(rid).state, LeaseEntryState::Free);
    EXPECT_EQ(e.st().expiriesAppended, 3u);
    EXPECT_EQ(e.st().expiryBatches, 1u);
}

// 2. Never early: the 1 ms tick; a key expires exactly at its deadline, never before.
TEST(LeaseEmitterRegression, NeverEarlyRegression) {
    {
        Em e;
        VerdictRig& r = e.r;
        const int64_t t = e.leaseNow;
        ASSERT_TRUE(e.produce({grantRec(1)}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        ASSERT_EQ(e.entry(1).state, LeaseEntryState::Armed);
        e.leaseNow = t + 5000 * kMs - 1;
        r.passes(5);
        EXPECT_TRUE(expires(r).empty()) << "one ns before the deadline";
        e.leaseNow = t + 5000 * kMs;
        r.pass();
        EXPECT_EQ(expires(r).size(), 1u) << "at the deadline";
    }
    {
        // The largest TTL the hook accepts (max_ttl_ms, 9e12 ms): due at T + 9e18 ns, not before.
        Em e;
        VerdictRig& r = e.r;
        const int64_t t = e.leaseNow;
        constexpr uint64_t kMaxTtl = 9'000'000'000'000ull;
        const auto ack = e.produce({grantRec(1, "a", kMaxTtl)});
        ASSERT_TRUE(ack);
        ASSERT_EQ(ack->code, 0);
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        ASSERT_EQ(e.entry(1).state, LeaseEntryState::Armed);
        EXPECT_EQ(e.entry(1).deadlineNs, t + int64_t(kMaxTtl) * kMs);
        e.leaseNow = t + int64_t(kMaxTtl) * kMs - 1;
        r.passes(3);
        EXPECT_TRUE(expires(r).empty());
        e.leaseNow = t + int64_t(kMaxTtl) * kMs;
        r.pass();
        EXPECT_EQ(expires(r).size(), 1u);
    }
    {
        // A TTL a wire u64 can carry but no spec accepts saturates: "not in this lifetime".  Injected as a Granted event.
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.waitRuntime());
        LeaseEvent ev;
        ev.kind = LeaseEventKind::Granted; ev.rid = 7; ev.ttlMs = UINT64_MAX;
        r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&ev, 1), e.leaseNow);
        ASSERT_EQ(e.rt()->timers().entry(7).state, LeaseEntryState::Armed);
        EXPECT_EQ(e.rt()->timers().entry(7).deadlineNs, INT64_MAX);
        e.leaseNow = INT64_MAX - 1;
        r.passes(5);
        EXPECT_TRUE(expires(r).empty());
        EXPECT_EQ(e.rt()->timers().entry(7).state, LeaseEntryState::Armed);
        EXPECT_TRUE(e.rt()->timers().deadline().linked(7));
    }
}

// 3. ISR below min-ISR: the batch is refused, nothing is appended, the entries stay Armed and are retried.
TEST(LeaseEmitterRegression, MinIsrAppendsNothingRegression) {
    Em e(1000, RigTuning{.minIsr = 2}, /*follower=*/true);
    VerdictRig& r = e.r;
    const int64_t c = e.send({grantRec(1)});
    const auto ack = r.ackFetching(c);
    ASSERT_TRUE(ack);
    ASSERT_EQ(ack->code, 0);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    ASSERT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    const int32_t live[1] = {1};
    r.replica->shrinkIsrToLive(std::span<const int32_t>(live, 1), r.now);   // the follower leaves the ISR: |ISR| 1 < minIsr 2
    e.advanceMs(5000 + 1);
    const int64_t leo = r.leo();
    r.passes(3);
    EXPECT_EQ(r.leo(), leo) << "nothing appended";
    EXPECT_TRUE(expires(r).empty());
    EXPECT_EQ(e.st().expiryBacklog, 3u) << "1 record x 3 attempts";
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    EXPECT_TRUE(e.linked(1));
    // the ISR is restored (a same-epoch re-assert with the full ISR): the next pass appends exactly one Expire
    r.replica->becomeLeader(r.replica->epoch(), std::vector<rune::replication::NodeId>{1, 2});
    for (int i = 0; i < 20 && expires(r).empty(); ++i) r.pass();
    EXPECT_EQ(expires(r).size(), 1u);
    r.passes(5);
    EXPECT_EQ(expires(r).size(), 1u);
}

// 4. A replica that is not (or no longer) the leader appends nothing; the entries stay Armed.
TEST(LeaseEmitterRegression, NotLeaderStopsRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    e.advanceMs(5000 + 1);
    r.replica->setAppendRefusedForTest(true);
    r.pass();
    EXPECT_TRUE(expires(r).empty());
    EXPECT_EQ(e.st().notLeaderStops, 1u);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    EXPECT_TRUE(e.linked(1));
    r.replica->setAppendRefusedForTest(false);
    r.pass();
    EXPECT_EQ(expires(r).size(), 1u);
}

// 5. The per-key lastAppended gate: an update appended but not yet judged is never overwritten by an Expire built from the old row.
TEST(LeaseEmitterRegression, LastAppendedGateRegression) {
    Em e;
    VerdictRig& r = e.r;
    const auto g = e.produce({grantRec(1, "a1")});
    ASSERT_TRUE(g);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    const int64_t ver = g->baseOffset;
    r.holdJudge(true);
    (void)e.send({updateRec(1, ver, "zz")});
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    e.advanceMs(5000 + 1);
    r.passes(3);
    EXPECT_TRUE(expires(r).empty());
    EXPECT_GE(e.st().expiryDeferred, 3u);
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    for (int i = 0; i < 10 && expires(r).empty(); ++i) r.pass();
    r.passes(5);
    const auto ex = expires(r);
    ASSERT_EQ(ex.size(), 1u);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(ex[0].value.data()) + 44, 2), "zz") << "the Expire carries the UPDATED user column";
}

// 6. Only an Armed entry emits.
TEST(LeaseEmitterRegression, OnlyArmedEmitsRegression) {
    {   // Emitting: one Expire in flight, never a second
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(1)}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        e.advanceMs(5000 + 1);
        r.holdJudge(true);
        r.pass();
        ASSERT_EQ(expires(r).size(), 1u);
        EXPECT_EQ(e.entry(1).state, LeaseEntryState::Emitting);
        for (int i = 0; i < 5; ++i) { e.advanceMs(1000); r.pass(); }
        EXPECT_EQ(expires(r).size(), 1u);
    }
    for (const LeaseEntryState s : {LeaseEntryState::Checkpointing, LeaseEntryState::Clearing}) {
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(2)}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        const uint32_t rid = e.ridOf(2);
        e.rt()->timersForTest().setState(rid, s, 12345);
        e.advanceMs(5000 + 1);
        r.passes(3);
        EXPECT_TRUE(expires(r).empty()) << "state " << int(s);
        e.rt()->timersForTest().setState(rid, LeaseEntryState::Armed);
        r.pass();
        EXPECT_EQ(expires(r).size(), 1u) << "state " << int(s);
    }
}

// 7. A revoke appended before the Expire is built: the Expire is never built (the key's lastAppended gate), the revoke ends the key.
TEST(LeaseEmitterRegression, RevokeBeforeExpireBuiltRegression) {
    Em e;
    VerdictRig& r = e.r;
    const auto g = e.produce({grantRec(1)});
    ASSERT_TRUE(g);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    r.holdJudge(true);
    (void)e.send({revokeRec(1, g->baseOffset)});
    r.passes(30);
    ASSERT_TRUE(r.leaseReady());
    e.advanceMs(5000 + 1);
    r.passes(3);
    EXPECT_TRUE(expires(r).empty());
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(5);
    EXPECT_EQ(countOp(r, rec::LeaseOp::Expire), 0u);
    EXPECT_EQ(countOp(r, rec::LeaseOp::Revoke), 1u);
    EXPECT_FALSE(r.vs->leaseColumnForTest(VerdictRig::kSrc, 0, 1, 0).has_value()) << "the revoke ended the key";
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->leaseRejectedStaleBroker, 0u);
    EXPECT_EQ(r.stats()->rejectionsLeaseNotFound, 0u);
}

// 8. A revoke appended after the Expire: the Expire ends the key, the revoke is refused LeaseNotFound.
TEST(LeaseEmitterRegression, RevokeAfterExpireRegression) {
    Em e;
    VerdictRig& r = e.r;
    const auto g = e.produce({grantRec(1)});
    ASSERT_TRUE(g);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    e.advanceMs(5000 + 1);
    r.holdJudge(true);
    r.pass();
    ASSERT_EQ(expires(r).size(), 1u);
    const int64_t c = e.send({revokeRec(1, g->baseOffset)});
    r.passes(10);
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(5);
    EXPECT_FALSE(r.vs->leaseColumnForTest(VerdictRig::kSrc, 0, 1, 0).has_value()) << "the Expire was accepted";
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->rejectionsLeaseNotFound, 1u);
    EXPECT_EQ(r.stats()->leaseRejectedStaleBroker, 0u);
    const auto ack = r.ack(c);
    ASSERT_TRUE(ack);
    ASSERT_EQ(ack->verdicts.size(), 1u);
    EXPECT_EQ(std::get<1>(ack->verdicts[0]), 62) << "LeaseNotFound";
}

// 9. Namespace gates do not pause expiry.  The worker has no per-partition "suspended" state (ReadOnly / RejectAll are enforced at the
// gateway by the session's write table); the worker-side gates a namespace can put on its partitions are the over-disk and deleting bytes
// and the produce throttles.  Expiry is the broker's own housekeeping and consults none of them.
TEST(LeaseEmitterRegression, SuspensionDoesNotPauseRegression) {
    enum class Gate { OverDisk, Deleting, ProduceThrottles };
    for (const Gate gate : {Gate::OverDisk, Gate::Deleting, Gate::ProduceThrottles}) {
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(1)}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        constexpr uint16_t kSlot = 3;
        constexpr uint32_t kNs = 77;
        rune::protocol::NamespaceLimits limits;
        r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
        r.log->setNsTag(kNs, kSlot);
        switch (gate) {
            case Gate::OverDisk: r.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::Disk, INT64_MAX); break;
            case Gate::Deleting: r.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::NsDeleting, 0); break;
            case Gate::ProduceThrottles:
                for (const ThrottleKind k : {ThrottleKind::ProduceBytes, ThrottleKind::ProduceRecords, ThrottleKind::RequestOps})
                    r.worker->applyNsThrottleDirect(kSlot, kNs, k, INT64_MAX);
                break;
        }
        if (gate == Gate::OverDisk) ASSERT_TRUE(r.worker->nsOverDiskForTest(kSlot));
        if (gate == Gate::Deleting) ASSERT_TRUE(r.worker->nsDeletingForTest(kSlot));
        e.advanceMs(5000 + 1);
        r.pass();
        EXPECT_EQ(expires(r).size(), 1u) << "gate " << int(gate);
        ASSERT_TRUE(r.driveUntilJudged());
        r.passes(3);
        EXPECT_FALSE(r.vs->leaseColumnForTest(VerdictRig::kSrc, 0, 1, 0).has_value()) << "gate " << int(gate) << ": the Expire was judged";
    }
}

// 10. A full batch continues next cycle: 256 per cycle, none lost, none doubled.
TEST(LeaseEmitterRegression, FullBatchContinuesNextCycleRegression) {
    const auto t0 = std::chrono::steady_clock::now();
    Em e(12000);
    VerdictRig& r = e.r;
    ASSERT_NE(r.writeState(), nullptr);
    ASSERT_EQ(r.writeState()->spec->lease->expiryBatch, 256);
    for (uint64_t base = 1; base <= 10000; base += 1000) {
        std::vector<RawRec> recs;
        for (uint64_t k = base; k < base + 1000; ++k) recs.push_back(grantRec(k, "x"));
        const auto ack = e.produce(recs);
        ASSERT_TRUE(ack) << "batch at " << base;
        ASSERT_EQ(ack->code, 0);
    }
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 10000u);
    e.advanceMs(5000 + 1);
    for (int i = 1; i <= 40; ++i) {
        r.pass();
        ASSERT_EQ(expires(r).size(), std::min<std::size_t>(256u * std::size_t(i), 10000u)) << "after pass " << i;
    }
    ASSERT_TRUE(r.driveUntilJudged());
    r.passes(3);
    EXPECT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 0u);
    ASSERT_NE(r.stats(), nullptr);
    EXPECT_EQ(r.stats()->leaseRejectedStaleBroker, 0u);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    std::printf("FullBatchContinuesNextCycle: %lld ms\n", static_cast<long long>(ms));   // printed only: wall time is no assertion
}

// 11. The byte cap splits a batch: 25 due keys, a cap that fits 10 records -> 10, 10, 5.
TEST(LeaseEmitterRegression, ByteCapSplitsBatchRegression) {
    Em e;
    VerdictRig& r = e.r;
    std::vector<RawRec> recs;
    for (uint64_t k = 1; k <= 25; ++k) recs.push_back(grantRec(k, "x"));
    ASSERT_TRUE(e.produce(recs));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    constexpr std::size_t kCap = 850;   // exact sizes (Task 11a): 61 header + 10 x 75 = 811 fit, 11 x 75 = 886 do not (900 fit eleven)
    r.worker->setLeaseBatchCapForTest(kCap);
    e.advanceMs(5000 + 1);
    const int64_t before = r.leo();
    const std::size_t expected[3] = {10, 10, 5};
    for (int i = 0; i < 3; ++i) {
        r.pass();
        std::size_t n = 0;
        for (const auto& b : storedBatches(r, before)) {
            if (b.control) continue;
            EXPECT_LE(b.bytes, kCap);
            for (const auto& s : b.recs) n += rec::leaseOpOf(s.attrs) == rec::LeaseOp::Expire ? 1 : 0;
        }
        std::size_t want = 0;
        for (int j = 0; j <= i; ++j) want += expected[j];
        ASSERT_EQ(n, want) << "after pass " << i + 1;
    }
    for (const auto& b : storedBatches(r, before)) if (!b.control) EXPECT_LE(b.bytes, kCap);
}

// 12. A rejected record of the entry's own returns it to Armed; a broker rejection of another leadership's record changes nothing.
TEST(LeaseEmitterRegression, OwnRecordRejectedReturnsArmedRegression) {
    Em e;
    VerdictRig& r = e.r;
    const auto g = e.produce({grantRec(1, "a1")});
    ASSERT_TRUE(g);
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    e.advanceMs(5000 + 1);
    r.holdJudge(true);
    r.pass();
    const auto ex = expires(r);
    ASSERT_EQ(ex.size(), 1u);
    const int64_t x = ex[0].offset;
    const uint32_t rid = e.ridOf(1);
    ASSERT_EQ(e.entry(1).state, LeaseEntryState::Emitting);
    ASSERT_EQ(e.entry(1).inflightOffset, x);
    LeaseEvent other;
    other.kind = LeaseEventKind::BrokerRejected; other.rid = rid; other.offset = x + 1; other.version = e.versionOf(1);
    r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&other, 1), e.leaseNow);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Emitting) << "a different offset is ignored";
    EXPECT_EQ(e.st().ownRecordRejected, 0u);
    LeaseEvent own = other;
    own.offset = x;
    r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&own, 1), e.leaseNow);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    EXPECT_TRUE(e.linked(1));
    EXPECT_EQ(e.st().ownRecordRejected, 1u);
    EXPECT_EQ(e.st().brokerRejectedSeen, 2u);
    r.pass();   // due again: rebuilt from the current row
    ASSERT_EQ(expires(r).size(), 2u);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Emitting);
    EXPECT_NE(e.entry(1).inflightOffset, x);

    {   // judge level: a stale broker record of ANOTHER leadership, on a live leader with the runtime present
        Em f;
        VerdictRig& q = f.r;
        const auto gg = f.produce({grantRec(1, "a1")});
        ASSERT_TRUE(gg);
        ASSERT_TRUE(q.driveUntilJudged());
        ASSERT_TRUE(f.waitRuntime());
        ASSERT_EQ(f.entry(1).state, LeaseEntryState::Armed);
        rune::protocol::RecordBatchBuilder b(0, 0);   // an Expire of a version the row never had
        auto stale = leaseValue(1, gg->baseOffset + 1000, "a1");
        b.append(keyOf(1), std::string_view(reinterpret_cast<const char*>(stale.data()), stale.size()), 0, rec::withLeaseOp(0, rec::LeaseOp::Expire));
        const auto batch = b.build();
        ASSERT_GE(q.replica->appendLeaseBatch(batch), 0);
        for (int i = 0; i < 30 && f.st().brokerRejectedSeen == 0; ++i) q.pass();
        ASSERT_NE(q.stats(), nullptr);
        EXPECT_EQ(q.stats()->leaseRejectedStaleBroker, 1u);
        EXPECT_EQ(f.st().brokerRejectedSeen, 1u) << "the event was seen";
        EXPECT_EQ(f.st().ownRecordRejected, 0u) << "but it was not this entry's record";
        EXPECT_EQ(f.entry(1).state, LeaseEntryState::Armed);
    }
}

// 13. A demotion destroys every timer; nothing is carried into the next leadership (the promotion pass is Task 12).
TEST(LeaseEmitterRegression, DemotionDiscardsRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1), grantRec(2), grantRec(3)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    ASSERT_NE(e.rt(), nullptr);
    r.demote();
    r.passes(3);
    EXPECT_EQ(e.rt(), nullptr) << "the runtime of a follower is gone";
    r.promote();
    r.passes(60);
    ASSERT_NE(e.rt(), nullptr) << "a new runtime of the new leadership exists";
    EXPECT_TRUE(statsAllZero(e.st())) << "a new runtime: nothing carried over";
    EXPECT_EQ(e.st().promotionLinked, 3u);
    for (uint64_t k = 1; k <= 3; ++k) {   // the promotion pass armed every live row at ready + extend (0 here) + ttl
        const uint32_t rid = e.ridOf(k);
        ASSERT_NE(rid, UINT32_MAX);
        EXPECT_EQ(e.rt()->timers().entry(rid).state, LeaseEntryState::Armed) << "key " << k;
        EXPECT_EQ(e.rt()->timers().entry(rid).deadlineNs, e.rt()->readyNs() + 5000 * kMs) << "key " << k;
    }
    e.advanceMs(5000 - 1);
    r.passes(5);
    EXPECT_TRUE(expires(r).empty()) << "not before the promotion deadline";
    e.advanceMs(2);
    r.passes(5);
    EXPECT_EQ(expires(r).size(), 3u) << "the promoted keys expire at their promotion deadline";
}

// 14. The emitter allocates nothing in steady state (hard rule 4): across the emitting passes and the judge folds of the Expires.
TEST(LeaseEmitterAllocRegression, EmitterNoAllocRegression) {
    Em e(1000);
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    e.advanceMs(5000 + 1);
    r.passes(3);   // warm-up: the work area exists, one Expire emitted and judged
    ASSERT_EQ(expires(r).size(), 1u);
    std::vector<RawRec> recs;
    for (uint64_t k = 100; k < 400; ++k) recs.push_back(grantRec(k, "x"));
    ASSERT_TRUE(e.produce(recs));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_EQ(r.vs->keyIndexSizeForTest(VerdictRig::kSrc, 0), 300u);
    e.advanceMs(5000 + 1);
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow w;
        r.passes(4);   // pass 1 emits 256, pass 2 judges them and emits 44, passes 3-4 judge the rest
        allocs = w.count();
    }
    EXPECT_EQ(expires(r).size(), 301u);
    EXPECT_EQ(allocs, 0u);
}

// 15. A record bigger than the namespace's batch cap does not stall the emitter (Task 11a addendum A): the first record of a batch is
// accepted when it fits the hard cap, so the due keys behind it still expire.  Each goes out alone (every record is over the cap).
// Mutation: a "full" flag set when the record is over the cap with nothing in the batch yet stalls the loop at n == 0: red.
TEST(LeaseEmitterRegression, OversizedRecordDoesNotStallRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1), grantRec(2), grantRec(3), grantRec(4), grantRec(5), grantRec(6)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    rune::protocol::NamespaceLimits limits;
    limits.maxBatchBytes = 100;   // below one encoded Expire batch (the 61-byte overhead plus a ~75-byte record)
    r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
    r.log->setNsTag(kNs, kSlot);
    const int64_t leoBefore = r.leo();
    e.advanceMs(5000 + 1);
    for (int i = 0; i < 40 && expires(r).size() < 6; ++i) r.pass();
    EXPECT_EQ(expires(r).size(), 6u) << "all six expired";
    const auto batches = storedBatches(r, leoBefore);
    ASSERT_FALSE(batches.empty());
    EXPECT_EQ(batches[0].recs.size(), 1u) << "the first record goes out alone";
    for (const auto& b : batches) EXPECT_EQ(b.recs.size(), 1u) << "every record is over the cap: one per batch";
    EXPECT_GE(e.st().oversizedDeferred + e.st().expiriesAppended, 6u);
}

// 15b. Rows over the HARD cap (min(1 MiB, the worker's intake limit)).  Every Expire of one emit is re-encoded at the same latest writer
// and lease keys are fixed-width, so the records of one cycle all have the same size: over the hard cap means every row is.  They are
// counted (oversizedDeferred), stay Armed and linked, nothing is appended, and they all expire once the cap admits them.
TEST(LeaseEmitterRegression, OverHardCapRowsWaitAndRecoverRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1), grantRec(2), grantRec(3)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    r.worker->setMaxBatchBytes(100);   // below one Expire batch (61 + ~75)
    e.advanceMs(5000 + 1);
    r.passes(5);
    EXPECT_TRUE(expires(r).empty());
    EXPECT_GE(e.st().oversizedDeferred, 3u);
    for (uint64_t k = 1; k <= 3; ++k) {
        EXPECT_EQ(e.entry(k).state, LeaseEntryState::Armed);
        EXPECT_TRUE(e.linked(k));
    }
    r.worker->setMaxBatchBytes(64u << 10);
    for (int i = 0; i < 40 && expires(r).size() < 3; ++i) r.pass();
    EXPECT_EQ(expires(r).size(), 3u);
}

// 16. A throwing append (a failed storage) is a refusal, never a terminate (Task 11a addendum B): the entry stays Armed and linked,
// the error is counted, and the key expires once the append works again.
TEST(LeaseEmitterRegression, AppendThrowDoesNotTerminateRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    e.advanceMs(5000 + 1);
    r.replica->setLeaseAppendThrowForTest(true);
    r.passes(3);   // a throw out of a noexcept frame would terminate the test binary here
    EXPECT_TRUE(expires(r).empty());
    EXPECT_GE(e.st().appendErrors, 1u);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    EXPECT_TRUE(e.linked(1));
    r.replica->setLeaseAppendThrowForTest(false);
    r.pass();
    EXPECT_EQ(expires(r).size(), 1u);
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Emitting);
}

// 17. (F3) An identity change DISCARDS the runtime: nothing is carried over and no Expire is emitted for the old deadlines.  Two legs:
// (a) a new writer field id bumps the lease layout generation: the index is rebuilt (new rebuild epoch);
// (b) a demote + promote inside ONE doWork (the generation changes, the passes in between never run).
// Mutation: delete the `dropLeaseRuntime_` of the sameIdentity check in leaseDuty_: the old runtime emits at its old deadlines: red (both legs).
TEST(LeaseEmitterRegression, IdentityChangeDiscardsRegression) {
    for (const int leg : {0, 1}) {
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(1), grantRec(2), grantRec(3)})) << "leg " << leg;
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        const uint32_t rids[3] = {e.ridOf(1), e.ridOf(2), e.ridOf(3)};
        for (uint64_t k = 1; k <= 3; ++k) ASSERT_EQ(e.entry(k).state, LeaseEntryState::Armed) << "leg " << leg << " key " << k;
        const uint32_t epochBefore = r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0);
        const uint64_t genBefore   = r.replica->leadershipGeneration();
        const int64_t leoStable    = r.leo();
        const int64_t t0           = e.leaseNow;
        e.advanceMs(3000);   // the new runtime is ready 3 s after the old deadlines were set: its promotion deadlines are t0 + 8 s
        if (leg == 0) {
            r.addLeasePort();   // the evolving descriptor gains a field id: the layout generation moves, the index is rebuilt
            r.passes(30);
            ASSERT_TRUE(r.driveUntilJudged());
            r.passes(30);
            ASSERT_TRUE(r.leaseReady());
            ASSERT_GT(r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0), epochBefore) << "the identity changed";
        } else {
            r.replica->becomeFollower(r.replica->epoch() + 1, 2);                                       // both role changes land before
            r.replica->becomeLeader(r.replica->epoch() + 1, std::vector<rune::replication::NodeId>{1});   // one worker pass runs
            r.passes(60);
            ASSERT_TRUE(r.driveUntilJudged());
            r.passes(30);
            ASSERT_TRUE(r.leaseReady());
            ASSERT_NE(r.replica->leadershipGeneration(), genBefore) << "the identity changed";
        }
        if (leg == 0) {
            // The index was rebuilt under the SAME leadership while the duty pass idled (it checks the generation only).  The first keepalive
            // sees the stale identity: it answers NotLeader and drops the runtime, so the next duty pass (never idle-skipped: no runtime)
            // recreates it at the then leaseNow = t0 + 3 s with NO clock advance; the retried keepalive is Ok within 3 passes.
            ASSERT_NE(e.rt(), nullptr);
            ASSERT_FALSE(e.rt()->sameIdentity(r.replica->leadershipGeneration(), r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0))) << "the stale runtime is still there";
            const auto first = e.keepAlive(1, e.versionOf(1));
            EXPECT_EQ(first.code, kNotLeaderCode) << "the stale runtime answers NotLeader";
            for (int i = 0; i < 3 && (e.rt() == nullptr || !e.rt()->sameIdentity(r.replica->leadershipGeneration(), r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0))); ++i) r.pass();
            ASSERT_NE(e.rt(), nullptr) << "recreated within a pass or two, not at the next wheel deadline";
            EXPECT_EQ(e.rt()->readyNs(), t0 + 3000 * kMs) << "ready at the then leaseNow";
            Em::KaOut again;
            for (int i = 0; i < 3 && again.code != 0; ++i) again = e.keepAlive(1, e.versionOf(1));
            EXPECT_EQ(again.code, 0) << "the retried keepalive is served";
            EXPECT_EQ(again.ttl, 5000u);
        }
        // Past every old deadline (t0 + 5 s).  Leg 1's new runtime was created above, at t0 + 3 s; leg 0's just now (also t0 + 3 s).
        e.advanceMs(2000 + 1);
        r.passes(5);
        ASSERT_NE(e.rt(), nullptr) << "leg " << leg << ": the runtime of the new identity exists";
        EXPECT_TRUE(r.leaseReady());
        EXPECT_TRUE(e.rt()->sameIdentity(r.replica->leadershipGeneration(), r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0))) << "leg " << leg;
        EXPECT_TRUE(statsAllZero(e.st())) << "leg " << leg << ": a new runtime, nothing carried over";
        for (std::size_t i = 0; i < 3; ++i) {
            const uint32_t rid = e.ridOf(uint64_t(i + 1));
            ASSERT_NE(rid, UINT32_MAX);
            EXPECT_EQ(e.rt()->timers().entry(rid).state, LeaseEntryState::Armed) << "leg " << leg << " key " << i + 1 << ": armed by the promotion pass";
            EXPECT_EQ(e.rt()->timers().entry(rid).deadlineNs, e.rt()->readyNs() + 5000 * kMs) << "leg " << leg << " key " << i + 1 << ": the NEW deadline";
            EXPECT_GE(e.rt()->readyNs(), t0 + 3000 * kMs) << "leg " << leg;
            EXPECT_EQ(rid, rids[i]) << "leg " << leg << ": the rebuilt table assigns the same row ids";
        }
        EXPECT_TRUE(expires(r).empty()) << "leg " << leg << ": no Expire at the old deadlines";
        EXPECT_EQ(r.leo(), leoStable) << "leg " << leg << ": nothing appended";
    }
}

// 17b. (F2) After an index rebuild under the SAME leadership, a duty pass that idles never notices the stale identity, so the leader's lease ops
// themselves must: a keepalive (or TimeToLive) on the stale runtime answers NotLeader and drops it, the next duty pass (no runtime: never
// idle-skipped) recreates it, and the retry is served within 3 passes - at once, not at the next wheel deadline.
// Mutation: remove the `dropLeaseRuntime_` of the stale identity in leaseServeable_: the runtime stays stale, the retries stay NotLeader: red.
TEST(LeaseEmitterRegression, IdleRebuildServesKeepAliveRegression) {
    for (const int op : {0, 1}) {   // 0: LeaseKeepAlive, 1: LeaseTimeToLive
        SCOPED_TRACE(op == 0 ? "keepalive" : "timeToLive");
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(1)}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        const int64_t t0 = e.leaseNow;
        e.advanceMs(3000);
        r.addLeasePort();
        r.passes(30);
        ASSERT_TRUE(r.driveUntilJudged());
        r.passes(30);
        ASSERT_TRUE(r.leaseReady());
        ASSERT_NE(e.rt(), nullptr);
        ASSERT_FALSE(e.rt()->sameIdentity(r.replica->leadershipGeneration(), r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0))) << "idle: the stale runtime is still there";
        const int64_t v = e.versionOf(1);
        if (op == 0) EXPECT_EQ(e.keepAlive(1, v).code, kNotLeaderCode);
        else         EXPECT_EQ(e.timeToLive(1).code, int16_t(kNotLeaderCode));
        bool served = false;
        for (int i = 0; i < 3 && !served; ++i) {
            if (op == 0) {
                const auto o = e.keepAlive(1, v);
                served = o.code == 0 && o.ttl == 5000;
            } else {
                const auto o = e.timeToLive(1);
                served = o.code == 0 && o.remainingMs == 5000;
            }
        }
        EXPECT_TRUE(served) << "served within 3 passes";
        ASSERT_NE(e.rt(), nullptr);
        EXPECT_EQ(e.rt()->readyNs(), t0 + 3000 * kMs);
        EXPECT_TRUE(e.rt()->sameIdentity(r.replica->leadershipGeneration(), r.vs->leaseRebuildEpoch(VerdictRig::kSrc, 0)));
    }
}

// 18. (F4) The `!lk.ready` emit gate: a runtime of the current identity does NOT emit while the lease index is not ready, and does as
// soon as it is.  The index goes not-ready without losing the runtime: an update of a key with no row, appended while the judge is held,
// raises leaseUntrackedThrough (the hook cannot track it), so the index is not ready until the judge has passed it.
// Mutation: drop `|| !lk.ready` from the gate in leaseDuty_: the Expire goes out while not ready: red.
TEST(LeaseEmitterRegression, NotReadyGateRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    ASSERT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    lease::LeaseRuntime* const before = e.rt();
    r.holdJudge(true);
    (void)e.send({updateRec(99, 0, "zz")});   // no row for key 99
    r.passes(30);
    ASSERT_FALSE(r.leaseReady()) << "the index is not ready (an update it could not track)";
    ASSERT_EQ(e.rt(), before) << "the runtime is still the same one";
    e.advanceMs(5000 + 1);
    r.passes(5);
    EXPECT_TRUE(expires(r).empty()) << "not ready: nothing is emitted";
    EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
    EXPECT_TRUE(e.linked(1));
    r.holdJudge(false);
    ASSERT_TRUE(r.driveUntilJudged());
    for (int i = 0; i < 30 && expires(r).empty(); ++i) r.pass();
    r.passes(5);
    EXPECT_TRUE(r.leaseReady());
    EXPECT_EQ(expires(r).size(), 1u) << "ready again: exactly one Expire";
}

// 19. (F6) The PRODUCTION byte cap, through the namespace's limit (applyNsSlotAssignDirect + the log's ns tag; no override seam) and the
// intake's limit (setMaxBatchBytes).  One Expire of these rows is a 75-byte record on the 61-byte batch overhead.
// Mutations: drop the namespace term of leaseBatchCap_ -> cell 0 red; drop the intake term -> cell 1 red.
TEST(LeaseEmitterRegression, NamespaceByteCapRegression) {
    constexpr uint64_t kThree = 61 + 3 * 75 + 10;   // room for three Expires, not four (61 + 4 * 75 = 361)
    for (const int cell : {0, 1}) {
        Em e;
        VerdictRig& r = e.r;
        std::vector<RawRec> recs;
        for (uint64_t k = 1; k <= 7; ++k) recs.push_back(grantRec(k, "x"));
        ASSERT_TRUE(e.produce(recs));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        constexpr uint16_t kSlot = 3;
        constexpr uint32_t kNs = 77;
        rune::protocol::NamespaceLimits limits;
        limits.maxBatchBytes = cell == 0 ? kThree : (1u << 20);
        r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
        r.log->setNsTag(kNs, kSlot);
        if (cell == 1) r.worker->setMaxBatchBytes(kThree);   // the intake's limit is the smaller one
        const int64_t leoBefore = r.leo();
        e.advanceMs(5000 + 1);
        for (int i = 0; i < 3; ++i) r.pass();
        std::vector<std::size_t> sizes;
        for (const auto& b : storedBatches(r, leoBefore)) if (!b.control) sizes.push_back(b.recs.size());
        ASSERT_EQ(sizes.size(), 3u) << "cell " << cell;
        EXPECT_EQ(sizes[0], 3u) << "cell " << cell;
        EXPECT_EQ(sizes[1], 3u) << "cell " << cell;
        EXPECT_EQ(sizes[2], 1u) << "cell " << cell;
        for (const auto& b : storedBatches(r, leoBefore)) EXPECT_LE(b.bytes, kThree) << "cell " << cell;
    }
}

// 20. (F7) Both doWork paths run leaseDuty_.  The rig drives its worker in shard mode (doWorkShard_; ensureDriver_ sets it) once the ack sink is
// on, and a grant needs the ack sink's session version, so the classic doWork leg grants in shard mode and then flips the (single-threaded)
// worker to the classic path for the expiry.  The Expire is in the log and the entry is Emitting, then Free after the fold.
// Mutation: remove the leaseDuty_ call of a path -> that leg red.
TEST(LeaseEmitterRegression, DoWorkPathsRegression) {
    for (const bool classic : {true, false}) {
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(r.worker->shardMode()) << "the rig's default is the shard path";
        ASSERT_TRUE(e.produce({grantRec(1)})) << "classic " << classic;
        ASSERT_TRUE(r.driveUntilJudged()) << "classic " << classic;
        ASSERT_TRUE(e.waitRuntime()) << "classic " << classic;
        ASSERT_EQ(e.entry(1).state, LeaseEntryState::Armed) << "classic " << classic;
        const uint32_t rid = e.ridOf(1);   // the row is gone after the fold: keep the rid
        if (classic) r.worker->setShardMode(false);
        ASSERT_EQ(r.worker->shardMode(), !classic);
        e.advanceMs(5000 + 1);
        r.holdJudge(true);
        r.pass();
        ASSERT_EQ(expires(r).size(), 1u) << "classic " << classic << ": the Expire is in the log";
        EXPECT_EQ(e.entry(1).state, LeaseEntryState::Emitting) << "classic " << classic;
        r.holdJudge(false);
        ASSERT_TRUE(r.driveUntilJudged());
        r.passes(3);
        EXPECT_EQ(e.rt()->timers().entry(rid).state, LeaseEntryState::Free) << "classic " << classic << ": the fold ended the key";
        EXPECT_EQ(expires(r).size(), 1u);
    }
}

// 21. (F7) The Strict tier: the lease batch joins the pass's durable flush.  Mutation: remove the dirtyThisCycle_ push in noteLeaseAppended_
// -> no durability round starts for the Expire: red.
TEST(LeaseEmitterRegression, StrictTierExpireJoinsDurableRoundRegression) {
    Em e(1000, RigTuning{.durabilityTier = 2});
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    for (int i = 0; i < 50 * 600 && (r.worker->syncRoundInFlightForTest() || r.worker->lastSyncedLeoForTest(r.log.get()) < r.leo()); ++i) r.pass();
    ASSERT_EQ(r.worker->lastSyncedLeoForTest(r.log.get()), r.leo()) << "the grant is durable";
    const uint64_t rounds = r.worker->syncRoundsStartedForTest();
    e.advanceMs(5000 + 1);
    r.pass();
    ASSERT_EQ(expires(r).size(), 1u) << "the Expire batch is appended";
    EXPECT_GT(r.worker->syncRoundsStartedForTest(), rounds) << "a durability round started for it in the same pass";
    for (int i = 0; i < 50 * 600 && r.worker->lastSyncedLeoForTest(r.log.get()) < r.leo(); ++i) r.pass();
    EXPECT_EQ(r.worker->lastSyncedLeoForTest(r.log.get()), r.leo()) << "the log was flushed through the Expire";
}

// 22. (F8) The idle cost: a runtime with nothing to fold, no wheel due and no clear owed makes no index lookup.  Mutation: remove the idle
// skip: the counter grows every pass: red.
TEST(LeaseEmitterRegression, IdleSkipsLookupsRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    r.passes(10);   // the grant's events consumed
    const uint64_t before = r.worker->leaseDutyLookupsForTest();
    r.passes(100);
    EXPECT_EQ(r.worker->leaseDutyLookupsForTest(), before) << "100 idle passes: no lookup";
    e.advanceMs(5000 + 1);
    r.pass();
    EXPECT_EQ(expires(r).size(), 1u) << "the due key is still expired on time";
    EXPECT_GT(r.worker->leaseDutyLookupsForTest(), before);
}

// 23. (F9) Roll back-pressure: while the log's roll is deferred (a provisioned log with no ready segment) the emitter appends nothing, and
// does once a segment is ready.  Mutation: drop the rollDeferred check in leaseDuty_: the Expire lands while held: red.
TEST(LeaseEmitterRegression, RollDeferredHoldsExpiresRegression) {
    Em e(1000, {}, false, true, /*rollBytes=*/100);
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1), grantRec(2)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh{&Hook::fn, nullptr};
    r.log->setProvisioning(&demandCh);
    ASSERT_TRUE(r.log->rollDeferred()) << "the active segment is full and no segment is ready";
    e.advanceMs(5000 + 1);
    const int64_t leo = r.leo();
    r.passes(5);
    EXPECT_TRUE(expires(r).empty()) << "held: no Expire";
    EXPECT_EQ(r.leo(), leo);
    EXPECT_GE(e.st().rollDeferredSkips, 5u);
    ASSERT_EQ(r.log->provisionReady(1), 1);
    for (int i = 0; i < 20 && expires(r).size() < 2; ++i) r.pass();
    EXPECT_EQ(expires(r).size(), 2u) << "released: both expire";
    (void)r.log->finalizeStagingRenames();
    (void)r.log->drainStagingRenamesDone();
}

// 24. (F10) A BrokerRejected event carries the refused record's version.  An in-flight entry whose row still has that version is the
// entry's own refused record (back to Armed); a row that moved on (or is gone) means the record is stale: the entry is Free.
TEST(LeaseEmitterRegression, RejectedVersionRegression) {
    for (const bool matches : {true, false}) {
        Em e;
        VerdictRig& r = e.r;
        ASSERT_TRUE(e.produce({grantRec(1, "a1")}));
        ASSERT_TRUE(r.driveUntilJudged());
        ASSERT_TRUE(e.waitRuntime());
        e.advanceMs(5000 + 1);
        r.holdJudge(true);
        r.pass();
        const auto ex = expires(r);
        ASSERT_EQ(ex.size(), 1u);
        const uint32_t rid = e.ridOf(1);
        ASSERT_EQ(e.entry(1).state, LeaseEntryState::Emitting);
        LeaseEvent ev;
        ev.kind = LeaseEventKind::BrokerRejected; ev.rid = rid; ev.offset = ex[0].offset;
        ev.version = matches ? e.versionOf(1) : e.versionOf(1) + 1000;
        r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&ev, 1), e.leaseNow);
        if (matches) {
            EXPECT_EQ(e.entry(1).state, LeaseEntryState::Armed);
            EXPECT_EQ(e.st().ownRecordRejected, 1u);
            EXPECT_EQ(e.st().rejectedStale, 0u);
        } else {
            EXPECT_EQ(e.entry(1).state, LeaseEntryState::Free);
            EXPECT_EQ(e.st().ownRecordRejected, 0u);
            EXPECT_EQ(e.st().rejectedStale, 1u);
        }
    }
}

// 25. (F15) A lease batch counts in the namespace's disk bytes, as a produce does.  Mutation: drop the noteDiskAppend_ call: red.
TEST(LeaseEmitterRegression, NamespaceDiskBytesRegression) {
    Em e;
    VerdictRig& r = e.r;
    ASSERT_TRUE(e.produce({grantRec(1), grantRec(2)}));
    ASSERT_TRUE(r.driveUntilJudged());
    ASSERT_TRUE(e.waitRuntime());
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    r.worker->applyNsSlotAssignDirect(kSlot, kNs, rune::protocol::NamespaceLimits{});
    r.log->setNsTag(kNs, kSlot);
    const int64_t disk0 = r.worker->nsDiskBytesForTest(kSlot);
    const int64_t leoBefore = r.leo();
    e.advanceMs(5000 + 1);
    r.pass();
    const auto batches = storedBatches(r, leoBefore);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(r.worker->nsDiskBytesForTest(kSlot) - disk0, int64_t(batches[0].bytes));
}
