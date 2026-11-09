// Lease topics Task 10: the expiry emitter on a real worker (LeaseRuntime::emit, Worker::leaseDuty_, PartitionReplica::appendLeaseBatch).
//
// Every test drives the lease clock by hand (setLeaseClockForTest): the wheels run on it, never on wall time.  The clock override is
// installed BEFORE the first worker pass, so the runtime (created when the index first becomes ready) and every poll see one
// monotonic clock.
#include "CheckedWritesRig.h"
#include "AllocWindow.h"

#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>

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
    explicit Em(uint32_t maxKeys = 1000, RigTuning tuning = {}, bool follower = false)
        : r(follower, 64LL * 1024 * 1024, tuning), ackPasses(tuning.durabilityTier == 2 ? 600 * 50 : 600) {
        r.worker->setLeaseClockForTest(&leaseNow);
        r.createLeaseTopic(maxKeys);
        r.passes(30);
        WorkerOp level;
        level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
        r.send(std::move(level));
        r.enableAcks(rune::protocol::client::kClientVersionLeases, 0);
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
};

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
    std::printf("FullBatchContinuesNextCycle: %lld ms\n", static_cast<long long>(ms));
    EXPECT_LT(ms, 20'000);
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
    constexpr std::size_t kCap = 900;   // 61 header + 10 x 75 bytes of record fit, an 11th does not
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
    other.kind = LeaseEventKind::BrokerRejected; other.rid = rid; other.offset = x + 1;
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
    const lease::LeaseRuntime* old = e.rt();
    ASSERT_NE(old, nullptr);
    r.demote();
    r.passes(3);
    EXPECT_EQ(e.rt(), nullptr) << "the runtime of a follower is gone";
    r.promote();
    r.passes(60);
    if (e.rt() != nullptr) {   // a new runtime of the new leadership: every entry Free
        for (uint64_t k = 1; k <= 3; ++k) {
            const uint32_t rid = e.ridOf(k);
            ASSERT_NE(rid, UINT32_MAX);
            EXPECT_EQ(e.rt()->timers().entry(rid).state, LeaseEntryState::Free) << "key " << k;
        }
    }
    e.advanceMs(5000 + 1);
    r.passes(5);
    EXPECT_TRUE(expires(r).empty()) << "Task 12 arms these on promotion";
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
