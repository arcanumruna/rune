// Lease topics Task 11a: keepalive / TimeToLive serving on the worker and the park table (LeaseRuntime::keepAlive / flushClears,
// Worker::executeLeaseKeepAlive_ / executeLeaseTimeToLive_ / drainLeaseParks_).
//
// Real worker, real lease index, real judge: the lease clock is driven by hand (setLeaseClockForTest) and every drive loop is
// capped.  An op enqueued with enqueue() is served by the NEXT pass (op drain runs at Step 0, before driveViews_ and leaseDuty_).
#include "CheckedWritesRig.h"
#include "AllocWindow.h"

#include <rune/node/lease/LeaseIndex.h>
#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/Protocol.h>

#include <cstring>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;
namespace pc  = rune::protocol::client;
using rune::node::lease::LeaseEntryState;

namespace {

constexpr int64_t kMs = 1'000'000;
constexpr uint16_t kNotLeader = static_cast<uint16_t>(pc::ErrorCode::NotLeader);
constexpr uint16_t kNotFound  = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
constexpr uint16_t kThrottled = static_cast<uint16_t>(pc::ErrorCode::Throttled);
constexpr uint16_t kUnknownTopic = static_cast<uint16_t>(pc::ErrorCode::UnknownTopic);
constexpr uint16_t kWhole = pc::kLeaseWholeRequest;
constexpr int kCap = 10'000;   // the hard iteration cap of every drive loop

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
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t ttl = 5000, uint64_t remaining = 0) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, 1, 8);
    putLe(b, 28, remaining, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    return b;
}
RawRec grantRec(uint64_t id, uint64_t ttl = 5000) { return {keyOf(id), leaseValue(id, 0, "a", ttl), 1}; }
RawRec updateRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, "b"), 2}; }

struct Stored { int64_t offset; uint8_t attrs; std::string key; std::vector<uint8_t> value; };
std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (int64_t off = 0; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
            const std::size_t total = static_cast<std::size_t>(rd.totalLength());
            if (total < 12 || p + total > v.len) break;
            if ((rd.attributes() & rune::protocol::batch::kAttrControl) == 0)
                rd.forEachRecord([&](std::string_view k, std::string_view val, int64_t o, int64_t, uint8_t a) {
                    out.push_back({o, a, std::string(k), std::vector<uint8_t>(val.begin(), val.end())});
                });
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}
std::vector<Stored> withOp(VerdictRig& r, rec::LeaseOp op) {
    std::vector<Stored> out;
    for (auto& s : storedRecords(r)) if (rec::leaseOpOf(s.attrs) == op) out.push_back(std::move(s));
    return out;
}

struct Ans { uint16_t index; uint16_t code; uint64_t ttl; };
struct Frame { int64_t corr; bool final; std::vector<Ans> es; };

/// A lease topic on a real worker, the lease clock under the test's control, plus the keepalive helpers.
struct Ka {
    int64_t leaseNow = 1'000'000'000;   // declared before the rig: outlives the worker that reads it
    VerdictRig r;
    int32_t seq = 0;
    int64_t corr = 1000;
    std::vector<Frame> frames;   // every keepalive return seen so far, in order

    explicit Ka(uint32_t maxKeys = 1000, bool follower = false, bool leaseTopic = true) : r(follower, 64LL * 1024 * 1024, RigTuning{}) {
        r.worker->setLeaseClockForTest(&leaseNow);
        if (leaseTopic) r.createLeaseTopic(maxKeys);
        else r.typed();   // a plain topic: its write spec has no lease block
        r.passes(30);
        WorkerOp level;
        level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
        r.send(std::move(level));
        r.enableAcks(pc::kClientVersionLeases, 0);
        r.passes(30);
    }
    void advanceMs(int64_t ms) { leaseNow += ms * kMs; }
    bool produce(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), pc::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        const auto ack = r.ack(c, 600);
        return ack && ack->code == 0;
    }
    int64_t send(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), pc::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        return c;
    }
    lease::LeaseRuntime* rt() { return r.worker->leaseRuntimeForTest(VerdictRig::kSrc, 0); }
    uint32_t ridOf(uint64_t key) {
        const auto lk = r.vs->leaseIndexFor(VerdictRig::kSrc, 0, r.replica->leadershipGeneration(), true, true);
        uint32_t rid = UINT32_MAX;
        if (lk.ref.table == nullptr || !lk.ref.table->ridOf(rune::view::ViewKey::fromU64(key), rid)) return UINT32_MAX;
        return rid;
    }
    const lease::LeaseEntry& entry(uint64_t key) { return rt()->timers().entry(ridOf(key)); }
    bool waitRuntime(int maxPasses = 400) {
        for (int i = 0; i < maxPasses && rt() == nullptr; ++i) r.pass();
        return rt() != nullptr;
    }
    /// The grant offset of `key` (its lease_version).
    int64_t versionOf(uint64_t key) {
        for (const auto& s : storedRecords(r)) if (s.key == keyOf(key) && rec::leaseOpOf(s.attrs) == rec::LeaseOp::Put) return s.offset;
        return -1;
    }
    /// A lease_remaining_ms of the folded row (UINT64_MAX: no row).
    uint64_t rowRemaining(uint64_t key) {
        const auto lk = r.vs->leaseIndexFor(VerdictRig::kSrc, 0, r.replica->leadershipGeneration(), true, true);
        uint32_t rid = 0;
        if (lk.ref.table == nullptr) return UINT64_MAX;
        const uint8_t* row = lk.ref.table->find(rune::view::ViewKey::fromU64(key), rid);
        return row == nullptr ? UINT64_MAX : leaseRowRemaining(row, *lk.ref.layout);
    }
    /// A broker Checkpoint(remaining) of `key` at `version`, appended by the test (hold the judge to keep it unfolded).
    int64_t appendCheckpoint(uint64_t key, int64_t version, uint64_t remainingMs, uint64_t ttl = 5000) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        const auto v = leaseValue(key, version, "a", ttl, remainingMs);
        b.append(keyOf(key), std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0, rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
        const auto batch = b.build();
        const int64_t base = r.replica->appendLeaseBatch(batch);
        EXPECT_GE(base, 0);
        return base;
    }
    /// Checkpoints `key` to `remainingMs` and drives until the row shows it.
    bool checkpoint(uint64_t key, int64_t version, uint64_t remainingMs) {
        appendCheckpoint(key, version, remainingMs);
        for (int i = 0; i < kCap; ++i) {
            if (rowRemaining(key) == remainingMs) return true;
            r.pass();
        }
        ADD_FAILURE() << "the row of key " << key << " never showed remaining " << remainingMs;
        return false;
    }

    /// Enqueues one LeaseKeepAlive op (served by the next pass).  Returns its correlation id.
    int64_t enqueue(const std::vector<std::pair<std::string, int64_t>>& entries, int64_t sessionId = 77) {
        std::vector<uint8_t> tail;
        for (const auto& [k, ver] : entries) {
            tail.push_back(uint8_t(k.size()));
            tail.insert(tail.end(), k.begin(), k.end());
            for (int i = 0; i < 8; ++i) tail.push_back(uint8_t(uint64_t(ver) >> (8 * i)));
        }
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseKeepAlive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = sessionId; op.correlationId = ++corr; op.recordCount = int32_t(entries.size());
        op.leaseTail = std::span<const uint8_t>(tail.data(), tail.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        return corr;
    }
    /// One drained keepalive return as a Frame.
    static Frame frameOf(const WorkerReturn& w) {
        Frame f{w.correlationId, w.lastOffset == 1, {}};
        EXPECT_EQ(w.chunk.size(), std::size_t(w.recordCount) * 12);
        for (int i = 0; i < w.recordCount; ++i) {
            const uint8_t* p = w.chunk.data() + std::size_t(i) * 12;
            uint64_t t = 0;
            for (int j = 0; j < 8; ++j) t |= uint64_t(p[4 + j]) << (8 * j);
            f.es.push_back({uint16_t(p[0] | (p[1] << 8)), uint16_t(p[2] | (p[3] << 8)), t});
        }
        return f;
    }
    /// Moves the keepalive returns the rig drained into `frames`.
    void collect() {
        for (auto& w : r.leaseKaAnswers) frames.push_back(frameOf(w));
        r.leaseKaAnswers.clear();
    }
    /// One LeaseKeepAlive op with a hand-built tail and count (served by the next pass).  Returns its correlation id.
    int64_t enqueueRaw(const std::vector<uint8_t>& tail, int32_t count, int64_t sessionId = 77) {
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseKeepAlive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = sessionId; op.correlationId = ++corr; op.recordCount = count;
        op.leaseTail = std::span<const uint8_t>(tail.data(), tail.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        return corr;
    }
    /// One pass, then the frames it produced.
    void step() { r.pass(); collect(); }
    /// Enqueue + ONE pass: the frames of that pass (the answers decided at once).
    std::vector<Frame> keepAlive(const std::vector<std::pair<std::string, int64_t>>& entries, int64_t sessionId = 77) {
        const std::size_t before = frames.size();
        enqueue(entries, sessionId);
        step();
        return std::vector<Frame>(frames.begin() + std::ptrdiff_t(before), frames.end());
    }
    /// Passes until `n` frames in total were seen (hard cap).
    bool waitFrames(std::size_t n) {
        for (int i = 0; i < kCap && frames.size() < n; ++i) step();
        return frames.size() >= n;
    }
    /// A TimeToLive op and its answer.
    struct Ttl { bool got = false; int16_t code = 0; int64_t remainingMs = 0; int64_t version = 0; uint64_t ttl = 0; uint64_t holder = 0; };
    Ttl timeToLive(const std::string& key) {
        r.leaseTtlAnswers.clear();
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseTimeToLive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = 5; op.correlationId = ++corr;
        op.leaseTail = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
        r.pass();
        Ttl t;
        if (r.leaseTtlAnswers.size() != 1) { ADD_FAILURE() << r.leaseTtlAnswers.size() << " TimeToLive answers"; return t; }
        const WorkerReturn& w = r.leaseTtlAnswers[0];
        t.got = true; t.code = w.errorCode; t.remainingMs = w.baseOffset; t.version = w.lastOffset;
        EXPECT_EQ(w.correlationId, corr);
        if (w.chunk.size() == 16) {
            for (int i = 0; i < 8; ++i) { t.ttl |= uint64_t(w.chunk[std::size_t(i)]) << (8 * i); t.holder |= uint64_t(w.chunk[std::size_t(8 + i)]) << (8 * i); }
        }
        r.leaseTtlAnswers.clear();
        return t;
    }
    /// The standard setup: `keys` granted (ttl 5000), judged, the runtime up.
    bool setup(std::initializer_list<uint64_t> keys, uint64_t ttl = 5000) {
        std::vector<RawRec> recs;
        for (uint64_t k : keys) recs.push_back(grantRec(k, ttl));
        if (!produce(recs)) return false;
        if (!r.driveUntilJudged()) return false;
        return waitRuntime();
    }
};

std::pair<std::string, int64_t> E(uint64_t key, int64_t version) { return {keyOf(key), version}; }

}  // namespace

// 1. The read lease gates the keepalive like a view read.  Mutation M11 (drop the check in leaseServeable_) turns it red.
TEST(LeaseKeepAliveTableRegression, NoReadLeaseNotLeaderRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.r.worker->requireViewReadLease();   // no lease held yet
    auto fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    ASSERT_EQ(fr[0].es.size(), 1u);
    EXPECT_EQ(fr[0].es[0].index, kWhole);
    EXPECT_EQ(fr[0].es[0].code, kNotLeader);
    EXPECT_TRUE(fr[0].final);
    EXPECT_GE(k.r.worker->viewReadsWithoutLeaseForTest(), 1u);
    WorkerOp lease;
    lease.type = WorkerOp::Type::ReadLease; lease.nextOffset = k.leaseNow + 10'000 * kMs;
    ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(lease)));
    k.r.pass();
    fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    ASSERT_EQ(fr[0].es.size(), 1u);
    EXPECT_EQ(fr[0].es[0].index, 0);
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_EQ(fr[0].es[0].ttl, 5000u);
    k.leaseNow += 20'000 * kMs;   // the lease ended
    fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(fr[0].es[0].code, kNotLeader);
}

// 2. No runtime (a follower, a demoted leader) answers NotLeader for the whole request.
TEST(LeaseKeepAliveTableRegression, NotReadyNotLeaderRegression) {
    {   // a fresh leader: the runtime exists only once the lease index is ready (not yet, right after the promotion)
        Ka k;
        ASSERT_TRUE(k.setup({1}));
        const int64_t v = k.versionOf(1);
        k.r.demote();
        k.r.promoteWithoutPass();
        ASSERT_EQ(k.rt(), nullptr);
        const auto fr = k.keepAlive({E(1, v)});
        ASSERT_EQ(fr.size(), 1u);
        ASSERT_EQ(fr[0].es.size(), 1u);
        EXPECT_EQ(fr[0].es[0].index, kWhole);
        EXPECT_EQ(fr[0].es[0].code, kNotLeader);
        EXPECT_TRUE(fr[0].final);
    }
    {
        Ka k;
        ASSERT_TRUE(k.setup({1}));
        const int64_t v = k.versionOf(1);
        k.r.demote();
        ASSERT_EQ(k.rt(), nullptr);
        const auto fr = k.keepAlive({E(1, v), E(1, v)});
        ASSERT_EQ(fr.size(), 1u);
        ASSERT_EQ(fr[0].es.size(), 1u) << "one whole-request refusal, not one per entry";
        EXPECT_EQ(fr[0].es[0].index, kWhole);
        EXPECT_EQ(fr[0].es[0].code, kNotLeader);
    }
}

// 3. Absent key and wrong version are LeaseNotFound (definite); the right version is Ok with the granted TTL.
TEST(LeaseKeepAliveTableRegression, AbsentAndWrongVersionRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    const auto fr = k.keepAlive({E(99, 1), E(1, v + 1), E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_TRUE(fr[0].final);
    ASSERT_EQ(fr[0].es.size(), 3u);
    EXPECT_EQ(fr[0].es[0].index, 0); EXPECT_EQ(fr[0].es[0].code, kNotFound); EXPECT_EQ(fr[0].es[0].ttl, 0u);
    EXPECT_EQ(fr[0].es[1].index, 1); EXPECT_EQ(fr[0].es[1].code, kNotFound);
    EXPECT_EQ(fr[0].es[2].index, 2); EXPECT_EQ(fr[0].es[2].code, 0); EXPECT_EQ(fr[0].es[2].ttl, 5000u);
    EXPECT_EQ(fr[0].corr, k.corr);
}

// 4. Armed, no checkpoint: Ok at once, the deadline moves to now + ttl exactly, nothing is appended.  Mutation (skip the rearm) turns it red.
TEST(LeaseKeepAliveTableRegression, ArmedNoCheckpointRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.advanceMs(3000);
    const int64_t leo = k.r.leo();
    const auto fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    ASSERT_EQ(fr[0].es.size(), 1u);
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_EQ(fr[0].es[0].ttl, 5000u);
    EXPECT_TRUE(fr[0].final);
    EXPECT_EQ(k.entry(1).deadlineNs, k.leaseNow + 5000 * kMs);
    k.r.passes(3);
    EXPECT_EQ(k.r.leo(), leo) << "no record for a plain keepalive";
    k.advanceMs(4999);   // would be 1 ms past the ORIGINAL deadline (grant + 5 s) - the rearm moved it
    k.r.passes(3);
    EXPECT_TRUE(withOp(k.r, rec::LeaseOp::Expire).empty());
}

// 5. A checkpointed row (remaining > 0): the keepalive appends Checkpoint(0) (the record's bytes, the entry Clearing, one clear counted).
TEST(LeaseKeepAliveTableRegression, ArmedCheckpointClearsRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    k.r.holdJudge(true);
    const std::size_t cps = withOp(k.r, rec::LeaseOp::Checkpoint).size();
    const auto fr = k.keepAlive({E(1, v)});
    EXPECT_TRUE(fr.empty()) << "no answer in the pass that appended the clear";
    const auto now = withOp(k.r, rec::LeaseOp::Checkpoint);
    ASSERT_EQ(now.size(), cps + 1);
    const Stored& c = now.back();
    EXPECT_EQ(c.key, keyOf(1));
    EXPECT_EQ(c.attrs, rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
    EXPECT_EQ(le(c.value, 28), 0u) << "lease_remaining_ms = 0";
    EXPECT_EQ(int64_t(le(c.value, 4)), v) << "the row's version";
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Clearing);
    EXPECT_EQ(k.rt()->stats().clearsAppended, 1u);
    k.r.holdJudge(false);
}

// 5b. The answer comes only after the Checkpoint(0) folds.  Mutation M10 (answer Ok when the clear is appended) turns the "never Ok
// before it folds" assertions red.
TEST(LeaseKeepAliveTableRegression, ClearAnswersAfterFoldRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    k.r.holdJudge(true);
    EXPECT_TRUE(k.keepAlive({E(1, v)}).empty()) << "no answer in the pass that appended the clear";
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Clearing);
    for (int i = 0; i < 10; ++i) k.step();
    EXPECT_TRUE(k.frames.empty()) << "the clear has not folded: never Ok before it does";
    EXPECT_EQ(k.rowRemaining(1), 4000u);
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    ASSERT_EQ(k.frames.size(), 1u);
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].index, 0); EXPECT_EQ(k.frames[0].es[0].code, 0); EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).deadlineNs, k.leaseNow + 5000 * kMs);
    EXPECT_EQ(k.rowRemaining(1), 0u);
}

// 6. The clear waits behind a client update of the key that is appended but not judged (the last-appended gate): no Checkpoint is
// appended while it is open.  When the update folds (the broker-owned remaining time is not a client field: it survives) the gate
// opens, the Checkpoint(0) is appended, and only when that folds is the keepalive answered Ok.
TEST(LeaseKeepAliveTableRegression, ClearBehindClosedGateRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    const std::size_t cps = withOp(k.r, rec::LeaseOp::Checkpoint).size();
    k.r.holdJudge(true);
    (void)k.send({updateRec(1, v)});
    k.r.passes(30);
    const auto fr = k.keepAlive({E(1, v)});
    EXPECT_TRUE(fr.empty());
    for (int i = 0; i < 10; ++i) k.step();
    EXPECT_TRUE(k.frames.empty());
    EXPECT_EQ(withOp(k.r, rec::LeaseOp::Checkpoint).size(), cps) << "the gate is closed: no clear appended";
    EXPECT_GE(k.rt()->stats().clearDeferred, 1u);
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].code, 0);
    EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(withOp(k.r, rec::LeaseOp::Checkpoint).size(), cps + 1) << "the open gate let the Checkpoint(0) out, once";
    EXPECT_EQ(k.rowRemaining(1), 0u) << "answered after the clear folded";
}

// 7. A key past its deadline is never answered Ok: it parks until the Expire folds, then LeaseNotFound.  Mutation M12 (answer Ok for a due
// key) turns it red.
TEST(LeaseKeepAliveTableRegression, DueParksRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.advanceMs(5000 + 1);
    k.r.holdJudge(true);
    const auto fr = k.keepAlive({E(1, v)});   // served at Step 0, before this pass's leaseDuty_ emits the Expire
    EXPECT_TRUE(fr.empty()) << "a due key is not answered Ok";
    EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 1u);
    EXPECT_EQ(withOp(k.r, rec::LeaseOp::Expire).size(), 1u) << "the same pass's emit expired it";
    for (int i = 0; i < 5; ++i) k.step();
    EXPECT_TRUE(k.frames.empty());
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].index, 0);
    EXPECT_EQ(k.frames[0].es[0].code, kNotFound);
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 0u);
}

// 8a. Emitting parks until the Expire folds (LeaseNotFound).
TEST(LeaseKeepAliveTableRegression, EmittingParksRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.advanceMs(5000 + 1);
    k.r.holdJudge(true);
    k.r.pass();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Emitting);
    const auto fr = k.keepAlive({E(1, v)});
    EXPECT_TRUE(fr.empty());
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    EXPECT_EQ(k.frames[0].es[0].code, kNotFound);
    EXPECT_TRUE(k.frames[0].final);
}

// 8b. Clearing parks: a second keepalive while the first one's clear is in flight; both are answered Ok when it folds, in arrival order.
TEST(LeaseKeepAliveTableRegression, ClearingParksRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    k.r.holdJudge(true);
    const int64_t c1 = k.enqueue({E(1, v)});
    k.step();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Clearing);
    const std::size_t cps = withOp(k.r, rec::LeaseOp::Checkpoint).size();
    const int64_t c2 = k.enqueue({E(1, v)}, 78);
    k.step();
    EXPECT_TRUE(k.frames.empty());
    EXPECT_EQ(withOp(k.r, rec::LeaseOp::Checkpoint).size(), cps) << "one clear serves both";
    EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 2u);
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(2));
    ASSERT_EQ(k.frames.size(), 2u);
    EXPECT_EQ(k.frames[0].corr, c1); EXPECT_EQ(k.frames[1].corr, c2);
    for (const Frame& f : k.frames) {
        ASSERT_EQ(f.es.size(), 1u);
        EXPECT_EQ(f.es[0].code, 0); EXPECT_EQ(f.es[0].ttl, 5000u); EXPECT_TRUE(f.final);
    }
}

// 8c. Checkpointing parks (M3): a Checkpoint(R) in flight.  When it folds the remaining time is cleared (a Checkpoint(0) is appended)
// and only then Ok.  Mutation M3 (treat Checkpointing like Armed with remaining 0) answers Ok at once: red.
TEST(LeaseKeepAliveTableRegression, CheckpointingParksRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);
    const int64_t off = k.appendCheckpoint(1, v, 4000);
    ASSERT_GE(off, 0);
    k.rt()->timersForTest().setState(k.ridOf(1), LeaseEntryState::Checkpointing, off);
    const auto fr = k.keepAlive({E(1, v)});
    EXPECT_TRUE(fr.empty()) << "a checkpoint is in flight: not Ok";
    for (int i = 0; i < 5; ++i) k.step();
    EXPECT_TRUE(k.frames.empty());
    const std::size_t cps = withOp(k.r, rec::LeaseOp::Checkpoint).size();   // the test's Checkpoint(R)
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    EXPECT_EQ(k.frames[0].es[0].code, 0);
    EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(withOp(k.r, rec::LeaseOp::Checkpoint).size(), cps + 1) << "its fold appended the Checkpoint(0)";
    EXPECT_EQ(k.rowRemaining(1), 0u);
}

// 9. One request, mixed outcomes: the decided entries go out at once (final = 0), the parked one when its clear folds (final = 1).
TEST(LeaseKeepAliveTableRegression, BatchMixedAnswersRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1, 2, 3, 4}));
    ASSERT_TRUE(k.checkpoint(2, k.versionOf(2), 4000));
    k.r.holdJudge(true);
    const int64_t c = k.enqueue({E(99, 1), E(1, k.versionOf(1)), E(2, k.versionOf(2)), E(3, k.versionOf(3) + 1), E(4, k.versionOf(4))});
    k.step();
    ASSERT_EQ(k.frames.size(), 1u);
    EXPECT_EQ(k.frames[0].corr, c);
    EXPECT_FALSE(k.frames[0].final);
    ASSERT_EQ(k.frames[0].es.size(), 4u);
    const uint16_t idx[4]  = {0, 1, 3, 4};
    const uint16_t code[4] = {kNotFound, 0, kNotFound, 0};
    const uint64_t ttl[4]  = {0, 5000, 0, 5000};
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(k.frames[0].es[std::size_t(i)].index, idx[i]);
        EXPECT_EQ(k.frames[0].es[std::size_t(i)].code, code[i]);
        EXPECT_EQ(k.frames[0].es[std::size_t(i)].ttl, ttl[i]);
    }
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(2));
    ASSERT_EQ(k.frames.size(), 2u);
    EXPECT_EQ(k.frames[1].corr, c);
    EXPECT_TRUE(k.frames[1].final);
    ASSERT_EQ(k.frames[1].es.size(), 1u);
    EXPECT_EQ(k.frames[1].es[0].index, 2); EXPECT_EQ(k.frames[1].es[0].code, 0); EXPECT_EQ(k.frames[1].es[0].ttl, 5000u);
}

// 10. A topic drop answers the parked keepalives UnknownTopic (final) and destroys the runtime.
TEST(LeaseKeepAliveTableRegression, DropWithParkedUnknownTopicRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    k.r.holdJudge(true);
    const int64_t c = k.enqueue({E(1, v)});
    k.step();
    ASSERT_TRUE(k.frames.empty());
    ASSERT_EQ(k.r.worker->leaseParksForTest().inUse(), 1u);
    WorkerOp drop;
    drop.type = WorkerOp::Type::TopicDropped; drop.topicId = VerdictRig::kSrc;
    ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(drop)));
    k.step();
    ASSERT_EQ(k.frames.size(), 1u);
    EXPECT_EQ(k.frames[0].corr, c);
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].index, 0);
    EXPECT_EQ(k.frames[0].es[0].code, kUnknownTopic);
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(k.rt(), nullptr);
    EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 0u);
}

// 11. The park table (4096) binds: the next entry is Throttled (ttl 1 ms) and the parked ones stay parked.  A smaller owed cap binds first.
TEST(LeaseKeepAliveTableRegression, ParkTableFullThrottledRegression) {
    for (const bool smallCap : {false, true}) {
        Ka k;
        ASSERT_TRUE(k.setup({1}));
        const int64_t v = k.versionOf(1);
        k.r.worker->setMaxOwedReturns(smallCap ? 256 : 8192);
        const std::size_t limit = smallCap ? 256 : lease::kLeaseParkTableEntries;
        k.advanceMs(5000 + 1);
        k.r.holdJudge(true);
        k.r.pass();   // Emitting: every keepalive of the key parks
        ASSERT_EQ(k.entry(1).state, LeaseEntryState::Emitting);
        std::vector<std::pair<std::string, int64_t>> entries;
        for (std::size_t sent = 0; sent < limit; sent += 256) {
            entries.assign(std::min<std::size_t>(256, limit - sent), E(1, v));
            k.enqueue(entries);
            k.step();
        }
        EXPECT_TRUE(k.frames.empty());
        const std::size_t parked = k.r.worker->leaseParksForTest().inUse();
        EXPECT_EQ(parked, limit) << "smallCap " << smallCap;
        if (!smallCap) EXPECT_TRUE(k.r.worker->leaseParksForTest().full());
        const auto fr = k.keepAlive({E(1, v)});
        ASSERT_EQ(fr.size(), 1u) << "smallCap " << smallCap;
        ASSERT_EQ(fr[0].es.size(), 1u);
        EXPECT_EQ(fr[0].es[0].index, 0);
        EXPECT_EQ(fr[0].es[0].code, kThrottled);
        EXPECT_EQ(fr[0].es[0].ttl, 1u);
        EXPECT_TRUE(fr[0].final);
        EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), parked) << "the parked ones stay";
        k.r.holdJudge(false);
        for (int i = 0; i < 200; ++i) k.step();
        EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 0u);
        std::size_t answered = 0;
        for (const Frame& f : k.frames) for (const Ans& a : f.es) if (a.code == kNotFound) ++answered;
        EXPECT_EQ(answered, limit) << "every parked entry answered LeaseNotFound by the Expire's fold";
    }
}

// 12. Demotion answers every parked entry NotLeader, finals right.
TEST(LeaseKeepAliveTableRegression, ParkedThenDemotedRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.advanceMs(5000 + 1);
    k.r.holdJudge(true);
    k.r.pass();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Emitting);
    const int64_t c1 = k.enqueue({E(1, v), E(1, v)});
    k.step();
    const int64_t c2 = k.enqueue({E(1, v)}, 78);
    k.step();
    ASSERT_TRUE(k.frames.empty());
    ASSERT_EQ(k.r.worker->leaseParksForTest().inUse(), 3u);
    k.r.demote();
    k.collect();
    ASSERT_EQ(k.frames.size(), 3u);
    int finals1 = 0, finals2 = 0, n1 = 0, n2 = 0;
    for (const Frame& f : k.frames) {
        ASSERT_EQ(f.es.size(), 1u);
        EXPECT_EQ(f.es[0].code, kNotLeader);
        if (f.corr == c1) { ++n1; finals1 += f.final ? 1 : 0; }
        if (f.corr == c2) { ++n2; finals2 += f.final ? 1 : 0; }
    }
    EXPECT_EQ(n1, 2); EXPECT_EQ(finals1, 1) << "exactly the request's last answer is final";
    EXPECT_EQ(n2, 1); EXPECT_EQ(finals2, 1);
    EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 0u);
}

// 13. The op drain runs before leaseDuty_: a keepalive and the duty pass at 1 ns before the deadline rearm it, no Expire follows.
TEST(LeaseKeepAliveTableRegression, KeepAliveBeforePollReArmsRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    const int64_t deadline = k.entry(1).deadlineNs;
    k.leaseNow = deadline - 1;
    const auto fr = k.keepAlive({E(1, v)});   // Step 0 of this pass; leaseDuty_ of the same pass follows
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_TRUE(withOp(k.r, rec::LeaseOp::Expire).empty());
    k.leaseNow = deadline + 10 * kMs;
    k.r.passes(5);
    EXPECT_TRUE(withOp(k.r, rec::LeaseOp::Expire).empty()) << "the old deadline passed; the rearmed one did not";
    EXPECT_EQ(k.entry(1).deadlineNs, deadline - 1 + 5000 * kMs);
}

// 14. An Expire already built (Emitting): the keepalive parks and the Expire's fold answers LeaseNotFound.
TEST(LeaseKeepAliveTableRegression, KeepAliveAfterBuildParksRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    k.advanceMs(5000);
    k.r.holdJudge(true);
    k.r.pass();   // due at exactly the deadline: the Expire is appended
    ASSERT_EQ(withOp(k.r, rec::LeaseOp::Expire).size(), 1u);
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Emitting);
    EXPECT_TRUE(k.keepAlive({E(1, v)}).empty());
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    EXPECT_EQ(k.frames[0].es[0].code, kNotFound);
    EXPECT_TRUE(k.frames[0].final);
}

// 15. The namespace produce meter: every entry is charged before any is served; a throttled namespace refuses the whole request.
TEST(LeaseKeepAliveTableRegression, RateChargedPerEntryRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    rune::protocol::NamespaceLimits limits;
    k.r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
    k.r.log->setNsTag(kNs, kSlot);
    const uint64_t before = k.r.worker->nsProduceRecordsForTest(kSlot);
    std::vector<std::pair<std::string, int64_t>> ten(10, E(1, v));
    const auto fr = k.keepAlive(ten);
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(fr[0].es.size(), 10u);
    EXPECT_EQ(k.r.worker->nsProduceRecordsForTest(kSlot), before + 10);
    // throttled: produceUntil is 2.5 ms past the next pass's clock -> retry-after ceil = 3 ms
    k.r.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::ProduceRecords, k.r.now + 1 * kMs + 2'500'000);
    const int64_t deadline = k.entry(1).deadlineNs;
    k.advanceMs(100);
    const auto thr = k.keepAlive(ten);
    ASSERT_EQ(thr.size(), 1u);
    ASSERT_EQ(thr[0].es.size(), 1u);
    EXPECT_EQ(thr[0].es[0].index, kWhole);
    EXPECT_EQ(thr[0].es[0].code, kThrottled);
    EXPECT_EQ(thr[0].es[0].ttl, 3u);
    EXPECT_TRUE(thr[0].final);
    EXPECT_EQ(k.r.worker->nsProduceRecordsForTest(kSlot), before + 10) << "nothing charged";
    EXPECT_EQ(k.entry(1).deadlineNs, deadline) << "nothing served";
}

// 16. TimeToLive: present (three times, one past due), absent, not leader, and a Free entry (a fresh runtime) reports its promotion deadline.
TEST(LeaseKeepAliveTableRegression, TimeToLiveRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    const int64_t deadline = k.entry(1).deadlineNs;
    for (const int64_t at : {int64_t(0), int64_t(1234) * kMs + 999'999, int64_t(4000) * kMs}) {
        k.leaseNow = deadline - 5000 * kMs + at;
        const auto t = k.timeToLive(keyOf(1));
        ASSERT_TRUE(t.got);
        EXPECT_EQ(t.code, 0);
        EXPECT_EQ(t.ttl, 5000u);
        EXPECT_EQ(t.version, v);
        EXPECT_EQ(t.holder, 1u);
        const int64_t expectMs = at == 0 ? 5000 : (at == 4000 * kMs ? 1000 : 3765);   // 3765.000001 ms left: floor
        EXPECT_EQ(t.remainingMs, expectMs) << "at " << at;
    }
    k.r.holdJudge(true);   // keep the row through the pass that expires it
    k.leaseNow = deadline + 7 * kMs;
    const auto late = k.timeToLive(keyOf(1));
    EXPECT_EQ(late.code, 0);
    EXPECT_EQ(late.remainingMs, 0) << "past due";
    k.r.holdJudge(false);
    EXPECT_TRUE(k.r.driveUntilJudged());
    const auto absent = k.timeToLive(keyOf(99));
    EXPECT_EQ(absent.code, int16_t(kNotFound));
    EXPECT_EQ(absent.remainingMs, -1);
    EXPECT_EQ(absent.version, 0);
    EXPECT_EQ(absent.ttl, 0u);
    EXPECT_EQ(absent.holder, 0u);

    Ka f;
    ASSERT_TRUE(f.setup({1}));
    f.r.demote();
    const auto nl = f.timeToLive(keyOf(1));
    EXPECT_EQ(nl.code, int16_t(kNotLeader));
    EXPECT_EQ(nl.remainingMs, -1);
    f.r.promote();
    ASSERT_TRUE(f.waitRuntime());
    ASSERT_EQ(f.entry(1).state, LeaseEntryState::Free);
    const auto fresh = f.timeToLive(keyOf(1));
    EXPECT_EQ(fresh.code, 0);
    EXPECT_EQ(fresh.remainingMs, 5000) << "a Free entry: the promotion deadline (ready time + ttl; extend is 0 here)";
}

// 17. No allocation on the keepalive path: 100 ops of 256 entries (Ok, parks, clears), their clears, folds and answers - the worker's
// whole answer path runs inside the window.  Only the return collector (the gateway side: drainReturn builds a vector per return) is
// outside it.
TEST(LeaseKeepAliveAllocRegression, KeepAliveNoAllocRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1, 2}));
    const int64_t v1 = k.versionOf(1), v2 = k.versionOf(2);
    std::vector<uint8_t> tail;
    for (int i = 0; i < 256; ++i) {
        const bool second = i < 8;   // 8 entries of the checkpointed key, 248 plain ones
        const std::string key = keyOf(second ? 2 : 1);
        tail.push_back(uint8_t(key.size()));
        tail.insert(tail.end(), key.begin(), key.end());
        const int64_t ver = second ? v2 : v1;
        for (int j = 0; j < 8; ++j) tail.push_back(uint8_t(uint64_t(ver) >> (8 * j)));
    }
    uint64_t allocs = 0;
    uint64_t clears = k.rt()->stats().clearsAppended;
    for (int i = 0; i < 100; ++i) {
        const bool parks = i % 5 == 0;
        if (parks) ASSERT_TRUE(k.checkpoint(2, v2, 4000));   // outside the window: the next op parks 8 clears again
        k.r.pass();
        k.collect();
        k.frames.clear();
        WorkerOp op;
        op.type = WorkerOp::Type::LeaseKeepAlive; op.topicId = VerdictRig::kSrc; op.partition = 0;
        op.sessionId = 77; op.correlationId = ++k.corr; op.recordCount = 256;
        op.leaseTail = std::span<const uint8_t>(tail.data(), tail.size());
        uint64_t used = 0;
        int passesRun = 0;
        {
            rune::testing::AllocWindow w;
            const bool ok = k.r.worker->tryEnqueueOp(std::move(op));
            EXPECT_TRUE(ok);
            // the op, then the passes that append the clear, fold it and send the parked answers: until nothing is parked
            for (; passesRun < 50; ++passesRun) {
                k.r.now += 1'000'000;
                k.r.worker->doWork(k.r.now);
                if (passesRun > 0 && k.r.worker->leaseParksForTest().inUse() == 0) { ++passesRun; break; }
            }
            used = w.count();
        }
        ASSERT_LT(passesRun, 50) << "iteration " << i << ": the parked answers never left";
        if (i > 0) allocs += used;   // op 0 is the warm-up: the emit work area and the first growth are cold
        std::vector<Frame> got;
        k.r.worker->drainReturn([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::LeaseKeepAliveResult) got.push_back(Ka::frameOf(r));
        }, [](const TxnReturn&) {});
        std::size_t entries = 0, finals = 0;
        for (const Frame& f : got) {
            EXPECT_EQ(f.corr, k.corr);
            for (const Ans& a : f.es) {
                EXPECT_EQ(a.code, 0) << "iteration " << i;
                EXPECT_EQ(a.ttl, 5000u);
                ++entries;
            }
            finals += f.final ? 1 : 0;
        }
        EXPECT_EQ(entries, 256u) << "iteration " << i;
        EXPECT_EQ(finals, 1u) << "exactly the request's last frame is final";
        ASSERT_FALSE(got.empty());
        EXPECT_TRUE(got.back().final);
        if (parks) {
            ASSERT_EQ(got.size(), 9u) << "248 decided entries in the first frame, then the 8 parked answers one per frame";
            EXPECT_EQ(got[0].es.size(), 248u);
            EXPECT_FALSE(got[0].final);
        } else {
            ASSERT_EQ(got.size(), 1u);
            EXPECT_EQ(got[0].es.size(), 256u);
        }
        EXPECT_EQ(k.r.worker->leaseParksForTest().inUse(), 0u);
        const uint64_t nowClears = k.rt()->stats().clearsAppended;
        EXPECT_EQ(nowClears - clears, parks ? 1u : 0u) << "one Checkpoint(0) serves the 8 parked entries of the key";
        clears = nowClears;
    }
    EXPECT_EQ(allocs, 0u);
}

// 18. A Free entry (row present, not linked by a promotion pass yet) is armed by the first keepalive: at now + ttl, or at its promotion
// deadline when the row is checkpointed, then the clear path.
TEST(LeaseKeepAliveTableRegression, FreeEntryKeepAliveArmsRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1, 2}));
    const int64_t v1 = k.versionOf(1), v2 = k.versionOf(2);
    ASSERT_TRUE(k.checkpoint(2, v2, 4000));
    k.r.demote();
    k.r.promote();
    ASSERT_TRUE(k.waitRuntime());
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Free);
    ASSERT_EQ(k.entry(2).state, LeaseEntryState::Free);
    const int64_t ready = k.leaseNow;   // the runtime's ready time: the lease clock has not moved since it was created
    k.advanceMs(250);
    auto fr = k.keepAlive({E(1, v1)});
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_EQ(fr[0].es[0].ttl, 5000u);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).deadlineNs, k.leaseNow + 5000 * kMs);
    k.r.holdJudge(true);
    fr = k.keepAlive({E(2, v2)});
    EXPECT_TRUE(fr.empty());
    EXPECT_EQ(k.entry(2).state, LeaseEntryState::Clearing);
    EXPECT_EQ(k.entry(2).deadlineNs, ready + 4000 * kMs) << "armed at the promotion deadline (remaining), not at now + ttl";
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(2));   // frame 0 was key 1's immediate answer
    EXPECT_EQ(k.frames[1].es[0].code, 0);
    EXPECT_EQ(k.frames[1].es[0].ttl, 5000u);
    EXPECT_TRUE(k.frames[1].final);
    EXPECT_EQ(k.entry(2).deadlineNs, k.leaseNow + 5000 * kMs);
}

// 18b. R14: the leader heartbeat timeout (setLeaseExtendNs) is part of the promotion deadline of a Free entry.  A checkpointed row with
// a closed gate arms at exactly readyNs + extend + remaining; TimeToLive reports it before and after.  Mutation: drop extend in
// promotionDeadline turns the deadline assertions red.
TEST(LeaseKeepAliveTableRegression, FreeEntryPromotionDeadlineIncludesExtendRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_TRUE(k.checkpoint(1, v, 4000));
    k.r.worker->setLeaseExtendNs(2000 * kMs);
    k.r.demote();
    k.r.promote();
    ASSERT_TRUE(k.waitRuntime());
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Free);
    const int64_t ready = k.leaseNow;
    const auto before = k.timeToLive(keyOf(1));
    EXPECT_EQ(before.code, 0);
    EXPECT_EQ(before.remainingMs, 6000) << "ready + extend (2 s) + remaining (4 s)";
    k.r.holdJudge(true);
    (void)k.send({updateRec(1, v)});
    k.r.passes(30);
    EXPECT_TRUE(k.keepAlive({E(1, v)}).empty());
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed) << "parked behind the closed gate";
    EXPECT_EQ(k.entry(1).deadlineNs, ready + 2000 * kMs + 4000 * kMs);
    const auto after = k.timeToLive(keyOf(1));
    EXPECT_EQ(after.remainingMs, 6000);
}

// R1. A deferred Ok is only valid under the view-read lease: one sent after the lease ended is NotLeader.  Control leg: the lease still
// held -> Ok.  Mutation: drop the check in drainLeaseParks_ turns the lapsed leg red.
TEST(LeaseKeepAliveTableRegression, ParkedOkAfterReadLeaseEndsIsNotLeaderRegression) {
    for (const bool lapse : {false, true}) {
        Ka k;
        ASSERT_TRUE(k.setup({1}));
        const int64_t v = k.versionOf(1);
        ASSERT_TRUE(k.checkpoint(1, v, 4000));
        k.r.worker->requireViewReadLease();
        WorkerOp lease;
        lease.type = WorkerOp::Type::ReadLease; lease.nextOffset = k.leaseNow + 10'000 * kMs;
        ASSERT_TRUE(k.r.worker->tryEnqueueOp(std::move(lease)));
        k.r.pass();
        k.r.holdJudge(true);
        EXPECT_TRUE(k.keepAlive({E(1, v)}).empty()) << "parked behind the clear";
        ASSERT_EQ(k.entry(1).state, LeaseEntryState::Clearing);
        if (lapse) k.leaseNow += 20'000 * kMs;   // the read lease ended; no role change
        k.r.holdJudge(false);
        ASSERT_TRUE(k.waitFrames(1)) << "lapse " << lapse;
        ASSERT_EQ(k.frames.size(), 1u);
        ASSERT_EQ(k.frames[0].es.size(), 1u);
        EXPECT_EQ(k.frames[0].es[0].index, 0);
        EXPECT_TRUE(k.frames[0].final);
        if (lapse) {
            EXPECT_EQ(k.frames[0].es[0].code, kNotLeader);
            EXPECT_EQ(k.frames[0].es[0].ttl, 0u);
            EXPECT_EQ(k.r.worker->leaseOkWithheldForTest(), 1u);
        } else {
            EXPECT_EQ(k.frames[0].es[0].code, 0);
            EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
            EXPECT_EQ(k.r.worker->leaseOkWithheldForTest(), 0u);
        }
    }
}

// R2. The clear ring (capacity 8 here) never loses a park: stale entries (the entry was reset to Free while its rid sat on the ring) are
// compacted away, and a park that finds the ring genuinely full is Throttled - never parked without its clear flag.  Mutation: restore
// the silent return on a full ring (no compaction, no Throttled) leaves a parked rid unflagged: red.
TEST(LeaseKeepAliveTableRegression, ClearRingStaleEntriesNeverDropAParkRegression) {
    Ka k;
    constexpr uint64_t kKeys = 12;
    std::vector<RawRec> grants;
    for (uint64_t id = 1; id <= kKeys; ++id) grants.push_back(grantRec(id));
    ASSERT_TRUE(k.produce(grants));
    ASSERT_TRUE(k.r.driveUntilJudged());
    ASSERT_TRUE(k.waitRuntime());
    std::vector<int64_t> ver(kKeys + 1);
    for (uint64_t id = 1; id <= kKeys; ++id) {
        ver[id] = k.versionOf(id);
        ASSERT_TRUE(k.checkpoint(id, ver[id], 4000));
    }
    k.r.holdJudge(true);
    std::vector<RawRec> ups;
    for (uint64_t id = 1; id <= kKeys; ++id) ups.push_back(updateRec(id, ver[id]));
    (void)k.send(ups);   // appended, not judged: the gate of every key is closed
    k.r.passes(30);
    lease::LeaseRuntime* const rt = k.rt();
    ASSERT_NE(rt, nullptr);
    rt->setClearRingCapForTest(8);
    const auto lk = k.r.vs->leaseIndexFor(VerdictRig::kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
    ASSERT_NE(lk.ref.table, nullptr);
    lease::LeaseParkTable& parks = k.r.worker->leaseParksMutForTest();
    const int64_t judged = k.r.writeState()->judgedThrough;
    auto call = [&](uint64_t id, uint16_t index) {
        const std::string key = keyOf(id);
        uint32_t req = lease::kLeaseNil;
        const lease::LeaseKaOutcome o = rt->keepAlive(lk.ref, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size()), ver[id],
                                                      judged, k.leaseNow, parks, req, 77, ++k.corr, index, true);
        if (o.parked) EXPECT_TRUE(rt->clearFlagged(k.ridOf(id))) << "key " << id << ": parked without its clear flag";
        return o;
    };
    // 4 rounds x 3 keys: park (a ring entry), then the key ends (the entry is Free: its flag resets, the ring entry is now stale).
    for (int round = 0; round < 4; ++round) {
        std::vector<LeaseEvent> ended;
        for (uint64_t id = 1; id <= 3; ++id) {
            const lease::LeaseKaOutcome o = call(id, 0);
            ASSERT_TRUE(o.parked) << "round " << round << " key " << id;
            LeaseEvent ev;
            ev.kind = LeaseEventKind::Ended; ev.endOp = uint8_t(rec::LeaseOp::Revoke); ev.rid = k.ridOf(id); ev.version = ver[id]; ev.offset = 1'000'000 + round;
            ended.push_back(ev);
        }
        k.r.worker->onEventsForTest(VerdictRig::kSrc, 0, ended, k.leaseNow);
        for (uint64_t id = 1; id <= 3; ++id) EXPECT_EQ(k.entry(id).state, LeaseEntryState::Free);
    }
    // The final park of every key: 8 fit the ring, the rest are Throttled with ttl 1.
    std::size_t parked = 0, throttled = 0;
    for (uint64_t id = 1; id <= kKeys; ++id) {
        const lease::LeaseKaOutcome o = call(id, 0);
        if (o.parked) ++parked;
        else if (o.code == kThrottled && o.ttlMs == 1) ++throttled;
        else ADD_FAILURE() << "key " << id << ": code " << o.code;
    }
    EXPECT_EQ(parked, 8u);
    EXPECT_EQ(throttled, kKeys - 8);
    // The gate opens: every parked key's clear goes out and each is answered Ok exactly once.
    k.r.holdJudge(false);
    for (int i = 0; i < kCap; ++i) {
        k.step();
        std::size_t ok = 0;
        for (const Frame& f : k.frames) for (const Ans& a : f.es) if (a.code == 0) ++ok;
        if (ok >= parked) break;
    }
    std::size_t ok = 0, notFound = 0;
    for (const Frame& f : k.frames) for (const Ans& a : f.es) { if (a.code == 0) ++ok; if (a.code == kNotFound) ++notFound; }
    EXPECT_EQ(ok, parked) << "every flagged park was answered Ok";
    EXPECT_EQ(notFound, 12u) << "the 4 rounds' parks, answered when their keys ended";
    EXPECT_EQ(parks.inUse(), 0u);
}

// R3. A tail that does not hold `count` whole entries is one InvalidRequest for the whole request, before anything is served or charged.
TEST(LeaseKeepAliveTableRegression, KeepAliveBadTailInvalidRequestRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    rune::protocol::NamespaceLimits limits;
    k.r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
    k.r.log->setNsTag(kNs, kSlot);
    const uint64_t before = k.r.worker->nsProduceRecordsForTest(kSlot);
    const int64_t deadline = k.entry(1).deadlineNs;
    std::vector<uint8_t> tail;
    for (int e = 0; e < 2; ++e) {
        const std::string key = keyOf(1);
        tail.push_back(uint8_t(key.size()));
        tail.insert(tail.end(), key.begin(), key.end());
        for (int i = 0; i < 8; ++i) tail.push_back(uint8_t(uint64_t(v) >> (8 * i)));
    }
    k.advanceMs(100);
    k.enqueueRaw(tail, 3);   // count 3, two entries
    k.step();
    ASSERT_EQ(k.frames.size(), 1u);
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].index, kWhole);
    EXPECT_EQ(k.frames[0].es[0].code, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(k.frames[0].final);
    EXPECT_EQ(k.r.worker->nsProduceRecordsForTest(kSlot), before) << "nothing charged";
    EXPECT_EQ(k.entry(1).deadlineNs, deadline) << "nothing served: the two whole entries were not rearmed";
    // control: the same two entries with count 2 are served and charged
    k.enqueueRaw(tail, 2);
    k.step();
    ASSERT_EQ(k.frames.size(), 2u);
    EXPECT_EQ(k.frames[1].es.size(), 2u);
    EXPECT_EQ(k.r.worker->nsProduceRecordsForTest(kSlot), before + 2);
}

// R4. A plain (non-lease) topic is InvalidRequest, for both ops; the cells of R11 (a dropped topic) are UnknownTopic.
TEST(LeaseKeepAliveTableRegression, PlainTopicInvalidRequestRegression) {
    Ka k(1000, false, /*leaseTopic=*/false);
    const std::string key = keyOf(1);
    std::vector<uint8_t> tail;
    tail.push_back(uint8_t(key.size()));
    tail.insert(tail.end(), key.begin(), key.end());
    for (int i = 0; i < 8; ++i) tail.push_back(0);
    k.enqueueRaw(tail, 1);
    k.step();
    ASSERT_EQ(k.frames.size(), 1u);
    ASSERT_EQ(k.frames[0].es.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].index, kWhole);
    EXPECT_EQ(k.frames[0].es[0].code, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(k.frames[0].final);
    const auto t = k.timeToLive(key);
    ASSERT_TRUE(t.got);
    EXPECT_EQ(t.code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(t.remainingMs, -1);
}

// R11. A dropped topic: both ops answer UnknownTopic, and its runtime is gone after one pass.
TEST(LeaseKeepAliveTableRegression, DroppedTopicUnknownTopicRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    ASSERT_NE(k.r.writeState()->leaseRuntime.get(), nullptr);
    k.r.worker->setDroppedTopicsDirect({VerdictRig::kSrc});
    k.r.pass();
    EXPECT_EQ(k.r.writeState()->leaseRuntime.get(), nullptr) << "the runtime of a dropped topic does not outlive a pass";
    const auto fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u);
    ASSERT_EQ(fr[0].es.size(), 1u);
    EXPECT_EQ(fr[0].es[0].index, kWhole);
    EXPECT_EQ(fr[0].es[0].code, kUnknownTopic);
    EXPECT_TRUE(fr[0].final);
    const auto t = k.timeToLive(keyOf(1));
    ASSERT_TRUE(t.got);
    EXPECT_EQ(t.code, int16_t(kUnknownTopic));
    EXPECT_EQ(t.remainingMs, -1);
}

// R9. Both ops count as request ops; a paced TimeToLive is Throttled (remainingMs -1).
TEST(LeaseKeepAliveTableRegression, LeaseOpsMeteredAndPacedRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    constexpr uint16_t kSlot = 3;
    constexpr uint32_t kNs = 77;
    rune::protocol::NamespaceLimits limits;
    k.r.worker->applyNsSlotAssignDirect(kSlot, kNs, limits);
    k.r.log->setNsTag(kNs, kSlot);
    const uint64_t ops0 = k.r.worker->nsRequestOpsForTest(kSlot);
    (void)k.keepAlive({E(1, v)});
    EXPECT_EQ(k.r.worker->nsRequestOpsForTest(kSlot), ops0 + 1);
    auto t = k.timeToLive(keyOf(1));
    EXPECT_EQ(t.code, 0);
    EXPECT_EQ(k.r.worker->nsRequestOpsForTest(kSlot), ops0 + 2);
    k.r.worker->applyNsThrottleDirect(kSlot, kNs, ThrottleKind::QueryCycles, k.r.now + 1 * kMs + 2'500'000);
    t = k.timeToLive(keyOf(1));
    ASSERT_TRUE(t.got);
    EXPECT_EQ(t.code, int16_t(kThrottled));
    EXPECT_EQ(t.remainingMs, -1);
    EXPECT_EQ(k.r.worker->nsRequestOpsForTest(kSlot), ops0 + 3) << "a paced op is still counted";
}

// R7. The two ops and their two returns through the real op ring and return ring: every field the handlers and the collector see.
TEST(WorkerOpWireRegression, LeaseOpsRoundTripThroughWorkerRegression) {
    Ka k;
    ASSERT_TRUE(k.setup({1}));
    const int64_t v = k.versionOf(1);
    // LeaseKeepAlive: a 2-entry tail (a hit, then an absent key).
    k.r.leaseKaAnswers.clear();
    const int64_t c = k.enqueue({E(1, v), E(99, 7)}, /*sessionId=*/4242);
    k.r.pass();
    ASSERT_EQ(k.r.leaseKaAnswers.size(), 1u);
    const WorkerReturn& w = k.r.leaseKaAnswers[0];
    EXPECT_EQ(w.type, WorkerReturn::Type::LeaseKeepAliveResult);
    EXPECT_EQ(w.sessionId, 4242);
    EXPECT_EQ(w.correlationId, c);
    EXPECT_EQ(w.readTopicId, VerdictRig::kSrc);
    EXPECT_EQ(w.readPartition, 0);
    EXPECT_EQ(w.recordCount, 2);
    EXPECT_EQ(w.lastOffset, 1) << "final";
    std::vector<uint8_t> expect(24, 0);
    putLe(expect, 0, 0, 2); putLe(expect, 2, 0, 2); putLe(expect, 4, 5000, 8);               // index 0: Ok, ttl 5000
    putLe(expect, 12, 1, 2); putLe(expect, 14, kNotFound, 2); putLe(expect, 16, 0, 8);       // index 1: LeaseNotFound
    EXPECT_EQ(std::vector<uint8_t>(w.chunk.begin(), w.chunk.end()), expect);
    k.r.leaseKaAnswers.clear();
    // LeaseTimeToLive: the key bytes.
    const auto t = k.timeToLive(keyOf(1));
    ASSERT_TRUE(t.got);
    EXPECT_EQ(t.code, 0);
    EXPECT_EQ(t.version, v);
    EXPECT_EQ(t.ttl, 5000u);
    EXPECT_EQ(t.holder, 1u);
    EXPECT_EQ(t.remainingMs, 5000);
    const auto absent = k.timeToLive(keyOf(2));
    EXPECT_EQ(absent.code, int16_t(kNotFound));
    EXPECT_EQ(absent.remainingMs, -1);
}
