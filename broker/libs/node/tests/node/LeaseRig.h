#pragma once
// LeaseRig.h - lease topics Task 12: the shared rig of LeaseCheckpointTest.cpp and LeasePromotionTest.cpp (a real worker, a real lease
// index, a real judge, the lease clock under the test's control).  Same seams as LeaseEmitterTest / LeaseKeepAliveTest; the topic takes a
// whole LeaseSpec, so the checkpoint keys can be set (and altered).
#include "CheckedWritesRig.h"

#include <rune/node/lease/LeaseIndex.h>
#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace ltr {

using namespace rune::node;
using namespace rune::node::cw;
namespace rec = rune::protocol::record;
namespace pc  = rune::protocol::client;
using rune::node::lease::LeaseEntryState;

inline constexpr int64_t kMs  = 1'000'000;
inline constexpr int      kCap = 10'000;   // the hard iteration cap of every drive loop

inline std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}
inline void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}
inline uint64_t le(const std::vector<uint8_t>& v, std::size_t at, int bytes = 8) {
    uint64_t x = 0;
    for (int i = 0; i < bytes; ++i) x |= uint64_t(v[at + std::size_t(i)]) << (8 * i);
    return x;
}
inline std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t ttl = 5000, uint64_t remaining = 0) {
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
inline RawRec grantRec(uint64_t id, uint64_t ttl = 5000) { return {keyOf(id), leaseValue(id, 0, "a", ttl), 1}; }
inline RawRec updateRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, "b"), 2}; }
inline constexpr uint8_t kRevokeAttr = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);
inline RawRec revokeRec(uint64_t id, int64_t ver) { return {keyOf(id), leaseValue(id, ver, ""), kRevokeAttr}; }

struct Stored { int64_t offset; uint8_t attrs; std::string key; std::vector<uint8_t> value; };
struct StoredBatch { int64_t base = 0; bool control = false; std::vector<Stored> recs; };
/// Every batch of the log from `fromOffset`.
inline std::vector<StoredBatch> storedBatches(VerdictRig& r, int64_t fromOffset = 0) {
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
                b.base    = rd.baseOffset();
                b.control = (rd.attributes() & rune::protocol::batch::kAttrControl) != 0;
                if (!b.control)
                    rd.forEachRecord([&](std::string_view k, std::string_view val, int64_t o, int64_t, uint8_t a) {
                        b.recs.push_back({o, a, std::string(k), std::vector<uint8_t>(val.begin(), val.end())});
                    });
                out.push_back(std::move(b));
            }
            p += total;
        }
        off = v.nextOffset;
    }
    return out;
}
inline std::vector<Stored> storedRecords(VerdictRig& r) {
    std::vector<Stored> out;
    for (auto& b : storedBatches(r)) if (!b.control) out.insert(out.end(), b.recs.begin(), b.recs.end());
    return out;
}
inline std::vector<Stored> withOp(VerdictRig& r, rec::LeaseOp op) {
    std::vector<Stored> out;
    for (auto& s : storedRecords(r)) if (rec::leaseOpOf(s.attrs) == op) out.push_back(std::move(s));
    return out;
}
inline std::size_t countOp(VerdictRig& r, rec::LeaseOp op) { return withOp(r, op).size(); }

struct Ans { uint16_t index; uint16_t code; uint64_t ttl; };
struct Frame { int64_t corr; bool final; std::vector<Ans> es; };

inline rune::protocol::LeaseSpec specWith(uint32_t maxKeys, uint8_t checkpoint, uint32_t intervalMs, uint32_t rate) {
    rune::protocol::LeaseSpec s;
    s.maxKeys = maxKeys; s.checkpoint = checkpoint; s.checkpointIntervalMs = intervalMs; s.checkpointRate = rate;
    return s;
}

/// A lease topic on a real worker, the lease clock under the test's control, plus the keepalive helpers.
struct Lk {
    int64_t leaseNow = 1'000'000'000;   // declared before the rig: outlives the worker that reads it
    VerdictRig r;
    int32_t seq = 0;
    int64_t corr = 1000;
    std::vector<Frame> frames;
    std::vector<uint8_t> scanTail_;

    explicit Lk(const rune::protocol::LeaseSpec& spec, bool follower = false, RigTuning tuning = {}, int64_t rollBytes = 64LL * 1024 * 1024,
                KeyShape key = KeyShape{rune::codec::PrimType::UInt64}, std::optional<rune::codec::Descriptor> src = std::nullopt)
        : r(follower, rollBytes, tuning) {
        r.worker->setLeaseClockForTest(&leaseNow);
        r.createLeaseTopicSpec(spec, std::move(src), key);
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
    /// Demotion, then leadership again, one pass at a time until the runtime exists (the pass that creates it also runs the first promotion
    /// chunk).  The number of passes it took, or -1.
    int promoteStepwise() {
        r.demote();
        r.promoteWithoutPass();
        for (int i = 1; i <= 400; ++i) {
            r.pass();
            if (rt() != nullptr) return i;
        }
        return -1;
    }
    int64_t versionOf(uint64_t key) {
        for (const auto& s : storedRecords(r)) if (s.key == keyOf(key) && rec::leaseOpOf(s.attrs) == rec::LeaseOp::Put) return s.offset;
        return -1;
    }
    /// A column of the folded row (UINT64_MAX: no row).
    uint64_t rowRemaining(uint64_t key) {
        const auto lk = r.vs->leaseIndexFor(VerdictRig::kSrc, 0, r.replica->leadershipGeneration(), true, true);
        uint32_t rid = 0;
        if (lk.ref.table == nullptr) return UINT64_MAX;
        const uint8_t* row = lk.ref.table->find(rune::view::ViewKey::fromU64(key), rid);
        return row == nullptr ? UINT64_MAX : leaseRowRemaining(row, *lk.ref.layout);
    }
    uint64_t rowTtl(uint64_t key) {
        const auto lk = r.vs->leaseIndexFor(VerdictRig::kSrc, 0, r.replica->leadershipGeneration(), true, true);
        uint32_t rid = 0;
        if (lk.ref.table == nullptr) return UINT64_MAX;
        const uint8_t* row = lk.ref.table->find(rune::view::ViewKey::fromU64(key), rid);
        return row == nullptr ? UINT64_MAX : leaseRowTtl(row, *lk.ref.layout);
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
    /// Grants `count` keys (1..count) in batches of 500 with `ttl`, judged.  The runtime is NOT waited for.
    bool grantMany(uint64_t count, uint64_t ttl, uint64_t firstKey = 1) {
        for (uint64_t k = firstKey; k < firstKey + count;) {
            std::vector<RawRec> recs;
            for (int i = 0; i < 500 && k < firstKey + count; ++i, ++k) recs.push_back(grantRec(k, ttl));
            if (!produce(recs)) return false;
        }
        return r.driveUntilJudged();
    }

    /// Enqueues one LeaseKeepAlive op (served by the next pass).
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
    /// One decoded ScanLeases page (Task 13b).
    struct ScanPage {
        int16_t code = -99;   ///< -99: no answer within the pass cap
        int64_t asOf = -1;
        uint64_t lastHash = 0;
        bool done = false;
        std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> rows;   ///< (key, row payload)
        std::vector<uint64_t> ids() const {   // 8-byte keys as ids
            std::vector<uint64_t> v;
            for (const auto& [k, _] : rows) { uint64_t x = 0; for (std::size_t i = 0; i < k.size() && i < 8; ++i) x |= uint64_t(k[i]) << (8 * i); v.push_back(x); }
            return v;
        }
    };
    /// Enqueues one ScanLeases op (no pass).
    void postScan(std::span<const uint8_t> prefix, int32_t maxRows, uint64_t resumeHash, bool first, int32_t topic = VerdictRig::kSrc) {
        scanTail_.assign(prefix.begin(), prefix.end());
        WorkerOp op;
        op.type = WorkerOp::Type::ScanLeases; op.topicId = topic; op.partition = 0;
        op.sessionId = 1; op.correlationId = ++corr;
        op.nextOffset = int64_t(resumeHash); op.recordCount = maxRows; op.isolation = first ? 1 : 0;
        op.leaseTail = std::span<const uint8_t>(scanTail_.data(), scanTail_.size());
        EXPECT_TRUE(r.worker->tryEnqueueOp(std::move(op)));
    }
    static ScanPage decodeScan(const WorkerReturn& w) {
        ScanPage p;
        p.code = w.errorCode; p.asOf = w.baseOffset; p.lastHash = uint64_t(w.lastOffset); p.done = w.isolation == 1;
        const std::span<const uint8_t> c = w.chunkBytes();
        std::size_t at = 0;
        for (int i = 0; i < w.recordCount; ++i) {
            const std::size_t kl = c[at];
            std::vector<uint8_t> key(c.begin() + std::ptrdiff_t(at + 1), c.begin() + std::ptrdiff_t(at + 1 + kl));
            uint32_t rl = 0;
            std::memcpy(&rl, c.data() + at + 1 + kl, 4);
            std::vector<uint8_t> row(c.begin() + std::ptrdiff_t(at + 5 + kl), c.begin() + std::ptrdiff_t(at + 5 + kl + rl));
            p.rows.emplace_back(std::move(key), std::move(row));
            at += 5 + kl + rl;
        }
        EXPECT_EQ(at, c.size());
        return p;
    }
    /// One scan: post, pass until the answer arrives (hard cap).
    ScanPage scan(std::span<const uint8_t> prefix, int32_t maxRows, uint64_t resumeHash, bool first, int passes = 8) {
        r.leaseScanAnswers.clear();
        postScan(prefix, maxRows, resumeHash, first);
        for (int i = 0; i < passes; ++i) {
            r.pass();
            if (!r.leaseScanAnswers.empty()) { ScanPage p = decodeScan(r.leaseScanAnswers.front()); r.leaseScanAnswers.clear(); return p; }
        }
        return {};
    }
    /// Every page of a prefix scan, resumed by lastHash (hard cap on pages).  The pages are appended to `pages`.
    std::vector<uint64_t> scanAll(int32_t maxRows, std::vector<ScanPage>* pages = nullptr, std::span<const uint8_t> prefix = {}) {
        std::vector<uint64_t> all;
        uint64_t resume = 0;
        bool first = true;
        for (int i = 0; i < 4096; ++i) {
            ScanPage p = scan(prefix, maxRows, resume, first);
            EXPECT_EQ(p.code, 0);
            for (uint64_t id : p.ids()) all.push_back(id);
            const bool done = p.done || p.code != 0;
            resume = p.lastHash; first = false;
            if (pages != nullptr) pages->push_back(std::move(p));
            if (done) break;
        }
        return all;
    }
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
    void collect() {
        for (auto& w : r.leaseKaAnswers) frames.push_back(frameOf(w));
        r.leaseKaAnswers.clear();
    }
    void step() { r.pass(); collect(); }
    /// Enqueue + ONE pass: the frames of that pass.
    std::vector<Frame> keepAlive(const std::vector<std::pair<std::string, int64_t>>& entries, int64_t sessionId = 77) {
        const std::size_t before = frames.size();
        enqueue(entries, sessionId);
        step();
        return std::vector<Frame>(frames.begin() + std::ptrdiff_t(before), frames.end());
    }
    bool waitFrames(std::size_t n) {
        for (int i = 0; i < kCap && frames.size() < n; ++i) step();
        return frames.size() >= n;
    }
};

inline std::pair<std::string, int64_t> E(uint64_t key, int64_t version) { return {keyOf(key), version}; }

}  // namespace ltr
